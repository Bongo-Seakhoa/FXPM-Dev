"""
FX Portfolio Manager - Trading Strategies
==========================================

Contains all trading strategy implementations.
Includes 100 strategies across categories:
- Trend Following (33 strategies)
- Mean Reversion (39 strategies)
- Breakout/Momentum (28 strategies)

Each strategy provides:
- Signal generation (returns -1, 0, 1)
- Stop loss/take profit calculation
- Parameter grid for optimization
- Default parameters
- Feature requirements (for lazy loading optimization)

All strategies share a standardized SL/TP ATR multiplier grid defined in
_GLOBAL_SL_GRID and _GLOBAL_TP_GRID.

Version: 4.1 (Portfolio Manager — efficiency optimizations)
"""

import logging
import numpy as np
import pandas as pd
import weakref
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Tuple, Optional, Set

from pm_core import StrategyCategory, get_instrument_spec, InstrumentSpec, FeatureComputer


logger = logging.getLogger(__name__)

# Warn once when a strategy's cartesian grid gets very large.
_LARGE_PARAM_GRID_WARN_THRESHOLD: int = 1000
_LARGE_GRID_WARN_EMITTED: Set[Tuple[str, int]] = set()

# Optional structural-stop helper for trend and breakout families.
_STRUCTURAL_SL_DEFAULT_LOOKBACK: int = 20
_STRUCTURAL_SL_ATR_MAX_MULT: float = 1.5

# Optional live-only regime-aware TP multipliers.
#
# Defaults are neutral so the optimizer trades exactly the SL/TP surface that won
# discovery. Operators may still opt into a downstream live overlay by setting
# non-1.0 values in PositionConfig/config.json.
_DEFAULT_REGIME_TP_MULTIPLIERS: Dict[str, float] = {
    "TREND": 1.0,
    "BREAKOUT": 1.0,
    "RANGE": 1.0,
    "CHOP": 1.0,
}
_REGIME_TP_MULTIPLIERS: Dict[str, float] = dict(_DEFAULT_REGIME_TP_MULTIPLIERS)


def set_regime_tp_multipliers(multipliers: Optional[Dict[str, float]]) -> None:
    """Replace the active regime -> TP-multiplier table.

    Empty / None resets to the neutral module default. Keys are upper-cased so
    live trade intents agree on `'trend'` vs `'TREND'` with mixed-case configs.
    Non-positive or non-numeric values are dropped (treated as no-op).
    """
    global _REGIME_TP_MULTIPLIERS
    if not multipliers:
        _REGIME_TP_MULTIPLIERS = dict(_DEFAULT_REGIME_TP_MULTIPLIERS)
        return
    cleaned: Dict[str, float] = {}
    for k, v in dict(multipliers).items():
        try:
            mv = float(v)
        except (TypeError, ValueError):
            continue
        if mv <= 0:
            continue
        cleaned[str(k).strip().upper()] = mv
    _REGIME_TP_MULTIPLIERS = cleaned or dict(_DEFAULT_REGIME_TP_MULTIPLIERS)


def get_regime_tp_multipliers() -> Dict[str, float]:
    """Return a copy of the active regime → TP-multiplier table."""
    return dict(_REGIME_TP_MULTIPLIERS)


# =============================================================================
# GLOBAL SL/TP PARAM GRID — single source of truth for all strategies
# =============================================================================

_GLOBAL_SL_GRID: List[float] = list(np.arange(1.5, 3.5, 0.5))   # [1.5, 2.0, 2.5, 3.0]
_GLOBAL_TP_GRID: List[float] = list(np.arange(1.0, 6.5, 0.5))   # [1.0 .. 6.0]
_STRATEGY_HELPER_CACHE: Dict[int, Dict[Tuple[Any, ...], Any]] = {}
_STRATEGY_HELPER_FINALIZERS: Dict[int, weakref.finalize] = {}


# =============================================================================
# FEATURE LOOKUP HELPERS (efficiency optimization)
# =============================================================================

# Per-DataFrame registry so helpers only trust precomputed indicator columns
# when they were built with the expected parameters.
_PRECOMPUTED_ATTR = '_fxpm_precomputed_params'


def mark_precomputed(features: pd.DataFrame, **params_by_key: Tuple[Any, ...]) -> None:
    """Record which precomputed columns on *features* use which parameter tuples.

    Called by the feature-layer (pm_core) after writing precomputed indicators.
    Helper short-circuits on this tag rather than column presence, so a leftover
    column from a previous strategy run can never be returned as if it matched
    the current caller's parameters.
    """
    tags = features.attrs.get(_PRECOMPUTED_ATTR)
    if tags is None:
        tags = {}
        features.attrs[_PRECOMPUTED_ATTR] = tags
    tags.update(params_by_key)


def _precomputed_matches(features: pd.DataFrame, key: str, expected: Tuple[Any, ...]) -> bool:
    tags = features.attrs.get(_PRECOMPUTED_ATTR)
    if not tags:
        return False
    return tags.get(key) == expected


def _memoize_series(features: pd.DataFrame, col: str, result: pd.Series) -> None:
    """Memoize computed helper series unless ``features`` is a slice/view."""
    if getattr(features, "_is_copy", None) is not None:
        return
    features.loc[:, col] = result


def _get_ema(features: pd.DataFrame, period: int) -> pd.Series:
    """Get EMA from precomputed features or compute if missing (C3: memoize)."""
    col = f'EMA_{period}'
    if col in features.columns:
        return features[col]
    cache = _strategy_cache(features)
    key = ('EMA', period)
    if key in cache:
        return cache[key]
    result = features['Close'].ewm(span=period, adjust=False).mean()
    cache[key] = result
    return result


def _get_sma(features: pd.DataFrame, period: int) -> pd.Series:
    """Get SMA from precomputed features or compute if missing (C3: memoize)."""
    col = f'SMA_{period}'
    if col in features.columns:
        return features[col]
    cache = _strategy_cache(features)
    key = ('SMA', period)
    if key in cache:
        return cache[key]
    result = features['Close'].rolling(period).mean()
    cache[key] = result
    return result


def _get_tr(features: pd.DataFrame) -> pd.Series:
    """Get True Range. Memoized into features."""
    col = '_TR'
    if col in features.columns:
        return features[col]
    cache = _strategy_cache(features)
    key = ('TR',)
    if key in cache:
        return cache[key]
    high_low = features['High'] - features['Low']
    high_close = (features['High'] - features['Close'].shift(1)).abs()
    low_close = (features['Low'] - features['Close'].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    cache[key] = tr
    return tr


def _get_atr(features: pd.DataFrame, period: int) -> pd.Series:
    """Get ATR from precomputed features or compute if missing (C3: memoize)."""
    col = f'ATR_{period}'
    if col in features.columns:
        return features[col]
    cache = _strategy_cache(features)
    key = ('ATR', period)
    if key in cache:
        return cache[key]
    tr = _get_tr(features)
    result = tr.rolling(period).mean()
    cache[key] = result
    return result


def _cache_tag(*parts: Any) -> str:
    """Create a compact, DataFrame-safe cache suffix."""
    def _tag(value: Any) -> str:
        if isinstance(value, float):
            text = f"{value:.6g}"
        else:
            text = str(value)
        return text.replace("-", "m").replace(".", "p").replace(" ", "")

    return "_".join(_tag(part) for part in parts)


def _strategy_cache(features: pd.DataFrame) -> Dict[Tuple[Any, ...], Any]:
    """Transient helper cache that avoids mutating the feature frame layout."""
    frame_id = id(features)
    cache = _STRATEGY_HELPER_CACHE.get(frame_id)
    if cache is None:
        cache = {}
        _STRATEGY_HELPER_CACHE[frame_id] = cache
        _STRATEGY_HELPER_FINALIZERS[frame_id] = weakref.finalize(
            features,
            lambda fid=frame_id: (
                _STRATEGY_HELPER_CACHE.pop(fid, None),
                _STRATEGY_HELPER_FINALIZERS.pop(fid, None),
            ),
        )
    return cache


def _get_rsi(features: pd.DataFrame, period: int) -> pd.Series:
    """Get RSI from precomputed features or compute if missing (C3: memoize)."""
    col = f'RSI_{period}'
    if col in features.columns:
        return features[col]
    cache = _strategy_cache(features)
    key = ('RSI', period)
    if key in cache:
        return cache[key]
    result = FeatureComputer.rsi(features['Close'], period)
    cache[key] = result
    return result


def _get_keltner(features: pd.DataFrame, ema_period: int = 20,
                 atr_period: int = 14, mult: float = 2.0
                 ) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get Keltner Channel (mid, upper, lower) with parameter-aware caching."""
    requested = (int(ema_period), int(atr_period), float(mult))
    if (requested == (20, 20, 2.0)
            and _precomputed_matches(features, 'KC', requested)
            and {'KC_MID', 'KC_UPPER', 'KC_LOWER'}.issubset(features.columns)):
        return features['KC_MID'], features['KC_UPPER'], features['KC_LOWER']
    cache = _strategy_cache(features)
    cache_key = ('KC', ema_period, atr_period, float(mult))
    if cache_key in cache:
        return cache[cache_key]

    mid = _get_ema(features, ema_period)
    atr = _get_atr(features, atr_period)
    upper = mid + mult * atr
    lower = mid - mult * atr
    result = (mid, upper, lower)
    cache[cache_key] = result
    return result


def _detect_swing_points(series: pd.Series, order: int = 5
                         ) -> Tuple[pd.Series, pd.Series]:
    """Detect swing highs and lows using rolling-window comparison.

    Returns boolean Series pair (swing_highs, swing_lows).
    A swing high at bar i means series[i] >= all values in [i-order, i+order].
    Shifted by `order` to avoid lookahead.
    """
    swing_highs = pd.Series(False, index=series.index)
    swing_lows = pd.Series(False, index=series.index)
    if order <= 0:
        return swing_highs, swing_lows

    vals = pd.to_numeric(series, errors='coerce').to_numpy(dtype=float)
    window = 2 * order + 1
    if len(vals) < window:
        return swing_highs, swing_lows

    windows = np.lib.stride_tricks.sliding_window_view(vals, window_shape=window)
    center_vals = windows[:, order]
    all_nan = np.isnan(windows).all(axis=1)
    max_vals = np.max(np.where(np.isnan(windows), -np.inf, windows), axis=1)
    min_vals = np.min(np.where(np.isnan(windows), np.inf, windows), axis=1)

    delayed_start = window - 1
    swing_highs.iloc[delayed_start:] = (~all_nan) & (center_vals == max_vals)
    swing_lows.iloc[delayed_start:] = (~all_nan) & (center_vals == min_vals)
    return swing_highs, swing_lows


def _rolling_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank (0-100) of the current value within its window."""
    def _pct_rank(arr):
        if len(arr) < 2:
            return 50.0
        current = arr[-1]
        return (np.sum(arr[:-1] < current) / (len(arr) - 1)) * 100.0
    return series.rolling(window, min_periods=window).apply(_pct_rank, raw=True)


def _get_adx_di(features: pd.DataFrame, period: int = 14
                ) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get ADX, +DI, -DI from precomputed features or compute if missing (memoized).
    Uses the same rolling-mean / ATR basis as the core feature layer so
    helper and precomputed values stay on one mathematical surface.
    """
    requested = (int(period),)
    if (requested == (14,)
            and _precomputed_matches(features, 'ADX', requested)
            and {'ADX', 'PLUS_DI', 'MINUS_DI'}.issubset(features.columns)):
        return features['ADX'], features['PLUS_DI'], features['MINUS_DI']

    cache_tag = _cache_tag(period)
    adx_col = f'_ADX_{cache_tag}'
    pdi_col = f'_PLUS_DI_{cache_tag}'
    mdi_col = f'_MINUS_DI_{cache_tag}'

    # Return cached if available
    if {adx_col, pdi_col, mdi_col}.issubset(features.columns):
        return features[adx_col], features[pdi_col], features[mdi_col]
    cache = _strategy_cache(features)
    cache_key = ('ADX_DI', period)
    if cache_key in cache:
        return cache[cache_key]

    high = features['High']
    low = features['Low']

    # Match the core feature layer's directional-movement construction.
    plus_dm = high.diff()
    minus_dm = low.diff().abs() * -1
    plus_dm = plus_dm.where((plus_dm > minus_dm.abs()) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.abs().where((minus_dm.abs() > plus_dm) & (minus_dm < 0), 0.0)

    tr = _get_tr(features)
    atr = tr.rolling(period).mean()
    plus_di = 100.0 * (plus_dm.rolling(period).mean() / (atr + 1e-10))
    minus_di = 100.0 * (minus_dm.rolling(period).mean() / (atr + 1e-10))

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
    adx = dx.rolling(period).mean()

    result = (adx, plus_di, minus_di)
    cache[cache_key] = result
    return result


def _get_hull_ma(features: pd.DataFrame, period: int) -> pd.Series:
    """Get Hull MA from precomputed features or compute if missing (C3: memoize)."""
    col = f'HULL_MA_{period}' if period != 20 else 'HULL_MA'
    if col in features.columns:
        return features[col]
    cache = _strategy_cache(features)
    key = ('HULL_MA', period)
    if key in cache:
        return cache[key]
    result = FeatureComputer.hull_ma(features['Close'], period)
    cache[key] = result
    return result


def _get_bb(features: pd.DataFrame, period: int, std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get Bollinger Bands from precomputed features or compute if missing."""
    requested = (int(period), float(std))
    if (requested == (20, 2.0)
            and _precomputed_matches(features, 'BB_20', requested)
            and {'BB_MID_20', 'BB_UPPER_20', 'BB_LOWER_20'}.issubset(features.columns)):
        return features['BB_MID_20'], features['BB_UPPER_20'], features['BB_LOWER_20']
    cache = _strategy_cache(features)
    cache_key = ('BB', period, float(std))
    if cache_key in cache:
        return cache[cache_key]

    mid, upper, lower = FeatureComputer.bollinger_bands(features['Close'], period, std)
    result = (mid, upper, lower)
    cache[cache_key] = result
    return result


def _get_stochastic(features: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
    """Get Stochastic from precomputed features or compute if missing."""
    requested = (int(k_period), int(d_period))
    if (requested == (14, 3)
            and _precomputed_matches(features, 'STOCH', requested)
            and {'STOCH_K', 'STOCH_D'}.issubset(features.columns)):
        return features['STOCH_K'], features['STOCH_D']
    cache = _strategy_cache(features)
    cache_key = ('STOCH', k_period, d_period)
    if cache_key in cache:
        return cache[cache_key]
    low_min = features['Low'].rolling(window=k_period).min()
    high_max = features['High'].rolling(window=k_period).max()
    denom = high_max - low_min
    k = pd.Series(np.nan, index=features.index)
    valid = denom > 1e-10
    k[valid] = 100.0 * (features['Close'][valid] - low_min[valid]) / denom[valid]
    d = k.rolling(window=d_period).mean()
    result = (k, d)
    cache[cache_key] = result
    return result


def _get_macd(features: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get MACD from precomputed features or compute if missing."""
    requested = (int(fast), int(slow), int(signal))
    if (requested == (12, 26, 9)
            and _precomputed_matches(features, 'MACD', requested)
            and {'MACD', 'MACD_SIGNAL', 'MACD_HIST'}.issubset(features.columns)):
        return features['MACD'], features['MACD_SIGNAL'], features['MACD_HIST']
    return FeatureComputer.macd(features['Close'], fast, slow, signal)


def _get_volume(features: pd.DataFrame, default: float = 1.0) -> pd.Series:
    """Return broker/tick volume with a stable non-zero fallback."""
    if "Volume" in features.columns:
        vol = features["Volume"]
    elif "RealVolume" in features.columns:
        vol = features["RealVolume"]
    else:
        vol = pd.Series(default, index=features.index)
    vol = pd.to_numeric(vol, errors="coerce").replace([np.inf, -np.inf], np.nan)
    return vol.fillna(default).clip(lower=0.0)


def _session_keys(features: pd.DataFrame, session_start_hour: int = 0) -> pd.Series:
    """Session key shifted by start hour; non-datetime data is one session."""
    if isinstance(features.index, pd.DatetimeIndex):
        shifted = features.index - pd.Timedelta(hours=int(session_start_hour))
        return pd.Series(shifted.date, index=features.index)
    return pd.Series(0, index=features.index)


def _bar_number_in_session(features: pd.DataFrame, session_start_hour: int = 0) -> pd.Series:
    keys = _session_keys(features, session_start_hour)
    return pd.Series(1, index=features.index).groupby(keys).cumcount()


def _get_session_vwap(features: pd.DataFrame, session_start_hour: int = 0) -> pd.Series:
    volume = _get_volume(features, default=1.0)
    keys = _session_keys(features, session_start_hour)
    typical = (features["High"] + features["Low"] + features["Close"]) / 3.0
    pv = typical * volume.replace(0.0, 1.0)
    cum_pv = pv.groupby(keys).cumsum()
    cum_vol = volume.replace(0.0, 1.0).groupby(keys).cumsum()
    return cum_pv / (cum_vol + 1e-10)


def _double_ema(series: pd.Series, period: int) -> pd.Series:
    ema = series.ewm(span=period, adjust=False).mean()
    return 2.0 * ema - ema.ewm(span=period, adjust=False).mean()


def _heikin_ashi(features: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    ha_close = (features["Open"] + features["High"] + features["Low"] + features["Close"]) / 4.0
    ha_open = pd.Series(np.nan, index=features.index, dtype=float)
    if len(features) == 0:
        return ha_open, ha_close
    open_arr = pd.to_numeric(features["Open"], errors="coerce").to_numpy(dtype=float)
    close_arr = pd.to_numeric(features["Close"], errors="coerce").to_numpy(dtype=float)
    ha_close_arr = ha_close.to_numpy(dtype=float)
    ha_open_arr = np.empty(len(features), dtype=float)
    ha_open_arr[0] = (open_arr[0] + close_arr[0]) / 2.0
    for i in range(1, len(features)):
        ha_open_arr[i] = (ha_open_arr[i - 1] + ha_close_arr[i - 1]) / 2.0
    ha_open.iloc[:] = ha_open_arr
    return ha_open, ha_close


def _candle_parts(features: pd.DataFrame) -> Tuple[pd.Series, ...]:
    """Return open/high/low/close/body/range/wicks/bull/bear series."""
    open_ = pd.to_numeric(features["Open"], errors="coerce")
    high = pd.to_numeric(features["High"], errors="coerce")
    low = pd.to_numeric(features["Low"], errors="coerce")
    close = pd.to_numeric(features["Close"], errors="coerce")
    body = (close - open_).abs()
    candle_range = (high - low).abs()
    upper_wick = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_wick = pd.concat([open_, close], axis=1).min(axis=1) - low
    bull = close > open_
    bear = close < open_
    return open_, high, low, close, body, candle_range, upper_wick, lower_wick, bull, bear


# =============================================================================
# BASE STRATEGY CLASS
# =============================================================================


@dataclass
class TradeIntent:
    """Typed handoff from a local winner to the live executor."""

    symbol: str
    timeframe: str
    regime: str
    strategy_name: str
    direction: int
    entry_bar_time: Optional[str] = None
    signal_strength: float = 0.0
    selection_score: float = 0.0
    quality_score: float = 0.0
    stop_loss_pips: float = 0.0
    take_profit_pips: float = 0.0
    governance_hint: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.
    
    All strategies must implement:
    - name: Strategy identifier
    - category: Strategy category (trend, mean_reversion, breakout)
    - get_default_params(): Default parameter values
    - generate_signals(): Signal generation logic
    
    Optional:
    - get_required_features(): List of feature columns needed (for lazy loading)
    """
    
    def __init__(self, **params):
        """
        Initialize strategy with parameters.
        
        Args:
            **params: Strategy parameters (overrides defaults)
        """
        self.params = {}
        self._default_params = self.get_default_params()
        
        # Start with defaults
        for key, value in self._default_params.items():
            self.params[key] = value
        
        # Override with provided params
        for key, value in params.items():
            self.params[key] = value

        self.params = self.normalize_params(self.params)
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Strategy name/identifier."""
        pass
    
    @property
    @abstractmethod
    def category(self) -> StrategyCategory:
        """Strategy category."""
        pass
    
    @abstractmethod
    def get_default_params(self) -> Dict[str, Any]:
        """Get default parameter values."""
        pass

    def normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize legacy parameters into the current parameter surface."""
        return dict(params)
    
    @abstractmethod
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        """
        Generate trading signals.
        
        Args:
            features: DataFrame with OHLCV and indicators
            symbol: Symbol being traded
            
        Returns:
            Series of signals: -1 (short), 0 (flat), 1 (long)
        """
        pass
    
    MIN_BARS: int = 50

    @staticmethod
    def _zero_warmup(signals: pd.Series, warmup: int) -> pd.Series:
        """Zero out the first ``warmup`` bars to prevent NaN-derived signals."""
        if warmup > 0 and len(signals) > warmup:
            signals.iloc[:warmup] = 0
        return signals

    def get_required_features(self) -> Set[str]:
        """
        Get set of feature columns this strategy requires.

        Override in subclasses for lazy feature loading optimization.
        Returns empty set by default (compute all features).

        Returns:
            Set of column names required by this strategy
        """
        return set()

    def get_stop_required_features(self) -> Set[str]:
        """Return features required by stop calculation."""
        return {'ATR_14'}

    def get_feature_request(self) -> Set[str]:
        """Return the full parameter-aware feature request for this strategy."""
        return set(self.get_required_features()) | set(self.get_stop_required_features())
    
    def calculate_stops(self, features: pd.DataFrame,
                        signal: int, symbol: str,
                        spec: Optional[InstrumentSpec] = None,
                        bar_index: Optional[int] = None) -> Tuple[float, float]:
        """
        Calculate stop loss and take profit in pips.

        Default implementation uses ATR-based stops.

        Args:
            features: DataFrame with OHLCV and indicators (full or sliced)
            signal: Direction (1 for long, -1 for short)
            symbol: Symbol being traded
            spec: Optional InstrumentSpec
            bar_index: Optional bar index (if provided, features is the full DataFrame
                       and we read values at bar_index using .iat for O(1) access)

        Returns:
            Tuple of (stop_loss_pips, take_profit_pips)
        """
        # Get ATR (use bar_index with .iat for O(1) access in backtests)
        atr_col = 'ATR_14'
        if atr_col in features.columns:
            if bar_index is not None:
                atr = float(features[atr_col].iat[bar_index])
            else:
                atr = float(features[atr_col].iloc[-1])
        else:
            # Fallback (should be rare if FeatureComputer is producing ATR_14)
            if bar_index is None:
                _df = features
            else:
                _df = features.iloc[:bar_index + 1]
            high_low = _df['High'] - _df['Low']
            high_close = abs(_df['High'] - _df['Close'].shift(1))
            low_close = abs(_df['Low'] - _df['Close'].shift(1))
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = float(tr.rolling(14).mean().iloc[-1])

        if spec is None:
            spec = get_instrument_spec(symbol)
        atr_pips = spec.price_to_pips(atr)
        sl_mult = self.params.get('sl_atr_mult', 2.0)
        tp_mult = self.params.get('tp_atr_mult', 2.0)

        min_exit_pips = 5.0
        sl_pips = max(min_exit_pips, atr_pips * sl_mult)
        tp_pips = max(min_exit_pips, atr_pips * tp_mult)

        if self.params.get('use_structural_sl', False):
            structural_sl = self._compute_structural_sl_pips(
                features=features, signal=signal, spec=spec,
                bar_index=bar_index, atr_sl_pips=sl_pips,
            )
            if structural_sl is not None:
                # max(ATR, swing) widens to the structural level when it sits
                # beyond the ATR stop; clip at 1.5× ATR so a chaotic swing
                # cannot blow up risk per trade.
                cap = sl_pips * _STRUCTURAL_SL_ATR_MAX_MULT
                sl_pips = float(min(cap, max(sl_pips, structural_sl)))

        return sl_pips, tp_pips

    def build_trade_intent(
        self,
        features: pd.DataFrame,
        *,
        symbol: str,
        timeframe: str,
        regime: str,
        signal: int,
        spec: Optional[InstrumentSpec] = None,
        bar_index: Optional[int] = None,
        signal_strength: float = 0.0,
        selection_score: float = 0.0,
        quality_score: float = 0.0,
        governance_hint: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        apply_regime_tp_multiplier: bool = True,
    ) -> TradeIntent:
        """Build a typed executor payload without adding strategy-level policy."""
        sl_pips, tp_pips = self.calculate_stops(
            features,
            signal,
            symbol,
            spec=spec,
            bar_index=bar_index,
        )
        if apply_regime_tp_multiplier:
            tp_pips = self._apply_regime_tp_multiplier(tp_pips, features, bar_index)
        entry_bar_time: Optional[str] = None
        if bar_index is not None and 0 <= int(bar_index) < len(features.index):
            entry_bar_time = str(features.index[int(bar_index)])
        elif len(features.index) > 0:
            entry_bar_time = str(features.index[-1])
        return TradeIntent(
            symbol=symbol,
            timeframe=str(timeframe or "").upper(),
            regime=str(regime or "").upper(),
            strategy_name=self.name,
            direction=int(signal),
            entry_bar_time=entry_bar_time,
            signal_strength=float(signal_strength or 0.0),
            selection_score=float(selection_score or 0.0),
            quality_score=float(quality_score or 0.0),
            stop_loss_pips=float(sl_pips),
            take_profit_pips=float(tp_pips),
            governance_hint=dict(governance_hint or {}),
            metadata=dict(metadata or {}),
        )

    @staticmethod
    def _apply_regime_tp_multiplier(
        tp_pips: float,
        features: pd.DataFrame,
        bar_index: Optional[int],
    ) -> float:
        """Scale the base TP by the active regime's multiplier.

        Reads the look-ahead-safe ``REGIME_LIVE`` label (falls back to ``REGIME``
        when the live column is absent, e.g. in legacy fixtures). Silent no-op if
        the column is missing, the value is non-finite, the regime label is not
        in the active multipliers table, or the multiplier is non-positive. The
        neutral minimum-exit floor from ``calculate_stops`` is preserved.
        """
        if not _REGIME_TP_MULTIPLIERS:
            return tp_pips
        col = 'REGIME_LIVE' if 'REGIME_LIVE' in features.columns else (
            'REGIME' if 'REGIME' in features.columns else None
        )
        if col is None:
            return tp_pips
        try:
            if bar_index is not None:
                label = features[col].iat[int(bar_index)]
            else:
                label = features[col].iloc[-1]
        except (IndexError, KeyError, ValueError):
            return tp_pips
        if label is None:
            return tp_pips
        try:
            if isinstance(label, float) and not np.isfinite(label):
                return tp_pips
        except TypeError:
            pass
        key = str(label).strip().upper()
        if not key or key == 'NAN':
            return tp_pips
        mult = _REGIME_TP_MULTIPLIERS.get(key)
        if mult is None or mult <= 0:
            return tp_pips
        return max(5.0, float(tp_pips) * float(mult))

    def _compute_structural_sl_pips(
        self,
        features: pd.DataFrame,
        signal: int,
        spec: InstrumentSpec,
        bar_index: Optional[int],
        atr_sl_pips: float,
    ) -> Optional[float]:
        """Distance in pips from current Close to nearest structural swing.

        For longs we take the lowest Low in the lookback window (a structural
        support that has actually held). For shorts the highest High. Returns
        None when the lookback window is shorter than the available history
        or when prices are non-finite — the caller falls back to the pure
        ATR stop in that case.

        Pure read-from-past — never references bars at or after `bar_index`.
        """
        try:
            lookback = int(self.params.get('structural_sl_lookback',
                                           _STRUCTURAL_SL_DEFAULT_LOOKBACK))
        except (TypeError, ValueError):
            lookback = _STRUCTURAL_SL_DEFAULT_LOOKBACK
        if lookback < 2:
            return None

        if bar_index is None:
            end = len(features)
        else:
            end = int(bar_index) + 1
        start = max(0, end - lookback)
        if end - start < 2:
            return None

        close_arr = features['Close'].to_numpy()
        if end - 1 < 0 or end - 1 >= len(close_arr):
            return None
        current_close = float(close_arr[end - 1])
        if not np.isfinite(current_close):
            return None

        if signal > 0:
            window = features['Low'].to_numpy()[start:end]
            if window.size == 0:
                return None
            ref = float(np.nanmin(window))
            if not np.isfinite(ref) or ref >= current_close:
                return None
            distance = current_close - ref
        elif signal < 0:
            window = features['High'].to_numpy()[start:end]
            if window.size == 0:
                return None
            ref = float(np.nanmax(window))
            if not np.isfinite(ref) or ref <= current_close:
                return None
            distance = ref - current_close
        else:
            return None

        return float(spec.price_to_pips(distance))
    
    def get_param_grid(self) -> Dict[str, List]:
        """Get parameter grid for optimization."""
        return {}

    def cartesian_grid_size(self) -> int:
        """Compute cartesian-product size of `get_param_grid()`.

        Returns 0 for an empty grid. Pure observability — used by the optimizer
        log surface and the large-grid WARN. Does not influence search.
        """
        grid = self.get_param_grid()
        if not grid:
            return 0
        size = 1
        for values in grid.values():
            n = len(values) if values else 0
            if n == 0:
                return 0
            size *= n
        return size

    def warn_if_param_grid_large(self) -> int:
        """Emit one-time WARN per (strategy, size) when cartesian > threshold.

        Returns the cartesian size for caller logging. Idempotent across calls
        with identical (name, size); reset by clearing `_LARGE_GRID_WARN_EMITTED`.
        """
        size = self.cartesian_grid_size()
        if size > _LARGE_PARAM_GRID_WARN_THRESHOLD:
            key = (self.name, size)
            if key not in _LARGE_GRID_WARN_EMITTED:
                _LARGE_GRID_WARN_EMITTED.add(key)
                logger.warning(
                    "Large parameter grid: %s cartesian=%d > threshold=%d. "
                    "Under flat trial budget this family will receive sparser "
                    "coverage than smaller families. Consider enabling "
                    "`optuna_family_size_aware_budget` or raising "
                    "`optuna_max_trials_per_strategy`.",
                    self.name, size, _LARGE_PARAM_GRID_WARN_THRESHOLD,
                )
        return size

    @staticmethod
    def _base_sl_tp_grid() -> Dict[str, List[float]]:
        """Return the global SL/TP grid entries for use in get_param_grid()."""
        return {
            'sl_atr_mult': _GLOBAL_SL_GRID,
            'tp_atr_mult': _GLOBAL_TP_GRID,
        }

    @staticmethod
    def _structural_sl_defaults() -> Dict[str, Any]:
        """Opt-in structural-SL defaults for families that support it."""
        return {
            'use_structural_sl': False,
            'structural_sl_lookback': _STRUCTURAL_SL_DEFAULT_LOOKBACK,
        }

    @staticmethod
    def _structural_sl_grid() -> Dict[str, List]:
        """Opt-in structural-SL search axis."""
        return {'use_structural_sl': [True, False]}
    
    def set_params(self, **params):
        """Update strategy parameters."""
        for key, value in params.items():
            self.params[key] = value
    
    def get_params(self) -> Dict[str, Any]:
        """Get current parameters."""
        return self.params.copy()
    
    def __repr__(self) -> str:
        return f"{self.name}({self.params})"


# =============================================================================
# TREND FOLLOWING STRATEGIES
# =============================================================================

class EMACrossoverStrategy(BaseStrategy):
    """EMA Crossover Strategy - Fast/Slow EMA crossovers."""
    
    @property
    def name(self) -> str:
        return "EMACrossoverStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'fast_period': 10,
            'slow_period': 20,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        """EMA crossover needs EMA columns for fast and slow periods."""
        fast = self.params.get('fast_period', 10)
        slow = self.params.get('slow_period', 20)
        return {f'EMA_{fast}', f'EMA_{slow}'}
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        fast = self.params.get('fast_period', 10)
        slow = self.params.get('slow_period', 20)
        
        # Use precomputed EMAs if available (efficiency optimization)
        fast_ema = _get_ema(features, fast)
        slow_ema = _get_ema(features, slow)
        
        signals = pd.Series(0, index=features.index)
        cross_above = (fast_ema > slow_ema) & (fast_ema.shift(1) <= slow_ema.shift(1))
        cross_below = (fast_ema < slow_ema) & (fast_ema.shift(1) >= slow_ema.shift(1))
        
        signals[cross_above] = 1
        signals[cross_below] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'fast_period': [5, 8, 10, 13, 15],
            'slow_period': [20, 26, 30, 40, 50],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class SupertrendStrategy(BaseStrategy):
    """Supertrend Strategy - ATR-based trend following with band continuation.
    
    Signals fire on direction change (flip) only, not continuously.
    Implements proper recursive band continuation rules.
    """
    
    @property
    def name(self) -> str:
        return "SupertrendStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'atr_period': 10,
            'multiplier': 3.0,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        period = int(self.params.get('atr_period', 10))
        return {f'ATR_{period}'} if period in {7, 10, 14, 20} else set()
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('atr_period', 10)
        mult = self.params.get('multiplier', 3.0)
        
        # Use pre-computed ATR if available (efficiency optimization)
        atr_col = f'ATR_{period}'
        if atr_col in features.columns:
            atr = features[atr_col]
        elif 'ATR_14' in features.columns and period == 14:
            atr = features['ATR_14']
        else:
            # Calculate ATR
            high_low = features['High'] - features['Low']
            high_close = abs(features['High'] - features['Close'].shift(1))
            low_close = abs(features['Low'] - features['Close'].shift(1))
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = tr.rolling(period).mean()
        
        # Calculate bands
        hl2 = (features['High'] + features['Low']) / 2
        upperband = (hl2 + (mult * atr)).values
        lowerband = (hl2 - (mult * atr)).values
        close = features['Close'].values
        n = len(features)

        # Track direction with continuation rules - OPTIMIZED with NumPy
        direction = np.zeros(n, dtype=np.int32)
        final_upper = upperband.copy()
        final_lower = lowerband.copy()

        atr_values = atr.to_numpy(dtype=float)
        valid_start = np.flatnonzero(~np.isnan(atr_values))
        if len(valid_start) == 0:
            return pd.Series(np.zeros(n, dtype=np.int32), index=features.index)

        start = int(valid_start[0])
        final_upper[start] = upperband[start]
        final_lower[start] = lowerband[start]

        if close[start] > final_upper[start]:
            direction[start] = 1
        elif close[start] < final_lower[start]:
            direction[start] = -1

        for i in range(start + 1, n):
            # Band continuation rules (must remain sequential due to dependency)
            if lowerband[i] > final_lower[i-1] or close[i-1] < final_lower[i-1]:
                final_lower[i] = lowerband[i]
            else:
                final_lower[i] = final_lower[i-1]
            
            if upperband[i] < final_upper[i-1] or close[i-1] > final_upper[i-1]:
                final_upper[i] = upperband[i]
            else:
                final_upper[i] = final_upper[i-1]
            
            # Determine direction
            if close[i] > final_upper[i-1]:
                direction[i] = 1
            elif close[i] < final_lower[i-1]:
                direction[i] = -1
            else:
                direction[i] = direction[i-1]
        
        # Generate signals on direction change, including the first real
        # transition out of the initial neutral state.
        signals = np.zeros(n, dtype=np.int32)
        prev_direction = np.roll(direction, 1)
        prev_direction[:start + 1] = 0
        signals[(direction == 1) & (prev_direction != 1)] = 1
        signals[(direction == -1) & (prev_direction != -1)] = -1

        return pd.Series(signals, index=features.index)
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'atr_period': [7, 10, 14, 20, 25],
            'multiplier': [1.5, 2.0, 2.5, 3.0, 4.0],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class MACDTrendStrategy(BaseStrategy):
    """MACD Trend Strategy - MACD/Signal line crossovers."""
    
    @property
    def name(self) -> str:
        return "MACDTrendStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'fast_period': 12,
            'slow_period': 26,
            'signal_period': 9,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }
    
    def get_required_features(self) -> Set[str]:
        """MACD strategy uses precomputed MACD columns for default params."""
        fast = self.params.get('fast_period', 12)
        slow = self.params.get('slow_period', 26)
        sig = self.params.get('signal_period', 9)
        if fast == 12 and slow == 26 and sig == 9:
            return {'MACD', 'MACD_SIGNAL'}
        return set()
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        fast = self.params.get('fast_period', 12)
        slow = self.params.get('slow_period', 26)
        sig = self.params.get('signal_period', 9)
        
        # Use precomputed MACD if available (efficiency optimization)
        macd, signal_line, _ = _get_macd(features, fast, slow, sig)
        
        signals = pd.Series(0, index=features.index)
        cross_above = (macd > signal_line) & (macd.shift(1) <= signal_line.shift(1))
        cross_below = (macd < signal_line) & (macd.shift(1) >= signal_line.shift(1))
        
        signals[cross_above] = 1
        signals[cross_below] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'fast_period': [8, 10, 12, 16],
            'slow_period': [21, 26, 30, 34],
            'signal_period': [7, 9, 12],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class ADXTrendStrategy(BaseStrategy):
    """ADX Trend Strategy - DI crossovers with ADX strength filter.
    
    Entry-event based: fires on +DI/-DI cross when ADX confirms
    trend strength above threshold.
    """
    
    @property
    def name(self) -> str:
        return "ADXTrendStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'adx_period': 14,
            'adx_threshold': 25,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        period = int(self.params.get('adx_period', 14))
        required: Set[str] = set()
        if period == 14:
            required.update({'ADX', 'PLUS_DI', 'MINUS_DI'})
        if period in {7, 10, 14, 20}:
            required.add(f'ATR_{period}')
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('adx_period', 14)
        threshold = self.params.get('adx_threshold', 25)

        adx, plus_di, minus_di = _get_adx_di(features, period)
        
        signals = pd.Series(0, index=features.index)
        buy_signal = (plus_di > minus_di) & (plus_di.shift(1) <= minus_di.shift(1)) & (adx > threshold)
        sell_signal = (minus_di > plus_di) & (minus_di.shift(1) <= plus_di.shift(1)) & (adx > threshold)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'adx_period': [7, 10, 14, 20, 25],
            'adx_threshold': [15, 20, 25, 30, 35],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class IchimokuStrategy(BaseStrategy):
    """Ichimoku Cloud Strategy - Tenkan/Kijun crossovers with cloud filter."""
    
    @property
    def name(self) -> str:
        return "IchimokuStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'tenkan_period': 9,
            'kijun_period': 26,
            'senkou_b_period': 52,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        tenkan_p = self.params.get('tenkan_period', 9)
        kijun_p = self.params.get('kijun_period', 26)
        senkou_b_p = self.params.get('senkou_b_period', 52)
        
        tenkan = (features['High'].rolling(tenkan_p).max() + 
                  features['Low'].rolling(tenkan_p).min()) / 2
        kijun = (features['High'].rolling(kijun_p).max() + 
                 features['Low'].rolling(kijun_p).min()) / 2
        senkou_a = ((tenkan + kijun) / 2).shift(kijun_p)
        senkou_b = ((features['High'].rolling(senkou_b_p).max() + 
                     features['Low'].rolling(senkou_b_p).min()) / 2).shift(kijun_p)
        
        cloud_top = pd.concat([senkou_a, senkou_b], axis=1).max(axis=1)
        cloud_bottom = pd.concat([senkou_a, senkou_b], axis=1).min(axis=1)
        
        signals = pd.Series(0, index=features.index)
        above_cloud = features['Close'] > cloud_top
        below_cloud = features['Close'] < cloud_bottom
        tenkan_cross_up = (tenkan > kijun) & (tenkan.shift(1) <= kijun.shift(1))
        tenkan_cross_down = (tenkan < kijun) & (tenkan.shift(1) >= kijun.shift(1))
        
        signals[above_cloud & tenkan_cross_up] = 1
        signals[below_cloud & tenkan_cross_down] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'tenkan_period': [7, 9, 12, 15, 18],
            'kijun_period': [18, 22, 26, 30, 34],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class HullMATrendStrategy(BaseStrategy):
    """Hull MA Trend Strategy - Hull MA direction changes.
    
    Uses the optimized cached Hull MA implementation from FeatureComputer.
    """
    
    @property
    def name(self) -> str:
        return "HullMATrendStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 20,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        """Hull MA strategy uses precomputed HULL_MA for period 20."""
        period = self.params.get('period', 20)
        if period == 20:
            return {'HULL_MA'}
        return set()
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 20)
        
        # Use precomputed Hull MA if available (efficiency optimization)
        hma = _get_hull_ma(features, period)
        
        signals = pd.Series(0, index=features.index)
        hma_diff = hma.diff()
        turn_up = (hma_diff > 0) & (hma_diff.shift(1) <= 0)
        turn_down = (hma_diff < 0) & (hma_diff.shift(1) >= 0)
        
        signals[turn_up] = 1
        signals[turn_down] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [9, 14, 20, 30, 50],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class AroonTrendStrategy(BaseStrategy):
    """Aroon Trend Timing Strategy — "Time since highs/lows" trend detection.

    Aroon Up   = 100 * (period - bars_since_highest) / period
    Aroon Down = 100 * (period - bars_since_lowest)  / period

    Long entry:  Aroon Up crosses above Aroon Down AND Aroon Up > strength_level
    Short entry: Aroon Down crosses above Aroon Up AND Aroon Down > strength_level

    Signals fire on the crossover event only (not continuously).
    """

    @property
    def name(self) -> str:
        return "AroonTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 25,
            'strength_level': 70,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    @staticmethod
    def _aroon(high: pd.Series, low: pd.Series, period: int) -> Tuple[pd.Series, pd.Series]:
        """Compute Aroon Up and Aroon Down without lookahead.

        Uses a rolling window of ``period + 1`` bars (current bar inclusive).
        ``bars_since_high`` = number of bars since the highest high in the window.
        Aroon Up = 100 * (period - bars_since_high) / period.
        """
        aroon_up = pd.Series(np.nan, index=high.index)
        aroon_down = pd.Series(np.nan, index=low.index)

        high_vals = high.to_numpy(dtype=float)
        low_vals = low.to_numpy(dtype=float)
        window = period + 1
        if period <= 0 or len(high_vals) < window:
            return aroon_up, aroon_down

        high_windows = np.lib.stride_tricks.sliding_window_view(high_vals, window_shape=window)
        low_windows = np.lib.stride_tricks.sliding_window_view(low_vals, window_shape=window)

        valid_high = ~np.isnan(high_windows).all(axis=1)
        valid_low = ~np.isnan(low_windows).all(axis=1)

        masked_high = np.where(np.isnan(high_windows), -np.inf, high_windows)
        masked_low = np.where(np.isnan(low_windows), np.inf, low_windows)

        high_max = np.max(masked_high, axis=1)
        low_min = np.min(masked_low, axis=1)
        high_match = masked_high == high_max[:, None]
        low_match = masked_low == low_min[:, None]

        recent_high_idx = window - 1 - np.argmax(high_match[:, ::-1], axis=1)
        recent_low_idx = window - 1 - np.argmax(low_match[:, ::-1], axis=1)

        aroon_up.iloc[period:] = np.where(valid_high, 100.0 * recent_high_idx / period, np.nan)
        aroon_down.iloc[period:] = np.where(valid_low, 100.0 * recent_low_idx / period, np.nan)

        return aroon_up, aroon_down

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('period', 25))
        strength = float(self.params.get('strength_level', 70))

        aroon_up, aroon_down = self._aroon(features['High'], features['Low'], period)

        cross_up = (aroon_up > aroon_down) & (aroon_up.shift(1) <= aroon_down.shift(1))
        cross_down = (aroon_down > aroon_up) & (aroon_down.shift(1) <= aroon_up.shift(1))

        signals = pd.Series(0, index=features.index)
        signals[cross_up & (aroon_up > strength)] = 1
        signals[cross_down & (aroon_down > strength)] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [14, 20, 25, 30, 50],
            'strength_level': [50, 60, 70, 80, 90],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class ADXDIStrengthStrategy(BaseStrategy):
    """ADX + DI Directional Strength Trend Entry.

    A regime-confirmed trend entry that triggers on DI dominance confirmed
    by strong ADX.  Distinguished from ADXTrendStrategy by requiring
    sustained DI dominance (DI spread > di_spread_min) rather than just
    the cross event, and by using a rising-ADX filter.

    Long:  +DI crosses above -DI, ADX > adx_threshold, +DI - -DI > di_spread_min
    Short: -DI crosses above +DI, ADX > adx_threshold, -DI - +DI > di_spread_min

    Entry-event based: fires on the cross bar only.
    """

    @property
    def name(self) -> str:
        return "ADXDIStrengthStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'adx_period': 14,
            'adx_threshold': 20,
            'di_spread_min': 5.0,
            'require_adx_rising': True,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('adx_period', 14))
        adx_th = float(self.params.get('adx_threshold', 22))
        spread_min = float(self.params.get('di_spread_min', 5.0))
        adx_rising = bool(self.params.get('require_adx_rising', True))

        adx, plus_di, minus_di = _get_adx_di(features, period)

        # DI crosses
        cross_up = (plus_di > minus_di) & (plus_di.shift(1) <= minus_di.shift(1))
        cross_down = (minus_di > plus_di) & (minus_di.shift(1) <= plus_di.shift(1))

        # Strength filters
        strong_adx = adx > adx_th
        long_spread = (plus_di - minus_di) > spread_min
        short_spread = (minus_di - plus_di) > spread_min

        if adx_rising:
            adx_up = adx > adx.shift(1)
            strong_adx = strong_adx & adx_up

        signals = pd.Series(0, index=features.index)
        signals[cross_up & strong_adx & long_spread] = 1
        signals[cross_down & strong_adx & short_spread] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'adx_period': [7, 10, 14, 20],
            'adx_threshold': [15, 20, 25, 30],
            'di_spread_min': [0.0, 5.0, 10.0, 15.0],
            'require_adx_rising': [True, False],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class KeltnerPullbackStrategy(BaseStrategy):
    """Keltner Pullback Continuation Strategy.

    Trend continuation that enters on a pullback rather than a breakout.

    Logic:
    1. Trend definition: EMA slope (EMA > EMA shifted = uptrend) AND
       price above EMA for longs (below for shorts).
    2. Pullback: prior bar's low dipped to the EMA/Keltner midline or
       lower band (upper band for shorts).
    3. Confirmation: current bar closes back in trend direction.

    Signal-only strategy; ATR SL/TP stays centralized via calculate_stops.
    """

    @property
    def name(self) -> str:
        return "KeltnerPullbackStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'kc_period': 20,
            'kc_mult': 2.0,
            'ema_slope_bars': 5,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        kc_period = int(self.params.get('kc_period', 20))
        required: Set[str] = {f'EMA_{kc_period}'}
        if kc_period in {7, 10, 14, 20}:
            required.add(f'ATR_{kc_period}')
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        kc_period = int(self.params.get('kc_period', 20))
        kc_mult = float(self.params.get('kc_mult', 2.0))
        slope_bars = int(self.params.get('ema_slope_bars', 5))

        close = features['Close']

        mid, upper, lower = _get_keltner(features, kc_period, kc_period, kc_mult)

        # Trend filter: EMA rising/falling and price on correct side
        ema_rising = mid > mid.shift(slope_bars)
        ema_falling = mid < mid.shift(slope_bars)
        above_ema = close > mid
        below_ema = close < mid

        # Pullback detection: prior bar tagged the outer band, current bar
        # reclaims the trend side of the midline. kc_mult now changes behavior.
        long_pullback = features['Low'].shift(1) <= lower.shift(1)
        short_pullback = features['High'].shift(1) >= upper.shift(1)
        bullish_reclaim = close > mid
        bearish_reclaim = close < mid

        signals = pd.Series(0, index=features.index)
        signals[ema_rising & above_ema & long_pullback & bullish_reclaim] = 1
        signals[ema_falling & below_ema & short_pullback & bearish_reclaim] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'kc_period': [10, 15, 20, 25, 30],
            'kc_mult': [1.0, 1.5, 2.0, 2.5],
            'ema_slope_bars': [3, 5, 8, 10],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


# =============================================================================
# MEAN REVERSION STRATEGIES
# =============================================================================

class RSIExtremesStrategy(BaseStrategy):
    """RSI Extremes Strategy - Oversold/overbought reversals."""
    
    @property
    def name(self) -> str:
        return "RSIExtremesStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'rsi_period': 14,
            'oversold': 30,
            'overbought': 70,
            'use_ema200_trend_filter': False,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('rsi_period', 14)
        oversold = self.params.get('oversold', 30)
        overbought = self.params.get('overbought', 70)
        use_filter = bool(self.params.get('use_ema200_trend_filter', False))

        rsi = _get_rsi(features, period)

        signals = pd.Series(0, index=features.index)
        buy_signal = (rsi > oversold) & (rsi.shift(1) <= oversold)
        sell_signal = (rsi < overbought) & (rsi.shift(1) >= overbought)

        if use_filter:
            ema200 = _get_ema(features, 200)
            close = features['Close']
            buy_signal = buy_signal & (close > ema200)
            sell_signal = sell_signal & (close < ema200)

        signals[buy_signal] = 1
        signals[sell_signal] = -1

        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'rsi_period': [5, 7, 10, 14, 21],
            'oversold': [15, 20, 25, 30],
            'overbought': [70, 75, 80, 85],
            'use_ema200_trend_filter': [True, False],
            **self._base_sl_tp_grid(),
        }


class BollingerBounceStrategy(BaseStrategy):
    """Bollinger Bands Bounce Strategy - Mean reversion from band touches."""
    
    @property
    def name(self) -> str:
        return "BollingerBounceStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 20,
            'std_dev': 2.0,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 20)
        std_dev = self.params.get('std_dev', 2.0)
        _, upper, lower = _get_bb(features, period, std_dev)
        
        signals = pd.Series(0, index=features.index)
        touch_lower = features['Low'] <= lower
        bounce_up = features['Close'] > features['Close'].shift(1)
        touch_upper = features['High'] >= upper
        bounce_down = features['Close'] < features['Close'].shift(1)
        
        signals[touch_lower & bounce_up] = 1
        signals[touch_upper & bounce_down] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [10, 15, 20, 25, 30],
            'std_dev': [1.5, 1.75, 2.0, 2.25, 2.5],
            **self._base_sl_tp_grid(),
        }


class ZScoreMRStrategy(BaseStrategy):
    """Z-Score Mean Reversion Strategy - Statistical mean reversion."""
    
    @property
    def name(self) -> str:
        return "ZScoreMRStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 20,
            'entry_z': 2.0,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 20)
        entry_z = self.params.get('entry_z', 2.0)
        
        mean = features['Close'].rolling(period).mean()
        std = features['Close'].rolling(period).std()
        z_score = (features['Close'] - mean) / (std + 1e-10)
        
        signals = pd.Series(0, index=features.index)
        buy_signal = (z_score > -entry_z) & (z_score.shift(1) <= -entry_z)
        sell_signal = (z_score < entry_z) & (z_score.shift(1) >= entry_z)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [10, 15, 20, 30, 40],
            'entry_z': [1.5, 1.75, 2.0, 2.25, 2.5],
            **self._base_sl_tp_grid(),
        }


class StochasticReversalStrategy(BaseStrategy):
    """Stochastic Reversal Strategy - %K/%D crossovers in extreme zones."""
    
    @property
    def name(self) -> str:
        return "StochasticReversalStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'k_period': 14,
            'd_period': 3,
            'oversold': 20,
            'overbought': 80,
            'use_ema200_trend_filter': False,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        k_period = self.params.get('k_period', 14)
        d_period = self.params.get('d_period', 3)
        oversold = self.params.get('oversold', 20)
        overbought = self.params.get('overbought', 80)
        use_filter = bool(self.params.get('use_ema200_trend_filter', False))

        low_min = features['Low'].rolling(k_period).min()
        high_max = features['High'].rolling(k_period).max()
        denom = high_max - low_min
        stoch_k = pd.Series(50.0, index=features.index)
        valid = denom > 1e-8
        stoch_k[valid] = 100 * (features['Close'][valid] - low_min[valid]) / denom[valid]
        stoch_d = stoch_k.rolling(d_period).mean()

        signals = pd.Series(0, index=features.index)
        buy_signal = (stoch_k > stoch_d) & (stoch_k.shift(1) <= stoch_d.shift(1)) & (stoch_k < oversold + 10)
        sell_signal = (stoch_k < stoch_d) & (stoch_k.shift(1) >= stoch_d.shift(1)) & (stoch_k > overbought - 10)

        if use_filter:
            ema200 = _get_ema(features, 200)
            close = features['Close']
            buy_signal = buy_signal & (close > ema200)
            sell_signal = sell_signal & (close < ema200)

        signals[buy_signal] = 1
        signals[sell_signal] = -1

        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'k_period': [5, 9, 14, 21],
            'd_period': [3, 5, 7],
            'oversold': [10, 15, 20, 25],
            'overbought': [75, 80, 85, 90],
            'use_ema200_trend_filter': [True, False],
            **self._base_sl_tp_grid(),
        }


class CCIReversalStrategy(BaseStrategy):
    """CCI Reversal Strategy - Commodity Channel Index reversals."""
    
    @property
    def name(self) -> str:
        return "CCIReversalStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'cci_period': 20,
            'oversold': -100,
            'overbought': 100,
            'use_ema200_trend_filter': False,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('cci_period', 20)
        oversold = self.params.get('oversold', -100)
        overbought = self.params.get('overbought', 100)
        use_filter = bool(self.params.get('use_ema200_trend_filter', False))

        tp = (features['High'] + features['Low'] + features['Close']) / 3
        sma = tp.rolling(period).mean()
        mad = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean())
        cci = (tp - sma) / (0.015 * mad + 1e-10)

        signals = pd.Series(0, index=features.index)
        buy_signal = (cci > oversold) & (cci.shift(1) <= oversold)
        sell_signal = (cci < overbought) & (cci.shift(1) >= overbought)

        if use_filter:
            ema200 = _get_ema(features, 200)
            close = features['Close']
            buy_signal = buy_signal & (close > ema200)
            sell_signal = sell_signal & (close < ema200)

        signals[buy_signal] = 1
        signals[sell_signal] = -1

        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'cci_period': [10, 14, 20, 30],
            'oversold': [-150, -125, -100, -80],
            'overbought': [80, 100, 125, 150],
            'use_ema200_trend_filter': [True, False],
            **self._base_sl_tp_grid(),
        }


class WilliamsRStrategy(BaseStrategy):
    """Williams %R Strategy - Williams percent range reversals."""
    
    @property
    def name(self) -> str:
        return "WilliamsRStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 14,
            'oversold': -80,
            'overbought': -20,
            'use_ema200_trend_filter': False,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 14)
        oversold = self.params.get('oversold', -80)
        overbought = self.params.get('overbought', -20)
        use_filter = bool(self.params.get('use_ema200_trend_filter', False))

        high_max = features['High'].rolling(period).max()
        low_min = features['Low'].rolling(period).min()
        denom = high_max - low_min
        willr = pd.Series(-50.0, index=features.index)
        valid = denom > 1e-8
        willr[valid] = -100 * (high_max[valid] - features['Close'][valid]) / denom[valid]

        signals = pd.Series(0, index=features.index)
        buy_signal = (willr > oversold) & (willr.shift(1) <= oversold)
        sell_signal = (willr < overbought) & (willr.shift(1) >= overbought)

        if use_filter:
            ema200 = _get_ema(features, 200)
            close = features['Close']
            buy_signal = buy_signal & (close > ema200)
            sell_signal = sell_signal & (close < ema200)

        signals[buy_signal] = 1
        signals[sell_signal] = -1

        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [7, 10, 14, 21, 28],
            'oversold': [-95, -90, -85, -80],
            'overbought': [-20, -15, -10, -5],
            'use_ema200_trend_filter': [True, False],
            **self._base_sl_tp_grid(),
        }


class FisherTransformMRStrategy(BaseStrategy):
    """Fisher Transform Mean Reversion Strategy.

    Computes the proper clamped/recursive Fisher Transform of a normalised
    price oscillator, then triggers entries on the Fisher crossing back
    inside an extreme threshold.

    The Fisher Transform maps bounded values to an approximately Gaussian
    distribution, producing sharper turning-point signals than RSI/Stoch.

    Long:  Fisher crosses up through -threshold (was <= -threshold, now > -threshold)
    Short: Fisher crosses down through +threshold (was >= +threshold, now < +threshold)

    Best suited for mean-reversion / non-trending regimes.
    """

    @property
    def name(self) -> str:
        return "FisherTransformMRStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 10,
            'threshold': 1.5,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
        }

    def normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(params)
        normalized.pop('signal_period', None)
        return normalized

    @staticmethod
    def _fisher_transform(high: pd.Series, low: pd.Series, close: pd.Series,
                          period: int) -> Tuple[pd.Series, pd.Series]:
        """Compute recursive Fisher Transform.

        Steps:
        1. Normalise price to [-1, +1] using rolling highest/lowest over *period*.
        2. Apply EMA smoothing (alpha 0.5) to the normalised value.
        3. Clamp to (-0.999, +0.999) to prevent log blow-ups.
        4. Apply the Fisher Transform: fisher = 0.5 * ln((1+x)/(1-x))
           using recursive smoothing: fisher[i] = 0.5*transform + 0.5*fisher[i-1]
        5. Signal line = fisher shifted by 1 bar.
        """
        hl2 = (high + low) / 2.0
        highest = high.rolling(period, min_periods=period).max()
        lowest = low.rolling(period, min_periods=period).min()
        raw = 2.0 * ((hl2 - lowest) / (highest - lowest + 1e-10)) - 1.0

        # EMA smooth the normalised value (alpha=0.5 gives quick response)
        smooth = raw.ewm(alpha=0.5, min_periods=1, adjust=False).mean()

        # Clamp to avoid log blow-ups
        clamped = smooth.clip(-0.999, 0.999)

        # Recursive Fisher Transform
        fisher_vals = np.zeros(len(clamped))
        clamped_np = clamped.to_numpy(dtype=float)
        for i in range(len(clamped_np)):
            if np.isnan(clamped_np[i]):
                fisher_vals[i] = 0.0
            else:
                transform = 0.5 * np.log((1.0 + clamped_np[i]) / (1.0 - clamped_np[i]))
                fisher_vals[i] = 0.5 * transform + 0.5 * (fisher_vals[i - 1] if i > 0 else 0.0)

        fisher = pd.Series(fisher_vals, index=high.index)
        signal = fisher.shift(1)
        return fisher, signal

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('period', 10))
        threshold = float(self.params.get('threshold', 1.5))

        fisher, _ = self._fisher_transform(
            features['High'], features['Low'], features['Close'], period
        )

        # Cross back inside threshold (mean-reversion entry)
        long_cross = (fisher > -threshold) & (fisher.shift(1) <= -threshold)
        short_cross = (fisher < threshold) & (fisher.shift(1) >= threshold)

        signals = pd.Series(0, index=features.index)
        signals[long_cross] = 1
        signals[short_cross] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [5, 8, 10, 14, 20],
            'threshold': [1.0, 1.25, 1.5, 1.75, 2.0, 2.5],
            **self._base_sl_tp_grid(),
        }


class ZScoreVWAPReversionStrategy(BaseStrategy):
    """Z-Score / VWAP Deviation Mean Reversion Strategy.

    "Fair value reversion" using rolling VWAP (tick-volume weighted if
    available) and a z-score of the deviation.

    Entry on a cross back inside the z-threshold (instead of catching the
    knife at max deviation).  Optional ADX filter to prefer ranging regimes
    (ADX below threshold).

    Long:  z crosses up through -entry_z
    Short: z crosses down through +entry_z

    Uses precomputed VWAP column if present, else computes from Volume.
    """

    @property
    def name(self) -> str:
        return "ZScoreVWAPReversionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'vwap_window': 50,
            'z_window': 50,
            'entry_z': 2.0,
            'adx_threshold': 25,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
        }

    def normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(params)
        use_adx = normalized.pop('use_adx_filter', None)
        if use_adx is False:
            normalized['adx_threshold'] = 0
        return normalized

    @staticmethod
    def _rolling_vwap(df: pd.DataFrame, window: int) -> pd.Series:
        """Compute rolling VWAP.  Prefer precomputed column if available."""
        col = f'VWAP_{window}'
        if col in df.columns:
            return df[col]

        vol = pd.to_numeric(df.get('Volume', pd.Series(1.0, index=df.index)),
                            errors='coerce').fillna(1.0)
        tp = (df['High'] + df['Low'] + df['Close']) / 3.0
        pv = tp * vol
        vol_sum = vol.rolling(window=window, min_periods=window).sum()
        pv_sum = pv.rolling(window=window, min_periods=window).sum()
        return pv_sum / (vol_sum + 1e-10)

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        vwap_w = int(self.params.get('vwap_window', 50))
        z_w = int(self.params.get('z_window', 50))
        entry_z = float(self.params.get('entry_z', 2.0))

        vwap = self._rolling_vwap(features, vwap_w)
        dev = features['Close'] - vwap

        mu = dev.rolling(window=z_w, min_periods=z_w).mean()
        sd = dev.rolling(window=z_w, min_periods=z_w).std()
        z = (dev - mu) / (sd + 1e-10)

        # Optional ADX range filter: threshold <= 0 disables it.
        adx_th = float(self.params.get('adx_threshold', 25))
        if adx_th > 0 and 'ADX' in features.columns:
            allow = features['ADX'] < adx_th
        else:
            allow = pd.Series(True, index=features.index)

        long_entry = allow & (z > -entry_z) & (z.shift(1) <= -entry_z)
        short_entry = allow & (z < entry_z) & (z.shift(1) >= entry_z)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'vwap_window': [20, 50, 100, 150, 200],
            'z_window': [20, 30, 50, 100],
            'entry_z': [1.5, 1.75, 2.0, 2.25, 2.5],
            'adx_threshold': [0, 15, 20, 25, 30],
            **self._base_sl_tp_grid(),
        }


# =============================================================================
# BREAKOUT / MOMENTUM STRATEGIES
# =============================================================================

class DonchianBreakoutStrategy(BaseStrategy):
    """Donchian Breakout Strategy - Pure structure breakout.
    
    Clean breakout above prior rolling high / below prior rolling low.
    Uses shift(1) to avoid lookahead.  Exits handled entirely by the
    shared ATR SL/TP via calculate_stops.
    """
    
    @property
    def name(self) -> str:
        return "DonchianBreakoutStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 20,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 20)

        high_max = features['High'].rolling(period).max()
        low_min = features['Low'].rolling(period).min()

        signals = pd.Series(0, index=features.index)
        # Breakout above channel (shift(1) avoids lookahead)
        breakout_up = features['Close'] > high_max.shift(1)
        # Breakout below channel
        breakout_down = features['Close'] < low_min.shift(1)

        signals[breakout_up] = 1
        signals[breakout_down] = -1

        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [10, 15, 20, 30, 50],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class VolatilityBreakoutStrategy(BaseStrategy):
    """Volatility Breakout Strategy - ATR-based breakouts."""
    
    @property
    def name(self) -> str:
        return "VolatilityBreakoutStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'atr_period': 20,
            'breakout_mult': 1.0,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('atr_period', 20)
        mult = self.params.get('breakout_mult', 1.0)
        
        # Calculate ATR
        high_low = features['High'] - features['Low']
        high_close = abs(features['High'] - features['Close'].shift(1))
        low_close = abs(features['Low'] - features['Close'].shift(1))
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        # Event-based breakout: emit only when the move first exceeds the ATR threshold.
        price_change = features['Close'].diff()
        threshold = atr * mult
        cross_up = (price_change > threshold) & (price_change.shift(1) <= threshold)
        cross_down = (price_change < -threshold) & (price_change.shift(1) >= -threshold)

        signals = pd.Series(0, index=features.index)
        signals[cross_up] = 1
        signals[cross_down] = -1

        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'atr_period': [10, 14, 20, 30],
            'breakout_mult': [0.5, 0.75, 1.0, 1.25, 1.5],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class MomentumBurstStrategy(BaseStrategy):
    """Momentum Burst Strategy - Rapid momentum changes."""
    
    @property
    def name(self) -> str:
        return "MomentumBurstStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'momentum_period': 10,
            'threshold_pct': 1.0,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('momentum_period', 10)
        threshold = self.params.get('threshold_pct', 1.0)

        momentum = (features['Close'] / features['Close'].shift(period) - 1) * 100

        signals = pd.Series(0, index=features.index)
        signals[(momentum > threshold) & (momentum.shift(1) <= threshold)] = 1
        signals[(momentum < -threshold) & (momentum.shift(1) >= -threshold)] = -1

        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'momentum_period': [3, 5, 10, 15, 20],
            'threshold_pct': [0.3, 0.5, 1.0, 1.5, 2.0],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class SqueezeBreakoutStrategy(BaseStrategy):
    """Bollinger Squeeze -> Expansion Breakout Strategy.
    
    Detects squeeze (BB inside KC or low BB bandwidth), then takes the
    first expansion move.  "Squeeze released" is based on prior-bar
    squeeze state to avoid repainting.
    """
    
    @property
    def name(self) -> str:
        return "SqueezeBreakoutStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'bb_period': 20,
            'bb_std': 2.0,
            'kc_period': 20,
            'kc_mult': 1.5,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        bb_period = self.params.get('bb_period', 20)
        bb_std = self.params.get('bb_std', 2.0)
        kc_period = self.params.get('kc_period', 20)
        kc_mult = self.params.get('kc_mult', 1.5)
        
        # Bollinger Bands (shared helper)
        bb_mid, bb_upper, bb_lower = _get_bb(features, bb_period, bb_std)

        # Keltner Channels (shared helper)
        kc_mid, kc_upper, kc_lower = _get_keltner(features, kc_period, kc_period, kc_mult)
        
        # Squeeze detection: BB inside KC
        squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)
        squeeze_off = ~squeeze_on
        
        # Momentum
        momentum = features['Close'] - features['Close'].rolling(bb_period).mean()
        
        signals = pd.Series(0, index=features.index)
        # Squeeze releases with positive momentum (prior-bar squeeze state)
        buy_signal = squeeze_off & squeeze_on.shift(1) & (momentum > 0)
        sell_signal = squeeze_off & squeeze_on.shift(1) & (momentum < 0)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'bb_period': [10, 15, 20, 25],
            'bb_std': [1.5, 2.0, 2.5, 3.0],
            'kc_mult': [1.0, 1.25, 1.5, 2.0],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class KeltnerBreakoutStrategy(BaseStrategy):
    """Keltner Channel Breakout Strategy."""
    
    @property
    def name(self) -> str:
        return "KeltnerBreakoutStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 20,
            'atr_mult': 2.0,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 20)
        mult = self.params.get('atr_mult', 2.0)

        mid, upper, lower = _get_keltner(features, period, period, mult)

        signals = pd.Series(0, index=features.index)
        signals[(features['Close'] > upper) & (features['Close'].shift(1) <= upper.shift(1))] = 1
        signals[(features['Close'] < lower) & (features['Close'].shift(1) >= lower.shift(1))] = -1

        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [10, 15, 20, 25, 30],
            'atr_mult': [1.25, 1.5, 1.75, 2.0, 2.5],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class PivotBreakoutStrategy(BaseStrategy):
    """Pivot Break + Retest Confirmation Strategy.

    Two-stage logic to reduce false breakouts:
      Stage 1 — Breakout: Close breaks through a prior swing high/low
                (rolling pivot level, shifted to avoid lookahead).
      Stage 2 — Retest:   Within a configurable confirmation window after
                the breakout, price must pull back near the broken level
                (within retest_tolerance * ATR) and then close back in
                the trend direction.

    The strategy emits a single entry signal at confirmation time only.
    SL/TP is handled centrally by calculate_stops.
    """

    @property
    def name(self) -> str:
        return "PivotBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'lookback': 10,
            'confirm_window': 5,
            'retest_tolerance': 0.5,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get('lookback', 10))
        confirm_window = int(self.params.get('confirm_window', 5))
        retest_tol = float(self.params.get('retest_tolerance', 0.5))

        high = features['High'].to_numpy(dtype=float)
        low = features['Low'].to_numpy(dtype=float)
        close = features['Close'].to_numpy(dtype=float)
        n = len(close)

        # ATR for tolerance scaling
        atr_col = 'ATR_14'
        if atr_col in features.columns:
            atr = features[atr_col].to_numpy(dtype=float)
        else:
            hl = features['High'] - features['Low']
            hc = (features['High'] - features['Close'].shift(1)).abs()
            lc = (features['Low'] - features['Close'].shift(1)).abs()
            tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
            atr = tr.rolling(14).mean().to_numpy(dtype=float)

        # Compute rolling pivot levels (shifted by 1 to avoid lookahead)
        pivot_high = features['High'].rolling(lookback).max().shift(1).to_numpy(dtype=float)
        pivot_low = features['Low'].rolling(lookback).min().shift(1).to_numpy(dtype=float)

        signals = np.zeros(n, dtype=int)

        # State tracking for pending breakouts
        pending_long_bar = -1
        pending_long_level = np.nan
        pending_short_bar = -1
        pending_short_level = np.nan

        for i in range(lookback + 1, n):
            cur_atr = atr[i] if not np.isnan(atr[i]) else 0.0

            # ----------------------------------------------------------
            # Stage 1: Detect new breakouts
            # ----------------------------------------------------------
            if not np.isnan(pivot_high[i]) and close[i] > pivot_high[i]:
                # New long breakout — start or reset pending
                pending_long_bar = i
                pending_long_level = pivot_high[i]

            if not np.isnan(pivot_low[i]) and close[i] < pivot_low[i]:
                # New short breakout
                pending_short_bar = i
                pending_short_level = pivot_low[i]

            # ----------------------------------------------------------
            # Stage 2: Check for retest confirmation (long)
            # ----------------------------------------------------------
            if pending_long_bar > 0 and i > pending_long_bar:
                bars_since = i - pending_long_bar
                if bars_since <= confirm_window:
                    tolerance = retest_tol * cur_atr
                    # Pullback near the broken level
                    retested = low[i] <= pending_long_level + tolerance
                    # Confirmation: close back above the level
                    confirmed = close[i] > pending_long_level
                    if retested and confirmed:
                        signals[i] = 1
                        pending_long_bar = -1
                        pending_long_level = np.nan
                else:
                    # Window expired — cancel pending
                    pending_long_bar = -1
                    pending_long_level = np.nan

            # ----------------------------------------------------------
            # Stage 2: Check for retest confirmation (short)
            # ----------------------------------------------------------
            if pending_short_bar > 0 and i > pending_short_bar:
                bars_since = i - pending_short_bar
                if bars_since <= confirm_window:
                    tolerance = retest_tol * cur_atr
                    retested = high[i] >= pending_short_level - tolerance
                    confirmed = close[i] < pending_short_level
                    if retested and confirmed:
                        signals[i] = -1
                        pending_short_bar = -1
                        pending_short_level = np.nan
                else:
                    pending_short_bar = -1
                    pending_short_level = np.nan

        return pd.Series(signals, index=features.index)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'lookback': [5, 7, 10, 15, 20],
            'confirm_window': [3, 5, 7, 10],
            'retest_tolerance': [0.25, 0.5, 0.75, 1.0],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


# =============================================================================
# ADDITIONAL ADVANCED STRATEGIES (Indicator-Based, PM-Compatible)
# =============================================================================

class EMARibbonADXStrategy(BaseStrategy):
    """EMA Ribbon + ADX Strategy - Trend following with regime confirmation.

    Long condition:
      - EMA_fast > EMA_mid > EMA_slow
      - ADX > adx_threshold
      - Optional: +DI > -DI

    Short condition:
      - EMA_fast < EMA_mid < EMA_slow
      - ADX > adx_threshold
      - Optional: -DI > +DI

    Signals fire only on transitions into a valid state (entry-style signals).
    """

    @property
    def name(self) -> str:
        return "EMARibbonADXStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "ema_fast": 8,
            "ema_mid": 21,
            "ema_slow": 50,
            "adx_period": 14,
            "adx_threshold": 20,
            "use_di_confirmation": True,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {
            f'EMA_{int(self.params.get("ema_fast", 8))}',
            f'EMA_{int(self.params.get("ema_mid", 21))}',
            f'EMA_{int(self.params.get("ema_slow", 50))}',
        }
        adx_period = int(self.params.get("adx_period", 14))
        if adx_period == 14:
            required.update({'ADX', 'PLUS_DI', 'MINUS_DI'})
        if adx_period in {7, 10, 14, 20}:
            required.add(f'ATR_{adx_period}')
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ema_fast_p = int(self.params.get("ema_fast", 8))
        ema_mid_p = int(self.params.get("ema_mid", 21))
        ema_slow_p = int(self.params.get("ema_slow", 50))
        adx_period = int(self.params.get("adx_period", 14))
        adx_th = float(self.params.get("adx_threshold", 20))
        use_di = bool(self.params.get("use_di_confirmation", True))

        ema_fast = _get_ema(features, ema_fast_p)
        ema_mid = _get_ema(features, ema_mid_p)
        ema_slow = _get_ema(features, ema_slow_p)
        adx, plus_di, minus_di = _get_adx_di(features, adx_period)

        long_state = (ema_fast > ema_mid) & (ema_mid > ema_slow) & (adx > adx_th)
        short_state = (ema_fast < ema_mid) & (ema_mid < ema_slow) & (adx > adx_th)

        if use_di:
            long_state = long_state & (plus_di > minus_di)
            short_state = short_state & (minus_di > plus_di)

        # Convert to boolean numpy arrays (avoids pandas object downcasting warnings and is faster)
        long_np = long_state.to_numpy(dtype=bool)
        short_np = short_state.to_numpy(dtype=bool)
        prev_long = np.empty_like(long_np)
        prev_short = np.empty_like(short_np)
        if len(long_np) > 0:
            prev_long[0] = False
            prev_short[0] = False
            if len(long_np) > 1:
                prev_long[1:] = long_np[:-1]
                prev_short[1:] = short_np[:-1]
        enter_long = long_np & (~prev_long)
        enter_short = short_np & (~prev_short)
        sig = np.zeros(len(long_np), dtype=np.int8)
        sig[enter_long] = 1
        sig[enter_short] = -1
        return pd.Series(sig.astype(int), index=features.index, dtype=int)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "ema_fast": [5, 8, 10, 13, 15],
            "ema_mid": [20, 25, 30, 35],
            "ema_slow": [50, 100, 150, 200],
            "adx_threshold": [15, 20, 25, 30],
            "adx_period": [10, 14, 20],
            "use_di_confirmation": [True, False],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class RSITrendFilteredMRStrategy(BaseStrategy):
    """RSI mean reversion with a trend/regime filter.

    Long:
      - RSI crosses up through oversold
      - Price above EMA(trend)

    Short:
      - RSI crosses down through overbought
      - Price below EMA(trend)
    """

    @property
    def name(self) -> str:
        return "RSITrendFilteredMRStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "rsi_period": 14,
            "oversold": 30,
            "overbought": 70,
            "ema_trend_period": 200,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        rsi_p = int(self.params.get("rsi_period", 14))
        oversold = float(self.params.get("oversold", 30))
        overbought = float(self.params.get("overbought", 70))
        ema_p = int(self.params.get("ema_trend_period", 200))

        close = features["Close"]
        rsi = _get_rsi(features, rsi_p)
        ema = _get_ema(features, ema_p)

        trend_up = close > ema
        trend_down = close < ema

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[trend_up & (rsi > oversold) & (rsi.shift(1) <= oversold)] = 1
        signals[trend_down & (rsi < overbought) & (rsi.shift(1) >= overbought)] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "rsi_period": [5, 7, 10, 14, 21],
            "oversold": [20, 25, 30, 35],
            "overbought": [65, 70, 75, 80],
            "ema_trend_period": [50, 100, 150, 200],
            **self._base_sl_tp_grid(),
        }


class MACDHistogramMomentumStrategy(BaseStrategy):
    """MACD Histogram Momentum Strategy - Histogram zero-line crosses.

    Long when MACD histogram crosses above 0.
    Short when MACD histogram crosses below 0.

    Optional filters:
      - EMA filter: only long above EMA, short below EMA
      - ADX filter: only trade when ADX > threshold
    """

    @property
    def name(self) -> str:
        return "MACDHistogramMomentumStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            "ema_filter_period": 50,
            "adx_threshold": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(params)
        use_ema = normalized.pop("use_ema_filter", None)
        use_adx = normalized.pop("use_adx_filter", None)
        if use_ema is False:
            normalized["ema_filter_period"] = 0
        if use_adx is False:
            normalized["adx_threshold"] = 0
        return normalized

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        fast = int(self.params.get("macd_fast", 12))
        slow = int(self.params.get("macd_slow", 26))
        sig = int(self.params.get("macd_signal", 9))

        close = features["Close"]

        _, _, hist = _get_macd(features, fast, slow, sig)

        cross_up = (hist > 0) & (hist.shift(1) <= 0)
        cross_down = (hist < 0) & (hist.shift(1) >= 0)

        # Filters
        ema_p = int(self.params.get("ema_filter_period", 50))
        if ema_p > 0:
            ema = _get_ema(features, ema_p)
            cross_up = cross_up & (close > ema)
            cross_down = cross_down & (close < ema)

        adx_th = float(self.params.get("adx_threshold", 0))
        if adx_th > 0 and "ADX" in features.columns:
            cross_up = cross_up & (features["ADX"] > adx_th)
            cross_down = cross_down & (features["ADX"] > adx_th)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[cross_up] = 1
        signals[cross_down] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "macd_fast": [8, 10, 12, 16],
            "macd_slow": [21, 26, 30, 34],
            "macd_signal": [7, 9, 12],
            "ema_filter_period": [0, 20, 50, 100],
            "adx_threshold": [0, 15, 20, 25, 30],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class StochRSITrendGateStrategy(BaseStrategy):
    """StochRSI reversal entries gated by a slow EMA trend filter.

    Long:
      - %K crosses above %D AND both are below lower_band
      - Close above EMA(trend)

    Short:
      - %K crosses below %D AND both are above upper_band
      - Close below EMA(trend)
    """

    @property
    def name(self) -> str:
        return "StochRSITrendGateStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "rsi_period": 14,
            "stoch_period": 14,
            "smooth_k": 3,
            "smooth_d": 3,
            "lower_band": 20,
            "upper_band": 80,
            "ema_trend_period": 200,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    @staticmethod
    def _stoch_rsi_from_rsi(rsi: pd.Series, stoch_period: int,
                            smooth_k: int, smooth_d: int) -> Tuple[pd.Series, pd.Series]:
        rsi_min = rsi.rolling(stoch_period).min()
        rsi_max = rsi.rolling(stoch_period).max()
        rsi_denom = rsi_max - rsi_min
        stoch = pd.Series(np.nan, index=rsi.index)
        rsi_valid = rsi_denom > 1e-8
        stoch[rsi_valid] = 100 * (rsi[rsi_valid] - rsi_min[rsi_valid]) / rsi_denom[rsi_valid]

        k = stoch.rolling(smooth_k).mean()
        d = k.rolling(smooth_d).mean()
        return k, d

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        rsi_p = int(self.params.get("rsi_period", 14))
        stoch_p = int(self.params.get("stoch_period", 14))
        smooth_k = int(self.params.get("smooth_k", 3))
        smooth_d = int(self.params.get("smooth_d", 3))
        lower = float(self.params.get("lower_band", 20))
        upper = float(self.params.get("upper_band", 80))
        ema_p = int(self.params.get("ema_trend_period", 200))

        close = features["Close"]
        ema = _get_ema(features, ema_p)

        rsi = _get_rsi(features, rsi_p)
        k, d = self._stoch_rsi_from_rsi(rsi, stoch_p, smooth_k, smooth_d)

        cross_up = (k > d) & (k.shift(1) <= d.shift(1)) & (k < lower) & (d < lower)
        cross_down = (k < d) & (k.shift(1) >= d.shift(1)) & (k > upper) & (d > upper)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[(close > ema) & cross_up] = 1
        signals[(close < ema) & cross_down] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "rsi_period": [5, 7, 10, 14, 21],
            "stoch_period": [10, 14, 20],
            "smooth_k": [3, 5],
            "smooth_d": [3, 5],
            "lower_band": [10, 15, 20, 30],
            "upper_band": [70, 80, 85, 90],
            "ema_trend_period": [50, 100, 150, 200],
            **self._base_sl_tp_grid(),
        }


# Existing configs migrate VWAPDeviationReversionStrategy to the
# retained ZScoreVWAPReversionStrategy on load.


# =============================================================================
# Additional strategies
# =============================================================================


class SessionVWAPReversionStrategy(BaseStrategy):
    """Session VWAP reversion using intraday fair value and ATR bands."""

    @property
    def name(self) -> str:
        return "SessionVWAPReversionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 7,
            "session_end_hour": 20,
            "deviation_atr_mult": 1.25,
            "atr_period": 14,
            "trend_ema_period": 0,
            "adx_max": 30,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    @staticmethod
    def _session_vwap(features: pd.DataFrame) -> pd.Series:
        volume = pd.to_numeric(features.get("Volume", pd.Series(1.0, index=features.index)), errors="coerce").fillna(1.0)
        typical = (features["High"] + features["Low"] + features["Close"]) / 3.0
        if isinstance(features.index, pd.DatetimeIndex):
            session_key = pd.Series(features.index.date, index=features.index)
        else:
            session_key = pd.Series(0, index=features.index)
        pv = typical * volume
        cum_pv = pv.groupby(session_key).cumsum()
        cum_vol = volume.groupby(session_key).cumsum()
        return cum_pv / (cum_vol + 1e-10)

    def get_required_features(self) -> Set[str]:
        required = {f'ATR_{int(self.params.get("atr_period", 14))}'}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        if float(self.params.get("adx_max", 0)) > 0:
            required.add("ADX")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 7))
        end_hour = int(self.params.get("session_end_hour", 20))
        deviation_mult = float(self.params.get("deviation_atr_mult", 1.25))
        atr_period = int(self.params.get("atr_period", 14))

        close = features["Close"]
        atr = _get_atr(features, atr_period)
        vwap = self._session_vwap(features)
        lower = vwap - deviation_mult * atr
        upper = vwap + deviation_mult * atr

        if isinstance(features.index, pd.DatetimeIndex):
            hours = pd.Series(features.index.hour, index=features.index)
            if start_hour <= end_hour:
                session = (hours >= start_hour) & (hours <= end_hour)
            else:
                session = (hours >= start_hour) | (hours <= end_hour)
        else:
            session = pd.Series(True, index=features.index)

        long_entry = session & (close > lower) & (close.shift(1) <= lower.shift(1))
        short_entry = session & (close < upper) & (close.shift(1) >= upper.shift(1))

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        adx_max = float(self.params.get("adx_max", 30))
        if adx_max > 0 and "ADX" in features.columns:
            allow = features["ADX"] <= adx_max
            long_entry = long_entry & allow
            short_entry = short_entry & allow

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(atr_period, ema_period, 2))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 7, 8],
            "session_end_hour": [16, 20, 23],
            "deviation_atr_mult": [0.75, 1.0, 1.25, 1.5, 2.0],
            "atr_period": [10, 14, 20],
            "trend_ema_period": [0, 50, 100, 200],
            "adx_max": [0, 20, 25, 30],
            **self._base_sl_tp_grid(),
        }


class LiquiditySweepReversalStrategy(BaseStrategy):
    """Reversal after a stop sweep through recent structure."""

    @property
    def name(self) -> str:
        return "LiquiditySweepReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 20,
            "min_sweep_atr": 0.10,
            "atr_period": 14,
            "require_close_inside": True,
            "ema_filter_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        required = {f'ATR_{int(self.params.get("atr_period", 14))}'}
        ema_period = int(self.params.get("ema_filter_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 20))
        atr_period = int(self.params.get("atr_period", 14))
        min_sweep_atr = float(self.params.get("min_sweep_atr", 0.10))
        require_inside = bool(self.params.get("require_close_inside", True))

        high = features["High"]
        low = features["Low"]
        close = features["Close"]
        atr = _get_atr(features, atr_period)
        prior_high = high.shift(1).rolling(lookback).max()
        prior_low = low.shift(1).rolling(lookback).min()
        min_sweep = min_sweep_atr * atr

        long_entry = low < (prior_low - min_sweep)
        short_entry = high > (prior_high + min_sweep)
        if require_inside:
            long_entry = long_entry & (close > prior_low)
            short_entry = short_entry & (close < prior_high)

        ema_period = int(self.params.get("ema_filter_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(lookback + 1, atr_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [10, 20, 30, 50],
            "min_sweep_atr": [0.0, 0.1, 0.2, 0.35],
            "atr_period": [10, 14, 20],
            "require_close_inside": [True, False],
            "ema_filter_period": [0, 50, 100, 200],
            **self._base_sl_tp_grid(),
        }


class RegimeTransitionBreakoutStrategy(BaseStrategy):
    """Breakout entry on fresh transition into trend/breakout regimes."""

    @property
    def name(self) -> str:
        return "RegimeTransitionBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 20,
            "transition_window": 5,
            "source_regimes": ("CHOP", "RANGE"),
            "target_regimes": ("BREAKOUT",),
            "min_regime_gap": 0.05,
            "adx_threshold": 18,
            "ema_filter_period": 50,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(params)
        if "allowed_regimes" in normalized and "target_regimes" not in normalized:
            normalized["target_regimes"] = normalized.pop("allowed_regimes")
        for key, default in {"source_regimes": ("CHOP", "RANGE"), "target_regimes": ("BREAKOUT",)}.items():
            values = normalized.get(key, default)
            if isinstance(values, str):
                values = tuple(item.strip().upper() for item in values.split(",") if item.strip())
            normalized[key] = tuple(str(item).upper() for item in values)
        return normalized

    def get_required_features(self) -> Set[str]:
        required = {"REGIME_LIVE", "REGIME_GAP", "ADX"}
        ema_period = int(self.params.get("ema_filter_period", 50))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 20))
        transition_window = int(self.params.get("transition_window", 5))
        source_regimes = set(self.params.get("source_regimes", ("CHOP", "RANGE")))
        target_regimes = set(self.params.get("target_regimes", ("BREAKOUT",)))
        min_regime_gap = float(self.params.get("min_regime_gap", 0.05))
        adx_threshold = float(self.params.get("adx_threshold", 18))

        regime_col = "REGIME_LIVE" if "REGIME_LIVE" in features.columns else "REGIME" if "REGIME" in features.columns else None
        if regime_col is None:
            return pd.Series(0, index=features.index, dtype=int)
        regimes = features[regime_col].astype(str).str.upper()
        prior_regimes = regimes.shift(1)
        transition_now = prior_regimes.isin(source_regimes) & regimes.isin(target_regimes)
        fresh_transition = transition_now.rolling(transition_window, min_periods=1).max().astype(bool)
        if "REGIME_GAP" in features.columns and min_regime_gap > 0:
            gap_ok = pd.to_numeric(features["REGIME_GAP"], errors="coerce").abs() >= min_regime_gap
        else:
            gap_ok = pd.Series(True, index=features.index)

        high = features["High"]
        low = features["Low"]
        close = features["Close"]
        prior_high = high.shift(1).rolling(lookback).max()
        prior_low = low.shift(1).rolling(lookback).min()
        long_entry = regimes.isin(target_regimes) & fresh_transition & gap_ok & (close > prior_high)
        short_entry = regimes.isin(target_regimes) & fresh_transition & gap_ok & (close < prior_low)

        if adx_threshold > 0 and "ADX" in features.columns:
            strong = features["ADX"] >= adx_threshold
            long_entry = long_entry & strong
            short_entry = short_entry & strong

        ema_period = int(self.params.get("ema_filter_period", 50))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(lookback + 1, ema_period, transition_window))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [10, 20, 30, 50],
            "transition_window": [2, 3, 5, 8],
            "source_regimes": [("CHOP",), ("RANGE",), ("CHOP", "RANGE")],
            "target_regimes": [("BREAKOUT",), ("TREND",), ("BREAKOUT", "TREND")],
            "min_regime_gap": [0.0, 0.03, 0.05, 0.08],
            "adx_threshold": [0, 15, 18, 22, 25],
            "ema_filter_period": [0, 20, 50, 100],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class InsideBarBreakoutStrategy(BaseStrategy):
    """Inside bar breakout: mother bar engulfs child bar(s), breakout of mother range."""

    @property
    def name(self) -> str:
        return "InsideBarBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {'min_inside_bars': 1, 'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        min_ib = int(self.params.get('min_inside_bars', 1))
        h, l, c = features['High'], features['Low'], features['Close']
        signals = pd.Series(0, index=features.index, dtype=int)

        inside_streak = 0
        pending = False
        mother_h = np.nan
        mother_l = np.nan

        for i in range(1, len(features)):
            is_inside = (h.iloc[i] < h.iloc[i - 1]) and (l.iloc[i] > l.iloc[i - 1])

            if is_inside:
                inside_streak += 1
                if inside_streak >= min_ib and not pending:
                    mother_idx = i - min_ib
                    if mother_idx >= 0:
                        pending = True
                        mother_h = h.iloc[mother_idx]
                        mother_l = l.iloc[mother_idx]
                continue

            if pending:
                if c.iloc[i] > mother_h:
                    signals.iloc[i] = 1
                    pending = False
                    inside_streak = 0
                    mother_h = np.nan
                    mother_l = np.nan
                    continue
                if c.iloc[i] < mother_l:
                    signals.iloc[i] = -1
                    pending = False
                    inside_streak = 0
                    mother_h = np.nan
                    mother_l = np.nan
                    continue

            inside_streak = 0
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'min_inside_bars': [1, 2, 3], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class NarrowRangeBreakoutStrategy(BaseStrategy):
    """Narrow range breakout: bar range is narrowest in N bars, trade breakout."""

    @property
    def name(self) -> str:
        return "NarrowRangeBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {'nr_lookback': 7, 'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get('nr_lookback', 7))
        bar_range = features['High'] - features['Low']
        min_range = bar_range.rolling(lookback).min()
        is_nr = (bar_range == min_range) & (bar_range > 0)
        # Breakout on next bar
        nr_h = features['High'].shift(1)
        nr_l = features['Low'].shift(1)
        nr_flag = is_nr.shift(1, fill_value=False)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[nr_flag & (features['Close'] > nr_h)] = 1
        signals[nr_flag & (features['Close'] < nr_l)] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'nr_lookback': [4, 7, 10], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class TurtleSoupReversalStrategy(BaseStrategy):
    """Turtle soup: fade failed Donchian breakouts that close back inside the channel."""

    @property
    def name(self) -> str:
        return "TurtleSoupReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'channel_period': 20, 'reclaim_window': 2,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ch = int(self.params.get('channel_period', 20))
        rw = int(self.params.get('reclaim_window', 2))
        h, l, c = features['High'], features['Low'], features['Close']
        don_h = h.rolling(ch).max().shift(1)
        don_l = l.rolling(ch).min().shift(1)
        signals = pd.Series(0, index=features.index, dtype=int)

        pending_short_bar = -1
        pending_short_level = np.nan
        pending_long_bar = -1
        pending_long_level = np.nan

        for i in range(ch, len(features)):
            if not np.isnan(don_h.iloc[i]) and c.iloc[i] > don_h.iloc[i]:
                pending_short_bar = i
                pending_short_level = don_h.iloc[i]
            if not np.isnan(don_l.iloc[i]) and c.iloc[i] < don_l.iloc[i]:
                pending_long_bar = i
                pending_long_level = don_l.iloc[i]

            if pending_short_bar >= 0 and i > pending_short_bar:
                if i - pending_short_bar <= rw and c.iloc[i] < pending_short_level:
                    signals.iloc[i] = -1
                    pending_short_bar = -1
                    pending_short_level = np.nan
                elif i - pending_short_bar > rw:
                    pending_short_bar = -1
                    pending_short_level = np.nan

            if pending_long_bar >= 0 and i > pending_long_bar:
                if i - pending_long_bar <= rw and c.iloc[i] > pending_long_level:
                    signals.iloc[i] = 1
                    pending_long_bar = -1
                    pending_long_level = np.nan
                elif i - pending_long_bar > rw:
                    pending_long_bar = -1
                    pending_long_level = np.nan
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'channel_period': [10, 15, 20, 25],
                'reclaim_window': [1, 2, 3], **self._base_sl_tp_grid()}


class PinBarReversalStrategy(BaseStrategy):
    """Pin bar reversal: wick/body ratio detection with location filter."""

    @property
    def name(self) -> str:
        return "PinBarReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'wick_ratio': 2.5, 'proximity_atr': 1.0,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14', 'BB_LOWER_20', 'BB_UPPER_20'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        wick_r = float(self.params.get('wick_ratio', 2.5))
        prox = float(self.params.get('proximity_atr', 1.0))
        o, h, l, c = features['Open'], features['High'], features['Low'], features['Close']
        body = (c - o).abs()
        upper_wick = h - pd.concat([o, c], axis=1).max(axis=1)
        lower_wick = pd.concat([o, c], axis=1).min(axis=1) - l
        body_safe = body.clip(lower=1e-10)
        atr = _get_atr(features, 14)
        _, bb_upper, bb_lower = _get_bb(features, 20)
        # Bullish pin: long lower wick near BB lower band
        bull_pin = (lower_wick / body_safe > wick_r) & (l <= bb_lower + prox * atr)
        # Bearish pin: long upper wick near BB upper band
        bear_pin = (upper_wick / body_safe > wick_r) & (h >= bb_upper - prox * atr)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bull_pin] = 1
        signals[bear_pin] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'wick_ratio': [2.0, 2.5, 3.0],
                'proximity_atr': [0.5, 1.0, 1.5], **self._base_sl_tp_grid()}


class EngulfingPatternStrategy(BaseStrategy):
    """Engulfing pattern with location filter (BB/Donchian proximity)."""

    @property
    def name(self) -> str:
        return "EngulfingPatternStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'lookback_level': 20, 'use_adx_filter': True,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14', 'BB_LOWER_20', 'BB_UPPER_20', 'ADX'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get('lookback_level', 20))
        o, c = features['Open'], features['Close']
        o1, c1 = o.shift(1), c.shift(1)
        body = (c - o)
        body1 = (c1 - o1)
        # Bullish engulfing: prior bar bearish, current bar bullish, body engulfs
        bull_eng = (body1 < 0) & (body > 0) & (o <= c1) & (c >= o1)
        # Bearish engulfing
        bear_eng = (body1 > 0) & (body < 0) & (o >= c1) & (c <= o1)
        _, bb_upper, bb_lower = _get_bb(features, 20)
        atr = _get_atr(features, 14)
        recent_low = features['Low'].rolling(lookback, min_periods=lookback).min().shift(1)
        recent_high = features['High'].rolling(lookback, min_periods=lookback).max().shift(1)
        near_lower = (features['Low'] <= bb_lower + atr) & (features['Low'] <= recent_low + atr)
        near_upper = (features['High'] >= bb_upper - atr) & (features['High'] >= recent_high - atr)
        # ADX filter (prefer range)
        if bool(self.params.get('use_adx_filter', True)) and 'ADX' in features.columns:
            allow = features['ADX'] < 30
        else:
            allow = pd.Series(True, index=features.index)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bull_eng & near_lower & allow] = 1
        signals[bear_eng & near_upper & allow] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'lookback_level': [10, 20, 30],
                'use_adx_filter': [True, False], **self._base_sl_tp_grid()}


class VolumeSpikeMomentumStrategy(BaseStrategy):
    """Volume spike + directional close: breakout on unusual volume."""

    @property
    def name(self) -> str:
        return "VolumeSpikeMomentumStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {'vol_mult': 2.0, 'vol_lookback': 20,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        vm = float(self.params.get('vol_mult', 2.0))
        vl = int(self.params.get('vol_lookback', 20))
        vol = pd.to_numeric(features.get('Volume', pd.Series(0, index=features.index)),
                            errors='coerce').fillna(0)
        avg_vol = vol.rolling(vl, min_periods=vl).mean()
        spike = vol > vm * avg_vol
        spike_start = spike & ~spike.shift(1, fill_value=False)
        bar_range = features['High'] - features['Low']
        close_pos = (features['Close'] - features['Low']) / (bar_range + 1e-10)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[spike_start & (close_pos > 0.7)] = 1   # Close in upper 30%
        signals[spike_start & (close_pos < 0.3)] = -1  # Close in lower 30%
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'vol_mult': [1.5, 2.0, 2.5, 3.0],
                'vol_lookback': [10, 20, 30], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class RSIDivergenceStrategy(BaseStrategy):
    """RSI divergence: swing point divergence between price and RSI."""

    @property
    def name(self) -> str:
        return "RSIDivergenceStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'rsi_period': 14, 'swing_order': 5,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        rsi_p = int(self.params.get('rsi_period', 14))
        order = int(self.params.get('swing_order', 5))
        rsi = _get_rsi(features, rsi_p)
        close = features['Close']
        _, price_lows = _detect_swing_points(close, order)
        price_highs, _ = _detect_swing_points(close, order)
        signals = pd.Series(0, index=features.index, dtype=int)
        # Bullish divergence: price lower low, RSI higher low
        for i in range(2 * order + 1, len(features)):
            if price_lows.iloc[i]:
                # Find previous swing low
                prev_lows = price_lows.iloc[max(0, i - 5 * order):i]
                prev_idx = prev_lows[prev_lows].index
                if len(prev_idx) > 0:
                    j = features.index.get_loc(prev_idx[-1])
                    if close.iloc[i] < close.iloc[j] and rsi.iloc[i] > rsi.iloc[j]:
                        signals.iloc[i] = 1
            if price_highs.iloc[i]:
                prev_highs = price_highs.iloc[max(0, i - 5 * order):i]
                prev_idx = prev_highs[prev_highs].index
                if len(prev_idx) > 0:
                    j = features.index.get_loc(prev_idx[-1])
                    if close.iloc[i] > close.iloc[j] and rsi.iloc[i] < rsi.iloc[j]:
                        signals.iloc[i] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'rsi_period': [7, 14, 21], 'swing_order': [3, 5, 7],
                **self._base_sl_tp_grid()}


class MACDDivergenceStrategy(BaseStrategy):
    """MACD histogram divergence with price."""

    @property
    def name(self) -> str:
        return "MACDDivergenceStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'swing_order': 5, 'macd_fast': 12, 'macd_slow': 26,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14', 'MACD_HIST'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        order = int(self.params.get('swing_order', 5))
        fast = int(self.params.get('macd_fast', 12))
        slow = int(self.params.get('macd_slow', 26))
        _, _, hist = _get_macd(features, fast, slow)
        close = features['Close']
        _, price_lows = _detect_swing_points(close, order)
        price_highs, _ = _detect_swing_points(close, order)
        signals = pd.Series(0, index=features.index, dtype=int)
        for i in range(2 * order + 1, len(features)):
            if price_lows.iloc[i]:
                prev_lows = price_lows.iloc[max(0, i - 5 * order):i]
                prev_idx = prev_lows[prev_lows].index
                if len(prev_idx) > 0:
                    j = features.index.get_loc(prev_idx[-1])
                    if close.iloc[i] < close.iloc[j] and hist.iloc[i] > hist.iloc[j]:
                        signals.iloc[i] = 1
            if price_highs.iloc[i]:
                prev_highs = price_highs.iloc[max(0, i - 5 * order):i]
                prev_idx = prev_highs[prev_highs].index
                if len(prev_idx) > 0:
                    j = features.index.get_loc(prev_idx[-1])
                    if close.iloc[i] > close.iloc[j] and hist.iloc[i] < hist.iloc[j]:
                        signals.iloc[i] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'swing_order': [3, 5, 7], 'macd_fast': [8, 12],
                'macd_slow': [21, 26], **self._base_sl_tp_grid()}


class OBVDivergenceStrategy(BaseStrategy):
    """On-Balance Volume divergence with price."""

    @property
    def name(self) -> str:
        return "OBVDivergenceStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {'swing_order': 5, 'obv_smooth': 5,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        order = int(self.params.get('swing_order', 5))
        smooth = int(self.params.get('obv_smooth', 5))
        vol = pd.to_numeric(features.get('Volume', pd.Series(0, index=features.index)),
                            errors='coerce').fillna(0)
        direction = features['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        obv = (vol * direction).cumsum()
        if smooth > 0:
            obv = obv.rolling(smooth, min_periods=1).mean()
        close = features['Close']
        _, price_lows = _detect_swing_points(close, order)
        price_highs, _ = _detect_swing_points(close, order)
        signals = pd.Series(0, index=features.index, dtype=int)
        for i in range(2 * order + 1, len(features)):
            if price_lows.iloc[i]:
                prev_lows = price_lows.iloc[max(0, i - 5 * order):i]
                prev_idx = prev_lows[prev_lows].index
                if len(prev_idx) > 0:
                    j = features.index.get_loc(prev_idx[-1])
                    if close.iloc[i] < close.iloc[j] and obv.iloc[i] > obv.iloc[j]:
                        signals.iloc[i] = 1
            if price_highs.iloc[i]:
                prev_highs = price_highs.iloc[max(0, i - 5 * order):i]
                prev_idx = prev_highs[prev_highs].index
                if len(prev_idx) > 0:
                    j = features.index.get_loc(prev_idx[-1])
                    if close.iloc[i] > close.iloc[j] and obv.iloc[i] < obv.iloc[j]:
                        signals.iloc[i] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'swing_order': [3, 5, 7], 'obv_smooth': [0, 5, 10],
                **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class KeltnerFadeStrategy(BaseStrategy):
    """Fade outer Keltner Channel band back to midline."""

    @property
    def name(self) -> str:
        return "KeltnerFadeStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'kc_ema': 20, 'kc_mult': 2.0, 'adx_threshold': 25,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        ema_period = int(self.params.get('kc_ema', 20))
        return {'ATR_14', f'EMA_{ema_period}', 'ADX'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ema_p = int(self.params.get('kc_ema', 20))
        mult = float(self.params.get('kc_mult', 2.0))
        adx_t = float(self.params.get('adx_threshold', 25))
        mid, upper, lower = _get_keltner(features, ema_p, 14, mult)
        c = features['Close']
        # Touch upper KC then next bar closes inside → short
        touched_upper = (features['High'].shift(1) >= upper.shift(1)) & (c < upper)
        touched_lower = (features['Low'].shift(1) <= lower.shift(1)) & (c > lower)
        allow = pd.Series(True, index=features.index)
        if 'ADX' in features.columns:
            allow = features['ADX'] < adx_t
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[touched_lower & allow] = 1
        signals[touched_upper & allow] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'kc_ema': [15, 20, 25], 'kc_mult': [1.5, 2.0, 2.5],
                'adx_threshold': [20, 25, 30], **self._base_sl_tp_grid()}


class ROCExhaustionReversalStrategy(BaseStrategy):
    """Rate of Change exhaustion: ROC reaches extreme percentile then reverses."""

    @property
    def name(self) -> str:
        return "ROCExhaustionReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {'roc_period': 10, 'pct_lookback': 100, 'extreme_pct': 10,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        roc_p = int(self.params.get('roc_period', 10))
        pct_lb = int(self.params.get('pct_lookback', 100))
        ext_pct = float(self.params.get('extreme_pct', 10))
        roc = features['Close'].pct_change(roc_p) * 100
        pct_rank = _rolling_percentile_rank(roc, pct_lb)
        # Extreme high ROC crossing back below threshold
        was_extreme_high = (pct_rank.shift(1) >= (100 - ext_pct))
        now_normal_high = (pct_rank < (100 - ext_pct))
        was_extreme_low = (pct_rank.shift(1) <= ext_pct)
        now_normal_low = (pct_rank > ext_pct)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[was_extreme_low & now_normal_low] = 1    # Oversold bounce
        signals[was_extreme_high & now_normal_high] = -1  # Overbought fade
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'roc_period': [5, 10, 14, 20], 'pct_lookback': [50, 100, 200],
                'extreme_pct': [5, 10, 15], **self._base_sl_tp_grid()}


class EMAPullbackContinuationStrategy(BaseStrategy):
    """EMA crossover confirms trend, enter on pullback to fast EMA."""

    @property
    def name(self) -> str:
        return "EMAPullbackContinuationStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {'fast_period': 10, 'slow_period': 30, 'touch_atr': 0.5,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {
            'ATR_14',
            f'EMA_{int(self.params.get("fast_period", 10))}',
            f'EMA_{int(self.params.get("slow_period", 30))}',
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        fp = int(self.params.get('fast_period', 10))
        sp = int(self.params.get('slow_period', 30))
        touch = float(self.params.get('touch_atr', 0.5))
        fast_ema = _get_ema(features, fp)
        slow_ema = _get_ema(features, sp)
        atr = _get_atr(features, 14)
        uptrend = fast_ema > slow_ema
        downtrend = fast_ema < slow_ema
        # Pullback to fast EMA (within touch_atr * ATR)
        near_fast = (features['Low'] <= fast_ema + touch * atr) & (features['Close'] > fast_ema)
        near_fast_short = (features['High'] >= fast_ema - touch * atr) & (features['Close'] < fast_ema)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[uptrend & near_fast] = 1
        signals[downtrend & near_fast_short] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'fast_period': [8, 10, 13], 'slow_period': [20, 30, 50],
                'touch_atr': [0.3, 0.5, 0.7], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class ParabolicSARTrendStrategy(BaseStrategy):
    """Parabolic SAR trend-following with optional ADX filter."""

    @property
    def name(self) -> str:
        return "ParabolicSARTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {'af_start': 0.02, 'af_max': 0.20, 'adx_threshold': 20,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14', 'ADX'}

    @staticmethod
    def _compute_psar(high: np.ndarray, low: np.ndarray,
                      af_start: float, af_max: float) -> np.ndarray:
        """Compute Parabolic SAR."""
        n = len(high)
        psar = np.zeros(n)
        bull = True
        af = af_start
        ep = low[0]
        psar[0] = high[0]
        for i in range(1, n):
            if bull:
                psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
                psar[i] = min(psar[i], low[i - 1], low[max(0, i - 2)])
                if low[i] < psar[i]:
                    bull = False
                    psar[i] = ep
                    ep = low[i]
                    af = af_start
                else:
                    if high[i] > ep:
                        ep = high[i]
                        af = min(af + af_start, af_max)
            else:
                psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
                psar[i] = max(psar[i], high[i - 1], high[max(0, i - 2)])
                if high[i] > psar[i]:
                    bull = True
                    psar[i] = ep
                    ep = high[i]
                    af = af_start
                else:
                    if low[i] < ep:
                        ep = low[i]
                        af = min(af + af_start, af_max)
        return psar

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        af_s = float(self.params.get('af_start', 0.02))
        af_m = float(self.params.get('af_max', 0.20))
        adx_t = float(self.params.get('adx_threshold', 20))
        psar = self._compute_psar(features['High'].values, features['Low'].values,
                                  af_s, af_m)
        psar_s = pd.Series(psar, index=features.index)
        c = features['Close']
        # PSAR flip signals
        above = c > psar_s
        flip_long = above & ~above.shift(1, fill_value=False)
        flip_short = ~above & above.shift(1, fill_value=False)
        allow = pd.Series(True, index=features.index)
        if 'ADX' in features.columns:
            allow = features['ADX'] > adx_t
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[flip_long & allow] = 1
        signals[flip_short & allow] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'af_start': [0.01, 0.02, 0.03], 'af_max': [0.15, 0.20, 0.25],
                'adx_threshold': [15, 20, 25], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class ATRPercentileBreakoutStrategy(BaseStrategy):
    """ATR compression→expansion breakout: low ATR percentile followed by rise."""

    @property
    def name(self) -> str:
        return "ATRPercentileBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {'pct_lookback': 100, 'compress_pct': 20, 'expand_pct': 70,
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        pct_lb = int(self.params.get('pct_lookback', 100))
        comp = float(self.params.get('compress_pct', 20))
        exp = float(self.params.get('expand_pct', 70))
        atr = _get_atr(features, 14)
        atr_pct = _rolling_percentile_rank(atr, pct_lb)
        # Was compressed, now expanding
        was_low = atr_pct.shift(1) <= comp
        now_high = atr_pct >= exp
        trigger = was_low & now_high
        bar_range = features['High'] - features['Low']
        close_pos = (features['Close'] - features['Low']) / (bar_range + 1e-10)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[trigger & (close_pos > 0.6)] = 1
        signals[trigger & (close_pos < 0.4)] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'pct_lookback': [50, 100, 200], 'compress_pct': [10, 20, 30],
                'expand_pct': [60, 70, 80], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class KaufmanAMATrendStrategy(BaseStrategy):
    """Kaufman Adaptive Moving Average: efficiency ratio adjusts smoothing speed."""

    @property
    def name(self) -> str:
        return "KaufmanAMATrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {'er_period': 10, 'fast_period': 2, 'slow_period': 30,
                'signal_mode': 'direction', 'sl_atr_mult': 2.0, 'tp_atr_mult': 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {'ATR_14'}

    @staticmethod
    def _kaufman_ama(close: np.ndarray, er_period: int,
                     fast_period: int, slow_period: int) -> np.ndarray:
        """Compute Kaufman Adaptive Moving Average."""
        n = len(close)
        ama = np.full(n, np.nan)
        fast_sc = 2.0 / (fast_period + 1)
        slow_sc = 2.0 / (slow_period + 1)
        if er_period >= n:
            return ama
        ama[er_period] = close[er_period]
        abs_changes = np.abs(np.diff(close))
        cumsum_changes = np.concatenate(([0.0], np.cumsum(abs_changes)))
        for i in range(er_period + 1, n):
            direction = abs(close[i] - close[i - er_period])
            volatility = cumsum_changes[i] - cumsum_changes[i - er_period]
            er = direction / volatility if volatility > 0 else 0.0
            sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
            ama[i] = ama[i - 1] + sc * (close[i] - ama[i - 1])
        return ama

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        er_p = int(self.params.get('er_period', 10))
        fast_p = int(self.params.get('fast_period', 2))
        slow_p = int(self.params.get('slow_period', 30))
        mode = str(self.params.get('signal_mode', 'direction'))
        close = features['Close'].values
        ama = self._kaufman_ama(close, er_p, fast_p, slow_p)
        ama_s = pd.Series(ama, index=features.index)
        signals = pd.Series(0, index=features.index, dtype=int)
        if mode == 'crossover':
            # Price crosses above/below AMA
            above = features['Close'] > ama_s
            prev_above = above.shift(1, fill_value=False)
            signals[above & ~prev_above] = 1
            signals[~above & prev_above] = -1
        else:
            # AMA direction change
            ama_diff = ama_s.diff()
            signals[(ama_diff > 0) & (ama_diff.shift(1) <= 0)] = 1
            signals[(ama_diff < 0) & (ama_diff.shift(1) >= 0)] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {'er_period': [5, 10, 20], 'fast_period': [2, 3],
                'slow_period': [20, 30, 50],
                'signal_mode': ['direction', 'crossover'],
                **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


# =============================================================================
# PACKAGE G7 STRATEGIES
# =============================================================================

class VortexTrendStrategy(BaseStrategy):
    """Vortex Indicator trend strategy — VI+/VI- crossovers.

    Vortex Movement (VM+) = |High[t] - Low[t-1]|
    Vortex Movement (VM-) = |Low[t] - High[t-1]|
    VI+ = SUM(VM+, period) / SUM(TR, period)
    VI- = SUM(VM-, period) / SUM(TR, period)

    Buy when VI+ crosses above VI-, sell when VI- crosses above VI+.
    """

    @property
    def name(self) -> str:
        return "VortexTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 14,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return set()

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('period', 14))
        high = features['High']
        low = features['Low']

        # Vortex movement
        vm_plus = (high - low.shift(1)).abs()
        vm_minus = (low - high.shift(1)).abs()

        tr = _get_tr(features)

        # Sum over period
        sum_vm_plus = vm_plus.rolling(period).sum()
        sum_vm_minus = vm_minus.rolling(period).sum()
        sum_tr = tr.rolling(period).sum()

        vi_plus = sum_vm_plus / (sum_tr + 1e-10)
        vi_minus = sum_vm_minus / (sum_tr + 1e-10)

        # Crossover events
        cross_up = (vi_plus > vi_minus) & (vi_plus.shift(1) <= vi_minus.shift(1))
        cross_down = (vi_minus > vi_plus) & (vi_minus.shift(1) <= vi_plus.shift(1))

        signals = pd.Series(0, index=features.index)
        signals[cross_up] = 1
        signals[cross_down] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [7, 10, 14, 20, 25],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class TRIXSignalStrategy(BaseStrategy):
    """TRIX strategy — Triple EMA rate of change with signal line.

    TRIX = 100 * (EMA3 - EMA3[1]) / EMA3[1]  where EMA3 = EMA(EMA(EMA(Close)))
    Signal = EMA(TRIX, signal_period)

    Buy when TRIX crosses above signal line, sell when below.
    """

    @property
    def name(self) -> str:
        return "TRIXSignalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'trix_period': 15,
            'signal_period': 9,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return set()

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        trix_p = int(self.params.get('trix_period', 15))
        sig_p = int(self.params.get('signal_period', 9))
        close = features['Close']

        # Triple EMA
        ema1 = close.ewm(span=trix_p, adjust=False).mean()
        ema2 = ema1.ewm(span=trix_p, adjust=False).mean()
        ema3 = ema2.ewm(span=trix_p, adjust=False).mean()

        # TRIX = percentage rate of change of ema3
        trix = 100.0 * ema3.pct_change()

        # Signal line
        sig_line = trix.ewm(span=sig_p, adjust=False).mean()

        # Crossover events
        cross_up = (trix > sig_line) & (trix.shift(1) <= sig_line.shift(1))
        cross_down = (trix < sig_line) & (trix.shift(1) >= sig_line.shift(1))

        signals = pd.Series(0, index=features.index)
        signals[cross_up] = 1
        signals[cross_down] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'trix_period': [9, 12, 15, 20],
            'signal_period': [5, 7, 9],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class RelativeVigorIndexStrategy(BaseStrategy):
    """Relative Vigor Index (RVI) strategy — compares close-open to high-low range.

    RVI = SMA((Close-Open), period) / SMA((High-Low), period)
    Signal = SMA(RVI, 4)

    Buy when RVI crosses above signal, sell when below.
    """

    @property
    def name(self) -> str:
        return "RelativeVigorIndexStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 10,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return set()

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('period', 10))
        close_open = features['Close'] - features['Open']
        high_low = features['High'] - features['Low']

        num = close_open.rolling(period).mean()
        den = high_low.rolling(period).mean()

        rvi = num / (den + 1e-10)
        signal_line = rvi.rolling(4).mean()

        cross_up = (rvi > signal_line) & (rvi.shift(1) <= signal_line.shift(1))
        cross_down = (rvi < signal_line) & (rvi.shift(1) >= signal_line.shift(1))

        signals = pd.Series(0, index=features.index)
        signals[cross_up] = 1
        signals[cross_down] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [5, 7, 10, 14, 20],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class VIDYABandTrendStrategy(BaseStrategy):
    """VIDYA Band Trend strategy — Variable Index Dynamic Average with ATR bands.

    VIDYA = VIDYA[1] + sc * cmo_ratio * (Close - VIDYA[1])
    where cmo_ratio = abs(CMO) and sc = 2 / (period + 1).

    Buy when price breaks above VIDYA + band_mult * ATR,
    sell when price breaks below VIDYA - band_mult * ATR.
    """

    @property
    def name(self) -> str:
        return "VIDYABandTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'vidya_period': 14,
            'cmo_period': 9,
            'band_mult': 1.5,
            'atr_period': 14,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        atr_p = self.params.get('atr_period', 14)
        return {f'ATR_{atr_p}'} if atr_p in {7, 10, 14, 20} else set()

    @staticmethod
    def _vidya(close: np.ndarray, vidya_period: int, cmo_period: int) -> np.ndarray:
        """Compute VIDYA using Chande Momentum Oscillator as volatility ratio."""
        n = len(close)
        vidya = np.full(n, np.nan)
        sc = 2.0 / (vidya_period + 1)

        # CMO: (sum_up - sum_down) / (sum_up + sum_down)
        diff = np.diff(close, prepend=np.nan)
        up = np.where(diff > 0, diff, 0.0)
        down = np.where(diff < 0, -diff, 0.0)

        start = cmo_period + 1
        if start >= n:
            return vidya

        # Initial VIDYA seed = close at start
        vidya[start] = close[start]
        for i in range(start + 1, n):
            sum_up = up[i - cmo_period + 1:i + 1].sum()
            sum_down = down[i - cmo_period + 1:i + 1].sum()
            denom = sum_up + sum_down
            cmo_ratio = abs(sum_up - sum_down) / denom if denom > 1e-10 else 0.0
            vidya[i] = vidya[i - 1] + sc * cmo_ratio * (close[i] - vidya[i - 1])
        return vidya

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        vidya_p = int(self.params.get('vidya_period', 14))
        cmo_p = int(self.params.get('cmo_period', 9))
        band_mult = float(self.params.get('band_mult', 1.5))
        atr_p = int(self.params.get('atr_period', 14))

        close_arr = features['Close'].values.astype(float)
        vidya_arr = self._vidya(close_arr, vidya_p, cmo_p)
        vidya_s = pd.Series(vidya_arr, index=features.index)

        atr = _get_atr(features, atr_p)
        upper = vidya_s + band_mult * atr
        lower = vidya_s - band_mult * atr

        close = features['Close']

        # Break above upper band (event, not level)
        break_up = (close > upper) & (close.shift(1) <= upper.shift(1))
        break_down = (close < lower) & (close.shift(1) >= lower.shift(1))

        signals = pd.Series(0, index=features.index)
        signals[break_up] = 1
        signals[break_down] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'vidya_period': [10, 14, 20],
            'cmo_period': [5, 9, 14],
            'band_mult': [1.0, 1.5, 2.0],
            'atr_period': [10, 14],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class ChoppinessCompressionBreakoutStrategy(BaseStrategy):
    """Choppiness Index compression breakout strategy.

    CI = 100 * LOG10(SUM(ATR, period) / (Highest - Lowest)) / LOG10(period)

    High CI (>61.8) = choppy/ranging market.  Low CI (<38.2) = trending.
    Strategy: wait for high CI (compression), then trade breakout when CI drops
    below the threshold, using price direction for signal.
    """

    @property
    def name(self) -> str:
        return "ChoppinessCompressionBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            'ci_period': 14,
            'chop_threshold': 61.8,
            'trend_threshold': 38.2,
            'lookback': 5,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return set()

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ci_p = int(self.params.get('ci_period', 14))
        chop_thresh = float(self.params.get('chop_threshold', 61.8))
        trend_thresh = float(self.params.get('trend_threshold', 38.2))
        lookback = int(self.params.get('lookback', 5))

        tr = _get_tr(features)
        atr_sum = tr.rolling(ci_p).sum()
        highest = features['High'].rolling(ci_p).max()
        lowest = features['Low'].rolling(ci_p).min()
        hl_range = highest - lowest

        # Choppiness Index. Fully flat windows can make both ATR sum and range
        # collapse to zero, which makes the formula undefined; keep those as NaN
        # rather than emitting log10(0) warnings during optimization.
        ci = pd.Series(np.nan, index=features.index, dtype=float)
        log_period = np.log10(ci_p) if ci_p > 1 else np.nan
        if np.isfinite(log_period) and log_period > 0:
            valid = (atr_sum > 0) & (hl_range > 0)
            if valid.any():
                ratio = (atr_sum[valid] / hl_range[valid]).clip(lower=1.0)
                ci.loc[valid] = 100.0 * np.log10(ratio) / log_period

        # Was recently choppy (compression phase)
        was_choppy = ci.rolling(lookback).max() > chop_thresh

        # CI now dropping below trend threshold = breakout starting
        ci_drop = (ci < trend_thresh) & (ci.shift(1) >= trend_thresh)

        # Direction: use recent close momentum
        close = features['Close']
        direction = close - close.shift(lookback)

        signals = pd.Series(0, index=features.index)
        breakout = ci_drop & was_choppy
        signals[breakout & (direction > 0)] = 1
        signals[breakout & (direction < 0)] = -1
        return signals

    def get_param_grid(self) -> Dict[str, List]:
        return {
            'ci_period': [7, 10, 14, 20],
            'chop_threshold': [55.0, 61.8],
            'trend_threshold': [35.0, 38.2, 42.0],
            'lookback': [3, 5, 7],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class OpeningRangeBreakoutStrategy(BaseStrategy):
    """Breakout from the first bars of a trading session."""

    @property
    def name(self) -> str:
        return "OpeningRangeBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 8,
            "range_bars": 6,
            "breakout_buffer_atr": 0.05,
            "volume_mult": 0.0,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 8))
        range_bars = max(1, int(self.params.get("range_bars", 6)))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        volume_mult = float(self.params.get("volume_mult", 0.0))

        keys = _session_keys(features, start_hour)
        bar_no = _bar_number_in_session(features, start_hour)
        in_range = bar_no < range_bars
        range_high = features["High"].where(in_range).groupby(keys).cummax().groupby(keys).ffill()
        range_low = features["Low"].where(in_range).groupby(keys).cummin().groupby(keys).ffill()
        atr = _get_atr(features, 14)
        upper = range_high + buffer_atr * atr
        lower = range_low - buffer_atr * atr
        close = features["Close"]

        ready = bar_no >= range_bars
        long_entry = ready & (close > upper) & (close.shift(1) <= upper.shift(1))
        short_entry = ready & (close < lower) & (close.shift(1) >= lower.shift(1))

        if volume_mult > 0:
            vol = _get_volume(features)
            avg_vol = vol.shift(1).rolling(20, min_periods=20).mean()
            vol_ok = vol >= volume_mult * avg_vol
            long_entry = long_entry & vol_ok
            short_entry = short_entry & vol_ok

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(range_bars + 2, 20, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 8, 13],
            "range_bars": [3, 6, 12],
            "breakout_buffer_atr": [0.0, 0.05, 0.10],
            "volume_mult": [0.0, 1.25],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class GapContinuationStrategy(BaseStrategy):
    """Continuation after a session gap that holds beyond the opening bar."""

    @property
    def name(self) -> str:
        return "GapContinuationStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 8,
            "gap_atr_mult": 0.50,
            "confirm_bars": 2,
            "breakout_buffer_atr": 0.05,
            "volume_mult": 0.0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 8))
        gap_mult = float(self.params.get("gap_atr_mult", 0.50))
        confirm_bars = max(1, int(self.params.get("confirm_bars", 2)))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        volume_mult = float(self.params.get("volume_mult", 0.0))

        keys = _session_keys(features, start_hour)
        bar_no = _bar_number_in_session(features, start_hour)
        close = features["Close"]
        atr = _get_atr(features, 14)

        session_last_close = close.groupby(keys).last()
        prev_close_by_session = session_last_close.shift(1)
        prev_close = keys.map(prev_close_by_session)
        first_open = features["Open"].groupby(keys).transform("first")
        first_high = features["High"].where(bar_no == 0).groupby(keys).ffill()
        first_low = features["Low"].where(bar_no == 0).groupby(keys).ffill()
        gap = first_open - prev_close
        gap_up = gap >= gap_mult * atr
        gap_down = gap <= -gap_mult * atr

        active = (bar_no >= 1) & (bar_no <= confirm_bars)
        long_entry = active & gap_up & (close > first_high + buffer_atr * atr)
        short_entry = active & gap_down & (close < first_low - buffer_atr * atr)

        if volume_mult > 0:
            vol = _get_volume(features)
            avg_vol = vol.shift(1).rolling(20, min_periods=20).mean()
            vol_ok = vol >= volume_mult * avg_vol
            long_entry = long_entry & vol_ok
            short_entry = short_entry & vol_ok

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 8, 13],
            "gap_atr_mult": [0.25, 0.50, 0.75],
            "confirm_bars": [1, 2, 3],
            "breakout_buffer_atr": [0.0, 0.05],
            "volume_mult": [0.0, 1.20],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class VWAPTrendContinuationStrategy(BaseStrategy):
    """Session VWAP pullback continuation in the direction of value migration."""

    @property
    def name(self) -> str:
        return "VWAPTrendContinuationStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 8,
            "pullback_atr": 0.50,
            "slope_bars": 5,
            "trend_ema_period": 100,
            "volume_mult": 0.0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 8))
        pullback_atr = float(self.params.get("pullback_atr", 0.50))
        slope_bars = max(1, int(self.params.get("slope_bars", 5)))
        volume_mult = float(self.params.get("volume_mult", 0.0))

        close = features["Close"]
        atr = _get_atr(features, 14)
        vwap = _get_session_vwap(features, start_hour)
        touched_long = (features["Low"] <= vwap + pullback_atr * atr).rolling(slope_bars, min_periods=1).max().astype(bool)
        touched_short = (features["High"] >= vwap - pullback_atr * atr).rolling(slope_bars, min_periods=1).max().astype(bool)
        vwap_up = vwap > vwap.shift(slope_bars)
        vwap_down = vwap < vwap.shift(slope_bars)

        long_entry = (close > vwap) & vwap_up & touched_long & (close > features["High"].shift(1))
        short_entry = (close < vwap) & vwap_down & touched_short & (close < features["Low"].shift(1))

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        if volume_mult > 0:
            vol = _get_volume(features)
            avg_vol = vol.shift(1).rolling(20, min_periods=20).mean()
            vol_ok = vol >= volume_mult * avg_vol
            long_entry = long_entry & vol_ok
            short_entry = short_entry & vol_ok

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(slope_bars + 2, 20, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 8, 13],
            "pullback_atr": [0.25, 0.50, 0.75],
            "slope_bars": [3, 5, 8],
            "trend_ema_period": [0, 100, 200],
            "volume_mult": [0.0, 1.10],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class RSI2TrendPullbackStrategy(BaseStrategy):
    """Short-horizon RSI pullback inside a longer trend filter."""

    @property
    def name(self) -> str:
        return "RSI2TrendPullbackStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "rsi_period": 2,
            "trend_ema_period": 200,
            "oversold": 10,
            "overbought": 90,
            "require_reclaim": True,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        return {
            f'RSI_{int(self.params.get("rsi_period", 2))}',
            f'EMA_{int(self.params.get("trend_ema_period", 200))}',
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        rsi_period = int(self.params.get("rsi_period", 2))
        ema_period = int(self.params.get("trend_ema_period", 200))
        oversold = float(self.params.get("oversold", 10))
        overbought = float(self.params.get("overbought", 90))
        require_reclaim = bool(self.params.get("require_reclaim", True))

        close = features["Close"]
        rsi = _get_rsi(features, rsi_period)
        ema = _get_ema(features, ema_period)
        trend_up = close > ema
        trend_down = close < ema
        if require_reclaim:
            long_entry = trend_up & (rsi.shift(1) <= oversold) & (rsi > rsi.shift(1))
            short_entry = trend_down & (rsi.shift(1) >= overbought) & (rsi < rsi.shift(1))
        else:
            long_entry = trend_up & (rsi <= oversold) & (close > close.shift(1))
            short_entry = trend_down & (rsi >= overbought) & (close < close.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(rsi_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "rsi_period": [2, 3, 4],
            "trend_ema_period": [100, 150, 200],
            "oversold": [5, 10, 15, 20],
            "overbought": [80, 85, 90, 95],
            "require_reclaim": [True, False],
            **self._base_sl_tp_grid(),
        }


class DoubleEMATrendStrategy(BaseStrategy):
    """Double-EMA crossover with optional slope and ADX confirmation."""

    @property
    def name(self) -> str:
        return "DoubleEMATrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "fast_period": 13,
            "slow_period": 55,
            "slope_bars": 3,
            "adx_threshold": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        if float(self.params.get("adx_threshold", 0)) > 0:
            required.add("ADX")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        fast = int(self.params.get("fast_period", 13))
        slow = int(self.params.get("slow_period", 55))
        slope_bars = int(self.params.get("slope_bars", 3))
        if fast >= slow:
            return pd.Series(0, index=features.index, dtype=int)

        close = features["Close"]
        dema_fast = _double_ema(close, fast)
        dema_slow = _double_ema(close, slow)
        long_entry = (dema_fast > dema_slow) & (dema_fast.shift(1) <= dema_slow.shift(1))
        short_entry = (dema_fast < dema_slow) & (dema_fast.shift(1) >= dema_slow.shift(1))

        if slope_bars > 0:
            long_entry = long_entry & (dema_slow > dema_slow.shift(slope_bars))
            short_entry = short_entry & (dema_slow < dema_slow.shift(slope_bars))

        adx_threshold = float(self.params.get("adx_threshold", 0))
        if adx_threshold > 0:
            adx, _, _ = _get_adx_di(features, 14)
            allow = adx >= adx_threshold
            long_entry = long_entry & allow
            short_entry = short_entry & allow

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(slow + slope_bars + 2, 20))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "fast_period": [8, 13, 21],
            "slow_period": [34, 55, 89, 144],
            "slope_bars": [0, 3, 5],
            "adx_threshold": [0, 18, 22],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class HeikinAshiTrendStrategy(BaseStrategy):
    """Heikin-Ashi body persistence with optional EMA trend filter."""

    @property
    def name(self) -> str:
        return "HeikinAshiTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "confirm_bars": 3,
            "trend_ema_period": 100,
            "min_body_atr": 0.10,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        confirm = max(1, int(self.params.get("confirm_bars", 3)))
        ema_period = int(self.params.get("trend_ema_period", 100))
        min_body_atr = float(self.params.get("min_body_atr", 0.10))
        atr = _get_atr(features, 14)
        ha_open, ha_close = _heikin_ashi(features)
        body = ha_close - ha_open
        strong_body = body.abs() >= min_body_atr * atr
        bull = (body > 0) & strong_body
        bear = (body < 0) & strong_body
        bull_run = bull.rolling(confirm, min_periods=confirm).sum() >= confirm
        bear_run = bear.rolling(confirm, min_periods=confirm).sum() >= confirm

        long_entry = bull_run & ~bull_run.shift(1, fill_value=False)
        short_entry = bear_run & ~bear_run.shift(1, fill_value=False)
        close = features["Close"]
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(14, confirm + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "confirm_bars": [2, 3, 4],
            "trend_ema_period": [0, 50, 100, 200],
            "min_body_atr": [0.0, 0.10, 0.20],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class BollingerBandWalkStrategy(BaseStrategy):
    """Continuation when price begins walking the outer Bollinger band."""

    @property
    def name(self) -> str:
        return "BollingerBandWalkStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "period": 20,
            "std_dev": 2.0,
            "slope_bars": 5,
            "adx_threshold": 18,
            "min_bandwidth_pct": 0.0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        if float(self.params.get("adx_threshold", 0)) > 0:
            required.add("ADX")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get("period", 20))
        std_dev = float(self.params.get("std_dev", 2.0))
        slope_bars = max(1, int(self.params.get("slope_bars", 5)))
        adx_threshold = float(self.params.get("adx_threshold", 18))
        min_width = float(self.params.get("min_bandwidth_pct", 0.0))
        mid, upper, lower = _get_bb(features, period, std_dev)
        close = features["Close"]
        width_pct = (upper - lower) / close.replace(0.0, np.nan) * 100.0
        width_ok = width_pct >= min_width

        long_entry = (
            (close > upper)
            & (close.shift(1) <= upper.shift(1))
            & (mid > mid.shift(slope_bars))
            & width_ok
        )
        short_entry = (
            (close < lower)
            & (close.shift(1) >= lower.shift(1))
            & (mid < mid.shift(slope_bars))
            & width_ok
        )

        if adx_threshold > 0:
            adx, _, _ = _get_adx_di(features, 14)
            allow = adx >= adx_threshold
            long_entry = long_entry & allow
            short_entry = short_entry & allow

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(period, slope_bars, 14) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "period": [20, 30, 40],
            "std_dev": [1.5, 2.0, 2.5],
            "slope_bars": [3, 5, 8],
            "adx_threshold": [0, 18, 25],
            "min_bandwidth_pct": [0.0, 0.20],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class PercentileChannelReversionStrategy(BaseStrategy):
    """Reversion from extreme rolling percentile channel levels."""

    @property
    def name(self) -> str:
        return "PercentileChannelReversionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 50,
            "lower_pct": 10,
            "upper_pct": 90,
            "adx_max": 25,
            "require_reclaim": True,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        if float(self.params.get("adx_max", 0)) > 0:
            required.add("ADX")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 50))
        lower_pct = float(self.params.get("lower_pct", 10)) / 100.0
        upper_pct = float(self.params.get("upper_pct", 90)) / 100.0
        adx_max = float(self.params.get("adx_max", 25))
        require_reclaim = bool(self.params.get("require_reclaim", True))
        close = features["Close"]
        lower = close.shift(1).rolling(lookback, min_periods=lookback).quantile(lower_pct)
        upper = close.shift(1).rolling(lookback, min_periods=lookback).quantile(upper_pct)

        if require_reclaim:
            long_entry = (close > lower) & (close.shift(1) <= lower.shift(1))
            short_entry = (close < upper) & (close.shift(1) >= upper.shift(1))
        else:
            long_entry = (close <= lower) & (close > close.shift(1))
            short_entry = (close >= upper) & (close < close.shift(1))

        if adx_max > 0:
            adx, _, _ = _get_adx_di(features, 14)
            allow = adx <= adx_max
            long_entry = long_entry & allow
            short_entry = short_entry & allow

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(lookback + 2, 14))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [20, 50, 80],
            "lower_pct": [5, 10, 15],
            "upper_pct": [85, 90, 95],
            "adx_max": [0, 20, 25],
            "require_reclaim": [True, False],
            **self._base_sl_tp_grid(),
        }


class MorningStarEveningStarStrategy(BaseStrategy):
    """Three-candle morning/evening star reversal pattern."""

    @property
    def name(self) -> str:
        return "MorningStarEveningStarStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "atr_period": 14,
            "body_atr_mult": 0.40,
            "star_body_frac": 0.35,
            "penetration": 0.50,
            "trend_ema_period": 0,
            "require_gap": False,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        atr_period = int(self.params.get("atr_period", 14))
        body_atr = float(self.params.get("body_atr_mult", 0.40))
        star_frac = float(self.params.get("star_body_frac", 0.35))
        penetration = float(self.params.get("penetration", 0.50))
        require_gap = bool(self.params.get("require_gap", False))
        open_, high, low, close, body, _, _, _, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)

        body_2 = body.shift(2)
        open_2 = open_.shift(2)
        close_2 = close.shift(2)
        body_1 = body.shift(1)
        open_1 = open_.shift(1)
        close_1 = close.shift(1)
        large_2 = body_2 >= body_atr * atr.shift(2)
        small_star = body_1 <= star_frac * body_2
        prev_mid = close_2 + penetration * (open_2 - close_2)
        bull_star = bear.shift(2) & large_2 & small_star & bull & (close >= prev_mid)
        bear_mid = close_2 - penetration * (close_2 - open_2)
        bear_star = bull.shift(2) & large_2 & small_star & bear & (close <= bear_mid)

        if require_gap:
            bull_star = bull_star & (pd.concat([open_1, close_1], axis=1).max(axis=1) < close_2)
            bear_star = bear_star & (pd.concat([open_1, close_1], axis=1).min(axis=1) > close_2)

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            bull_star = bull_star & (close.shift(1) <= ema.shift(1))
            bear_star = bear_star & (close.shift(1) >= ema.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bull_star] = 1
        signals[bear_star] = -1
        return self._zero_warmup(signals, max(atr_period + 3, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "atr_period": [10, 14, 20],
            "body_atr_mult": [0.25, 0.40, 0.60],
            "star_body_frac": [0.25, 0.35, 0.50],
            "penetration": [0.40, 0.50, 0.60],
            "trend_ema_period": [0, 100, 200],
            "require_gap": [False, True],
            **self._base_sl_tp_grid(),
        }


class PiercingDarkCloudStrategy(BaseStrategy):
    """Piercing line and dark-cloud-cover two-candle reversal pattern."""

    @property
    def name(self) -> str:
        return "PiercingDarkCloudStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "atr_period": 14,
            "body_atr_mult": 0.40,
            "penetration": 0.50,
            "require_gap": False,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        atr_period = int(self.params.get("atr_period", 14))
        body_atr = float(self.params.get("body_atr_mult", 0.40))
        penetration = float(self.params.get("penetration", 0.50))
        require_gap = bool(self.params.get("require_gap", False))
        open_, _, _, close, body, _, _, _, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        prev_large = body.shift(1) >= body_atr * atr.shift(1)
        prev_mid_bear = close.shift(1) + penetration * (open_.shift(1) - close.shift(1))
        prev_mid_bull = close.shift(1) - penetration * (close.shift(1) - open_.shift(1))

        piercing = bear.shift(1) & prev_large & bull & (close >= prev_mid_bear) & (close < open_.shift(1))
        dark_cloud = bull.shift(1) & prev_large & bear & (close <= prev_mid_bull) & (close > open_.shift(1))
        if require_gap:
            piercing = piercing & (open_ < close.shift(1))
            dark_cloud = dark_cloud & (open_ > close.shift(1))

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            piercing = piercing & (close.shift(1) <= ema.shift(1))
            dark_cloud = dark_cloud & (close.shift(1) >= ema.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[piercing] = 1
        signals[dark_cloud] = -1
        return self._zero_warmup(signals, max(atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "atr_period": [10, 14, 20],
            "body_atr_mult": [0.25, 0.40, 0.60],
            "penetration": [0.45, 0.50, 0.60],
            "require_gap": [False, True],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
        }


class HaramiReversalStrategy(BaseStrategy):
    """Bullish/bearish harami reversal after a large parent candle."""

    @property
    def name(self) -> str:
        return "HaramiReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "atr_period": 14,
            "min_parent_atr": 0.40,
            "child_body_ratio": 0.50,
            "require_opposite": True,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        atr_period = int(self.params.get("atr_period", 14))
        min_parent_atr = float(self.params.get("min_parent_atr", 0.40))
        child_ratio = float(self.params.get("child_body_ratio", 0.50))
        require_opposite = bool(self.params.get("require_opposite", True))
        open_, _, _, close, body, _, _, _, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)

        parent_top = pd.concat([open_.shift(1), close.shift(1)], axis=1).max(axis=1)
        parent_bottom = pd.concat([open_.shift(1), close.shift(1)], axis=1).min(axis=1)
        child_top = pd.concat([open_, close], axis=1).max(axis=1)
        child_bottom = pd.concat([open_, close], axis=1).min(axis=1)
        inside_parent = (child_top <= parent_top) & (child_bottom >= parent_bottom)
        large_parent = body.shift(1) >= min_parent_atr * atr.shift(1)
        small_child = body <= child_ratio * body.shift(1)

        bullish = bear.shift(1) & large_parent & small_child & inside_parent
        bearish = bull.shift(1) & large_parent & small_child & inside_parent
        if require_opposite:
            bullish = bullish & bull
            bearish = bearish & bear

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            bullish = bullish & (close.shift(1) <= ema.shift(1))
            bearish = bearish & (close.shift(1) >= ema.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bullish] = 1
        signals[bearish] = -1
        return self._zero_warmup(signals, max(atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "atr_period": [10, 14, 20],
            "min_parent_atr": [0.25, 0.40, 0.60],
            "child_body_ratio": [0.35, 0.50, 0.65],
            "require_opposite": [True, False],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
        }


class TweezerReversalStrategy(BaseStrategy):
    """Tweezer top/bottom reversal near a rolling high or low."""

    @property
    def name(self) -> str:
        return "TweezerReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 20,
            "atr_period": 14,
            "tolerance_atr": 0.10,
            "min_body_atr": 0.15,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 20))
        atr_period = int(self.params.get("atr_period", 14))
        tolerance = float(self.params.get("tolerance_atr", 0.10))
        min_body = float(self.params.get("min_body_atr", 0.15))
        _, high, low, close, body, _, _, _, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        same_low = (low - low.shift(1)).abs() <= tolerance * atr
        same_high = (high - high.shift(1)).abs() <= tolerance * atr
        at_low = low <= low.shift(1).rolling(lookback, min_periods=lookback).min() + tolerance * atr
        at_high = high >= high.shift(1).rolling(lookback, min_periods=lookback).max() - tolerance * atr
        meaningful = body >= min_body * atr

        bullish = bear.shift(1) & bull & same_low & at_low & meaningful
        bearish = bull.shift(1) & bear & same_high & at_high & meaningful

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            bullish = bullish & (close.shift(1) <= ema.shift(1))
            bearish = bearish & (close.shift(1) >= ema.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bullish] = 1
        signals[bearish] = -1
        return self._zero_warmup(signals, max(lookback + 2, atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [10, 20, 40],
            "atr_period": [10, 14, 20],
            "tolerance_atr": [0.05, 0.10, 0.20],
            "min_body_atr": [0.0, 0.15, 0.30],
            "trend_ema_period": [0, 100],
            **self._base_sl_tp_grid(),
        }


class ThreeBarReversalStrategy(BaseStrategy):
    """Three-bar reversal that breaks the prior short swing."""

    @property
    def name(self) -> str:
        return "ThreeBarReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "swing_bars": 2,
            "atr_period": 14,
            "break_buffer_atr": 0.05,
            "min_reversal_atr": 0.25,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        swing_bars = max(2, int(self.params.get("swing_bars", 2)))
        atr_period = int(self.params.get("atr_period", 14))
        buffer_atr = float(self.params.get("break_buffer_atr", 0.05))
        min_reversal = float(self.params.get("min_reversal_atr", 0.25))
        _, high, low, close, body, _, _, _, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        prior_high = high.shift(1).rolling(swing_bars, min_periods=swing_bars).max()
        prior_low = low.shift(1).rolling(swing_bars, min_periods=swing_bars).min()
        made_lower_low = low.shift(1) <= low.shift(2).rolling(swing_bars, min_periods=swing_bars).min()
        made_higher_high = high.shift(1) >= high.shift(2).rolling(swing_bars, min_periods=swing_bars).max()
        meaningful = body >= min_reversal * atr

        bullish = made_lower_low & bull & meaningful & (close > prior_high + buffer_atr * atr)
        bearish = made_higher_high & bear & meaningful & (close < prior_low - buffer_atr * atr)

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            bullish = bullish & (close.shift(1) <= ema.shift(1))
            bearish = bearish & (close.shift(1) >= ema.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bullish] = 1
        signals[bearish] = -1
        return self._zero_warmup(signals, max(swing_bars + atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "swing_bars": [2, 3, 4],
            "atr_period": [10, 14, 20],
            "break_buffer_atr": [0.0, 0.05, 0.10],
            "min_reversal_atr": [0.0, 0.25, 0.50],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
        }


class DojiBreakoutStrategy(BaseStrategy):
    """Breakout from a doji/indecision candle."""

    @property
    def name(self) -> str:
        return "DojiBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "atr_period": 14,
            "doji_body_frac": 0.10,
            "min_range_atr": 0.25,
            "breakout_buffer_atr": 0.05,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        atr_period = int(self.params.get("atr_period", 14))
        doji_frac = float(self.params.get("doji_body_frac", 0.10))
        min_range = float(self.params.get("min_range_atr", 0.25))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        _, high, low, close, body, rng, _, _, _, _ = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        prior_doji = (body.shift(1) <= doji_frac * rng.shift(1)) & (rng.shift(1) >= min_range * atr.shift(1))
        long_entry = prior_doji & (close > high.shift(1) + buffer_atr * atr)
        short_entry = prior_doji & (close < low.shift(1) - buffer_atr * atr)

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "atr_period": [10, 14, 20],
            "doji_body_frac": [0.05, 0.10, 0.15],
            "min_range_atr": [0.10, 0.25, 0.50],
            "breakout_buffer_atr": [0.0, 0.05, 0.10],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class MarubozuMomentumStrategy(BaseStrategy):
    """Large-bodied low-wick candle momentum continuation."""

    @property
    def name(self) -> str:
        return "MarubozuMomentumStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "atr_period": 14,
            "min_body_frac": 0.75,
            "max_wick_frac": 0.20,
            "min_range_atr": 0.75,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        atr_period = int(self.params.get("atr_period", 14))
        min_body_frac = float(self.params.get("min_body_frac", 0.75))
        max_wick_frac = float(self.params.get("max_wick_frac", 0.20))
        min_range_atr = float(self.params.get("min_range_atr", 0.75))
        _, _, _, close, body, rng, upper, lower, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        valid_range = rng > 1e-10
        body_ok = valid_range & (body / rng >= min_body_frac) & (rng >= min_range_atr * atr)
        long_entry = bull & body_ok & (upper / rng <= max_wick_frac)
        short_entry = bear & body_ok & (lower / rng <= max_wick_frac)

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "atr_period": [10, 14, 20],
            "min_body_frac": [0.65, 0.75, 0.85],
            "max_wick_frac": [0.10, 0.20, 0.30],
            "min_range_atr": [0.50, 0.75, 1.00],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class ThreeSoldiersCrowsStrategy(BaseStrategy):
    """Three white soldiers / three black crows continuation pattern."""

    @property
    def name(self) -> str:
        return "ThreeSoldiersCrowsStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "confirm_bars": 3,
            "atr_period": 14,
            "min_body_atr": 0.30,
            "max_wick_frac": 0.40,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        confirm = max(2, int(self.params.get("confirm_bars", 3)))
        atr_period = int(self.params.get("atr_period", 14))
        min_body_atr = float(self.params.get("min_body_atr", 0.30))
        max_wick = float(self.params.get("max_wick_frac", 0.40))
        open_, _, _, close, body, rng, upper, lower, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        valid_range = rng > 1e-10
        strong = valid_range & (body >= min_body_atr * atr)
        bull_quality = bull & strong & (upper / rng <= max_wick)
        bear_quality = bear & strong & (lower / rng <= max_wick)
        bull_run = bull_quality.rolling(confirm, min_periods=confirm).sum() >= confirm
        bear_run = bear_quality.rolling(confirm, min_periods=confirm).sum() >= confirm
        higher_closes = close > close.shift(1)
        lower_closes = close < close.shift(1)
        bull_closes = higher_closes.rolling(confirm - 1, min_periods=confirm - 1).sum() >= (confirm - 1)
        bear_closes = lower_closes.rolling(confirm - 1, min_periods=confirm - 1).sum() >= (confirm - 1)
        long_entry = bull_run & bull_closes & ~bull_run.shift(1, fill_value=False)
        short_entry = bear_run & bear_closes & ~bear_run.shift(1, fill_value=False)

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(atr_period + confirm + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "confirm_bars": [3, 4],
            "atr_period": [10, 14, 20],
            "min_body_atr": [0.20, 0.30, 0.50],
            "max_wick_frac": [0.30, 0.40, 0.50],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class DoubleTopBottomReversalStrategy(BaseStrategy):
    """Double-top/double-bottom rejection near rolling structure."""

    @property
    def name(self) -> str:
        return "DoubleTopBottomReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 40,
            "atr_period": 14,
            "tolerance_atr": 0.20,
            "rejection_body_atr": 0.20,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        atr_period = int(self.params.get("atr_period", 14))
        required = {f"ATR_{atr_period}"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 40))
        atr_period = int(self.params.get("atr_period", 14))
        tolerance = float(self.params.get("tolerance_atr", 0.20))
        min_body = float(self.params.get("rejection_body_atr", 0.20))
        _, high, low, close, body, _, _, _, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        prior_high = high.shift(1).rolling(lookback, min_periods=lookback).max()
        prior_low = low.shift(1).rolling(lookback, min_periods=lookback).min()

        top_touch = high >= prior_high - tolerance * atr
        bottom_touch = low <= prior_low + tolerance * atr
        meaningful = body >= min_body * atr
        bearish = top_touch & bear & meaningful & (close < prior_high - tolerance * atr)
        bullish = bottom_touch & bull & meaningful & (close > prior_low + tolerance * atr)

        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            bearish = bearish & (close.shift(1) >= ema.shift(1))
            bullish = bullish & (close.shift(1) <= ema.shift(1))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[bullish] = 1
        signals[bearish] = -1
        return self._zero_warmup(signals, max(lookback + 2, atr_period + 2, ema_period))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [20, 40, 80],
            "atr_period": [10, 14, 20],
            "tolerance_atr": [0.10, 0.20, 0.35],
            "rejection_body_atr": [0.0, 0.20, 0.40],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
        }


class TriangleCompressionBreakoutStrategy(BaseStrategy):
    """Breakout after converging highs/lows and volatility compression."""

    @property
    def name(self) -> str:
        return "TriangleCompressionBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 30,
            "atr_period": 14,
            "max_width_atr": 6.0,
            "breakout_buffer_atr": 0.05,
            "require_convergence": True,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return {f'ATR_{int(self.params.get("atr_period", 14))}'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 30))
        atr_period = int(self.params.get("atr_period", 14))
        max_width = float(self.params.get("max_width_atr", 6.0))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        require_conv = bool(self.params.get("require_convergence", True))
        _, high, low, close, _, _, _, _, _, _ = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        prior_high = high.shift(1).rolling(lookback, min_periods=lookback).max()
        prior_low = low.shift(1).rolling(lookback, min_periods=lookback).min()
        width = prior_high - prior_low
        compressed = width <= max_width * atr

        if require_conv:
            hi_slope = prior_high - prior_high.shift(max(2, lookback // 2))
            lo_slope = prior_low - prior_low.shift(max(2, lookback // 2))
            compressed = compressed & (hi_slope <= 0) & (lo_slope >= 0)

        long_entry = compressed & (close > prior_high + buffer_atr * atr)
        short_entry = compressed & (close < prior_low - buffer_atr * atr)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(lookback * 2, atr_period + 2))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [20, 30, 50],
            "atr_period": [10, 14, 20],
            "max_width_atr": [4.0, 6.0, 8.0],
            "breakout_buffer_atr": [0.0, 0.05, 0.10],
            "require_convergence": [True, False],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class FlagPullbackContinuationStrategy(BaseStrategy):
    """Continuation after a directional impulse and shallow pullback."""

    @property
    def name(self) -> str:
        return "FlagPullbackContinuationStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "impulse_bars": 10,
            "pullback_bars": 5,
            "impulse_atr_mult": 1.5,
            "max_retrace_frac": 0.618,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        impulse_bars = int(self.params.get("impulse_bars", 10))
        pullback_bars = int(self.params.get("pullback_bars", 5))
        impulse_mult = float(self.params.get("impulse_atr_mult", 2.0))
        retrace_frac = float(self.params.get("max_retrace_frac", 0.50))
        ema_period = int(self.params.get("trend_ema_period", 100))
        close = features["Close"]
        high = features["High"]
        low = features["Low"]
        atr = _get_atr(features, 14)
        impulse_start = close.shift(pullback_bars + impulse_bars)
        impulse_end = close.shift(pullback_bars)
        impulse = impulse_end - impulse_start
        impulse_abs_ok = impulse.abs() >= impulse_mult * atr.shift(pullback_bars) * max(1, impulse_bars / 5.0)

        pullback_low = low.shift(1).rolling(pullback_bars, min_periods=pullback_bars).min()
        pullback_high = high.shift(1).rolling(pullback_bars, min_periods=pullback_bars).max()
        long_retrace = (impulse_end - pullback_low) / (impulse.abs() + 1e-10)
        short_retrace = (pullback_high - impulse_end) / (impulse.abs() + 1e-10)
        long_setup = (impulse > 0) & impulse_abs_ok & (long_retrace >= 0.0) & (long_retrace <= retrace_frac)
        short_setup = (impulse < 0) & impulse_abs_ok & (short_retrace >= 0.0) & (short_retrace <= retrace_frac)

        long_entry = long_setup & (close > high.shift(1))
        short_entry = short_setup & (close < low.shift(1))

        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close >= ema)
            short_entry = short_entry & (close <= ema)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(impulse_bars + pullback_bars + 2, ema_period, 14))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "impulse_bars": [6, 10, 15],
            "pullback_bars": [3, 5, 8],
            "impulse_atr_mult": [1.5, 2.0, 3.0],
            "max_retrace_frac": [0.382, 0.50, 0.618],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class GapFadeStrategy(BaseStrategy):
    """Fade a session gap when continuation fails."""

    @property
    def name(self) -> str:
        return "GapFadeStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 8,
            "gap_atr_mult": 0.50,
            "fade_window_bars": 6,
            "reclaim_frac": 0.25,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 8))
        gap_mult = float(self.params.get("gap_atr_mult", 0.50))
        window = int(self.params.get("fade_window_bars", 6))
        reclaim_frac = float(self.params.get("reclaim_frac", 0.25))
        keys = _session_keys(features, start_hour)
        bar_no = _bar_number_in_session(features, start_hour)
        open_ = features["Open"]
        close = features["Close"]
        atr = _get_atr(features, 14)
        session_last_close = close.groupby(keys).last()
        prev_close = keys.map(session_last_close.shift(1))
        first_open = open_.groupby(keys).transform("first")
        gap = first_open - prev_close
        active = (bar_no >= 1) & (bar_no <= window)
        gap_up = gap >= gap_mult * atr
        gap_down = gap <= -gap_mult * atr
        up_reclaim = first_open - reclaim_frac * gap.abs()
        down_reclaim = first_open + reclaim_frac * gap.abs()
        short_entry = active & gap_up & (close < up_reclaim)
        long_entry = active & gap_down & (close > down_reclaim)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 8, 13],
            "gap_atr_mult": [0.25, 0.50, 0.75],
            "fade_window_bars": [3, 6, 12],
            "reclaim_frac": [0.25, 0.50, 0.75],
            **self._base_sl_tp_grid(),
        }


class OpeningDriveFailureStrategy(BaseStrategy):
    """Fade an opening drive that breaks the range then loses session VWAP."""

    @property
    def name(self) -> str:
        return "OpeningDriveFailureStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 8,
            "range_bars": 3,
            "failure_window_bars": 12,
            "breakout_buffer_atr": 0.05,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 8))
        range_bars = int(self.params.get("range_bars", 3))
        failure_window = int(self.params.get("failure_window_bars", 12))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        keys = _session_keys(features, start_hour)
        bar_no = _bar_number_in_session(features, start_hour)
        atr = _get_atr(features, 14)
        in_range = bar_no < range_bars
        range_high = features["High"].where(in_range).groupby(keys).cummax().groupby(keys).ffill()
        range_low = features["Low"].where(in_range).groupby(keys).cummin().groupby(keys).ffill()
        close = features["Close"]
        vwap = _get_session_vwap(features, start_hour)
        active = (bar_no >= range_bars) & (bar_no <= failure_window)
        drove_up = (features["High"].groupby(keys).cummax() > range_high + buffer_atr * atr)
        drove_down = (features["Low"].groupby(keys).cummin() < range_low - buffer_atr * atr)
        short_entry = active & drove_up & (close < vwap) & (close.shift(1) >= vwap.shift(1))
        long_entry = active & drove_down & (close > vwap) & (close.shift(1) <= vwap.shift(1))
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(range_bars + 2, 20))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 8, 13],
            "range_bars": [2, 3, 6],
            "failure_window_bars": [6, 12, 18],
            "breakout_buffer_atr": [0.0, 0.05, 0.10],
            **self._base_sl_tp_grid(),
        }


class PreviousSessionLevelRejectionStrategy(BaseStrategy):
    """Reject previous session high/low with close back inside the level."""

    @property
    def name(self) -> str:
        return "PreviousSessionLevelRejectionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "session_start_hour": 0,
            "atr_period": 14,
            "tolerance_atr": 0.10,
            "wick_ratio": 1.5,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        return {f'ATR_{int(self.params.get("atr_period", 14))}'}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        start_hour = int(self.params.get("session_start_hour", 0))
        atr_period = int(self.params.get("atr_period", 14))
        tolerance = float(self.params.get("tolerance_atr", 0.10))
        wick_ratio = float(self.params.get("wick_ratio", 1.5))
        keys = _session_keys(features, start_hour)
        prev_high = keys.map(features["High"].groupby(keys).max().shift(1))
        prev_low = keys.map(features["Low"].groupby(keys).min().shift(1))
        _, high, low, close, body, _, upper, lower, bull, bear = _candle_parts(features)
        atr = _get_atr(features, atr_period)
        body_safe = body.replace(0.0, np.nan)
        reject_high = (high >= prev_high - tolerance * atr) & (close < prev_high) & bear & ((upper / body_safe) >= wick_ratio)
        reject_low = (low <= prev_low + tolerance * atr) & (close > prev_low) & bull & ((lower / body_safe) >= wick_ratio)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[reject_low] = 1
        signals[reject_high] = -1
        return self._zero_warmup(signals, max(atr_period + 2, 20))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "session_start_hour": [0, 8],
            "atr_period": [10, 14, 20],
            "tolerance_atr": [0.05, 0.10, 0.20],
            "wick_ratio": [1.0, 1.5, 2.0],
            **self._base_sl_tp_grid(),
        }


class SupportResistanceRetestBreakoutStrategy(BaseStrategy):
    """Breakout, retest, and continuation around rolling support/resistance."""

    @property
    def name(self) -> str:
        return "SupportResistanceRetestBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 50,
            "retest_window": 6,
            "tolerance_atr": 0.20,
            "breakout_buffer_atr": 0.05,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 50))
        retest_window = int(self.params.get("retest_window", 6))
        tolerance = float(self.params.get("tolerance_atr", 0.20))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        high = features["High"]
        low = features["Low"]
        close = features["Close"]
        atr = _get_atr(features, 14)
        resistance = high.shift(1).rolling(lookback, min_periods=lookback).max()
        support = low.shift(1).rolling(lookback, min_periods=lookback).min()
        broke_up = close > resistance + buffer_atr * atr
        broke_down = close < support - buffer_atr * atr
        prior_broke_up = broke_up.shift(1, fill_value=False)
        prior_broke_down = broke_down.shift(1, fill_value=False)
        recent_up_break = prior_broke_up.rolling(retest_window, min_periods=1).max().astype(bool)
        recent_down_break = prior_broke_down.rolling(retest_window, min_periods=1).max().astype(bool)
        retest_res = low <= resistance + tolerance * atr
        retest_sup = high >= support - tolerance * atr
        long_entry = recent_up_break & retest_res & (close > resistance)
        short_entry = recent_down_break & retest_sup & (close < support)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry & ~long_entry.shift(1, fill_value=False)] = 1
        signals[short_entry & ~short_entry.shift(1, fill_value=False)] = -1
        return self._zero_warmup(signals, max(lookback + retest_window + 2, 20))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [30, 50, 80],
            "retest_window": [3, 6, 10],
            "tolerance_atr": [0.10, 0.20, 0.35],
            "breakout_buffer_atr": [0.0, 0.05, 0.10],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class MoneyFlowIndexReversalStrategy(BaseStrategy):
    """MFI exhaustion with price reclaim."""

    @property
    def name(self) -> str:
        return "MoneyFlowIndexReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "mfi_period": 14,
            "oversold": 20,
            "overbought": 80,
            "trend_ema_period": 0,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    def get_required_features(self) -> Set[str]:
        required = {"ATR_14"}
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            required.add(f"EMA_{ema_period}")
        return required

    @staticmethod
    def _mfi(features: pd.DataFrame, period: int) -> pd.Series:
        typical = (features["High"] + features["Low"] + features["Close"]) / 3.0
        flow = typical * _get_volume(features, default=1.0)
        positive = flow.where(typical > typical.shift(1), 0.0)
        negative = flow.where(typical < typical.shift(1), 0.0)
        pos_sum = positive.rolling(period, min_periods=period).sum()
        neg_sum = negative.rolling(period, min_periods=period).sum()
        ratio = pos_sum / (neg_sum + 1e-10)
        return 100.0 - (100.0 / (1.0 + ratio))

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get("mfi_period", 14))
        oversold = float(self.params.get("oversold", 20))
        overbought = float(self.params.get("overbought", 80))
        close = features["Close"]
        mfi = self._mfi(features, period)
        long_entry = (mfi.shift(1) <= oversold) & (mfi > mfi.shift(1)) & (close > close.shift(1))
        short_entry = (mfi.shift(1) >= overbought) & (mfi < mfi.shift(1)) & (close < close.shift(1))
        ema_period = int(self.params.get("trend_ema_period", 0))
        if ema_period > 0:
            ema = _get_ema(features, ema_period)
            long_entry = long_entry & (close <= ema)
            short_entry = short_entry & (close >= ema)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(period + 2, ema_period, 14))

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "mfi_period": [10, 14, 20],
            "oversold": [15, 20, 25],
            "overbought": [75, 80, 85],
            "trend_ema_period": [0, 100, 200],
            **self._base_sl_tp_grid(),
        }


class ChaikinMoneyFlowTrendStrategy(BaseStrategy):
    """CMF-confirmed trend continuation."""

    @property
    def name(self) -> str:
        return "ChaikinMoneyFlowTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "cmf_period": 20,
            "cmf_threshold": 0.05,
            "trend_ema_period": 100,
            "breakout_lookback": 10,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14", f'EMA_{int(self.params.get("trend_ema_period", 100))}'}

    @staticmethod
    def _cmf(features: pd.DataFrame, period: int) -> pd.Series:
        high = features["High"]
        low = features["Low"]
        close = features["Close"]
        vol = _get_volume(features, default=1.0)
        multiplier = ((close - low) - (high - close)) / ((high - low) + 1e-10)
        mfv = multiplier * vol
        return mfv.rolling(period, min_periods=period).sum() / (vol.rolling(period, min_periods=period).sum() + 1e-10)

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        cmf_period = int(self.params.get("cmf_period", 20))
        threshold = float(self.params.get("cmf_threshold", 0.05))
        ema_period = int(self.params.get("trend_ema_period", 100))
        lookback = int(self.params.get("breakout_lookback", 10))
        close = features["Close"]
        ema = _get_ema(features, ema_period)
        cmf = self._cmf(features, cmf_period)
        prior_high = features["High"].shift(1).rolling(lookback, min_periods=lookback).max()
        prior_low = features["Low"].shift(1).rolling(lookback, min_periods=lookback).min()
        long_entry = (close > ema) & (cmf >= threshold) & (close > prior_high)
        short_entry = (close < ema) & (cmf <= -threshold) & (close < prior_low)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(cmf_period, ema_period, lookback) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "cmf_period": [10, 20, 30],
            "cmf_threshold": [0.0, 0.05, 0.10],
            "trend_ema_period": [50, 100, 200],
            "breakout_lookback": [5, 10, 20],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


class AccumulationDistributionBreakoutStrategy(BaseStrategy):
    """Price breakout confirmed by accumulation/distribution line direction."""

    @property
    def name(self) -> str:
        return "AccumulationDistributionBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 20,
            "adl_slope_bars": 10,
            "breakout_buffer_atr": 0.05,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            **self._structural_sl_defaults(),
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    @staticmethod
    def _adl(features: pd.DataFrame) -> pd.Series:
        high = features["High"]
        low = features["Low"]
        close = features["Close"]
        vol = _get_volume(features, default=1.0)
        mfm = ((close - low) - (high - close)) / ((high - low) + 1e-10)
        return (mfm * vol).cumsum()

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lookback = int(self.params.get("lookback", 20))
        slope_bars = int(self.params.get("adl_slope_bars", 10))
        buffer_atr = float(self.params.get("breakout_buffer_atr", 0.05))
        close = features["Close"]
        atr = _get_atr(features, 14)
        adl = self._adl(features)
        prior_high = features["High"].shift(1).rolling(lookback, min_periods=lookback).max()
        prior_low = features["Low"].shift(1).rolling(lookback, min_periods=lookback).min()
        adl_up = adl > adl.shift(slope_bars)
        adl_down = adl < adl.shift(slope_bars)
        long_entry = adl_up & (close > prior_high + buffer_atr * atr)
        short_entry = adl_down & (close < prior_low - buffer_atr * atr)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, max(lookback, slope_bars, 14) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [10, 20, 40],
            "adl_slope_bars": [5, 10, 20],
            "breakout_buffer_atr": [0.0, 0.05, 0.10],
            **self._base_sl_tp_grid(),
            **self._structural_sl_grid(),
        }


# =============================================================================
# PACKAGE K: TIME / SESSION / CALENDAR AND CARRY-AWARE STRATEGIES
# =============================================================================


def _index_hour(features: pd.DataFrame) -> pd.Series:
    idx = features.index
    if isinstance(idx, pd.DatetimeIndex):
        return pd.Series(idx.hour, index=idx)
    return pd.Series(np.nan, index=idx, dtype=float)


def _index_dow(features: pd.DataFrame) -> pd.Series:
    idx = features.index
    if isinstance(idx, pd.DatetimeIndex):
        return pd.Series(idx.dayofweek, index=idx)
    return pd.Series(np.nan, index=idx, dtype=float)


def _index_dom(features: pd.DataFrame) -> pd.Series:
    idx = features.index
    if isinstance(idx, pd.DatetimeIndex):
        return pd.Series(idx.day, index=idx)
    return pd.Series(np.nan, index=idx, dtype=float)


def _carry_direction(symbol: str, min_diff: float) -> int:
    """+1 for positive long carry, -1 for positive short carry, else 0.

    Static per symbol/broker spec; signal-only (no lookahead). If both sides
    are negative, the carry strategies stay flat instead of picking the less
    expensive bleed side.
    """
    try:
        spec = get_instrument_spec(symbol)
        swap_long = float(spec.swap_long)
        swap_short = float(spec.swap_short)
    except Exception:
        return 0
    if max(swap_long, swap_short) <= 0.0:
        return 0
    diff = swap_long - swap_short
    if abs(diff) < float(min_diff):
        return 0
    if diff > 0 and swap_long > 0.0:
        return 1
    if diff < 0 and swap_short > 0.0:
        return -1
    return 0


class TimeOfDayMomentumStrategy(BaseStrategy):
    """Directional momentum gated to an intraday hour window (e.g. London/NY drive). Flat
    outside the window so the edge is the session, not 24h noise."""

    @property
    def name(self) -> str:
        return "TimeOfDayMomentumStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"start_hour": 8, "end_hour": 16, "mom_lookback": 12, "atr_buffer": 0.1,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        sh, eh = int(self.params.get("start_hour", 8)), int(self.params.get("end_hour", 16))
        lb = max(1, int(self.params.get("mom_lookback", 12)))
        buf = float(self.params.get("atr_buffer", 0.1))
        hour = _index_hour(features)
        close = features["Close"]
        atr = _get_atr(features, 14)
        in_window = (hour >= sh) & (hour < eh)
        mom = close - close.shift(lb)
        thresh = buf * atr
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[in_window & (mom > thresh)] = 1
        signals[in_window & (mom < -thresh)] = -1
        return self._zero_warmup(signals, lb + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"start_hour": [0, 7, 8, 13], "end_hour": [16, 20, 24],
                "mom_lookback": [6, 12, 24], "atr_buffer": [0.0, 0.1, 0.25],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class SessionCloseReversionStrategy(BaseStrategy):
    """Fade intraday over-extension vs the session VWAP during the late session hours."""

    @property
    def name(self) -> str:
        return "SessionCloseReversionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"session_start_hour": 0, "late_start_hour": 19, "ext_atr": 1.0,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ss = int(self.params.get("session_start_hour", 0))
        late = int(self.params.get("late_start_hour", 20))
        ext = float(self.params.get("ext_atr", 1.0))
        hour = _index_hour(features)
        close = features["Close"]
        atr = _get_atr(features, 14)
        vwap = _get_session_vwap(features, ss)
        band = ext * atr
        in_late = hour >= late
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[in_late & (close < vwap - band)] = 1
        signals[in_late & (close > vwap + band)] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"session_start_hour": [0, 8], "late_start_hour": [16, 19, 21],
                "ext_atr": [0.5, 1.0, 1.5], **self._base_sl_tp_grid()}


class DayOfWeekBiasStrategy(BaseStrategy):
    """Trade a configured weekday's known momentum/reversion tendency only (calendar effect)."""

    @property
    def name(self) -> str:
        return "DayOfWeekBiasStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"weekday": 0, "mode": "momentum", "lookback": 5, "atr_buffer": 0.1,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        wd = int(self.params.get("weekday", 0))
        mode = str(self.params.get("mode", "momentum"))
        lb = max(1, int(self.params.get("lookback", 5)))
        buf = float(self.params.get("atr_buffer", 0.1))
        dow = _index_dow(features)
        close = features["Close"]
        atr = _get_atr(features, 14)
        target = dow == wd
        mom = close - close.shift(lb)
        thresh = buf * atr
        up = target & (mom > thresh)
        dn = target & (mom < -thresh)
        signals = pd.Series(0, index=features.index, dtype=int)
        if mode == "reversion":
            signals[up] = -1
            signals[dn] = 1
        else:
            signals[up] = 1
            signals[dn] = -1
        return self._zero_warmup(signals, lb + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"weekday": [0, 1, 2, 3, 4], "mode": ["momentum", "reversion"],
                "lookback": [3, 5, 10], "atr_buffer": [0.0, 0.1, 0.25],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class TurnOfMonthStrategy(BaseStrategy):
    """Turn-of-month seasonal bias (last/first calendar days), with optional trend agreement.
    Most relevant for indices/equities; the optimizer picks the per-symbol direction."""

    @property
    def name(self) -> str:
        return "TurnOfMonthStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"tom_start_day": 26, "tom_end_day": 3, "bias": 1, "trend_ema": 0,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ts = int(self.params.get("tom_start_day", 26))
        te = int(self.params.get("tom_end_day", 3))
        bias = 1 if int(self.params.get("bias", 1)) >= 0 else -1
        ema_p = int(self.params.get("trend_ema", 0))
        dom = _index_dom(features)
        close = features["Close"]
        in_window = (dom >= ts) | (dom <= te)
        trend_ok = pd.Series(True, index=features.index)
        if ema_p > 0:
            ema = _get_ema(features, ema_p)
            trend_ok = (close >= ema) if bias == 1 else (close <= ema)
        entry = in_window & trend_ok
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[entry] = bias
        return self._zero_warmup(signals, max(ema_p, 5) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"tom_start_day": [24, 26, 28], "tom_end_day": [2, 3, 5], "bias": [1, -1],
                "trend_ema": [0, 100, 200], **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class OvernightDriftStrategy(BaseStrategy):
    """At a pre-close hour, enter in the direction of the day's move vs the session open to
    capture overnight drift continuation."""

    @property
    def name(self) -> str:
        return "OvernightDriftStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"session_start_hour": 0, "entry_hour": 21, "min_move_atr": 0.5,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ss = int(self.params.get("session_start_hour", 0))
        eh = int(self.params.get("entry_hour", 21))
        mm = float(self.params.get("min_move_atr", 0.5))
        hour = _index_hour(features)
        keys = _session_keys(features, ss)
        close = features["Close"]
        atr = _get_atr(features, 14)
        open_source = features["Open"] if "Open" in features.columns else close
        session_open = open_source.groupby(keys).transform("first")
        move = close - session_open
        previous_hour_in_session = hour.groupby(keys).shift(1)
        at_entry = (hour == eh) & (previous_hour_in_session != eh)
        band = mm * atr
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[at_entry & (move > band)] = 1
        signals[at_entry & (move < -band)] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"session_start_hour": [0, 8], "entry_hour": [17, 20, 21, 22],
                "min_move_atr": [0.0, 0.5, 1.0], **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class IntradayRangeFadeStrategy(BaseStrategy):
    """During quiet hours, fade breaks beyond the developing session range (mean reversion)."""

    @property
    def name(self) -> str:
        return "IntradayRangeFadeStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"session_start_hour": 0, "quiet_start_hour": 1, "quiet_end_hour": 7,
                "ext_atr": 0.5, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ss = int(self.params.get("session_start_hour", 0))
        qs = int(self.params.get("quiet_start_hour", 1))
        qe = int(self.params.get("quiet_end_hour", 7))
        ext = float(self.params.get("ext_atr", 0.5))
        hour = _index_hour(features)
        keys = _session_keys(features, ss)
        close = features["Close"]
        atr = _get_atr(features, 14)
        dev_high = features["High"].groupby(keys).cummax().groupby(keys).shift(1)
        dev_low = features["Low"].groupby(keys).cummin().groupby(keys).shift(1)
        quiet = (hour >= qs) & (hour < qe)
        band = ext * atr
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[quiet & (close < dev_low - band)] = 1
        signals[quiet & (close > dev_high + band)] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"session_start_hour": [0, 8], "quiet_start_hour": [0, 1, 2],
                "quiet_end_hour": [5, 7, 9], "ext_atr": [0.25, 0.5, 1.0],
                **self._base_sl_tp_grid()}


class SwapCarryTrendStrategy(BaseStrategy):
    """Trade only in the positive-carry direction (from broker swap) when a slow trend agrees,
    so longer holds are paid by carry rather than bleeding negative swap. H4/D1 in spirit."""

    @property
    def name(self) -> str:
        return "SwapCarryTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"ema_period": 100, "slope_bars": 20, "min_swap_diff": 1.0,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        slope = max(1, int(self.params.get("slope_bars", 20)))
        carry_dir = _carry_direction(symbol, float(self.params.get("min_swap_diff", 1.0)))
        signals = pd.Series(0, index=features.index, dtype=int)
        if carry_dir == 0:
            return signals
        close = features["Close"]
        ema = _get_ema(features, ema_p)
        rising = ema > ema.shift(slope)
        falling = ema < ema.shift(slope)
        if carry_dir == 1:
            signals[rising & (close > ema)] = 1
        else:
            signals[falling & (close < ema)] = -1
        return self._zero_warmup(signals, ema_p + slope + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"ema_period": [50, 100, 200], "slope_bars": [10, 20, 40],
                "min_swap_diff": [0.5, 1.0, 2.0], **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class CarryFilteredPullbackStrategy(BaseStrategy):
    """Pullback-continuation entries taken only in the positive-carry direction."""

    @property
    def name(self) -> str:
        return "CarryFilteredPullbackStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"ema_period": 50, "pullback_atr": 1.0, "min_swap_diff": 1.0,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ema_p = max(2, int(self.params.get("ema_period", 50)))
        pb = float(self.params.get("pullback_atr", 1.0))
        carry_dir = _carry_direction(symbol, float(self.params.get("min_swap_diff", 1.0)))
        signals = pd.Series(0, index=features.index, dtype=int)
        if carry_dir == 0:
            return signals
        close = features["Close"]
        ema = _get_ema(features, ema_p)
        atr = _get_atr(features, 14)
        band = pb * atr
        if carry_dir == 1:
            dipped = close.shift(1) < (ema.shift(1) - band.shift(1))
            signals[dipped & (close > ema)] = 1
        else:
            popped = close.shift(1) > (ema.shift(1) + band.shift(1))
            signals[popped & (close < ema)] = -1
        return self._zero_warmup(signals, ema_p + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"ema_period": [20, 50, 100], "pullback_atr": [0.5, 1.0, 1.5],
                "min_swap_diff": [0.5, 1.0, 2.0], **self._base_sl_tp_grid(), **self._structural_sl_grid()}


# =============================================================================
# PACKAGE L: STRUCTURE / VWAP / EFFICIENCY-RATIO COVERAGE STRATEGIES
# =============================================================================


def _efficiency_ratio(close: pd.Series, period: int) -> pd.Series:
    direction = (close - close.shift(period)).abs()
    volatility = close.diff().abs().rolling(period, min_periods=period).sum()
    return (direction / (volatility.replace(0.0, np.nan) + 1e-12)).fillna(0.0).clip(0.0, 1.0)


class FractalChannelBreakoutStrategy(BaseStrategy):
    """Break confirmed swing/fractal highs/lows, using only already-confirmed pivots."""

    @property
    def name(self) -> str:
        return "FractalChannelBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"wing_bars": 2, "atr_buffer": 0.1, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        return {"ATR_14", f"EMA_{ema_p}"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        wing = max(1, int(self.params.get("wing_bars", 2)))
        buf = float(self.params.get("atr_buffer", 0.1))
        high, low, close = features["High"], features["Low"], features["Close"]
        atr = _get_atr(features, 14)

        center_high = high.shift(wing + 1)
        center_low = low.shift(wing + 1)
        left_high = high.shift(wing + 2).rolling(wing, min_periods=wing).max()
        left_low = low.shift(wing + 2).rolling(wing, min_periods=wing).min()
        right_high = high.shift(1).rolling(wing, min_periods=wing).max()
        right_low = low.shift(1).rolling(wing, min_periods=wing).min()

        fractal_high = center_high.where((center_high > left_high) & (center_high >= right_high)).ffill()
        fractal_low = center_low.where((center_low < left_low) & (center_low <= right_low)).ffill()
        upper = fractal_high + buf * atr
        lower = fractal_low - buf * atr

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[(close > upper) & (close.shift(1) <= upper.shift(1))] = 1
        signals[(close < lower) & (close.shift(1) >= lower.shift(1))] = -1
        return self._zero_warmup(signals, wing * 2 + 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"wing_bars": [2, 3, 5], "atr_buffer": [0.0, 0.1, 0.25],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class SwingFailureReversalStrategy(BaseStrategy):
    """Fade failed sweeps of recent swing highs/lows after price closes back inside."""

    @property
    def name(self) -> str:
        return "SwingFailureReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"lookback": 20, "sweep_atr": 0.05, "close_back_atr": 0.0,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lb = max(3, int(self.params.get("lookback", 20)))
        sweep = float(self.params.get("sweep_atr", 0.05))
        back = float(self.params.get("close_back_atr", 0.0))
        atr = _get_atr(features, 14)
        high, low, close = features["High"], features["Low"], features["Close"]
        prev_high = high.shift(1).rolling(lb, min_periods=lb).max()
        prev_low = low.shift(1).rolling(lb, min_periods=lb).min()

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[(low < prev_low - sweep * atr) & (close > prev_low + back * atr)] = 1
        signals[(high > prev_high + sweep * atr) & (close < prev_high - back * atr)] = -1
        return self._zero_warmup(signals, lb + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"lookback": [10, 20, 40], "sweep_atr": [0.0, 0.05, 0.15],
                "close_back_atr": [0.0, 0.05], **self._base_sl_tp_grid()}


class AnchoredVWAPReclaimStrategy(BaseStrategy):
    """Reclaim session VWAP after an ATR-sized flush, filtered by the broader trend."""

    @property
    def name(self) -> str:
        return "AnchoredVWAPReclaimStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"session_start_hour": 0, "ema_period": 100, "flush_atr": 0.5,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ss = int(self.params.get("session_start_hour", 0))
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        flush = float(self.params.get("flush_atr", 0.5))
        close = features["Close"]
        atr = _get_atr(features, 14)
        keys = _session_keys(features, ss)
        same_session_prev = keys == keys.shift(1)
        vwap = _get_session_vwap(features, ss)
        ema = _get_ema(features, ema_p)
        band = flush * atr

        long_entry = (same_session_prev & (close.shift(1) < vwap.shift(1) - band.shift(1))
                      & (close > vwap) & (close > ema))
        short_entry = (same_session_prev & (close.shift(1) > vwap.shift(1) + band.shift(1))
                       & (close < vwap) & (close < ema))
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, ema_p + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"session_start_hour": [0, 8], "ema_period": [50, 100, 200],
                "flush_atr": [0.25, 0.5, 1.0], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class VolumeClimaxReversalStrategy(BaseStrategy):
    """Fade capitulation bars with unusually high volume, large range, and rejection wick."""

    @property
    def name(self) -> str:
        return "VolumeClimaxReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"vol_lookback": 30, "volume_mult": 2.0, "range_atr": 1.5, "wick_frac": 0.35,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        return {"ATR_14", f"EMA_{ema_p}"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lb = max(5, int(self.params.get("vol_lookback", 30)))
        vm = float(self.params.get("volume_mult", 2.0))
        rm = float(self.params.get("range_atr", 1.5))
        wf = float(self.params.get("wick_frac", 0.35))
        open_, high, low, close = features["Open"], features["High"], features["Low"], features["Close"]
        atr = _get_atr(features, 14)
        volume = _get_volume(features)
        avg_volume = volume.shift(1).rolling(lb, min_periods=lb).mean()
        bar_range = (high - low).replace(0.0, np.nan)
        upper_wick = high - pd.concat([open_, close], axis=1).max(axis=1)
        lower_wick = pd.concat([open_, close], axis=1).min(axis=1) - low
        climax = (volume >= vm * avg_volume) & ((high - low) >= rm * atr)

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[climax & (lower_wick / bar_range >= wf) & (close > open_)] = 1
        signals[climax & (upper_wick / bar_range >= wf) & (close < open_)] = -1
        return self._zero_warmup(signals, max(lb, 20) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"vol_lookback": [20, 30, 50], "volume_mult": [1.5, 2.0, 3.0],
                "range_atr": [1.0, 1.5, 2.0], "wick_frac": [0.25, 0.35, 0.50],
                **self._base_sl_tp_grid()}


class EfficiencyRatioTrendStrategy(BaseStrategy):
    """Trend continuation when Kaufman's efficiency ratio shows directional movement."""

    @property
    def name(self) -> str:
        return "EfficiencyRatioTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"er_period": 20, "min_er": 0.35, "ema_period": 100, "slope_bars": 10,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        er_p = max(2, int(self.params.get("er_period", 20)))
        min_er = float(self.params.get("min_er", 0.35))
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        slope = max(1, int(self.params.get("slope_bars", 10)))
        close = features["Close"]
        ema = _get_ema(features, ema_p)
        er = _efficiency_ratio(close, er_p)
        long_state = (er >= min_er) & (close > ema) & (ema > ema.shift(slope))
        short_state = (er >= min_er) & (close < ema) & (ema < ema.shift(slope))
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_state & ~long_state.shift(1, fill_value=False)] = 1
        signals[short_state & ~short_state.shift(1, fill_value=False)] = -1
        return self._zero_warmup(signals, max(er_p, ema_p + slope) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"er_period": [10, 20, 40], "min_er": [0.25, 0.35, 0.50],
                "ema_period": [50, 100, 200], "slope_bars": [5, 10, 20],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class EfficiencyRatioMeanReversionStrategy(BaseStrategy):
    """Fade z-score extremes only when efficiency ratio says price is not trending."""

    @property
    def name(self) -> str:
        return "EfficiencyRatioMeanReversionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"er_period": 20, "max_er": 0.25, "z_period": 30, "z_entry": 1.5,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        er_p = max(2, int(self.params.get("er_period", 20)))
        max_er = float(self.params.get("max_er", 0.25))
        z_p = max(5, int(self.params.get("z_period", 30)))
        z_entry = float(self.params.get("z_entry", 1.5))
        close = features["Close"]
        er = _efficiency_ratio(close, er_p)
        mean = close.rolling(z_p, min_periods=z_p).mean()
        std = close.rolling(z_p, min_periods=z_p).std().replace(0.0, np.nan)
        z = (close - mean) / (std + 1e-12)
        choppy = er <= max_er
        long_state = choppy & (z <= -z_entry)
        short_state = choppy & (z >= z_entry)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_state & ~long_state.shift(1, fill_value=False)] = 1
        signals[short_state & ~short_state.shift(1, fill_value=False)] = -1
        return self._zero_warmup(signals, max(er_p, z_p) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"er_period": [10, 20, 40], "max_er": [0.15, 0.25, 0.35],
                "z_period": [20, 30, 50], "z_entry": [1.0, 1.5, 2.0],
                **self._base_sl_tp_grid()}


class OpeningRangeRetestStrategy(BaseStrategy):
    """Enter after an opening-range break survives a pullback/retest."""

    @property
    def name(self) -> str:
        return "OpeningRangeRetestStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"session_start_hour": 8, "range_bars": 6, "retest_window": 12, "retest_atr": 0.25,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ss = int(self.params.get("session_start_hour", 8))
        range_bars = max(1, int(self.params.get("range_bars", 6)))
        retest_window = max(1, int(self.params.get("retest_window", 12)))
        retest_atr = float(self.params.get("retest_atr", 0.25))
        keys = _session_keys(features, ss)
        bar_no = _bar_number_in_session(features, ss)
        in_range = bar_no < range_bars
        range_high = features["High"].where(in_range).groupby(keys).cummax().groupby(keys).ffill()
        range_low = features["Low"].where(in_range).groupby(keys).cummin().groupby(keys).ffill()
        close = features["Close"]
        atr = _get_atr(features, 14)
        same_session_prev = keys == keys.shift(1)
        ready = bar_no >= range_bars
        broke_up = ready & same_session_prev & (close > range_high) & (close.shift(1) <= range_high.shift(1))
        broke_down = ready & same_session_prev & (close < range_low) & (close.shift(1) >= range_low.shift(1))
        prior_up = broke_up.shift(1, fill_value=False).astype(int)
        prior_down = broke_down.shift(1, fill_value=False).astype(int)
        recent_up = (
            prior_up.groupby(keys).rolling(retest_window, min_periods=1).max()
            .reset_index(level=0, drop=True).astype(bool)
        )
        recent_down = (
            prior_down.groupby(keys).rolling(retest_window, min_periods=1).max()
            .reset_index(level=0, drop=True).astype(bool)
        )
        long_touch = recent_up & (features["Low"] <= range_high + retest_atr * atr) & (close > range_high)
        short_touch = recent_down & (features["High"] >= range_low - retest_atr * atr) & (close < range_low)
        up_event_id = broke_up.astype(int).groupby(keys).cumsum()
        down_event_id = broke_down.astype(int).groupby(keys).cumsum()
        long_retest = long_touch & (up_event_id > 0) & (
            long_touch.astype(int).groupby([keys, up_event_id]).cumsum() == 1
        )
        short_retest = short_touch & (down_event_id > 0) & (
            short_touch.astype(int).groupby([keys, down_event_id]).cumsum() == 1
        )

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_retest] = 1
        signals[short_retest] = -1
        return self._zero_warmup(signals, max(range_bars + retest_window + 2, 20))

    def get_param_grid(self) -> Dict[str, List]:
        return {"session_start_hour": [0, 8, 13], "range_bars": [3, 6, 12],
                "retest_window": [6, 12, 24], "retest_atr": [0.0, 0.25, 0.50],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class SessionVWAPPullbackTrendStrategy(BaseStrategy):
    """Trend-following pullback to session VWAP, then continuation away from value."""

    @property
    def name(self) -> str:
        return "SessionVWAPPullbackTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"session_start_hour": 0, "ema_period": 100, "slope_bars": 10, "pullback_atr": 0.25,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        return {"ATR_14", f"EMA_{ema_p}"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ss = int(self.params.get("session_start_hour", 0))
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        slope = max(1, int(self.params.get("slope_bars", 10)))
        pb = float(self.params.get("pullback_atr", 0.25))
        close = features["Close"]
        atr = _get_atr(features, 14)
        keys = _session_keys(features, ss)
        same_session_prev = keys == keys.shift(1)
        vwap = _get_session_vwap(features, ss)
        ema = _get_ema(features, ema_p)
        band = pb * atr
        uptrend = (close > ema) & (ema > ema.shift(slope))
        downtrend = (close < ema) & (ema < ema.shift(slope))
        long_state = (same_session_prev & uptrend & (close > vwap)
                      & (close.shift(1) <= vwap.shift(1) + band.shift(1)))
        short_state = (same_session_prev & downtrend & (close < vwap)
                       & (close.shift(1) >= vwap.shift(1) - band.shift(1)))

        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_state & ~long_state.shift(1, fill_value=False)] = 1
        signals[short_state & ~short_state.shift(1, fill_value=False)] = -1
        return self._zero_warmup(signals, max(ema_p + slope, 20) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"session_start_hour": [0, 8], "ema_period": [50, 100, 200],
                "slope_bars": [5, 10, 20], "pullback_atr": [0.0, 0.25, 0.5],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


# =============================================================================
# PACKAGE M: HIGHER-TIMEFRAME LEVELS / ADR / STATISTICAL REGIME STRATEGIES
# =============================================================================


def _period_series(features: pd.DataFrame, freq: str) -> Optional[pd.Series]:
    idx = features.index
    if not isinstance(idx, pd.DatetimeIndex):
        return None
    naive_idx = idx.tz_localize(None) if idx.tz is not None else idx
    freq = str(freq).upper()
    if freq.startswith("W"):
        shifted = naive_idx - pd.Timedelta(hours=24)
        return pd.Series(shifted.to_period("W-MON"), index=idx)
    return pd.Series(naive_idx.to_period(freq), index=idx)


def _previous_period_levels(features: pd.DataFrame, freq: str) -> Tuple[pd.Series, pd.Series]:
    periods = _period_series(features, freq)
    if periods is None:
        idx = features.index
        nan = pd.Series(np.nan, index=idx, dtype=float)
        return nan, nan
    period_high = features["High"].groupby(periods).max()
    period_low = features["Low"].groupby(periods).min()
    prev_high = periods.map(period_high.shift(1)).astype(float)
    prev_low = periods.map(period_low.shift(1)).astype(float)
    return pd.Series(prev_high, index=features.index), pd.Series(prev_low, index=features.index)


def _previous_day_context(features: pd.DataFrame, adr_period: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
    idx = features.index
    if not isinstance(idx, pd.DatetimeIndex):
        nan = pd.Series(np.nan, index=idx, dtype=float)
        return nan, nan, nan
    naive_idx = idx.tz_localize(None) if idx.tz is not None else idx
    days = pd.Series(naive_idx.date, index=idx)
    daily_high = features["High"].groupby(days).max()
    daily_low = features["Low"].groupby(days).min()
    daily_range = daily_high - daily_low
    prev_high = days.map(daily_high.shift(1)).astype(float)
    prev_low = days.map(daily_low.shift(1)).astype(float)
    adr = days.map(daily_range.shift(1).rolling(max(1, int(adr_period)), min_periods=1).mean()).astype(float)
    return pd.Series(prev_high, index=idx), pd.Series(prev_low, index=idx), pd.Series(adr, index=idx)


def _variance_ratio(close: pd.Series, period: int, lag: int) -> pd.Series:
    lag = max(2, int(lag))
    period = max(lag + 2, int(period))
    ret_1 = close.diff()
    ret_lag = close - close.shift(lag)
    var_1 = ret_1.rolling(period, min_periods=period).var()
    var_lag = ret_lag.rolling(period, min_periods=period).var()
    return (var_lag / (lag * var_1.replace(0.0, np.nan) + 1e-12)).replace([np.inf, -np.inf], np.nan)


class DarvasBoxBreakoutStrategy(BaseStrategy):
    """Break compact rolling boxes; avoids broad Donchian-style channels."""

    @property
    def name(self) -> str:
        return "DarvasBoxBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"lookback": 20, "max_box_atr": 4.0, "breakout_atr": 0.1, "volume_mult": 0.0,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        lb = max(5, int(self.params.get("lookback", 20)))
        max_box = float(self.params.get("max_box_atr", 4.0))
        buf = float(self.params.get("breakout_atr", 0.1))
        vol_mult = float(self.params.get("volume_mult", 0.0))
        high, low, close = features["High"], features["Low"], features["Close"]
        atr = _get_atr(features, 14)
        box_high = high.shift(1).rolling(lb, min_periods=lb).max()
        box_low = low.shift(1).rolling(lb, min_periods=lb).min()
        compact = (box_high - box_low) <= max_box * atr
        long_entry = compact & (close > box_high + buf * atr) & (close.shift(1) <= box_high.shift(1))
        short_entry = compact & (close < box_low - buf * atr) & (close.shift(1) >= box_low.shift(1))
        if vol_mult > 0:
            vol = _get_volume(features)
            avg_vol = vol.shift(1).rolling(lb, min_periods=lb).mean()
            vol_ok = vol >= vol_mult * avg_vol
            long_entry &= vol_ok
            short_entry &= vol_ok
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_entry] = 1
        signals[short_entry] = -1
        return self._zero_warmup(signals, lb + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"lookback": [10, 20, 40], "max_box_atr": [2.5, 4.0, 6.0],
                "breakout_atr": [0.0, 0.1, 0.25], "volume_mult": [0.0, 1.25],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class ADRExpansionBreakoutStrategy(BaseStrategy):
    """Trade prior-day range breaks only when the day has room for ADR expansion."""

    @property
    def name(self) -> str:
        return "ADRExpansionBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"adr_period": 10, "adr_extension": 0.25, "atr_buffer": 0.1,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        adr_p = max(2, int(self.params.get("adr_period", 10)))
        ext = float(self.params.get("adr_extension", 0.25))
        buf = float(self.params.get("atr_buffer", 0.1))
        prev_high, prev_low, adr = _previous_day_context(features, adr_p)
        close = features["Close"]
        atr = _get_atr(features, 14)
        upper = prev_high + ext * adr + buf * atr
        lower = prev_low - ext * adr - buf * atr
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[(close > upper) & (close.shift(1) <= upper.shift(1))] = 1
        signals[(close < lower) & (close.shift(1) >= lower.shift(1))] = -1
        return self._zero_warmup(signals, adr_p + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"adr_period": [5, 10, 20], "adr_extension": [0.0, 0.25, 0.50],
                "atr_buffer": [0.0, 0.1, 0.25], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class PriorDayRangeReclaimStrategy(BaseStrategy):
    """Fade failed breaks of the prior day's high/low after close reclaims the level."""

    @property
    def name(self) -> str:
        return "PriorDayRangeReclaimStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"sweep_atr": 0.05, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        sweep = float(self.params.get("sweep_atr", 0.05))
        prev_high, prev_low, _adr = _previous_day_context(features, 2)
        atr = _get_atr(features, 14)
        high, low, close = features["High"], features["Low"], features["Close"]
        idx = features.index
        if isinstance(idx, pd.DatetimeIndex):
            naive_idx = idx.tz_localize(None) if idx.tz is not None else idx
            keys: Any = pd.Series(naive_idx.date, index=idx)
        else:
            keys = pd.Series(0, index=idx)
        long_touch = (low < prev_low - sweep * atr) & (close > prev_low)
        short_touch = (high > prev_high + sweep * atr) & (close < prev_high)
        long_once = long_touch & (long_touch.astype(int).groupby(keys).cumsum() == 1)
        short_once = short_touch & (short_touch.astype(int).groupby(keys).cumsum() == 1)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_once] = 1
        signals[short_once] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"sweep_atr": [0.0, 0.05, 0.15], **self._base_sl_tp_grid()}


class WeeklyMonthlyLevelRejectionStrategy(BaseStrategy):
    """Reject prior week/month extremes; low-frequency and spread-tolerant by design."""

    @property
    def name(self) -> str:
        return "WeeklyMonthlyLevelRejectionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"period_type": "W", "sweep_atr": 0.1, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        freq = "M" if str(self.params.get("period_type", "W")).upper().startswith("M") else "W"
        sweep = float(self.params.get("sweep_atr", 0.1))
        prev_high, prev_low = _previous_period_levels(features, freq)
        atr = _get_atr(features, 14)
        high, low, close = features["High"], features["Low"], features["Close"]
        keys = _period_series(features, freq)
        if keys is None:
            keys = pd.Series(0, index=features.index)
        long_touch = (low < prev_low - sweep * atr) & (close > prev_low)
        short_touch = (high > prev_high + sweep * atr) & (close < prev_high)
        long_once = long_touch & (long_touch.astype(int).groupby(keys).cumsum() == 1)
        short_once = short_touch & (short_touch.astype(int).groupby(keys).cumsum() == 1)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_once] = 1
        signals[short_once] = -1
        return self._zero_warmup(signals, 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"period_type": ["W", "M"], "sweep_atr": [0.0, 0.1, 0.25],
                **self._base_sl_tp_grid()}


class PreviousWeekRangeBreakoutStrategy(BaseStrategy):
    """Break prior-week high/low with ATR buffer; a slower companion to daily breakouts."""

    @property
    def name(self) -> str:
        return "PreviousWeekRangeBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"atr_buffer": 0.1, "ema_period": 100, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0,
                **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        return {"ATR_14", f"EMA_{ema_p}"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        buf = float(self.params.get("atr_buffer", 0.1))
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        prev_high, prev_low = _previous_period_levels(features, "W")
        close = features["Close"]
        atr = _get_atr(features, 14)
        ema = _get_ema(features, ema_p)
        upper = prev_high + buf * atr
        lower = prev_low - buf * atr
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[(close > upper) & (close.shift(1) <= upper.shift(1)) & (close > ema)] = 1
        signals[(close < lower) & (close.shift(1) >= lower.shift(1)) & (close < ema)] = -1
        return self._zero_warmup(signals, ema_p + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"atr_buffer": [0.0, 0.1, 0.25], "ema_period": [50, 100, 200],
                **self._base_sl_tp_grid(), **self._structural_sl_grid()}


class HeadAndShouldersNecklineBreakStrategy(BaseStrategy):
    """Approximate three-swing neckline break using confirmed pivots only."""

    @property
    def name(self) -> str:
        return "HeadAndShouldersNecklineBreakStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {"wing_bars": 2, "shoulder_tolerance_atr": 1.5, "neckline_buffer_atr": 0.1,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        wing = max(1, int(self.params.get("wing_bars", 2)))
        shoulder_tol = float(self.params.get("shoulder_tolerance_atr", 1.5))
        buf = float(self.params.get("neckline_buffer_atr", 0.1))
        high, low, close = features["High"], features["Low"], features["Close"]
        atr = _get_atr(features, 14).ffill()
        n = len(features)
        signals = pd.Series(0, index=features.index, dtype=int)
        piv_hi: List[Tuple[int, float]] = []
        piv_lo: List[Tuple[int, float]] = []
        last_short_pattern: Optional[Tuple[int, int, int]] = None
        last_long_pattern: Optional[Tuple[int, int, int]] = None

        for i in range(wing * 2 + 1, n):
            c = i - wing
            if c - wing < 0:
                continue
            h = float(high.iat[c])
            l = float(low.iat[c])
            if h >= float(high.iloc[c - wing:c].max()) and h > float(high.iloc[c + 1:i + 1].max()):
                piv_hi.append((c, h))
                piv_hi = piv_hi[-6:]
            if l <= float(low.iloc[c - wing:c].min()) and l < float(low.iloc[c + 1:i + 1].min()):
                piv_lo.append((c, l))
                piv_lo = piv_lo[-6:]

            if len(piv_hi) >= 3:
                (a_i, a), (b_i, b), (c_i, c_hi) = piv_hi[-3:]
                tol = shoulder_tol * float(atr.iat[i] if pd.notna(atr.iat[i]) else 0.0)
                if a_i < b_i < c_i and b > a and b > c_hi and abs(a - c_hi) <= tol:
                    neckline = float(low.iloc[a_i:c_i + 1].min())
                    pattern = (a_i, b_i, c_i)
                    if (last_short_pattern != pattern and close.iat[i] < neckline - buf * atr.iat[i]
                            and close.iat[i - 1] >= neckline - buf * atr.iat[i - 1]):
                        signals.iat[i] = -1
                        last_short_pattern = pattern

            if len(piv_lo) >= 3:
                (a_i, a), (b_i, b), (c_i, c_lo) = piv_lo[-3:]
                tol = shoulder_tol * float(atr.iat[i] if pd.notna(atr.iat[i]) else 0.0)
                if a_i < b_i < c_i and b < a and b < c_lo and abs(a - c_lo) <= tol:
                    neckline = float(high.iloc[a_i:c_i + 1].max())
                    pattern = (a_i, b_i, c_i)
                    if (last_long_pattern != pattern and close.iat[i] > neckline + buf * atr.iat[i]
                            and close.iat[i - 1] <= neckline + buf * atr.iat[i - 1]):
                        signals.iat[i] = 1
                        last_long_pattern = pattern

        return self._zero_warmup(signals, wing * 2 + 20)

    def get_param_grid(self) -> Dict[str, List]:
        return {"wing_bars": [1, 2, 3], "shoulder_tolerance_atr": [1.0, 1.5, 2.5],
                "neckline_buffer_atr": [0.0, 0.1, 0.25], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class VarianceRatioTrendStrategy(BaseStrategy):
    """Trend event when variance ratio indicates persistence and EMA direction agrees."""

    @property
    def name(self) -> str:
        return "VarianceRatioTrendStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self) -> Dict[str, Any]:
        return {"vr_period": 40, "lag": 4, "min_vr": 1.15, "ema_period": 100,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, **self._structural_sl_defaults()}

    def get_required_features(self) -> Set[str]:
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        return {"ATR_14", f"EMA_{ema_p}"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = max(5, int(self.params.get("vr_period", 40)))
        lag = max(2, int(self.params.get("lag", 4)))
        min_vr = float(self.params.get("min_vr", 1.15))
        ema_p = max(2, int(self.params.get("ema_period", 100)))
        close = features["Close"]
        vr = _variance_ratio(close, period, lag)
        ema = _get_ema(features, ema_p)
        long_state = (vr >= min_vr) & (close > ema)
        short_state = (vr >= min_vr) & (close < ema)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_state & ~long_state.shift(1, fill_value=False)] = 1
        signals[short_state & ~short_state.shift(1, fill_value=False)] = -1
        return self._zero_warmup(signals, max(period + lag, ema_p) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"vr_period": [20, 40, 80], "lag": [2, 4, 8], "min_vr": [1.05, 1.15, 1.30],
                "ema_period": [50, 100, 200], **self._base_sl_tp_grid(),
                **self._structural_sl_grid()}


class AutocorrelationReversalStrategy(BaseStrategy):
    """Fade return extremes when short-lag autocorrelation is negative."""

    @property
    def name(self) -> str:
        return "AutocorrelationReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {"corr_period": 30, "lag": 1, "max_autocorr": -0.10, "z_period": 20, "z_entry": 1.5,
                "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        corr_p = max(5, int(self.params.get("corr_period", 30)))
        lag = max(1, int(self.params.get("lag", 1)))
        max_ac = float(self.params.get("max_autocorr", -0.10))
        z_p = max(5, int(self.params.get("z_period", 20)))
        z_entry = float(self.params.get("z_entry", 1.5))
        close = features["Close"]
        ret = close.pct_change()
        autocorr = ret.rolling(corr_p, min_periods=corr_p).corr(ret.shift(lag))
        mean = ret.rolling(z_p, min_periods=z_p).mean()
        std = ret.rolling(z_p, min_periods=z_p).std().replace(0.0, np.nan)
        z = (ret - mean) / (std + 1e-12)
        mr_state = autocorr <= max_ac
        long_state = mr_state & (z <= -z_entry)
        short_state = mr_state & (z >= z_entry)
        signals = pd.Series(0, index=features.index, dtype=int)
        signals[long_state & ~long_state.shift(1, fill_value=False)] = 1
        signals[short_state & ~short_state.shift(1, fill_value=False)] = -1
        return self._zero_warmup(signals, max(corr_p + lag, z_p) + 2)

    def get_param_grid(self) -> Dict[str, List]:
        return {"corr_period": [20, 30, 50], "lag": [1, 2], "max_autocorr": [-0.20, -0.10, 0.0],
                "z_period": [10, 20, 40], "z_entry": [1.0, 1.5, 2.0],
                **self._base_sl_tp_grid()}


# =============================================================================
# STRATEGY REGISTRY
# =============================================================================

class StrategyRegistry:
    """
    Central registry for all available strategies.
    
    Provides:
    - Strategy discovery and listing
    - Strategy instantiation by name
    - Category-based filtering
    """
    
    # All available strategies
    _strategies: Dict[str, type] = {
        # Trend Following
        'EMACrossoverStrategy': EMACrossoverStrategy,
        'SupertrendStrategy': SupertrendStrategy,
        'MACDTrendStrategy': MACDTrendStrategy,
        'ADXTrendStrategy': ADXTrendStrategy,
        'IchimokuStrategy': IchimokuStrategy,
        'HullMATrendStrategy': HullMATrendStrategy,
        'EMARibbonADXStrategy': EMARibbonADXStrategy,
        'AroonTrendStrategy': AroonTrendStrategy,
        'ADXDIStrengthStrategy': ADXDIStrengthStrategy,
        'KeltnerPullbackStrategy': KeltnerPullbackStrategy,
        
        # Mean Reversion
        'RSIExtremesStrategy': RSIExtremesStrategy,
        'BollingerBounceStrategy': BollingerBounceStrategy,
        'ZScoreMRStrategy': ZScoreMRStrategy,
        'StochasticReversalStrategy': StochasticReversalStrategy,
        'CCIReversalStrategy': CCIReversalStrategy,
        'WilliamsRStrategy': WilliamsRStrategy,
        'RSITrendFilteredMRStrategy': RSITrendFilteredMRStrategy,
        'StochRSITrendGateStrategy': StochRSITrendGateStrategy,
        'FisherTransformMRStrategy': FisherTransformMRStrategy,
        'ZScoreVWAPReversionStrategy': ZScoreVWAPReversionStrategy,
        'SessionVWAPReversionStrategy': SessionVWAPReversionStrategy,
        'LiquiditySweepReversalStrategy': LiquiditySweepReversalStrategy,
        
        # Breakout/Momentum
        'DonchianBreakoutStrategy': DonchianBreakoutStrategy,
        'VolatilityBreakoutStrategy': VolatilityBreakoutStrategy,
        'MomentumBurstStrategy': MomentumBurstStrategy,
        'SqueezeBreakoutStrategy': SqueezeBreakoutStrategy,
        'KeltnerBreakoutStrategy': KeltnerBreakoutStrategy,
        'PivotBreakoutStrategy': PivotBreakoutStrategy,
        'MACDHistogramMomentumStrategy': MACDHistogramMomentumStrategy,
        'RegimeTransitionBreakoutStrategy': RegimeTransitionBreakoutStrategy,

        # Additional strategies
        # Breakout
        'InsideBarBreakoutStrategy': InsideBarBreakoutStrategy,
        'NarrowRangeBreakoutStrategy': NarrowRangeBreakoutStrategy,
        'VolumeSpikeMomentumStrategy': VolumeSpikeMomentumStrategy,
        'ATRPercentileBreakoutStrategy': ATRPercentileBreakoutStrategy,
        # Mean Reversion
        'TurtleSoupReversalStrategy': TurtleSoupReversalStrategy,
        'PinBarReversalStrategy': PinBarReversalStrategy,
        'EngulfingPatternStrategy': EngulfingPatternStrategy,
        'RSIDivergenceStrategy': RSIDivergenceStrategy,
        'MACDDivergenceStrategy': MACDDivergenceStrategy,
        'KeltnerFadeStrategy': KeltnerFadeStrategy,
        'ROCExhaustionReversalStrategy': ROCExhaustionReversalStrategy,
        # Trend Following
        'OBVDivergenceStrategy': OBVDivergenceStrategy,
        'EMAPullbackContinuationStrategy': EMAPullbackContinuationStrategy,
        'ParabolicSARTrendStrategy': ParabolicSARTrendStrategy,
        'KaufmanAMATrendStrategy': KaufmanAMATrendStrategy,

        # Package G7 strategies
        'VortexTrendStrategy': VortexTrendStrategy,
        'TRIXSignalStrategy': TRIXSignalStrategy,
        'RelativeVigorIndexStrategy': RelativeVigorIndexStrategy,
        'VIDYABandTrendStrategy': VIDYABandTrendStrategy,
        'ChoppinessCompressionBreakoutStrategy': ChoppinessCompressionBreakoutStrategy,

        # Package H strategies: session, gap, value-continuation, and smoother trend surfaces
        'OpeningRangeBreakoutStrategy': OpeningRangeBreakoutStrategy,
        'GapContinuationStrategy': GapContinuationStrategy,
        'VWAPTrendContinuationStrategy': VWAPTrendContinuationStrategy,
        'RSI2TrendPullbackStrategy': RSI2TrendPullbackStrategy,
        'DoubleEMATrendStrategy': DoubleEMATrendStrategy,
        'HeikinAshiTrendStrategy': HeikinAshiTrendStrategy,
        'BollingerBandWalkStrategy': BollingerBandWalkStrategy,
        'PercentileChannelReversionStrategy': PercentileChannelReversionStrategy,

        # Package I strategies: conventional candlestick and multi-candle patterns
        'MorningStarEveningStarStrategy': MorningStarEveningStarStrategy,
        'PiercingDarkCloudStrategy': PiercingDarkCloudStrategy,
        'HaramiReversalStrategy': HaramiReversalStrategy,
        'TweezerReversalStrategy': TweezerReversalStrategy,
        'ThreeBarReversalStrategy': ThreeBarReversalStrategy,
        'DojiBreakoutStrategy': DojiBreakoutStrategy,
        'MarubozuMomentumStrategy': MarubozuMomentumStrategy,
        'ThreeSoldiersCrowsStrategy': ThreeSoldiersCrowsStrategy,

        # Package J strategies: structure, session fade, and money-flow surfaces
        'DoubleTopBottomReversalStrategy': DoubleTopBottomReversalStrategy,
        'TriangleCompressionBreakoutStrategy': TriangleCompressionBreakoutStrategy,
        'FlagPullbackContinuationStrategy': FlagPullbackContinuationStrategy,
        'GapFadeStrategy': GapFadeStrategy,
        'OpeningDriveFailureStrategy': OpeningDriveFailureStrategy,
        'PreviousSessionLevelRejectionStrategy': PreviousSessionLevelRejectionStrategy,
        'SupportResistanceRetestBreakoutStrategy': SupportResistanceRetestBreakoutStrategy,
        'MoneyFlowIndexReversalStrategy': MoneyFlowIndexReversalStrategy,
        'ChaikinMoneyFlowTrendStrategy': ChaikinMoneyFlowTrendStrategy,
        'AccumulationDistributionBreakoutStrategy': AccumulationDistributionBreakoutStrategy,

        # Package K strategies: time/session/calendar and carry-aware surfaces
        'TimeOfDayMomentumStrategy': TimeOfDayMomentumStrategy,
        'SessionCloseReversionStrategy': SessionCloseReversionStrategy,
        'DayOfWeekBiasStrategy': DayOfWeekBiasStrategy,
        'TurnOfMonthStrategy': TurnOfMonthStrategy,
        'OvernightDriftStrategy': OvernightDriftStrategy,
        'IntradayRangeFadeStrategy': IntradayRangeFadeStrategy,
        'SwapCarryTrendStrategy': SwapCarryTrendStrategy,
        'CarryFilteredPullbackStrategy': CarryFilteredPullbackStrategy,

        # Package L strategies: structure, VWAP, volume climax, and efficiency-ratio surfaces
        'FractalChannelBreakoutStrategy': FractalChannelBreakoutStrategy,
        'SwingFailureReversalStrategy': SwingFailureReversalStrategy,
        'AnchoredVWAPReclaimStrategy': AnchoredVWAPReclaimStrategy,
        'VolumeClimaxReversalStrategy': VolumeClimaxReversalStrategy,
        'EfficiencyRatioTrendStrategy': EfficiencyRatioTrendStrategy,
        'EfficiencyRatioMeanReversionStrategy': EfficiencyRatioMeanReversionStrategy,
        'OpeningRangeRetestStrategy': OpeningRangeRetestStrategy,
        'SessionVWAPPullbackTrendStrategy': SessionVWAPPullbackTrendStrategy,

        # Package M strategies: HTF levels, ADR, and deterministic statistical regimes
        'DarvasBoxBreakoutStrategy': DarvasBoxBreakoutStrategy,
        'ADRExpansionBreakoutStrategy': ADRExpansionBreakoutStrategy,
        'PriorDayRangeReclaimStrategy': PriorDayRangeReclaimStrategy,
        'WeeklyMonthlyLevelRejectionStrategy': WeeklyMonthlyLevelRejectionStrategy,
        'PreviousWeekRangeBreakoutStrategy': PreviousWeekRangeBreakoutStrategy,
        'HeadAndShouldersNecklineBreakStrategy': HeadAndShouldersNecklineBreakStrategy,
        'VarianceRatioTrendStrategy': VarianceRatioTrendStrategy,
        'AutocorrelationReversalStrategy': AutocorrelationReversalStrategy,
    }
    
    @classmethod
    def get(cls, name: str, **params) -> BaseStrategy:
        """Get strategy instance by name."""
        resolved_name = name
        if resolved_name not in cls._strategies:
            resolved_name = _STRATEGY_MIGRATION.get(resolved_name, resolved_name)
        if resolved_name not in cls._strategies:
            raise ValueError(f"Unknown strategy: {name}")
        return cls._strategies[resolved_name](**params)
    
    @classmethod
    def list_all(cls) -> List[str]:
        """List all available strategy names."""
        return list(cls._strategies.keys())
    
    @classmethod
    def list_by_category(cls, category: StrategyCategory) -> List[str]:
        """List strategies in a specific category."""
        result = []
        for name, strategy_cls in cls._strategies.items():
            instance = strategy_cls()
            if instance.category == category:
                result.append(name)
        return result
    
    @classmethod
    def register(cls, strategy_cls: type):
        """Register a new strategy class."""
        instance = strategy_cls()
        cls._strategies[instance.name] = strategy_cls
    
    @classmethod
    def get_all_instances(cls) -> List[BaseStrategy]:
        """Get instances of all strategies with default params."""
        return [strategy_cls() for strategy_cls in cls._strategies.values()]
    
    @classmethod
    def count(cls) -> int:
        """Get total number of registered strategies."""
        return len(cls._strategies)


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'BaseStrategy',
    'StrategyRegistry',
    'StrategyCategory',
    # Global grid constants
    '_GLOBAL_SL_GRID',
    '_GLOBAL_TP_GRID',
    # Trend Following
    'EMACrossoverStrategy',
    'SupertrendStrategy',
    'MACDTrendStrategy',
    'ADXTrendStrategy',
    'IchimokuStrategy',
    'HullMATrendStrategy',
    'EMARibbonADXStrategy',
    'AroonTrendStrategy',
    'ADXDIStrengthStrategy',
    'KeltnerPullbackStrategy',
    # Mean Reversion
    'RSIExtremesStrategy',
    'BollingerBounceStrategy',
    'ZScoreMRStrategy',
    'StochasticReversalStrategy',
    'CCIReversalStrategy',
    'WilliamsRStrategy',
    'RSITrendFilteredMRStrategy',
    'StochRSITrendGateStrategy',
    'FisherTransformMRStrategy',
    'ZScoreVWAPReversionStrategy',
    'SessionVWAPReversionStrategy',
    'LiquiditySweepReversalStrategy',
    # Breakout/Momentum
    'DonchianBreakoutStrategy',
    'VolatilityBreakoutStrategy',
    'MomentumBurstStrategy',
    'SqueezeBreakoutStrategy',
    'KeltnerBreakoutStrategy',
    'PivotBreakoutStrategy',
    'MACDHistogramMomentumStrategy',
    'RegimeTransitionBreakoutStrategy',
    # Additional strategies
    'InsideBarBreakoutStrategy',
    'NarrowRangeBreakoutStrategy',
    'TurtleSoupReversalStrategy',
    'PinBarReversalStrategy',
    'EngulfingPatternStrategy',
    'VolumeSpikeMomentumStrategy',
    'RSIDivergenceStrategy',
    'MACDDivergenceStrategy',
    'OBVDivergenceStrategy',
    'KeltnerFadeStrategy',
    'ROCExhaustionReversalStrategy',
    'EMAPullbackContinuationStrategy',
    'ParabolicSARTrendStrategy',
    'ATRPercentileBreakoutStrategy',
    'KaufmanAMATrendStrategy',
    # Package G7
    'VortexTrendStrategy',
    'TRIXSignalStrategy',
    'RelativeVigorIndexStrategy',
    'VIDYABandTrendStrategy',
    'ChoppinessCompressionBreakoutStrategy',
    # Package H
    'OpeningRangeBreakoutStrategy',
    'GapContinuationStrategy',
    'VWAPTrendContinuationStrategy',
    'RSI2TrendPullbackStrategy',
    'DoubleEMATrendStrategy',
    'HeikinAshiTrendStrategy',
    'BollingerBandWalkStrategy',
    'PercentileChannelReversionStrategy',
    # Package I
    'MorningStarEveningStarStrategy',
    'PiercingDarkCloudStrategy',
    'HaramiReversalStrategy',
    'TweezerReversalStrategy',
    'ThreeBarReversalStrategy',
    'DojiBreakoutStrategy',
    'MarubozuMomentumStrategy',
    'ThreeSoldiersCrowsStrategy',
    # Package J
    'DoubleTopBottomReversalStrategy',
    'TriangleCompressionBreakoutStrategy',
    'FlagPullbackContinuationStrategy',
    'GapFadeStrategy',
    'OpeningDriveFailureStrategy',
    'PreviousSessionLevelRejectionStrategy',
    'SupportResistanceRetestBreakoutStrategy',
    'MoneyFlowIndexReversalStrategy',
    'ChaikinMoneyFlowTrendStrategy',
    'AccumulationDistributionBreakoutStrategy',
    # Package K
    'TimeOfDayMomentumStrategy',
    'SessionCloseReversionStrategy',
    'DayOfWeekBiasStrategy',
    'TurnOfMonthStrategy',
    'OvernightDriftStrategy',
    'IntradayRangeFadeStrategy',
    'SwapCarryTrendStrategy',
    'CarryFilteredPullbackStrategy',
    # Package L
    'FractalChannelBreakoutStrategy',
    'SwingFailureReversalStrategy',
    'AnchoredVWAPReclaimStrategy',
    'VolumeClimaxReversalStrategy',
    'EfficiencyRatioTrendStrategy',
    'EfficiencyRatioMeanReversionStrategy',
    'OpeningRangeRetestStrategy',
    'SessionVWAPPullbackTrendStrategy',
    # Package M
    'DarvasBoxBreakoutStrategy',
    'ADRExpansionBreakoutStrategy',
    'PriorDayRangeReclaimStrategy',
    'WeeklyMonthlyLevelRejectionStrategy',
    'PreviousWeekRangeBreakoutStrategy',
    'HeadAndShouldersNecklineBreakStrategy',
    'VarianceRatioTrendStrategy',
    'AutocorrelationReversalStrategy',
]

# Migration map for retired strategy names → current names
_STRATEGY_MIGRATION = {
    'VWAPDeviationReversionStrategy': 'ZScoreVWAPReversionStrategy',
    'VWAPDeviationReversalStrategy': 'ZScoreVWAPReversionStrategy',
}


if __name__ == "__main__":
    # Test strategy registry
    print(f"Total strategies: {StrategyRegistry.count()}")
    print(f"\nAll strategies: {StrategyRegistry.list_all()}")
    
    print(f"\nTrend Following: {StrategyRegistry.list_by_category(StrategyCategory.TREND_FOLLOWING)}")
    print(f"Mean Reversion: {StrategyRegistry.list_by_category(StrategyCategory.MEAN_REVERSION)}")
    print(f"Breakout: {StrategyRegistry.list_by_category(StrategyCategory.BREAKOUT_MOMENTUM)}")
    
    # Test instantiation
    st = StrategyRegistry.get('SupertrendStrategy', atr_period=7, multiplier=2.0)
    print(f"\nInstantiated: {st}")
    print(f"Param grid: {st.get_param_grid()}")

    # Verify all strategies have standardized SL/TP grids
    print("\n--- SL/TP Grid Verification ---")
    expected_sl = _GLOBAL_SL_GRID
    expected_tp = _GLOBAL_TP_GRID
    all_ok = True
    for name in StrategyRegistry.list_all():
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()
        sl = grid.get('sl_atr_mult', [])
        tp = grid.get('tp_atr_mult', [])
        if sl != expected_sl or tp != expected_tp:
            print(f"  MISMATCH: {name} - sl={sl}, tp={tp}")
            all_ok = False
    if all_ok:
        print("  All strategies have standardized SL/TP grids ✓")
