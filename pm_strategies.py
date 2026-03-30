"""
FX Portfolio Manager - Trading Strategies
==========================================

Contains all trading strategy implementations.
Includes 47 strategies across categories:
- Trend Following (18 strategies)
- Mean Reversion (17 strategies)
- Breakout/Momentum (12 strategies)

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

import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Tuple, Optional, Set

from pm_core import StrategyCategory, get_instrument_spec, InstrumentSpec, FeatureComputer


# =============================================================================
# GLOBAL SL/TP PARAM GRID — single source of truth for all strategies
# =============================================================================

_GLOBAL_SL_GRID: List[float] = list(np.arange(1.5, 3.5, 0.5))   # [1.5, 2.0, 2.5, 3.0]
_GLOBAL_TP_GRID: List[float] = list(np.arange(1.0, 6.5, 0.5))   # [1.0 .. 6.0]


# =============================================================================
# FEATURE LOOKUP HELPERS (efficiency optimization)
# =============================================================================

def _get_ema(features: pd.DataFrame, period: int) -> pd.Series:
    """Get EMA from precomputed features or compute if missing (C3: memoize)."""
    col = f'EMA_{period}'
    if col in features.columns:
        return features[col]
    result = features['Close'].ewm(span=period, adjust=False).mean()
    features.loc[:, col] = result
    return result


def _get_sma(features: pd.DataFrame, period: int) -> pd.Series:
    """Get SMA from precomputed features or compute if missing (C3: memoize)."""
    col = f'SMA_{period}'
    if col in features.columns:
        return features[col]
    result = features['Close'].rolling(period).mean()
    features.loc[:, col] = result
    return result


def _get_tr(features: pd.DataFrame) -> pd.Series:
    """Get True Range (C2: consolidated helper). Memoized into features."""
    col = '_TR'
    if col in features.columns:
        return features[col]
    high_low = features['High'] - features['Low']
    high_close = (features['High'] - features['Close'].shift(1)).abs()
    low_close = (features['Low'] - features['Close'].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    features.loc[:, col] = tr
    return tr


def _get_atr(features: pd.DataFrame, period: int) -> pd.Series:
    """Get ATR from precomputed features or compute if missing (C3: memoize)."""
    col = f'ATR_{period}'
    if col in features.columns:
        return features[col]
    tr = _get_tr(features)
    result = tr.rolling(period).mean()
    features.loc[:, col] = result
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


def _get_rsi(features: pd.DataFrame, period: int) -> pd.Series:
    """Get RSI from precomputed features or compute if missing (C3: memoize)."""
    col = f'RSI_{period}'
    if col in features.columns:
        return features[col]
    result = FeatureComputer.rsi(features['Close'], period)
    features.loc[:, col] = result
    return result


def _get_keltner(features: pd.DataFrame, ema_period: int = 20,
                 atr_period: int = 14, mult: float = 2.0
                 ) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get Keltner Channel (mid, upper, lower) with parameter-aware caching."""
    if ema_period == 20 and atr_period == 20 and float(mult) == 2.0:
        if {'KC_MID', 'KC_UPPER', 'KC_LOWER'}.issubset(features.columns):
            return features['KC_MID'], features['KC_UPPER'], features['KC_LOWER']
        mid = _get_ema(features, ema_period)
        atr = _get_atr(features, atr_period)
        upper = mid + mult * atr
        lower = mid - mult * atr
        features.loc[:, 'KC_MID'] = mid
        features.loc[:, 'KC_UPPER'] = upper
        features.loc[:, 'KC_LOWER'] = lower
        return mid, upper, lower

    cache_tag = _cache_tag(ema_period, atr_period, mult)
    mid_col = f'_KC_MID_{cache_tag}'
    upper_col = f'_KC_UPPER_{cache_tag}'
    lower_col = f'_KC_LOWER_{cache_tag}'
    if {mid_col, upper_col, lower_col}.issubset(features.columns):
        return features[mid_col], features[upper_col], features[lower_col]

    mid = _get_ema(features, ema_period)
    atr = _get_atr(features, atr_period)
    upper = mid + mult * atr
    lower = mid - mult * atr
    features.loc[:, mid_col] = mid
    features.loc[:, upper_col] = upper
    features.loc[:, lower_col] = lower
    return mid, upper, lower


def _detect_swing_points(series: pd.Series, order: int = 5
                         ) -> Tuple[pd.Series, pd.Series]:
    """Detect swing highs and lows using rolling window comparison (D0 helper).

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
    """Rolling percentile rank (0-100) of current value within its window (D0 helper)."""
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
    if period == 14 and {'ADX', 'PLUS_DI', 'MINUS_DI'}.issubset(features.columns):
        return features['ADX'], features['PLUS_DI'], features['MINUS_DI']

    cache_tag = _cache_tag(period)
    adx_col = f'_ADX_{cache_tag}'
    pdi_col = f'_PLUS_DI_{cache_tag}'
    mdi_col = f'_MINUS_DI_{cache_tag}'

    # Return cached if available
    if {adx_col, pdi_col, mdi_col}.issubset(features.columns):
        return features[adx_col], features[pdi_col], features[mdi_col]

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

    features.loc[:, adx_col] = adx
    features.loc[:, pdi_col] = plus_di
    features.loc[:, mdi_col] = minus_di
    return adx, plus_di, minus_di


def _get_hull_ma(features: pd.DataFrame, period: int) -> pd.Series:
    """Get Hull MA from precomputed features or compute if missing (C3: memoize)."""
    col = f'HULL_MA_{period}' if period != 20 else 'HULL_MA'
    if col in features.columns:
        return features[col]
    result = FeatureComputer.hull_ma(features['Close'], period)
    features.loc[:, col] = result
    return result


def _get_bb(features: pd.DataFrame, period: int, std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get Bollinger Bands from precomputed features or compute if missing."""
    if period == 20 and float(std) == 2.0 and {'BB_MID_20', 'BB_UPPER_20', 'BB_LOWER_20'}.issubset(features.columns):
        return features['BB_MID_20'], features['BB_UPPER_20'], features['BB_LOWER_20']

    cache_tag = _cache_tag(period, std)
    mid_col = f'_BB_MID_{cache_tag}'
    upper_col = f'_BB_UPPER_{cache_tag}'
    lower_col = f'_BB_LOWER_{cache_tag}'
    if {mid_col, upper_col, lower_col}.issubset(features.columns):
        return features[mid_col], features[upper_col], features[lower_col]

    mid, upper, lower = FeatureComputer.bollinger_bands(features['Close'], period, std)
    if period == 20 and float(std) == 2.0:
        features.loc[:, 'BB_MID_20'] = mid
        features.loc[:, 'BB_UPPER_20'] = upper
        features.loc[:, 'BB_LOWER_20'] = lower
    else:
        features.loc[:, mid_col] = mid
        features.loc[:, upper_col] = upper
        features.loc[:, lower_col] = lower
    return mid, upper, lower


def _get_stochastic(features: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
    """Get Stochastic from precomputed features or compute if missing."""
    if 'STOCH_K' in features.columns and 'STOCH_D' in features.columns and k_period == 14 and d_period == 3:
        return features['STOCH_K'], features['STOCH_D']
    cache_tag = _cache_tag(k_period, d_period)
    k_col = f'_STOCH_K_{cache_tag}'
    d_col = f'_STOCH_D_{cache_tag}'
    if {k_col, d_col}.issubset(features.columns):
        return features[k_col], features[d_col]
    low_min = features['Low'].rolling(window=k_period).min()
    high_max = features['High'].rolling(window=k_period).max()
    denom = high_max - low_min
    k = pd.Series(np.nan, index=features.index)
    valid = denom > 1e-10
    k[valid] = 100.0 * (features['Close'][valid] - low_min[valid]) / denom[valid]
    d = k.rolling(window=d_period).mean()
    if k_period == 14 and d_period == 3:
        features.loc[:, 'STOCH_K'] = k
        features.loc[:, 'STOCH_D'] = d
    else:
        features.loc[:, k_col] = k
        features.loc[:, d_col] = d
    return k, d


def _get_macd(features: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get MACD from precomputed features or compute if missing."""
    if 'MACD' in features.columns and fast == 12 and slow == 26 and signal == 9:
        return features['MACD'], features['MACD_SIGNAL'], features['MACD_HIST']
    return FeatureComputer.macd(features['Close'], fast, slow, signal)


# =============================================================================
# BASE STRATEGY CLASS
# =============================================================================

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
        tp_mult = self.params.get('tp_atr_mult', 3.0)
        
        sl_pips = max(5.0, atr_pips * sl_mult)
        tp_pips = max(10.0, atr_pips * tp_mult)
        
        return sl_pips, tp_pips
    
    def get_param_grid(self) -> Dict[str, List]:
        """Get parameter grid for optimization."""
        return {}
    
    @staticmethod
    def _base_sl_tp_grid() -> Dict[str, List[float]]:
        """Return the global SL/TP grid entries for use in get_param_grid()."""
        return {
            'sl_atr_mult': _GLOBAL_SL_GRID,
            'tp_atr_mult': _GLOBAL_TP_GRID,
        }
    
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
            'tp_atr_mult': 3.0
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
            'sl_atr_mult': 1.5,
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0,
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
            'tp_atr_mult': 3.0,
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('adx_period', 14))
        adx_th = float(self.params.get('adx_threshold', 20))
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
            'tp_atr_mult': 3.0,
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
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('rsi_period', 14)
        oversold = self.params.get('oversold', 30)
        overbought = self.params.get('overbought', 70)

        rsi = _get_rsi(features, period)
        
        signals = pd.Series(0, index=features.index)
        buy_signal = (rsi > oversold) & (rsi.shift(1) <= oversold)
        sell_signal = (rsi < overbought) & (rsi.shift(1) >= overbought)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'rsi_period': [5, 7, 10, 14, 21],
            'oversold': [15, 20, 25, 30],
            'overbought': [70, 75, 80, 85],
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
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        k_period = self.params.get('k_period', 14)
        d_period = self.params.get('d_period', 3)
        oversold = self.params.get('oversold', 20)
        overbought = self.params.get('overbought', 80)
        
        low_min = features['Low'].rolling(k_period).min()
        high_max = features['High'].rolling(k_period).max()
        denom = high_max - low_min
        stoch_k = pd.Series(50.0, index=features.index)
        valid = denom > 1e-8
        stoch_k[valid] = 100 * (features['Close'][valid] - low_min[valid]) / denom[valid]
        stoch_d = stoch_k.rolling(d_period).mean()
        
        signals = pd.Series(0, index=features.index)
        # %K crosses above %D in oversold zone
        buy_signal = (stoch_k > stoch_d) & (stoch_k.shift(1) <= stoch_d.shift(1)) & (stoch_k < oversold + 10)
        # %K crosses below %D in overbought zone
        sell_signal = (stoch_k < stoch_d) & (stoch_k.shift(1) >= stoch_d.shift(1)) & (stoch_k > overbought - 10)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'k_period': [5, 9, 14, 21],
            'd_period': [3, 5, 7],
            'oversold': [10, 15, 20, 25],
            'overbought': [75, 80, 85, 90],
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
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('cci_period', 20)
        oversold = self.params.get('oversold', -100)
        overbought = self.params.get('overbought', 100)
        
        tp = (features['High'] + features['Low'] + features['Close']) / 3
        sma = tp.rolling(period).mean()
        mad = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean())
        cci = (tp - sma) / (0.015 * mad + 1e-10)
        
        signals = pd.Series(0, index=features.index)
        buy_signal = (cci > oversold) & (cci.shift(1) <= oversold)
        sell_signal = (cci < overbought) & (cci.shift(1) >= overbought)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'cci_period': [10, 14, 20, 30],
            'oversold': [-150, -125, -100, -80],
            'overbought': [80, 100, 125, 150],
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
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('period', 14)
        oversold = self.params.get('oversold', -80)
        overbought = self.params.get('overbought', -20)
        
        high_max = features['High'].rolling(period).max()
        low_min = features['Low'].rolling(period).min()
        denom = high_max - low_min
        willr = pd.Series(-50.0, index=features.index)
        valid = denom > 1e-8
        willr[valid] = -100 * (high_max[valid] - features['Close'][valid]) / denom[valid]
        
        signals = pd.Series(0, index=features.index)
        buy_signal = (willr > oversold) & (willr.shift(1) <= oversold)
        sell_signal = (willr < overbought) & (willr.shift(1) >= overbought)
        
        signals[buy_signal] = 1
        signals[sell_signal] = -1
        
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [7, 10, 14, 21, 28],
            'oversold': [-95, -90, -85, -80],
            'overbought': [-20, -15, -10, -5],
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
            'tp_atr_mult': 3.0
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
            'sl_atr_mult': 1.5,
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0
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
            'tp_atr_mult': 3.0,
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
            "tp_atr_mult": 3.0,
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
            "tp_atr_mult": 3.0,
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


# VWAPDeviationReversionStrategy retired — near-duplicate of ZScoreVWAPReversionStrategy.
# Existing configs are migrated to ZScoreVWAPReversionStrategy on load.


# =============================================================================
# NEW STRATEGIES (D1-D15)
# =============================================================================


class InsideBarBreakoutStrategy(BaseStrategy):
    """Inside bar breakout: mother bar engulfs child bar(s), breakout of mother range."""

    @property
    def name(self) -> str:
        return "InsideBarBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {'min_inside_bars': 1, 'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
        return {'min_inside_bars': [1, 2, 3], **self._base_sl_tp_grid()}


class NarrowRangeBreakoutStrategy(BaseStrategy):
    """Narrow range breakout: bar range is narrowest in N bars, trade breakout."""

    @property
    def name(self) -> str:
        return "NarrowRangeBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self) -> Dict[str, Any]:
        return {'nr_lookback': 7, 'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
        return {'nr_lookback': [4, 7, 10], **self._base_sl_tp_grid()}


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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.5}

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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
                'vol_lookback': [10, 20, 30], **self._base_sl_tp_grid()}


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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.5}

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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.5}

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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 2.5}

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
                **self._base_sl_tp_grid()}


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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
                'touch_atr': [0.3, 0.5, 0.7], **self._base_sl_tp_grid()}


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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
                'adx_threshold': [15, 20, 25], **self._base_sl_tp_grid()}


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
                'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
                'expand_pct': [60, 70, 80], **self._base_sl_tp_grid()}


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
                'signal_mode': 'direction', 'sl_atr_mult': 2.0, 'tp_atr_mult': 3.0}

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
                **self._base_sl_tp_grid()}


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
            'tp_atr_mult': 3.0,
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
            'tp_atr_mult': 3.0,
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
            'tp_atr_mult': 3.0,
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
            'tp_atr_mult': 3.0,
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
            'tp_atr_mult': 3.0,
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

        # Choppiness Index
        ci = 100.0 * np.log10(atr_sum / (hl_range + 1e-10)) / np.log10(ci_p)

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
        }


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
        
        # Breakout/Momentum
        'DonchianBreakoutStrategy': DonchianBreakoutStrategy,
        'VolatilityBreakoutStrategy': VolatilityBreakoutStrategy,
        'MomentumBurstStrategy': MomentumBurstStrategy,
        'SqueezeBreakoutStrategy': SqueezeBreakoutStrategy,
        'KeltnerBreakoutStrategy': KeltnerBreakoutStrategy,
        'PivotBreakoutStrategy': PivotBreakoutStrategy,
        'MACDHistogramMomentumStrategy': MACDHistogramMomentumStrategy,

        # New strategies (D1-D15)
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
    }
    
    @classmethod
    def get(cls, name: str, **params) -> BaseStrategy:
        """Get strategy instance by name."""
        if name not in cls._strategies:
            raise ValueError(f"Unknown strategy: {name}")
        return cls._strategies[name](**params)
    
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
    # Breakout/Momentum
    'DonchianBreakoutStrategy',
    'VolatilityBreakoutStrategy',
    'MomentumBurstStrategy',
    'SqueezeBreakoutStrategy',
    'KeltnerBreakoutStrategy',
    'PivotBreakoutStrategy',
    'MACDHistogramMomentumStrategy',
    # New strategies (D1-D15)
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
]

# Migration map for retired strategy names → current names
_STRATEGY_MIGRATION = {
    'VWAPDeviationReversionStrategy': 'ZScoreVWAPReversionStrategy',
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
