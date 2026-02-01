"""
FX Portfolio Manager - Trading Strategies
==========================================

Contains all trading strategy implementations.
Includes 28 strategies across categories:
- Trend Following (10 strategies)
- Mean Reversion (10 strategies)
- Breakout/Momentum (8 strategies)

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
    """Get EMA from precomputed features or compute if missing."""
    col = f'EMA_{period}'
    if col in features.columns:
        return features[col]
    return features['Close'].ewm(span=period, adjust=False).mean()


def _get_sma(features: pd.DataFrame, period: int) -> pd.Series:
    """Get SMA from precomputed features or compute if missing."""
    col = f'SMA_{period}'
    if col in features.columns:
        return features[col]
    return features['Close'].rolling(period).mean()


def _get_atr(features: pd.DataFrame, period: int) -> pd.Series:
    """Get ATR from precomputed features or compute if missing."""
    col = f'ATR_{period}'
    if col in features.columns:
        return features[col]
    # Fallback computation
    high_low = features['High'] - features['Low']
    high_close = abs(features['High'] - features['Close'].shift(1))
    low_close = abs(features['Low'] - features['Close'].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _get_rsi(features: pd.DataFrame, period: int) -> pd.Series:
    """Get RSI from precomputed features or compute if missing."""
    col = f'RSI_{period}'
    if col in features.columns:
        return features[col]
    return FeatureComputer.rsi(features['Close'], period)


def _get_hull_ma(features: pd.DataFrame, period: int) -> pd.Series:
    """Get Hull MA from precomputed features or compute if missing."""
    # Check for precomputed (only period 20 is precomputed by default)
    if period == 20 and 'HULL_MA' in features.columns:
        return features['HULL_MA']
    # Use the optimized FeatureComputer method with caching
    return FeatureComputer.hull_ma(features['Close'], period)


def _get_bb(features: pd.DataFrame, period: int, std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Get Bollinger Bands from precomputed features or compute if missing."""
    mid_col = f'BB_MID_{period}'
    if mid_col in features.columns:
        return features[mid_col], features[f'BB_UPPER_{period}'], features[f'BB_LOWER_{period}']
    return FeatureComputer.bollinger_bands(features['Close'], period, std)


def _get_stochastic(features: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
    """Get Stochastic from precomputed features or compute if missing."""
    if 'STOCH_K' in features.columns and k_period == 14:
        return features['STOCH_K'], features['STOCH_D']
    return FeatureComputer.stochastic(features, k_period, d_period)


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
    
    def get_required_features(self) -> Set[str]:
        """
        Get set of feature columns this strategy requires.
        
        Override in subclasses for lazy feature loading optimization.
        Returns empty set by default (compute all features).
        
        Returns:
            Set of column names required by this strategy
        """
        return set()
    
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
        
        for i in range(1, n):
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
        
        # Generate signals on direction change - vectorized
        signals = np.zeros(n, dtype=np.int32)
        dir_change = np.diff(direction, prepend=0)
        signals[dir_change == 2] = 1   # -1 to 1
        signals[dir_change == -2] = -1  # 1 to -1
        
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
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = self.params.get('adx_period', 14)
        threshold = self.params.get('adx_threshold', 25)
        
        # Calculate +DM and -DM
        plus_dm = features['High'].diff()
        minus_dm = -features['Low'].diff()
        
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
        
        # Calculate ATR
        high_low = features['High'] - features['Low']
        high_close = abs(features['High'] - features['Close'].shift(1))
        low_close = abs(features['Low'] - features['Close'].shift(1))
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        # Calculate +DI and -DI
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
        
        # Calculate ADX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
        adx = dx.rolling(period).mean()
        
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
        n = len(high_vals)

        for i in range(period, n):
            window_h = high_vals[i - period: i + 1]
            window_l = low_vals[i - period: i + 1]
            # argmax/argmin give index from window start; bars_since = period - idx
            bars_since_high = period - int(np.argmax(window_h))
            bars_since_low = period - int(np.argmin(window_l))
            aroon_up.iat[i] = 100.0 * (period - bars_since_high) / period
            aroon_down.iat[i] = 100.0 * (period - bars_since_low) / period

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

    @staticmethod
    def _compute_adx_di(df: pd.DataFrame, period: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Compute ADX, +DI, -DI (Wilder-style EMA)."""
        high = df['High']
        low = df['Low']
        close = df['Close']

        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        plus_di = 100.0 * (plus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / (atr + 1e-10))
        minus_di = 100.0 * (minus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / (atr + 1e-10))

        dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        adx = dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        return adx, plus_di, minus_di

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        period = int(self.params.get('adx_period', 14))
        adx_th = float(self.params.get('adx_threshold', 20))
        spread_min = float(self.params.get('di_spread_min', 5.0))
        adx_rising = bool(self.params.get('require_adx_rising', True))

        # Prefer precomputed columns when available
        if {'ADX', 'PLUS_DI', 'MINUS_DI'}.issubset(features.columns):
            adx = features['ADX']
            plus_di = features['PLUS_DI']
            minus_di = features['MINUS_DI']
        else:
            adx, plus_di, minus_di = self._compute_adx_di(features, period)

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

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        kc_period = int(self.params.get('kc_period', 20))
        kc_mult = float(self.params.get('kc_mult', 2.0))
        slope_bars = int(self.params.get('ema_slope_bars', 5))

        close = features['Close']

        # Keltner Channel
        high_low = features['High'] - features['Low']
        high_close = (features['High'] - close.shift(1)).abs()
        low_close = (features['Low'] - close.shift(1)).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(kc_period).mean()

        mid = close.ewm(span=kc_period, adjust=False).mean()
        upper = mid + (kc_mult * atr)
        lower = mid - (kc_mult * atr)

        # Trend filter: EMA rising/falling and price on correct side
        ema_rising = mid > mid.shift(slope_bars)
        ema_falling = mid < mid.shift(slope_bars)
        above_ema = close > mid
        below_ema = close < mid

        # Pullback detection: prior bar touched the midline or lower/upper band
        long_pullback = features['Low'].shift(1) <= mid.shift(1)
        short_pullback = features['High'].shift(1) >= mid.shift(1)

        # Confirmation: current bar closes in trend direction
        bullish_close = close > close.shift(1)
        bearish_close = close < close.shift(1)

        signals = pd.Series(0, index=features.index)
        signals[ema_rising & above_ema & long_pullback & bullish_close] = 1
        signals[ema_falling & below_ema & short_pullback & bearish_close] = -1
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
        
        delta = features['Close'].diff()
        gain = delta.where(delta > 0, 0).rolling(period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
        rs = gain / (loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        
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
        
        mid = features['Close'].rolling(period).mean()
        std = features['Close'].rolling(period).std()
        upper = mid + (std_dev * std)
        lower = mid - (std_dev * std)
        
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
        stoch_k = 100 * (features['Close'] - low_min) / (high_max - low_min + 1e-10)
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
        willr = -100 * (high_max - features['Close']) / (high_max - low_min + 1e-10)
        
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
            'signal_period': 1,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
        }

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
        sig_period = int(self.params.get('signal_period', 1))

        fisher, fisher_signal = self._fisher_transform(
            features['High'], features['Low'], features['Close'], period
        )

        if sig_period > 1:
            fisher_signal = fisher.rolling(sig_period).mean().shift(1)

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
            'signal_period': [1, 2, 3],
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
            'use_adx_filter': True,
            'adx_threshold': 25,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 2.0,
        }

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

        # Optional ADX range filter
        if bool(self.params.get('use_adx_filter', True)) and 'ADX' in features.columns:
            allow = features['ADX'] < float(self.params.get('adx_threshold', 25))
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
            'use_adx_filter': [True, False],
            'adx_threshold': [15, 20, 25, 30],
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
        
        # Price change vs ATR
        price_change = features['Close'].diff()
        threshold = atr * mult
        
        signals = pd.Series(0, index=features.index)
        signals[price_change > threshold] = 1
        signals[price_change < -threshold] = -1
        
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
        signals[momentum > threshold] = 1
        signals[momentum < -threshold] = -1
        
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
        
        # Bollinger Bands
        bb_mid = features['Close'].rolling(bb_period).mean()
        bb_std_val = features['Close'].rolling(bb_period).std()
        bb_upper = bb_mid + (bb_std * bb_std_val)
        bb_lower = bb_mid - (bb_std * bb_std_val)
        
        # Keltner Channels
        high_low = features['High'] - features['Low']
        high_close = abs(features['High'] - features['Close'].shift(1))
        low_close = abs(features['Low'] - features['Close'].shift(1))
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(kc_period).mean()
        
        kc_mid = features['Close'].ewm(span=kc_period, adjust=False).mean()
        kc_upper = kc_mid + (kc_mult * atr)
        kc_lower = kc_mid - (kc_mult * atr)
        
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
        
        # Calculate ATR
        high_low = features['High'] - features['Low']
        high_close = abs(features['High'] - features['Close'].shift(1))
        low_close = abs(features['Low'] - features['Close'].shift(1))
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        # Keltner Channels
        mid = features['Close'].ewm(span=period, adjust=False).mean()
        upper = mid + (mult * atr)
        lower = mid - (mult * atr)
        
        signals = pd.Series(0, index=features.index)
        signals[features['Close'] > upper] = 1
        signals[features['Close'] < lower] = -1
        
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

    @staticmethod
    def _compute_adx_di(df: pd.DataFrame, period: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Compute ADX, +DI, -DI (Wilder-style). Used only as a fallback when columns are missing."""
        high = df["High"]
        low = df["Low"]
        close = df["Close"]

        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        plus_di = 100.0 * (plus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean() / (atr + 1e-10))
        minus_di = 100.0 * (minus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean() / (atr + 1e-10))

        dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        adx = dx.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        return adx, plus_di, minus_di

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        ema_fast_p = int(self.params.get("ema_fast", 8))
        ema_mid_p = int(self.params.get("ema_mid", 21))
        ema_slow_p = int(self.params.get("ema_slow", 50))
        adx_period = int(self.params.get("adx_period", 14))
        adx_th = float(self.params.get("adx_threshold", 20))
        use_di = bool(self.params.get("use_di_confirmation", True))

        close = features["Close"]

        ema_fast = features.get(f"EMA_{ema_fast_p}")
        if ema_fast is None:
            ema_fast = close.ewm(span=ema_fast_p, adjust=False).mean()

        ema_mid = features.get(f"EMA_{ema_mid_p}")
        if ema_mid is None:
            ema_mid = close.ewm(span=ema_mid_p, adjust=False).mean()

        ema_slow = features.get(f"EMA_{ema_slow_p}")
        if ema_slow is None:
            ema_slow = close.ewm(span=ema_slow_p, adjust=False).mean()

        if {"ADX", "PLUS_DI", "MINUS_DI"}.issubset(features.columns):
            adx = features["ADX"]
            plus_di = features["PLUS_DI"]
            minus_di = features["MINUS_DI"]
        else:
            adx, plus_di, minus_di = self._compute_adx_di(features, adx_period)

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

    @staticmethod
    def _rsi(close: pd.Series, period: int) -> pd.Series:
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        rsi_p = int(self.params.get("rsi_period", 14))
        oversold = float(self.params.get("oversold", 30))
        overbought = float(self.params.get("overbought", 70))
        ema_p = int(self.params.get("ema_trend_period", 200))

        close = features["Close"]
        rsi = features.get(f"RSI_{rsi_p}")
        if rsi is None:
            rsi = self._rsi(close, rsi_p)

        ema = features.get(f"EMA_{ema_p}")
        if ema is None:
            ema = close.ewm(span=ema_p, adjust=False).mean()

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
            "use_ema_filter": True,
            "ema_filter_period": 50,
            "use_adx_filter": False,
            "adx_threshold": 20,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 3.0,
        }

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        fast = int(self.params.get("macd_fast", 12))
        slow = int(self.params.get("macd_slow", 26))
        sig = int(self.params.get("macd_signal", 9))

        close = features["Close"]

        if {"MACD", "MACD_SIGNAL", "MACD_HIST"}.issubset(features.columns) and (fast, slow, sig) == (12, 26, 9):
            hist = features["MACD_HIST"]
        else:
            ema_fast = close.ewm(span=fast, adjust=False).mean()
            ema_slow = close.ewm(span=slow, adjust=False).mean()
            macd = ema_fast - ema_slow
            signal_line = macd.ewm(span=sig, adjust=False).mean()
            hist = macd - signal_line

        cross_up = (hist > 0) & (hist.shift(1) <= 0)
        cross_down = (hist < 0) & (hist.shift(1) >= 0)

        # Filters
        if bool(self.params.get("use_ema_filter", True)):
            ema_p = int(self.params.get("ema_filter_period", 50))
            ema = features.get(f"EMA_{ema_p}")
            if ema is None:
                ema = close.ewm(span=ema_p, adjust=False).mean()
            cross_up = cross_up & (close > ema)
            cross_down = cross_down & (close < ema)

        if bool(self.params.get("use_adx_filter", False)) and "ADX" in features.columns:
            adx_th = float(self.params.get("adx_threshold", 20))
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
            "use_ema_filter": [True, False],
            "ema_filter_period": [20, 50, 100],
            "use_adx_filter": [False, True],
            "adx_threshold": [15, 20, 25, 30],
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
    def _stoch_rsi(close: pd.Series, rsi_period: int, stoch_period: int,
                   smooth_k: int, smooth_d: int) -> Tuple[pd.Series, pd.Series]:
        # RSI (Wilder)
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/rsi_period, min_periods=rsi_period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/rsi_period, min_periods=rsi_period, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))

        rsi_min = rsi.rolling(stoch_period).min()
        rsi_max = rsi.rolling(stoch_period).max()
        stoch = 100 * (rsi - rsi_min) / (rsi_max - rsi_min + 1e-10)

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
        ema = features.get(f"EMA_{ema_p}")
        if ema is None:
            ema = close.ewm(span=ema_p, adjust=False).mean()

        # Prefer precomputed if available
        if "STOCH_RSI_K" in features.columns and "STOCH_RSI_D" in features.columns:
            k = features["STOCH_RSI_K"]
            d = features["STOCH_RSI_D"]
        else:
            k, d = self._stoch_rsi(close, rsi_p, stoch_p, smooth_k, smooth_d)

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
            "stoch_smooth": [3, 5],
            "lower_band": [10, 15, 20, 30],
            "upper_band": [70, 80, 85, 90],
            "ema_trend_period": [50, 100, 150, 200],
            **self._base_sl_tp_grid(),
        }


class VWAPDeviationReversionStrategy(BaseStrategy):
    """VWAP Deviation Mean Reversion Strategy (uses tick volume if available).

    Implementation details:
      - Rolling VWAP over vwap_window using typical price (H+L+C)/3 weighted by Volume.
      - z-score of deviation (Close - VWAP) over z_window.
      - Entry on cross back inside threshold to avoid catching extreme momentum moves.

    Long entry:
      z crosses up through -entry_z (previous <= -entry_z and current > -entry_z)

    Short entry:
      z crosses down through +entry_z (previous >= entry_z and current < entry_z)

    Optional ADX filter to prefer range regimes (ADX < adx_threshold).
    """

    @property
    def name(self) -> str:
        return "VWAPDeviationReversionStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "vwap_window": 50,
            "z_window": 50,
            "entry_z": 2.0,
            "use_adx_filter": True,
            "adx_threshold": 20,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
        }

    @staticmethod
    def _rolling_vwap(df: pd.DataFrame, window: int) -> pd.Series:
        col = f"VWAP_{window}"
        if col in df.columns:
            return df[col]

        vol = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0) if "Volume" in df.columns else pd.Series(1.0, index=df.index)
        tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
        pv = tp * vol
        vol_sum = vol.rolling(window=window, min_periods=window).sum()
        pv_sum = pv.rolling(window=window, min_periods=window).sum()
        return pv_sum / (vol_sum + 1e-10)

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        vwap_w = int(self.params.get("vwap_window", 50))
        z_w = int(self.params.get("z_window", 50))
        entry_z = float(self.params.get("entry_z", 2.0))

        vwap = self._rolling_vwap(features, vwap_w)
        dev = features["Close"] - vwap

        mu = dev.rolling(window=z_w, min_periods=z_w).mean()
        sd = dev.rolling(window=z_w, min_periods=z_w).std()
        z = (dev - mu) / (sd + 1e-10)

        if bool(self.params.get("use_adx_filter", True)) and "ADX" in features.columns:
            allow = features["ADX"] < float(self.params.get("adx_threshold", 20))
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
            "vwap_window": [20, 50, 100, 150, 200],
            "z_window": [20, 30, 50, 100],
            "entry_z": [1.5, 1.75, 2.0, 2.25, 2.5],
            "use_adx_filter": [True, False],
            "adx_threshold": [15, 20, 25, 30],
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
        'VWAPDeviationReversionStrategy': VWAPDeviationReversionStrategy,
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
    'VWAPDeviationReversionStrategy',
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
]


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
