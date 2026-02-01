"""
FX Portfolio Manager - Regime Parameter Tuner
==============================================

Standalone script to tune regime detection parameters for each symbol/timeframe.

Outputs: regime_params.json
    {
        "EURUSD": {
            "H1": {"k_confirm": 2, "gap_min": 0.12, ...},
            "H4": {...}
        },
        ...
    }

Usage:
    python pm_regime_tuner.py --data-dir ./data --output regime_params.json
    python pm_regime_tuner.py --symbols EURUSD GBPUSD --timeframes H1 H4

This script is separate from the main optimization flow and should be run
periodically (e.g., weekly) to calibrate regime detection parameters.

Version: 3.0 (Portfolio Manager)
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from itertools import product
from collections import Counter

import numpy as np
import pandas as pd

from pm_regime import (
    RegimeParams, 
    MarketRegimeDetector, 
    RegimeType,
    DEFAULT_PARAMS_BY_TIMEFRAME,
    save_regime_params
)

logger = logging.getLogger(__name__)


# =============================================================================
# TUNING CONFIGURATION
# =============================================================================

# Timeframe-specific parameter search spaces
PARAM_SEARCH_SPACE = {
    'M5': {
        'k_confirm': [2, 3, 4],
        'gap_min': [0.08, 0.10, 0.12, 0.15],
        'k_hold': [4, 5, 6, 8]
    },
    'M15': {
        'k_confirm': [2, 3, 4],
        'gap_min': [0.08, 0.10, 0.12],
        'k_hold': [3, 4, 5, 6]
    },
    'M30': {
        'k_confirm': [2, 3],
        'gap_min': [0.08, 0.10, 0.12],
        'k_hold': [3, 4, 5]
    },
    'H1': {
        'k_confirm': [2, 3],
        'gap_min': [0.08, 0.10, 0.12],
        'k_hold': [2, 3, 4]
    },
    'H4': {
        'k_confirm': [1, 2],
        'gap_min': [0.06, 0.08, 0.10],
        'k_hold': [2, 3]
    },
    'D1': {
        'k_confirm': [1, 2],
        'gap_min': [0.05, 0.08, 0.10],
        'k_hold': [1, 2]
    },
}


# =============================================================================
# DATA LOADING
# =============================================================================

class TunerDataLoader:
    """Simple data loader for tuning."""
    
    RESAMPLE_MAP = {
        'M1': '1min', 'M5': '5min', 'M15': '15min', 'M30': '30min',
        'H1': '1h', 'H4': '4h', 'D1': '1D', 'W1': '1W',
    }
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self._cache: Dict[str, pd.DataFrame] = {}
    
    def load_symbol(self, symbol: str) -> Optional[pd.DataFrame]:
        """Load M5 data for a symbol."""
        if symbol in self._cache:
            return self._cache[symbol].copy()
        
        # Try different file patterns
        patterns = [
            f"{symbol}_M5*.csv",
            f"{symbol.upper()}_M5*.csv",
            f"{symbol.lower()}_M5*.csv"
        ]
        
        for pattern in patterns:
            matches = list(self.data_dir.glob(pattern))
            if matches:
                try:
                    df = pd.read_csv(matches[0])
                    df = self._standardize(df)
                    self._cache[symbol] = df
                    logger.info(f"Loaded {symbol}: {len(df)} bars from {matches[0].name}")
                    return df.copy()
                except Exception as e:
                    logger.error(f"Failed to load {symbol}: {e}")
                    return None
        
        logger.warning(f"No data file found for {symbol}")
        return None
    
    def _standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and set datetime index."""
        df.columns = df.columns.str.lower()
        
        column_map = {
            'open': 'Open', 'high': 'High', 'low': 'Low', 
            'close': 'Close', 'volume': 'Volume', 'tick_volume': 'Volume'
        }
        df = df.rename(columns=column_map)
        
        # Find and parse datetime
        for col in ['time', 'datetime', 'date', 'timestamp']:
            if col in df.columns.str.lower():
                time_col = [c for c in df.columns if c.lower() == col][0]
                df[time_col] = pd.to_datetime(df[time_col])
                df = df.set_index(time_col).sort_index()
                break
        
        return df
    
    def resample(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Resample to target timeframe."""
        if timeframe == 'M5':
            return df.copy()
        
        rule = self.RESAMPLE_MAP.get(timeframe, '1h')
        
        resampled = df.resample(rule).agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 
            'Close': 'last', 'Volume': 'sum'
        }).dropna()
        
        return resampled
    
    def clear_cache(self):
        """Clear the data cache."""
        self._cache.clear()


# =============================================================================
# TUNING METRICS
# =============================================================================

def compute_regime_quality_metrics(regime_series: pd.Series, 
                                    gap_series: pd.Series,
                                    price_series: pd.Series) -> Dict[str, Any]:
    """
    Compute quality metrics for a regime detection configuration.
    
    Metrics:
    - stability: Fewer regime changes = more stable
    - avg_gap: Higher average gap = more confident detection
    - regime_balance: Reasonable distribution across regimes
    - trend_return_ratio: Returns magnitude in TREND vs RANGE
    
    Returns:
        Dict with quality metrics
    """
    n = len(regime_series)
    
    if n < 100:
        return {
            'stability': 0.0, 'avg_gap': 0.0, 'balance_score': 0.0,
            'trend_return_ratio': 1.0, 'regime_counts': {}, 'total_changes': 0
        }
    
    # Filter out None/NaN values
    valid_mask = regime_series.notna()
    regime_valid = regime_series[valid_mask]
    gap_valid = gap_series[valid_mask]
    
    # Stability: count regime changes
    changes = (regime_valid != regime_valid.shift(1)).sum()
    stability = 1.0 - (changes / len(regime_valid)) if len(regime_valid) > 0 else 0.0
    
    # Gap confidence: average gap
    avg_gap = gap_valid.mean() if len(gap_valid) > 0 else 0.0
    
    # Regime distribution
    regime_counts = regime_valid.value_counts(normalize=True)
    
    # Balance score: penalize if any regime > 60% or < 5%
    balance_score = 1.0
    if len(regime_counts) > 0:
        max_pct = regime_counts.max()
        min_pct = regime_counts.min()
        
        if max_pct > 0.60:
            balance_score *= 0.8
        if min_pct < 0.05:
            balance_score *= 0.9
    
    # Trend return ratio: do TREND periods show larger moves?
    returns = price_series.pct_change().abs()
    
    trend_mask = regime_series == RegimeType.TREND
    range_mask = regime_series == RegimeType.RANGE
    
    trend_returns_mean = returns[trend_mask].mean() if trend_mask.sum() > 20 else 0.0
    range_returns_mean = returns[range_mask].mean() if range_mask.sum() > 20 else trend_returns_mean
    
    if range_returns_mean > 1e-10:
        trend_return_ratio = trend_returns_mean / range_returns_mean
    else:
        trend_return_ratio = 1.0
    
    return {
        'stability': float(stability),
        'avg_gap': float(avg_gap),
        'balance_score': float(balance_score),
        'trend_return_ratio': float(min(2.0, trend_return_ratio)),
        'regime_counts': dict(regime_counts),
        'total_changes': int(changes)
    }


def compute_tuning_score(metrics: Dict[str, Any]) -> float:
    """
    Compute composite score for tuning.
    
    Higher = better regime detection configuration.
    """
    # Weights
    w_stability = 0.25
    w_gap = 0.25
    w_balance = 0.20
    w_trend_ratio = 0.30
    
    stability = metrics.get('stability', 0.0)
    avg_gap = metrics.get('avg_gap', 0.0)
    balance = metrics.get('balance_score', 0.0)
    trend_ratio = metrics.get('trend_return_ratio', 1.0)
    
    # Normalize gap to 0-1 (gaps typically range 0-0.2)
    gap_normalized = min(1.0, avg_gap * 5)
    
    # Trend ratio bonus (want > 1.0)
    trend_bonus = min(1.0, (trend_ratio - 0.8) / 0.7) if trend_ratio > 0.8 else 0.0
    
    score = (
        w_stability * stability +
        w_gap * gap_normalized +
        w_balance * balance +
        w_trend_ratio * trend_bonus
    )
    
    return float(score)


# =============================================================================
# TUNER CLASS
# =============================================================================

class RegimeParamTuner:
    """
    Tunes regime detection parameters for each symbol/timeframe.
    """
    
    def __init__(self, data_dir: Path):
        self.data_loader = TunerDataLoader(data_dir)
    
    def tune_symbol_timeframe(self, symbol: str, timeframe: str,
                               param_space: Optional[Dict] = None) -> Tuple[RegimeParams, Dict]:
        """
        Tune parameters for a single symbol/timeframe combination.
        
        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            param_space: Optional custom parameter search space
            
        Returns:
            Tuple of (best_params, tuning_results)
        """
        # Load data
        df_m5 = self.data_loader.load_symbol(symbol)
        if df_m5 is None:
            logger.warning(f"No data for {symbol}, using defaults")
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}
        
        # Resample
        df = self.data_loader.resample(df_m5, timeframe)
        
        if len(df) < 500:
            logger.warning(f"{symbol} {timeframe}: Only {len(df)} bars, need 500+, using defaults")
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}
        
        # Get param search space
        if param_space is None:
            param_space = PARAM_SEARCH_SPACE.get(timeframe, {
                'k_confirm': [2, 3],
                'gap_min': [0.08, 0.10, 0.12],
                'k_hold': [3, 4, 5]
            })
        
        # Generate combinations
        param_names = list(param_space.keys())
        param_values = list(param_space.values())
        combinations = list(product(*param_values))
        
        logger.info(f"[{symbol}] [{timeframe}] Testing {len(combinations)} param combinations")
        
        best_score = -float('inf')
        best_params = DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams())
        best_metrics = {}
        
        for combo in combinations:
            params_dict = dict(zip(param_names, combo))
            
            # Create params object
            params = RegimeParams(
                k_confirm=params_dict.get('k_confirm', 2),
                gap_min=params_dict.get('gap_min', 0.10),
                k_hold=params_dict.get('k_hold', 3)
            )
            
            try:
                # Run regime detection
                detector = MarketRegimeDetector(params)
                result = detector.compute_regime_scores(df)
                
                # Compute quality metrics
                metrics = compute_regime_quality_metrics(
                    result['REGIME'],
                    result['REGIME_GAP'],
                    df['Close']
                )
                
                score = compute_tuning_score(metrics)
                
                if score > best_score:
                    best_score = score
                    best_params = params
                    best_metrics = metrics
                    
            except Exception as e:
                logger.debug(f"Combo {params_dict} failed: {e}")
                continue
        
        logger.info(f"[{symbol}] [{timeframe}] Best: k_confirm={best_params.k_confirm}, "
                   f"gap_min={best_params.gap_min:.2f}, k_hold={best_params.k_hold} "
                   f"(score={best_score:.3f})")
        
        return best_params, {
            'score': best_score,
            'metrics': best_metrics,
        }
    
    def tune_all(self, symbols: List[str], timeframes: List[str],
                 output_path: str = "regime_params.json") -> Dict[str, Dict[str, RegimeParams]]:
        """
        Tune parameters for all symbol/timeframe combinations.
        
        Args:
            symbols: List of symbols
            timeframes: List of timeframes
            output_path: Path to save results
            
        Returns:
            Dict with all tuned parameters
        """
        all_params: Dict[str, Dict[str, RegimeParams]] = {}
        
        total = len(symbols) * len(timeframes)
        current = 0
        
        for symbol in symbols:
            all_params[symbol] = {}
            
            for tf in timeframes:
                current += 1
                logger.info(f"Progress: {current}/{total}")
                
                best_params, _ = self.tune_symbol_timeframe(symbol, tf)
                all_params[symbol][tf] = best_params
            
            # Clear cache after each symbol to manage memory
            self.data_loader.clear_cache()
        
        # Save to file
        save_regime_params(all_params, output_path)
        
        return all_params


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Tune regime detection parameters for FX Portfolio Manager"
    )
    parser.add_argument(
        '--data-dir', type=str, default='./data',
        help='Data directory with M5 CSV files'
    )
    parser.add_argument(
        '--output', type=str, default='regime_params.json',
        help='Output file path'
    )
    parser.add_argument(
        '--symbols', type=str, nargs='+', default=None,
        help='Symbols to tune (default: all in data dir)'
    )
    parser.add_argument(
        '--timeframes', type=str, nargs='+', 
        default=['M5', 'M15', 'M30', 'H1', 'H4', 'D1'],
        help='Timeframes to tune'
    )
    parser.add_argument(
        '--log-level', type=str, default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    
    data_dir = Path(args.data_dir)
    
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return 1
    
    # Discover symbols if not specified
    if args.symbols is None:
        csv_files = list(data_dir.glob("*_M5*.csv"))
        symbols = list(set(f.name.split('_')[0].upper() for f in csv_files))
        symbols = sorted(symbols)
    else:
        symbols = [s.upper() for s in args.symbols]
    
    if not symbols:
        logger.error("No symbols found. Check --data-dir path.")
        return 1
    
    logger.info(f"Tuning regime parameters")
    logger.info(f"  Symbols: {len(symbols)}")
    logger.info(f"  Timeframes: {args.timeframes}")
    logger.info(f"  Output: {args.output}")
    
    # Run tuning
    tuner = RegimeParamTuner(data_dir)
    tuner.tune_all(symbols, args.timeframes, args.output)
    
    logger.info("Tuning complete!")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
