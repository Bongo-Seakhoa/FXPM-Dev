"""
FX Portfolio Manager - Core Components
=======================================

Core classes and utilities for the FX Portfolio Manager.
Contains: Configuration, Data Loading, Feature Engineering, Data Splitting,
and Backtesting Engine with complete performance metrics.

This is the foundation module that provides:
- PipelineConfig: Global configuration management
- DataLoader: Load and resample OHLCV data from MT5 or CSV
- FeatureComputer: Technical indicator calculations
- DataSplitter: 80/30 split with 10% overlap for training/validation
- Backtester: Full backtesting engine matching original implementation
  - Includes Numba JIT-compiled main loop for 3-10x speedup

Version: 3.1 (Portfolio Manager - Numba backtester optimization)
"""

import io
import json
import os
import shutil
import time
import math
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field, fields, asdict
from enum import Enum
from typing import Dict, List, Any, Optional, Tuple, Union, Set

import numpy as np
import pandas as pd
from pm_order_governance import candidate_policy_names, evaluate_policy, make_policy, GovernanceContext
from pm_metrics import (
    METRIC_CONTRACT_VERSION,
    profit_factor_from_gross,
    return_drawdown_metrics,
    round_metric,
    sharpe_sortino_from_returns,
)
from pm_symbol import infer_asset_class
from pm_native_activation import resolve_active_native_artifact
from pm_evidence import (
    CALENDAR_SPLIT_MODE,
    DEFAULT_MIN_NATIVE_BARS,
    DEFAULT_TIMEFRAME_WINDOW_YEARS,
    DEFAULT_WINDOW_YEARS,
    normalize_min_native_bars,
    normalize_timeframe_window_years,
    normalize_window_years,
    validate_native_provenance,
)

# Configure module logger
logger = logging.getLogger(__name__)


_WEEKDAY_NAME_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


# =============================================================================
# NUMBA JIT COMPILATION FOR BACKTESTER (optional - graceful fallback)
# =============================================================================

try:
    from numba import jit
    NUMBA_AVAILABLE = True
    logger.debug("Numba JIT compilation available for backtester")
except ImportError:
    NUMBA_AVAILABLE = False
    logger.debug("Numba not available - using pure Python backtester")
    
    # Create no-op decorator for graceful fallback
    def jit(*args, **kwargs):
        def decorator(func):
            return func
        return decorator


# Exit reason codes for JIT kernel (cannot use Enum in Numba)
EXIT_NONE = 0
EXIT_SL = 1
EXIT_TP = 2
EXIT_EOD = 3

# Trade records emitted after this version include per-trade MAE/MFE and
# underwater timing fields. Older artifacts without this field must not be
# interpreted as zero-excursion records.
BACKTEST_TRADE_RECORD_SCHEMA_VERSION = "2026-06-18-mae-mfe-v1"
BACKTEST_EXECUTION_COST_CONTRACT_VERSION = "entry_slippage_parity_v1"
BACKTEST_SWAP_ACCRUAL_CONTRACT_VERSION = "causal_swap_accrual_v1"


@jit(nopython=True, cache=True)
def _round_volume_numba(volume: float, min_lot: float, max_lot: float, step: float) -> float:
    """
    Numba-safe volume rounding helper.
    
    Replicates broker volume "floor to step" logic without calling Python methods.
    
    Args:
        volume: Raw position size
        min_lot: Minimum lot size
        max_lot: Maximum lot size
        step: Volume step (e.g., 0.01)
        
    Returns:
        Rounded volume clamped to min/max
    """
    if step <= 0.0:
        step = 0.01
    # Floor to step (risk-safe - never round up).  Calculations that are
    # mathematically on a broker step can arrive a few ULPs below it (for
    # example 0.49999999999999956 instead of 0.500).  Snap only that tiny
    # representation error to the grid before flooring; genuinely sub-step
    # volumes retain the conservative floor.
    scaled = volume / step
    nearest_step = np.round(scaled)
    tolerance = 1e-10 * max(1.0, abs(scaled))
    if abs(scaled - nearest_step) <= tolerance:
        scaled = nearest_step
    rounded = np.floor(scaled) * step
    # A risk-sized value below the broker minimum is not tradable.  Never
    # clamp it upward: that can multiply the intended stop risk arbitrarily.
    if rounded < min_lot:
        return 0.0
    if rounded > max_lot:
        rounded = max_lot
    # Stabilize float noise
    return np.round(rounded, 8)


@jit(nopython=True, cache=True)
def _calc_trade_pnl_numba(entry_px: float,
                          exit_px: float,
                          direction: int,
                          size: float,
                          tick_size: float,
                          tick_value: float,
                          pip_size: float,
                          pip_value: float,
                          use_commission: bool,
                          commission_per_lot: float) -> Tuple[float, float]:
    """Numba-safe trade PnL helper used by the backtest kernel."""
    if tick_size > 0.0 and tick_value > 0.0:
        price_diff = (exit_px - entry_px) * direction
        ticks = price_diff / tick_size
        pnl_dollars_local = ticks * tick_value * size
    else:
        pnl_dollars_local = 0.0
        if pip_size > 0.0 and pip_value > 0.0:
            if direction == 1:
                pnl_pips_local = (exit_px - entry_px) / pip_size
            else:
                pnl_pips_local = (entry_px - exit_px) / pip_size
            pnl_dollars_local = pnl_pips_local * size * pip_value

    if pip_size > 0.0:
        if direction == 1:
            pnl_pips_local = (exit_px - entry_px) / pip_size
        else:
            pnl_pips_local = (entry_px - exit_px) / pip_size
    else:
        pnl_pips_local = 0.0

    if use_commission:
        pnl_dollars_local -= commission_per_lot * size

    return pnl_dollars_local, pnl_pips_local


@jit(nopython=True, cache=True)
def _backtest_loop_numba(
    # OHLC arrays (float64 for precision)
    open_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    close_arr: np.ndarray,
    # Signal array (int32)
    sig_arr: np.ndarray,
    # Pre-computed stop/entry prices (SL/TP must be in Python due to strategy.calculate_stops)
    sl_prices: np.ndarray,
    tp_prices: np.ndarray,
    entry_prices: np.ndarray,
    # Config values
    initial_capital: float,
    position_size_pct: float,
    half_spread_arr: np.ndarray,
    slippage_price: float,
    use_spread: bool,
    use_slippage: bool,
    use_commission: bool,
    commission_per_lot: float,
    # Tick calculation params
    tick_size: float,
    tick_value: float,
    pip_size: float,
    pip_value: float,
    # Volume rounding params (for live-equity sizing)
    min_lot: float,
    max_lot: float,
    volume_step: float,
    warmup_bars: int,
    # Optional causal-carry inputs are appended so the long-standing direct
    # kernel call contract remains backward compatible.
    rollover_units_arr=None,
    swap_long_per_lot_arr=None,
    swap_short_per_lot_arr=None,
    causal_swap_enabled: bool = False,
    pre_open_rollover_units_arr=None,
) -> Tuple:
    """
    Numba JIT-compiled backtest loop for 3-10x speedup.
    
    CRITICAL: This function preserves the Python backtester semantics:
    - SL is checked before TP
    - Entry happens on bar i open from bar i-1 signal
    - Same-bar exits are allowed after entry
    - Gap-through stops exit at the worst tradable open
    - Position sizing uses live equity
    
    Args:
        OHLC arrays, signals, pre-computed stops (entry_prices, sl_prices, tp_prices)
        Config params including position_size_pct and volume rounding params
        
    Returns:
        Tuple of arrays for trade reconstruction:
        - trade_signal_bars, trade_entry_bars, trade_exit_bars
        - trade_directions, trade_entry_prices, trade_exit_prices
        - trade_sl_prices, trade_tp_prices, trade_sizes
        - trade_pnl_dollars, trade_pnl_pips, trade_exit_reasons, trade_risk_amounts
        - equity_curve, final_equity, max_drawdown
        - MAE/MFE excursion fields appended after the legacy result fields
    """
    n_bars = len(open_arr)
    if rollover_units_arr is None:
        rollover_units_arr = np.zeros(n_bars, dtype=np.int32)
    if pre_open_rollover_units_arr is None:
        pre_open_rollover_units_arr = np.zeros(n_bars, dtype=np.int32)
    if swap_long_per_lot_arr is None:
        swap_long_per_lot_arr = np.zeros(n_bars, dtype=np.float64)
    if swap_short_per_lot_arr is None:
        swap_short_per_lot_arr = np.zeros(n_bars, dtype=np.float64)
    
    # Pre-allocate trade arrays (max possible = n_bars trades)
    # We'll return only the filled portion
    max_trades = n_bars
    trade_signal_bars = np.zeros(max_trades, dtype=np.int64)
    trade_entry_bars = np.zeros(max_trades, dtype=np.int64)
    trade_exit_bars = np.zeros(max_trades, dtype=np.int64)
    trade_directions = np.zeros(max_trades, dtype=np.int32)
    trade_entry_prices = np.zeros(max_trades, dtype=np.float64)
    trade_exit_prices = np.zeros(max_trades, dtype=np.float64)
    trade_sl_prices = np.zeros(max_trades, dtype=np.float64)
    trade_tp_prices = np.zeros(max_trades, dtype=np.float64)
    trade_sizes = np.zeros(max_trades, dtype=np.float64)
    trade_gross_pnl_dollars = np.zeros(max_trades, dtype=np.float64)
    trade_pnl_dollars = np.zeros(max_trades, dtype=np.float64)
    trade_pnl_pips = np.zeros(max_trades, dtype=np.float64)
    trade_exit_reasons = np.zeros(max_trades, dtype=np.int32)
    trade_risk_amounts = np.zeros(max_trades, dtype=np.float64)
    trade_swap_rollover_units = np.zeros(max_trades, dtype=np.int64)
    trade_swap_dollars = np.zeros(max_trades, dtype=np.float64)
    trade_mae_pips = np.zeros(max_trades, dtype=np.float64)
    trade_mfe_pips = np.zeros(max_trades, dtype=np.float64)
    trade_mae_r = np.zeros(max_trades, dtype=np.float64)
    trade_mfe_r = np.zeros(max_trades, dtype=np.float64)
    trade_time_to_mae_bars = np.zeros(max_trades, dtype=np.int64)
    trade_time_to_mfe_bars = np.zeros(max_trades, dtype=np.int64)
    trade_time_underwater_bars = np.zeros(max_trades, dtype=np.int64)
    trade_entry_to_stop_pips = np.zeros(max_trades, dtype=np.float64)
    trade_entry_to_target_pips = np.zeros(max_trades, dtype=np.float64)
    
    # Equity curve
    equity_curve = np.zeros(n_bars, dtype=np.float64)
    equity_curve[0] = initial_capital
    
    # State
    equity = initial_capital
    peak_equity = equity
    max_drawdown = 0.0
    
    in_position = False
    position_direction = 0
    entry_price = 0.0
    entry_bar = 0
    signal_bar = 0
    stop_loss = 0.0
    take_profit = 0.0
    position_size = 0.0
    risk_amount_at_entry = 0.0
    daily_swap_per_lot_current = 0.0
    accrued_swap_units_current = 0
    accrued_swap_dollars_current = 0.0
    entry_to_stop_pips = 0.0
    entry_to_target_pips = 0.0
    mae_pips_current = 0.0
    mfe_pips_current = 0.0
    time_to_mae_bars_current = 0
    time_to_mfe_bars_current = 0
    time_underwater_bars_current = 0
    
    trade_count = 0
    
    # Main loop
    for i in range(1, n_bars):
        open_price = open_arr[i]
        high_price = high_arr[i]
        low_price = low_arr[i]
        close_price = close_arr[i]

        half_spread = half_spread_arr[i] if i < len(half_spread_arr) else 0.0
        if use_spread:
            bid_open = open_price - half_spread
            ask_open = open_price + half_spread
            bid_high = high_price - half_spread
            bid_low = low_price - half_spread
            ask_high = high_price + half_spread
            ask_low = low_price + half_spread
            bid_close = close_price - half_spread
            ask_close = close_price + half_spread
        else:
            bid_open = open_price
            ask_open = open_price
            bid_high = high_price
            bid_low = low_price
            ask_high = high_price
            ask_low = low_price
            bid_close = close_price
            ask_close = close_price

        # Rollover events inside an archive/session gap belong only to a
        # position carried across that gap.  Book them before gap-open exits
        # and before a new bar-open position can be sized.
        if in_position and causal_swap_enabled:
            pre_open_units = (
                int(pre_open_rollover_units_arr[i])
                if i < len(pre_open_rollover_units_arr)
                else 0
            )
            if pre_open_units > 0:
                swap_cashflow = (
                    daily_swap_per_lot_current
                    * position_size
                    * pre_open_units
                )
                equity += swap_cashflow
                accrued_swap_units_current += pre_open_units
                accrued_swap_dollars_current += swap_cashflow
        
        if in_position:
            bars_held_now = i - entry_bar
            if pip_size > 0.0:
                if position_direction == 1:
                    adverse_pips = (entry_price - bid_low) / pip_size
                    favorable_pips = (bid_high - entry_price) / pip_size
                    close_mark_pips = (bid_close - entry_price) / pip_size
                else:
                    adverse_pips = (ask_high - entry_price) / pip_size
                    favorable_pips = (entry_price - ask_low) / pip_size
                    close_mark_pips = (entry_price - ask_close) / pip_size
                if adverse_pips > mae_pips_current:
                    mae_pips_current = adverse_pips
                    time_to_mae_bars_current = bars_held_now
                if favorable_pips > mfe_pips_current:
                    mfe_pips_current = favorable_pips
                    time_to_mfe_bars_current = bars_held_now
                if close_mark_pips < 0.0:
                    time_underwater_bars_current += 1

            exit_price = -1.0
            exit_reason = EXIT_NONE

            if position_direction == 1:
                if bid_open <= stop_loss:
                    exit_price = bid_open
                    exit_reason = EXIT_SL
                elif bid_low <= stop_loss:
                    exit_price = stop_loss - slippage_price if use_slippage else stop_loss
                    exit_reason = EXIT_SL
                elif bid_high >= take_profit:
                    exit_price = take_profit
                    exit_reason = EXIT_TP
            else:
                if ask_open >= stop_loss:
                    exit_price = ask_open
                    exit_reason = EXIT_SL
                elif ask_high >= stop_loss:
                    exit_price = stop_loss + slippage_price if use_slippage else stop_loss
                    exit_reason = EXIT_SL
                elif ask_low <= take_profit:
                    exit_price = take_profit
                    exit_reason = EXIT_TP
            
            if exit_reason != EXIT_NONE:
                gross_pnl_dollars, pnl_pips = _calc_trade_pnl_numba(
                    entry_price, exit_price, position_direction, position_size,
                    tick_size, tick_value, pip_size, pip_value,
                    use_commission, commission_per_lot
                )
                equity += gross_pnl_dollars
                net_pnl_dollars = gross_pnl_dollars + accrued_swap_dollars_current
                trade_signal_bars[trade_count] = signal_bar
                trade_entry_bars[trade_count] = entry_bar
                trade_exit_bars[trade_count] = i
                trade_directions[trade_count] = position_direction
                trade_entry_prices[trade_count] = entry_price
                trade_exit_prices[trade_count] = exit_price
                trade_sl_prices[trade_count] = stop_loss
                trade_tp_prices[trade_count] = take_profit
                trade_sizes[trade_count] = position_size
                trade_gross_pnl_dollars[trade_count] = gross_pnl_dollars
                trade_pnl_dollars[trade_count] = net_pnl_dollars
                trade_pnl_pips[trade_count] = pnl_pips
                trade_exit_reasons[trade_count] = exit_reason
                trade_risk_amounts[trade_count] = risk_amount_at_entry
                trade_swap_rollover_units[trade_count] = accrued_swap_units_current
                trade_swap_dollars[trade_count] = accrued_swap_dollars_current
                trade_mae_pips[trade_count] = mae_pips_current
                trade_mfe_pips[trade_count] = mfe_pips_current
                trade_mae_r[trade_count] = mae_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
                trade_mfe_r[trade_count] = mfe_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
                trade_time_to_mae_bars[trade_count] = time_to_mae_bars_current
                trade_time_to_mfe_bars[trade_count] = time_to_mfe_bars_current
                trade_time_underwater_bars[trade_count] = time_underwater_bars_current
                trade_entry_to_stop_pips[trade_count] = entry_to_stop_pips
                trade_entry_to_target_pips[trade_count] = entry_to_target_pips
                trade_count += 1
                in_position = False

        if (not in_position) and i >= warmup_bars:
            signal = sig_arr[i - 1]
            if signal != 0:
                direction = signal
                signal_bar_index = i - 1
                entry_price_candidate = entry_prices[i]
                stop_loss_candidate = sl_prices[i]
                take_profit_candidate = tp_prices[i]
                if np.isnan(entry_price_candidate) or np.isnan(stop_loss_candidate) or np.isnan(take_profit_candidate):
                    equity_curve[i] = equity
                    if equity > peak_equity:
                        peak_equity = equity
                    if peak_equity > 0:
                        dd = (peak_equity - equity) / peak_equity * 100.0
                        if dd > max_drawdown:
                            max_drawdown = dd
                    continue

                risk_amount_at_entry = equity * (position_size_pct / 100.0)
                if direction == 1:
                    dist = entry_price_candidate - stop_loss_candidate
                else:
                    dist = stop_loss_candidate - entry_price_candidate

                loss_per_lot = 0.0
                if tick_size > 0.0 and tick_value > 0.0:
                    ticks = dist / tick_size
                    loss_per_lot = ticks * tick_value
                elif pip_size > 0.0 and pip_value > 0.0:
                    pips = dist / pip_size
                    loss_per_lot = pips * pip_value
                if use_commission:
                    loss_per_lot += commission_per_lot
                if loss_per_lot <= 0.0:
                    equity_curve[i] = equity
                    if equity > peak_equity:
                        peak_equity = equity
                    if peak_equity > 0:
                        dd = (peak_equity - equity) / peak_equity * 100.0
                        if dd > max_drawdown:
                            max_drawdown = dd
                    continue

                raw_size = risk_amount_at_entry / loss_per_lot
                position_size = _round_volume_numba(raw_size, min_lot, max_lot, volume_step)
                if position_size <= 0.0:
                    equity_curve[i] = equity
                    if equity > peak_equity:
                        peak_equity = equity
                    if peak_equity > 0:
                        dd = (peak_equity - equity) / peak_equity * 100.0
                        if dd > max_drawdown:
                            max_drawdown = dd
                    continue

                entry_price = entry_price_candidate
                stop_loss = stop_loss_candidate
                take_profit = take_profit_candidate
                if pip_size > 0.0:
                    if direction == 1:
                        entry_to_stop_pips = (entry_price - stop_loss) / pip_size
                        entry_to_target_pips = (take_profit - entry_price) / pip_size
                    else:
                        entry_to_stop_pips = (stop_loss - entry_price) / pip_size
                        entry_to_target_pips = (entry_price - take_profit) / pip_size
                else:
                    entry_to_stop_pips = 0.0
                    entry_to_target_pips = 0.0
                mae_pips_current = 0.0
                mfe_pips_current = 0.0
                time_to_mae_bars_current = 0
                time_to_mfe_bars_current = 0
                time_underwater_bars_current = 0
                in_position = True
                position_direction = direction
                entry_bar = i
                signal_bar = signal_bar_index
                daily_swap_per_lot_current = (
                    swap_long_per_lot_arr[i]
                    if position_direction == 1
                    else swap_short_per_lot_arr[i]
                )
                accrued_swap_units_current = 0
                accrued_swap_dollars_current = 0.0
                if pip_size > 0.0:
                    if position_direction == 1:
                        adverse_pips = (entry_price - bid_low) / pip_size
                        favorable_pips = (bid_high - entry_price) / pip_size
                        close_mark_pips = (bid_close - entry_price) / pip_size
                    else:
                        adverse_pips = (ask_high - entry_price) / pip_size
                        favorable_pips = (entry_price - ask_low) / pip_size
                        close_mark_pips = (entry_price - ask_close) / pip_size
                    if adverse_pips > mae_pips_current:
                        mae_pips_current = adverse_pips
                        time_to_mae_bars_current = 0
                    if favorable_pips > mfe_pips_current:
                        mfe_pips_current = favorable_pips
                        time_to_mfe_bars_current = 0
                    if close_mark_pips < 0.0:
                        time_underwater_bars_current += 1

                # Same-bar exit: check gap-through at open FIRST, then intra-bar
                exit_price = -1.0
                exit_reason = EXIT_NONE
                if position_direction == 1:
                    if bid_open <= stop_loss:
                        exit_price = bid_open
                        exit_reason = EXIT_SL
                    elif bid_low <= stop_loss:
                        exit_price = stop_loss - slippage_price if use_slippage else stop_loss
                        exit_reason = EXIT_SL
                    elif bid_high >= take_profit:
                        exit_price = take_profit
                        exit_reason = EXIT_TP
                else:
                    if ask_open >= stop_loss:
                        exit_price = ask_open
                        exit_reason = EXIT_SL
                    elif ask_high >= stop_loss:
                        exit_price = stop_loss + slippage_price if use_slippage else stop_loss
                        exit_reason = EXIT_SL
                    elif ask_low <= take_profit:
                        exit_price = take_profit
                        exit_reason = EXIT_TP

                if exit_reason != EXIT_NONE:
                    gross_pnl_dollars, pnl_pips = _calc_trade_pnl_numba(
                        entry_price, exit_price, position_direction, position_size,
                        tick_size, tick_value, pip_size, pip_value,
                        use_commission, commission_per_lot
                    )
                    equity += gross_pnl_dollars
                    net_pnl_dollars = gross_pnl_dollars + accrued_swap_dollars_current
                    trade_signal_bars[trade_count] = signal_bar
                    trade_entry_bars[trade_count] = entry_bar
                    trade_exit_bars[trade_count] = i
                    trade_directions[trade_count] = position_direction
                    trade_entry_prices[trade_count] = entry_price
                    trade_exit_prices[trade_count] = exit_price
                    trade_sl_prices[trade_count] = stop_loss
                    trade_tp_prices[trade_count] = take_profit
                    trade_sizes[trade_count] = position_size
                    trade_gross_pnl_dollars[trade_count] = gross_pnl_dollars
                    trade_pnl_dollars[trade_count] = net_pnl_dollars
                    trade_pnl_pips[trade_count] = pnl_pips
                    trade_exit_reasons[trade_count] = exit_reason
                    trade_risk_amounts[trade_count] = risk_amount_at_entry
                    trade_swap_rollover_units[trade_count] = accrued_swap_units_current
                    trade_swap_dollars[trade_count] = accrued_swap_dollars_current
                    trade_mae_pips[trade_count] = mae_pips_current
                    trade_mfe_pips[trade_count] = mfe_pips_current
                    trade_mae_r[trade_count] = mae_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
                    trade_mfe_r[trade_count] = mfe_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
                    trade_time_to_mae_bars[trade_count] = time_to_mae_bars_current
                    trade_time_to_mfe_bars[trade_count] = time_to_mfe_bars_current
                    trade_time_underwater_bars[trade_count] = time_underwater_bars_current
                    trade_entry_to_stop_pips[trade_count] = entry_to_stop_pips
                    trade_entry_to_target_pips[trade_count] = entry_to_target_pips
                    trade_count += 1
                    in_position = False

        if in_position and causal_swap_enabled:
            rollover_units = (
                int(rollover_units_arr[i])
                if i < len(rollover_units_arr)
                else 0
            )
            if rollover_units > 0:
                swap_cashflow = (
                    daily_swap_per_lot_current
                    * position_size
                    * rollover_units
                )
                equity += swap_cashflow
                accrued_swap_units_current += rollover_units
                accrued_swap_dollars_current += swap_cashflow

        current_equity = equity
        if in_position:
            mark_exit_price = bid_close if position_direction == 1 else ask_close
            unrealized_pnl, _ = _calc_trade_pnl_numba(
                entry_price, mark_exit_price, position_direction, position_size,
                tick_size, tick_value, pip_size, pip_value,
                use_commission, commission_per_lot
            )
            current_equity = equity + unrealized_pnl

        equity_curve[i] = current_equity
        if current_equity > peak_equity:
            peak_equity = current_equity
        if peak_equity > 0:
            dd = (peak_equity - current_equity) / peak_equity * 100.0
            if dd > max_drawdown:
                max_drawdown = dd

    if in_position:
        half_spread = half_spread_arr[n_bars - 1] if n_bars - 1 < len(half_spread_arr) else 0.0
        if use_spread:
            exit_price = close_arr[n_bars - 1] - half_spread if position_direction == 1 else close_arr[n_bars - 1] + half_spread
        else:
            exit_price = close_arr[n_bars - 1]
        gross_pnl_dollars, pnl_pips = _calc_trade_pnl_numba(
            entry_price, exit_price, position_direction, position_size,
            tick_size, tick_value, pip_size, pip_value,
            use_commission, commission_per_lot
        )
        equity += gross_pnl_dollars
        net_pnl_dollars = gross_pnl_dollars + accrued_swap_dollars_current
        trade_signal_bars[trade_count] = signal_bar
        trade_entry_bars[trade_count] = entry_bar
        trade_exit_bars[trade_count] = n_bars - 1
        trade_directions[trade_count] = position_direction
        trade_entry_prices[trade_count] = entry_price
        trade_exit_prices[trade_count] = exit_price
        trade_sl_prices[trade_count] = stop_loss
        trade_tp_prices[trade_count] = take_profit
        trade_sizes[trade_count] = position_size
        trade_gross_pnl_dollars[trade_count] = gross_pnl_dollars
        trade_pnl_dollars[trade_count] = net_pnl_dollars
        trade_pnl_pips[trade_count] = pnl_pips
        trade_exit_reasons[trade_count] = EXIT_EOD
        trade_risk_amounts[trade_count] = risk_amount_at_entry
        trade_swap_rollover_units[trade_count] = accrued_swap_units_current
        trade_swap_dollars[trade_count] = accrued_swap_dollars_current
        trade_mae_pips[trade_count] = mae_pips_current
        trade_mfe_pips[trade_count] = mfe_pips_current
        trade_mae_r[trade_count] = mae_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
        trade_mfe_r[trade_count] = mfe_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
        trade_time_to_mae_bars[trade_count] = time_to_mae_bars_current
        trade_time_to_mfe_bars[trade_count] = time_to_mfe_bars_current
        trade_time_underwater_bars[trade_count] = time_underwater_bars_current
        trade_entry_to_stop_pips[trade_count] = entry_to_stop_pips
        trade_entry_to_target_pips[trade_count] = entry_to_target_pips
        trade_count += 1
        equity_curve[n_bars - 1] = equity
        if equity > peak_equity:
            peak_equity = equity
        if peak_equity > 0.0:
            dd = (peak_equity - equity) / peak_equity * 100.0
            if dd > max_drawdown:
                max_drawdown = dd
    
    # Return only filled portion of arrays
    return (
        trade_signal_bars[:trade_count],
        trade_entry_bars[:trade_count],
        trade_exit_bars[:trade_count],
        trade_directions[:trade_count],
        trade_entry_prices[:trade_count],
        trade_exit_prices[:trade_count],
        trade_sl_prices[:trade_count],
        trade_tp_prices[:trade_count],
        trade_sizes[:trade_count],
        trade_pnl_dollars[:trade_count],
        trade_pnl_pips[:trade_count],
        trade_exit_reasons[:trade_count],
        trade_risk_amounts[:trade_count],
        equity_curve,
        equity,
        max_drawdown,
        trade_mae_pips[:trade_count],
        trade_mfe_pips[:trade_count],
        trade_mae_r[:trade_count],
        trade_mfe_r[:trade_count],
        trade_time_to_mae_bars[:trade_count],
        trade_time_to_mfe_bars[:trade_count],
        trade_time_underwater_bars[:trade_count],
        trade_entry_to_stop_pips[:trade_count],
        trade_entry_to_target_pips[:trade_count],
        # Cost attribution fields are appended after every legacy result field
        # so direct consumers retain stable positional indexes.
        trade_gross_pnl_dollars[:trade_count],
        trade_swap_rollover_units[:trade_count],
        trade_swap_dollars[:trade_count],
    )


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class PipelineConfig:
    """
    Global pipeline configuration.
    
    Controls all aspects of the Portfolio Manager including:
    - Data directories and paths
    - Train/validation split ratios
    - Backtest settings (capital, risk, costs)
    - Optimization parameters
    - Evaluation thresholds
    """
    # Data directories
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    output_dir: Path = field(default_factory=lambda: Path("./pm_outputs"))
    log_dir: Path = field(default_factory=lambda: Path("./logs"))
    
    # Active-recent M5 workflow:
    # The pipeline uses the latest `max_bars` M5 rows, reserves the oldest
    # `historical_stress_audit_bars` as audit-only context, and lets the
    # newest `active_universe_bars` drive production strategy selection.
    # Production split mode:
    # - calendar_lockbox_v2: common timestamp windows across every native
    #   timeframe: 5y train -> purge -> 2y tuning -> purge -> 2y lockbox.
    #   This is the production default.
    # - chronological_lockbox_v1: legacy fixed-M5-row lockbox geometry.
    # - bleeding_edge: full active universe is used for broad eligibility,
    #   newest `active_stage2_pct` drives optimization up to the live edge,
    #   and the immediately prior `walk_forward_validation_bars` are production
    #   validation with an embargo before optimization.
    # - research_clean_holdout: retains the older academic clean-holdout split.
    historical_stress_audit_bars: int = 50000
    active_universe_bars: int = 250000
    active_stage2_pct: float = 50.0
    # Research-only split modes remain available for reproducibility, but they
    # must be opted into explicitly and can never pass funded-live readiness.
    research_mode: bool = False
    production_split_mode: str = CALENDAR_SPLIT_MODE
    tuning_validation_bars: int = 50000
    final_lockbox_bars: int = 50000
    split_embargo_bars: int = 300
    # Necessary evidence gate, evaluated before feature computation/model search.
    # Calendar production forces these gates on. Non-calendar research modes
    # may explicitly disable them, but are outside this funded-live contract.
    evidence_sufficiency_enabled: bool = True
    evidence_require_native_timeframes: bool = True
    evidence_require_native_provenance: bool = True
    evidence_require_session_coverage: bool = True
    evidence_min_expected_session_coverage: float = 0.95
    evidence_boundary_tolerance_days: float = 7.0
    evidence_purge_days: float = 7.0
    evidence_warmup_days: float = 400.0
    # Optional deterministic research/as-of cutoff. Empty means use current
    # UTC and then cap to the last fully closed native M5 bar.
    evidence_as_of_utc: str = ""
    evidence_window_years: Dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_WINDOW_YEARS)
    )
    # V3 acquisition and model-selection geometry: every timeframe is sourced
    # independently; tuning/final remain on one concurrent clock while only
    # optimizer-train depth may differ by specialist timeframe.
    evidence_timeframe_window_years: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: {
            timeframe: dict(partitions)
            for timeframe, partitions in DEFAULT_TIMEFRAME_WINDOW_YEARS.items()
        }
    )
    native_history_window_contract: str = "fxpm-timeframe-evidence-v3"
    evidence_min_native_bars: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: {
            timeframe: dict(partitions)
            for timeframe, partitions in DEFAULT_MIN_NATIVE_BARS.items()
        }
    )
    walk_forward_validation_bars: int = 50000
    clean_holdout_pct: float = 0.0  # Research-only percent of stage2 reserved as newest unseen holdout.
    overlap_pct: float = 10.0  # Warmup window as a fraction of the active universe size

    # Backtest settings
    initial_capital: float = 10000.0
    risk_per_trade_pct: float = 1.0
    # Fixed research/discovery sizing. This is intentionally separate from
    # operator/live risk so winner ranking stays invariant across account modes.
    discovery_risk_per_trade_pct: float = 1.0

    # Cost modeling
    # - Spread: fixed/average per-symbol from config.json by default. Evidence
    #   modes can use historical bar-level Spread columns once broker point/pip
    #   units are trusted.
    # - Commission: per-lot fee (use_commission=True). Set to False for
    #   spread-only accounts where commission is embedded in the spread.
    # - Slippage: applied adversely to market entry and SL exits. TP exits are
    #   limit fills and do not incur slippage.
    # - Swap: modeled from InstrumentSpec.swap_long/swap_short when
    #   swap_model_enabled=True. Triple rollover is applied on the configured
    #   broker rollover weekday.
    use_spread: bool = True
    use_commission: bool = True
    use_slippage: bool = True
    slippage_pips: float = 0.5
    backtest_spread_mode: str = "fixed_spec"  # fixed_spec|historical_bar|historical_max|conservative_max|stress_p90|stress_p95|stress_max
    swap_model_enabled: bool = True
    swap_model_triple_rollover_day: str = "wednesday"
    swap_model_min_holding_hours: float = 20.0
    
    # Optimization settings
    max_param_combos: int = 150
    min_trades: int = 25
    optimization_max_workers: int = 1  # 1 = sequential
    feature_cache_max_entries: int = 24  # 0 disables in-memory feature-frame caching
    feature_disk_cache_enabled: bool = False
    feature_disk_cache_dir: str = ""
    feature_disk_cache_max_gb: float = 0.0
    feature_disk_cache_min_free_gb: float = 4.0
    feature_disk_cache_precompute_enabled: bool = False
    feature_disk_cache_precompute_workers: int = 1
    screening_atr_anchor_enabled: bool = False
    screening_atr_anchor_multipliers: List[float] = field(default_factory=lambda: [2.0])

    # Optional family-size-aware search budget.
    # When enabled, the per-strategy Optuna trial budget scales with the cartesian
    # size of `get_param_grid()` instead of a flat `max_param_combos`. This prevents
    # large families (e.g., EMARibbonADX, 46k+ combos) from receiving a coarser
    # search than a 48-combo family. Disabled by default so existing behavior is
    # preserved; opt-in per operator policy. Effective trials per strategy:
    #     n = clamp(min, max, max(min, round(coverage * cartesian_size)))
    optuna_family_size_aware_budget: bool = False
    optuna_min_trials_per_strategy: int = 50
    optuna_max_trials_per_strategy: int = 5000
    optuna_target_coverage_pct: float = 0.10  # 10% of cartesian; capped by max

    # FX backtester-aligned optimization/validation thresholds
    fx_opt_min_trades: int = 15          # used during param search (train only)
    fx_val_min_trades: int = 15          # minimum validation trades required
    fx_val_max_drawdown: float = 12.0    # maximum validation drawdown (%)
    fx_val_sharpe_override: float = 0.3  # validation Sharpe can override robustness threshold
    # Generalization controls (fx_backtester)
    fx_selection_top_k: int = 5          # validate only top-K strategy/timeframe candidates
    fx_opt_top_k: int = 5                # validate only top-K parameter combos
    fx_gap_penalty_lambda: float = 0.70  # penalty strength for train->val score gaps
    fx_robustness_boost: float = 0.15    # robustness multiplier weight (0..0.5 recommended)
    fx_min_robustness_ratio: float = 0.80  # minimum val_score/train_score ratio for validation
    # Multiplicative variance / stability penalty in fx_generalization_score: exp(-k * std_weekly_return)
    # Set 0.0 to disable the validation-side stability penalty.
    fx_stability_penalty_k: float = 2.0

    regime_validation_top_k: int = 5
    regime_min_val_return_dd_ratio: float = 1.0
    scoring_use_continuous_dd: bool = True
    scoring_use_sortino_blend: bool = True
    scoring_use_tail_risk: bool = True
    scoring_use_consistency: bool = True
    # More trades are not evidence of more edge once costs and dependence are
    # modeled. Keep the old bonus available to research, but never enable it by
    # default in the production selector.
    scoring_use_trade_frequency_bonus: bool = False
    legacy_confidence_bonus_cap: float = 0.08
    regime_scoring_model: str = "legacy"  # legacy|legacy_confidence|specialist_spokes|specialist_spokes_v2|specialist_spokes_asym|specialist_spokes_threadspark
    regime_trade_stability_floor: float = 0.7  # 0.7 legacy; 0.9 is Phase-1 experiment candidate
    specialist_spokes_v2_cost_frequency_strength: float = 0.50
    specialist_spokes_v2_default_fallback_enabled: bool = True
    specialist_spokes_v2_allow_promote_small_default: bool = True
    specialist_spokes_v2_max_validation_sharpe: float = 0.0

    # Global false-discovery controls are part of the default production
    # contract. Research runs may disable them explicitly, but live readiness
    # will reject such artifacts.
    false_discovery_control_enabled: bool = True
    false_discovery_min_dsr: float = 0.95
    false_discovery_confidence_floor: float = 0.35
    false_discovery_scope_power: float = 0.25
    false_discovery_symbol_count: int = 0
    false_discovery_min_daily_observations: int = 252
    false_discovery_min_effective_observations: float = 60.0
    false_discovery_max_pbo: float = 0.50
    false_discovery_pbo_min_candidates: int = 5
    false_discovery_pbo_blocks: int = 10
    false_discovery_require_bootstrap: bool = True
    false_discovery_bootstrap_repetitions: int = 300
    false_discovery_bootstrap_confidence: float = 0.95
    false_discovery_bootstrap_min_mean_lower: float = 0.0
    false_discovery_bootstrap_min_pf_lower: float = 1.0
    false_discovery_bootstrap_max_dd_upper: float = 0.20
    false_discovery_holdout_policy: str = "block"  # off|promote_small|block
    false_discovery_holdout_min_trades: int = 5
    false_discovery_holdout_min_profit_factor: float = 1.0
    false_discovery_holdout_min_return_pct: float = 0.0
    false_discovery_holdout_max_drawdown_pct: float = 20.0
    lockbox_one_shot_registry_enabled: bool = True
    lockbox_access_registry_path: str = ""
    oos_validation_mode: str = "fixed_holdout"  # fixed_holdout|purged_kfold
    oos_kfold_splits: int = 5
    oos_embargo_bars: int = 1
    oos_min_trades: int = 5
    oos_aggregate_policy: str = "aggregate_with_worst_fold_floor"  # all_folds|aggregate_with_worst_fold_floor
    oos_evidence_export_enabled: bool = True
    oos_evidence_periods: int = 64
    oos_evidence_min_periods: int = 8
    oos_evidence_dirname: str = "oos_evidence"
    oos_evidence_trade_path_max_points: int = 256
    oos_evidence_include_execution_cost_survival: bool = True
    oos_evidence_execution_stress_mode: str = "stress_p95"
    oos_evidence_execution_cost_max_candidates_per_tf: int = 0  # 0 = no cap when enabled
    threadspark_selection_surface: str = "validation"  # validation|clean_holdout
    threadspark_use_historical_cost_evidence: bool = True
    threadspark_historical_cost_scope: str = "workflow"  # workflow|audit
    threadspark_historical_chunk_count: int = 5

    # Timeframes to evaluate
    timeframes: List[str] = field(default_factory=lambda: ['M5', 'M15', 'M30', 'H1', 'H4', 'D1'])
    
    
    # Live trading data window
    live_bars_count: int = 1500      # bars loaded per timeframe during live trading
    live_min_bars: int = 300         # minimum bars required to evaluate a timeframe in live trading
    live_loop_trigger_mode: str = "bar"  # bar|scheduled
    live_bar_poll_seconds: float = 0.25  # CPU-idle polling interval; decisions are gated by MT5 bar changes
    live_tick_poll_seconds: float = 0.25  # backward-compatible alias for older configs
    live_bar_settle_seconds: int = 5
    live_stale_retry_seconds: int = 15
    live_artifact_drift_policy: str = "block"  # block|warn|ignore
    live_config_expiry_grace_minutes: int = 0
    manual_new_entries_enabled: bool = True
    manual_kill_switch_path: str = ""
    production_require_promotion_gate: bool = True
    # ``ledger_status`` and the old multi-file review mode remain readable for
    # historical/research runs.  The shipped funded profile uses
    # ``immutable_exact`` and live readiness refuses every weaker mode.
    production_promotion_gate_mode: str = "ledger_status"  # immutable_exact|external_review_exact|ledger_status
    production_promotion_authorization_path: str = ""
    production_promotion_authorization_sha256: str = ""
    production_promotion_authorization_max_age_days: int = 31
    production_promotion_review_path: str = ""
    production_promotion_cell_safety_path: str = ""
    production_completion_marker_path: str = ""
    production_expected_promote_cells: List[str] = field(default_factory=list)
    live_allowed_promotion_statuses: List[str] = field(default_factory=lambda: ["PROMOTED"])
    production_replay_audit_enabled: bool = True
    production_replay_audit_mode: str = "apply"  # report_only|apply
    production_replay_audit_starting_balance: float = 10000.0
    production_replay_audit_min_profit_factor: float = 1.05
    production_replay_audit_max_drawdown_pct: float = 30.0
    production_replay_audit_min_trades: int = 30
    production_replay_audit_block_unverified: bool = True
    production_replay_audit_fail_on_error: bool = True
    production_replay_audit_out_dir: str = ""
    # Funded live additionally requires one sealed, content-addressed replay of
    # every promoted cell through a synchronized shared-capital account.  The
    # diagnostic per-cell replay above cannot satisfy this portfolio contract.
    production_prelockbox_roster_path: str = ""
    production_prelockbox_roster_sha256: str = ""
    production_cap_calibration_path: str = ""
    production_cap_calibration_sha256: str = ""
    production_portfolio_replay_evidence_required: bool = True
    production_portfolio_replay_evidence_path: str = ""
    production_portfolio_replay_required_starting_balances: List[float] = field(
        default_factory=lambda: [1000.0, 10000.0]
    )
    production_portfolio_replay_min_calendar_days: int = 730
    production_portfolio_replay_min_positions: int = 100
    production_portfolio_replay_min_profit_factor: float = 1.05
    production_portfolio_replay_min_return_pct: float = 0.0
    production_portfolio_replay_max_drawdown_pct: float = 20.0
    production_portfolio_replay_require_mtm: bool = True
    production_require_full_symbol_ledger: bool = True
    live_allow_partial_symbol_ledger: bool = False
    production_symbol_pool_target_count: int = 0
    live_min_account_equity: float = 0.0
    live_min_account_equity_currency: str = ""
    reconnect_max_attempts: int = 5
    reconnect_attempt_interval_seconds: int = 5
    reconnect_failure_retry_seconds: int = 30
    live_risk_scalars_enabled: bool = False
    # Tri-state mode for the risk-scalar stack: off = disabled,
    # shadow = log-only, on = authoritative. The legacy boolean still
    # maps True -> "on" when the mode is left at "off".
    live_risk_scalars_mode: str = "off"
    # Tri-state mode for the MarketDrivenExitPack: off = disabled,
    # paper = log-only, on = authoritative.
    market_driven_exit_pack_mode: str = "off"
    portfolio_observatory_enabled: bool = False
    institutional_supervisor_enabled: bool = True
    institutional_supervisor_mode: str = "circuit_breaker"  # off|observe|circuit_breaker
    portfolio_control_policy: str = "hard_breakpoints_only"
    max_total_open_risk_pct: float = 8.0
    # Gross, non-netted factor exposure (currencies/equity beta/metals/etc.).
    max_factor_group_open_risk_pct: float = 2.0
    max_new_entries_when_total_open_risk_above_pct: float = 0.0
    open_risk_action: str = "observe"  # observe|pause_new_entries|derate
    daily_loss_hard_block_pct: float = 0.0
    session_loss_hard_block_pct: float = 0.0
    weekly_loss_hard_block_pct: float = 0.0
    open_drawdown_hard_block_pct: float = 0.0
    hard_block_reopen_mode: str = "when_clear"
    cycle_drift_monitor_enabled: bool = True
    cycle_drift_mode: str = "lightweight"  # off|observe|lightweight
    cycle_drift_min_trades: int = 6
    cycle_drift_consecutive_losses: int = 5
    cycle_drift_execution_slippage_mult: float = 2.5
    cycle_drift_spread_degradation_mult: float = 2.0
    cycle_drift_action: str = "pause_context"  # observe|pause_context|derate_context
    # Durable position-close accounting. This subsystem is observation-only:
    # it can pause/derate an exact cell through CycleDriftMonitor, but it has no
    # order placement or automatic-promotion capability.
    live_outcome_feedback_enabled: bool = True
    live_outcome_require_position_history: bool = True
    live_outcome_feed_cycle_drift: bool = True
    live_outcome_ledger_path: str = ""
    live_outcome_state_path: str = ""
    live_outcome_volume_tolerance: float = 1e-8
    walk_forward_audit_enabled: bool = True
    walk_forward_audit_windows: int = 3
    walk_forward_audit_mode: str = "severe_fragility_only"
    walk_forward_promotion_policy: str = "promotion_gate"
    walk_forward_live_blocking_enabled: bool = True
    walk_forward_do_not_displace_latest_surface: bool = True
    # PROMOTE_SMALL winners trade live with a derated risk multiplier (0..1).
    promote_small_risk_multiplier: float = 0.5
    # Probation tier (agent_audit/comms/CHANNEL.md [004]): stored-window evidence predicts
    # full-sample profitability poorly (Spearman <= ~0.30), so freshly (re)trained winners
    # trade at reduced risk until forward evidence accrues. Conservative-only: it can only
    # lower the live risk multiplier, never raise it.
    probation_enabled: bool = True
    probation_days: int = 14
    probation_risk_multiplier: float = 0.5
    probation_min_live_outcomes: int = 30
    probation_min_mean_r: float = 0.0
    probation_min_profit_factor: float = 1.0
    local_governance_tournament_enabled: bool = False
    local_governance_live_mode: str = "off"
    local_governance_candidate_policies: List[str] = field(
        default_factory=lambda: [
            "control_fixed",
            "breakeven_1r",
            "stepped_breakeven",
            "atr_trail_capped",
            "chandelier_atr",
            "structure_trail",
            "partial_tp_runner",
            "time_decay_exit",
            "volatility_contraction_exit",
            "regime_flip_reduce",
            "pure_atr",
        ]
    )
    local_governance_require_baseline_improvement: bool = True
    local_governance_min_score_improvement: float = 0.0
    # Governance/risk-policy selection currently runs after the strategy's
    # one-shot holdout evaluation, so enabling it would deploy a policy that the
    # lockbox did not test.  Keep it research-only until the entire selected
    # policy is frozen before lockbox evaluation.
    risk_management_optimization_enabled: bool = False
    risk_management_selection_stage: str = "stage3"
    historical_audit_min_trades: int = 5
    historical_audit_max_drawdown: float = 35.0
    historical_audit_min_profit_factor: float = 0.60
    winner_ledger_path: str = "pm_configs.json"
    daily_loss_advisory_pct: float = 0.0
    session_loss_advisory_pct: float = 0.0
    target_annual_vol: float = 0.10
    execution_spread_filter_enabled: bool = True
    execution_spread_min_edge_mult: float = 1.5
    execution_spread_spike_mult: float = 2.0
    execution_spread_penalty_start_mult: float = 0.5
    regime_profitability_audit_enabled: bool = True
    regime_boundary_audit_enabled: bool = True
    regime_ambiguity_policy: str = "observe_only"
    regime_min_live_gap_warn: float = 0.05
    event_calendar_enabled: bool = False
    event_calendar_mode: str = "observe"
    event_calendar_action: str = "annotate"
    event_calendar_path: str = ""
    event_calendar_high_impact_action: str = "observe"

    # Storage governance
    storage_enabled: bool = True
    storage_observe_only: bool = True
    storage_signal_ledger_enabled: bool = True
    storage_warn_free_gb: float = 15.0
    storage_critical_free_gb: float = 10.0
    storage_pause_entries_below_free_gb: Optional[float] = None
    storage_measure_interval_seconds: int = 300
    storage_housekeeping_interval_seconds: int = 900
    storage_metaquotes_review_interval_seconds: int = 21600
    storage_write_protect_minutes: int = 5
    storage_local_data_first_enabled: bool = True
    storage_live_sync_bars: int = 3000
    storage_live_sync_overlap_bars: int = 100
    storage_live_cache_max_age_days: int = 7
    storage_delta_sync_overlap_minutes: int = 1440
    storage_resample_cache_max_gb: float = 1.0
    storage_resample_cache_max_age_days: int = 7
    storage_logs_keep_days: int = 14
    storage_pm_outputs_keep_days: int = 14
    storage_pm_outputs_keep_count: int = 30
    storage_metaquotes_cleanup_enabled: bool = False
    storage_metaquotes_root: str = ""
    storage_metaquotes_active_root_allowlist: List[str] = field(default_factory=list)
    storage_metaquotes_demo_servers: List[str] = field(default_factory=lambda: ["FBS-Demo", "MetaQuotes-Demo"])
    storage_metaquotes_stale_tester_days: int = 14
    storage_manifest_max_mb: float = 16.0
    storage_manifest_keep_rotations: int = 2
    storage_manifest_dedupe_hours: float = 24.0
    storage_signal_ledger_compress_after_months: int = 2
    state_filename: str = "storage_state.json"
    manifest_filename: str = "storage_manifest.jsonl"
    ledger_filename: str = "signal_ledger.jsonl"

    # Retrain periods to evaluate (research-only; production uses the fixed calendar schedule below)
    retrain_periods: List[int] = field(default_factory=lambda: [7, 14, 30, 60, 90])
    fixed_retrain_days: int = 14
    production_retrain_mode: str = "auto"
    production_retrain_interval_weeks: int = 4
    production_retrain_weekday: str = "sunday"
    production_retrain_time: str = "00:01"
    production_retrain_anchor_date: str = "2026-03-29"
    production_retrain_poll_seconds: int = 60
    
    # Maximum base M5 bars to load per symbol for optimization.
    max_bars: int = 300000
    # Data-completeness gate for production optimization. The fractional form is
    # the source of truth: a symbol is valid when its downloaded M5 history is at
    # least this fraction of `max_bars` (e.g. 0.85 -> 255k of 300k). It keeps the
    # 300k standard the majority meet while admitting close-but-short symbols
    # (ES35 ~267k) instead of failing them on a hard 300k rule. `optimization_min_m5_bars`
    # remains an optional absolute floor used only when the fraction is unset.
    optimization_min_m5_bars_fraction: float = 0.0
    optimization_min_m5_bars: int = 0
    historical_fetch_retry_attempts: int = 1
    historical_fetch_retry_sleep_seconds: float = 2.0
    historical_fetch_deep_backfill_enabled: bool = False
    historical_fetch_deep_backfill_years: int = 18
    
    # Regime-aware optimization settings
    regime_min_train_trades: int = 25  # Minimum trades per regime bucket in training
    regime_min_val_trades: int = 15    # Minimum trades per regime bucket in validation
    regime_timeframe_val_trade_floors_enabled: bool = False
    regime_timeframe_val_trade_floors: Dict[str, int] = field(
        default_factory=lambda: {
            "D1": 15,
            "H4": 25,
            "H1": 25,
            "M30": 30,
            "M15": 30,
            "M5": 30,
        }
    )
    regime_params_file: str = "regime_params.json"  # Path to tuned regime params
    live_require_tuned_regime_params: bool = True  # Live entries require an explicit tuned params row
    # Live-only optional TP overlay. Discovery/backtesting keeps this neutral so
    # the tournament trades the SL/TP surface that actually won.
    regime_tp_multipliers: Dict[str, float] = field(default_factory=lambda: {
        "TREND": 1.0,
        "BREAKOUT": 1.0,
        "RANGE": 1.0,
        "CHOP": 1.0,
    })
    
    # Hyperparameter tuning settings for regime optimization
    regime_enable_hyperparam_tuning: bool = True  # Enable hyperparameter tuning in regime optimization
    regime_hyperparam_top_k: int = 3  # Top K strategies to tune per regime (screening phase)
    regime_hyperparam_max_combos: int = 150  # Max param combinations to test per strategy
    # Equal-budget train-only B0 probe before any family is shortlisted. This
    # prevents a poor default/ATR anchor from eliminating a family whose tuned
    # region is strong. Actual per-cell trials are min(grid size, this budget),
    # with mandatory default/ATR anchors included and deduplicated.
    regime_family_probe_trials: int = 5
    
    # Validation profitability gates for stored regime winners.
    regime_min_val_profit_factor: float = 1.05   # Minimum validation PF to be stored as winner
    regime_min_val_return_pct: float = 5.0       # Minimum validation return % floor for validated winners
    regime_allow_losing_winners: bool = False    # If True, allows PF < 1 (not recommended)
    regime_no_winner_marker: str = "NO_TRADE"    # Strategy name for "no valid winner" state

    # ===== POST-B0 / PRE-DEEP-TUNING ELIGIBILITY GATES =====
    # Applied to each family-regime cell's equal-budget train-only B0 winner.
    # Purpose: skip truly catastrophic cells before deeper independent studies.
    # Must stay lenient because B0 is a bounded probe, not exhaustive search.
    train_min_profit_factor: float = 0.5     # Reject only extremely bad PF
    train_min_return_pct: float = -30.0      # Allow moderate losses (tuning may fix)
    train_max_drawdown: float = 60.0         # Reject only catastrophic blowups

    # ===== WEAK-TRAIN EXCEPTIONAL VALIDATION =====
    # When training PF < 1.0 or negative return, require exceptional validation
    # to still allow the strategy as a winner.
    exceptional_val_profit_factor: float = 1.5  # Min val PF to override weak train
    exceptional_val_return_pct: float = 10.0    # Min val return % to override weak train

    # ===== LIVE RISK POLICY (WINNERS-ONLY) =====
    # Risk per trade comes from PositionConfig.risk_per_trade_pct.
    # PositionConfig.max_risk_pct acts as the hard safety cap.
    min_trade_risk_pct: float = 0.1        # minimum non-zero risk for a placed trade

    # ===== MARGIN PROTECTION (BLACK SWAN GUARD) =====
    # Cycle-based margin protection layer (thresholds are config-driven).
    # Uses MT5-native margin_level (equity/margin*100). No sleep/cooldown logic.
    margin_entry_block_level: float = 200.0       # 50% used-margin ceiling for new entries
    margin_recovery_start_level: float = 150.0    # start forced closures below this %
    margin_panic_level: float = 120.0              # aggressive forced closures below this %
    margin_reopen_level: float = 300.0            # reopen only after recovery to 33.33% use
    margin_recovery_closes_per_cycle: int = 1     # max forced closes per cycle in RECOVERY
    margin_panic_closes_per_cycle: int = 3        # max forced closes per cycle in PANIC
    margin_reopen_cooldown_minutes: float = 15.0  # block all new entries for N minutes after any forced close (flash-crash guard)

    
    # ===== POSITION TIMEFRAME OVERRIDES =====
    # Manual overrides for position timeframe inference when comment/magic
    # decoding fails.  Keys: "magic:<number>" or "ticket:<number>".
    # Values: timeframe string (e.g. "D1", "H4").
    position_timeframe_overrides: Dict[str, str] = field(default_factory=dict)

    # Allow up to two concurrent trades per symbol: one D1 + one lower-TF.
    allow_d1_plus_lower_tf: bool = True          # Exact one-D1 + one-lower topology only
    d1_secondary_risk_multiplier: float = 1.0   # Risk for second trade when D1 is open
    max_combined_risk_pct: float = 1.0          # Max combined risk per symbol (D1 + lower)
    secondary_trade_max_risk_pct: float = 0.25  # hard cap for the secondary trade
    
    # Compatibility alias for older artifacts/metrics. Production validity now
    # comes from the fixed calendar schedule above.
    optimization_valid_days: int = 14

    def __post_init__(self):
        """Ensure directories exist and sync aliases."""
        self.data_dir = Path(self.data_dir)
        self.output_dir = Path(self.output_dir)
        self.log_dir = Path(self.log_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        try:
            margin_reopen = float(self.margin_reopen_level)
            margin_block = float(self.margin_entry_block_level)
            margin_recovery = float(self.margin_recovery_start_level)
            margin_panic = float(self.margin_panic_level)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("margin protection thresholds must be finite numbers") from exc
        if not all(math.isfinite(value) for value in (
            margin_reopen, margin_block, margin_recovery, margin_panic
        )) or not (margin_reopen >= margin_block > margin_recovery > margin_panic > 0.0):
            raise ValueError(
                "margin thresholds must satisfy "
                "margin_reopen_level >= margin_entry_block_level > "
                "margin_recovery_start_level > margin_panic_level > 0"
            )
        self.margin_reopen_level = margin_reopen
        self.margin_entry_block_level = margin_block
        self.margin_recovery_start_level = margin_recovery
        self.margin_panic_level = margin_panic
        # Clamp workers to sane minimum
        try:
            self.optimization_max_workers = max(1, int(self.optimization_max_workers))
        except (TypeError, ValueError, OverflowError):
            self.optimization_max_workers = 1
        try:
            self.feature_cache_max_entries = max(0, int(self.feature_cache_max_entries))
        except (TypeError, ValueError, OverflowError):
            self.feature_cache_max_entries = 24
        self.feature_disk_cache_enabled = bool(self.feature_disk_cache_enabled)
        self.feature_disk_cache_dir = str(self.feature_disk_cache_dir or "").strip()
        try:
            self.feature_disk_cache_max_gb = max(0.0, float(self.feature_disk_cache_max_gb))
        except (TypeError, ValueError, OverflowError):
            self.feature_disk_cache_max_gb = 0.0
        try:
            self.feature_disk_cache_min_free_gb = max(0.0, float(self.feature_disk_cache_min_free_gb))
        except (TypeError, ValueError, OverflowError):
            self.feature_disk_cache_min_free_gb = 4.0
        self.feature_disk_cache_precompute_enabled = bool(self.feature_disk_cache_precompute_enabled)
        try:
            self.feature_disk_cache_precompute_workers = max(1, int(self.feature_disk_cache_precompute_workers))
        except (TypeError, ValueError, OverflowError):
            self.feature_disk_cache_precompute_workers = 1
        try:
            anchors = []
            for value in self.screening_atr_anchor_multipliers or []:
                anchor = float(value)
                if math.isfinite(anchor) and anchor > 0:
                    anchors.append(round(anchor, 6))
            self.screening_atr_anchor_multipliers = sorted(set(anchors)) or [2.0]
        except (TypeError, ValueError, OverflowError):
            self.screening_atr_anchor_multipliers = [2.0]
        self.screening_atr_anchor_enabled = bool(self.screening_atr_anchor_enabled)
        try:
            self.regime_family_probe_trials = max(1, int(self.regime_family_probe_trials))
        except (TypeError, ValueError, OverflowError):
            self.regime_family_probe_trials = 5

        try:
            risk = float(self.discovery_risk_per_trade_pct)
            self.discovery_risk_per_trade_pct = risk if math.isfinite(risk) and risk > 0 else 1.0
        except (TypeError, ValueError, OverflowError):
            self.discovery_risk_per_trade_pct = 1.0

        # Clamp regime validation descent depth to a safe minimum.
        try:
            self.regime_validation_top_k = max(1, int(self.regime_validation_top_k))
        except (TypeError, ValueError, OverflowError):
            self.regime_validation_top_k = 5

        # Clamp return/DD hard gate threshold to a safe positive value.
        try:
            ratio = float(self.regime_min_val_return_dd_ratio)
            self.regime_min_val_return_dd_ratio = ratio if math.isfinite(ratio) and ratio > 0 else 1.0
        except (TypeError, ValueError, OverflowError):
            self.regime_min_val_return_dd_ratio = 1.0
        self.live_require_tuned_regime_params = bool(
            getattr(self, "live_require_tuned_regime_params", True)
        )
        try:
            normalized_regime_tp: Dict[str, float] = {}
            for key, value in (self.regime_tp_multipliers or {}).items():
                regime_key = str(key).strip().upper()
                multiplier = float(value)
                if regime_key and math.isfinite(multiplier) and multiplier > 0:
                    normalized_regime_tp[regime_key] = multiplier
            self.regime_tp_multipliers = normalized_regime_tp or {
                "TREND": 1.0,
                "BREAKOUT": 1.0,
                "RANGE": 1.0,
                "CHOP": 1.0,
            }
        except (TypeError, ValueError, OverflowError):
            self.regime_tp_multipliers = {
                "TREND": 1.0,
                "BREAKOUT": 1.0,
                "RANGE": 1.0,
                "CHOP": 1.0,
            }

        self.production_retrain_mode = str(self.production_retrain_mode or "auto").strip().lower()
        if self.production_retrain_mode not in {"auto", "notify", "off"}:
            raise ValueError(
                f"Invalid production_retrain_mode: {self.production_retrain_mode}. "
                "Use 'auto', 'notify', or 'off'."
            )
        try:
            self.production_retrain_interval_weeks = max(1, int(self.production_retrain_interval_weeks))
        except Exception:
            self.production_retrain_interval_weeks = 4
        self.production_retrain_weekday = str(self.production_retrain_weekday or "sunday").strip().lower()
        if self.production_retrain_weekday not in _WEEKDAY_NAME_TO_INDEX:
            raise ValueError(
                f"Invalid production_retrain_weekday: {self.production_retrain_weekday}. "
                "Use a weekday name such as 'sunday'."
            )
        try:
            parsed_time = datetime.strptime(str(self.production_retrain_time or "00:01").strip(), "%H:%M")
            self.production_retrain_time = parsed_time.strftime("%H:%M")
        except Exception as exc:
            raise ValueError(f"Invalid production_retrain_time: {self.production_retrain_time}") from exc
        try:
            anchor_date = datetime.strptime(str(self.production_retrain_anchor_date), "%Y-%m-%d")
        except Exception as exc:
            raise ValueError(
                f"Invalid production_retrain_anchor_date: {self.production_retrain_anchor_date}"
            ) from exc
        if anchor_date.weekday() != self.retrain_weekday_index:
            raise ValueError(
                "production_retrain_anchor_date must fall on the configured production_retrain_weekday"
            )
        try:
            self.production_retrain_poll_seconds = max(1, int(self.production_retrain_poll_seconds))
        except Exception:
            self.production_retrain_poll_seconds = 60
        self.production_require_full_symbol_ledger = bool(self.production_require_full_symbol_ledger)
        self.live_allow_partial_symbol_ledger = bool(
            getattr(self, "live_allow_partial_symbol_ledger", False)
        )
        try:
            self.production_symbol_pool_target_count = max(0, int(self.production_symbol_pool_target_count))
        except Exception:
            self.production_symbol_pool_target_count = 0
        try:
            min_equity = float(self.live_min_account_equity)
            self.live_min_account_equity = min_equity if math.isfinite(min_equity) and min_equity > 0 else 0.0
        except (TypeError, ValueError, OverflowError):
            self.live_min_account_equity = 0.0
        self.live_min_account_equity_currency = str(self.live_min_account_equity_currency or "").strip().upper()
        self.execution_spread_filter_enabled = bool(self.execution_spread_filter_enabled)
        self.live_risk_scalars_enabled = bool(self.live_risk_scalars_enabled)
        raw_loop_mode = getattr(self, "live_loop_trigger_mode", "bar")
        if isinstance(raw_loop_mode, str):
            loop_mode = raw_loop_mode.strip().lower()
        else:
            loop_mode = "bar"
        loop_mode_aliases = {
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
        loop_mode = loop_mode_aliases.get(loop_mode, loop_mode)
        if loop_mode not in {"bar", "scheduled"}:
            logger.warning(
                "Invalid live_loop_trigger_mode=%r; falling back to 'bar'. "
                "Allowed values: bar | scheduled.",
                raw_loop_mode,
            )
            loop_mode = "bar"
        self.live_loop_trigger_mode = loop_mode
        try:
            self.live_bar_poll_seconds = max(0.05, float(self.live_bar_poll_seconds))
        except Exception:
            try:
                self.live_bar_poll_seconds = max(0.05, float(self.live_tick_poll_seconds))
            except Exception:
                self.live_bar_poll_seconds = 0.25
        self.live_tick_poll_seconds = self.live_bar_poll_seconds
        raw_artifact_policy = getattr(self, "live_artifact_drift_policy", "block")
        if isinstance(raw_artifact_policy, str):
            artifact_policy = raw_artifact_policy.strip().lower()
        else:
            artifact_policy = "block"
        policy_aliases = {"strict": "block", "allow": "ignore", "log": "warn"}
        artifact_policy = policy_aliases.get(artifact_policy, artifact_policy)
        if artifact_policy not in {"block", "warn", "ignore"}:
            logger.warning(
                "Invalid live_artifact_drift_policy=%r; falling back to 'block'. "
                "Allowed values: block | warn | ignore.",
                raw_artifact_policy,
            )
            artifact_policy = "block"
        self.live_artifact_drift_policy = artifact_policy
        try:
            self.live_config_expiry_grace_minutes = max(0, int(self.live_config_expiry_grace_minutes))
        except Exception:
            self.live_config_expiry_grace_minutes = 0
        # Normalize the tri-state risk-scalar mode once at startup.
        _raw_mode = getattr(self, "live_risk_scalars_mode", "off")
        if isinstance(_raw_mode, str):
            _norm_mode = _raw_mode.strip().lower()
        else:
            _norm_mode = "off"
        if _norm_mode not in {"off", "shadow", "on"}:
            logger.warning(
                "Invalid live_risk_scalars_mode=%r; falling back to 'off'. "
                "Allowed values: off | shadow | on.",
                _raw_mode,
            )
            _norm_mode = "off"
        self.live_risk_scalars_mode = _norm_mode
        # Normalize the tri-state market-driven exit-pack mode once at startup.
        _raw_exit_mode = getattr(self, "market_driven_exit_pack_mode", "off")
        if isinstance(_raw_exit_mode, str):
            _norm_exit_mode = _raw_exit_mode.strip().lower()
        else:
            _norm_exit_mode = "off"
        if _norm_exit_mode not in {"off", "paper", "on"}:
            logger.warning(
                "Invalid market_driven_exit_pack_mode=%r; falling back to 'off'. "
                "Allowed values: off | paper | on.",
                _raw_exit_mode,
            )
            _norm_exit_mode = "off"
        self.market_driven_exit_pack_mode = _norm_exit_mode
        self.institutional_supervisor_enabled = bool(self.institutional_supervisor_enabled)
        _raw_supervisor_mode = getattr(self, "institutional_supervisor_mode", "circuit_breaker")
        _norm_supervisor_mode = (
            _raw_supervisor_mode.strip().lower()
            if isinstance(_raw_supervisor_mode, str) else "circuit_breaker"
        )
        _norm_supervisor_mode = {
            "disabled": "off",
            "false": "off",
            "log": "observe",
            "shadow": "observe",
            "enabled": "circuit_breaker",
            "on": "circuit_breaker",
            "block": "circuit_breaker",
        }.get(_norm_supervisor_mode, _norm_supervisor_mode)
        if _norm_supervisor_mode not in {"off", "observe", "circuit_breaker"}:
            logger.warning(
                "Invalid institutional_supervisor_mode=%r; falling back to 'circuit_breaker'. "
                "Allowed values: off | observe | circuit_breaker.",
                _raw_supervisor_mode,
            )
            _norm_supervisor_mode = "circuit_breaker"
        self.institutional_supervisor_mode = _norm_supervisor_mode
        _raw_portfolio_policy = getattr(self, "portfolio_control_policy", "hard_breakpoints_only")
        _portfolio_policy = (
            _raw_portfolio_policy.strip().lower()
            if isinstance(_raw_portfolio_policy, str) else "hard_breakpoints_only"
        )
        _portfolio_policy = {
            "hard_breakpoints": "hard_breakpoints_only",
            "breakpoints": "hard_breakpoints_only",
            "circuit_breaker": "hard_breakpoints_only",
            "disabled": "off",
        }.get(_portfolio_policy, _portfolio_policy)
        if _portfolio_policy not in {"off", "hard_breakpoints_only"}:
            logger.warning(
                "Invalid portfolio_control_policy=%r; falling back to 'hard_breakpoints_only'.",
                _raw_portfolio_policy,
            )
            _portfolio_policy = "hard_breakpoints_only"
        self.portfolio_control_policy = _portfolio_policy

        def _clamp_float_attr(name: str, default: float, low: float = 0.0, high: Optional[float] = None) -> None:
            try:
                value = float(getattr(self, name, default))
                if not math.isfinite(value):
                    raise ValueError
                value = max(low, value)
                if high is not None:
                    value = min(high, value)
                setattr(self, name, value)
            except (TypeError, ValueError, OverflowError):
                setattr(self, name, default)

        def _clamp_int_attr(name: str, default: int, low: int = 0) -> None:
            try:
                setattr(self, name, max(low, int(getattr(self, name, default))))
            except (TypeError, ValueError, OverflowError):
                setattr(self, name, default)

        for _name, _default in {
            "max_total_open_risk_pct": 8.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_new_entries_when_total_open_risk_above_pct": 0.0,
            "daily_loss_hard_block_pct": 0.0,
            "session_loss_hard_block_pct": 0.0,
            "weekly_loss_hard_block_pct": 0.0,
            "open_drawdown_hard_block_pct": 0.0,
            "cycle_drift_execution_slippage_mult": 2.5,
            "cycle_drift_spread_degradation_mult": 2.0,
            "swap_model_min_holding_hours": 20.0,
            "regime_min_live_gap_warn": 0.05,
            "legacy_confidence_bonus_cap": 0.08,
        }.items():
            if _name == "legacy_confidence_bonus_cap":
                _clamp_float_attr(_name, _default, low=0.0, high=0.50)
            else:
                _clamp_float_attr(_name, _default)
        _clamp_int_attr("cycle_drift_min_trades", 6, low=1)
        _clamp_int_attr("cycle_drift_consecutive_losses", 5, low=1)

        _open_risk_action = str(getattr(self, "open_risk_action", "pause_new_entries") or "").strip().lower()
        if _open_risk_action not in {"observe", "pause_new_entries", "derate"}:
            _open_risk_action = "pause_new_entries"
        self.open_risk_action = _open_risk_action
        _reopen_mode = str(getattr(self, "hard_block_reopen_mode", "when_clear") or "").strip().lower()
        if _reopen_mode not in {"when_clear", "manual_or_next_cycle", "manual", "next_cycle"}:
            _reopen_mode = "when_clear"
        self.hard_block_reopen_mode = _reopen_mode
        self.cycle_drift_monitor_enabled = bool(self.cycle_drift_monitor_enabled)
        _drift_mode = str(getattr(self, "cycle_drift_mode", "lightweight") or "").strip().lower()
        _drift_mode = {"log": "observe", "shadow": "observe", "on": "lightweight"}.get(_drift_mode, _drift_mode)
        if _drift_mode not in {"off", "observe", "lightweight"}:
            _drift_mode = "lightweight"
        self.cycle_drift_mode = _drift_mode
        _drift_action = str(getattr(self, "cycle_drift_action", "pause_context") or "").strip().lower()
        if _drift_action not in {"observe", "pause_context", "derate_context"}:
            _drift_action = "pause_context"
        self.cycle_drift_action = _drift_action
        self.portfolio_observatory_enabled = bool(self.portfolio_observatory_enabled)
        self.walk_forward_audit_enabled = bool(self.walk_forward_audit_enabled)
        self.walk_forward_do_not_displace_latest_surface = bool(self.walk_forward_do_not_displace_latest_surface)
        self.walk_forward_audit_mode = str(
            getattr(self, "walk_forward_audit_mode", "severe_fragility_only") or "severe_fragility_only"
        ).strip().lower()
        if self.walk_forward_audit_mode not in {"off", "observe", "severe_fragility_only"}:
            self.walk_forward_audit_mode = "severe_fragility_only"
        self.walk_forward_promotion_policy = str(
            getattr(self, "walk_forward_promotion_policy", "promotion_gate") or "promotion_gate"
        ).strip().lower()
        if self.walk_forward_promotion_policy not in {"observe_only", "block_severe_only", "promotion_gate"}:
            self.walk_forward_promotion_policy = "promotion_gate"
        self.walk_forward_live_blocking_enabled = bool(
            getattr(self, "walk_forward_live_blocking_enabled", True)
        )
        regime_scoring_model = str(getattr(self, "regime_scoring_model", "legacy") or "legacy").strip().lower()
        if regime_scoring_model not in {
            "legacy",
            "legacy_confidence",
            "specialist_spokes",
            "specialist_spokes_v2",
            "specialist_spokes_asym",
            "specialist_spokes_threadspark",
        }:
            logger.warning("Invalid regime_scoring_model=%r; falling back to legacy.", regime_scoring_model)
            regime_scoring_model = "legacy"
        self.regime_scoring_model = regime_scoring_model
        try:
            v2_strength = float(getattr(self, "specialist_spokes_v2_cost_frequency_strength", 0.50))
        except (TypeError, ValueError, OverflowError):
            v2_strength = 0.50
        self.specialist_spokes_v2_cost_frequency_strength = max(0.0, min(1.0, v2_strength))
        self.specialist_spokes_v2_default_fallback_enabled = bool(
            getattr(self, "specialist_spokes_v2_default_fallback_enabled", True)
        )
        self.specialist_spokes_v2_allow_promote_small_default = bool(
            getattr(self, "specialist_spokes_v2_allow_promote_small_default", True)
        )
        try:
            v2_max_sharpe = float(getattr(self, "specialist_spokes_v2_max_validation_sharpe", 0.0))
        except (TypeError, ValueError, OverflowError):
            v2_max_sharpe = 0.0
        self.specialist_spokes_v2_max_validation_sharpe = max(0.0, v2_max_sharpe)
        try:
            stability_floor = float(getattr(self, "regime_trade_stability_floor", 0.7))
        except (TypeError, ValueError, OverflowError):
            stability_floor = 0.7
        self.regime_trade_stability_floor = max(0.0, min(1.0, stability_floor))
        self.false_discovery_control_enabled = bool(
            getattr(self, "false_discovery_control_enabled", False)
        )
        try:
            min_dsr = float(getattr(self, "false_discovery_min_dsr", 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            min_dsr = 0.0
        self.false_discovery_min_dsr = max(0.0, min(1.0, min_dsr))
        try:
            confidence_floor = float(getattr(self, "false_discovery_confidence_floor", 0.35) or 0.0)
        except (TypeError, ValueError, OverflowError):
            confidence_floor = 0.35
        self.false_discovery_confidence_floor = max(0.0, min(1.0, confidence_floor))
        try:
            scope_power = float(getattr(self, "false_discovery_scope_power", 0.25) or 0.0)
        except (TypeError, ValueError, OverflowError):
            scope_power = 0.25
        self.false_discovery_scope_power = max(0.0, min(1.0, scope_power))
        try:
            symbol_count = int(getattr(self, "false_discovery_symbol_count", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            symbol_count = 0
        self.false_discovery_symbol_count = max(0, symbol_count)
        try:
            self.false_discovery_min_daily_observations = max(
                2, int(getattr(self, "false_discovery_min_daily_observations", 252) or 252)
            )
        except (TypeError, ValueError, OverflowError):
            self.false_discovery_min_daily_observations = 252
        try:
            effective_observations = float(
                getattr(self, "false_discovery_min_effective_observations", 60.0)
                or 60.0
            )
        except (TypeError, ValueError, OverflowError):
            effective_observations = 60.0
        self.false_discovery_min_effective_observations = (
            effective_observations
            if math.isfinite(effective_observations) and effective_observations >= 2.0
            else 60.0
        )
        try:
            max_pbo = float(getattr(self, "false_discovery_max_pbo", 0.50))
        except (TypeError, ValueError, OverflowError):
            max_pbo = 0.50
        self.false_discovery_max_pbo = (
            max(0.0, min(1.0, max_pbo)) if math.isfinite(max_pbo) else 0.50
        )
        try:
            self.false_discovery_pbo_min_candidates = max(
                2, int(getattr(self, "false_discovery_pbo_min_candidates", 5) or 5)
            )
        except (TypeError, ValueError, OverflowError):
            self.false_discovery_pbo_min_candidates = 5
        try:
            pbo_blocks = max(2, int(getattr(self, "false_discovery_pbo_blocks", 10) or 10))
        except (TypeError, ValueError, OverflowError):
            pbo_blocks = 10
        self.false_discovery_pbo_blocks = pbo_blocks - (pbo_blocks % 2)
        self.false_discovery_require_bootstrap = bool(
            getattr(self, "false_discovery_require_bootstrap", True)
        )
        try:
            self.false_discovery_bootstrap_repetitions = max(
                100,
                int(getattr(self, "false_discovery_bootstrap_repetitions", 300) or 300),
            )
        except (TypeError, ValueError, OverflowError):
            self.false_discovery_bootstrap_repetitions = 300
        try:
            bootstrap_confidence = float(
                getattr(self, "false_discovery_bootstrap_confidence", 0.95) or 0.95
            )
        except (TypeError, ValueError, OverflowError):
            bootstrap_confidence = 0.95
        self.false_discovery_bootstrap_confidence = (
            max(0.50, min(0.999, bootstrap_confidence))
            if math.isfinite(bootstrap_confidence)
            else 0.95
        )
        for _name, _default in (
            ("false_discovery_bootstrap_min_mean_lower", 0.0),
            ("false_discovery_bootstrap_min_pf_lower", 1.0),
            ("false_discovery_bootstrap_max_dd_upper", 0.20),
        ):
            try:
                _value = float(getattr(self, _name, _default))
            except (TypeError, ValueError, OverflowError):
                _value = _default
            if not math.isfinite(_value):
                _value = _default
            setattr(self, _name, max(0.0, _value))
        self.lockbox_one_shot_registry_enabled = bool(
            getattr(self, "lockbox_one_shot_registry_enabled", True)
        )
        self.lockbox_access_registry_path = str(
            getattr(self, "lockbox_access_registry_path", "") or ""
        ).strip()
        policy = str(getattr(self, "false_discovery_holdout_policy", "block") or "block").strip().lower()
        if policy not in {"off", "promote_small", "block"}:
            policy = "block"
        self.false_discovery_holdout_policy = policy
        try:
            holdout_min_trades = int(getattr(self, "false_discovery_holdout_min_trades", 5) or 0)
        except (TypeError, ValueError, OverflowError):
            holdout_min_trades = 5
        self.false_discovery_holdout_min_trades = max(0, holdout_min_trades)
        try:
            holdout_min_pf = float(getattr(self, "false_discovery_holdout_min_profit_factor", 1.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            holdout_min_pf = 1.0
        self.false_discovery_holdout_min_profit_factor = max(0.0, holdout_min_pf)
        try:
            holdout_min_return = float(getattr(self, "false_discovery_holdout_min_return_pct", 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            holdout_min_return = 0.0
        self.false_discovery_holdout_min_return_pct = holdout_min_return
        try:
            holdout_max_dd = float(
                getattr(self, "false_discovery_holdout_max_drawdown_pct", 20.0)
            )
            if not math.isfinite(holdout_max_dd):
                holdout_max_dd = 20.0
        except (TypeError, ValueError, OverflowError):
            holdout_max_dd = 20.0
        self.false_discovery_holdout_max_drawdown_pct = max(0.0, holdout_max_dd)
        oos_mode = str(getattr(self, "oos_validation_mode", "fixed_holdout") or "fixed_holdout").strip().lower()
        if oos_mode not in {"fixed_holdout", "purged_kfold"}:
            oos_mode = "fixed_holdout"
        self.oos_validation_mode = oos_mode
        try:
            kfold_splits = int(getattr(self, "oos_kfold_splits", 5) or 5)
        except (TypeError, ValueError, OverflowError):
            kfold_splits = 5
        self.oos_kfold_splits = max(2, min(20, kfold_splits))
        try:
            embargo_bars = int(getattr(self, "oos_embargo_bars", 1) or 0)
        except (TypeError, ValueError, OverflowError):
            embargo_bars = 1
        self.oos_embargo_bars = max(0, embargo_bars)
        try:
            oos_min_trades = int(getattr(self, "oos_min_trades", 5) or 0)
        except (TypeError, ValueError, OverflowError):
            oos_min_trades = 5
        self.oos_min_trades = max(0, oos_min_trades)
        oos_policy = str(
            getattr(self, "oos_aggregate_policy", "aggregate_with_worst_fold_floor")
            or "aggregate_with_worst_fold_floor"
        ).strip().lower()
        if oos_policy not in {"all_folds", "aggregate_with_worst_fold_floor"}:
            oos_policy = "aggregate_with_worst_fold_floor"
        self.oos_aggregate_policy = oos_policy
        self.oos_evidence_export_enabled = bool(
            getattr(self, "oos_evidence_export_enabled", False)
        )
        try:
            evidence_periods = int(getattr(self, "oos_evidence_periods", 64) or 64)
        except (TypeError, ValueError, OverflowError):
            evidence_periods = 64
        self.oos_evidence_periods = max(2, evidence_periods)
        try:
            evidence_min_periods = int(getattr(self, "oos_evidence_min_periods", 8) or 8)
        except (TypeError, ValueError, OverflowError):
            evidence_min_periods = 8
        self.oos_evidence_min_periods = max(2, min(evidence_min_periods, self.oos_evidence_periods))
        evidence_dirname = str(getattr(self, "oos_evidence_dirname", "oos_evidence") or "oos_evidence").strip()
        if not evidence_dirname or any(sep in evidence_dirname for sep in ("/", "\\")):
            evidence_dirname = "oos_evidence"
        self.oos_evidence_dirname = evidence_dirname
        try:
            trade_path_points = int(getattr(self, "oos_evidence_trade_path_max_points", 256) or 0)
        except (TypeError, ValueError, OverflowError):
            trade_path_points = 256
        self.oos_evidence_trade_path_max_points = max(0, min(5000, trade_path_points))
        self.oos_evidence_include_execution_cost_survival = bool(
            getattr(self, "oos_evidence_include_execution_cost_survival", False)
        )
        selection_surface = str(
            getattr(self, "threadspark_selection_surface", "validation") or "validation"
        ).strip().lower()
        if selection_surface not in {"clean_holdout", "validation"}:
            selection_surface = "validation"
        self.threadspark_selection_surface = selection_surface
        self.threadspark_use_historical_cost_evidence = bool(
            getattr(self, "threadspark_use_historical_cost_evidence", True)
        )
        historical_scope = str(
            getattr(self, "threadspark_historical_cost_scope", "workflow") or "workflow"
        ).strip().lower()
        if historical_scope not in {"workflow", "audit"}:
            historical_scope = "workflow"
        self.threadspark_historical_cost_scope = historical_scope
        try:
            historical_chunks = int(getattr(self, "threadspark_historical_chunk_count", 5) or 0)
        except (TypeError, ValueError, OverflowError):
            historical_chunks = 5
        self.threadspark_historical_chunk_count = max(1, min(12, historical_chunks))
        stress_mode = str(
            getattr(self, "oos_evidence_execution_stress_mode", "stress_p95") or "stress_p95"
        ).strip().lower()
        if stress_mode not in {"stress_p90", "stress_p95", "stress_max"}:
            logger.warning(
                "Invalid oos_evidence_execution_stress_mode=%r; falling back to stress_p95.",
                stress_mode,
            )
            stress_mode = "stress_p95"
        self.oos_evidence_execution_stress_mode = stress_mode
        try:
            stress_cap = int(getattr(self, "oos_evidence_execution_cost_max_candidates_per_tf", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            stress_cap = 0
        self.oos_evidence_execution_cost_max_candidates_per_tf = max(0, stress_cap)
        try:
            mult = float(getattr(self, "promote_small_risk_multiplier", 0.5) or 0.0)
        except (TypeError, ValueError, OverflowError):
            mult = 0.5
        self.promote_small_risk_multiplier = max(0.0, min(1.0, mult))
        self.probation_enabled = bool(getattr(self, "probation_enabled", True))
        try:
            self.probation_days = max(0, int(getattr(self, "probation_days", 14) or 0))
        except (TypeError, ValueError, OverflowError):
            self.probation_days = 14
        try:
            probation_mult = float(getattr(self, "probation_risk_multiplier", 0.5) or 0.5)
        except (TypeError, ValueError, OverflowError):
            probation_mult = 0.5
        self.probation_risk_multiplier = max(0.05, min(1.0, probation_mult))
        try:
            self.probation_min_live_outcomes = max(
                1, int(getattr(self, "probation_min_live_outcomes", 30) or 30)
            )
        except (TypeError, ValueError, OverflowError):
            self.probation_min_live_outcomes = 30
        try:
            probation_mean_r = float(getattr(self, "probation_min_mean_r", 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            probation_mean_r = 0.0
        self.probation_min_mean_r = probation_mean_r if math.isfinite(probation_mean_r) else 0.0
        try:
            probation_pf = float(getattr(self, "probation_min_profit_factor", 1.0) or 1.0)
        except (TypeError, ValueError, OverflowError):
            probation_pf = 1.0
        self.probation_min_profit_factor = (
            max(0.0, probation_pf) if math.isfinite(probation_pf) else 1.0
        )
        self.swap_model_enabled = bool(self.swap_model_enabled)
        _spread_mode = str(getattr(self, "backtest_spread_mode", "fixed_spec") or "fixed_spec").strip().lower()
        if _spread_mode not in {
            "fixed_spec",
            "historical_bar",
            "historical_max",
            "conservative_max",
            "stress_p90",
            "stress_p95",
            "stress_max",
        }:
            logger.warning("Invalid backtest_spread_mode=%r; falling back to fixed_spec.", _spread_mode)
            _spread_mode = "fixed_spec"
        self.backtest_spread_mode = _spread_mode
        _rollover_day = str(getattr(self, "swap_model_triple_rollover_day", "wednesday") or "").strip().lower()
        if _rollover_day not in _WEEKDAY_NAME_TO_INDEX:
            logger.warning(
                "Invalid swap_model_triple_rollover_day=%r; falling back to 'wednesday'.",
                getattr(self, "swap_model_triple_rollover_day", None),
            )
            _rollover_day = "wednesday"
        self.swap_model_triple_rollover_day = _rollover_day
        self.regime_profitability_audit_enabled = bool(self.regime_profitability_audit_enabled)
        self.regime_boundary_audit_enabled = bool(self.regime_boundary_audit_enabled)
        self.regime_ambiguity_policy = str(
            getattr(self, "regime_ambiguity_policy", "observe_only") or "observe_only"
        ).strip().lower()
        if self.regime_ambiguity_policy not in {"observe_only", "block"}:
            self.regime_ambiguity_policy = "observe_only"
        self.event_calendar_enabled = bool(self.event_calendar_enabled)
        self.event_calendar_mode = str(getattr(self, "event_calendar_mode", "observe") or "observe").strip().lower()
        if self.event_calendar_mode == "observe_only":
            self.event_calendar_mode = "observe"
        if self.event_calendar_mode not in {"off", "observe"}:
            self.event_calendar_mode = "observe"
        self.event_calendar_action = str(getattr(self, "event_calendar_action", "annotate") or "annotate").strip().lower()
        self.event_calendar_path = str(getattr(self, "event_calendar_path", "") or "").strip()
        self.event_calendar_high_impact_action = str(
            getattr(self, "event_calendar_high_impact_action", "observe") or "observe"
        ).strip().lower()
        if self.event_calendar_high_impact_action not in {"observe", "annotate"}:
            self.event_calendar_high_impact_action = "observe"
        self.local_governance_tournament_enabled = bool(self.local_governance_tournament_enabled)
        self.risk_management_optimization_enabled = bool(self.risk_management_optimization_enabled)
        self.risk_management_selection_stage = str(
            getattr(self, "risk_management_selection_stage", "stage3") or ""
        ).strip().lower()
        if self.risk_management_selection_stage not in {"stage2", "stage3"}:
            logger.warning(
                "Invalid risk_management_selection_stage=%r; falling back to stage3.",
                getattr(self, "risk_management_selection_stage", None),
            )
            self.risk_management_selection_stage = "stage3"
        try:
            self.max_bars = max(1000, int(self.max_bars))
        except Exception:
            self.max_bars = 300000
        try:
            self.optimization_min_m5_bars = max(0, int(self.optimization_min_m5_bars))
        except Exception:
            self.optimization_min_m5_bars = 0
        try:
            frac = float(self.optimization_min_m5_bars_fraction)
            self.optimization_min_m5_bars_fraction = frac if math.isfinite(frac) and 0.0 <= frac <= 1.0 else 0.0
        except (TypeError, ValueError, OverflowError):
            self.optimization_min_m5_bars_fraction = 0.0
        try:
            self.historical_fetch_retry_attempts = max(1, int(self.historical_fetch_retry_attempts))
        except Exception:
            self.historical_fetch_retry_attempts = 1
        try:
            retry_sleep = float(self.historical_fetch_retry_sleep_seconds)
            self.historical_fetch_retry_sleep_seconds = retry_sleep if math.isfinite(retry_sleep) and retry_sleep >= 0 else 2.0
        except (TypeError, ValueError, OverflowError):
            self.historical_fetch_retry_sleep_seconds = 2.0
        self.historical_fetch_deep_backfill_enabled = bool(self.historical_fetch_deep_backfill_enabled)
        try:
            self.historical_fetch_deep_backfill_years = max(1, int(self.historical_fetch_deep_backfill_years))
        except Exception:
            self.historical_fetch_deep_backfill_years = 18
        try:
            self.historical_stress_audit_bars = max(0, int(self.historical_stress_audit_bars))
        except Exception:
            self.historical_stress_audit_bars = 50000
        try:
            self.active_universe_bars = max(1000, int(self.active_universe_bars))
        except Exception:
            self.active_universe_bars = 250000
        try:
            self.active_stage2_pct = max(1.0, min(100.0, float(self.active_stage2_pct)))
        except Exception:
            self.active_stage2_pct = 50.0
        split_mode = str(
            getattr(self, "production_split_mode", CALENDAR_SPLIT_MODE)
            or CALENDAR_SPLIT_MODE
        ).strip().lower()
        split_aliases = {
            "calendar": CALENDAR_SPLIT_MODE,
            "calendar_lockbox": CALENDAR_SPLIT_MODE,
            "lockbox": CALENDAR_SPLIT_MODE,
            "three_way": CALENDAR_SPLIT_MODE,
            "chronological_lockbox": "chronological_lockbox_v1",
            "production": "bleeding_edge",
            "practical": "bleeding_edge",
            "clean_holdout": "research_clean_holdout",
            "legacy_clean_holdout": "research_clean_holdout",
        }
        split_mode = split_aliases.get(split_mode, split_mode)
        if split_mode not in {
            CALENDAR_SPLIT_MODE,
            "chronological_lockbox_v1",
            "bleeding_edge",
            "research_clean_holdout",
        }:
            split_mode = CALENDAR_SPLIT_MODE
        self.research_mode = bool(getattr(self, "research_mode", False))
        if split_mode != CALENDAR_SPLIT_MODE and not self.research_mode:
            raise ValueError(
                f"production_split_mode={split_mode!r} is research-only; "
                "set research_mode=True explicitly. Funded production requires "
                f"{CALENDAR_SPLIT_MODE!r}."
            )
        if self.oos_validation_mode != "fixed_holdout" and not self.research_mode:
            raise ValueError(
                f"oos_validation_mode={self.oos_validation_mode!r} is research-only; "
                "funded production requires the one-shot fixed final lockbox"
            )
        self.production_split_mode = split_mode
        try:
            self.tuning_validation_bars = max(1, int(self.tuning_validation_bars))
        except Exception:
            self.tuning_validation_bars = 50000
        try:
            self.final_lockbox_bars = max(1, int(self.final_lockbox_bars))
        except Exception:
            self.final_lockbox_bars = 50000
        try:
            self.split_embargo_bars = max(0, int(self.split_embargo_bars))
        except Exception:
            self.split_embargo_bars = 300
        self.evidence_sufficiency_enabled = bool(
            getattr(self, "evidence_sufficiency_enabled", True)
        )
        self.evidence_require_native_timeframes = bool(
            getattr(self, "evidence_require_native_timeframes", True)
        )
        self.evidence_require_native_provenance = bool(
            getattr(self, "evidence_require_native_provenance", True)
        )
        self.evidence_require_session_coverage = bool(
            getattr(self, "evidence_require_session_coverage", True)
        )
        if self.production_split_mode == CALENDAR_SPLIT_MODE:
            # Calendar lockbox is the funded-production contract. Its native
            # source, immutable provenance, and session-coverage gates cannot
            # be disabled by a stale or research-oriented JSON override.
            self.evidence_sufficiency_enabled = True
            self.evidence_require_native_timeframes = True
            self.evidence_require_native_provenance = True
            self.evidence_require_session_coverage = True
        try:
            evidence_coverage = float(
                getattr(self, "evidence_min_expected_session_coverage", 0.95)
            )
            self.evidence_min_expected_session_coverage = (
                evidence_coverage
                if math.isfinite(evidence_coverage) and 0.0 <= evidence_coverage <= 1.0
                else 0.95
            )
        except (TypeError, ValueError, OverflowError):
            self.evidence_min_expected_session_coverage = 0.95
        if self.production_split_mode == CALENDAR_SPLIT_MODE:
            # The activation universe and every later funded evidence stage
            # share one ex-ante threshold. Raising it after acquisition could
            # prune cells using final-window availability metadata; lowering
            # it would weaken sufficiency. Research modes may still explore.
            self.evidence_min_expected_session_coverage = 0.95
        try:
            tolerance_days = float(getattr(self, "evidence_boundary_tolerance_days", 7.0))
            self.evidence_boundary_tolerance_days = (
                tolerance_days
                if math.isfinite(tolerance_days) and tolerance_days >= 0.0
                else 7.0
            )
        except (TypeError, ValueError, OverflowError):
            self.evidence_boundary_tolerance_days = 7.0
        try:
            purge_days = float(getattr(self, "evidence_purge_days", 7.0))
            self.evidence_purge_days = (
                purge_days if math.isfinite(purge_days) and purge_days >= 0.0 else 7.0
            )
        except (TypeError, ValueError, OverflowError):
            self.evidence_purge_days = 7.0
        try:
            warmup_days = float(getattr(self, "evidence_warmup_days", 400.0))
            self.evidence_warmup_days = (
                warmup_days if math.isfinite(warmup_days) and warmup_days >= 0.0 else 400.0
            )
        except (TypeError, ValueError, OverflowError):
            self.evidence_warmup_days = 400.0
        self.evidence_as_of_utc = str(
            getattr(self, "evidence_as_of_utc", "") or ""
        ).strip()
        self.evidence_window_years = normalize_window_years(
            getattr(self, "evidence_window_years", None)
        )
        self.evidence_timeframe_window_years = normalize_timeframe_window_years(
            getattr(self, "evidence_timeframe_window_years", None)
        )
        self.native_history_window_contract = str(
            getattr(
                self,
                "native_history_window_contract",
                "fxpm-timeframe-evidence-v3",
            )
            or "fxpm-timeframe-evidence-v3"
        ).strip()
        self.evidence_min_native_bars = normalize_min_native_bars(
            getattr(self, "evidence_min_native_bars", None)
        )
        try:
            self.walk_forward_validation_bars = max(0, int(self.walk_forward_validation_bars))
        except Exception:
            self.walk_forward_validation_bars = 50000
        try:
            self.clean_holdout_pct = max(0.0, min(50.0, float(self.clean_holdout_pct)))
        except Exception:
            self.clean_holdout_pct = 0.0
        try:
            self.historical_audit_min_trades = max(0, int(self.historical_audit_min_trades))
        except Exception:
            self.historical_audit_min_trades = 5
        try:
            self.historical_audit_max_drawdown = max(0.0, float(self.historical_audit_max_drawdown))
        except Exception:
            self.historical_audit_max_drawdown = 35.0
        try:
            self.historical_audit_min_profit_factor = max(0.0, float(self.historical_audit_min_profit_factor))
        except Exception:
            self.historical_audit_min_profit_factor = 0.60
        try:
            self.walk_forward_audit_windows = max(1, int(self.walk_forward_audit_windows))
        except Exception:
            self.walk_forward_audit_windows = 3
        raw_governance_mode = getattr(self, "local_governance_live_mode", "off")
        if isinstance(raw_governance_mode, str):
            governance_mode = raw_governance_mode.strip().lower()
        else:
            governance_mode = "off"
        if governance_mode not in {"off", "shadow", "on"}:
            logger.warning(
                "Invalid local_governance_live_mode=%r; falling back to 'off'. "
                "Allowed values: off | shadow | on.",
                raw_governance_mode,
            )
            governance_mode = "off"
        self.local_governance_live_mode = governance_mode
        self.local_governance_candidate_policies = candidate_policy_names(
            getattr(self, "local_governance_candidate_policies", None)
        )
        self.local_governance_require_baseline_improvement = bool(
            self.local_governance_require_baseline_improvement
        )
        try:
            self.local_governance_min_score_improvement = max(
                0.0,
                float(self.local_governance_min_score_improvement),
            )
        except Exception:
            self.local_governance_min_score_improvement = 0.0
        winner_ledger_path = str(getattr(self, "winner_ledger_path", "pm_configs.json") or "").strip()
        self.winner_ledger_path = winner_ledger_path or "pm_configs.json"
        self.storage_enabled = bool(self.storage_enabled)
        self.storage_observe_only = bool(self.storage_observe_only)
        self.storage_signal_ledger_enabled = bool(self.storage_signal_ledger_enabled)
        self.storage_metaquotes_cleanup_enabled = bool(self.storage_metaquotes_cleanup_enabled)
        self.storage_local_data_first_enabled = bool(self.storage_local_data_first_enabled)
        try:
            self.target_annual_vol = max(0.0, float(self.target_annual_vol))
        except Exception:
            self.target_annual_vol = 0.10
        try:
            self.execution_spread_min_edge_mult = max(0.0, float(self.execution_spread_min_edge_mult))
        except Exception:
            self.execution_spread_min_edge_mult = 1.5
        try:
            self.execution_spread_spike_mult = max(1.0, float(self.execution_spread_spike_mult))
        except Exception:
            self.execution_spread_spike_mult = 2.0
        try:
            self.execution_spread_penalty_start_mult = max(0.0, float(self.execution_spread_penalty_start_mult))
        except Exception:
            self.execution_spread_penalty_start_mult = 0.5
        try:
            self.daily_loss_advisory_pct = max(0.0, float(self.daily_loss_advisory_pct))
        except Exception:
            self.daily_loss_advisory_pct = 0.0
        try:
            self.session_loss_advisory_pct = max(0.0, float(self.session_loss_advisory_pct))
        except Exception:
            self.session_loss_advisory_pct = 0.0
        self.manual_new_entries_enabled = bool(getattr(self, "manual_new_entries_enabled", True))
        self.manual_kill_switch_path = str(getattr(self, "manual_kill_switch_path", "") or "").strip()
        self.production_require_promotion_gate = bool(
            getattr(self, "production_require_promotion_gate", False)
        )
        gate_mode = str(getattr(self, "production_promotion_gate_mode", "external_review_exact") or "").strip().lower()
        gate_mode_aliases = {
            "immutable": "immutable_exact",
            "authorization": "immutable_exact",
            "authorization_exact": "immutable_exact",
            "external": "external_review_exact",
            "exact": "external_review_exact",
            "review_exact": "external_review_exact",
            "review": "external_review_exact",
            "ledger": "ledger_status",
            "current_ledger": "ledger_status",
            "ledger_authoritative": "ledger_status",
        }
        gate_mode = gate_mode_aliases.get(gate_mode, gate_mode)
        if gate_mode not in {"immutable_exact", "external_review_exact", "ledger_status"}:
            logger.warning(
                "Invalid production_promotion_gate_mode=%r; falling back to 'external_review_exact'.",
                getattr(self, "production_promotion_gate_mode", None),
            )
            gate_mode = "external_review_exact"
        self.production_promotion_gate_mode = gate_mode
        self.production_promotion_authorization_path = str(
            getattr(self, "production_promotion_authorization_path", "") or ""
        ).strip()
        self.production_promotion_authorization_sha256 = str(
            getattr(self, "production_promotion_authorization_sha256", "") or ""
        ).strip().lower()
        try:
            self.production_promotion_authorization_max_age_days = max(
                1,
                int(getattr(self, "production_promotion_authorization_max_age_days", 31) or 31),
            )
        except (TypeError, ValueError, OverflowError):
            self.production_promotion_authorization_max_age_days = 31
        self.production_promotion_review_path = str(
            getattr(self, "production_promotion_review_path", "") or ""
        ).strip()
        self.production_promotion_cell_safety_path = str(
            getattr(self, "production_promotion_cell_safety_path", "") or ""
        ).strip()
        self.production_completion_marker_path = str(
            getattr(self, "production_completion_marker_path", "") or ""
        ).strip()
        raw_cells = getattr(self, "production_expected_promote_cells", []) or []
        if isinstance(raw_cells, str):
            raw_cells = [raw_cells]
        self.production_expected_promote_cells = [
            str(cell or "").strip().upper()
            for cell in raw_cells
            if str(cell or "").strip()
        ]
        raw_live_statuses = getattr(self, "live_allowed_promotion_statuses", ["PROMOTED"])
        if isinstance(raw_live_statuses, str):
            raw_live_statuses = [
                item.strip() for item in raw_live_statuses.split(",") if item.strip()
            ]
        normalized_live_statuses: List[str] = []
        for raw_status in raw_live_statuses or []:
            status = str(raw_status or "").strip().upper()
            if status in {"PROMOTED", "PROMOTE_SMALL"} and status not in normalized_live_statuses:
                normalized_live_statuses.append(status)
        self.live_allowed_promotion_statuses = normalized_live_statuses or ["PROMOTED"]
        try:
            self.storage_warn_free_gb = max(0.0, float(self.storage_warn_free_gb))
        except Exception:
            self.storage_warn_free_gb = 15.0
        try:
            self.storage_critical_free_gb = max(0.0, float(self.storage_critical_free_gb))
        except Exception:
            self.storage_critical_free_gb = 10.0
        if self.storage_warn_free_gb and self.storage_critical_free_gb and self.storage_warn_free_gb < self.storage_critical_free_gb:
            self.storage_warn_free_gb = self.storage_critical_free_gb
        try:
            if self.storage_pause_entries_below_free_gb in ("", None):
                self.storage_pause_entries_below_free_gb = None
            else:
                self.storage_pause_entries_below_free_gb = max(0.0, float(self.storage_pause_entries_below_free_gb))
        except Exception:
            self.storage_pause_entries_below_free_gb = None
        try:
            self.storage_measure_interval_seconds = max(1, int(self.storage_measure_interval_seconds))
        except Exception:
            self.storage_measure_interval_seconds = 300
        try:
            self.storage_housekeeping_interval_seconds = max(1, int(self.storage_housekeeping_interval_seconds))
        except Exception:
            self.storage_housekeeping_interval_seconds = 900
        try:
            self.storage_metaquotes_review_interval_seconds = max(1, int(self.storage_metaquotes_review_interval_seconds))
        except Exception:
            self.storage_metaquotes_review_interval_seconds = 21600
        try:
            self.storage_write_protect_minutes = max(1, int(self.storage_write_protect_minutes))
        except Exception:
            self.storage_write_protect_minutes = 5
        try:
            self.storage_live_sync_bars = max(100, int(self.storage_live_sync_bars))
        except Exception:
            self.storage_live_sync_bars = 3000
        try:
            self.storage_live_sync_overlap_bars = max(0, int(self.storage_live_sync_overlap_bars))
        except Exception:
            self.storage_live_sync_overlap_bars = 100
        try:
            self.storage_live_cache_max_age_days = max(0, int(self.storage_live_cache_max_age_days))
        except Exception:
            self.storage_live_cache_max_age_days = 7
        try:
            self.storage_delta_sync_overlap_minutes = max(1, int(self.storage_delta_sync_overlap_minutes))
        except Exception:
            self.storage_delta_sync_overlap_minutes = 1440
        try:
            self.storage_resample_cache_max_gb = max(0.0, float(self.storage_resample_cache_max_gb))
        except Exception:
            self.storage_resample_cache_max_gb = 1.0
        try:
            self.storage_resample_cache_max_age_days = max(0, int(self.storage_resample_cache_max_age_days))
        except Exception:
            self.storage_resample_cache_max_age_days = 7
        try:
            self.storage_logs_keep_days = max(0, int(self.storage_logs_keep_days))
        except Exception:
            self.storage_logs_keep_days = 14
        try:
            self.storage_pm_outputs_keep_days = max(0, int(self.storage_pm_outputs_keep_days))
        except Exception:
            self.storage_pm_outputs_keep_days = 14
        try:
            self.storage_pm_outputs_keep_count = max(0, int(self.storage_pm_outputs_keep_count))
        except Exception:
            self.storage_pm_outputs_keep_count = 30
        try:
            self.storage_metaquotes_stale_tester_days = max(1, int(self.storage_metaquotes_stale_tester_days))
        except Exception:
            self.storage_metaquotes_stale_tester_days = 14
        try:
            manifest_max_mb = float(self.storage_manifest_max_mb)
            if not math.isfinite(manifest_max_mb) or manifest_max_mb <= 0:
                raise ValueError("manifest cap must be finite and positive")
            self.storage_manifest_max_mb = min(1024.0, manifest_max_mb)
        except Exception:
            self.storage_manifest_max_mb = 16.0
        try:
            self.storage_manifest_keep_rotations = min(
                16,
                max(0, int(self.storage_manifest_keep_rotations)),
            )
        except Exception:
            self.storage_manifest_keep_rotations = 2
        try:
            manifest_dedupe_hours = float(self.storage_manifest_dedupe_hours)
            if not math.isfinite(manifest_dedupe_hours):
                raise ValueError("manifest dedupe window must be finite")
            self.storage_manifest_dedupe_hours = min(
                24.0 * 365.0,
                max(0.0, manifest_dedupe_hours),
            )
        except Exception:
            self.storage_manifest_dedupe_hours = 24.0
        try:
            self.storage_signal_ledger_compress_after_months = min(
                120,
                max(0, int(self.storage_signal_ledger_compress_after_months)),
            )
        except Exception:
            self.storage_signal_ledger_compress_after_months = 2
        self.storage_metaquotes_root = str(self.storage_metaquotes_root or "").strip()
        self.storage_metaquotes_active_root_allowlist = [
            str(item).strip() for item in (self.storage_metaquotes_active_root_allowlist or []) if str(item).strip()
        ]
        self.storage_metaquotes_demo_servers = [
            str(item).strip() for item in (self.storage_metaquotes_demo_servers or []) if str(item).strip()
        ]
        self.state_filename = str(self.state_filename or "storage_state.json").strip() or "storage_state.json"
        self.manifest_filename = str(self.manifest_filename or "storage_manifest.jsonl").strip() or "storage_manifest.jsonl"
        self.ledger_filename = str(self.ledger_filename or "signal_ledger.jsonl").strip() or "signal_ledger.jsonl"
        try:
            self.live_bar_settle_seconds = max(0, int(self.live_bar_settle_seconds))
        except Exception:
            self.live_bar_settle_seconds = 5
        try:
            self.live_stale_retry_seconds = max(1, int(self.live_stale_retry_seconds))
        except Exception:
            self.live_stale_retry_seconds = 15
        try:
            self.reconnect_max_attempts = max(1, int(self.reconnect_max_attempts))
        except Exception:
            self.reconnect_max_attempts = 5
        try:
            self.reconnect_attempt_interval_seconds = max(1, int(self.reconnect_attempt_interval_seconds))
        except Exception:
            self.reconnect_attempt_interval_seconds = 5
        try:
            self.reconnect_failure_retry_seconds = max(1, int(self.reconnect_failure_retry_seconds))
        except Exception:
            self.reconnect_failure_retry_seconds = 30
        try:
            self.fixed_retrain_days = max(
                1,
                int(self.production_retrain_interval_weeks * 7),
            )
        except Exception:
            self.fixed_retrain_days = 14
        self.optimization_valid_days = self.fixed_retrain_days

        if not 0.0 <= float(self.overlap_pct) <= 100.0:
            raise ValueError("overlap_pct must be between 0 and 100")

    @property
    def retrain_weekday_index(self) -> int:
        """Configured weekday index for the production retrain schedule."""
        return _WEEKDAY_NAME_TO_INDEX[self.production_retrain_weekday]

    @property
    def retrain_interval(self) -> timedelta:
        """Timedelta for one production retrain interval."""
        return timedelta(weeks=int(self.production_retrain_interval_weeks))

    def get_retrain_anchor_datetime(self) -> datetime:
        """Calendar anchor for the fixed production retrain schedule."""
        anchor = datetime.strptime(str(self.production_retrain_anchor_date), "%Y-%m-%d")
        hh, mm = self.get_retrain_time_components()
        return anchor.replace(hour=hh, minute=mm, second=0, microsecond=0)

    def get_retrain_time_components(self) -> Tuple[int, int]:
        """Parsed production retrain time as (hour, minute)."""
        parsed = datetime.strptime(str(self.production_retrain_time), "%H:%M")
        return parsed.hour, parsed.minute

    def get_next_retrain_at(self, from_dt: Optional[datetime] = None) -> datetime:
        """
        Return the next scheduled retrain slot strictly after `from_dt`.

        This keeps production retraining calendar-anchored instead of using
        rolling `now + N days` expiry.
        """
        probe = from_dt or datetime.now()
        anchor = self.get_retrain_anchor_datetime()
        interval = self.retrain_interval
        if probe < anchor:
            return anchor
        elapsed = probe - anchor
        steps = int(elapsed // interval) + 1
        return anchor + (interval * steps)

    def get_last_retrain_slot(self, at_dt: Optional[datetime] = None) -> datetime:
        """Return the most recent scheduled retrain slot at or before `at_dt`."""
        probe = at_dt or datetime.now()
        anchor = self.get_retrain_anchor_datetime()
        interval = self.retrain_interval
        if probe <= anchor:
            return anchor
        elapsed = probe - anchor
        steps = int(elapsed // interval)
        return anchor + (interval * steps)

    def describe_retrain_schedule(self) -> str:
        """Human-readable production retrain schedule."""
        weekday = self.production_retrain_weekday.capitalize()
        weeks = int(self.production_retrain_interval_weeks)
        return f"every {weeks} week{'s' if weeks != 1 else ''} on {weekday} {self.production_retrain_time}"


# =============================================================================
# ENUMERATIONS
# =============================================================================

class SignalType(Enum):
    """Trading signal types."""
    LONG = 1
    SHORT = -1
    FLAT = 0


class StrategyCategory(Enum):
    """Strategy categories."""
    TREND_FOLLOWING = "trend"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT_MOMENTUM = "breakout"
    VOLATILITY = "volatility"
    HYBRID = "hybrid"


class TradeStatus(Enum):
    """Trade exit reasons."""
    OPEN = "open"
    CLOSED_TP = "closed_tp"
    CLOSED_SL = "closed_sl"
    CLOSED_SIGNAL = "closed_signal"
    CLOSED_TIME = "closed_time"
    CLOSED_EOD = "end_of_data"


# =============================================================================
# UTILITY CLASSES
# =============================================================================

class Timer:
    """Context manager for timing operations."""
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start = None
        self.elapsed = 0
        
    def __enter__(self):
        self.start = time.time()
        return self
    
    def __exit__(self, *args):
        self.elapsed = time.time() - self.start
        logger.info(f"[TIME] {self.name}: {self.elapsed:.2f}s")


# =============================================================================
# INSTRUMENT SPECIFICATIONS
# =============================================================================


@dataclass
class StorageConfig:
    """
    Storage governance configuration.

    This config is intentionally conservative by default. The live PM can wire
    it in later without changing behavior until storage management is explicitly
    enabled and scheduled.
    """
    enabled: bool = True
    observe_only: bool = True
    warn_free_gb: float = 15.0
    critical_free_gb: float = 10.0
    pause_new_entries_below_free_gb: Optional[float] = None
    write_protection_seconds: int = 300

    signal_ledger_enabled: bool = True
    data_cache_max_age_days: int = 7
    data_cache_max_bytes: int = 1_000_000_000
    live_cache_max_age_days: int = 7
    live_sync_overlap_bars: int = 100
    log_keep_days: int = 14
    output_keep_days: int = 14
    output_keep_count: int = 30

    maintenance_interval_seconds: int = 120
    post_optimization_interval_seconds: int = 0

    metaquotes_cleanup_enabled: bool = False
    metaquotes_active_root_allowlist: List[str] = field(default_factory=list)

    state_filename: str = "storage_state.json"
    manifest_filename: str = "storage_manifest.jsonl"
    ledger_filename: str = "signal_ledger.jsonl"

    def __post_init__(self):
        try:
            self.warn_free_gb = max(0.0, float(self.warn_free_gb))
        except Exception:
            self.warn_free_gb = 15.0
        try:
            self.critical_free_gb = max(0.0, float(self.critical_free_gb))
        except Exception:
            self.critical_free_gb = 10.0
        if self.pause_new_entries_below_free_gb is not None:
            try:
                self.pause_new_entries_below_free_gb = max(0.0, float(self.pause_new_entries_below_free_gb))
            except Exception:
                self.pause_new_entries_below_free_gb = None
        try:
            self.write_protection_seconds = max(0, int(self.write_protection_seconds))
        except Exception:
            self.write_protection_seconds = 300
        self.signal_ledger_enabled = bool(self.signal_ledger_enabled)
        try:
            self.data_cache_max_age_days = max(0, int(self.data_cache_max_age_days))
        except Exception:
            self.data_cache_max_age_days = 7
        try:
            self.data_cache_max_bytes = max(0, int(self.data_cache_max_bytes))
        except Exception:
            self.data_cache_max_bytes = 1_000_000_000
        try:
            self.live_cache_max_age_days = max(0, int(self.live_cache_max_age_days))
        except Exception:
            self.live_cache_max_age_days = 7
        try:
            self.live_sync_overlap_bars = max(0, int(self.live_sync_overlap_bars))
        except Exception:
            self.live_sync_overlap_bars = 100
        try:
            self.log_keep_days = max(0, int(self.log_keep_days))
        except Exception:
            self.log_keep_days = 14
        try:
            self.output_keep_days = max(0, int(self.output_keep_days))
        except Exception:
            self.output_keep_days = 14
        try:
            self.output_keep_count = max(0, int(self.output_keep_count))
        except Exception:
            self.output_keep_count = 30
        try:
            self.maintenance_interval_seconds = max(1, int(self.maintenance_interval_seconds))
        except Exception:
            self.maintenance_interval_seconds = 120
        try:
            self.post_optimization_interval_seconds = max(0, int(self.post_optimization_interval_seconds))
        except Exception:
            self.post_optimization_interval_seconds = 0
        self.enabled = bool(self.enabled)
        self.observe_only = bool(self.observe_only)
        self.metaquotes_cleanup_enabled = bool(self.metaquotes_cleanup_enabled)
        self.metaquotes_active_root_allowlist = [
            str(item).strip() for item in (self.metaquotes_active_root_allowlist or []) if str(item).strip()
        ]
        self.state_filename = str(self.state_filename or "storage_state.json").strip() or "storage_state.json"
        self.manifest_filename = str(self.manifest_filename or "storage_manifest.jsonl").strip() or "storage_manifest.jsonl"
        self.ledger_filename = str(self.ledger_filename or "signal_ledger.jsonl").strip() or "signal_ledger.jsonl"
        # Compatibility aliases for the existing storage manager and later PM wiring.
        self.storage_enabled = bool(self.enabled)
        self.storage_observe_only = bool(self.observe_only)
        self.storage_signal_ledger_enabled = bool(self.signal_ledger_enabled)
        self.storage_warn_free_gb = float(self.warn_free_gb)
        self.storage_critical_free_gb = float(self.critical_free_gb)
        self.storage_pause_entries_below_free_gb = self.pause_new_entries_below_free_gb
        self.storage_write_protect_minutes = max(1, int(round(self.write_protection_seconds / 60.0)) or 5)
        self.storage_measure_interval_seconds = 300
        self.storage_housekeeping_interval_seconds = 900
        self.storage_metaquotes_review_interval_seconds = 21600
        self.storage_live_cache_max_age_days = int(self.live_cache_max_age_days)
        self.storage_live_sync_overlap_bars = int(self.live_sync_overlap_bars)
        self.storage_metaquotes_cleanup_enabled = bool(self.metaquotes_cleanup_enabled)
        self.storage_metaquotes_stale_tester_days = 14
        self.storage_metaquotes_demo_servers = ["FBS-Demo", "MetaQuotes-Demo"]
        self.storage_metaquotes_active_root_allowlist = list(self.metaquotes_active_root_allowlist)


def _infer_default_digits(symbol: str, pip_position: int) -> int:
    """Infer offline digits when no broker snapshot is available."""
    try:
        pip_pos = max(0, int(pip_position))
    except Exception:
        pip_pos = 4
    sym = (symbol or "").split(".")[0].split("#")[0].upper()
    non_fx_prefixes = (
        "XAU", "XAG", "XTI", "XBR", "XNG",
        "BTC", "ETH", "LTC", "SOL", "BCH", "DOG", "TRX", "XRP", "TON",
    )
    if sym.startswith(non_fx_prefixes):
        return pip_pos
    if len(sym) == 6 and sym.isalpha():
        if sym.endswith("JPY") and pip_pos == 2:
            return 3
        if pip_pos == 4:
            return 5
    return pip_pos


@dataclass
class InstrumentSpec:
    """
    Specification for a trading instrument.
    
    Contains all instrument-specific parameters needed for:
    - Pip calculations
    - Spread/commission modeling
    - Position sizing
    - P&L calculation (tick-based for broker parity)
    
    Note: pip_value represents USD per pip per STANDARD lot.
    - For FX pairs: 1 standard lot = 100,000 units
    - For Gold (XAUUSD): 1 lot = 100 troy oz
    - For Silver (XAGUSD): 1 lot = 5,000 oz
    
    Broker-real fields (from MT5 snapshot):
    - tick_size, tick_value: For tick-based P&L calculation
    - contract_size: For proper lot sizing
    - volume_step: For proper volume rounding
    - stops_level: Minimum stop distance in points
    """
    symbol: str
    pip_position: int  # Decimal position (4 for EURUSD, 2 for USDJPY/XAUUSD, 3 for XAGUSD)
    pip_value: float = 10.0  # USD per pip per standard lot
    spread_avg: float = 1.0  # Average spread in pips
    min_lot: float = 0.01
    max_lot: float = 100.0
    commission_per_lot: float = 7.0  # Commission in USD per round trip per lot
    swap_long: float = 0.0  # Daily swap for long positions
    swap_short: float = 0.0  # Daily swap for short positions
    swap_mode: int = -1  # -1=config/account-currency-per-lot; MT5 mode otherwise
    swap_rollover3days: int = 3  # MT5 weekday index used for triple rollover when available
    
    # Broker-real fields (populated from MT5 snapshot or broker_specs.json)
    tick_size: float = 0.0  # trade_tick_size from MT5
    tick_value: float = 0.0  # trade_tick_value from MT5 (value of 1 tick in account currency)
    contract_size: float = 100000.0  # trade_contract_size from MT5
    volume_step: float = 0.01  # volume_step from MT5
    stops_level: int = 0  # trade_stops_level from MT5 (in points)
    point: float = 0.0  # point from MT5 (minimum price change)
    digits: int = 0  # digits from MT5; 0 means infer for offline defaults
    asset_class: str = "unknown"
    
    def __post_init__(self):
        """Initialize derived values if not set."""
        self.asset_class = infer_asset_class(
            self.symbol,
            broker_data={"asset_class": self.asset_class},
            default="unknown",
        )
        if not self.digits or self.digits <= 0:
            self.digits = _infer_default_digits(self.symbol, self.pip_position)
        # Derive point from digits if not explicitly set.
        if self.point == 0.0:
            self.point = 10 ** (-self.digits) if self.digits >= 0 else 10 ** (-self.pip_position)
        # Derive tick_size from point if not set
        if self.tick_size == 0.0:
            self.tick_size = self.point
        # Derive tick_value from pip_value if not set
        if self.tick_value == 0.0:
            # tick_value = pip_value * (tick_size / pip_size)
            pip_sz = 10 ** (-self.pip_position)
            if pip_sz > 0:
                self.tick_value = self.pip_value * (self.tick_size / pip_sz)
    
    @property
    def pip_size(self) -> float:
        """Price change for one pip."""
        return 10 ** (-self.pip_position)
    
    def price_to_pips(self, price_diff: float) -> float:
        """Convert price difference to pips."""
        if self.pip_size <= 0:
            return 0.0
        return price_diff / self.pip_size
    
    def pips_to_price(self, pips: float) -> float:
        """Convert pips to price difference."""
        return pips * self.pip_size
    
    def get_half_spread_price(self) -> float:
        """Get half spread as price (for bid/ask calculations)."""
        return self.pips_to_price(self.spread_avg / 2.0)
    
    def get_entry_price(self, mid_price: float, is_long: bool) -> float:
        """
        Calculate entry price including spread.
        - Longs enter at ASK (mid + half_spread)
        - Shorts enter at BID (mid - half_spread)
        """
        half_spread = self.get_half_spread_price()
        return mid_price + half_spread if is_long else mid_price - half_spread
    
    def get_exit_price(self, mid_price: float, is_long: bool) -> float:
        """
        Calculate exit price including spread.
        - Longs exit at BID (mid - half_spread)
        - Shorts exit at ASK (mid + half_spread)
        """
        half_spread = self.get_half_spread_price()
        return mid_price - half_spread if is_long else mid_price + half_spread
    
    def calculate_tick_profit(self, entry_price: float, exit_price: float, 
                              volume: float, direction: int) -> float:
        """
        Calculate P&L using tick-based math (MT5 parity).
        
        This is the same calculation MT5 uses internally:
        profit = ticks * tick_value * volume
        where ticks = (exit - entry) / tick_size * direction_sign
        
        Args:
            entry_price: Entry price
            exit_price: Exit price  
            volume: Position size in lots
            direction: 1 for long, -1 for short
            
        Returns:
            Profit/loss in account currency
        """
        if self.tick_size <= 0 or self.tick_value <= 0:
            # Fallback to pip-based calculation
            if direction == 1:
                pnl_pips = self.price_to_pips(exit_price - entry_price)
            else:
                pnl_pips = self.price_to_pips(entry_price - exit_price)
            return pnl_pips * volume * self.pip_value
        
        # Tick-based calculation (MT5 parity)
        price_diff = (exit_price - entry_price) * direction
        ticks = price_diff / self.tick_size
        profit = ticks * self.tick_value * volume
        return profit
    
    def calculate_loss_at_stop(self, entry_price: float, stop_price: float,
                               volume: float, direction: int) -> float:
        """
        Calculate loss amount at stop price using tick math.
        
        Args:
            entry_price: Entry price
            stop_price: Stop loss price
            volume: Position size in lots
            direction: 1 for long, -1 for short
            
        Returns:
            Absolute loss amount in account currency
        """
        profit = self.calculate_tick_profit(entry_price, stop_price, volume, direction)
        return abs(profit)
    
    def round_volume(self, volume: float) -> float:
        """
        Round volume to valid lot size using broker volume_step.
        
        Uses floor rounding to avoid risk increases.
        
        Args:
            volume: Raw volume to round
            
        Returns:
            Volume rounded down to volume_step and max_lot, or zero when the
            risk-sized result cannot meet the broker minimum.
        """
        step = self.volume_step if self.volume_step > 0 else 0.01
        # Correct only floating-point representation noise at an exact broker
        # grid boundary, then conservatively floor genuinely sub-step values.
        scaled = volume / step
        nearest_step = round(scaled)
        tolerance = 1e-10 * max(1.0, abs(scaled))
        if abs(scaled - nearest_step) <= tolerance:
            scaled = float(nearest_step)
        rounded = math.floor(scaled) * step
        # A sub-minimum risk-sized order is not executable without increasing
        # exposure above the model's budget, so fail closed to no trade.
        if rounded < self.min_lot:
            return 0.0
        rounded = min(self.max_lot, rounded)
        # Round to avoid floating point issues
        return round(rounded, 8)
    
    def get_min_stop_distance_price(self) -> float:
        """
        Get minimum stop distance as price distance.
        
        Returns:
            Minimum distance in price units
        """
        if self.stops_level > 0 and self.point > 0:
            return self.stops_level * self.point
        return 0.0


def sync_instrument_spec_from_mt5(spec: InstrumentSpec, mt5_symbol_info) -> InstrumentSpec:
    """
    Update InstrumentSpec with live MT5 broker values.
    Call this during LiveTrader initialization for all symbols.

    This ensures position sizing uses real-time broker values instead of stale config.json values,
    which is critical for cross pairs and instruments with broker-specific contract sizes.

    Args:
        spec: Existing InstrumentSpec from config
        mt5_symbol_info: MT5SymbolInfo object from get_symbol_info()

    Returns:
        Updated InstrumentSpec (modified in-place)
    """
    if not mt5_symbol_info:
        return spec

    # Update critical tick-based values from MT5
    spec.tick_value = mt5_symbol_info.trade_tick_value
    spec.tick_size = mt5_symbol_info.trade_tick_size
    spec.volume_step = mt5_symbol_info.volume_step
    spec.min_lot = mt5_symbol_info.volume_min
    spec.max_lot = mt5_symbol_info.volume_max

    # Update spread (convert MT5 points to pips)
    if mt5_symbol_info.spread > 0 and spec.pip_size > 0:
        # spread is in points, convert to pips
        spec.spread_avg = mt5_symbol_info.spread * mt5_symbol_info.point / spec.pip_size

    # Update contract size
    if mt5_symbol_info.trade_contract_size > 0:
        spec.contract_size = mt5_symbol_info.trade_contract_size

    # Update point and digits
    spec.point = mt5_symbol_info.point
    spec.digits = mt5_symbol_info.digits

    # Update stops level
    spec.stops_level = mt5_symbol_info.trade_stops_level

    # Update swap rates
    spec.swap_long = mt5_symbol_info.swap_long
    spec.swap_short = mt5_symbol_info.swap_short
    raw_swap_mode = getattr(mt5_symbol_info, "swap_mode", -1)
    spec.swap_mode = int(raw_swap_mode if raw_swap_mode is not None else -1)
    raw_triple_rollover = getattr(mt5_symbol_info, "swap_rollover3days", 3)
    spec.swap_rollover3days = int(raw_triple_rollover if raw_triple_rollover is not None else 3)
    spec.asset_class = infer_asset_class(
        spec.symbol,
        broker_data={
            "asset_class": getattr(mt5_symbol_info, "asset_class", ""),
            "path": getattr(mt5_symbol_info, "path", ""),
        },
        default="unknown",
    )

    return spec


# Default instrument specifications
# These are used when MT5 data is not available (e.g., backtesting from CSV)
# Live trading will override these with actual broker values
INSTRUMENT_SPECS = {
    # Major FX Pairs
    'EURUSD': InstrumentSpec('EURUSD', 4, 10.0, 1.0, 0.01, 100.0, 7.0, -6.5, 1.2),
    'GBPUSD': InstrumentSpec('GBPUSD', 4, 10.0, 1.2, 0.01, 100.0, 7.0, -5.0, 0.8),
    'AUDUSD': InstrumentSpec('AUDUSD', 4, 10.0, 1.2, 0.01, 100.0, 7.0, -4.0, 0.5),
    'NZDUSD': InstrumentSpec('NZDUSD', 4, 10.0, 1.5, 0.01, 100.0, 7.0, -3.5, 0.3),
    'USDJPY': InstrumentSpec('USDJPY', 2, 9.0, 1.0, 0.01, 100.0, 7.0, 8.5, -15.0),
    'USDCAD': InstrumentSpec('USDCAD', 4, 7.5, 1.5, 0.01, 100.0, 7.0, 2.5, -8.0),
    'USDCHF': InstrumentSpec('USDCHF', 4, 11.0, 1.5, 0.01, 100.0, 7.0, 5.0, -10.0),
    
    # Cross pairs
    'AUDNZD': InstrumentSpec('AUDNZD', 4, 6.0, 2.5, 0.01, 100.0, 8.0, -2.0, -1.5),
    'EURGBP': InstrumentSpec('EURGBP', 4, 12.5, 1.5, 0.01, 100.0, 7.0, -4.0, 0.5),
    'EURJPY': InstrumentSpec('EURJPY', 2, 9.0, 1.5, 0.01, 100.0, 7.0, 3.0, -10.0),
    'GBPJPY': InstrumentSpec('GBPJPY', 2, 9.0, 2.0, 0.01, 100.0, 7.0, 5.0, -12.0),
    
    # Exotic FX Pairs
    'USDBRL': InstrumentSpec('USDBRL', 4, 2.0, 50.0, 0.01, 100.0, 15.0, 15.0, -40.0),
    'USDMXN': InstrumentSpec('USDMXN', 4, 0.55, 30.0, 0.01, 100.0, 12.0, 20.0, -45.0),
    'USDPLN': InstrumentSpec('USDPLN', 4, 2.5, 25.0, 0.01, 100.0, 10.0, 3.0, -12.0),
    'USDNOK': InstrumentSpec('USDNOK', 4, 0.95, 30.0, 0.01, 100.0, 10.0, 2.0, -10.0),
    'USDSEK': InstrumentSpec('USDSEK', 4, 0.95, 30.0, 0.01, 100.0, 10.0, 2.0, -10.0),
    'USDSGD': InstrumentSpec('USDSGD', 4, 7.5, 2.0, 0.01, 100.0, 8.0, 1.5, -6.0),
    'USDTRY': InstrumentSpec('USDTRY', 4, 0.035, 100.0, 0.01, 100.0, 20.0, 50.0, -150.0),
    'USDZAR': InstrumentSpec('USDZAR', 4, 0.55, 50.0, 0.01, 100.0, 15.0, 12.0, -35.0),
    'USDCNH': InstrumentSpec('USDCNH', 4, 1.4, 20.0, 0.01, 100.0, 10.0, 2.0, -10.0),
    'EURCNH': InstrumentSpec('EURCNH', 4, 1.4, 10.0, 0.01, 100.0, 10.0, 2.0, -10.0),
    'EURPLN': InstrumentSpec('EURPLN', 4, 2.5, 25.0, 0.01, 100.0, 10.0, 3.0, -12.0),
    'EURNOK': InstrumentSpec('EURNOK', 4, 0.95, 30.0, 0.01, 100.0, 10.0, 2.0, -10.0),
    'EURSEK': InstrumentSpec('EURSEK', 4, 0.95, 30.0, 0.01, 100.0, 10.0, 2.0, -10.0),
    'GBPNOK': InstrumentSpec('GBPNOK', 4, 0.95, 35.0, 0.01, 100.0, 10.0, 2.0, -12.0),
    'GBPSEK': InstrumentSpec('GBPSEK', 4, 0.95, 35.0, 0.01, 100.0, 10.0, 2.0, -12.0),
    'EURTRY': InstrumentSpec('EURTRY', 4, 0.035, 120.0, 0.01, 100.0, 20.0, 45.0, -130.0),
    'Platinum': InstrumentSpec('Platinum', 2, 1.0, 8.0, 0.01, 100.0, 5.0, -5.0, -3.0),
    'Palladium': InstrumentSpec('Palladium', 2, 1.0, 8.0, 0.01, 100.0, 5.0, -6.0, -4.0),
    'EURMXN': InstrumentSpec('EURMXN', 4, 0.55, 140.0, 0.01, 100.0, 12.0, 20.0, -45.0),
    'NOKJPY': InstrumentSpec('NOKJPY', 2, 9.0, 2.0, 0.01, 100.0, 8.0, -1.0, -2.0),
    
    # Commodities
    # XAUUSD: 1 lot = 100 troy oz, pip_size = 0.01, pip_value = $1.00
    'XAUUSD': InstrumentSpec('XAUUSD', 2, 1.0, 3.0, 0.01, 100.0, 5.0, -8.0, -5.0),
    # XAGUSD: 1 lot = 5,000 oz, pip_size = 0.001, pip_value = $5.00
    'XAGUSD': InstrumentSpec('XAGUSD', 3, 5.0, 2.0, 0.01, 100.0, 5.0, -3.0, -2.0),
    # Metal crosses
    'XAUEUR': InstrumentSpec('XAUEUR', 2, 1.0, 3.0, 0.01, 100.0, 5.0, -8.0, -5.0),
    'XAUGBP': InstrumentSpec('XAUGBP', 2, 1.0, 3.0, 0.01, 100.0, 5.0, -8.0, -5.0),
    'XAUAUD': InstrumentSpec('XAUAUD', 2, 1.0, 3.0, 0.01, 100.0, 5.0, -8.0, -5.0),
    'XAGEUR': InstrumentSpec('XAGEUR', 3, 5.0, 2.0, 0.01, 100.0, 5.0, -3.0, -2.0),
    'XRX': InstrumentSpec('XRX', 3, 1.0, 5.0, 0.01, 100.0, 5.0, 0.0, 0.0),
    # Additional FX Pairs (aligned with config)
    'AUDJPY': InstrumentSpec('AUDJPY', 2, 9.0, 2.0, 0.01, 100.0, 7.0, -4.0, 1.0),
    'EURAUD': InstrumentSpec('EURAUD', 4, 7.0, 2.0, 0.01, 100.0, 7.0, -4.0, 0.5),
    'EURCHF': InstrumentSpec('EURCHF', 4, 11.0, 2.0, 0.01, 100.0, 7.0, -3.0, 0.5),
    'EURCAD': InstrumentSpec('EURCAD', 4, 7.5, 2.0, 0.01, 100.0, 7.0, -4.0, 0.5),
    'EURNZD': InstrumentSpec('EURNZD', 4, 6.0, 3.0, 0.01, 100.0, 7.0, -4.0, 0.5),
    'GBPAUD': InstrumentSpec('GBPAUD', 4, 7.0, 2.5, 0.01, 100.0, 7.0, -5.0, 0.8),
    'GBPCAD': InstrumentSpec('GBPCAD', 4, 7.5, 2.5, 0.01, 100.0, 7.0, -5.0, 0.8),
    'GBPCHF': InstrumentSpec('GBPCHF', 4, 11.0, 2.5, 0.01, 100.0, 7.0, -5.0, 0.8),
    'CADJPY': InstrumentSpec('CADJPY', 2, 9.0, 2.0, 0.01, 100.0, 7.0, -2.0, 0.5),
    'NZDJPY': InstrumentSpec('NZDJPY', 2, 9.0, 2.0, 0.01, 100.0, 7.0, -2.0, 0.5),
    'AUDCAD': InstrumentSpec('AUDCAD', 4, 7.5, 1.5, 0.01, 100.0, 7.0, 2.5, -8.0),
    'AUDCHF': InstrumentSpec('AUDCHF', 4, 11.0, 2.0, 0.01, 100.0, 7.0, -3.0, 0.5),
    'CADCHF': InstrumentSpec('CADCHF', 4, 11.0, 2.0, 0.01, 100.0, 7.0, -3.0, 0.5),
    'CHFJPY': InstrumentSpec('CHFJPY', 2, 9.0, 1.0, 0.01, 100.0, 7.0, 8.5, -15.0),
    'NZDCAD': InstrumentSpec('NZDCAD', 4, 7.5, 1.5, 0.01, 100.0, 7.0, 2.5, -8.0),
    'NZDCHF': InstrumentSpec('NZDCHF', 4, 11.0, 2.0, 0.01, 100.0, 7.0, -3.0, 0.5),
    'GBPNZD': InstrumentSpec('GBPNZD', 4, 6.0, 3.0, 0.01, 100.0, 7.0, -4.0, 0.5),

    # Crypto (CFDs) - defaults used mainly for offline backtests; live trading overrides via broker specs
    'BTCETH': InstrumentSpec('BTCETH', 2, 1.0, 8.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'BTCXAU': InstrumentSpec('BTCXAU', 2, 1.0, 10.0, 0.01, 100.0, 0.0, 0.0, 0.0),

    # Exotic FX Crosses (ZAR)
    'EURZAR': InstrumentSpec('EURZAR', 4, 0.55, 60.0, 0.01, 100.0, 15.0, 10.0, -30.0),
    'GBPZAR': InstrumentSpec('GBPZAR', 4, 0.55, 70.0, 0.01, 100.0, 15.0, 8.0, -28.0),

    # Indices - point-based instruments; defaults used mainly for offline backtests; live trading overrides via broker specs
    'US100': InstrumentSpec('US100', 0, 1.0, 2.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'US30':  InstrumentSpec('US30',  0, 1.0, 3.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'US500': InstrumentSpec('US500', 0, 1.0, 1.5, 0.01, 100.0, 0.0, 0.0, 0.0),
    'DE30':  InstrumentSpec('DE30',  0, 1.0, 2.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'EU50':  InstrumentSpec('EU50',  0, 1.0, 2.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'UK100': InstrumentSpec('UK100', 0, 1.0, 2.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'JP225': InstrumentSpec('JP225', 0, 1.0, 8.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'FR40':  InstrumentSpec('FR40',  0, 1.0, 2.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'ES35':  InstrumentSpec('ES35',  0, 1.0, 5.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'HK50':  InstrumentSpec('HK50',  0, 1.0, 8.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'AU200': InstrumentSpec('AU200', 0, 1.0, 3.0, 0.01, 100.0, 0.0, 0.0, 0.0),

    # Crypto (CFDs) - defaults used mainly for offline backtests; live trading overrides via broker specs
    'BTCUSD': InstrumentSpec('BTCUSD', 2, 1.0, 15.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'ETHUSD': InstrumentSpec('ETHUSD', 2, 1.0, 8.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'XRPUSD': InstrumentSpec('XRPUSD', 4, 1.0, 12.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'TONUSD': InstrumentSpec('TONUSD', 3, 1.0, 12.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'LTCUSD': InstrumentSpec('LTCUSD', 2, 1.0, 5.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'SOLUSD': InstrumentSpec('SOLUSD', 2, 1.0, 5.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'BCHUSD': InstrumentSpec('BCHUSD', 2, 1.0, 5.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'DOGUSD': InstrumentSpec('DOGUSD', 4, 1.0, 10.0, 0.01, 100.0, 0.0, 0.0, 0.0),
    'TRXUSD': InstrumentSpec('TRXUSD', 4, 1.0, 10.0, 0.01, 100.0, 0.0, 0.0, 0.0),

    # Energy Commodities
    'XTIUSD': InstrumentSpec('XTIUSD', 2, 1.0, 3.0, 0.01, 100.0, 5.0, -5.0, -3.0),
    'XBRUSD': InstrumentSpec('XBRUSD', 2, 1.0, 3.0, 0.01, 100.0, 5.0, -5.0, -3.0),
    'XNGUSD': InstrumentSpec('XNGUSD', 3, 1.0, 5.0, 0.01, 100.0, 5.0, -3.0, -2.0),
}


# Broker specs cache (loaded from broker_specs.json if available)
_BROKER_SPECS_CACHE: Dict[str, Dict[str, Any]] = {}
_BROKER_SPECS_LOADED = False
_BROKER_SPECS_PATH = "broker_specs.json"
_BROKER_SPECS_LOADED_PATH: Optional[str] = None

# Config-provided instrument spec overrides (single source of truth)
_CONFIG_INSTRUMENT_SPECS: Dict[str, InstrumentSpec] = {}
_CONFIG_SPEC_DEFAULTS: Dict[str, Any] = {}
_CONFIG_SPECS_LOADED: bool = False
_CONFIG_SPECS_LOADED_PATH: Optional[str] = None

# Warn once per symbol when spec is missing to avoid log spam
_MISSING_SPEC_WARNED: Set[str] = set()


def load_broker_specs(filepath: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load broker specifications from JSON file.
    
    The JSON file should contain MT5 symbol info captured via save_broker_specs().
    
    Args:
        filepath: Path to broker_specs.json
        
    Returns:
        Dict mapping symbol -> broker spec dict
    """
    global _BROKER_SPECS_CACHE, _BROKER_SPECS_LOADED, _BROKER_SPECS_LOADED_PATH
    
    if filepath is None:
        filepath = _BROKER_SPECS_PATH

    # Return cached if already loaded from the same path
    if _BROKER_SPECS_LOADED and _BROKER_SPECS_LOADED_PATH == filepath:
        return _BROKER_SPECS_CACHE
    
    try:
        import json
        with open(filepath, 'r') as f:
            specs = json.load(f)
        if not isinstance(specs, dict):
            raise ValueError("broker specs JSON must be an object keyed by symbol")
        _BROKER_SPECS_CACHE = specs
        _BROKER_SPECS_LOADED = True
        _BROKER_SPECS_LOADED_PATH = filepath
        logger.info(f"Loaded broker specs for {len(specs)} symbols from {filepath}")
    except FileNotFoundError:
        logger.debug(f"No broker specs file found at {filepath}, using defaults")
        _BROKER_SPECS_CACHE = {}
        _BROKER_SPECS_LOADED = True
        _BROKER_SPECS_LOADED_PATH = filepath
    except Exception as e:
        logger.warning(f"Failed to load broker_specs.json: {e}")
        _BROKER_SPECS_CACHE = {}
        _BROKER_SPECS_LOADED = True
        _BROKER_SPECS_LOADED_PATH = filepath
    
    return _BROKER_SPECS_CACHE


def set_broker_specs_path(filepath: str) -> None:
    """
    Set the broker specs JSON path and force reload on next access.

    Args:
        filepath: Path to broker specs JSON file
    """
    global _BROKER_SPECS_PATH, _BROKER_SPECS_LOADED, _BROKER_SPECS_LOADED_PATH
    if filepath:
        _BROKER_SPECS_PATH = filepath
    _BROKER_SPECS_LOADED = False
    _BROKER_SPECS_LOADED_PATH = None


def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol for lookup (remove suffixes, uppercase)."""
    if not symbol:
        return ""
    return symbol.split('.')[0].split('#')[0].upper()


def _lookup_broker_spec(symbol: str, broker_specs: Dict[str, Dict[str, Any]]) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Find broker spec by exact key, normalized key, or recorded MT5 aliases."""
    if not symbol or not broker_specs:
        return None

    normalized = _normalize_symbol(str(symbol))
    candidates = [str(symbol), normalized]
    for candidate in candidates:
        payload = broker_specs.get(candidate)
        if isinstance(payload, dict):
            return candidate, payload

    for key, payload in broker_specs.items():
        if not isinstance(payload, dict):
            continue
        if _normalize_symbol(str(key)) == normalized:
            return str(key), payload
        for alias in (payload.get("symbol"), payload.get("broker_symbol")):
            if alias and _normalize_symbol(str(alias)) == normalized:
                return str(key), payload
    return None


def _maybe_load_config_specs(config_path: Optional[str] = None) -> None:
    """Lazy-load instrument specs from config.json for child processes."""
    global _CONFIG_SPECS_LOADED, _CONFIG_SPECS_LOADED_PATH
    if _CONFIG_INSTRUMENT_SPECS:
        return

    path = config_path or os.environ.get("PM_CONFIG_PATH") or "config.json"
    if not path:
        _CONFIG_SPECS_LOADED = True
        _CONFIG_SPECS_LOADED_PATH = path
        return

    if not os.path.isabs(path):
        path = os.path.abspath(path)

    if _CONFIG_SPECS_LOADED and _CONFIG_SPECS_LOADED_PATH == path:
        return

    try:
        import json
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        _CONFIG_SPECS_LOADED = True
        _CONFIG_SPECS_LOADED_PATH = path
        return
    except Exception as exc:
        logger.warning(f"Failed to load config specs from {path}: {exc}")
        _CONFIG_SPECS_LOADED = True
        _CONFIG_SPECS_LOADED_PATH = path
        return

    broker_specs_path = data.get("broker_specs_path")
    if broker_specs_path:
        if not os.path.isabs(broker_specs_path):
            broker_specs_path = os.path.join(os.path.dirname(path), broker_specs_path)
        set_broker_specs_path(broker_specs_path)

    set_instrument_specs(
        specs=data.get("instrument_specs"),
        defaults=data.get("instrument_spec_defaults"),
    )
    _CONFIG_SPECS_LOADED = True
    _CONFIG_SPECS_LOADED_PATH = path


def _spec_from_config_dict(symbol: str, data: Optional[Dict[str, Any]],
                           defaults: Optional[Dict[str, Any]] = None) -> InstrumentSpec:
    """
    Build an InstrumentSpec from config dict with optional defaults and inheritance.
    """
    data = data or {}
    merged: Dict[str, Any] = {}

    # Inherit from another symbol if requested
    inherit_symbol = data.get("inherit")
    if inherit_symbol:
        parent_key = _normalize_symbol(str(inherit_symbol))
        parent_spec = _CONFIG_INSTRUMENT_SPECS.get(parent_key)
        if parent_spec is None:
            broker_specs = load_broker_specs()
            broker_match = _lookup_broker_spec(parent_key, broker_specs)
            if broker_match is not None:
                parent_spec = _create_spec_from_broker_data(parent_key, broker_match[1])
            elif parent_key in INSTRUMENT_SPECS:
                parent_spec = INSTRUMENT_SPECS[parent_key]
        if parent_spec is not None:
            merged.update(asdict(parent_spec))

    # Apply defaults, then explicit overrides
    if defaults:
        merged.update(defaults)
    merged.update({k: v for k, v in data.items() if k != "inherit"})

    # Required fields
    merged["symbol"] = symbol

    # Infer pip_position if not provided
    if "pip_position" not in merged or merged["pip_position"] in (None, ""):
        digits = merged.get("digits")
        if digits is not None:
            try:
                digits_i = int(digits)
                merged["pip_position"] = digits_i - 1 if digits_i in (3, 5) else digits_i
            except Exception:
                merged["pip_position"] = 4
        else:
            merged["pip_position"] = 4

    # Filter only valid InstrumentSpec fields
    valid_keys = {f.name for f in fields(InstrumentSpec)}
    filtered = {k: v for k, v in merged.items() if k in valid_keys}

    # Coerce numeric types lightly (avoid strings in config)
    float_fields = {
        "pip_value", "spread_avg", "min_lot", "max_lot", "commission_per_lot",
        "swap_long", "swap_short", "tick_size", "tick_value", "contract_size",
        "volume_step", "point"
    }
    int_fields = {"pip_position", "stops_level", "digits", "swap_mode", "swap_rollover3days"}
    for k in float_fields:
        if k in filtered and filtered[k] is not None:
            try:
                filtered[k] = float(filtered[k])
            except (TypeError, ValueError, OverflowError) as exc:
                logger.debug("Ignoring invalid float instrument spec field %s=%r: %s", k, filtered[k], exc)
    for k in int_fields:
        if k in filtered and filtered[k] is not None:
            try:
                filtered[k] = int(filtered[k])
            except (TypeError, ValueError, OverflowError) as exc:
                logger.debug("Ignoring invalid int instrument spec field %s=%r: %s", k, filtered[k], exc)

    return InstrumentSpec(**filtered)


def set_instrument_specs(specs: Optional[Dict[str, Any]] = None,
                         defaults: Optional[Dict[str, Any]] = None) -> int:
    """
    Set config-provided instrument specs and defaults.

    Args:
        specs: Dict of symbol -> spec dict (may include 'inherit')
        defaults: Dict of default values applied to config specs

    Returns:
        Number of specs loaded
    """
    global _CONFIG_INSTRUMENT_SPECS, _CONFIG_SPEC_DEFAULTS
    _CONFIG_SPEC_DEFAULTS = defaults or {}
    _CONFIG_INSTRUMENT_SPECS = {}
    if not specs:
        return 0

    # Two-pass load: first specs without inherit, then inherit-based
    pending: Dict[str, Any] = {}
    for sym, data in specs.items():
        key = _normalize_symbol(sym)
        if isinstance(data, dict) and data.get("inherit"):
            pending[key] = data
        else:
            _CONFIG_INSTRUMENT_SPECS[key] = _spec_from_config_dict(key, data, _CONFIG_SPEC_DEFAULTS)

    for sym, data in pending.items():
        _CONFIG_INSTRUMENT_SPECS[sym] = _spec_from_config_dict(sym, data, _CONFIG_SPEC_DEFAULTS)

    return len(_CONFIG_INSTRUMENT_SPECS)


def _clone_spec(spec: InstrumentSpec, symbol_override: Optional[str] = None) -> InstrumentSpec:
    """Return a shallow copy of an InstrumentSpec, optionally overriding symbol."""
    spec_dict = asdict(spec)
    if symbol_override:
        spec_dict["symbol"] = symbol_override
    return InstrumentSpec(**spec_dict)


def _apply_spec_defaults(spec: InstrumentSpec) -> InstrumentSpec:
    """Apply config-level default overrides to a spec (non-broker only)."""
    if not _CONFIG_SPEC_DEFAULTS:
        return spec
    spec_dict = asdict(spec)
    for k, v in _CONFIG_SPEC_DEFAULTS.items():
        if k in spec_dict and v is not None:
            spec_dict[k] = v
    return InstrumentSpec(**spec_dict)


def save_broker_specs(specs: Dict[str, Dict[str, Any]], filepath: str = "broker_specs.json"):
    """
    Save broker specifications to JSON file.
    
    Args:
        specs: Dict mapping symbol -> broker spec dict
        filepath: Output file path
    """
    import json
    with open(filepath, 'w') as f:
        json.dump(specs, f, indent=2)
    logger.info(f"Saved broker specs for {len(specs)} symbols to {filepath}")


def _create_spec_from_broker_data(
    symbol: str,
    broker_data: Dict[str, Any],
    base_spec: Optional[InstrumentSpec] = None,
) -> InstrumentSpec:
    """Create an InstrumentSpec from broker spec data.

    This function must be robust to broker/MT5 edge-cases where certain fields
    can be missing or zero for CFDs/crypto/indices (e.g., point=0, tick_size=0,
    pip_size=0). When this happens we should *not* crash; we should sanitize
    values and allow the system to fall back to safe defaults.

    Args:
        symbol: Symbol name
        broker_data: Dict with MT5 symbol info fields
        base_spec: Optional config/default spec used only for fields the broker
            snapshot does not carry, such as explicit commission.

    Returns:
        InstrumentSpec populated with broker-real values (sanitized)
    """
    # Digits / point
    digits = int(broker_data.get('digits', 5))
    point = float(broker_data.get('point', 0.0) or 0.0)

    # Derive point if broker returned 0/None
    if point <= 0.0:
        # Best-effort: MT5 point is usually 10**(-digits)
        try:
            point = float(10 ** (-digits)) if digits > 0 else 0.0001
        except Exception:
            point = 0.0001

    # Determine pip_position from digits
    if digits in (3, 5):
        pip_position = digits - 1
    else:
        pip_position = digits

    # Compute a safe pip_size for spread conversion
    # Priority:
    # 1) explicit broker pip_size if valid
    # 2) derive from digits/point (MT5-style)
    # 3) derive from pip_position
    pip_size = float(broker_data.get('pip_size', 0.0) or 0.0)
    if pip_size <= 0.0:
        if digits in (3, 5):
            pip_size = point * 10.0
        else:
            pip_size = point
    if pip_size <= 0.0:
        pip_size = float(10 ** (-pip_position)) if pip_position >= 0 else 0.0001

    # Tick fields (sanitize zeros)
    tick_size = float(broker_data.get('tick_size', 0.0) or 0.0)
    if tick_size <= 0.0:
        tick_size = point

    tick_value = float(broker_data.get('tick_value', 0.0) or 0.0)
    contract_size = float(broker_data.get('contract_size', 0.0) or 0.0)

    # Pip value (if broker provides it, trust it; otherwise infer from tick fields)
    pip_value = float(broker_data.get('pip_value', 0.0) or 0.0)
    if pip_value <= 0.0:
        if tick_size > 0.0 and tick_value > 0.0:
            pip_value = (pip_size / tick_size) * tick_value
        else:
            # Safe fallback used historically in the PM
            pip_value = 10.0

    # Spread conversion to pips (guard against division by zero)
    spread_raw = float(broker_data.get('spread', 2.0) or 2.0)
    if pip_size > 0.0:
        spread_avg = spread_raw * point / pip_size
    else:
        spread_avg = 2.0

    commission_raw = broker_data.get('commission_per_lot', None)
    if commission_raw is None and base_spec is not None:
        commission_raw = getattr(base_spec, "commission_per_lot", None)
    if commission_raw is None:
        commission_raw = 7.0
    try:
        commission_per_lot = float(commission_raw if commission_raw is not None else 7.0)
    except (TypeError, ValueError):
        commission_per_lot = 7.0

    try:
        return InstrumentSpec(
            symbol=symbol,
            pip_position=pip_position,
            pip_value=pip_value,
            spread_avg=spread_avg,
            min_lot=float(broker_data.get('volume_min', 0.01) or 0.01),
            max_lot=float(broker_data.get('volume_max', 100.0) or 100.0),
            volume_step=float(broker_data.get('volume_step', 0.01) or 0.01),
            commission_per_lot=commission_per_lot,
            swap_long=float(broker_data.get('swap_long', 0.0) or 0.0),
            swap_short=float(broker_data.get('swap_short', 0.0) or 0.0),
            swap_mode=int(broker_data.get('swap_mode', -1) if broker_data.get('swap_mode') is not None else -1),
            swap_rollover3days=int(
                broker_data.get('swap_rollover3days', 3)
                if broker_data.get('swap_rollover3days') is not None else 3
            ),
            # Broker-real fields (used for MT5-parity sizing/P&L)
            digits=digits,
            point=point,
            tick_size=tick_size,
            tick_value=tick_value,
            contract_size=contract_size,
            stops_level=int(broker_data.get('stops_level', broker_data.get('trade_stops_level', 0)) or 0),
            asset_class=infer_asset_class(symbol, broker_data=broker_data, default="unknown"),
        )
    except (TypeError, ValueError, KeyError) as e:
        logger.warning(
            f"InstrumentSpec construction failed for {symbol}: {e}. "
            f"Broker data keys: {list(broker_data.keys())}. Using safe defaults."
        )
        return InstrumentSpec(
            symbol=symbol,
            pip_position=4,
            asset_class=infer_asset_class(symbol, broker_data=broker_data, default="unknown"),
        )


def get_instrument_spec(symbol: str) -> InstrumentSpec:
    """
    Get instrument specification for a symbol.
    
    Priority:
    1. Broker specs from broker_specs.json (if available) for execution fields,
       with config specs used as commercial fallbacks such as commission.
    2. Config-provided instrument_specs overrides
    3. Default hardcoded specs
    4. Generic fallback spec
    
    Args:
        symbol: Symbol name
        
    Returns:
        InstrumentSpec for the symbol
    """
    if not _CONFIG_INSTRUMENT_SPECS:
        _maybe_load_config_specs()
    symbol_key = _normalize_symbol(symbol)

    config_spec = _CONFIG_INSTRUMENT_SPECS.get(symbol_key)

    # 1) Broker specs are the source of truth for point/digits/tick/spread.
    broker_specs = load_broker_specs()
    broker_match = _lookup_broker_spec(symbol, broker_specs)
    if broker_match is not None:
        return _create_spec_from_broker_data(symbol, broker_match[1], base_spec=config_spec)

    # 2) Config-provided overrides
    if config_spec is not None:
        return _apply_spec_defaults(_clone_spec(config_spec, symbol_override=symbol))
    
    # 3) Hardcoded defaults
    if symbol in INSTRUMENT_SPECS:
        return _apply_spec_defaults(_clone_spec(INSTRUMENT_SPECS[symbol]))
    
    if symbol_key in INSTRUMENT_SPECS:
        return _apply_spec_defaults(_clone_spec(INSTRUMENT_SPECS[symbol_key], symbol_override=symbol))
    
    # 4) Generic fallback (warn only once per symbol to avoid spam)
    if symbol not in _MISSING_SPEC_WARNED:
        logger.warning(
            f"No spec found for {symbol}, using default. "
            f"Add to config.json instrument_specs or provide broker specs."
        )
        _MISSING_SPEC_WARNED.add(symbol)
    return _apply_spec_defaults(InstrumentSpec(symbol, 4, 10.0, 2.0))


# =============================================================================
# DATA LOADING
# =============================================================================

class DataLoader:
    """
    Loads and resamples OHLCV data.
    
    Supports:
    - Loading from CSV files
    - Resampling M5 data to higher timeframes
    - Data validation and cleaning
    """
    
    # Timeframe resampling rules
    RESAMPLE_MAP = {
        'M1': '1min',
        'M5': '5min',
        'M15': '15min',
        'M30': '30min',
        'H1': '1h',
        'H4': '4h',
        'D1': '1D',
        'W1': '1W',
    }

    TIMEFRAME_MINUTES = {
        'M1': 1,
        'M5': 5,
        'M15': 15,
        'M30': 30,
        'H1': 60,
        'H4': 240,
        'D1': 1440,
        'W1': 10080,
    }
    
    # Minimum bars required for each timeframe (increased for 5-year data)
    MIN_BARS = {
        'M5': 50000,   # ~6 months of M5 data minimum
        'M15': 20000,  # ~6 months of M15 data minimum
        'M30': 10000,  # ~6 months of M30 data minimum
        'H1': 5000,    # ~6 months of H1 data minimum
        'H4': 2000,    # ~6 months of H4 data minimum  
        'D1': 500,     # ~2 years of D1 data minimum
    }
    RESAMPLE_CACHE_SCHEMA_VERSION = 3
    
    def __init__(self, data_dir: Path, cache_resampled: bool = True, cache_dir: Optional[Path] = None):
        """
        Initialize DataLoader.
        
        Args:
            data_dir: Directory containing CSV data files
        """
        self.data_dir = Path(data_dir)
        self._cache: Dict[str, pd.DataFrame] = {}
        self._resample_cache: Dict[str, pd.DataFrame] = {}
        self._resample_meta: Dict[str, Dict[str, Any]] = {}
        self._source_meta: Dict[str, Dict[str, Any]] = {}
        self._source_sha256_cache: Dict[Tuple[str, int, int], str] = {}
        # One DataLoader instance is one immutable native evidence snapshot.
        # An ACTIVE pointer change requires a new loader/run; mixing activations
        # across symbols or timeframes would invalidate a portfolio experiment.
        self._native_activation_pin: Optional[Tuple[str, str]] = None
        self._resample_cache_stats: Dict[str, Any] = {
            "memory_hits": 0,
            "disk_hits": 0,
            "misses": 0,
            "invalidations": 0,
            "writes": 0,
            "read_failures": 0,
            "write_failures": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "read_seconds": 0.0,
            "write_seconds": 0.0,
        }

        self.cache_resampled = bool(cache_resampled)
        self.cache_dir = Path(cache_dir) if cache_dir else (self.data_dir / ".cache")
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            # If cache dir can't be created, fall back to in-memory only
            self.cache_resampled = False

    def _resolve_data_file(self, symbol: str, base_timeframe: str) -> Optional[Path]:
        """Resolve a single unambiguous CSV for a symbol/timeframe pair."""
        exact_candidates = [
            self.data_dir / f"{symbol}_{base_timeframe}.csv",
            self.data_dir / f"{symbol.upper()}_{base_timeframe}.csv",
            self.data_dir / f"{symbol.lower()}_{base_timeframe}.csv",
        ]
        for candidate in exact_candidates:
            if candidate.exists():
                return candidate

        matches: Dict[str, Path] = {}
        for pattern in (
            f"{symbol}_{base_timeframe}*.csv",
            f"{symbol.upper()}_{base_timeframe}*.csv",
            f"{symbol.lower()}_{base_timeframe}*.csv",
        ):
            for match in sorted(self.data_dir.glob(pattern)):
                matches[str(match.resolve())] = match

        if len(matches) == 1:
            return next(iter(matches.values()))
        if len(matches) > 1:
            logger.error(
                f"{symbol} {base_timeframe}: ambiguous source files: "
                f"{', '.join(p.name for p in matches.values())}"
            )
        return None

    def _file_sha256(self, path: Path) -> str:
        """Content fingerprint cached by immutable filesystem identity."""
        resolved = Path(path).resolve()
        stat = resolved.stat()
        key = (str(resolved), int(stat.st_mtime_ns), int(stat.st_size))
        cached = self._source_sha256_cache.get(key)
        if cached:
            return cached
        digest = hashlib.sha256()
        with resolved.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        value = digest.hexdigest()
        self._source_sha256_cache[key] = value
        return value

    def load_symbol(self, symbol: str, base_timeframe: str = 'M5') -> Optional[pd.DataFrame]:
        """
        Load data for a symbol from CSV.
        
        Args:
            symbol: Symbol name
            base_timeframe: Base timeframe of the data file
            
        Returns:
            DataFrame with OHLCV data or None if not found
        """
        # Check cache
        cache_key = f"{symbol}_{base_timeframe}"
        if cache_key in self._cache:
            return self._cache[cache_key].copy()
        
        data_file = self._resolve_data_file(symbol, base_timeframe)
        
        if not data_file:
            logger.warning(f"No data file found for {symbol} in {self.data_dir}")
            return None
        
        try:
            df = pd.read_csv(data_file)
            
            # Standardize column names
            df = self._standardize_columns(df)
            
            # Parse and set datetime index
            df = self._parse_datetime_index(df)
            
            # Validate data
            if not self._validate_data(df, symbol):
                return None
            
            # Cache and return
            self._cache[cache_key] = df

            # Track source metadata for resample cache validation
            try:
                stat = data_file.stat()
                self._source_meta[cache_key] = {
                    "path": str(data_file),
                    "mtime": stat.st_mtime,
                    "mtime_ns": int(stat.st_mtime_ns),
                    "size": int(stat.st_size),
                    "sha256": self._file_sha256(data_file),
                    "rows": len(df),
                    "last_index": df.index[-1].isoformat() if len(df) else None,
                }
            except Exception:
                self._source_meta[cache_key] = {
                    "path": str(data_file),
                    "mtime": None,
                    "mtime_ns": None,
                    "size": None,
                    "sha256": None,
                    "rows": len(df),
                    "last_index": df.index[-1].isoformat() if len(df) else None,
                }
            logger.info(f"Loaded {symbol}: {len(df)} bars from {data_file.name}")
            
            return df.copy()
            
        except Exception as e:
            logger.error(f"Error loading {symbol}: {e}")
            return None

    def _native_timeframe_candidates(self, symbol: str, timeframe: str) -> List[Path]:
        """Return deterministic native-file candidates without using resample caches."""
        tf = str(timeframe or "").upper()
        names = [
            f"{symbol}_{tf}.csv",
            f"{str(symbol).upper()}_{tf}.csv",
            f"{str(symbol).lower()}_{tf}.csv",
        ]
        candidates: List[Path] = []
        seen: Set[str] = set()
        for directory in (self.data_dir / "native", self.data_dir):
            for name in names:
                path = directory / name
                key = str(path.resolve())
                if key not in seen:
                    seen.add(key)
                    candidates.append(path)
        return candidates

    @staticmethod
    def _native_sidecar_candidates(data_file: Path) -> List[Path]:
        return [
            data_file.with_name(f"{data_file.name}.meta.json"),
            data_file.with_suffix(".meta.json"),
            data_file.with_suffix(".json"),
        ]

    def load_native_timeframe(
        self,
        symbol: str,
        timeframe: str,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """Load one hash-pinned active broker-native timeframe artifact.

        Resolution is fail-closed through ``data/native/ACTIVE.json`` and its
        immutable activation manifest. An invalid activation never falls back
        to a flat file. Once file resolution succeeds, a readable frame may be
        returned with provenance errors for diagnostics; production evidence
        must still require ``provenance_valid``.
        """
        tf = str(timeframe or "").strip().upper()
        # Native production data is resolved only through one atomic,
        # hash-pinned activation. Never scan flat files and never silently
        # substitute an M5-derived artifact here.
        activation = resolve_active_native_artifact(
            self.data_dir,
            symbol,
            tf,
            production_contract_required=True,
        )
        if not activation.valid:
            return None, {
                "source_kind": "missing",
                "symbol": str(symbol),
                "timeframe": tf,
                "provenance_valid": False,
                "provenance_errors": list(
                    activation.errors or ["NATIVE_ACTIVE_CELL_UNRESOLVED"]
                ),
                "activation_id": activation.activation_id,
                "activation_manifest_path": (
                    str(activation.activation_manifest_path)
                    if activation.activation_manifest_path is not None
                    else None
                ),
                "activation_manifest_sha256": activation.activation_manifest_sha256,
            }
        activation_identity = (
            str(activation.activation_id or ""),
            str(activation.activation_manifest_sha256 or ""),
        )
        if self._native_activation_pin is None:
            self._native_activation_pin = activation_identity
        elif self._native_activation_pin != activation_identity:
            return None, {
                "source_kind": "missing",
                "symbol": str(symbol),
                "timeframe": tf,
                "provenance_valid": False,
                "provenance_errors": ["NATIVE_ACTIVATION_CHANGED_DURING_LOADER_LIFETIME"],
                "activation_id": activation.activation_id,
                "activation_manifest_sha256": activation.activation_manifest_sha256,
                "pinned_activation_id": self._native_activation_pin[0],
                "pinned_activation_manifest_sha256": self._native_activation_pin[1],
            }
        cache_key = (
            f"native::{activation.activation_id}:{activation.activation_manifest_sha256}:"
            f"{symbol}_{tf}"
        )
        if cache_key in self._cache:
            meta = dict(self._source_meta.get(cache_key, {}) or {})
            return self._cache[cache_key].copy(), meta

        data_file = activation.data_path

        try:
            raw = pd.read_csv(data_file)
            frame = self._standardize_columns(raw)
            frame = self._parse_datetime_index(frame)
            provenance_errors: List[str] = []
            if frame.index.has_duplicates:
                provenance_errors.append("DUPLICATE_TIMESTAMPS")
            if not frame.index.is_monotonic_increasing:
                provenance_errors.append("NON_MONOTONIC_TIMESTAMPS")
            if not self._validate_data(frame, symbol):
                provenance_errors.append("OHLC_VALIDATION_FAILED")
            if "_BAR_CLOSE_TS" not in frame.columns:
                provenance_errors.append("BAR_CLOSE_TS_MISSING")
            else:
                declared_closes = pd.DatetimeIndex(
                    pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
                )
                frame_opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True))
                if declared_closes.isna().any() or not bool((declared_closes > frame_opens).all()):
                    provenance_errors.append("BAR_CLOSE_TS_INVALID")

            stat = data_file.stat()
            digest = self._file_sha256(data_file)
            sidecar_path = activation.sidecar_path
            sidecar: Optional[Dict[str, Any]] = None
            if sidecar_path is not None:
                try:
                    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
                    sidecar = payload if isinstance(payload, dict) else None
                    if sidecar is None:
                        provenance_errors.append("SIDECAR_JSON_NOT_OBJECT")
                except Exception:
                    provenance_errors.append("SIDECAR_JSON_INVALID")

            first_ts = frame.index[0] if len(frame) else None
            last_ts = frame.index[-1] if len(frame) else None
            provenance_errors.extend(
                validate_native_provenance(
                    sidecar,
                    symbol=symbol,
                    timeframe=tf,
                    sha256=digest,
                    rows=len(frame),
                    first_ts=first_ts,
                    last_ts=last_ts,
                )
            )
            # Preserve insertion order while de-duplicating reason codes.
            provenance_errors = list(dict.fromkeys(provenance_errors))
            meta: Dict[str, Any] = {
                "source_kind": "broker_native",
                "symbol": str(symbol),
                "timeframe": tf,
                "path": str(data_file.resolve()),
                "mtime": stat.st_mtime,
                "mtime_ns": int(stat.st_mtime_ns),
                "size": int(stat.st_size),
                "sha256": digest,
                "rows": int(len(frame)),
                "first_index": first_ts.isoformat() if first_ts is not None else None,
                "last_index": last_ts.isoformat() if last_ts is not None else None,
                "sidecar_path": str(sidecar_path.resolve()) if sidecar_path is not None else None,
                "provenance_valid": not provenance_errors,
                "provenance_errors": provenance_errors,
                "activation_id": activation.activation_id,
                "activation_manifest_path": str(activation.activation_manifest_path),
                "activation_manifest_sha256": activation.activation_manifest_sha256,
                "activation_entry": dict(activation.entry or {}),
            }
            if isinstance(sidecar, dict):
                if "expected_session_coverage" in sidecar:
                    meta["expected_session_coverage"] = sidecar.get("expected_session_coverage")
                if "expected_session_coverage_by_partition" in sidecar:
                    meta["expected_session_coverage_by_partition"] = sidecar.get(
                        "expected_session_coverage_by_partition"
                    )
                if "observed_session_bars_by_partition" in sidecar:
                    meta["observed_session_bars_by_partition"] = sidecar.get(
                        "observed_session_bars_by_partition"
                    )
                if "expected_session_bars_by_partition" in sidecar:
                    meta["expected_session_bars_by_partition"] = sidecar.get(
                        "expected_session_bars_by_partition"
                    )
                if "timestamp_semantics" in sidecar:
                    meta["timestamp_semantics"] = sidecar.get("timestamp_semantics")
                if "common_calendar_boundaries" in sidecar:
                    meta["common_calendar_boundaries"] = sidecar.get(
                        "common_calendar_boundaries"
                    )
                meta["sidecar_schema_version"] = sidecar.get("schema_version")

            self._cache[cache_key] = frame.copy()
            self._source_meta[cache_key] = dict(meta)
            return frame.copy(), dict(meta)
        except Exception as exc:
            logger.warning(f"{symbol} {tf}: native timeframe load failed: {exc}")
            return None, {
                "source_kind": "broker_native",
                "symbol": str(symbol),
                "timeframe": tf,
                "path": str(data_file),
                "provenance_valid": False,
                "provenance_errors": [f"NATIVE_LOAD_FAILED:{type(exc).__name__}"],
            }

    def estimate_source_rows(self,
                             timeframe: str,
                             target_bars: int,
                             *,
                             base_timeframe: str = 'M5',
                             padding_bars: int = 2) -> int:
        """
        Estimate how many base-timeframe rows are needed to produce a recent
        tail of `target_bars` for the requested timeframe.

        This is used by the live loop to bound local file reads without
        degrading higher-timeframe signal quality.
        """
        try:
            target_bars = max(1, int(target_bars))
        except Exception:
            target_bars = 1
        try:
            padding_bars = max(0, int(padding_bars))
        except Exception:
            padding_bars = 2

        tf = str(timeframe or base_timeframe).upper()
        base_tf = str(base_timeframe or 'M5').upper()
        target_minutes = int(self.TIMEFRAME_MINUTES.get(tf, self.TIMEFRAME_MINUTES.get(base_tf, 5)))
        base_minutes = int(self.TIMEFRAME_MINUTES.get(base_tf, 5))
        if base_minutes <= 0:
            base_minutes = 5
        compression = max(1, int(math.ceil(target_minutes / base_minutes)))
        return max(target_bars, (target_bars + padding_bars) * compression)

    def _read_recent_csv_rows(self, data_file: Path, row_count: int) -> pd.DataFrame:
        """
        Read only the trailing `row_count` data rows from a CSV.

        The PM's canonical/live M5 files are append-oriented and line based, so
        reading from the end keeps live sweeps bounded even when the full
        canonical history is much larger.
        """
        try:
            row_count = max(0, int(row_count))
        except Exception:
            row_count = 0
        if row_count <= 0:
            return pd.read_csv(data_file)

        with data_file.open("rb") as handle:
            header = handle.readline()
            if not header:
                return pd.DataFrame()
            header_line = header.rstrip(b"\r\n")
            handle.seek(0, os.SEEK_END)
            position = handle.tell()
            buffer = b""
            newline_target = row_count + 1
            while position > 0 and buffer.count(b"\n") <= newline_target:
                read_size = min(65536, position)
                position -= read_size
                handle.seek(position)
                buffer = handle.read(read_size) + buffer

        lines = buffer.splitlines()
        if lines and lines[0] == header_line:
            lines = lines[1:]
        tail_lines = lines[-row_count:] if row_count > 0 else lines
        payload = header if header.endswith((b"\n", b"\r")) else header + b"\n"
        if tail_lines:
            payload += b"\n".join(tail_lines) + b"\n"
        return pd.read_csv(io.BytesIO(payload))

    def _load_recent_source(self,
                            symbol: str,
                            *,
                            base_timeframe: str = 'M5',
                            row_count: int,
                            source_path: Optional[Path] = None) -> Optional[pd.DataFrame]:
        """Load a bounded recent source window from canonical or live-local CSV."""
        data_file = Path(source_path) if source_path else self._resolve_data_file(symbol, base_timeframe)
        if not data_file or not data_file.exists():
            return None

        try:
            df = self._read_recent_csv_rows(data_file, row_count)
            df = self._standardize_columns(df)
            df = self._parse_datetime_index(df)
            if not self._validate_data(df, symbol):
                return None
            return df.copy()
        except Exception as exc:
            logger.debug(
                f"{symbol} {base_timeframe}: recent load failed from {getattr(data_file, 'name', data_file)}: {exc}"
            )
            return None
    
    def resample(self, df: pd.DataFrame, target_timeframe: str) -> pd.DataFrame:
        """
        Resample data to a higher timeframe.
        
        Args:
            df: Source DataFrame (must have datetime index)
            target_timeframe: Target timeframe (e.g., 'H1', 'D1')
            
        Returns:
            Resampled DataFrame
        """
        if target_timeframe not in self.RESAMPLE_MAP:
            raise ValueError(f"Unknown timeframe: {target_timeframe}")
        
        resample_rule = self.RESAMPLE_MAP[target_timeframe]
        
        # Resample OHLCV
        agg_map = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }
        if 'RealVolume' in df.columns:
            agg_map['RealVolume'] = 'sum'
        resampled = df.resample(resample_rule).agg(agg_map).dropna()
        source_ts = pd.Series(pd.DatetimeIndex(df.index), index=df.index)
        source_groups = source_ts.resample(resample_rule)
        resampled['_SOURCE_FIRST_TS'] = source_groups.first().reindex(resampled.index)
        resampled['_SOURCE_LAST_TS'] = source_groups.last().reindex(resampled.index)
        
        # Handle Spread if present
        if 'Spread' in df.columns:
            spread_numeric = pd.to_numeric(df['Spread'], errors='coerce')
            grouped_spread = spread_numeric.resample(resample_rule)
            resampled['Spread'] = grouped_spread.mean()
            resampled['SpreadOpen'] = grouped_spread.first()
            resampled['SpreadMax'] = grouped_spread.max()
            resampled['SpreadP90'] = grouped_spread.quantile(0.90)
            resampled['SpreadP95'] = grouped_spread.quantile(0.95)
        
        return resampled

    def _resample_cache_key(self, symbol: str, timeframe: str) -> str:
        return f"{symbol}_{timeframe}"

    def _resample_cache_path(self, symbol: str, timeframe: str) -> Path:
        safe_symbol = symbol.replace("/", "_").replace("\\", "_").replace(":", "_")
        return self.cache_dir / f"{safe_symbol}_{timeframe}.pkl"

    def _is_resample_cache_valid(self, meta: Dict[str, Any], source_meta: Dict[str, Any]) -> bool:
        if not meta or not source_meta:
            return False
        return (
            meta.get("schema_version") == self.RESAMPLE_CACHE_SCHEMA_VERSION and
            meta.get("source_mtime") == source_meta.get("mtime") and
            meta.get("source_mtime_ns") == source_meta.get("mtime_ns") and
            meta.get("source_size") == source_meta.get("size") and
            meta.get("source_sha256") == source_meta.get("sha256") and
            meta.get("source_rows") == source_meta.get("rows") and
            meta.get("source_last_index") == source_meta.get("last_index")
        )

    def _load_resample_cache(self, symbol: str, timeframe: str, source_meta: Dict[str, Any]) -> Optional[pd.DataFrame]:
        cache_key = self._resample_cache_key(symbol, timeframe)

        # In-memory cache
        meta = self._resample_meta.get(cache_key)
        if cache_key in self._resample_cache and self._is_resample_cache_valid(meta, source_meta):
            self._resample_cache_stats["memory_hits"] += 1
            return self._resample_cache[cache_key].copy()
        if cache_key in self._resample_cache:
            self._resample_cache_stats["invalidations"] += 1

        if not self.cache_resampled:
            self._resample_cache_stats["misses"] += 1
            return None

        # Disk cache
        path = self._resample_cache_path(symbol, timeframe)
        if not path.exists():
            self._resample_cache_stats["misses"] += 1
            return None

        start = time.perf_counter()
        try:
            size_bytes = path.stat().st_size
            payload = pd.read_pickle(path)
            self._resample_cache_stats["read_seconds"] += max(0.0, time.perf_counter() - start)
            self._resample_cache_stats["bytes_read"] += int(size_bytes)
            if not isinstance(payload, dict):
                self._resample_cache_stats["invalidations"] += 1
                return None
            meta = payload.get("meta", {})
            if not self._is_resample_cache_valid(meta, source_meta):
                self._resample_cache_stats["invalidations"] += 1
                return None
            df = payload.get("data")
            if isinstance(df, pd.DataFrame):
                self._resample_cache[cache_key] = df.copy()
                self._resample_meta[cache_key] = meta
                self._resample_cache_stats["disk_hits"] += 1
                return df.copy()
        except Exception:
            self._resample_cache_stats["read_failures"] += 1
            return None

        self._resample_cache_stats["misses"] += 1
        return None

    def _save_resample_cache(self, symbol: str, timeframe: str, source_meta: Dict[str, Any],
                             df: pd.DataFrame) -> None:
        cache_key = self._resample_cache_key(symbol, timeframe)
        meta = {
            "schema_version": self.RESAMPLE_CACHE_SCHEMA_VERSION,
            "source_mtime": source_meta.get("mtime"),
            "source_mtime_ns": source_meta.get("mtime_ns"),
            "source_size": source_meta.get("size"),
            "source_sha256": source_meta.get("sha256"),
            "source_rows": source_meta.get("rows"),
            "source_last_index": source_meta.get("last_index"),
        }
        df_copy = df.copy()
        self._resample_cache[cache_key] = df_copy
        self._resample_meta[cache_key] = meta

        if not self.cache_resampled:
            return
        try:
            path = self._resample_cache_path(symbol, timeframe)
            payload = {"meta": meta, "data": df_copy}
            start = time.perf_counter()
            pd.to_pickle(payload, path)
            self._resample_cache_stats["write_seconds"] += max(0.0, time.perf_counter() - start)
            self._resample_cache_stats["writes"] += 1
            try:
                self._resample_cache_stats["bytes_written"] += int(path.stat().st_size)
            except OSError:
                pass
        except Exception:
            self._resample_cache_stats["write_failures"] += 1
            pass
    
    def get_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Get data for a symbol at a specific timeframe.
        
        Loads base M5 data and resamples if needed.
        
        Args:
            symbol: Symbol name
            timeframe: Target timeframe
            
        Returns:
            DataFrame with OHLCV data or None
        """
        # Load base data
        base_df = self.load_symbol(symbol, 'M5')
        if base_df is None:
            return None
        
        # Resample if needed
        if timeframe == 'M5':
            return base_df.copy()
        
        # Try cache (memory/disk) keyed to the M5 source file state
        base_meta = self._source_meta.get(f"{symbol}_M5", {})
        cached = self._load_resample_cache(symbol, timeframe, base_meta)
        if cached is not None:
            resampled = cached
        else:
            resampled = self.resample(base_df, timeframe)
            self._save_resample_cache(symbol, timeframe, base_meta, resampled)
        
        # Check minimum bars
        min_bars = self.MIN_BARS.get(timeframe, 100)
        if len(resampled) < min_bars:
            logger.warning(f"{symbol} {timeframe}: Only {len(resampled)} bars, need {min_bars}")
            return None
        
        return resampled.copy()

    def get_recent_data(self,
                        symbol: str,
                        timeframe: str,
                        *,
                        count: int,
                        min_required: int = 0,
                        base_timeframe: str = 'M5',
                        source_path: Optional[Path] = None) -> Optional[pd.DataFrame]:
        """
        Get only the recent tail needed for live evaluation.

        Unlike `get_data`, this method does not load the full canonical M5 file
        or enforce research/backtest minimum-bar thresholds. It is intended for
        the live loop, where `count`/`min_required` are the authoritative bounds.
        """
        target_bars = max(int(count or 0), int(min_required or 0), 1)
        source_rows = self.estimate_source_rows(
            timeframe,
            target_bars,
            base_timeframe=base_timeframe,
        )
        base_df = self._load_recent_source(
            symbol,
            base_timeframe=base_timeframe,
            row_count=source_rows,
            source_path=source_path,
        )
        if base_df is None or len(base_df) == 0:
            return None

        tf = str(timeframe or base_timeframe).upper()
        base_tf = str(base_timeframe or 'M5').upper()
        result = base_df.copy() if tf == base_tf else self.resample(base_df, tf)
        if result is None or len(result) == 0:
            return None
        if count > 0 and len(result) > count:
            result = result.tail(int(count))
        if min_required > 0 and len(result) < int(min_required):
            return None
        return result.copy()
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to Open, High, Low, Close, Volume."""
        column_map = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume',
            'tick_volume': 'Volume',
            'tickvolume': 'Volume',
            'spread': 'Spread',
            'real_volume': 'RealVolume',
            'realvolume': 'RealVolume',
        }

        normalized_names = [
            column_map.get(str(col).strip().lower(), str(col).strip())
            for col in df.columns
        ]
        df = df.copy()
        df.columns = normalized_names

        if df.columns.is_unique:
            return df

        merged_columns: Dict[str, pd.Series] = {}
        ordered_names: List[str] = []
        for column_name in df.columns:
            if column_name in merged_columns:
                continue
            ordered_names.append(column_name)
            column_frame = df.loc[:, df.columns == column_name]
            merged = column_frame.iloc[:, 0].copy()
            for idx in range(1, column_frame.shape[1]):
                merged = merged.combine_first(column_frame.iloc[:, idx])
            merged_columns[column_name] = merged

        return pd.DataFrame({name: merged_columns[name] for name in ordered_names}, index=df.index)
    
    def _parse_datetime_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse datetime and set as index."""
        df.columns = [str(col) for col in df.columns]
        time_col = None
        for col in ['time', 'datetime', 'date', 'timestamp']:
            if col in df.columns.str.lower():
                time_col = [c for c in df.columns if c.lower() == col][0]
                break

        if time_col is None:
            unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed:")]
            fallback_candidates = unnamed_cols or ([df.columns[0]] if len(df.columns) > 0 else [])
            for candidate in fallback_candidates:
                parsed_candidate = pd.to_datetime(df[candidate], errors='coerce')
                if parsed_candidate.notna().sum() >= max(1, len(parsed_candidate) // 2):
                    time_col = candidate
                    df[candidate] = parsed_candidate
                    break

        if time_col is None:
            raise ValueError("No datetime column found")

        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        if df[time_col].isna().all():
            raise ValueError("Datetime column could not be parsed")
        df = df.set_index(time_col)
        df = df.sort_index()
        
        return df
    
    def _validate_data(self, df: pd.DataFrame, symbol: str) -> bool:
        """Validate data quality."""
        required_cols = ['Open', 'High', 'Low', 'Close']
        
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"{symbol}: Missing column {col}")
                return False

        # Coerce OHLC (and optional numeric columns) to numeric to avoid mixed-dtype issues
        # when reading from CSVs. This is especially important for ATR and other features.
        for ncol in ['Open', 'High', 'Low', 'Close', 'Volume', 'Spread']:
            if ncol in df.columns:
                df[ncol] = pd.to_numeric(df[ncol], errors='coerce')

        # Drop rows with missing critical OHLC values (safe: called on fresh DataFrames before caching)
        mask = df[['Open', 'High', 'Low', 'Close']].notna().all(axis=1)
        rows_to_drop = (~mask).sum()
        if rows_to_drop > 0:
            indices_to_drop = df.index[~mask]
            df.drop(indices_to_drop, inplace=True)
            logger.warning(f"{symbol}: Dropped {rows_to_drop} rows with non-numeric/missing OHLC")

        # Check for valid OHLC relationships
        invalid_mask = (
            (df['High'] < df['Low']) |
            (df['High'] < df['Open']) |
            (df['High'] < df['Close']) |
            (df['Low'] > df['Open']) |
            (df['Low'] > df['Close'])
        )
        invalid_bars = int(invalid_mask.sum())
        if invalid_bars > 0:
            df.drop(df.index[invalid_mask], inplace=True)
            logger.warning(f"{symbol}: Dropped {invalid_bars} bars with invalid OHLC relationships")

        if df.empty:
            logger.error(f"{symbol}: No usable rows remain after validation")
            return False
        
        # Check for Volume
        if 'Volume' not in df.columns:
            df['Volume'] = 0
            logger.warning(f"{symbol}: No volume data, using 0")
        
        return True

    @staticmethod
    def _parse_resample_cache_identity(path: Path) -> Tuple[str, str]:
        """Best-effort parse of ``<symbol>_<timeframe>.pkl`` cache filenames."""
        stem = path.stem
        if "_" not in stem:
            return "", ""
        symbol, timeframe = stem.rsplit("_", 1)
        return symbol.upper(), timeframe.upper()

    def prune_resample_cache(self,
                             max_age_days: Optional[int] = None,
                             max_total_bytes: Optional[int] = None,
                             active_symbols: Optional[List[str]] = None,
                             active_timeframes: Optional[List[str]] = None,
                             dry_run: bool = True,
                             recent_window_seconds: int = 300) -> Dict[str, Any]:
        """
        Prune disk-backed resample cache entries.

        This is a storage helper only. It never affects live data loading unless
        explicitly called by a storage manager.
        """
        stats = {
            "files_scanned": 0,
            "files_removed": 0,
            "bytes_reclaimed": 0,
            "files_kept": 0,
            "skipped_recent": 0,
            "skipped_unclassified": 0,
            "dry_run": bool(dry_run),
        }
        if not self.cache_dir.exists():
            return stats

        now_ts = time.time()
        max_age_seconds = None
        if max_age_days is not None:
            try:
                max_age_seconds = max(0, int(max_age_days)) * 86400
            except Exception:
                max_age_seconds = None
        try:
            recent_window_seconds = max(0, int(recent_window_seconds))
        except Exception:
            recent_window_seconds = 300

        active_symbol_set = {str(sym).strip().upper() for sym in (active_symbols or []) if str(sym).strip()}
        active_tf_set = {str(tf).strip().upper() for tf in (active_timeframes or []) if str(tf).strip()}

        entries: List[Dict[str, Any]] = []
        for path in sorted(self.cache_dir.glob("*.pkl")):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            stats["files_scanned"] += 1
            symbol, timeframe = self._parse_resample_cache_identity(path)
            age_seconds = max(0.0, now_ts - float(stat.st_mtime))
            recent = age_seconds < recent_window_seconds
            classified = bool(symbol and timeframe)
            active_match = True
            if active_symbol_set and symbol:
                active_match = active_match and symbol in active_symbol_set
            if active_tf_set and timeframe:
                active_match = active_match and timeframe in active_tf_set
            elif active_tf_set and not timeframe:
                active_match = False

            entries.append({
                "path": path,
                "size": int(stat.st_size),
                "mtime": float(stat.st_mtime),
                "age_seconds": age_seconds,
                "recent": recent,
                "classified": classified,
                "active_match": active_match,
                "remove": False,
            })

        if not entries:
            return stats

        for item in entries:
            if item["recent"]:
                stats["skipped_recent"] += 1
                continue
            remove = False
            if max_age_seconds is not None and item["age_seconds"] > max_age_seconds:
                remove = True
            if item["classified"] and not item["active_match"]:
                remove = True
            if not item["classified"] and max_age_seconds is None:
                stats["skipped_unclassified"] += 1
            item["remove"] = remove

        def _delete_item(item: Dict[str, Any]) -> None:
            path = item["path"]
            try:
                if not dry_run:
                    path.unlink(missing_ok=True)
                stats["files_removed"] += 1
                stats["bytes_reclaimed"] += int(item["size"])
            except Exception:
                return

        for item in entries:
            if item["remove"]:
                _delete_item(item)

        remaining = [item for item in entries if not item["remove"]]
        if max_total_bytes is not None:
            try:
                max_total_bytes = max(0, int(max_total_bytes))
            except Exception:
                max_total_bytes = None
        if max_total_bytes is not None:
            remaining_bytes = sum(int(item["size"]) for item in remaining)
            if remaining_bytes > max_total_bytes:
                quota_candidates = sorted(
                    [item for item in remaining if not item["recent"]],
                    key=lambda item: (item["mtime"], item["size"]),
                )
                for item in quota_candidates:
                    if remaining_bytes <= max_total_bytes:
                        break
                    _delete_item(item)
                    remaining_bytes -= int(item["size"])

        stats["files_kept"] = max(0, stats["files_scanned"] - stats["files_removed"])
        return stats

    def clear_cache(self):
        """Clear the data cache."""
        stats = self.get_resample_cache_stats(reset=True)
        total_lookups = int(stats.get("memory_hits", 0)) + int(stats.get("disk_hits", 0)) + int(stats.get("misses", 0))
        if total_lookups or int(stats.get("writes", 0)):
            hits = int(stats.get("memory_hits", 0)) + int(stats.get("disk_hits", 0))
            hit_rate = (hits / total_lookups * 100.0) if total_lookups else 0.0
            logger.info(
                "ResampleCache stats: "
                f"hits={hits} (mem={stats.get('memory_hits', 0)}, disk={stats.get('disk_hits', 0)}) "
                f"misses={stats.get('misses', 0)} invalidations={stats.get('invalidations', 0)} "
                f"writes={stats.get('writes', 0)} hit_rate={hit_rate:.1f}% "
                f"read_mb={float(stats.get('bytes_read', 0)) / (1024 ** 2):.1f} "
                f"write_mb={float(stats.get('bytes_written', 0)) / (1024 ** 2):.1f} "
                f"read_s={float(stats.get('read_seconds', 0.0)):.3f} "
                f"write_s={float(stats.get('write_seconds', 0.0)):.3f}"
            )
        self._cache.clear()
        self._resample_cache.clear()
        self._resample_meta.clear()
        self._source_meta.clear()

    def get_resample_cache_stats(self, reset: bool = False) -> Dict[str, Any]:
        """Return disk/memory resample-cache telemetry for optimization visibility."""
        stats = dict(self._resample_cache_stats)
        stats["enabled"] = bool(self.cache_resampled)
        stats["cache_dir"] = str(self.cache_dir)
        stats["memory_entries"] = len(self._resample_cache)
        if reset:
            for key in list(self._resample_cache_stats.keys()):
                if isinstance(self._resample_cache_stats[key], float):
                    self._resample_cache_stats[key] = 0.0
                else:
                    self._resample_cache_stats[key] = 0
        return stats


# =============================================================================
# FEATURE COMPUTATION
# =============================================================================

class FeatureComputer:
    """
    Computes technical indicators and features.
    
    Adds common indicators used by strategies:
    - Moving averages (SMA, EMA)
    - ATR (Average True Range)
    - RSI (Relative Strength Index)
    - Bollinger Bands
    - MACD
    - Stochastic
    - And more...
    
    Supports lazy feature computation for efficiency when only specific
    features are needed.
    """

    # In-memory feature cache (bounded to avoid memory bloat).
    # Capacity 24 covers the common per-symbol retrain footprint without thrashing.
    _FEATURE_CACHE: Dict[Tuple[Any, ...], pd.DataFrame] = {}
    _FEATURE_CACHE_ORDER: List[Tuple[Any, ...]] = []
    _FEATURE_CACHE_MAX = 24
    _FEATURE_CACHE_HITS = 0
    _FEATURE_CACHE_MISSES = 0
    _FEATURE_DISK_CACHE_ENABLED = False
    _FEATURE_DISK_CACHE_DIR: Optional[Path] = None
    _FEATURE_DISK_CACHE_MAX_BYTES = 0
    _FEATURE_DISK_CACHE_MIN_FREE_BYTES = 0
    _FEATURE_DISK_CACHE_HITS = 0
    _FEATURE_DISK_CACHE_MISSES = 0
    _FEATURE_DISK_CACHE_WRITES = 0
    _FEATURE_DISK_CACHE_SKIPPED_STORAGE = 0
    _FEATURE_DISK_CACHE_INVALIDATIONS = 0
    _FEATURE_DISK_CACHE_READ_FAILURES = 0
    _FEATURE_DISK_CACHE_WRITE_FAILURES = 0
    _FEATURE_DISK_CACHE_BYTES_READ = 0
    _FEATURE_DISK_CACHE_BYTES_WRITTEN = 0
    _FEATURE_DISK_CACHE_READ_SECONDS = 0.0
    _FEATURE_DISK_CACHE_WRITE_SECONDS = 0.0
    _FEATURE_CODE_FINGERPRINT: Optional[str] = None
    FEATURE_CACHE_VERSION = "2026-05-30-disk-v2"

    @classmethod
    def configure_cache(cls,
                        max_entries: Optional[int] = None,
                        *,
                        disk_enabled: Optional[bool] = None,
                        disk_dir: Optional[Union[str, Path]] = None,
                        disk_max_gb: Optional[float] = None,
                        disk_min_free_gb: Optional[float] = None) -> None:
        """Configure feature caching and drop stale in-memory entries."""
        if max_entries is not None:
            try:
                limit = max(0, int(max_entries))
            except Exception:
                limit = 24
            if limit != cls._FEATURE_CACHE_MAX:
                cls.clear_cache()
                cls._FEATURE_CACHE_MAX = limit

        if disk_enabled is not None:
            cls._FEATURE_DISK_CACHE_ENABLED = bool(disk_enabled)
        if disk_dir is not None:
            disk_path = Path(disk_dir)
            if disk_path:
                cls._FEATURE_DISK_CACHE_DIR = disk_path
        if disk_max_gb is not None:
            try:
                cls._FEATURE_DISK_CACHE_MAX_BYTES = int(max(0.0, float(disk_max_gb)) * (1024 ** 3))
            except Exception:
                cls._FEATURE_DISK_CACHE_MAX_BYTES = 0
        if disk_min_free_gb is not None:
            try:
                cls._FEATURE_DISK_CACHE_MIN_FREE_BYTES = int(max(0.0, float(disk_min_free_gb)) * (1024 ** 3))
            except Exception:
                cls._FEATURE_DISK_CACHE_MIN_FREE_BYTES = 0
        if cls._FEATURE_DISK_CACHE_ENABLED:
            if cls._FEATURE_DISK_CACHE_DIR is None:
                cls._FEATURE_DISK_CACHE_DIR = Path("artifact/fxpm_runtime/feature_cache")
            try:
                cls._FEATURE_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                logger.warning(f"Feature disk cache disabled; cannot create cache dir: {exc}")
                cls._FEATURE_DISK_CACHE_ENABLED = False

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the in-memory feature cache and log hit-rate telemetry."""
        hits = cls._FEATURE_CACHE_HITS
        misses = cls._FEATURE_CACHE_MISSES
        total = hits + misses
        if total > 0:
            rate = (hits / total) * 100.0
            logger.info(
                f"FeatureCache stats: hits={hits} misses={misses} "
                f"hit_rate={rate:.1f}% (cap={cls._FEATURE_CACHE_MAX}, "
                f"size={len(cls._FEATURE_CACHE)})"
            )
        cls._FEATURE_CACHE.clear()
        cls._FEATURE_CACHE_ORDER.clear()
        cls._FEATURE_CACHE_HITS = 0
        cls._FEATURE_CACHE_MISSES = 0

    @classmethod
    def get_cache_stats(cls, reset: bool = False) -> Dict[str, Any]:
        """Return feature-cache telemetry without exposing cached frames."""
        stats = {
            "memory_cap": int(cls._FEATURE_CACHE_MAX),
            "memory_entries": len(cls._FEATURE_CACHE),
            "memory_hits": int(cls._FEATURE_CACHE_HITS),
            "memory_misses": int(cls._FEATURE_CACHE_MISSES),
            "disk_enabled": bool(cls._FEATURE_DISK_CACHE_ENABLED),
            "disk_dir": str(cls._FEATURE_DISK_CACHE_DIR or ""),
            "disk_max_bytes": int(cls._FEATURE_DISK_CACHE_MAX_BYTES),
            "disk_min_free_bytes": int(cls._FEATURE_DISK_CACHE_MIN_FREE_BYTES),
            "disk_free_bytes": cls._disk_free_bytes(),
            "disk_cache_bytes": cls._disk_cache_bytes(),
            "disk_hits": int(cls._FEATURE_DISK_CACHE_HITS),
            "disk_misses": int(cls._FEATURE_DISK_CACHE_MISSES),
            "disk_writes": int(cls._FEATURE_DISK_CACHE_WRITES),
            "disk_skipped_storage": int(cls._FEATURE_DISK_CACHE_SKIPPED_STORAGE),
            "disk_invalidations": int(cls._FEATURE_DISK_CACHE_INVALIDATIONS),
            "disk_read_failures": int(cls._FEATURE_DISK_CACHE_READ_FAILURES),
            "disk_write_failures": int(cls._FEATURE_DISK_CACHE_WRITE_FAILURES),
            "disk_bytes_read": int(cls._FEATURE_DISK_CACHE_BYTES_READ),
            "disk_bytes_written": int(cls._FEATURE_DISK_CACHE_BYTES_WRITTEN),
            "disk_read_seconds": float(cls._FEATURE_DISK_CACHE_READ_SECONDS),
            "disk_write_seconds": float(cls._FEATURE_DISK_CACHE_WRITE_SECONDS),
        }
        if reset:
            cls._FEATURE_CACHE_HITS = 0
            cls._FEATURE_CACHE_MISSES = 0
            cls._FEATURE_DISK_CACHE_HITS = 0
            cls._FEATURE_DISK_CACHE_MISSES = 0
            cls._FEATURE_DISK_CACHE_WRITES = 0
            cls._FEATURE_DISK_CACHE_SKIPPED_STORAGE = 0
            cls._FEATURE_DISK_CACHE_INVALIDATIONS = 0
            cls._FEATURE_DISK_CACHE_READ_FAILURES = 0
            cls._FEATURE_DISK_CACHE_WRITE_FAILURES = 0
            cls._FEATURE_DISK_CACHE_BYTES_READ = 0
            cls._FEATURE_DISK_CACHE_BYTES_WRITTEN = 0
            cls._FEATURE_DISK_CACHE_READ_SECONDS = 0.0
            cls._FEATURE_DISK_CACHE_WRITE_SECONDS = 0.0
        return stats

    # Per-DataFrame attribute key recording which precomputed columns use which
    # parameter tuples. Kept here as a string constant to avoid a circular import.
    _PRECOMPUTED_ATTR = '_fxpm_precomputed_params'

    @staticmethod
    def _stamp_precomputed(features: pd.DataFrame, **params_by_key: Tuple[Any, ...]) -> None:
        """Record which precomputed columns on *features* use which parameter tuples."""
        tags = features.attrs.get(FeatureComputer._PRECOMPUTED_ATTR)
        if tags is None:
            tags = {}
            features.attrs[FeatureComputer._PRECOMPUTED_ATTR] = tags
        tags.update(params_by_key)

    @staticmethod
    def _regime_params_fingerprint(regime_params_file: str) -> Tuple[Any, ...]:
        try:
            path = Path(regime_params_file)
            resolved = str(path.resolve())
            data = path.read_bytes()
            digest = hashlib.sha1(data).hexdigest()
            return resolved, digest, len(data)
        except Exception:
            return str(regime_params_file), None, None

    @classmethod
    def _feature_code_fingerprint(cls) -> str:
        """Hash feature/regime implementation files so code edits invalidate disk cache."""
        if cls._FEATURE_CODE_FINGERPRINT:
            return cls._FEATURE_CODE_FINGERPRINT
        digest = hashlib.sha1()
        for path in [Path(__file__), Path(__file__).with_name("pm_regime.py")]:
            try:
                digest.update(str(path.name).encode("utf-8"))
                digest.update(path.read_bytes())
            except Exception:
                digest.update(str(path).encode("utf-8"))
        cls._FEATURE_CODE_FINGERPRINT = digest.hexdigest()
        return cls._FEATURE_CODE_FINGERPRINT

    @staticmethod
    def _data_fingerprint(df: pd.DataFrame) -> str:
        try:
            raw_cols = [
                c for c in [
                    'Open', 'High', 'Low', 'Close', 'Volume', 'RealVolume',
                    'Spread', 'SpreadOpen', 'SpreadMax', 'SpreadP90', 'SpreadP95',
                ] if c in df.columns
            ]
            source = df.loc[:, raw_cols].copy() if raw_cols else df.copy()
            hashed = pd.util.hash_pandas_object(source, index=True).to_numpy(dtype=np.uint64)
            digest = hashlib.sha1(hashed.tobytes()).hexdigest()
            return digest
        except Exception:
            return "na"

    @staticmethod
    def _make_cache_key(df: pd.DataFrame, symbol: str, timeframe: str,
                        regime_params_file: str, tag: str = "all") -> Tuple[Any, ...]:
        try:
            first_idx = df.index[0]
            last_idx = df.index[-1]
            first_key = first_idx.isoformat() if hasattr(first_idx, "isoformat") else str(first_idx)
            last_key = last_idx.isoformat() if hasattr(last_idx, "isoformat") else str(last_idx)
        except Exception:
            first_key = "na"
            last_key = "na"
        return (
            tag,
            FeatureComputer.FEATURE_CACHE_VERSION,
            FeatureComputer._feature_code_fingerprint(),
            symbol,
            timeframe,
            FeatureComputer._regime_params_fingerprint(regime_params_file),
            len(df),
            first_key,
            last_key,
            tuple(str(c) for c in df.columns),
            FeatureComputer._data_fingerprint(df),
        )

    @classmethod
    def _cache_get(cls, key: Tuple[Any, ...]) -> Optional[pd.DataFrame]:
        if cls._FEATURE_CACHE_MAX <= 0:
            cls._FEATURE_CACHE_MISSES += 1
        elif key in cls._FEATURE_CACHE:
            cls._FEATURE_CACHE_HITS += 1
            return cls._FEATURE_CACHE[key].copy()
        else:
            cls._FEATURE_CACHE_MISSES += 1
        return cls._disk_cache_get(key)

    @classmethod
    def _cache_put(cls, key: Tuple[Any, ...], value: pd.DataFrame) -> None:
        if cls._FEATURE_CACHE_MAX > 0:
            cls._FEATURE_CACHE[key] = value.copy()
            cls._FEATURE_CACHE_ORDER.append(key)
            if len(cls._FEATURE_CACHE_ORDER) > cls._FEATURE_CACHE_MAX:
                oldest = cls._FEATURE_CACHE_ORDER.pop(0)
                cls._FEATURE_CACHE.pop(oldest, None)
        cls._disk_cache_put(key, value)

    @staticmethod
    def _cache_key_digest(key: Tuple[Any, ...]) -> str:
        payload = json.dumps(key, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha1(payload).hexdigest()

    @classmethod
    def _disk_free_bytes(cls) -> Optional[int]:
        cache_dir = cls._FEATURE_DISK_CACHE_DIR
        if cache_dir is None:
            return None
        try:
            target = cache_dir if cache_dir.exists() else cache_dir.parent
            usage = shutil.disk_usage(target)
            return int(usage.free)
        except Exception:
            return None

    @classmethod
    def _disk_cache_bytes(cls) -> int:
        cache_dir = cls._FEATURE_DISK_CACHE_DIR
        if cache_dir is None or not cache_dir.exists():
            return 0
        total = 0
        try:
            for path in cache_dir.glob("feature_*"):
                if path.is_file() and path.suffix in {".json", ".parquet", ".pkl"}:
                    total += int(path.stat().st_size)
        except Exception:
            return total
        return total

    @classmethod
    def _estimated_payload_bytes(cls, value: pd.DataFrame) -> int:
        try:
            raw = int(value.memory_usage(index=True, deep=True).sum())
        except Exception:
            raw = 0
        return max(raw * 2, raw + (64 * 1024 * 1024), 8 * 1024 * 1024)

    @classmethod
    def _has_disk_headroom(cls, reserve_bytes: int = 0) -> bool:
        min_free = int(cls._FEATURE_DISK_CACHE_MIN_FREE_BYTES or 0)
        if min_free <= 0:
            return True
        free = cls._disk_free_bytes()
        if free is None:
            return True
        return int(free) >= int(min_free) + max(0, int(reserve_bytes))

    @classmethod
    def _disk_cache_paths(cls, key: Tuple[Any, ...]) -> Optional[Dict[str, Path]]:
        if not cls._FEATURE_DISK_CACHE_ENABLED or cls._FEATURE_DISK_CACHE_DIR is None:
            return None
        digest = cls._cache_key_digest(key)
        base = cls._FEATURE_DISK_CACHE_DIR / f"feature_{digest}"
        return {
            "manifest": base.with_suffix(".json"),
            "parquet": base.with_suffix(".parquet"),
            "pickle": base.with_suffix(".pkl"),
        }

    @classmethod
    def _disk_cache_get(cls, key: Tuple[Any, ...]) -> Optional[pd.DataFrame]:
        paths = cls._disk_cache_paths(key)
        if paths is None:
            return None
        expected_digest = cls._cache_key_digest(key)
        manifest_path = paths["manifest"]
        pickle_path = paths["pickle"]
        if not manifest_path.exists() and not pickle_path.exists():
            cls._FEATURE_DISK_CACHE_MISSES += 1
            return None
        start = time.perf_counter()
        try:
            if manifest_path.exists():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                if not isinstance(manifest, dict):
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                if manifest.get("format_version") != "feature_cache_manifest_v2":
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                if manifest.get("key_digest") != expected_digest:
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                if manifest.get("feature_cache_version") != cls.FEATURE_CACHE_VERSION:
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                if manifest.get("feature_code_fingerprint") != cls._feature_code_fingerprint():
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                data_path = Path(manifest.get("data_path", ""))
                if not data_path.is_absolute():
                    data_path = manifest_path.parent / data_path
                if not data_path.exists():
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                data = pd.read_parquet(data_path)
                if int(manifest.get("rows", -1)) != int(len(data)):
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                manifest_columns = [str(col) for col in (manifest.get("columns", []) or [])]
                if manifest_columns and manifest_columns != [str(col) for col in data.columns]:
                    cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                    return None
                cls._FEATURE_DISK_CACHE_READ_SECONDS += max(0.0, time.perf_counter() - start)
                cls._FEATURE_DISK_CACHE_BYTES_READ += int(data_path.stat().st_size + manifest_path.stat().st_size)
                cls._FEATURE_DISK_CACHE_HITS += 1
                return data.copy()

            size_bytes = pickle_path.stat().st_size
            payload = pd.read_pickle(pickle_path)
            cls._FEATURE_DISK_CACHE_READ_SECONDS += max(0.0, time.perf_counter() - start)
            cls._FEATURE_DISK_CACHE_BYTES_READ += int(size_bytes)
            if not isinstance(payload, dict):
                cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                return None
            if payload.get("format_version") != "feature_cache_v1":
                cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                return None
            if payload.get("feature_cache_version") != cls.FEATURE_CACHE_VERSION:
                cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                return None
            if payload.get("feature_code_fingerprint") != cls._feature_code_fingerprint():
                cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                return None
            if payload.get("key_digest") != expected_digest or payload.get("key") != key:
                cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                return None
            data = payload.get("data")
            if not isinstance(data, pd.DataFrame):
                cls._FEATURE_DISK_CACHE_INVALIDATIONS += 1
                return None
            cls._FEATURE_DISK_CACHE_HITS += 1
            return data.copy()
        except Exception:
            cls._FEATURE_DISK_CACHE_READ_FAILURES += 1
            return None

    @classmethod
    def _disk_cache_put(cls, key: Tuple[Any, ...], value: pd.DataFrame) -> None:
        paths = cls._disk_cache_paths(key)
        if paths is None:
            return
        estimated_bytes = cls._estimated_payload_bytes(value)
        cls._prune_disk_cache(reserve_bytes=estimated_bytes)
        if not cls._has_disk_headroom(estimated_bytes):
            cls._FEATURE_DISK_CACHE_SKIPPED_STORAGE += 1
            logger.debug(
                "Feature disk cache write skipped: free-space reserve would be breached "
                f"(estimated={estimated_bytes / (1024 ** 2):.1f} MB, "
                f"free={(cls._disk_free_bytes() or 0) / (1024 ** 3):.2f} GB)"
            )
            return
        key_digest = cls._cache_key_digest(key)
        data_path = paths["parquet"]
        manifest_path = paths["manifest"]
        pickle_path = paths["pickle"]
        tmp_data_path = data_path.with_name(f"{data_path.stem}.{os.getpid()}.tmp.parquet")
        tmp_manifest_path = manifest_path.with_name(f"{manifest_path.stem}.{os.getpid()}.tmp.json")
        start = time.perf_counter()
        try:
            value.to_parquet(tmp_data_path, index=True)
            tmp_size = int(tmp_data_path.stat().st_size)
            cls._prune_disk_cache(reserve_bytes=tmp_size)
            if not cls._has_disk_headroom(0):
                cls._FEATURE_DISK_CACHE_SKIPPED_STORAGE += 1
                try:
                    tmp_data_path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.debug("Feature disk cache temp cleanup failed for %s: %s", tmp_data_path, exc)
                return
            manifest = {
                "format_version": "feature_cache_manifest_v2",
                "storage_format": "parquet",
                "feature_cache_version": cls.FEATURE_CACHE_VERSION,
                "feature_code_fingerprint": cls._feature_code_fingerprint(),
                "key_digest": key_digest,
                "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "data_path": data_path.name,
                "rows": int(len(value)),
                "columns": [str(col) for col in value.columns],
            }
            tmp_manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
            os.replace(tmp_data_path, data_path)
            os.replace(tmp_manifest_path, manifest_path)
            try:
                pickle_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("Feature disk cache legacy pickle cleanup failed for %s: %s", pickle_path, exc)
            cls._FEATURE_DISK_CACHE_WRITE_SECONDS += max(0.0, time.perf_counter() - start)
            cls._FEATURE_DISK_CACHE_WRITES += 1
            try:
                cls._FEATURE_DISK_CACHE_BYTES_WRITTEN += int(data_path.stat().st_size + manifest_path.stat().st_size)
            except OSError:
                pass
            cls._prune_disk_cache()
        except Exception:
            cls._FEATURE_DISK_CACHE_WRITE_FAILURES += 1
            try:
                tmp_data_path.unlink(missing_ok=True)
                tmp_manifest_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("Feature disk cache temp cleanup failed after write error: %s", exc)
            cls._disk_cache_put_pickle_fallback(key, value)

    @classmethod
    def _disk_cache_put_pickle_fallback(cls, key: Tuple[Any, ...], value: pd.DataFrame) -> None:
        paths = cls._disk_cache_paths(key)
        if paths is None:
            return
        pickle_path = paths["pickle"]
        tmp_path = pickle_path.with_name(f"{pickle_path.name}.{os.getpid()}.tmp")
        payload = {
            "format_version": "feature_cache_v1",
            "feature_cache_version": cls.FEATURE_CACHE_VERSION,
            "feature_code_fingerprint": cls._feature_code_fingerprint(),
            "key": key,
            "key_digest": cls._cache_key_digest(key),
            "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "data": value.copy(),
        }
        try:
            pd.to_pickle(payload, tmp_path)
            tmp_size = int(tmp_path.stat().st_size)
            cls._prune_disk_cache(reserve_bytes=tmp_size)
            if not cls._has_disk_headroom(0):
                cls._FEATURE_DISK_CACHE_SKIPPED_STORAGE += 1
                tmp_path.unlink(missing_ok=True)
                return
            os.replace(tmp_path, pickle_path)
            cls._FEATURE_DISK_CACHE_WRITES += 1
            cls._FEATURE_DISK_CACHE_BYTES_WRITTEN += int(pickle_path.stat().st_size)
        except Exception:
            cls._FEATURE_DISK_CACHE_WRITE_FAILURES += 1
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("Feature disk cache pickle temp cleanup failed for %s: %s", tmp_path, exc)

    @classmethod
    def _prune_disk_cache(cls, reserve_bytes: int = 0) -> None:
        max_bytes = int(cls._FEATURE_DISK_CACHE_MAX_BYTES or 0)
        cache_dir = cls._FEATURE_DISK_CACHE_DIR
        if cache_dir is None:
            return
        try:
            grouped: Dict[str, Dict[str, Any]] = {}
            for path in cache_dir.glob("feature_*"):
                if not path.is_file():
                    continue
                if ".tmp." in path.name:
                    continue
                if path.suffix not in {".json", ".parquet", ".pkl"}:
                    continue
                stat = path.stat()
                stem = path.stem
                entry = grouped.setdefault(stem, {"mtime": float(stat.st_mtime), "size": 0, "paths": []})
                entry["mtime"] = min(float(entry["mtime"]), float(stat.st_mtime))
                entry["size"] += int(stat.st_size)
                entry["paths"].append(path)
            entries = list(grouped.values())
            total_bytes = sum(int(entry["size"]) for entry in entries)
            free_bytes = cls._disk_free_bytes()
            min_free_target = int(cls._FEATURE_DISK_CACHE_MIN_FREE_BYTES or 0) + max(0, int(reserve_bytes))
            max_ok = max_bytes <= 0 or total_bytes <= max_bytes
            free_ok = free_bytes is None or int(free_bytes) >= min_free_target
            if max_ok and free_ok:
                return
            for entry in sorted(entries, key=lambda item: float(item["mtime"])):
                free_bytes = cls._disk_free_bytes()
                max_ok = max_bytes <= 0 or total_bytes <= max_bytes
                free_ok = free_bytes is None or int(free_bytes) >= min_free_target
                if max_ok and free_ok:
                    break
                for path in entry["paths"]:
                    try:
                        path.unlink(missing_ok=True)
                    except OSError as exc:
                        logger.debug("Feature disk cache prune could not remove %s: %s", path, exc)
                        continue
                total_bytes -= int(entry["size"])
        except OSError as exc:
            logger.debug("Feature disk cache prune skipped after filesystem error: %s", exc)
            return
    
    # Feature dependencies - maps feature names to their computation requirements
    _FEATURE_DEPS = {
        'ATR_7': set(),
        'ATR_10': set(),
        'ATR_14': set(),
        'ATR_20': set(),
        'SMA_5': set(), 'SMA_8': set(), 'SMA_10': set(), 'SMA_13': set(),
        'SMA_20': set(), 'SMA_21': set(), 'SMA_30': set(), 'SMA_50': set(),
        'SMA_100': set(), 'SMA_200': set(),
        'EMA_5': set(), 'EMA_8': set(), 'EMA_10': set(), 'EMA_13': set(),
        'EMA_20': set(), 'EMA_21': set(), 'EMA_30': set(), 'EMA_50': set(),
        'EMA_100': set(), 'EMA_200': set(),
        'RSI_7': set(), 'RSI_14': set(), 'RSI_21': set(),
        'BB_MID_20': set(), 'BB_UPPER_20': set(), 'BB_LOWER_20': set(),
        'MACD': set(), 'MACD_SIGNAL': set(), 'MACD_HIST': set(),
        'STOCH_K': set(), 'STOCH_D': set(),
        'ADX': {'ATR_14'}, 'PLUS_DI': {'ATR_14'}, 'MINUS_DI': {'ATR_14'},
        'CCI': set(),
        'WILLR': set(),
        'DONCHIAN_HIGH_20': set(), 'DONCHIAN_LOW_20': set(),
        'DONCHIAN_HIGH_50': set(), 'DONCHIAN_LOW_50': set(),
        'KC_MID': {'ATR_20'}, 'KC_UPPER': {'ATR_20'}, 'KC_LOWER': {'ATR_20'},
        'HULL_MA': set(),
        'CHANGE': set(), 'CHANGE_PCT': set(),
        'VOLATILITY': set(),
    }
    _REGIME_FEATURE_COLUMNS = {
        'TREND_SCORE', 'RANGE_SCORE', 'BREAKOUT_SCORE', 'CHOP_SCORE',
        'REGIME_RAW', 'REGIME', 'REGIME_STRENGTH', 'REGIME_GAP',
        'REGIME_LIVE', 'REGIME_STRENGTH_LIVE',
    }
    
    @staticmethod
    def compute_required(df: pd.DataFrame, required_features: set,
                        symbol: str = "", timeframe: str = "H1") -> pd.DataFrame:
        """
        Compute only the required features (lazy computation).
        
        This is more efficient than compute_all() when only specific
        features are needed, such as when evaluating a single strategy.
        
        Args:
            df: DataFrame with OHLCV data
            required_features: Set of feature column names needed
            symbol: Symbol name (for regime parameter lookup if needed)
            timeframe: Timeframe string
            
        Returns:
            DataFrame with only the required features computed
        """
        requested = tuple(sorted(str(feat) for feat in required_features))
        request_hash = hashlib.sha1(",".join(requested).encode("utf-8")).hexdigest()[:12]
        cache_key = FeatureComputer._make_cache_key(df, symbol, timeframe, "", tag=f"required:{request_hash}")
        cached = FeatureComputer._cache_get(cache_key)
        if cached is not None:
            return cached

        features = df.copy()
        computed = set(features.columns)
        
        # Expand dependencies
        to_compute = set(required_features)
        for feat in list(to_compute):
            deps = FeatureComputer._FEATURE_DEPS.get(feat, set())
            to_compute.update(deps)
        
        # Compute ATRs if needed
        for period in [7, 10, 14, 20]:
            col = f'ATR_{period}'
            if col in to_compute and col not in computed:
                features.loc[:, col] = FeatureComputer.atr(features, period)
                computed.add(col)

        # Compute SMAs if needed
        for period in [5, 8, 10, 13, 20, 21, 30, 50, 100, 200]:
            col = f'SMA_{period}'
            if col in to_compute and col not in computed:
                features.loc[:, col] = features['Close'].rolling(period).mean()
                computed.add(col)

        # Compute EMAs if needed
        for period in [5, 8, 10, 13, 20, 21, 30, 50, 100, 200]:
            col = f'EMA_{period}'
            if col in to_compute and col not in computed:
                features.loc[:, col] = features['Close'].ewm(span=period, adjust=False).mean()
                computed.add(col)

        # RSI
        for period in [7, 14, 21]:
            col = f'RSI_{period}'
            if col in to_compute and col not in computed:
                features.loc[:, col] = FeatureComputer.rsi(features['Close'], period)
                computed.add(col)

        # Bollinger Bands
        bb_cols = {'BB_MID_20', 'BB_UPPER_20', 'BB_LOWER_20'}
        if bb_cols & to_compute:
            bb_mid, bb_upper, bb_lower = FeatureComputer.bollinger_bands(features['Close'], 20, 2.0)
            if 'BB_MID_20' in to_compute:
                features.loc[:, 'BB_MID_20'] = bb_mid
            if 'BB_UPPER_20' in to_compute:
                features.loc[:, 'BB_UPPER_20'] = bb_upper
            if 'BB_LOWER_20' in to_compute:
                features.loc[:, 'BB_LOWER_20'] = bb_lower
            computed.update(bb_cols)
            if bb_cols.issubset(computed):
                FeatureComputer._stamp_precomputed(features, BB_20=(20, 2.0))

        # MACD
        macd_cols = {'MACD', 'MACD_SIGNAL', 'MACD_HIST'}
        if macd_cols & to_compute:
            macd, signal, hist = FeatureComputer.macd(features['Close'])
            features.loc[:, 'MACD'] = macd
            features.loc[:, 'MACD_SIGNAL'] = signal
            features.loc[:, 'MACD_HIST'] = hist
            computed.update(macd_cols)
            FeatureComputer._stamp_precomputed(features, MACD=(12, 26, 9))

        # Stochastic
        stoch_cols = {'STOCH_K', 'STOCH_D'}
        if stoch_cols & to_compute:
            stoch_k, stoch_d = FeatureComputer.stochastic(features)
            features.loc[:, 'STOCH_K'] = stoch_k
            features.loc[:, 'STOCH_D'] = stoch_d
            computed.update(stoch_cols)
            FeatureComputer._stamp_precomputed(features, STOCH=(14, 3))

        # ADX family (uses cached ATR)
        adx_cols = {'ADX', 'PLUS_DI', 'MINUS_DI'}
        if adx_cols & to_compute:
            atr_14 = features.get('ATR_14', FeatureComputer.atr(features, 14))
            if 'ADX' in to_compute:
                features.loc[:, 'ADX'] = FeatureComputer.adx(features, 14, atr_cache=atr_14)
            if 'PLUS_DI' in to_compute:
                features.loc[:, 'PLUS_DI'] = FeatureComputer.plus_di(features, 14, atr_cache=atr_14)
            if 'MINUS_DI' in to_compute:
                features.loc[:, 'MINUS_DI'] = FeatureComputer.minus_di(features, 14, atr_cache=atr_14)
            computed.update(adx_cols)
            if adx_cols.issubset(computed):
                FeatureComputer._stamp_precomputed(features, ADX=(14,))

        # CCI
        if 'CCI' in to_compute and 'CCI' not in computed:
            features.loc[:, 'CCI'] = FeatureComputer.cci(features, 20)
            computed.add('CCI')

        # Williams %R
        if 'WILLR' in to_compute and 'WILLR' not in computed:
            features.loc[:, 'WILLR'] = FeatureComputer.williams_r(features, 14)
            computed.add('WILLR')

        # Donchian
        for period in [20, 50]:
            high_col = f'DONCHIAN_HIGH_{period}'
            low_col = f'DONCHIAN_LOW_{period}'
            if high_col in to_compute and high_col not in computed:
                features.loc[:, high_col] = features['High'].rolling(period).max()
                computed.add(high_col)
            if low_col in to_compute and low_col not in computed:
                features.loc[:, low_col] = features['Low'].rolling(period).min()
                computed.add(low_col)

        # Keltner Channels
        kc_cols = {'KC_MID', 'KC_UPPER', 'KC_LOWER'}
        if kc_cols & to_compute:
            atr_20 = features.get('ATR_20', FeatureComputer.atr(features, 20))
            kc_mid, kc_upper, kc_lower = FeatureComputer.keltner_channels(
                features, 20, 2.0, atr_cache=atr_20
            )
            if 'KC_MID' in to_compute:
                features.loc[:, 'KC_MID'] = kc_mid
            if 'KC_UPPER' in to_compute:
                features.loc[:, 'KC_UPPER'] = kc_upper
            if 'KC_LOWER' in to_compute:
                features.loc[:, 'KC_LOWER'] = kc_lower
            computed.update(kc_cols)
            if kc_cols.issubset(computed):
                FeatureComputer._stamp_precomputed(features, KC=(20, 20, 2.0))

        # Hull MA
        if 'HULL_MA' in to_compute and 'HULL_MA' not in computed:
            features.loc[:, 'HULL_MA'] = FeatureComputer.hull_ma(features['Close'], 20)
            computed.add('HULL_MA')

        # Simple derived features
        if 'CHANGE' in to_compute and 'CHANGE' not in computed:
            features.loc[:, 'CHANGE'] = features['Close'].diff()
        if 'CHANGE_PCT' in to_compute and 'CHANGE_PCT' not in computed:
            features.loc[:, 'CHANGE_PCT'] = features['Close'].pct_change() * 100
        if 'VOLATILITY' in to_compute and 'VOLATILITY' not in computed:
            features.loc[:, 'VOLATILITY'] = features['Close'].rolling(20).std()
        
        FeatureComputer._cache_put(cache_key, features)
        return features

    @staticmethod
    def compute_for_strategy(df: pd.DataFrame, strategy: Any,
                             symbol: str = "", timeframe: str = "H1",
                             regime_params_file: str = "regime_params.json") -> pd.DataFrame:
        """Build a parameter-aware feature frame for a single strategy context."""
        if strategy is None:
            return FeatureComputer.compute_all(df, symbol=symbol, timeframe=timeframe,
                                               regime_params_file=regime_params_file)

        request: Set[str] = set()
        if hasattr(strategy, "get_feature_request"):
            request.update(set(strategy.get_feature_request() or set()))
        elif hasattr(strategy, "get_required_features"):
            request.update(set(strategy.get_required_features() or set()))
            if hasattr(strategy, "calculate_stops"):
                request.add("ATR_14")

        if not request:
            return df.copy()

        if request & FeatureComputer._REGIME_FEATURE_COLUMNS:
            return FeatureComputer.compute_all(
                df,
                symbol=symbol,
                timeframe=timeframe,
                regime_params_file=regime_params_file,
            )

        return FeatureComputer.compute_required(df, request, symbol=symbol, timeframe=timeframe)
    
    @staticmethod
    def compute_all(df: pd.DataFrame, symbol: str = "", timeframe: str = "H1",
                    regime_params_file: str = "regime_params.json") -> pd.DataFrame:
        """
        Compute all standard features including regime detection.
        
        Args:
            df: DataFrame with OHLCV data
            symbol: Symbol name (for regime parameter lookup)
            timeframe: Timeframe string (for regime parameter lookup)
            regime_params_file: Path to regime parameters JSON
            
        Returns:
            DataFrame with added feature columns including regime columns:
            - TREND_SCORE, RANGE_SCORE, BREAKOUT_SCORE, CHOP_SCORE
            - REGIME_RAW, REGIME, REGIME_STRENGTH, REGIME_GAP
            - REGIME_LIVE, REGIME_STRENGTH_LIVE
        """
        # In-memory cache lookup
        cache_key = FeatureComputer._make_cache_key(df, symbol, timeframe, regime_params_file, tag="all")
        cached = FeatureComputer._cache_get(cache_key)
        if cached is not None:
            return cached

        features = df.copy()
        
        # ATR (multiple periods)
        for period in [7, 10, 14, 20]:
            features.loc[:, f'ATR_{period}'] = FeatureComputer.atr(features, period)

        # Moving Averages
        for period in [5, 8, 10, 13, 20, 21, 30, 50, 100, 200]:
            features.loc[:, f'SMA_{period}'] = features['Close'].rolling(period).mean()
            features.loc[:, f'EMA_{period}'] = features['Close'].ewm(span=period, adjust=False).mean()

        # RSI
        for period in [7, 14, 21]:
            features.loc[:, f'RSI_{period}'] = FeatureComputer.rsi(features['Close'], period)

        # Bollinger Bands
        for period in [20]:
            bb_mid, bb_upper, bb_lower = FeatureComputer.bollinger_bands(
                features['Close'], period, 2.0
            )
            features.loc[:, f'BB_MID_{period}'] = bb_mid
            features.loc[:, f'BB_UPPER_{period}'] = bb_upper
            features.loc[:, f'BB_LOWER_{period}'] = bb_lower

        # MACD
        macd, signal, hist = FeatureComputer.macd(features['Close'])
        features.loc[:, 'MACD'] = macd
        features.loc[:, 'MACD_SIGNAL'] = signal
        features.loc[:, 'MACD_HIST'] = hist

        # Stochastic
        stoch_k, stoch_d = FeatureComputer.stochastic(features)
        features.loc[:, 'STOCH_K'] = stoch_k
        features.loc[:, 'STOCH_D'] = stoch_d

        # ADX, +DI, -DI (optimized: compute ATR once, share across all three)
        atr_14_cached = features['ATR_14']  # Already computed above
        features.loc[:, 'ADX'] = FeatureComputer.adx(features, 14, atr_cache=atr_14_cached)
        features.loc[:, 'PLUS_DI'] = FeatureComputer.plus_di(features, 14, atr_cache=atr_14_cached)
        features.loc[:, 'MINUS_DI'] = FeatureComputer.minus_di(features, 14, atr_cache=atr_14_cached)

        # CCI
        features.loc[:, 'CCI'] = FeatureComputer.cci(features, 20)

        # Williams %R
        features.loc[:, 'WILLR'] = FeatureComputer.williams_r(features, 14)

        # Donchian Channels
        for period in [20, 50]:
            features.loc[:, f'DONCHIAN_HIGH_{period}'] = features['High'].rolling(period).max()
            features.loc[:, f'DONCHIAN_LOW_{period}'] = features['Low'].rolling(period).min()

        # Keltner Channels (use cached ATR_20 if available)
        atr_20_cached = features.get('ATR_20')
        kc_mid, kc_upper, kc_lower = FeatureComputer.keltner_channels(
            features, 20, 2.0, atr_cache=atr_20_cached
        )
        features.loc[:, 'KC_MID'] = kc_mid
        features.loc[:, 'KC_UPPER'] = kc_upper
        features.loc[:, 'KC_LOWER'] = kc_lower

        # Hull MA
        features.loc[:, 'HULL_MA'] = FeatureComputer.hull_ma(features['Close'], 20)

        # Price changes
        features.loc[:, 'CHANGE'] = features['Close'].diff()
        features.loc[:, 'CHANGE_PCT'] = features['Close'].pct_change() * 100

        # Volatility
        features.loc[:, 'VOLATILITY'] = features['Close'].rolling(20).std()

        # Stamp precomputed parameter tuples so strategy helpers can trust
        # these columns without re-checking raw column names alone.
        FeatureComputer._stamp_precomputed(
            features,
            BB_20=(20, 2.0),
            MACD=(12, 26, 9),
            STOCH=(14, 3),
            ADX=(14,),
            KC=(20, 20, 2.0),
        )

        # =====================================================================
        # REGIME DETECTION
        # Compute regime features using symbol/timeframe-specific parameters
        # =====================================================================
        FeatureComputer._append_regime_features(features, symbol, timeframe, regime_params_file)
        
        # Cache and return
        FeatureComputer._cache_put(cache_key, features)
        return features
    
    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range.

        Notes:
            We coerce OHLC columns to numeric to avoid dtype issues when data
            is loaded from CSVs (e.g., strings) or mixed dtypes occur.
        """
        if df is None or len(df) == 0:
            return pd.Series(dtype=float)

        high = pd.to_numeric(df['High'], errors='coerce')
        low = pd.to_numeric(df['Low'], errors='coerce')
        close = pd.to_numeric(df['Close'], errors='coerce')

        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1, skipna=True)
        atr = tr.rolling(window=period, min_periods=period).mean()

        return atr
    
    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index using Wilder's smoothing method."""
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # Use Wilder's exponential smoothing (alpha = 1/period)
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        
        rs = avg_gain / (avg_loss + 1e-10)  # Add epsilon to prevent division by zero
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def bollinger_bands(series: pd.Series, period: int = 20, 
                        std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Bollinger Bands."""
        mid = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = mid + (std_dev * std)
        lower = mid - (std_dev * std)
        
        return mid, upper, lower
    
    @staticmethod
    def macd(series: pd.Series, fast: int = 12, slow: int = 26, 
             signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate MACD."""
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    @staticmethod
    def stochastic(df: pd.DataFrame, k_period: int = 14, 
                   d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        """Calculate Stochastic Oscillator."""
        low_min = df['Low'].rolling(window=k_period).min()
        high_max = df['High'].rolling(window=k_period).max()
        span = (high_max - low_min).replace(0, np.nan)

        stoch_k = 100 * (df['Close'] - low_min) / span
        stoch_d = stoch_k.rolling(window=d_period).mean()
        
        return stoch_k, stoch_d

    @staticmethod
    def _directional_indicators(df: pd.DataFrame, period: int = 14,
                                atr_cache: Optional[pd.Series] = None
                                ) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
        high = pd.to_numeric(df['High'], errors='coerce')
        low = pd.to_numeric(df['Low'], errors='coerce')
        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        atr = atr_cache if atr_cache is not None else FeatureComputer.atr(df, period)
        plus_di = 100 * (plus_dm.rolling(window=period, min_periods=period).mean() / (atr + 1e-10))
        minus_di = 100 * (minus_dm.rolling(window=period, min_periods=period).mean() / (atr + 1e-10))
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        adx = dx.rolling(window=period, min_periods=period).mean()
        return plus_di, minus_di, dx, adx

    @staticmethod
    def adx(df: pd.DataFrame, period: int = 14, atr_cache: pd.Series = None) -> pd.Series:
        """Calculate Average Directional Index."""
        _, _, _, adx = FeatureComputer._directional_indicators(df, period, atr_cache=atr_cache)
        return adx

    @staticmethod
    def plus_di(df: pd.DataFrame, period: int = 14, atr_cache: pd.Series = None) -> pd.Series:
        """Calculate +DI."""
        plus_di, _, _, _ = FeatureComputer._directional_indicators(df, period, atr_cache=atr_cache)
        return plus_di

    @staticmethod
    def minus_di(df: pd.DataFrame, period: int = 14, atr_cache: pd.Series = None) -> pd.Series:
        """Calculate -DI."""
        _, minus_di, _, _ = FeatureComputer._directional_indicators(df, period, atr_cache=atr_cache)
        return minus_di
    
    @staticmethod
    def cci(df: pd.DataFrame, period: int = 20) -> pd.Series:
        """Calculate Commodity Channel Index.
        
        Optimized with vectorized MAD calculation using raw=True for 3-5x speedup.
        """
        tp = (df['High'] + df['Low'] + df['Close']) / 3
        sma = tp.rolling(window=period).mean()
        # Vectorized MAD: use raw=True to pass numpy array directly (faster)
        mad = tp.rolling(window=period).apply(
            lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
        )
        # Protect against division by zero
        cci = (tp - sma) / (0.015 * mad.replace(0, np.nan))
        
        return cci
    
    @staticmethod
    def williams_r(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Williams %R."""
        high_max = df['High'].rolling(window=period).max()
        low_min = df['Low'].rolling(window=period).min()
        span = (high_max - low_min).replace(0, np.nan)

        wr = -100 * (high_max - df['Close']) / span
        
        return wr
    
    @staticmethod
    def keltner_channels(df: pd.DataFrame, period: int = 20,
                         mult: float = 2.0,
                         atr_cache: Optional[pd.Series] = None) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Keltner Channels."""
        mid = df['Close'].ewm(span=period, adjust=False).mean()
        atr = atr_cache if atr_cache is not None else FeatureComputer.atr(df, period)
        upper = mid + (mult * atr)
        lower = mid - (mult * atr)
        
        return mid, upper, lower

    @staticmethod
    def _append_regime_features(features: pd.DataFrame,
                                symbol: str,
                                timeframe: str,
                                regime_params_file: str) -> None:
        """Append regime features through the single canonical detector path."""
        try:
            from pm_regime import MarketRegimeDetector, load_regime_params

            params = load_regime_params(symbol, timeframe, regime_params_file)
            detector = MarketRegimeDetector(params)
            regime_df = detector.compute_regime_scores(features)
            regime_columns = [
                'TREND_SCORE', 'RANGE_SCORE', 'BREAKOUT_SCORE', 'CHOP_SCORE',
                'REGIME_RAW', 'REGIME', 'REGIME_STRENGTH', 'REGIME_GAP',
                'REGIME_LIVE', 'REGIME_STRENGTH_LIVE'
            ]
            for col in regime_columns:
                if col in regime_df.columns:
                    features.loc[:, col] = regime_df[col]
            logger.debug(f"Added regime features for {symbol} {timeframe}")
        except ImportError:
            logger.warning("pm_regime module not available, skipping regime features")
        except Exception as e:
            logger.warning(f"Failed to compute regime features for {symbol} {timeframe}: {e}")
    
    @staticmethod
    def hull_ma(series: pd.Series, period: int = 20) -> pd.Series:
        """Calculate Hull Moving Average using proper Weighted Moving Averages.
        
        Optimized with pre-computed weights array for 2-3x speedup.
        """
        half_period = max(1, int(period / 2))
        sqrt_period = max(1, int(np.sqrt(period)))
        
        # Pre-compute weights outside the rolling function for efficiency
        def weighted_ma(s: pd.Series, p: int, weights_cache: dict = {}) -> pd.Series:
            """Calculate Weighted Moving Average with cached weights."""
            if p not in weights_cache:
                weights_cache[p] = np.arange(1, p + 1, dtype=np.float64)
                weights_cache[f'{p}_sum'] = weights_cache[p].sum()
            weights = weights_cache[p]
            weight_sum = weights_cache[f'{p}_sum']
            return s.rolling(p).apply(lambda x: np.dot(x, weights) / weight_sum, raw=True)
        
        wma_half = weighted_ma(series, half_period)
        wma_full = weighted_ma(series, period)
        
        raw_hma = 2 * wma_half - wma_full
        hma = weighted_ma(raw_hma, sqrt_period)
        
        return hma


# =============================================================================
# DATA SPLITTING
# =============================================================================

class DataSplitter:
    """
    Split data into train, warmup, validation, and holdout regions.

    The overlap window is warmup-only and is never scored.
    """
    SPLIT_CONTRACT_VERSION = "2026-07-10-chronological-lockbox-v1"
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize DataSplitter.

        Args:
            config: Pipeline configuration with split ratios
        """
        self.config = config

    def get_workflow_indices(self, n: int) -> Dict[str, Tuple[int, int]]:
        """
        Get explicit base-M5 workflow indices.

        The active workflow uses the newest active/audit slice inside the
        configured `max_bars` load cap. The oldest configured audit rows in
        that slice are audit-only and the newest `active_universe_bars` rows
        form the live-relevant active universe. Stage 2/risk-management
        selection uses the newest `active_stage2_pct` percent of the active
        universe with warmup context.
        """
        split_mode = str(
            getattr(self.config, "production_split_mode", CALENDAR_SPLIT_MODE)
            or CALENDAR_SPLIT_MODE
        ).lower()
        if split_mode == CALENDAR_SPLIT_MODE:
            raise ValueError(
                "calendar_lockbox_v2 requires timestamp boundaries; "
                "use build_calendar_boundaries instead of integer M5 geometry"
            )
        n = max(0, int(n))
        if n <= 0:
            zero_keys = {
                "workflow_window": (0, 0),
                "historical_stress_audit": (0, 0),
                "active_universe": (0, 0),
                "base_selection": (0, 0),
                "optimizer_train": (0, 0),
                "optimizer_train_warmup": (0, 0),
                "optimizer_train_with_warmup": (0, 0),
                "embargo_train_validation": (0, 0),
                "tuning_validation": (0, 0),
                "tuning_validation_warmup": (0, 0),
                "tuning_validation_with_warmup": (0, 0),
                "embargo_validation_lockbox": (0, 0),
                "final_lockbox": (0, 0),
                "final_lockbox_warmup": (0, 0),
                "final_lockbox_with_warmup": (0, 0),
                "walk_forward_validation": (0, 0),
                "optimization_train": (0, 0),
                "optimization_train_with_warmup": (0, 0),
                "stage2_selection": (0, 0),
                "stage2_selection_with_warmup": (0, 0),
                "stage2_training_selection": (0, 0),
                "stage2_training_selection_with_warmup": (0, 0),
                "clean_holdout": (0, 0),
                "clean_holdout_with_warmup": (0, 0),
            }
            return zero_keys

        max_bars = max(1, int(getattr(self.config, "max_bars", 300000)))
        active_bars = max(1, int(getattr(self.config, "active_universe_bars", 250000)))
        audit_bars = max(0, int(getattr(self.config, "historical_stress_audit_bars", 50000)))
        stage2_pct = max(1.0, min(100.0, float(getattr(self.config, "active_stage2_pct", 50.0))))

        target_workflow_size = active_bars + audit_bars
        if target_workflow_size <= 0:
            target_workflow_size = active_bars
        workflow_size = min(n, max_bars, max(1, target_workflow_size))
        workflow_start = n - workflow_size
        workflow_end = n

        active_size = min(active_bars, workflow_size)
        active_start = workflow_end - active_size
        active_end = workflow_end

        audit_start = workflow_start
        audit_end = active_start

        stage2_size = max(1, int(math.ceil(active_size * (stage2_pct / 100.0))))
        stage2_start = active_end - min(active_size, stage2_size)
        stage2_end = active_end
        embargo_bars = max(0, int(getattr(self.config, "oos_embargo_bars", 0) or 0))

        warmup_bars = int(active_size * (float(getattr(self.config, "overlap_pct", 0.0)) / 100.0))
        stage2_warmup_start = max(active_start, stage2_start - max(0, warmup_bars))
        split_mode = str(
            getattr(self.config, "production_split_mode", "chronological_lockbox_v1")
            or "chronological_lockbox_v1"
        ).lower()
        if split_mode == "chronological_lockbox_v1":
            tuning_bars = max(1, int(getattr(self.config, "tuning_validation_bars", 50000) or 50000))
            lockbox_bars = max(1, int(getattr(self.config, "final_lockbox_bars", 50000) or 50000))
            split_embargo = max(0, int(getattr(self.config, "split_embargo_bars", 300) or 0))

            lockbox_end = active_end
            lockbox_start = lockbox_end - lockbox_bars
            validation_end = lockbox_start - split_embargo
            validation_start = validation_end - tuning_bars
            optimizer_train_end = validation_start - split_embargo
            if optimizer_train_end <= active_start:
                required = tuning_bars + lockbox_bars + (2 * split_embargo) + 1
                raise ValueError(
                    "chronological_lockbox_v1 requires at least one optimizer-train row: "
                    f"active_size={active_size}, required={required} "
                    f"(tuning={tuning_bars}, lockbox={lockbox_bars}, embargo={split_embargo})"
                )

            validation_warmup_start = max(active_start, validation_start - max(0, warmup_bars))
            lockbox_warmup_start = max(active_start, lockbox_start - max(0, warmup_bars))
            return {
                "workflow_window": (workflow_start, workflow_end),
                "historical_stress_audit": (audit_start, audit_end),
                "active_universe": (active_start, active_end),
                "base_selection": (active_start, active_end),
                "optimizer_train": (active_start, optimizer_train_end),
                "optimizer_train_warmup": (active_start, active_start),
                "optimizer_train_with_warmup": (active_start, optimizer_train_end),
                "embargo_train_validation": (optimizer_train_end, validation_start),
                "tuning_validation": (validation_start, validation_end),
                "tuning_validation_warmup": (validation_warmup_start, validation_start),
                "tuning_validation_with_warmup": (validation_warmup_start, validation_end),
                "embargo_validation_lockbox": (validation_end, lockbox_start),
                "final_lockbox": (lockbox_start, lockbox_end),
                "final_lockbox_warmup": (lockbox_warmup_start, lockbox_start),
                "final_lockbox_with_warmup": (lockbox_warmup_start, lockbox_end),
                # Compatibility aliases. New code should use the canonical keys.
                "optimization_train": (active_start, optimizer_train_end),
                "optimization_train_with_warmup": (active_start, optimizer_train_end),
                "walk_forward_validation": (validation_start, validation_end),
                "stage2_selection": (validation_start, validation_end),
                "stage2_selection_with_warmup": (validation_warmup_start, validation_end),
                "stage2_training_selection": (validation_start, validation_end),
                "stage2_training_selection_with_warmup": (validation_warmup_start, validation_end),
                "clean_holdout": (lockbox_start, lockbox_end),
                "clean_holdout_with_warmup": (lockbox_warmup_start, lockbox_end),
            }
        if split_mode == "bleeding_edge":
            walk_forward_bars = max(0, int(getattr(self.config, "walk_forward_validation_bars", 50000) or 0))
            walk_forward_end = max(active_start, stage2_start - embargo_bars)
            walk_forward_start = max(active_start, walk_forward_end - walk_forward_bars)
            return {
                "workflow_window": (workflow_start, workflow_end),
                "historical_stress_audit": (audit_start, audit_end),
                "active_universe": (active_start, active_end),
                "base_selection": (active_start, active_end),
                "optimizer_train": (stage2_start, stage2_end),
                "optimizer_train_with_warmup": (stage2_warmup_start, stage2_end),
                "tuning_validation": (walk_forward_start, walk_forward_end),
                "tuning_validation_with_warmup": (walk_forward_start, walk_forward_end),
                "final_lockbox": (stage2_end, stage2_end),
                "final_lockbox_with_warmup": (stage2_end, stage2_end),
                "embargo_train_validation": (walk_forward_end, stage2_start),
                "embargo_validation_lockbox": (stage2_end, stage2_end),
                "walk_forward_validation": (walk_forward_start, walk_forward_end),
                "optimization_train": (stage2_start, stage2_end),
                "optimization_train_with_warmup": (stage2_warmup_start, stage2_end),
                "stage2_selection": (stage2_start, stage2_end),
                "stage2_selection_with_warmup": (stage2_warmup_start, stage2_end),
                "stage2_training_selection": (stage2_start, stage2_end),
                "stage2_training_selection_with_warmup": (stage2_warmup_start, stage2_end),
                "clean_holdout": (stage2_end, stage2_end),
                "clean_holdout_with_warmup": (stage2_end, stage2_end),
            }

        oos_mode = str(getattr(self.config, "oos_validation_mode", "fixed_holdout") or "fixed_holdout").lower()
        clean_holdout_pct = max(0.0, min(50.0, float(getattr(self.config, "clean_holdout_pct", 0.0) or 0.0)))
        if oos_mode == "purged_kfold":
            clean_holdout_pct = 0.0
        clean_holdout_size = 0
        if clean_holdout_pct > 0.0 and stage2_size > 1:
            clean_holdout_size = int(math.ceil(stage2_size * (clean_holdout_pct / 100.0)))
            clean_holdout_size = min(max(1, clean_holdout_size), max(0, stage2_size - 1))

        clean_holdout_start = stage2_end - clean_holdout_size
        stage2_train_end = (
            max(stage2_start, clean_holdout_start - embargo_bars)
            if clean_holdout_size > 0 else stage2_end
        )
        optimization_train_end = stage2_train_end
        clean_holdout_warmup_start = (
            max(active_start, clean_holdout_start - max(0, warmup_bars))
            if clean_holdout_size > 0 else stage2_end
        )

        return {
            "workflow_window": (workflow_start, workflow_end),
            "historical_stress_audit": (audit_start, audit_end),
            "active_universe": (active_start, active_end),
            "base_selection": (active_start, active_end),
            "optimizer_train": (active_start, optimization_train_end),
            "optimizer_train_with_warmup": (active_start, optimization_train_end),
            "tuning_validation": (stage2_start, stage2_train_end),
            "tuning_validation_with_warmup": (stage2_warmup_start, stage2_train_end),
            "final_lockbox": (
                (clean_holdout_start, stage2_end) if clean_holdout_size > 0 else (stage2_end, stage2_end)
            ),
            "final_lockbox_with_warmup": (
                (clean_holdout_warmup_start, stage2_end) if clean_holdout_size > 0 else (stage2_end, stage2_end)
            ),
            "embargo_train_validation": (stage2_train_end, stage2_train_end),
            "embargo_validation_lockbox": (
                (stage2_train_end, clean_holdout_start) if clean_holdout_size > 0
                else (stage2_end, stage2_end)
            ),
            "walk_forward_validation": (stage2_start, stage2_train_end),
            "optimization_train": (active_start, optimization_train_end),
            "optimization_train_with_warmup": (active_start, optimization_train_end),
            "stage2_selection": (stage2_start, stage2_end),
            "stage2_selection_with_warmup": (stage2_warmup_start, stage2_end),
            "stage2_training_selection": (stage2_start, stage2_train_end),
            "stage2_training_selection_with_warmup": (stage2_warmup_start, stage2_train_end),
            "clean_holdout": (
                (clean_holdout_start, stage2_end) if clean_holdout_size > 0 else (stage2_end, stage2_end)
            ),
            "clean_holdout_with_warmup": (
                (clean_holdout_warmup_start, stage2_end) if clean_holdout_size > 0 else (stage2_end, stage2_end)
            ),
        }


# =============================================================================
# BACKTESTING ENGINE
# =============================================================================

class Backtester:
    """
    Full backtesting engine with complete performance metrics.
    
    Simulates trading with:
    - Tick-based P&L calculation (MT5 parity)
    - Proper entry/exit price calculation (spread)
    - Slippage applied only on STOP exits (not entries or TP)
    - Position sizing based on risk using tick math
    - Volume rounding using broker volume_step
    - Commission deduction
    - Complete performance metrics calculation
    
    Optimizations:
    - Pre-extracts OHLC to NumPy arrays
    - Caches spread price
    - Passes bar index through strategy.build_trade_intent()
    - Numba JIT-compiled main loop (3-10x speedup when available)
    
    The Numba optimization preserves EXACT trade semantics:
    - SL checked before TP (same-bar hit order preserved)
    - Same floating-point precision (float64, no fastmath)
    - Same exit pricing rules (bid for long, ask for short)
    """
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize Backtester.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        # Discovery/backtest scoring must not use live-only regime TP overlays.
        # Stage 2 can still search `tp_atr_mult` normally via strategy grids.

    def _discovery_risk_per_trade_pct(self) -> float:
        try:
            risk = float(getattr(self.config, "discovery_risk_per_trade_pct", 1.0))
            return risk if math.isfinite(risk) and risk > 0 else 1.0
        except Exception:
            return 1.0

    @staticmethod
    def _spread_points_to_half_price(raw: Any, spec: InstrumentSpec) -> Optional[np.ndarray]:
        """Convert MT5 spread points to half-spread price units for each bar."""
        try:
            point = float(getattr(spec, "point", 0.0) or 0.0)
        except Exception:
            point = 0.0
        if point <= 0.0:
            return None
        values = pd.to_numeric(raw, errors="coerce").to_numpy(dtype=np.float64)
        values = np.where(np.isfinite(values) & (values >= 0.0), values, np.nan)
        return values * point * 0.5

    def _build_half_spread_arrays(
        self,
        features: pd.DataFrame,
        spec: InstrumentSpec,
    ) -> Tuple[np.ndarray, np.ndarray, Dict[str, Any]]:
        """
        Build entry and intrabar half-spread arrays.

        Default `fixed_spec` preserves the historical behavior. Evidence modes use
        MT5 bar Spread columns, which are stored in points and converted via the
        instrument spec's broker point. `conservative_max` keeps entry at bar-open
        spread and uses max spread intrabar. Stress modes keep the same intrabar
        max-spread protection while changing only the entry-fill assumption:
        p90, p95, or max spread inside the entry bar.
        """
        n_bars = len(features)
        fixed_half = spec.get_half_spread_price() if bool(getattr(self.config, "use_spread", True)) else 0.0
        fixed_arr = np.full(n_bars, float(fixed_half), dtype=np.float64)
        mode = str(getattr(self.config, "backtest_spread_mode", "fixed_spec") or "fixed_spec").strip().lower()
        asset_class = str(getattr(spec, "asset_class", "unknown") or "unknown").strip().lower()
        broker_fixed_spread_price = float(fixed_half) * 2.0
        meta: Dict[str, Any] = {
            "mode": mode,
            "source": "fixed_spec",
            "units": "price",
            "fallback_to_fixed": False,
            "asset_class": asset_class,
            "broker_fixed_spread_price": broker_fixed_spread_price,
            "spread_provenance_status": "disabled" if not bool(getattr(self.config, "use_spread", True)) else "fixed_spec",
        }
        if not bool(getattr(self.config, "use_spread", True)) or mode == "fixed_spec" or n_bars <= 0:
            return fixed_arr, fixed_arr, meta

        stress_entry_preferences = {
            "stress_p90": ("SpreadP90", "SpreadMax"),
            "stress_p95": ("SpreadP95", "SpreadMax", "SpreadP90"),
            "stress_max": ("SpreadMax",),
        }
        requested_stress_cols = stress_entry_preferences.get(mode, ())
        entry_col = ""
        for candidate_col in requested_stress_cols:
            if candidate_col in features.columns:
                entry_col = candidate_col
                break
        if not entry_col:
            entry_col = "SpreadOpen" if "SpreadOpen" in features.columns else "Spread"
        if mode in {"historical_max", "conservative_max", "stress_p90", "stress_p95", "stress_max"} and "SpreadMax" in features.columns:
            bar_col = "SpreadMax"
        else:
            bar_col = "Spread"

        if entry_col not in features.columns or bar_col not in features.columns:
            meta["fallback_to_fixed"] = True
            meta["missing_columns"] = [col for col in {entry_col, bar_col} if col not in features.columns]
            return fixed_arr, fixed_arr, meta

        entry_hist = self._spread_points_to_half_price(features[entry_col], spec)
        bar_hist = self._spread_points_to_half_price(features[bar_col], spec)
        if entry_hist is None or bar_hist is None:
            meta["fallback_to_fixed"] = True
            meta["reason"] = "invalid_spec_point"
            return fixed_arr, fixed_arr, meta

        entry_hist = np.where(np.isfinite(entry_hist), entry_hist, fixed_arr)
        bar_hist = np.where(np.isfinite(bar_hist), bar_hist, fixed_arr)
        if mode in {"conservative_max", "stress_p90", "stress_p95", "stress_max"}:
            entry_arr = np.maximum(fixed_arr, entry_hist)
            bar_arr = np.maximum(fixed_arr, bar_hist)
        else:
            entry_arr = entry_hist
            bar_arr = bar_hist

        entry_model = {
            "SpreadP90": "p90_spread_stress",
            "SpreadP95": "p95_spread_stress",
            "SpreadMax": "max_spread_stress",
        }.get(entry_col, "bar_open_or_mean")
        meta.update({
            "source": "historical_spread_points",
            "requested_entry_columns": list(requested_stress_cols),
            "entry_column": entry_col,
            "bar_column": bar_col,
            "entry_model": entry_model,
            "stress_entry_fallback": bool(requested_stress_cols and entry_col != requested_stress_cols[0]),
        })
        try:
            effective_spread_price = bar_arr * 2.0
            meta["mean_effective_spread_price"] = float(np.nanmean(effective_spread_price))
            meta["max_effective_spread_price"] = float(np.nanmax(effective_spread_price))
            if broker_fixed_spread_price > 0:
                meta["mean_to_broker_spread_ratio"] = float(
                    meta["mean_effective_spread_price"] / broker_fixed_spread_price
                )
                meta["max_to_broker_spread_ratio"] = float(
                    meta["max_effective_spread_price"] / broker_fixed_spread_price
                )
            if spec.tick_size > 0 and spec.tick_value > 0:
                effective_spread_cash = effective_spread_price / spec.tick_size * spec.tick_value
                meta["mean_effective_spread_cash_per_lot"] = float(np.nanmean(effective_spread_cash))
                meta["max_effective_spread_cash_per_lot"] = float(np.nanmax(effective_spread_cash))
            if spec.pip_size > 0:
                meta["mean_effective_spread_pips"] = float(np.nanmean(effective_spread_price / spec.pip_size))
                meta["max_effective_spread_pips"] = float(np.nanmax(effective_spread_price / spec.pip_size))
            if broker_fixed_spread_price <= 0:
                meta["spread_provenance_status"] = "missing_broker_spread_review"
            elif asset_class == "fx":
                meta["spread_provenance_status"] = "fx_historical_spread"
            else:
                mean_ratio = float(meta.get("mean_to_broker_spread_ratio", 0.0) or 0.0)
                max_ratio = float(meta.get("max_to_broker_spread_ratio", 0.0) or 0.0)
                if mean_ratio >= 5.0 or max_ratio >= 25.0:
                    meta["spread_provenance_status"] = "non_fx_spread_outlier_review"
                else:
                    meta["spread_provenance_status"] = "non_fx_spread_review"
        except Exception as exc:
            logger.debug("Spread provenance metadata enrichment failed for %s: %s", spec.symbol, exc)
        return entry_arr.astype(np.float64), bar_arr.astype(np.float64), meta

    @staticmethod
    def _resolved_triple_rollover_weekday(spec: Optional[InstrumentSpec], config: Any) -> int:
        rollover_day = _WEEKDAY_NAME_TO_INDEX.get(
            str(
                getattr(config, "swap_model_triple_rollover_day", "wednesday")
                or "wednesday"
            ).lower(),
            2,
        )
        if spec is not None:
            try:
                mt5_rollover_day = int(getattr(spec, "swap_rollover3days", -1))
            except (TypeError, ValueError, OverflowError):
                mt5_rollover_day = -1
            if 0 <= mt5_rollover_day <= 6:
                # MT5: Sunday=0 ... Saturday=6. pandas/Python:
                # Monday=0 ... Sunday=6.
                rollover_day = (mt5_rollover_day - 1) % 7
        return int(rollover_day)

    def _build_causal_swap_inputs(
        self,
        features: pd.DataFrame,
        spec: InstrumentSpec,
        entry_prices: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, bool, Dict[str, Any]]:
        """Build observable per-close rollover and per-entry swap arrays."""
        count = int(len(features))
        pre_open_rollover_units = np.zeros(count, dtype=np.int32)
        rollover_units = np.zeros(count, dtype=np.int32)
        long_per_lot = np.zeros(count, dtype=np.float64)
        short_per_lot = np.zeros(count, dtype=np.float64)
        contract: Dict[str, Any] = {
            "contract_version": BACKTEST_SWAP_ACCRUAL_CONTRACT_VERSION,
            "status": "UNAVAILABLE",
            "mode": "noncausal_post_loop_fallback",
            "timestamp_source": None,
            "triple_rollover_weekday": int(
                self._resolved_triple_rollover_weekday(spec, self.config)
            ),
            "execution_state_compounding": False,
            "timestamp_evidence_level": "unavailable",
            "source_provenance_authenticated": False,
            "reason": "observable bar-close timestamps unavailable",
        }
        if not bool(getattr(self.config, "swap_model_enabled", True)):
            contract.update({
                "status": "DISABLED",
                "mode": "disabled",
                "execution_state_compounding": True,
                "reason": "swap model disabled",
            })
            return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, False, contract
        if count <= 0 or "_BAR_CLOSE_TS" not in features.columns:
            return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, False, contract
        try:
            closes = pd.DatetimeIndex(
                pd.to_datetime(features["_BAR_CLOSE_TS"], utc=True, errors="coerce")
            )
        except Exception as exc:
            contract["reason"] = f"bar-close conversion failed: {type(exc).__name__}"
            return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, False, contract
        if (
            len(closes) != count
            or closes.isna().any()
            or closes.has_duplicates
            or not closes.is_monotonic_increasing
        ):
            contract["reason"] = "bar-close axis is incomplete, duplicated, or non-monotonic"
            return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, False, contract
        try:
            opens = pd.DatetimeIndex(
                pd.to_datetime(features.index, utc=True, errors="coerce")
            )
            if len(opens) != count or opens.isna().any() or not bool((closes > opens).all()):
                contract["reason"] = "bar-close timestamps are not after bar opens"
                return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, False, contract
        except Exception as exc:
            contract["reason"] = f"bar-open validation failed: {type(exc).__name__}"
            return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, False, contract

        triple_day = self._resolved_triple_rollover_weekday(spec, self.config)
        first_day = pd.Timestamp(closes[0]).normalize()
        last_day = pd.Timestamp(closes[-1]).normalize()
        cursor = first_day
        while cursor <= last_day:
            # MT5 weekday multipliers name the trading day being rolled
            # forward (for example Wednesday -> Thursday).  ``cursor`` is the
            # rollover boundary itself, so its charge weekday is the preceding
            # calendar day.
            charge_weekday = int((cursor - pd.Timedelta(days=1)).weekday())
            if charge_weekday < 5:
                located = int(closes.searchsorted(cursor, side="left"))
                # A close after an event is not enough: after archive/session
                # gaps, the first later bar may begin after the rollover.  Only
                # attach an event to the interval that actually contains it.
                if (
                    located < count
                    and opens[located] < cursor <= closes[located]
                ):
                    rollover_units[located] += (
                        3 if charge_weekday == triple_day else 1
                    )
                elif located < count and cursor <= opens[located]:
                    pre_open_rollover_units[located] += (
                        3 if charge_weekday == triple_day else 1
                    )
            cursor += pd.Timedelta(days=1)

        raw_long = float(getattr(spec, "swap_long", 0.0) or 0.0)
        raw_short = float(getattr(spec, "swap_short", 0.0) or 0.0)
        for index in np.flatnonzero(np.isfinite(entry_prices)):
            entry_price = float(entry_prices[int(index)])
            long_per_lot[int(index)] = self._swap_value_per_lot_dollars(
                spec,
                raw_long,
                entry_price=entry_price,
            )
            short_per_lot[int(index)] = self._swap_value_per_lot_dollars(
                spec,
                raw_short,
                entry_price=entry_price,
            )
        contract.update({
            "status": "AVAILABLE",
            "mode": "explicit_observable_bar_close",
            "timestamp_source": "_BAR_CLOSE_TS",
            "timestamp_evidence_level": "observable_validated_column",
            "source_provenance_authenticated": False,
            "execution_state_compounding": True,
            "reason": "causal rollover cashflows enter live equity at observable closes",
            "rollover_event_bars": int(np.count_nonzero(
                pre_open_rollover_units.astype(np.int64)
                + rollover_units.astype(np.int64)
            )),
            "pre_open_rollover_event_bars": int(
                np.count_nonzero(pre_open_rollover_units)
            ),
            "close_rollover_event_bars": int(np.count_nonzero(rollover_units)),
            "rollover_units_on_axis": int(
                pre_open_rollover_units.sum() + rollover_units.sum()
            ),
        })
        return pre_open_rollover_units, rollover_units, long_per_lot, short_per_lot, True, contract
     
    def run(self,
            features: pd.DataFrame,
            signals: pd.Series,
            symbol: str,
            strategy: Any,
            spec: Optional[InstrumentSpec] = None,
            timeframe: str = "",
            warmup_bars: int = 0,
            governance_policy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run backtest on a dataset with signals.
        
        Uses the same entry/exit/sizing semantics in both Python and Numba paths.
        `warmup_bars` allows a prepended warmup window while preventing scored trades
        from starting before the scored validation/holdout region.
        
        Args:
            features: DataFrame with OHLCV and indicators
            signals: Series of signals (-1, 0, 1)
            symbol: Symbol being traded
            strategy: Strategy instance (for stop calculation)
            spec: Optional InstrumentSpec (uses default if None)
            
        Returns:
            Dict with complete backtest results and metrics
        """
        if spec is None:
            spec = get_instrument_spec(symbol)
        strategy_name = getattr(strategy, "name", strategy.__class__.__name__)

        # ----------------------------------------------------------------
        # Signal contract validation (never silently repair broken signals)
        # ----------------------------------------------------------------
        if isinstance(signals, pd.Series):
            signal_series = signals.copy()
        else:
            if len(signals) != len(features):
                raise ValueError(
                    f"Signal length mismatch for {strategy_name} on {symbol}: "
                    f"{len(signals)} vs {len(features)} bars"
                )
            signal_series = pd.Series(signals, index=features.index)

        if not signal_series.index.equals(features.index):
            raise ValueError(f"Signal index mismatch for {strategy_name} on {symbol}")

        signal_numeric = pd.to_numeric(signal_series, errors='coerce')
        if signal_numeric.isna().any():
            raise ValueError(f"Signal series contains NaN/non-numeric values for {strategy_name} on {symbol}")

        invalid_signal_mask = ~signal_numeric.isin([-1, 0, 1])
        if invalid_signal_mask.any():
            invalid_values = sorted(signal_numeric[invalid_signal_mask].unique().tolist())
            raise ValueError(
                f"Signal series contains invalid values for {strategy_name} on {symbol}: {invalid_values}"
            )
        signal_numeric = signal_numeric.astype(np.int32)

        n_bars = len(features)
        if n_bars == 0:
            # Never enter the Numba kernel with zero rows: its preallocated
            # equity curve writes index zero and bounds checks are disabled.
            # Preserve the normal empty-result and cost-attestation schema.
            (
                _pre_open_units,
                _close_units,
                _swap_long,
                _swap_short,
                _causal_swap,
                swap_accrual_contract,
            ) = self._build_causal_swap_inputs(
                features,
                spec,
                np.empty(0, dtype=np.float64),
            )
            metrics = self._empty_result()
            metrics["spread_model"] = {
                "source": "unavailable_empty_input",
                "spread_provenance_status": "unavailable",
            }
            metrics["execution_cost_contract"] = (
                BACKTEST_EXECUTION_COST_CONTRACT_VERSION
            )
            metrics["swap_accrual_contract"] = swap_accrual_contract
            metrics["execution_cost_timing_contract"] = {
                "entry_slippage": BACKTEST_EXECUTION_COST_CONTRACT_VERSION,
                "swap_accrual": BACKTEST_SWAP_ACCRUAL_CONTRACT_VERSION,
            }
            return metrics
        
        # ----------------------------------------------------------------
        # Pre-extract arrays for performance (avoid repeated .iloc calls)
        # ----------------------------------------------------------------
        open_arr = features['Open'].to_numpy().astype(np.float64)
        high_arr = features['High'].to_numpy().astype(np.float64)
        low_arr = features['Low'].to_numpy().astype(np.float64)
        close_arr = features['Close'].to_numpy().astype(np.float64)
        sig_arr = signal_numeric.to_numpy().astype(np.int32)
        
        # Cache spread-related arrays. The default mode is a fixed per-symbol
        # spread to preserve benchmark behavior; evidence modes can consume
        # historical bar-level spread columns.
        entry_half_spread_arr, half_spread_arr, spread_meta = self._build_half_spread_arrays(features, spec)
        slippage_price = spec.pips_to_price(self.config.slippage_pips) if self.config.use_slippage else 0.0
        
        if 'ATR_14' in features.columns:
            atr_arr = pd.to_numeric(features['ATR_14'], errors='coerce').to_numpy().astype(np.float64)
        else:
            atr_arr = np.full(n_bars, np.nan, dtype=np.float64)
        normalized_governance = make_policy(governance_policy) if governance_policy else None
        
        # ----------------------------------------------------------------
        # PRE-COMPUTE ENTRY/STOP PRICES ONLY (NOT position sizes)
        # Position sizing must be done inside the loop to use live equity
        # ----------------------------------------------------------------
        sl_prices = np.full(n_bars, np.nan, dtype=np.float64)
        tp_prices = np.full(n_bars, np.nan, dtype=np.float64)
        entry_prices = np.full(n_bars, np.nan, dtype=np.float64)

        strategy_cls_dict = getattr(getattr(strategy, "__class__", object), "__dict__", {})
        strategy_params = getattr(strategy, "params", {}) or {}
        default_atr_stop_fast_path = (
            strategy_cls_dict.get("calculate_stops") is None
            and strategy_cls_dict.get("build_trade_intent") is None
            and not bool(strategy_params.get("use_structural_sl", False))
            and "ATR_14" in features.columns
            and spec.pip_size > 0
        )

        start_i = max(1, warmup_bars)
        if default_atr_stop_fast_path and start_i < n_bars:
            entry_idx_all = np.arange(start_i, n_bars, dtype=np.int64)
            signal_idx_all = entry_idx_all - 1
            directions_all = sig_arr[signal_idx_all]
            active_mask = directions_all != 0
            if np.any(active_mask):
                entry_idx = entry_idx_all[active_mask]
                signal_idx = signal_idx_all[active_mask]
                directions = directions_all[active_mask]

                sl_mult = float(strategy_params.get("sl_atr_mult", 2.0) or 2.0)
                tp_mult = float(strategy_params.get("tp_atr_mult", 2.0) or 2.0)
                atr_pips = atr_arr[signal_idx] / float(spec.pip_size)
                raw_sl_pips = atr_pips * sl_mult
                raw_tp_pips = atr_pips * tp_mult
                sl_pips_arr = np.where(np.isnan(raw_sl_pips), 5.0, np.maximum(5.0, raw_sl_pips))
                tp_pips_arr = np.where(np.isnan(raw_tp_pips), 5.0, np.maximum(5.0, raw_tp_pips))

                bad_mask = (
                    np.isnan(sl_pips_arr) | np.isnan(tp_pips_arr)
                    | (sl_pips_arr <= 0) | (tp_pips_arr <= 0)
                )
                if np.any(bad_mask):
                    bad_pos = int(np.flatnonzero(bad_mask)[0])
                    raise ValueError(
                        f"{strategy_name} produced invalid stops at bar {int(signal_idx[bad_pos])} on {symbol}: "
                        f"sl={float(sl_pips_arr[bad_pos])}, tp={float(tp_pips_arr[bad_pos])}"
                    )

                entry_half_spreads = entry_half_spread_arr[entry_idx]
                if self.config.use_spread:
                    entry_px = np.where(
                        directions == 1,
                        open_arr[entry_idx] + entry_half_spreads,
                        open_arr[entry_idx] - entry_half_spreads,
                    )
                else:
                    entry_px = open_arr[entry_idx]
                if self.config.use_slippage:
                    entry_px = np.where(
                        directions == 1,
                        entry_px + slippage_price,
                        entry_px - slippage_price,
                    )

                sl_delta = sl_pips_arr * float(spec.pip_size)
                tp_delta = tp_pips_arr * float(spec.pip_size)
                entry_prices[entry_idx] = entry_px
                sl_prices[entry_idx] = np.where(directions == 1, entry_px - sl_delta, entry_px + sl_delta)
                tp_prices[entry_idx] = np.where(directions == 1, entry_px + tp_delta, entry_px - tp_delta)
        else:
            for i in range(start_i, n_bars):
                signal = sig_arr[i - 1]  # Signal from previous bar

                if signal != 0:
                    direction = int(signal)
                    is_long = direction == 1
                    signal_bar_index = i - 1

                    # Build the same execution intent live trading uses, evaluated at
                    # the SIGNAL bar (i-1). This keeps optimization, governance policy
                    # selection, and live execution on one exit surface.
                    try:
                        if callable(getattr(strategy, "build_trade_intent", None)):
                            regime_label = ""
                            regime_col = (
                                "REGIME_LIVE" if "REGIME_LIVE" in features.columns
                                else "REGIME" if "REGIME" in features.columns
                                else None
                            )
                            if regime_col is not None:
                                try:
                                    regime_label = str(features[regime_col].iat[signal_bar_index])
                                except Exception:
                                    regime_label = ""
                            intent = strategy.build_trade_intent(
                                features,
                                symbol=symbol,
                                timeframe=timeframe,
                                regime=regime_label,
                                signal=direction,
                                spec=spec,
                                bar_index=signal_bar_index,
                                apply_regime_tp_multiplier=False,
                            )
                            sl_pips = float(intent.stop_loss_pips)
                            tp_pips = float(intent.take_profit_pips)
                        else:
                            sl_pips, tp_pips = strategy.calculate_stops(
                                features, direction, symbol, spec=spec, bar_index=signal_bar_index
                            )
                    except TypeError:
                        # Fallback for legacy strategies/test doubles without the
                        # trade-intent or bar_index-aware stop signature.
                        try:
                            sl_pips, tp_pips = strategy.calculate_stops(
                                features.iloc[:signal_bar_index + 1].copy(), direction, symbol
                            )
                        except Exception as exc:
                            raise RuntimeError(
                                f"{strategy_name} stop calculation failed at bar {signal_bar_index} on {symbol}"
                            ) from exc
                    except Exception as exc:
                        raise RuntimeError(
                            f"{strategy_name} stop calculation failed at bar {signal_bar_index} on {symbol}"
                        ) from exc

                    if np.isnan(sl_pips) or np.isnan(tp_pips) or sl_pips <= 0 or tp_pips <= 0:
                        raise ValueError(
                            f"{strategy_name} produced invalid stops at bar {signal_bar_index} on {symbol}: "
                            f"sl={sl_pips}, tp={tp_pips}"
                        )

                    # Entry price: Open of ENTRY bar (i), plus spread and an
                    # adverse market-entry slippage allowance.
                    open_price = open_arr[i]
                    entry_half_spread = entry_half_spread_arr[i] if i < len(entry_half_spread_arr) else 0.0
                    if self.config.use_spread:
                        entry_price = open_price + entry_half_spread if is_long else open_price - entry_half_spread
                    else:
                        entry_price = open_price
                    if self.config.use_slippage:
                        entry_price = (
                            entry_price + slippage_price
                            if is_long
                            else entry_price - slippage_price
                        )

                    # Set SL/TP prices
                    if direction == 1:
                        stop_loss = entry_price - spec.pips_to_price(sl_pips)
                        take_profit = entry_price + spec.pips_to_price(tp_pips)
                    else:
                        stop_loss = entry_price + spec.pips_to_price(sl_pips)
                        take_profit = entry_price - spec.pips_to_price(tp_pips)

                    # Store pre-computed prices (NOT sizes - those use live equity)
                    entry_prices[i] = entry_price
                    sl_prices[i] = stop_loss
                    tp_prices[i] = take_profit
        
        # ----------------------------------------------------------------
        # RUN BACKTEST LOOP (Numba JIT or Pure Python)
        # ----------------------------------------------------------------
        discovery_risk_per_trade_pct = self._discovery_risk_per_trade_pct()
        (
            pre_open_rollover_units_arr,
            rollover_units_arr,
            swap_long_per_lot_arr,
            swap_short_per_lot_arr,
            causal_swap_enabled,
            swap_accrual_contract,
        ) = self._build_causal_swap_inputs(features, spec, entry_prices)
        if NUMBA_AVAILABLE and normalized_governance is None:
            # Use Numba JIT-compiled kernel with live-equity sizing
            (
                trade_signal_bars, trade_entry_bars, trade_exit_bars,
                trade_directions, trade_entry_prices, trade_exit_prices,
                trade_sl_prices, trade_tp_prices, trade_sizes,
                trade_pnl_dollars, trade_pnl_pips,
                trade_exit_reasons, trade_risk_amounts,
                equity_curve, final_equity, max_drawdown,
                trade_mae_pips, trade_mfe_pips, trade_mae_r, trade_mfe_r,
                trade_time_to_mae_bars, trade_time_to_mfe_bars,
                trade_time_underwater_bars, trade_entry_to_stop_pips,
                trade_entry_to_target_pips,
                trade_gross_pnl_dollars,
                trade_swap_rollover_units, trade_swap_dollars,
            ) = _backtest_loop_numba(
                open_arr, high_arr, low_arr, close_arr, sig_arr,
                sl_prices, tp_prices, entry_prices,
                self.config.initial_capital,
                discovery_risk_per_trade_pct,
                half_spread_arr, slippage_price,
                self.config.use_spread, self.config.use_slippage,
                self.config.use_commission, spec.commission_per_lot,
                spec.tick_size, spec.tick_value, spec.pip_size, spec.pip_value,
                spec.min_lot, spec.max_lot, spec.volume_step,
                warmup_bars,
                rollover_units_arr,
                swap_long_per_lot_arr,
                swap_short_per_lot_arr,
                causal_swap_enabled,
                pre_open_rollover_units_arr,
            )
            
            # Convert JIT results to trade dicts
            exit_reason_map = {
                EXIT_NONE: "none",
                EXIT_SL: TradeStatus.CLOSED_SL.value,
                EXIT_TP: TradeStatus.CLOSED_TP.value,
                EXIT_EOD: TradeStatus.CLOSED_EOD.value
            }
            
            trades = []
            for j in range(len(trade_signal_bars)):
                r_multiple = trade_pnl_dollars[j] / trade_risk_amounts[j] if trade_risk_amounts[j] > 0 else 0.0
                
                trades.append({
                    'signal_bar': int(trade_signal_bars[j]),
                    'entry_bar': int(trade_entry_bars[j]),
                    'exit_bar': int(trade_exit_bars[j]),
                    'signal_time': features.index[int(trade_signal_bars[j])],
                    'entry_time': features.index[int(trade_entry_bars[j])],
                    'exit_time': features.index[int(trade_exit_bars[j])],
                    'direction': 'LONG' if trade_directions[j] == 1 else 'SHORT',
                    'entry_price': trade_entry_prices[j],
                    'exit_price': trade_exit_prices[j],
                    'stop_loss': trade_sl_prices[j],
                    'take_profit': trade_tp_prices[j],
                    'position_size': trade_sizes[j],
                    'pnl_pips': round(trade_pnl_pips[j], 2),
                    'gross_pnl_dollars': round(trade_gross_pnl_dollars[j], 2),
                    'swap_rollover_units': int(trade_swap_rollover_units[j]),
                    'swap_dollars': round(trade_swap_dollars[j], 2),
                    'pnl_dollars': round(trade_pnl_dollars[j], 2),
                    'exit_reason': exit_reason_map.get(trade_exit_reasons[j], "unknown"),
                    'risk_amount': round(trade_risk_amounts[j], 2),
                    'r_multiple': round(r_multiple, 3),
                    'trade_record_schema_version': BACKTEST_TRADE_RECORD_SCHEMA_VERSION,
                    'mae_pips': round(float(trade_mae_pips[j]), 2),
                    'mfe_pips': round(float(trade_mfe_pips[j]), 2),
                    'mae_r': round(float(trade_mae_r[j]), 3),
                    'mfe_r': round(float(trade_mfe_r[j]), 3),
                    'time_to_mae_bars': int(trade_time_to_mae_bars[j]),
                    'time_to_mfe_bars': int(trade_time_to_mfe_bars[j]),
                    'time_underwater_bars': int(trade_time_underwater_bars[j]),
                    'entry_to_stop_pips': round(float(trade_entry_to_stop_pips[j]), 2),
                    'entry_to_target_pips': round(float(trade_entry_to_target_pips[j]), 2),
                    'governance_policy': 'control_fixed',
                    'tp_released': False,
                })
            
            equity_curve = equity_curve.tolist()
            
        else:
            trades, final_equity, max_drawdown, equity_curve = self._run_python_loop(
                features, open_arr, high_arr, low_arr, close_arr, sig_arr,
                sl_prices, tp_prices, entry_prices,
                half_spread_arr, slippage_price, spec, warmup_bars,
                atr_arr=atr_arr,
                governance_policy=normalized_governance,
                discovery_risk_per_trade_pct=discovery_risk_per_trade_pct,
                rollover_units_arr=rollover_units_arr,
                swap_long_per_lot_arr=swap_long_per_lot_arr,
                swap_short_per_lot_arr=swap_short_per_lot_arr,
                causal_swap_enabled=causal_swap_enabled,
                pre_open_rollover_units_arr=pre_open_rollover_units_arr,
            )
        
        if bool(getattr(self.config, "swap_model_enabled", True)) and not causal_swap_enabled:
            trades, final_equity, max_drawdown, equity_curve = self._apply_swap_model(
                trades,
                final_equity,
                equity_curve,
                max_drawdown,
                features,
                spec,
                timeframe=timeframe,
            )

        metrics = self._calculate_metrics(
            trades,
            final_equity,
            equity_curve,
            max_drawdown,
            features,
            timeframe,
            warmup_bars=warmup_bars,
        )
        metrics["spread_model"] = spread_meta
        metrics["execution_cost_contract"] = BACKTEST_EXECUTION_COST_CONTRACT_VERSION
        metrics["swap_accrual_contract"] = swap_accrual_contract
        metrics["execution_cost_timing_contract"] = {
            "entry_slippage": BACKTEST_EXECUTION_COST_CONTRACT_VERSION,
            "swap_accrual": BACKTEST_SWAP_ACCRUAL_CONTRACT_VERSION,
        }
        return metrics

    @staticmethod
    def _max_drawdown_from_equity_curve(equity_curve: List[float]) -> float:
        peak = None
        max_dd = 0.0
        for raw_value in equity_curve or []:
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            if peak is None or value > peak:
                peak = value
            if peak and peak > 0.0:
                max_dd = max(max_dd, (peak - value) / peak * 100.0)
        return max_dd

    @staticmethod
    def _trade_is_long(trade: Dict[str, Any]) -> bool:
        direction = trade.get("direction", "")
        if isinstance(direction, str):
            return direction.strip().upper() in {"LONG", "BUY", "1"}
        try:
            return int(direction) == 1
        except (TypeError, ValueError):
            return True

    def _calendar_swap_rollover_schedule(
        self,
        trade: Dict[str, Any],
        spec: Optional[InstrumentSpec] = None,
    ) -> Optional[List[Tuple[pd.Timestamp, int]]]:
        """Return modeled calendar rollover instants and their unit weights.

        ``None`` means timestamp evidence was unusable and the caller must use
        the legacy bar-duration fallback.  An empty list is a valid timestamped
        holding with no modeled rollover crossing.
        """
        try:
            entry_time = pd.Timestamp(trade.get("entry_time"))
            exit_time = pd.Timestamp(trade.get("exit_time"))
        except Exception:
            return None
        if (
            pd.isna(entry_time)
            or pd.isna(exit_time)
            or exit_time <= entry_time
        ):
            return None

        rollover_day = _WEEKDAY_NAME_TO_INDEX.get(
            str(
                getattr(
                    self.config,
                    "swap_model_triple_rollover_day",
                    "wednesday",
                )
            ).lower(),
            2,
        )
        if spec is not None:
            try:
                mt5_rollover_day = int(getattr(spec, "swap_rollover3days", -1))
            except (TypeError, ValueError, OverflowError):
                mt5_rollover_day = -1
            if 0 <= mt5_rollover_day <= 6:
                # MT5: Sunday=0 ... Saturday=6. pandas/Python:
                # Monday=0 ... Sunday=6.
                rollover_day = (mt5_rollover_day - 1) % 7

        schedule: List[Tuple[pd.Timestamp, int]] = []
        cursor = entry_time.normalize() + pd.Timedelta(days=1)
        end_day = exit_time.normalize()
        while cursor <= end_day:
            charge_weekday = int((cursor - pd.Timedelta(days=1)).weekday())
            if charge_weekday < 5:
                schedule.append(
                    (
                        pd.Timestamp(cursor),
                        3 if charge_weekday == rollover_day else 1,
                    )
                )
            cursor += pd.Timedelta(days=1)
        return schedule

    def _swap_rollover_units(self,
                             trade: Dict[str, Any],
                             features: Optional[pd.DataFrame],
                             timeframe: str = "",
                             spec: Optional[InstrumentSpec] = None) -> int:
        """Count broker rollover units for one closed trade."""
        schedule = self._calendar_swap_rollover_schedule(trade, spec=spec)
        if schedule is not None:
            return max(0, sum(int(units) for _timestamp, units in schedule))

        try:
            entry_bar = int(trade.get("entry_bar", 0) or 0)
            exit_bar = int(trade.get("exit_bar", entry_bar) or entry_bar)
            bars_held = max(0, exit_bar - entry_bar)
        except Exception:
            bars_held = 0
        if bars_held <= 0:
            return 0

        bars_per_day = {
            "M1": 1440,
            "M5": 288,
            "M15": 96,
            "M30": 48,
            "H1": 24,
            "H4": 6,
            "D1": 1,
        }.get(str(timeframe or "").upper(), 24)
        min_hours = float(getattr(self.config, "swap_model_min_holding_hours", 20.0) or 0.0)
        holding_hours = bars_held * (24.0 / max(1, bars_per_day))
        if holding_hours < min_hours:
            return 0
        return max(0, int(math.floor(bars_held / max(1, bars_per_day))))

    @staticmethod
    def _swap_value_per_lot_dollars(
        spec: InstrumentSpec,
        raw_swap: float,
        *,
        entry_price: Optional[float] = None,
    ) -> float:
        """Convert configured/MT5 swap value to account-currency dollars per lot."""
        try:
            swap_value = float(raw_swap or 0.0)
        except Exception:
            return 0.0
        try:
            mode = int(getattr(spec, "swap_mode", -1))
        except Exception:
            mode = -1
        if mode == 0:
            return 0.0
        if mode in {-1, 4}:
            return swap_value
        if mode == 1:
            tick_size = float(getattr(spec, "tick_size", 0.0) or 0.0)
            tick_value = float(getattr(spec, "tick_value", 0.0) or 0.0)
            point = float(getattr(spec, "point", 0.0) or 0.0)
            if tick_size > 0.0 and tick_value > 0.0 and point > 0.0:
                return swap_value * (point / tick_size) * tick_value
            if swap_value:
                raise ValueError("swap mode 1 requires positive point/tick geometry")
            return 0.0
        if mode == 6:
            # SYMBOL_SWAP_MODE_INTEREST_OPEN: annual percentage of the
            # position's opening notional on a 360-day bank year.
            try:
                price = float(entry_price)
                contract_size = float(getattr(spec, "contract_size", 0.0) or 0.0)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("swap mode 6 requires entry price and contract size") from exc
            if not (
                math.isfinite(price)
                and price > 0.0
                and math.isfinite(contract_size)
                and contract_size > 0.0
            ):
                raise ValueError("swap mode 6 requires entry price and contract size")
            return price * contract_size * (swap_value / 100.0) / 360.0
        if swap_value:
            raise ValueError(f"unsupported nonzero MT5 swap mode {mode}")
        return 0.0

    @staticmethod
    def _swap_accrual_bar_index(
        rollover_time: pd.Timestamp,
        features: Optional[pd.DataFrame],
        *,
        exit_bar: int,
        curve_length: int,
    ) -> int:
        """Map one modeled rollover to the first available closing equity mark.

        A valid close column makes bar-close timing observable.  No feature-
        index fallback is allowed to claim causal timing or source provenance.
        The result is bounded by the trade exit and the available equity path
        so terminal swap stays identical even on an incomplete timestamp axis.
        """
        terminal = max(0, min(int(exit_bar), max(0, int(curve_length) - 1)))
        if features is None or len(features) == 0 or curve_length <= 0:
            return terminal
        if "_BAR_CLOSE_TS" not in features.columns:
            return terminal
        try:
            raw_axis = features["_BAR_CLOSE_TS"]
            mark_axis = pd.DatetimeIndex(
                pd.to_datetime(raw_axis, utc=True, errors="coerce")
            )
            if (
                len(mark_axis) != len(features)
                or mark_axis.isna().any()
                or mark_axis.has_duplicates
                or not mark_axis.is_monotonic_increasing
            ):
                return terminal
            event = pd.Timestamp(rollover_time)
            event = (
                event.tz_localize("UTC")
                if event.tzinfo is None
                else event.tz_convert("UTC")
            )
            located = int(mark_axis.searchsorted(event, side="left"))
            if located >= len(mark_axis):
                return terminal
            open_axis = pd.DatetimeIndex(
                pd.to_datetime(features.index, utc=True, errors="coerce")
            )
            if (
                len(open_axis) != len(mark_axis)
                or open_axis.isna().any()
                or not (open_axis[located] < event <= mark_axis[located])
            ):
                return terminal
            return max(0, min(located, terminal, int(curve_length) - 1))
        except Exception:
            return terminal

    def _apply_swap_model(self,
                          trades: List[Dict[str, Any]],
                          final_equity: float,
                          equity_curve: List[float],
                          max_drawdown: float,
                          features: Optional[pd.DataFrame],
                          spec: InstrumentSpec,
                          *,
                          timeframe: str = "") -> Tuple[List[Dict[str, Any]], float, float, List[float]]:
        """Apply signed overnight swap to closed trades and the equity curve."""
        if not bool(getattr(self.config, "swap_model_enabled", True)):
            return trades, final_equity, max_drawdown, equity_curve
        if not trades:
            return trades, final_equity, max_drawdown, equity_curve

        curve_length = len(equity_curve or [])
        adjustments_by_effective_bar: Dict[int, float] = {}
        total_swap = 0.0
        for trade in trades:
            schedule = self._calendar_swap_rollover_schedule(trade, spec=spec)
            units = (
                sum(int(weight) for _timestamp, weight in schedule)
                if schedule is not None
                else self._swap_rollover_units(
                    trade, features, timeframe=timeframe, spec=spec
                )
            )
            if units <= 0:
                trade["swap_rollover_units"] = 0
                trade["swap_dollars"] = 0.0
                continue
            volume = float(trade.get("position_size", trade.get("size", 0.0)) or 0.0)
            raw_daily_swap = float(spec.swap_long if self._trade_is_long(trade) else spec.swap_short)
            daily_swap = self._swap_value_per_lot_dollars(
                spec,
                raw_daily_swap,
                entry_price=trade.get("entry_price"),
            )
            swap_dollars = daily_swap * volume * units
            if not math.isfinite(swap_dollars):
                raise ValueError("nonfinite modeled swap cashflow")
            gross_pnl = float(trade.get("pnl_dollars", 0.0) or 0.0)
            trade.setdefault("gross_pnl_dollars", round(gross_pnl, 2))
            trade["swap_rollover_units"] = int(units)
            trade["swap_dollars"] = round(float(swap_dollars), 2)
            trade["pnl_dollars"] = round(gross_pnl + swap_dollars, 2)
            risk_amount = float(trade.get("risk_amount", 0.0) or 0.0)
            if risk_amount > 0.0:
                trade["r_multiple"] = round(float(trade["pnl_dollars"]) / risk_amount, 3)
            total_swap += swap_dollars
            try:
                exit_bar = int(trade.get("exit_bar", 0) or 0)
                if schedule:
                    for rollover_time, rollover_units in schedule:
                        effective_bar = self._swap_accrual_bar_index(
                            rollover_time,
                            features,
                            exit_bar=exit_bar,
                            curve_length=curve_length,
                        )
                        amount = daily_swap * volume * int(rollover_units)
                        adjustments_by_effective_bar[effective_bar] = (
                            adjustments_by_effective_bar.get(effective_bar, 0.0)
                            + amount
                        )
                else:
                    # Timestamp-free compatibility fallback: retain the old
                    # exit booking because no causal rollover instant is known.
                    effective_bar = max(
                        0,
                        min(exit_bar, max(0, curve_length - 1)),
                    )
                    adjustments_by_effective_bar[effective_bar] = (
                        adjustments_by_effective_bar.get(effective_bar, 0.0)
                        + swap_dollars
                    )
            except Exception:
                effective_bar = max(0, min(
                    int(trade.get("exit_bar", 0) or 0),
                    max(0, curve_length - 1),
                ))
                adjustments_by_effective_bar[effective_bar] = (
                    adjustments_by_effective_bar.get(effective_bar, 0.0)
                    + swap_dollars
                )

        if not adjustments_by_effective_bar and abs(total_swap) <= 1e-12:
            return trades, final_equity, max_drawdown, equity_curve

        adjusted_curve = list(equity_curve or [])
        if adjusted_curve:
            cumulative = 0.0
            for idx in range(len(adjusted_curve)):
                cumulative += adjustments_by_effective_bar.get(idx, 0.0)
                if cumulative:
                    adjusted_curve[idx] = float(adjusted_curve[idx]) + cumulative
            last_adjusted_idx = len(adjusted_curve) - 1
            trailing = sum(
                amount
                for effective_bar, amount in adjustments_by_effective_bar.items()
                if int(effective_bar) > last_adjusted_idx
            )
            if trailing:
                adjusted_curve[-1] = float(adjusted_curve[-1]) + trailing
        adjusted_final_equity = float(final_equity) + float(total_swap)
        adjusted_max_dd = self._max_drawdown_from_equity_curve(adjusted_curve) if adjusted_curve else max_drawdown
        return trades, adjusted_final_equity, adjusted_max_dd, adjusted_curve
    
    def _run_python_loop(self, features, open_arr, high_arr, low_arr, close_arr, sig_arr,
                         sl_prices, tp_prices, entry_prices,
                         half_spread_arr, slippage_price, spec,
                         warmup_bars: int = 0,
                         atr_arr: Optional[np.ndarray] = None,
                         governance_policy: Optional[Dict[str, Any]] = None,
                         discovery_risk_per_trade_pct: float = 1.0,
                         rollover_units_arr: Optional[np.ndarray] = None,
                         swap_long_per_lot_arr: Optional[np.ndarray] = None,
                         swap_short_per_lot_arr: Optional[np.ndarray] = None,
                         causal_swap_enabled: bool = False,
                         pre_open_rollover_units_arr: Optional[np.ndarray] = None) -> Tuple[List[Dict], float, float, List[float]]:
        """
        Pure Python backtest loop (fallback when Numba not available).
        
        Uses LIVE EQUITY for position sizing (compounding preserved).
        """
        equity = self.config.initial_capital
        peak_equity = equity
        max_drawdown = 0.0
        
        trades: List[Dict] = []
        equity_curve: List[float] = [equity]
        
        # Position state
        in_position = False
        position_direction = 0
        entry_price = 0.0
        entry_bar = 0
        signal_bar = 0
        stop_loss = 0.0
        take_profit = 0.0
        position_size = 0.0
        risk_amount_at_entry = 0.0
        daily_swap_per_lot_current = 0.0
        accrued_swap_units_current = 0
        accrued_swap_dollars_current = 0.0
        initial_stop_loss = 0.0
        initial_take_profit = 0.0
        highest_since_entry = 0.0
        lowest_since_entry = 0.0
        entry_to_stop_pips = 0.0
        entry_to_target_pips = 0.0
        mae_pips_current = 0.0
        mfe_pips_current = 0.0
        time_to_mae_bars_current = 0
        time_to_mfe_bars_current = 0
        time_underwater_bars_current = 0
        governance_tp_released = False
        governance_partial_taken = False
        entry_regime_label = ""
        active_governance_name = (
            governance_policy.get("name", "control_fixed")
            if isinstance(governance_policy, dict) else "control_fixed"
        )
        pip_size_value = float(getattr(spec, "pip_size", 0.0) or 0.0)

        def _update_excursion(direction: int,
                              entry: float,
                              bars_held: int,
                              mae_pips: float,
                              mfe_pips: float,
                              time_to_mae: int,
                              time_to_mfe: int,
                              underwater_bars: int) -> Tuple[float, float, int, int, int]:
            if pip_size_value <= 0.0:
                return mae_pips, mfe_pips, time_to_mae, time_to_mfe, underwater_bars
            if direction == 1:
                adverse_pips = (entry - bid_low) / pip_size_value
                favorable_pips = (bid_high - entry) / pip_size_value
                close_mark_pips = (bid_close - entry) / pip_size_value
            else:
                adverse_pips = (ask_high - entry) / pip_size_value
                favorable_pips = (entry - ask_low) / pip_size_value
                close_mark_pips = (entry - ask_close) / pip_size_value
            if adverse_pips > mae_pips:
                mae_pips = adverse_pips
                time_to_mae = bars_held
            if favorable_pips > mfe_pips:
                mfe_pips = favorable_pips
                time_to_mfe = bars_held
            if close_mark_pips < 0.0:
                underwater_bars += 1
            return mae_pips, mfe_pips, time_to_mae, time_to_mfe, underwater_bars

        def _update_open_excursion(direction: int,
                                   entry: float,
                                   bars_held: int,
                                   mae_pips: float,
                                   mfe_pips: float,
                                   time_to_mae: int,
                                   time_to_mfe: int) -> Tuple[float, float, int, int]:
            """Mark only the causally executable current open for governance."""
            if pip_size_value <= 0.0:
                return mae_pips, mfe_pips, time_to_mae, time_to_mfe
            if direction == 1:
                open_mark = bid_open
                adverse_pips = (entry - open_mark) / pip_size_value
                favorable_pips = (open_mark - entry) / pip_size_value
            else:
                open_mark = ask_open
                adverse_pips = (open_mark - entry) / pip_size_value
                favorable_pips = (entry - open_mark) / pip_size_value
            if adverse_pips > mae_pips:
                mae_pips = adverse_pips
                time_to_mae = bars_held
            if favorable_pips > mfe_pips:
                mfe_pips = favorable_pips
                time_to_mfe = bars_held
            return mae_pips, mfe_pips, time_to_mae, time_to_mfe

        def _excursion_payload() -> Dict[str, Any]:
            mae_r = mae_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
            mfe_r = mfe_pips_current / entry_to_stop_pips if entry_to_stop_pips > 0.0 else 0.0
            return {
                'trade_record_schema_version': BACKTEST_TRADE_RECORD_SCHEMA_VERSION,
                'mae_pips': round(float(mae_pips_current), 2),
                'mfe_pips': round(float(mfe_pips_current), 2),
                'mae_r': round(float(mae_r), 3),
                'mfe_r': round(float(mfe_r), 3),
                'time_to_mae_bars': int(time_to_mae_bars_current),
                'time_to_mfe_bars': int(time_to_mfe_bars_current),
                'time_underwater_bars': int(time_underwater_bars_current),
                'entry_to_stop_pips': round(float(entry_to_stop_pips), 2),
                'entry_to_target_pips': round(float(entry_to_target_pips), 2),
            }

        n_bars = len(features)
        
        for i in range(1, n_bars):
            open_price = open_arr[i]
            high_price = high_arr[i]
            low_price = low_arr[i]
            close_price = close_arr[i]
            half_spread = half_spread_arr[i] if i < len(half_spread_arr) else 0.0

            if self.config.use_spread:
                bid_open = open_price - half_spread
                ask_open = open_price + half_spread
                bid_high = high_price - half_spread
                bid_low = low_price - half_spread
                ask_high = high_price + half_spread
                ask_low = low_price + half_spread
                bid_close = close_price - half_spread
                ask_close = close_price + half_spread
            else:
                bid_open = open_price
                ask_open = open_price
                bid_high = high_price
                bid_low = low_price
                ask_high = high_price
                ask_low = low_price
                bid_close = close_price
                ask_close = close_price

            # Apply rollovers crossed during a gap only to a position that was
            # already open before the gap.  This precedes gap-open exits and
            # any replacement/new entry sizing on the current bar.
            if in_position and causal_swap_enabled:
                pre_open_units = (
                    int(pre_open_rollover_units_arr[i])
                    if (
                        pre_open_rollover_units_arr is not None
                        and i < len(pre_open_rollover_units_arr)
                    )
                    else 0
                )
                if pre_open_units > 0:
                    swap_cashflow = (
                        daily_swap_per_lot_current
                        * position_size
                        * pre_open_units
                    )
                    equity += swap_cashflow
                    accrued_swap_units_current += pre_open_units
                    accrued_swap_dollars_current += swap_cashflow
            
            # CHECK EXITS FOR OPEN POSITIONS
            if in_position:
                governance_exit_price = None
                governance_exit_reason = None
                governance_open_excursion_updated = False
                if governance_policy:
                    prev_bar = i - 1
                    if prev_bar >= entry_bar:
                        highest_since_entry = max(highest_since_entry, float(high_arr[prev_bar]))
                        lowest_since_entry = min(lowest_since_entry, float(low_arr[prev_bar]))
                        prev_close = float(close_arr[prev_bar])
                        prev_price = prev_close - half_spread if position_direction == 1 and self.config.use_spread else (
                            prev_close + half_spread if position_direction == -1 and self.config.use_spread else prev_close
                        )
                        prev_atr = float(atr_arr[prev_bar]) if atr_arr is not None and prev_bar < len(atr_arr) else float("nan")
                        regime_col = (
                            "REGIME_LIVE" if "REGIME_LIVE" in features.columns
                            else "REGIME" if "REGIME" in features.columns
                            else None
                        )
                        current_regime_label = ""
                        if regime_col is not None:
                            try:
                                current_regime_label = str(features[regime_col].iat[prev_bar])
                            except Exception:
                                current_regime_label = ""
                        current_regime_gap = 0.0
                        if "REGIME_GAP" in features.columns:
                            try:
                                current_regime_gap = float(features["REGIME_GAP"].iat[prev_bar])
                                if not math.isfinite(current_regime_gap):
                                    current_regime_gap = 0.0
                            except Exception:
                                current_regime_gap = 0.0
                        realized_vol_ratio = None
                        if atr_arr is not None and prev_bar < len(atr_arr):
                            try:
                                atr_window = atr_arr[max(entry_bar, prev_bar - 49):prev_bar + 1]
                                atr_window = atr_window[np.isfinite(atr_window)]
                                if len(atr_window) > 0:
                                    atr_mean = float(np.mean(atr_window))
                                    if atr_mean > 0 and not np.isnan(prev_atr):
                                        realized_vol_ratio = float(prev_atr) / atr_mean
                            except Exception:
                                realized_vol_ratio = None
                        context = GovernanceContext(
                            symbol="",
                            timeframe="",
                            regime=current_regime_label,
                            direction=position_direction,
                            entry_price=entry_price,
                            current_stop_loss=stop_loss,
                            current_take_profit=take_profit,
                            initial_stop_loss=initial_stop_loss,
                            initial_take_profit=initial_take_profit,
                            current_price=prev_price,
                            current_atr=0.0 if np.isnan(prev_atr) else prev_atr,
                            highest_since_entry=highest_since_entry,
                            lowest_since_entry=lowest_since_entry,
                            pip_size=float(getattr(spec, "pip_size", 0.0) or 0.0),
                            price_step=float(getattr(spec, "point", 0.0) or getattr(spec, "pip_size", 0.0) or 0.0),
                            min_stop_distance=float(getattr(spec, "stops_level", 0) or 0) * float(getattr(spec, "point", 0.0) or 0.0),
                            tp_released=governance_tp_released,
                            bars_held=max(0, prev_bar - entry_bar + 1),
                            entry_regime=entry_regime_label,
                            current_regime=current_regime_label,
                            current_regime_gap=current_regime_gap,
                            recent_high=float(np.nanmax(high_arr[max(entry_bar, prev_bar - 19):prev_bar + 1])),
                            recent_low=float(np.nanmin(low_arr[max(entry_bar, prev_bar - 19):prev_bar + 1])),
                            realized_vol_ratio=realized_vol_ratio,
                            partial_taken=governance_partial_taken,
                        )
                        decision = evaluate_policy(governance_policy, context)
                        if decision.partial_close_pct > 0 and not governance_partial_taken and position_size > 0:
                            requested_partial_fraction = max(
                                0.0,
                                min(
                                    1.0,
                                    float(decision.partial_close_pct) / 100.0,
                                ),
                            )
                            raw_partial_size = (
                                position_size * requested_partial_fraction
                            )
                            partial_size = spec.round_volume(raw_partial_size)
                            remaining_size = round(
                                position_size - partial_size,
                                8,
                            )
                            if (
                                partial_size > 0.0
                                and partial_size < position_size
                                and remaining_size + 1e-10 >= spec.min_lot
                            ):
                                # Allocate risk and already-booked carry by the
                                # broker-executable fraction, not by the raw
                                # policy percentage.
                                partial_fraction = partial_size / position_size
                                (
                                    mae_pips_current,
                                    mfe_pips_current,
                                    time_to_mae_bars_current,
                                    time_to_mfe_bars_current,
                                ) = _update_open_excursion(
                                    position_direction,
                                    entry_price,
                                    max(0, i - entry_bar),
                                    mae_pips_current,
                                    mfe_pips_current,
                                    time_to_mae_bars_current,
                                    time_to_mfe_bars_current,
                                )
                                governance_open_excursion_updated = True
                                partial_exit_price = (
                                    bid_open
                                    if position_direction == 1
                                    else ask_open
                                )
                                partial_gross_pnl = spec.calculate_tick_profit(
                                    entry_price,
                                    partial_exit_price,
                                    partial_size,
                                    position_direction,
                                )
                                if self.config.use_commission:
                                    partial_gross_pnl -= spec.commission_per_lot * partial_size
                                partial_risk = risk_amount_at_entry * partial_fraction
                                partial_swap = accrued_swap_dollars_current * partial_fraction
                                partial_net_pnl = partial_gross_pnl + partial_swap
                                equity += partial_gross_pnl
                                trades.append({
                                    'signal_bar': signal_bar,
                                    'entry_bar': entry_bar,
                                    'exit_bar': i,
                                    'signal_time': features.index[signal_bar],
                                    'entry_time': features.index[entry_bar],
                                    'exit_time': features.index[i],
                                    'direction': 'LONG' if position_direction == 1 else 'SHORT',
                                    'entry_price': entry_price,
                                    'exit_price': partial_exit_price,
                                    'stop_loss': stop_loss,
                                    'take_profit': take_profit,
                                    'position_size': partial_size,
                                    'pnl_pips': round(
                                        spec.price_to_pips(partial_exit_price - entry_price)
                                        if position_direction == 1
                                        else spec.price_to_pips(entry_price - partial_exit_price),
                                        2,
                                    ),
                                    'gross_pnl_dollars': round(partial_gross_pnl, 2),
                                    'swap_rollover_units': int(accrued_swap_units_current),
                                    'swap_dollars': round(partial_swap, 2),
                                    'pnl_dollars': round(partial_net_pnl, 2),
                                    'exit_reason': 'governance_partial',
                                    'risk_amount': round(partial_risk, 2),
                                    'r_multiple': round(partial_net_pnl / partial_risk, 3) if partial_risk > 0 else 0.0,
                                    **_excursion_payload(),
                                    'governance_policy': active_governance_name,
                                    'tp_released': governance_tp_released,
                                })
                                position_size = remaining_size
                                risk_amount_at_entry = max(0.0, risk_amount_at_entry - partial_risk)
                                accrued_swap_dollars_current -= partial_swap
                                governance_partial_taken = True
                        if decision.stop_loss is not None:
                            stop_loss = float(decision.stop_loss)
                        if decision.take_profit is not None:
                            take_profit = float(decision.take_profit)
                        governance_tp_released = governance_tp_released or bool(decision.tp_released)
                        if decision.exit_now:
                            if not governance_open_excursion_updated:
                                (
                                    mae_pips_current,
                                    mfe_pips_current,
                                    time_to_mae_bars_current,
                                    time_to_mfe_bars_current,
                                ) = _update_open_excursion(
                                    position_direction,
                                    entry_price,
                                    max(0, i - entry_bar),
                                    mae_pips_current,
                                    mfe_pips_current,
                                    time_to_mae_bars_current,
                                    time_to_mfe_bars_current,
                                )
                            governance_exit_price = (
                                bid_open
                                if position_direction == 1
                                else ask_open
                            )
                            governance_exit_reason = 'governance_exit'
                exit_price = governance_exit_price
                exit_reason = governance_exit_reason

                if exit_price is None:
                    (
                        mae_pips_current,
                        mfe_pips_current,
                        time_to_mae_bars_current,
                        time_to_mfe_bars_current,
                        time_underwater_bars_current,
                    ) = _update_excursion(
                        position_direction,
                        entry_price,
                        max(0, i - entry_bar),
                        mae_pips_current,
                        mfe_pips_current,
                        time_to_mae_bars_current,
                        time_to_mfe_bars_current,
                        time_underwater_bars_current,
                    )
                
                if exit_price is None and position_direction == 1:  # Long position
                    if bid_open <= stop_loss:
                        exit_price = bid_open
                        exit_reason = TradeStatus.CLOSED_SL.value
                    elif bid_low <= stop_loss:
                        exit_price = stop_loss - slippage_price
                        exit_reason = TradeStatus.CLOSED_SL.value
                    elif bid_high >= take_profit:
                        exit_price = take_profit
                        exit_reason = TradeStatus.CLOSED_TP.value
                        
                elif exit_price is None:  # Short position
                    if ask_open >= stop_loss:
                        exit_price = ask_open
                        exit_reason = TradeStatus.CLOSED_SL.value
                    elif ask_high >= stop_loss:
                        exit_price = stop_loss + slippage_price
                        exit_reason = TradeStatus.CLOSED_SL.value
                    elif ask_low <= take_profit:
                        exit_price = take_profit
                        exit_reason = TradeStatus.CLOSED_TP.value
                
                if exit_price is not None:
                    # Calculate P&L using tick-based math
                    gross_pnl_dollars = spec.calculate_tick_profit(
                        entry_price, exit_price, position_size, position_direction
                    )
                    
                    # Calculate pips for reporting
                    if position_direction == 1:
                        pnl_pips = spec.price_to_pips(exit_price - entry_price)
                    else:
                        pnl_pips = spec.price_to_pips(entry_price - exit_price)
                    
                    # Deduct commission
                    if self.config.use_commission:
                        gross_pnl_dollars -= spec.commission_per_lot * position_size
                    net_pnl_dollars = gross_pnl_dollars + accrued_swap_dollars_current
                    
                    # Calculate R-multiple
                    r_multiple = net_pnl_dollars / risk_amount_at_entry if risk_amount_at_entry > 0 else 0.0
                    
                    equity += gross_pnl_dollars
                    
                    # Record trade
                    trades.append({
                        'signal_bar': signal_bar,
                        'entry_bar': entry_bar,
                        'exit_bar': i,
                        'signal_time': features.index[signal_bar],
                        'entry_time': features.index[entry_bar],
                        'exit_time': features.index[i],
                        'direction': 'LONG' if position_direction == 1 else 'SHORT',
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'position_size': position_size,
                        'pnl_pips': round(pnl_pips, 2),
                        'gross_pnl_dollars': round(gross_pnl_dollars, 2),
                        'swap_rollover_units': int(accrued_swap_units_current),
                        'swap_dollars': round(accrued_swap_dollars_current, 2),
                        'pnl_dollars': round(net_pnl_dollars, 2),
                        'exit_reason': exit_reason,
                        'risk_amount': round(risk_amount_at_entry, 2),
                        'r_multiple': round(r_multiple, 3),
                        **_excursion_payload(),
                        'governance_policy': active_governance_name,
                        'tp_released': governance_tp_released,
                    })
                    
                    in_position = False
            
            # CHECK ENTRIES
            if not in_position and i >= warmup_bars:
                signal = sig_arr[i - 1]
                
                if signal != 0:
                    # Use pre-computed entry/stop prices
                    if not np.isnan(entry_prices[i]) and not np.isnan(sl_prices[i]):
                        direction = int(signal)
                        
                        entry_price_candidate = entry_prices[i]
                        stop_loss_candidate = sl_prices[i]
                        take_profit_candidate = tp_prices[i]
                        
                        # ============================================================
                        # POSITION SIZING FROM DISCOVERY RISK AND LIVE EQUITY
                        # ============================================================
                        risk_amount_at_entry = equity * (discovery_risk_per_trade_pct / 100.0)
                        
                        # Calculate loss per lot at stop
                        loss_per_lot = spec.calculate_loss_at_stop(
                            entry_price_candidate, stop_loss_candidate, 1.0, direction
                        )
                        if self.config.use_commission:
                            loss_per_lot += spec.commission_per_lot
                        
                        if loss_per_lot > 0:
                            raw_size = risk_amount_at_entry / loss_per_lot
                            position_size = spec.round_volume(raw_size)
                        else:
                            position_size = 0.0
                        
                        if position_size > 0:
                            entry_price = entry_price_candidate
                            stop_loss = stop_loss_candidate
                            take_profit = take_profit_candidate
                            initial_stop_loss = stop_loss_candidate
                            initial_take_profit = take_profit_candidate
                            highest_since_entry = float(high_price)
                            lowest_since_entry = float(low_price)
                            if pip_size_value > 0.0:
                                entry_to_stop_pips = abs(entry_price - stop_loss) / pip_size_value
                                entry_to_target_pips = abs(take_profit - entry_price) / pip_size_value
                            else:
                                entry_to_stop_pips = 0.0
                                entry_to_target_pips = 0.0
                            mae_pips_current = 0.0
                            mfe_pips_current = 0.0
                            time_to_mae_bars_current = 0
                            time_to_mfe_bars_current = 0
                            time_underwater_bars_current = 0
                            governance_tp_released = False
                            governance_partial_taken = False
                            entry_regime_label = ""
                            
                            in_position = True
                            position_direction = direction
                            entry_bar = i
                            signal_bar = i - 1
                            daily_swap_per_lot_current = (
                                float(swap_long_per_lot_arr[i])
                                if (
                                    position_direction == 1
                                    and swap_long_per_lot_arr is not None
                                    and i < len(swap_long_per_lot_arr)
                                )
                                else float(swap_short_per_lot_arr[i])
                                if (
                                    position_direction == -1
                                    and swap_short_per_lot_arr is not None
                                    and i < len(swap_short_per_lot_arr)
                                )
                                else 0.0
                            )
                            accrued_swap_units_current = 0
                            accrued_swap_dollars_current = 0.0
                            regime_col = (
                                "REGIME_LIVE" if "REGIME_LIVE" in features.columns
                                else "REGIME" if "REGIME" in features.columns
                                else None
                            )
                            if regime_col is not None:
                                try:
                                    entry_regime_label = str(features[regime_col].iat[signal_bar])
                                except Exception:
                                    entry_regime_label = ""

                            (
                                mae_pips_current,
                                mfe_pips_current,
                                time_to_mae_bars_current,
                                time_to_mfe_bars_current,
                                time_underwater_bars_current,
                            ) = _update_excursion(
                                position_direction,
                                entry_price,
                                0,
                                mae_pips_current,
                                mfe_pips_current,
                                time_to_mae_bars_current,
                                time_to_mfe_bars_current,
                                time_underwater_bars_current,
                            )

                            # Same-bar exit: check gap-through at open FIRST, then intra-bar
                            exit_price = None
                            exit_reason = None
                            if position_direction == 1:
                                if bid_open <= stop_loss:
                                    exit_price = bid_open
                                    exit_reason = TradeStatus.CLOSED_SL.value
                                elif bid_low <= stop_loss:
                                    exit_price = stop_loss - slippage_price
                                    exit_reason = TradeStatus.CLOSED_SL.value
                                elif bid_high >= take_profit:
                                    exit_price = take_profit
                                    exit_reason = TradeStatus.CLOSED_TP.value
                            else:
                                if ask_open >= stop_loss:
                                    exit_price = ask_open
                                    exit_reason = TradeStatus.CLOSED_SL.value
                                elif ask_high >= stop_loss:
                                    exit_price = stop_loss + slippage_price
                                    exit_reason = TradeStatus.CLOSED_SL.value
                                elif ask_low <= take_profit:
                                    exit_price = take_profit
                                    exit_reason = TradeStatus.CLOSED_TP.value

                            if exit_price is not None:
                                gross_pnl_dollars = spec.calculate_tick_profit(
                                    entry_price, exit_price, position_size, position_direction
                                )
                                if position_direction == 1:
                                    pnl_pips = spec.price_to_pips(exit_price - entry_price)
                                else:
                                    pnl_pips = spec.price_to_pips(entry_price - exit_price)
                                if self.config.use_commission:
                                    gross_pnl_dollars -= spec.commission_per_lot * position_size
                                net_pnl_dollars = gross_pnl_dollars + accrued_swap_dollars_current
                                r_multiple = net_pnl_dollars / risk_amount_at_entry if risk_amount_at_entry > 0 else 0.0
                                equity += gross_pnl_dollars
                                trades.append({
                                    'signal_bar': signal_bar,
                                    'entry_bar': entry_bar,
                                    'exit_bar': i,
                                    'signal_time': features.index[signal_bar],
                                    'entry_time': features.index[entry_bar],
                                    'exit_time': features.index[i],
                                    'direction': 'LONG' if position_direction == 1 else 'SHORT',
                                    'entry_price': entry_price,
                                    'exit_price': exit_price,
                                    'stop_loss': stop_loss,
                                    'take_profit': take_profit,
                                    'position_size': position_size,
                                    'pnl_pips': round(pnl_pips, 2),
                                    'gross_pnl_dollars': round(gross_pnl_dollars, 2),
                                    'swap_rollover_units': int(accrued_swap_units_current),
                                    'swap_dollars': round(accrued_swap_dollars_current, 2),
                                    'pnl_dollars': round(net_pnl_dollars, 2),
                                    'exit_reason': exit_reason,
                                    'risk_amount': round(risk_amount_at_entry, 2),
                                    'r_multiple': round(r_multiple, 3),
                                    **_excursion_payload(),
                                    'governance_policy': active_governance_name,
                                    'tp_released': governance_tp_released,
                                })
                                in_position = False
            
            if in_position and causal_swap_enabled:
                rollover_units = (
                    int(rollover_units_arr[i])
                    if rollover_units_arr is not None and i < len(rollover_units_arr)
                    else 0
                )
                if rollover_units > 0:
                    swap_cashflow = (
                        daily_swap_per_lot_current
                        * position_size
                        * rollover_units
                    )
                    equity += swap_cashflow
                    accrued_swap_units_current += rollover_units
                    accrued_swap_dollars_current += swap_cashflow

            current_equity = equity
            if in_position:
                mark_exit_price = bid_close if position_direction == 1 else ask_close
                unrealized = spec.calculate_tick_profit(
                    entry_price, mark_exit_price, position_size, position_direction
                )
                if self.config.use_commission:
                    unrealized -= spec.commission_per_lot * position_size
                current_equity = equity + unrealized

            equity_curve.append(current_equity)
            peak_equity = max(peak_equity, current_equity)
            dd = (peak_equity - current_equity) / peak_equity * 100 if peak_equity > 0 else 0
            max_drawdown = max(max_drawdown, dd)
        
        # CLOSE ANY REMAINING POSITION AT END
        if in_position:
            is_long = position_direction == 1
            half_spread = half_spread_arr[-1] if len(half_spread_arr) else 0.0
            if self.config.use_spread:
                exit_price = close_arr[-1] - half_spread if is_long else close_arr[-1] + half_spread
            else:
                exit_price = close_arr[-1]
            
            gross_pnl_dollars = spec.calculate_tick_profit(
                entry_price, exit_price, position_size, position_direction
            )
            
            if position_direction == 1:
                pnl_pips = spec.price_to_pips(exit_price - entry_price)
            else:
                pnl_pips = spec.price_to_pips(entry_price - exit_price)
            
            if self.config.use_commission:
                gross_pnl_dollars -= spec.commission_per_lot * position_size
            net_pnl_dollars = gross_pnl_dollars + accrued_swap_dollars_current
            
            r_multiple = net_pnl_dollars / risk_amount_at_entry if risk_amount_at_entry > 0 else 0.0
            
            equity += gross_pnl_dollars
            
            trades.append({
                'signal_bar': signal_bar,
                'entry_bar': entry_bar,
                'exit_bar': n_bars - 1,
                'signal_time': features.index[signal_bar],
                'entry_time': features.index[entry_bar],
                'exit_time': features.index[-1],
                'direction': 'LONG' if position_direction == 1 else 'SHORT',
                'entry_price': entry_price,
                'exit_price': exit_price,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'position_size': position_size,
                'pnl_pips': round(pnl_pips, 2),
                'gross_pnl_dollars': round(gross_pnl_dollars, 2),
                'swap_rollover_units': int(accrued_swap_units_current),
                'swap_dollars': round(accrued_swap_dollars_current, 2),
                'pnl_dollars': round(net_pnl_dollars, 2),
                'exit_reason': TradeStatus.CLOSED_EOD.value,
                'risk_amount': round(risk_amount_at_entry, 2),
                'r_multiple': round(r_multiple, 3),
                **_excursion_payload(),
                'governance_policy': active_governance_name,
                'tp_released': governance_tp_released,
            })
            
            equity_curve[-1] = equity
            peak_equity = max(peak_equity, equity)
            dd = (peak_equity - equity) / peak_equity * 100 if peak_equity > 0 else 0
            max_drawdown = max(max_drawdown, dd)
        
        return trades, equity, max_drawdown, equity_curve
    
    # Bars-per-year lookup for known timeframes (252 trading days assumed)
    _TF_BARS_PER_YEAR = {
        'M1': 252 * 24 * 60, 'M5': 252 * 24 * 12, 'M15': 252 * 24 * 4,
        'M30': 252 * 24 * 2, 'H1': 252 * 24, 'H4': 252 * 6,
        'D1': 252, 'W1': 52, 'MN1': 12,
    }

    def _calculate_metrics(self,
                           trades: List[Dict],
                           final_equity: float,
                           equity_curve: List[float],
                           max_dd: float,
                           features: pd.DataFrame = None,
                           timeframe: str = "",
                           warmup_bars: int = 0) -> Dict[str, Any]:
        """
        Calculate complete performance metrics.

        Sharpe/Sortino ratios are now computed from equity curve returns (per bar),
        annualized based on timeframe. This provides more accurate risk-adjusted
        returns compared to per-trade calculations.

        Args:
            trades: List of trade dictionaries
            final_equity: Final account equity
            equity_curve: List of equity values
            max_dd: Maximum drawdown percentage
            features: Optional features DataFrame (for timeframe detection)
            timeframe: Timeframe string for annualization (e.g. 'H1', 'M15')
            warmup_bars: Exclude trades whose entry_bar < warmup_bars

        Returns:
            Dict with all performance metrics including R-multiple stats
        """
        if not trades:
            return self._empty_result()

        # Basic counts
        total_trades = len(trades)
        wins = [t for t in trades if t['pnl_dollars'] > 0]
        losses = [t for t in trades if t['pnl_dollars'] < 0]
        breakeven = [t for t in trades if t['pnl_dollars'] == 0]
        
        # P&L calculations
        total_pnl = sum(t['pnl_dollars'] for t in trades)
        win_pnl = sum(t['pnl_dollars'] for t in wins) if wins else 0
        loss_pnl = abs(sum(t['pnl_dollars'] for t in losses)) if losses else 0
        
        # Win rate
        win_rate = len(wins) / total_trades * 100 if total_trades > 0 else 0
        
        # Profit factor
        profit_factor = profit_factor_from_gross(win_pnl, loss_pnl)
        
        # Average trade metrics
        avg_win_pips = sum(t['pnl_pips'] for t in wins) / len(wins) if wins else 0
        avg_loss_pips = abs(sum(t['pnl_pips'] for t in losses) / len(losses)) if losses else 0
        avg_trade_pips = sum(t['pnl_pips'] for t in trades) / total_trades
        
        avg_win_dollars = win_pnl / len(wins) if wins else 0
        avg_loss_dollars = loss_pnl / len(losses) if losses else 0
        avg_trade_dollars = total_pnl / total_trades
        
        # Expectancy
        if win_rate > 0:
            expectancy = (win_rate / 100 * avg_win_pips) - ((1 - win_rate / 100) * avg_loss_pips)
        else:
            expectancy = -avg_loss_pips
        
        # ================================================================
        # Sharpe and Sortino ratios from EQUITY CURVE returns (per bar)
        # Annualized based on timeframe for proper comparison
        # ================================================================
        sharpe = 0.0
        sortino = 0.0
        bars_per_year = self._TF_BARS_PER_YEAR.get(timeframe, 0)
        risk_return_periods = 0
        std_weekly_return = 0.0  # findings.html §C4: variance/stability penalty input

        # Need at least 3 equity points to get 2 returns for meaningful std calculation
        if len(equity_curve) > 2:
            # Convert equity curve to returns
            equity_arr = np.array(equity_curve)
            if warmup_bars > 0 and len(equity_arr) > 2:
                risk_start = max(0, min(int(warmup_bars) - 1, len(equity_arr) - 2))
                equity_arr = equity_arr[risk_start:]
            # Per-bar returns (percentage)
            bar_returns = np.diff(equity_arr) / np.where(equity_arr[:-1] != 0, equity_arr[:-1], 1.0)
            risk_return_periods = int(len(bar_returns))
            
            # Determine annualization factor based on timeframe
            # Priority: explicit timeframe string → data inference → H1 default
            bars_per_year = self._TF_BARS_PER_YEAR.get(timeframe, 0)

            if bars_per_year <= 0 and features is not None and len(features) > 1:
                try:
                    time_diff = (features.index[1] - features.index[0]).total_seconds()
                    if time_diff > 0:
                        bars_per_day = 86400 / time_diff
                        bars_per_year = bars_per_day * 252
                except Exception as exc:
                    logger.debug("Could not infer bars_per_year from feature index; using fallback: %s", exc)

            if bars_per_year <= 0:
                bars_per_year = 252 * 24  # H1 default
            
            # Sharpe ratio (annualized from per-bar returns)
            # Need at least 2 returns for ddof=1 std calculation
            if len(bar_returns) >= 2:
                returns_mean = np.mean(bar_returns)
                returns_std = np.std(bar_returns, ddof=1) if len(bar_returns) > 1 else 0.0
                if returns_std > 1e-8:
                    sharpe = (returns_mean / returns_std) * np.sqrt(bars_per_year)
            
            # Sortino ratio (annualized, using downside deviation)
            # No artificial cap - let the metric speak for itself
            if len(bar_returns) >= 2:
                negative_returns = bar_returns[bar_returns < 0]
                # Need at least 2 negative returns for ddof=1 std calculation
                if len(negative_returns) >= 2:
                    downside_std = np.std(negative_returns, ddof=1)
                    if downside_std > 1e-8:
                        sortino = (np.mean(bar_returns) / downside_std) * np.sqrt(bars_per_year)
                elif len(negative_returns) == 1:
                    # Single negative return - use its absolute value as downside deviation proxy
                    downside_std = abs(negative_returns[0])
                    if downside_std > 1e-8:
                        sortino = (np.mean(bar_returns) / downside_std) * np.sqrt(bars_per_year)
                else:
                    if np.mean(bar_returns) > 0:
                        sortino = float('inf')

            # Weekly-return std for stability penalty (findings.html §C4).
            # Sample equity at ~weekly intervals; need ≥3 weekly samples for ddof=1 std.
            if len(bar_returns) >= 2:
                sharpe, sortino = sharpe_sortino_from_returns(bar_returns, bars_per_year)

            bars_per_week = max(1, int(round(bars_per_year / 52.0)))
            if len(equity_arr) >= 3 * bars_per_week:
                weekly_idxs = np.arange(0, len(equity_arr), bars_per_week)
                weekly_equity = equity_arr[weekly_idxs]
                if len(weekly_equity) >= 3:
                    weekly_returns = np.diff(weekly_equity) / np.where(
                        weekly_equity[:-1] != 0, weekly_equity[:-1], 1.0
                    )
                    if len(weekly_returns) >= 2:
                        std_weekly_return = float(np.std(weekly_returns, ddof=1))

        # ================================================================
        # R-Multiple Statistics (risk-normalized performance)
        # ================================================================
        r_multiples = [t.get('r_multiple', 0.0) for t in trades if 'r_multiple' in t]
        
        if r_multiples:
            mean_r = float(np.mean(r_multiples))
            median_r = float(np.median(r_multiples))
            r_count = int(len(r_multiples))
            pct_positive_r = len([r for r in r_multiples if r > 0]) / len(r_multiples) * 100
            worst_5pct_r = float(np.percentile(r_multiples, 5)) if len(r_multiples) >= 20 else min(r_multiples)
            r_arr = np.asarray(r_multiples, dtype=np.float64)
            std_r = float(np.std(r_arr, ddof=1)) if r_count >= 2 else 0.0
            se_r = float(std_r / math.sqrt(r_count)) if r_count >= 2 and std_r > 1e-12 else 0.0
            edge_t_stat_r = float(mean_r / se_r) if se_r > 1e-12 else 0.0
            downside_r = r_arr[r_arr < 0.0]
            if len(downside_r) >= 2:
                r_downside_deviation = float(np.std(downside_r, ddof=1))
            elif len(downside_r) == 1:
                r_downside_deviation = float(abs(downside_r[0]))
            else:
                r_downside_deviation = 0.0
            block_count = min(16, max(1, len(r_arr)))
            block_sums = np.asarray(
                [float(np.sum(block)) for block in np.array_split(r_arr, block_count) if len(block)],
                dtype=np.float64,
            )
            negative_blocks = block_sums[block_sums < 0.0]
            if len(negative_blocks) >= 2:
                r_block_downside_deviation = float(np.std(negative_blocks, ddof=1))
            elif len(negative_blocks) == 1:
                r_block_downside_deviation = float(abs(negative_blocks[0]))
            else:
                r_block_downside_deviation = 0.0
            if len(block_sums) > 0:
                tail_n = max(1, int(math.ceil(len(block_sums) * 0.20)))
                r_block_cvar = float(abs(np.mean(np.sort(block_sums)[:tail_n])))
            else:
                r_block_cvar = 0.0
        else:
            mean_r = median_r = pct_positive_r = worst_5pct_r = 0.0
            r_count = 0
            std_r = se_r = edge_t_stat_r = 0.0
            r_downside_deviation = r_block_downside_deviation = r_block_cvar = 0.0
        
        drawdown_metrics = return_drawdown_metrics(
            initial_equity=self.config.initial_capital,
            final_equity=final_equity,
            max_drawdown_pct=max_dd,
            periods=risk_return_periods,
            periods_per_year=bars_per_year,
        )
        total_return_pct = drawdown_metrics["total_return_pct"]
        
        # Consecutive wins/losses
        max_consec_wins = self._max_consecutive(trades, win=True)
        max_consec_losses = self._max_consecutive(trades, win=False)
        
        # Trade duration statistics
        if trades:
            durations = [t['exit_bar'] - t['entry_bar'] for t in trades]
            avg_duration = np.mean(durations)
            max_duration = max(durations)
            min_duration = min(durations)
        else:
            avg_duration = max_duration = min_duration = 0
        
        # Exit reason breakdown
        exit_reasons = {}
        for t in trades:
            reason = t['exit_reason']
            exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
        
        # Long/Short breakdown
        long_trades = [t for t in trades if t['direction'] == 'LONG']
        short_trades = [t for t in trades if t['direction'] == 'SHORT']
        
        long_win_rate = len([t for t in long_trades if t['pnl_dollars'] > 0]) / len(long_trades) * 100 if long_trades else 0
        short_win_rate = len([t for t in short_trades if t['pnl_dollars'] > 0]) / len(short_trades) * 100 if short_trades else 0
        
        return {
            # Core metrics
            'total_trades': total_trades,
            'winning_trades': len(wins),
            'losing_trades': len(losses),
            'breakeven_trades': len(breakeven),
            'win_rate': round(win_rate, 2),
            
            # P&L metrics
            'total_pnl': round(total_pnl, 2),
            'total_return_pct': round(total_return_pct, 2),
            'gross_profit': round(win_pnl, 2),
            'gross_loss': round(loss_pnl, 2),
            'profit_factor': round_metric(profit_factor, 2),
            
            # Average trade metrics
            'avg_win_pips': round(avg_win_pips, 2),
            'avg_loss_pips': round(avg_loss_pips, 2),
            'avg_trade_pips': round(avg_trade_pips, 2),
            'avg_win_dollars': round(avg_win_dollars, 2),
            'avg_loss_dollars': round(avg_loss_dollars, 2),
            'avg_trade_dollars': round(avg_trade_dollars, 2),
            
            # Risk metrics
            'max_drawdown_pct': round(max_dd, 2),
            'expectancy_pips': round(expectancy, 2),
            'sharpe_ratio': round(sharpe, 2),
            'sortino_ratio': round_metric(sortino, 2),
            'calmar_ratio': round_metric(drawdown_metrics["calmar_ratio"], 2),
            'return_drawdown_ratio': round_metric(drawdown_metrics["return_drawdown_ratio"], 2),
            'calmar_ratio_annualized': round_metric(drawdown_metrics["calmar_ratio_annualized"], 2),
            'annualized_return_pct': round_metric(drawdown_metrics["annualized_return_pct"], 2),
            'calmar_ratio_basis': drawdown_metrics["calmar_ratio_basis"],
            'metric_contract_version': METRIC_CONTRACT_VERSION,
            'std_weekly_return': round(std_weekly_return, 6),
            
            # R-Multiple metrics (risk-normalized)
            'r_count': r_count,
            'mean_r': round(mean_r, 3),
            'median_r': round(median_r, 3),
            'std_r': round(std_r, 6),
            'se_r': round(se_r, 6),
            'edge_t_stat_r': round(edge_t_stat_r, 6),
            'pct_positive_r': round(pct_positive_r, 2),
            'worst_5pct_r': round(worst_5pct_r, 3),
            'r_downside_deviation': round(r_downside_deviation, 6),
            'r_block_downside_deviation': round(r_block_downside_deviation, 6),
            'r_block_cvar': round(r_block_cvar, 6),
            
            # Streak metrics
            'max_consecutive_wins': max_consec_wins,
            'max_consecutive_losses': max_consec_losses,
            
            # Duration metrics
            'avg_trade_duration': round(avg_duration, 1),
            'max_trade_duration': max_duration,
            'min_trade_duration': min_duration,
            
            # Breakdown metrics
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'long_win_rate': round(long_win_rate, 2),
            'short_win_rate': round(short_win_rate, 2),
            'exit_reasons': exit_reasons,
            
            # Raw data
            'final_equity': round(final_equity, 2),
            'equity_curve': equity_curve,
            'trades': trades,
        }
    
    def _max_consecutive(self, trades: List[Dict], win: bool = True) -> int:
        """Calculate maximum consecutive wins or losses."""
        max_streak = 0
        current_streak = 0
        
        for trade in trades:
            is_win = trade['pnl_dollars'] > 0
            if is_win == win:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0
        
        return max_streak
    
    @staticmethod
    def validate_execution_timing(results: Dict[str, Any], max_trades_to_check: int = 5) -> Tuple[bool, List[str]]:
        """
        Validate that backtest execution timing matches the live trading contract.
        
        The contract states:
        - Signal from bar i-1 triggers entry on bar i
        - entry_bar_index = signal_bar_index + 1
        - entry_time > signal_time
        
        Args:
            results: Backtest results dictionary containing 'trades'
            max_trades_to_check: Number of trades to validate (default: first 5)
            
        Returns:
            Tuple of (is_valid, list of validation messages)
        """
        trades = results.get('trades', [])
        
        if not trades:
            return True, ["No trades to validate"]
        
        messages = []
        all_valid = True
        
        num_to_check = min(len(trades), max_trades_to_check)
        messages.append(f"Validating execution timing for first {num_to_check} trade(s):")
        messages.append("-" * 60)
        
        for idx, trade in enumerate(trades[:num_to_check]):
            signal_bar = trade.get('signal_bar')
            entry_bar = trade.get('entry_bar')
            signal_time = trade.get('signal_time')
            entry_time = trade.get('entry_time')
            
            # Check if required fields exist
            if signal_bar is None or entry_bar is None:
                messages.append(f"  Trade {idx + 1}: MISSING signal_bar or entry_bar fields")
                all_valid = False
                continue
            
            # Validate: entry_bar = signal_bar + 1 (exactly 1 bar delay)
            bar_delay_valid = (entry_bar == signal_bar + 1)
            
            # Validate: entry_time > signal_time
            time_order_valid = True
            if signal_time is not None and entry_time is not None:
                time_order_valid = (entry_time > signal_time)
            
            trade_valid = bar_delay_valid and time_order_valid
            status = "✓ PASS" if trade_valid else "✗ FAIL"
            
            messages.append(f"  Trade {idx + 1}: {status}")
            messages.append(f"    signal_bar={signal_bar}, entry_bar={entry_bar} "
                          f"(expected entry_bar={signal_bar + 1})")
            if signal_time is not None and entry_time is not None:
                messages.append(f"    signal_time={signal_time}")
                messages.append(f"    entry_time ={entry_time}")
                messages.append(f"    entry_time > signal_time: {time_order_valid}")
            
            if not trade_valid:
                all_valid = False
        
        messages.append("-" * 60)
        overall_status = "PASS" if all_valid else "FAIL"
        messages.append(f"Execution timing validation: {overall_status}")
        
        return all_valid, messages
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result when no trades."""
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'breakeven_trades': 0,
            'win_rate': 0,
            'total_pnl': 0,
            'total_return_pct': 0,
            'gross_profit': 0,
            'gross_loss': 0,
            'profit_factor': 0,
            'avg_win_pips': 0,
            'avg_loss_pips': 0,
            'avg_trade_pips': 0,
            'avg_win_dollars': 0,
            'avg_loss_dollars': 0,
            'avg_trade_dollars': 0,
            'max_drawdown_pct': 0,
            'expectancy_pips': 0,
            'sharpe_ratio': 0,
            'sortino_ratio': 0,
            'calmar_ratio': 0,
            'return_drawdown_ratio': 0,
            'calmar_ratio_annualized': 0,
            'annualized_return_pct': 0,
            'calmar_ratio_basis': "cumulative_return_pct_over_max_drawdown_pct",
            'metric_contract_version': METRIC_CONTRACT_VERSION,
            'std_weekly_return': 0.0,
            'r_count': 0,
            'mean_r': 0,
            'median_r': 0,
            'std_r': 0,
            'se_r': 0,
            'edge_t_stat_r': 0,
            'pct_positive_r': 0,
            'worst_5pct_r': 0,
            'r_downside_deviation': 0,
            'r_block_downside_deviation': 0,
            'r_block_cvar': 0,
            'max_consecutive_wins': 0,
            'max_consecutive_losses': 0,
            'avg_trade_duration': 0,
            'max_trade_duration': 0,
            'min_trade_duration': 0,
            'long_trades': 0,
            'short_trades': 0,
            'long_win_rate': 0,
            'short_win_rate': 0,
            'exit_reasons': {},
            'final_equity': self.config.initial_capital,
            'equity_curve': [self.config.initial_capital],
            'trades': [],
        }


# =============================================================================
# SCORING AND EVALUATION
# =============================================================================

class StrategyScorer:
    """
    Scores and ranks strategy performance using the FX backtester-aligned scoring.

    `score()` is the public entry point; `purpose="selection"` is used during
    rolling-window strategy/timeframe selection, `purpose="opt"` during
    hyperparameter tuning. `fx_generalization_score()` blends train and val
    metrics for the validation-aware selection pass.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config

    def calculate_fx_selection_score(self, metrics: Dict[str, Any]) -> float:
        """Backtester-aligned strategy selection score (training data only).

        Mirrors fx_pipeline_main._calculate_score() with calibration extensions:
        - Continuous DD penalty (replaces discrete buckets)
        - Sortino/Sharpe blend for downside-aware risk scoring
        - Tail-risk penalty via worst 5th-percentile R-multiple
        - Consistency penalty via max consecutive losses
        - Trade-frequency bonus (log-scaled statistical confidence)
        All extensions gated by PipelineConfig feature flags.
        """
        sharpe = min(max(metrics.get('sharpe_ratio', 0.0), -2.0), 5.0)
        pf = min(max(metrics.get('profit_factor', 0.0), 0.0), 10.0)
        win_rate = float(metrics.get('win_rate', 0.0)) / 100.0
        ret = float(metrics.get('total_return_pct', 0.0))
        dd = float(metrics.get('max_drawdown_pct', 100.0))

        return_dd = (ret / dd) if dd > 0 else ret
        return_dd = min(max(return_dd, -5.0), 20.0)

        expectancy = float(metrics.get('expectancy_pips', 0.0))

        # --- Sortino/Sharpe blend ---
        risk_adj = sharpe
        if getattr(self.config, 'scoring_use_sortino_blend', False):
            sortino = min(max(metrics.get('sortino_ratio', sharpe), -2.0), 5.0)
            risk_adj = 0.6 * sharpe + 0.4 * sortino

        score = (
            ((risk_adj + 2.0) / 7.0) * 25.0 +
            (pf / 10.0) * 20.0 +
            (win_rate * 15.0) +
            ((return_dd + 5.0) / 25.0) * 25.0 +
            min(expectancy / 10.0, 1.0) * 15.0
        )

        # --- Drawdown penalty ---
        if getattr(self.config, 'scoring_use_continuous_dd', False):
            # findings.html §4: tightened from -0.03·dd to -0.05·dd so the
            # selector pays a real cost for high-DD candidates.
            # dd=0 → 1.0, dd=15 → 0.47, dd=25 → 0.29, dd=35 → 0.17
            dd_mult = math.exp(-0.05 * dd)
            score *= dd_mult
        else:
            # Legacy discrete buckets
            if dd > 30.0:
                score *= 0.6
            elif dd > 25.0:
                score *= 0.75
            elif dd > 20.0:
                score *= 0.9

        # --- Tail-risk penalty ---
        if getattr(self.config, 'scoring_use_tail_risk', False):
            worst_5 = float(metrics.get('worst_5pct_r', 0.0))
            if worst_5 < -3.0:
                # Severe tail: penalize proportionally, clamped
                tail_penalty = max(0.70, 1.0 + 0.05 * worst_5)  # worst_5=-5 → 0.75
                score *= tail_penalty

        # --- Consistency penalty ---
        if getattr(self.config, 'scoring_use_consistency', False):
            max_consec = int(metrics.get('max_consecutive_losses', 0))
            if max_consec > 8:
                # Penalize fragile strategies with very long losing streaks
                consec_penalty = max(0.75, 1.0 - 0.03 * (max_consec - 8))  # 12 → 0.88, 16+ → 0.75
                score *= consec_penalty

        # --- Trade-frequency bonus ---
        if getattr(self.config, 'scoring_use_trade_frequency_bonus', False):
            tc = int(metrics.get('total_trades', 0))
            if tc > 30:
                # Log-scaled bonus: 30 trades → 0%, 100 → ~3.5%, 300 → ~6.5%
                freq_bonus = min(0.08, 0.03 * math.log(tc / 30.0))
                score *= (1.0 + freq_bonus)

        return float(score)

    def calculate_fx_opt_score(self, metrics: Dict[str, Any]) -> float:
        """Backtester-aligned optimization score (train only).

        Mirrors fx_pipeline_main._calc_score() with calibration extensions:
        - Continuous DD penalty (replaces 3-tier discrete)
        - Profit factor and win rate components
        - Sortino blend for downside awareness
        All extensions gated by PipelineConfig feature flags.
        """
        sharpe = max(-2.0, min(5.0, float(metrics.get('sharpe_ratio', 0.0))))
        ret = float(metrics.get('total_return_pct', 0.0))
        dd = float(metrics.get('max_drawdown_pct', 100.0))
        pf = min(max(float(metrics.get('profit_factor', 0.0)), 0.0), 10.0)
        wr = float(metrics.get('win_rate', 0.0)) / 100.0

        # --- Sortino blend ---
        risk_adj = sharpe
        if getattr(self.config, 'scoring_use_sortino_blend', False):
            sortino = max(-2.0, min(5.0, float(metrics.get('sortino_ratio', sharpe))))
            risk_adj = 0.6 * sharpe + 0.4 * sortino

        # Base score: risk-adjusted return + return magnitude + PF + win rate
        base = (
            risk_adj * 30.0 +
            min(ret, 500.0) * 0.1 +
            min(pf, 5.0) * 2.0 +
            wr * 5.0
        )

        # --- DD penalty ---
        # Matches the selection scorer's slope (findings.html §4) so TPE doesn't
        # surface candidates that the downstream selector will then discount.
        if getattr(self.config, 'scoring_use_continuous_dd', False):
            dd_penalty = math.exp(-0.05 * dd)
        else:
            dd_penalty = 1.0 if dd < 15.0 else 0.8 if dd < 25.0 else 0.5

        return float(base * dd_penalty)
    def calculate_fx_score_robustness_ratio(self,
                                            train_metrics: Dict[str, Any],
                                            val_metrics: Dict[str, Any],
                                            purpose: str = "selection",
                                            eps: float = 1e-9) -> float:
        """Robustness ratio based on scorer outputs (val_score/train_score)."""
        train_score = float(self.score(train_metrics, purpose=purpose))
        val_score = float(self.score(val_metrics, purpose=purpose))
        denom = train_score if abs(train_score) > eps else eps
        ratio = val_score / denom
        return float(np.clip(ratio, 0.0, 2.0))

    def fx_generalization_score(self,
                                train_metrics: Dict[str, Any],
                                val_metrics: Dict[str, Any],
                                purpose: str = "selection") -> Tuple[float, float, float, float]:
        """Compute validation-aware score for fx_backtester mode.

        Returns:
            (final_score, train_score, val_score, robustness_ratio)
        """
        train_score = float(self.score(train_metrics, purpose=purpose))
        val_score = float(self.score(val_metrics, purpose=purpose))

        gap = max(0.0, train_score - val_score)
        lam = float(getattr(self.config, "fx_gap_penalty_lambda", 0.50))
        final_score = val_score - lam * gap

        rr = self.calculate_fx_score_robustness_ratio(train_metrics, val_metrics, purpose="selection")
        boost_w = float(getattr(self.config, "fx_robustness_boost", 0.15))
        rr_clipped = float(np.clip(rr, 0.0, 1.25))
        robust_mult = (1.0 - boost_w) + boost_w * rr_clipped
        final_score *= robust_mult

        # Variance / stability penalty (findings.html §C4): exp(-k * std_weekly_return) on val side.
        # std_weekly_return is a fraction (e.g. 0.05 = 5%); k=2.0 default → 5% std → ~0.90 mult.
        k_var = float(getattr(self.config, "fx_stability_penalty_k", 2.0))
        if k_var > 0.0:
            std_wkly = float(val_metrics.get("std_weekly_return", 0.0) or 0.0)
            if std_wkly > 0.0:
                stability_mult = math.exp(-k_var * std_wkly)
                final_score *= stability_mult

        return float(final_score), float(train_score), float(val_score), float(rr)

    def score(self, metrics: Dict[str, Any], purpose: str = "selection") -> float:
        """FX backtester-aligned score.

        purpose:
          - "selection": strategy selection / rolling window scoring
          - "opt": hyperparameter tuning comparisons
        """
        if purpose == "opt":
            return self.calculate_fx_opt_score(metrics)
        return self.calculate_fx_selection_score(metrics)


# =============================================================================
# MAIN EXPORTS
# =============================================================================

__all__ = [
    'PipelineConfig',
    'SignalType',
    'StrategyCategory',
    'TradeStatus',
    'Timer',
    'InstrumentSpec',
    'INSTRUMENT_SPECS',
    'get_instrument_spec',
    'sync_instrument_spec_from_mt5',
    'load_broker_specs',
    'set_broker_specs_path',
    'set_instrument_specs',
    'save_broker_specs',
    'DataLoader',
    'FeatureComputer',
    'DataSplitter',
    'Backtester',
    'StrategyScorer',
]


if __name__ == "__main__":
    # Basic tests
    logging.basicConfig(level=logging.INFO)
    
    config = PipelineConfig()
    print(f"Config: {config}")
    
    # Test instrument spec
    spec = get_instrument_spec('EURUSD')
    print(f"\nEURUSD spec: pip_size={spec.pip_size}, pip_value={spec.pip_value}")
    print(f"  10 pips in price: {spec.pips_to_price(10)}")
    print(f"  0.001 price in pips: {spec.price_to_pips(0.001)}")
    
    # Test data splitter
    splitter = DataSplitter(config)
    indices = splitter.get_workflow_indices(1000)
    print(f"\nWorkflow indices for 1000 rows: {indices}")
    
    print("\npm_core.py loaded successfully!")
