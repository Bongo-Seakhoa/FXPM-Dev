"""
FX Portfolio Manager - MT5 Integration Module
===============================================

Handles all MetaTrader 5 integration:
- Connection management
- Symbol info retrieval
- Historical data fetching
- Live data updates
- Order execution
- Position management

This module bridges the Portfolio Manager with MT5 for live trading.

Version: 3.1 (Portfolio Manager)
"""

import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import time
import math

import pandas as pd
import numpy as np

# MT5 import with fallback
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    mt5 = None

from pm_core import get_instrument_spec, InstrumentSpec

# Configure module logger
logger = logging.getLogger(__name__)


# =============================================================================
# ENUMERATIONS
# =============================================================================

class OrderType(Enum):
    """Order types."""
    BUY = 0
    SELL = 1
    BUY_LIMIT = 2
    SELL_LIMIT = 3
    BUY_STOP = 4
    SELL_STOP = 5


class FillingType(Enum):
    """Order filling types."""
    FOK = 0  # Fill or Kill
    IOC = 1  # Immediate or Cancel
    RETURN = 2  # Return (partial fill allowed)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MT5Config:
    """MT5 connection configuration."""
    login: int = 0
    password: str = ""
    server: str = ""
    path: str = ""
    timeout: int = 60000
    portable: bool = False
    preferred_filling_type: str = "broker"


@dataclass
class MT5SymbolInfo:
    """Symbol information from MT5."""
    symbol: str
    digits: int
    point: float
    trade_tick_value: float
    trade_tick_size: float
    trade_contract_size: float
    volume_min: float
    volume_max: float
    volume_step: float
    spread: int
    spread_float: bool
    swap_long: float
    swap_short: float
    trade_stops_level: int
    visible: bool = True
    trade_mode: int = 2
    filling_mode: int = 0
    trade_exemode: int = 0
    trade_freeze_level: int = 0
    
    # Calculated values
    pip_size: float = 0.0
    pip_value: float = 0.0
    
    def __post_init__(self):
        """Calculate pip values (robust to MT5 edge cases)."""
        # Some brokers/CFDs can report point=0 for certain symbols.
        # We must never allow point/pip_size to be zero because it breaks spread
        # conversion and downstream risk sizing.
        if not self.point or self.point <= 0.0:
            try:
                self.point = float(10 ** (-self.digits)) if self.digits > 0 else 0.0001
            except Exception:
                self.point = 0.0001

        # Pip size based on digits (MT5 convention)
        if self.digits in (5, 3):
            self.pip_size = self.point * 10.0
        else:
            self.pip_size = self.point

        # Ultimate fallback if point was still unusable
        if not self.pip_size or self.pip_size <= 0.0:
            pip_position = (self.digits - 1) if self.digits in (5, 3) else self.digits
            self.pip_size = float(10 ** (-pip_position)) if pip_position >= 0 else 0.0001

        # Pip value calculation (tick-math parity when available)
        if self.trade_tick_size > 0.0 and self.trade_tick_value > 0.0:
            self.pip_value = (self.pip_size / self.trade_tick_size) * self.trade_tick_value
        else:
            self.pip_value = float(self.trade_tick_value or 0.0)
    
    def to_instrument_spec(self,
                           base_spec: Optional[InstrumentSpec] = None,
                           commission_per_lot: Optional[float] = None) -> InstrumentSpec:
        """Convert to InstrumentSpec for backtesting compatibility."""
        pip_position = self.digits - 1 if self.digits in [3, 5] else self.digits

        resolved_commission = commission_per_lot
        if resolved_commission is None and base_spec is not None:
            resolved_commission = getattr(base_spec, "commission_per_lot", None)
        if resolved_commission is None:
            try:
                resolved_commission = get_instrument_spec(self.symbol).commission_per_lot
            except Exception:
                resolved_commission = None
        try:
            resolved_commission = float(resolved_commission)
        except (TypeError, ValueError):
            resolved_commission = 7.0
        
        return InstrumentSpec(
            symbol=self.symbol,
            pip_position=pip_position,
            pip_value=self.pip_value,
            spread_avg=(self.spread * self.point / self.pip_size) if self.pip_size > 0 else float(self.spread),
            min_lot=self.volume_min,
            max_lot=self.volume_max,
            commission_per_lot=resolved_commission,
            swap_long=self.swap_long,
            swap_short=self.swap_short,
            tick_size=self.trade_tick_size,
            tick_value=self.trade_tick_value,
            contract_size=self.trade_contract_size,
            volume_step=self.volume_step,
            stops_level=self.trade_stops_level,
            point=self.point,
            digits=self.digits,
        )


@dataclass
class MT5AccountInfo:
    """Account information from MT5."""
    login: int
    server: str
    company: str
    balance: float
    equity: float
    margin: float
    margin_free: float
    margin_level: float
    profit: float
    leverage: int
    currency: str
    trade_allowed: bool
    trade_expert: bool


@dataclass
class MT5Tick:
    """Tick data from MT5."""
    time: datetime
    bid: float
    ask: float
    last: float
    volume: int


@dataclass
class MT5Position:
    """Position from MT5."""
    ticket: int
    symbol: str
    type: int  # 0=Buy, 1=Sell
    volume: float
    price_open: float
    price_current: float
    sl: float
    tp: float
    swap: float
    profit: float
    magic: int
    comment: str
    time: datetime
    identifier: int = 0        # POSITION_IDENTIFIER (stable ID for history lookups)
    reason: int = 0            # POSITION_REASON (0=CLIENT, 1=MOBILE, 2=WEB, 3=EXPERT, 4=SL, 5=TP, 6=SO)
    time_update: Optional[datetime] = None  # Last modification time


@dataclass
class MT5OrderResult:
    """Result of order execution."""
    success: bool
    retcode: int
    retcode_description: str
    deal: int = 0
    order: int = 0
    volume: float = 0.0
    price: float = 0.0
    error_message: str = ""


# Return code descriptions
RETCODE_DESCRIPTIONS = {
    0: "Check passed",
    10004: "Requote",
    10006: "Request rejected",
    10007: "Request canceled by trader",
    10008: "Order placed",
    10009: "Request completed",
    10010: "Only part of request completed",
    10011: "Request processing error",
    10012: "Request canceled by timeout",
    10013: "Invalid request",
    10014: "Invalid volume",
    10015: "Invalid price",
    10016: "Invalid stops",
    10017: "Trade disabled",
    10018: "Market closed",
    10019: "Insufficient funds",
    10020: "Prices changed",
    10021: "No quotes",
    10022: "Invalid expiration",
    10023: "Order state changed",
    10024: "Too many requests",
    10025: "No changes",
    10026: "Autotrading disabled by server",
    10027: "Autotrading disabled by client",
    10028: "Request locked",
    10029: "Order or position frozen",
    10030: "Invalid fill type",
    10031: "No connection to trade server",
    10032: "Operation allowed for live accounts only",
    10033: "Too many orders",
    10034: "No changes in order",
    10035: "No position with specified ticket",
    10036: "Invalid trade position",
    10038: "Close order rejected",
    10039: "Close volume exceeds position volume",
    10040: "Order for position already exists",
    10041: "Position limit reached",
    10042: "Pending order limit reached",
    10043: "Order or position not allowed",
    10044: "Position state changed",
    10045: "Request rejected due to rule",
}


# =============================================================================
# MT5 CONNECTOR CLASS
# =============================================================================

class MT5Connector:
    """
    MetaTrader 5 connection and trading class.
    
    Provides:
    - Connection management
    - Symbol information retrieval
    - Historical data fetching
    - Live tick data
    - Order execution
    - Position management
    """
    
    # Timeframe mapping
    TIMEFRAME_MAP = {
        'M1': mt5.TIMEFRAME_M1 if MT5_AVAILABLE else 1,
        'M5': mt5.TIMEFRAME_M5 if MT5_AVAILABLE else 5,
        'M15': mt5.TIMEFRAME_M15 if MT5_AVAILABLE else 15,
        'M30': mt5.TIMEFRAME_M30 if MT5_AVAILABLE else 30,
        'H1': mt5.TIMEFRAME_H1 if MT5_AVAILABLE else 60,
        'H4': mt5.TIMEFRAME_H4 if MT5_AVAILABLE else 240,
        'D1': mt5.TIMEFRAME_D1 if MT5_AVAILABLE else 1440,
        'W1': mt5.TIMEFRAME_W1 if MT5_AVAILABLE else 10080,
        'MN1': mt5.TIMEFRAME_MN1 if MT5_AVAILABLE else 43200,
    }
    
    def __init__(self, config: Optional[MT5Config] = None):
        """
        Initialize MT5 connector.
        
        Args:
            config: Optional MT5 configuration
        """
        self.config = config or MT5Config()
        self._connected = False
        self._symbol_cache: Dict[str, MT5SymbolInfo] = {}
        self._broker_symbol_cache: Dict[str, str] = {}
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self,
                login: int = None,
                password: str = None,
                server: str = None,
                path: str = None) -> bool:
        """
        Connect to MT5 terminal.
        
        Args:
            login: Account login (optional, uses config)
            password: Account password (optional, uses config)
            server: Server name (optional, uses config)
            path: Path to terminal (optional, uses config)
            
        Returns:
            True if connected successfully
        """
        if not MT5_AVAILABLE:
            logger.error("MetaTrader5 package not installed")
            return False
        
        # Use provided or config values
        login = login or self.config.login
        password = password or self.config.password
        server = server or self.config.server
        path = path or self.config.path
        
        # Initialize MT5
        init_kwargs = {}
        if path:
            init_kwargs['path'] = path
        if self.config.timeout:
            init_kwargs['timeout'] = self.config.timeout
        if self.config.portable:
            init_kwargs['portable'] = self.config.portable
        
        self._symbol_cache.clear()

        if not mt5.initialize(**init_kwargs):
            error = mt5.last_error()
            logger.error(f"MT5 initialization failed: {error}")
            return False
        
        # Login if credentials provided
        if login and password and server:
            authorized = mt5.login(login, password=password, server=server)
            if not authorized:
                error = mt5.last_error()
                logger.error(f"MT5 login failed: {error}")
                mt5.shutdown()
                return False
            logger.info(f"Connected to MT5: {login}@{server}")
        else:
            logger.info("Connected to MT5 (using existing session)")

        self._connected = True
        self._symbol_cache.clear()
        return True

    def disconnect(self):
        """Disconnect from MT5."""
        if MT5_AVAILABLE and self._connected:
            mt5.shutdown()
            self._connected = False
            self._symbol_cache.clear()
            logger.info("Disconnected from MT5")
    
    def is_connected(self) -> bool:
        """Check if connected to MT5."""
        if not MT5_AVAILABLE or not self._connected:
            return False
        
        # Verify connection by getting terminal info
        info = mt5.terminal_info()
        return info is not None
    
    def get_last_error(self) -> Tuple[int, str]:
        """Get last MT5 error."""
        if not MT5_AVAILABLE:
            return (-1, "MT5 not available")
        return mt5.last_error()
    
    # =========================================================================
    # Account Information
    # =========================================================================
    
    def get_account_info(self) -> Optional[MT5AccountInfo]:
        """Get current account information."""
        if not self._check_connection():
            return None
        
        info = mt5.account_info()
        if info is None:
            return None
        
        return MT5AccountInfo(
            login=info.login,
            server=info.server,
            company=info.company,
            balance=info.balance,
            equity=info.equity,
            margin=info.margin,
            margin_free=info.margin_free,
            margin_level=info.margin_level if info.margin_level else 0,
            profit=info.profit,
            leverage=info.leverage,
            currency=info.currency,
            trade_allowed=info.trade_allowed,
            trade_expert=info.trade_expert
        )
    
    def get_equity(self) -> float:
        """Get current account equity."""
        info = self.get_account_info()
        return info.equity if info else 0.0
    
    def get_balance(self) -> float:
        """Get current account balance."""
        info = self.get_account_info()
        return info.balance if info else 0.0
    
    # =========================================================================
    # Symbol Information
    # =========================================================================

    def _resolve_symbol(self, symbol: str) -> Optional[str]:
        """Resolve a canonical symbol to the broker-specific tradable symbol."""
        cached = self._broker_symbol_cache.get(symbol)
        if cached:
            return cached
        if not self._check_connection():
            return None
        if mt5.symbol_select(symbol, True):
            self._broker_symbol_cache[symbol] = symbol
            return symbol
        resolved = self.find_broker_symbol(symbol)
        if resolved:
            self._broker_symbol_cache[symbol] = resolved
            self._broker_symbol_cache[resolved] = resolved
        return resolved
    
    def get_symbol_info(self, symbol: str) -> Optional[MT5SymbolInfo]:
        """
        Get symbol information.
        
        Args:
            symbol: Symbol name
            
        Returns:
            MT5SymbolInfo or None
        """
        # Check cache
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]
        
        resolved_symbol = self._resolve_symbol(symbol)
        if resolved_symbol is None:
            logger.warning(f"Failed to resolve symbol: {symbol}")
            return None
        if resolved_symbol in self._symbol_cache:
            info_cached = self._symbol_cache[resolved_symbol]
            self._symbol_cache[symbol] = info_cached
            return info_cached

        info = mt5.symbol_info(resolved_symbol)
        if info is None:
            return None
        
        symbol_info = MT5SymbolInfo(
            symbol=info.name,
            digits=info.digits,
            point=info.point,
            trade_tick_value=info.trade_tick_value,
            trade_tick_size=info.trade_tick_size,
            trade_contract_size=info.trade_contract_size,
            volume_min=info.volume_min,
            volume_max=info.volume_max,
            volume_step=info.volume_step,
            spread=info.spread,
            spread_float=info.spread_float,
            swap_long=info.swap_long,
            swap_short=info.swap_short,
            trade_stops_level=info.trade_stops_level,
            visible=bool(getattr(info, "visible", True)),
            trade_mode=int(getattr(info, "trade_mode", 2) or 0),
            filling_mode=int(getattr(info, "filling_mode", 0) or 0),
            trade_exemode=int(getattr(info, "trade_exemode", 0) or 0),
            trade_freeze_level=int(getattr(info, "trade_freeze_level", 0) or 0),
        )
        
        # Cache it under both the requested alias and the broker symbol.
        self._symbol_cache[symbol] = symbol_info
        self._symbol_cache[resolved_symbol] = symbol_info
        self._broker_symbol_cache[symbol] = resolved_symbol
        self._broker_symbol_cache[resolved_symbol] = resolved_symbol
        
        return symbol_info
    
    def find_broker_symbol(self, base_symbol: str) -> Optional[str]:
        """
        Find actual broker symbol for a base symbol.
        
        Handles broker-specific suffixes like .a, .pro, m, etc.
        
        Args:
            base_symbol: Standard symbol name (e.g., 'EURUSD')
            
        Returns:
            Actual broker symbol or None
        """
        if not self._check_connection():
            return None
        
        cached = self._broker_symbol_cache.get(base_symbol)
        if cached:
            return cached

        # Try exact match first
        if mt5.symbol_select(base_symbol, True):
            self._broker_symbol_cache[base_symbol] = base_symbol
            return base_symbol
        
        # Get all symbols
        all_symbols = mt5.symbols_get()
        if all_symbols is None:
            return None
        
        # Common broker suffixes. We intentionally avoid loose prefix matching
        # because it can bind the PM to the wrong custom symbol/feed.
        suffixes = ['', '.a', '.pro', 'm', '.std', '.raw', '.ecn', '#', '.i']
        
        for suffix in suffixes:
            test_symbol = base_symbol + suffix
            for sym in all_symbols:
                if getattr(sym, "custom", False):
                    continue
                if sym.name.upper() == test_symbol.upper():
                    if mt5.symbol_select(sym.name, True):
                        logger.debug(f"Found broker symbol: {sym.name} for {base_symbol}")
                        self._broker_symbol_cache[base_symbol] = sym.name
                        self._broker_symbol_cache[sym.name] = sym.name
                        return sym.name
        return None
    
    def get_tick(self, symbol: str) -> Optional[MT5Tick]:
        """Get current tick for symbol."""
        if not self._check_connection():
            return None

        resolved_symbol = self._resolve_symbol(symbol)
        if resolved_symbol is None:
            return None

        tick = mt5.symbol_info_tick(resolved_symbol)
        if tick is None:
            return None
        
        return MT5Tick(
            time=datetime.fromtimestamp(tick.time),
            bid=tick.bid,
            ask=tick.ask,
            last=tick.last,
            volume=tick.volume
        )
    
    # =========================================================================
    # Historical Data
    # =========================================================================
    
    def get_bars(self,
                 symbol: str,
                 timeframe: str,
                 count: int = 1000,
                 from_date: datetime = None) -> Optional[pd.DataFrame]:
        """
        Get historical bars.
        
        Args:
            symbol: Symbol name
            timeframe: Timeframe string (M5, H1, D1, etc.)
            count: Number of bars to fetch
            from_date: Optional start date
            
        Returns:
            DataFrame with OHLCV data or None
        """
        if not self._check_connection():
            return None
        
        tf = self.TIMEFRAME_MAP.get(timeframe)
        if tf is None:
            logger.error(f"Unknown timeframe: {timeframe}")
            return None
        
        # Ensure symbol is selected
        broker_symbol = self.find_broker_symbol(symbol)
        if broker_symbol is None:
            logger.error(f"Symbol not found: {symbol}")
            return None
        
        # Fetch bars
        if from_date:
            rates = mt5.copy_rates_from(broker_symbol, tf, from_date, count)
        else:
            rates = mt5.copy_rates_from_pos(broker_symbol, tf, 0, count)
        
        if rates is None or len(rates) == 0:
            logger.warning(f"No data returned for {symbol} {timeframe}")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df = df.set_index('time')
        
        # Rename columns
        df = df.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'tick_volume': 'Volume',
            'spread': 'Spread',
            'real_volume': 'RealVolume'
        })
        
        return df
    
    def get_bars_range(self,
                       symbol: str,
                       timeframe: str,
                       start_date: datetime,
                       end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Get historical bars within date range.
        
        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with OHLCV data or None
        """
        if not self._check_connection():
            return None
        
        tf = self.TIMEFRAME_MAP.get(timeframe)
        if tf is None:
            return None
        
        broker_symbol = self.find_broker_symbol(symbol)
        if broker_symbol is None:
            return None
        
        rates = mt5.copy_rates_range(broker_symbol, tf, start_date, end_date)
        
        if rates is None or len(rates) == 0:
            return None
        
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df = df.set_index('time')
        df = df.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'tick_volume': 'Volume'
        })
        
        return df
    
    # =========================================================================
    # Order Execution
    # =========================================================================
    
    def send_market_order(self,
                          symbol: str,
                          order_type: OrderType,
                          volume: float,
                          sl: float = 0.0,
                          tp: float = 0.0,
                          deviation: int = 30,
                          magic: int = 0,
                          comment: str = "",
                          price: Optional[float] = None,
                          symbol_info: Optional[MT5SymbolInfo] = None) -> MT5OrderResult:
        """
        Send a market order.
        
        Args:
            symbol: Symbol to trade
            order_type: BUY or SELL
            volume: Lot size
            sl: Stop loss price (0 = no SL)
            tp: Take profit price (0 = no TP)
            deviation: Maximum price deviation in points
            magic: Magic number
            comment: Order comment
            
        Returns:
            MT5OrderResult
        """
        if not self._check_connection():
            return MT5OrderResult(False, -1, "Not connected")

        broker_symbol = self._resolve_symbol(symbol)
        if broker_symbol is None:
            return MT5OrderResult(False, -1, f"Symbol not available: {symbol}")

        # Get symbol info
        symbol_info = symbol_info or self.get_symbol_info(broker_symbol)
        if symbol_info is None:
            return MT5OrderResult(False, -1, f"Symbol info not available: {broker_symbol}")

        mt5_type = mt5.ORDER_TYPE_BUY if order_type == OrderType.BUY else mt5.ORDER_TYPE_SELL
        if price is None:
            # Get current price
            tick = self.get_tick(broker_symbol)
            if tick is None:
                return MT5OrderResult(False, -1, "No tick data")

            # Determine price
            price = tick.ask if order_type == OrderType.BUY else tick.bid
        else:
            try:
                price = float(price)
            except (TypeError, ValueError):
                return MT5OrderResult(False, 10015, f"Invalid price override: {price!r}")
            if price <= 0.0:
                return MT5OrderResult(False, 10015, f"Invalid price override: {price!r}")

        if order_type == OrderType.BUY:
            mt5_type = mt5.ORDER_TYPE_BUY
        else:
            mt5_type = mt5.ORDER_TYPE_SELL
        
        # Normalize volume
        volume = self._normalize_volume(volume, symbol_info)
        
        # Determine filling type
        filling_type = self._get_filling_type(broker_symbol, symbol_info=symbol_info)
        
        # Validate SL/TP against minimum stop level
        min_stop_distance = symbol_info.trade_stops_level * symbol_info.point
        
        if sl > 0:
            sl_distance = abs(price - sl)
            if sl_distance < min_stop_distance and min_stop_distance > 0:
                return MT5OrderResult(
                    False, 10016, 
                    f"SL too close: {sl_distance:.5f} < min {min_stop_distance:.5f} "
                    f"(stops_level={symbol_info.trade_stops_level}, point={symbol_info.point})"
                )
        
        if tp > 0:
            tp_distance = abs(price - tp)
            if tp_distance < min_stop_distance and min_stop_distance > 0:
                return MT5OrderResult(
                    False, 10016,
                    f"TP too close: {tp_distance:.5f} < min {min_stop_distance:.5f} "
                    f"(stops_level={symbol_info.trade_stops_level}, point={symbol_info.point})"
                )
        
        # Validate SL/TP are on correct side of entry price
        if order_type == OrderType.BUY:
            if sl > 0 and sl >= price:
                return MT5OrderResult(False, 10016, f"SL ({sl:.5f}) must be below entry ({price:.5f}) for BUY")
            if tp > 0 and tp <= price:
                return MT5OrderResult(False, 10016, f"TP ({tp:.5f}) must be above entry ({price:.5f}) for BUY")
        else:  # SELL
            if sl > 0 and sl <= price:
                return MT5OrderResult(False, 10016, f"SL ({sl:.5f}) must be above entry ({price:.5f}) for SELL")
            if tp > 0 and tp >= price:
                return MT5OrderResult(False, 10016, f"TP ({tp:.5f}) must be below entry ({price:.5f}) for SELL")
        
        # Create request
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": broker_symbol,
            "volume": volume,
            "type": mt5_type,
            "price": round(price, symbol_info.digits),
            "deviation": deviation,
            "magic": magic,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }
        
        # Round SL/TP to symbol digits to avoid rejection
        if sl > 0:
            request["sl"] = round(sl, symbol_info.digits)
        if tp > 0:
            request["tp"] = round(tp, symbol_info.digits)

        preflight = self._preflight_order_request(request)
        if preflight is not None:
            return preflight
        
        # Send order
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        
        retcode_desc = RETCODE_DESCRIPTIONS.get(result.retcode, f"Unknown ({result.retcode})")
        
        result_price = float(getattr(result, "price", 0.0) or 0.0)
        if result_price <= 0.0:
            result_price = float(request["price"])

        return MT5OrderResult(
            success=(result.retcode in [10008, 10009, 10010]),
            retcode=result.retcode,
            retcode_description=retcode_desc,
            deal=result.deal,
            order=result.order,
            volume=result.volume,
            price=result_price
        )
    
    def close_position(self,
                       position: MT5Position,
                       deviation: int = 30,
                       magic: int = 0,
                       comment: str = "") -> MT5OrderResult:
        """
        Close an open position.
        
        Args:
            position: Position to close
            deviation: Maximum price deviation
            magic: Magic number
            comment: Order comment
            
        Returns:
            MT5OrderResult
        """
        if not self._check_connection():
            return MT5OrderResult(False, -1, "Not connected")
        
        tick = self.get_tick(position.symbol)
        if tick is None:
            return MT5OrderResult(False, -1, "No tick data")
        
        # Determine close type and price
        if position.type == 0:  # Long position
            close_type = mt5.ORDER_TYPE_SELL
            price = tick.bid
        else:  # Short position
            close_type = mt5.ORDER_TYPE_BUY
            price = tick.ask
        
        symbol_info = self.get_symbol_info(position.symbol)
        filling_type = self._get_filling_type(position.symbol, symbol_info=symbol_info)
        
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": position.symbol,
            "volume": position.volume,
            "type": close_type,
            "position": position.ticket,
            "price": price,
            "deviation": deviation,
            "magic": magic or position.magic,
            "comment": comment or "Close position",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }

        preflight = self._preflight_order_request(request)
        if preflight is not None:
            return preflight
        
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        
        retcode_desc = RETCODE_DESCRIPTIONS.get(result.retcode, f"Unknown ({result.retcode})")
        
        result_price = float(getattr(result, "price", 0.0) or 0.0)
        if result_price <= 0.0:
            result_price = float(price)

        return MT5OrderResult(
            success=(result.retcode in [10008, 10009, 10010]),
            retcode=result.retcode,
            retcode_description=retcode_desc,
            deal=result.deal,
            order=result.order,
            volume=result.volume,
            price=result_price
        )
    
    def modify_position(self,
                        position: MT5Position,
                        sl: float = None,
                        tp: float = None) -> MT5OrderResult:
        """
        Modify position SL/TP.
        
        Args:
            position: Position to modify
            sl: New stop loss (None = keep current)
            tp: New take profit (None = keep current)
            
        Returns:
            MT5OrderResult
        """
        if not self._check_connection():
            return MT5OrderResult(False, -1, "Not connected")

        symbol_info = self.get_symbol_info(position.symbol)
        if symbol_info is None:
            return MT5OrderResult(False, -1, f"Symbol info not available: {position.symbol}")
        tick = self.get_tick(position.symbol)
        if tick is None:
            return MT5OrderResult(False, -1, "No tick data")

        is_long = position.type == 0
        reference_price = tick.bid if is_long else tick.ask
        min_stop_distance = float(symbol_info.trade_stops_level or 0) * float(symbol_info.point)
        freeze_distance = float(symbol_info.trade_freeze_level or 0) * float(symbol_info.point)
        required_distance = max(min_stop_distance, freeze_distance)

        new_sl = round(sl, symbol_info.digits) if sl is not None else position.sl
        new_tp = round(tp, symbol_info.digits) if tp is not None else position.tp

        if new_sl:
            if is_long and new_sl >= reference_price:
                return MT5OrderResult(False, 10016, "SL must be below current price for BUY")
            if (not is_long) and new_sl <= reference_price:
                return MT5OrderResult(False, 10016, "SL must be above current price for SELL")
            if required_distance > 0 and abs(reference_price - new_sl) < required_distance:
                return MT5OrderResult(False, 10016, "SL violates stop/freeze distance")
        if new_tp:
            if is_long and new_tp <= reference_price:
                return MT5OrderResult(False, 10016, "TP must be above current price for BUY")
            if (not is_long) and new_tp >= reference_price:
                return MT5OrderResult(False, 10016, "TP must be below current price for SELL")
            if required_distance > 0 and abs(reference_price - new_tp) < required_distance:
                return MT5OrderResult(False, 10016, "TP violates stop/freeze distance")
        
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "symbol": position.symbol,
            "position": position.ticket,
            "sl": new_sl,
            "tp": new_tp,
        }

        preflight = self._preflight_order_request(request)
        if preflight is not None:
            return preflight
        
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        
        return MT5OrderResult(
            success=(result.retcode in [10008, 10009, 10010]),
            retcode=result.retcode,
            retcode_description=RETCODE_DESCRIPTIONS.get(result.retcode, "Unknown")
        )
    
    # =========================================================================
    # Position Management
    # =========================================================================
    
    def get_positions(self, symbol: str = None, magic: int = None) -> Optional[List[MT5Position]]:
        """
        Get open positions.
        
        Args:
            symbol: Filter by symbol (optional)
            magic: Filter by magic number (optional)
            
        Returns:
            List of MT5Position, or None if MT5 could not provide a snapshot
        """
        if not self._check_connection():
            return None

        if symbol:
            resolved_symbol = self._resolve_symbol(symbol) or symbol
            positions = mt5.positions_get(symbol=resolved_symbol)
        else:
            positions = mt5.positions_get()

        if positions is None:
            return None

        result = []
        for pos in positions:
            if magic is not None and pos.magic != magic:
                continue
            
            time_update_raw = getattr(pos, 'time_update', None)
            result.append(MT5Position(
                ticket=pos.ticket,
                symbol=pos.symbol,
                type=pos.type,
                volume=pos.volume,
                price_open=pos.price_open,
                price_current=pos.price_current,
                sl=pos.sl,
                tp=pos.tp,
                swap=pos.swap,
                profit=pos.profit,
                magic=pos.magic,
                comment=pos.comment,
                time=datetime.fromtimestamp(pos.time),
                identifier=int(getattr(pos, 'identifier', 0) or 0),
                reason=int(getattr(pos, 'reason', 0) or 0),
                time_update=datetime.fromtimestamp(time_update_raw) if time_update_raw else None,
            ))
        
        return result
    
    def get_position_by_ticket(self, ticket: int) -> Optional[MT5Position]:
        """Get position by ticket number."""
        positions = self.get_positions()
        if positions is None:
            return None
        for pos in positions:
            if pos.ticket == ticket:
                return pos
        return None

    def get_position_by_symbol_magic(self, symbol: str, magic: int) -> Optional[MT5Position]:
        """Get position by symbol and magic number."""
        positions = self.get_positions(symbol=symbol, magic=magic)
        if positions is None:
            return None
        return positions[0] if positions else None

    def count_positions(self, symbol: str = None, magic: int = None) -> int:
        """Count open positions."""
        positions = self.get_positions(symbol=symbol, magic=magic)
        return len(positions) if positions is not None else 0

    def get_position_opening_metadata(self, position_identifier: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve the opening order/deal metadata for a position by its identifier.

        Uses MT5 history_orders_get(position=...) and history_deals_get(position=...)
        to find the original comment and magic from the entry order/deal. This is the
        robust recovery path for positions whose live comment was truncated or cleared.

        Returns dict with keys: comment, magic, reason  (or None on failure).
        """
        if not position_identifier or not self._check_connection():
            return None

        try:
            # Try deals first — deals carry the final executed metadata
            deals = mt5.history_deals_get(position=position_identifier)
            if deals:
                entry_type = getattr(mt5, 'DEAL_ENTRY_IN', 0)
                for deal in deals:
                    if getattr(deal, 'entry', None) == entry_type:
                        return {
                            'comment': getattr(deal, 'comment', '') or '',
                            'magic': int(getattr(deal, 'magic', 0) or 0),
                            'reason': int(getattr(deal, 'reason', 0) or 0),
                        }
                # Fallback: first deal if no explicit ENTRY_IN found
                deal = deals[0]
                return {
                    'comment': getattr(deal, 'comment', '') or '',
                    'magic': int(getattr(deal, 'magic', 0) or 0),
                    'reason': int(getattr(deal, 'reason', 0) or 0),
                }
        except Exception:
            pass

        try:
            # Fallback to orders — opening order may have the original comment
            orders = mt5.history_orders_get(position=position_identifier)
            if orders:
                order = orders[0]
                return {
                    'comment': getattr(order, 'comment', '') or '',
                    'magic': int(getattr(order, 'magic', 0) or 0),
                    'reason': int(getattr(order, 'reason', 0) or 0),
                }
        except Exception:
            pass

        return None

    def get_recent_closing_deals(self, lookback_hours: int = 48) -> List[Dict[str, Any]]:
        """Fetch recent closing deals for drift-monitor integration."""
        if not self._check_connection():
            return []

        from_time = datetime.now() - timedelta(hours=max(1, int(lookback_hours)))
        to_time = datetime.now()

        try:
            deals = mt5.history_deals_get(from_time, to_time)
        except Exception:
            return []

        if deals is None:
            return []

        result: List[Dict[str, Any]] = []
        exit_entries = {
            getattr(mt5, 'DEAL_ENTRY_OUT', 1),
            getattr(mt5, 'DEAL_ENTRY_OUT_BY', 3),
        }
        for deal in deals:
            entry_type = getattr(deal, 'entry', None)
            if entry_type is not None and entry_type not in exit_entries:
                continue
            deal_time = getattr(deal, 'time', None)
            result.append({
                'ticket': int(getattr(deal, 'ticket', 0) or 0),
                'position_id': int(getattr(deal, 'position_id', 0) or 0),
                'symbol': getattr(deal, 'symbol', ''),
                'profit': float(getattr(deal, 'profit', 0.0) or 0.0),
                'comment': getattr(deal, 'comment', '') or '',
                'magic': int(getattr(deal, 'magic', 0) or 0),
                'time': datetime.fromtimestamp(deal_time) if deal_time else to_time,
            })
        return result
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _check_connection(self) -> bool:
        """Check and log connection status."""
        if not MT5_AVAILABLE:
            logger.error("MetaTrader5 package not installed")
            return False
        
        if not self._connected:
            logger.error("Not connected to MT5")
            return False
        
        return True
    
    def _normalize_volume(self, volume: float, symbol_info: MT5SymbolInfo) -> float:
        """Normalize volume to valid lot size."""
        volume = max(symbol_info.volume_min, volume)
        volume = min(symbol_info.volume_max, volume)

        # Round to volume step (guard against broker returning 0)
        step = symbol_info.volume_step
        if step <= 0:
            step = 0.01
        volume = math.floor((volume / step) + 1e-12) * step  # floor-to-step (risk-safe)
        volume = max(symbol_info.volume_min, min(symbol_info.volume_max, volume))

        precision = max(
            self._volume_decimal_places(step),
            self._volume_decimal_places(symbol_info.volume_min),
        )
        return round(volume, precision)

    @staticmethod
    def _volume_decimal_places(value: float) -> int:
        text = f"{float(value):.10f}".rstrip("0").rstrip(".")
        if "." not in text:
            return 0
        return min(8, len(text.rsplit(".", 1)[1]))
    
    
    def normalize_volume(self, volume: float, symbol_info: MT5SymbolInfo) -> float:
        """Public wrapper for volume normalization (risk-safe)."""
        return self._normalize_volume(volume, symbol_info)

    def order_calc_profit(self, order_type: int, symbol: str, volume: float,
                          price_open: float, price_close: float) -> Optional[float]:
        """Wrapper around MT5 order_calc_profit (returns profit in deposit currency)."""
        if not MT5_AVAILABLE:
            return None
        try:
            resolved_symbol = self._resolve_symbol(symbol) or symbol
            return mt5.order_calc_profit(order_type, resolved_symbol, volume, price_open, price_close)
        except Exception as e:
            logger.debug(f"order_calc_profit failed for {symbol}: {e}")
            return None

    def calc_loss_amount(self, order_type: int, symbol: str, volume: float,
                         entry_price: float, stop_price: float) -> Optional[float]:
        """Compute absolute loss at stop (deposit currency) using MT5 contract math."""
        profit = self.order_calc_profit(order_type, symbol, volume, entry_price, stop_price)
        if profit is None:
            return None
        return abs(float(profit))

    def calc_margin_required(self, order_type: int, symbol: str, volume: float,
                             price: float) -> Optional[float]:
        """Estimate required margin for a proposed market order."""
        if not MT5_AVAILABLE or not hasattr(mt5, "order_calc_margin"):
            return None
        try:
            resolved_symbol = self._resolve_symbol(symbol) or symbol
            if int(order_type) == int(getattr(OrderType, "BUY").value):
                mt5_type = mt5.ORDER_TYPE_BUY
            else:
                mt5_type = mt5.ORDER_TYPE_SELL
            margin = mt5.order_calc_margin(mt5_type, resolved_symbol, float(volume), float(price))
        except Exception as e:
            logger.debug(f"order_calc_margin failed for {symbol}: {e}")
            return None
        if margin is None:
            return None
        try:
            return abs(float(margin))
        except (TypeError, ValueError):
            return None

    def _preflight_order_request(self, request: Dict[str, Any]) -> Optional[MT5OrderResult]:
        """Validate an MT5 trade request with order_check when available."""
        if not MT5_AVAILABLE or not hasattr(mt5, "order_check"):
            return None
        try:
            result = mt5.order_check(request)
        except Exception as exc:
            return MT5OrderResult(False, -1, f"order_check exception: {exc}")
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        retcode = int(getattr(result, "retcode", 0) or 0)
        if retcode == 0:
            return None
        description = (
            getattr(result, "comment", "")
            or RETCODE_DESCRIPTIONS.get(retcode, f"Order check failed ({retcode})")
        )
        return MT5OrderResult(False, retcode, str(description))

    def _get_filling_type(self, symbol: str, symbol_info: Optional[MT5SymbolInfo] = None) -> int:
        """Get appropriate filling type for symbol."""
        if not MT5_AVAILABLE:
            return 0

        info = symbol_info
        if info is None:
            info = mt5.symbol_info(symbol)
        if info is None:
            return mt5.ORDER_FILLING_IOC

        filling_modes = int(getattr(info, "filling_mode", 0) or 0)
        trade_exemode = int(getattr(info, "trade_exemode", 0) or 0)
        symbol_fok = int(getattr(mt5, "SYMBOL_FILLING_FOK", 1))
        symbol_ioc = int(getattr(mt5, "SYMBOL_FILLING_IOC", 2))
        market_execution = int(getattr(mt5, "SYMBOL_TRADE_EXECUTION_MARKET", 2))

        def _broker_preferred_fill() -> int:
            if filling_modes & symbol_fok:
                return mt5.ORDER_FILLING_FOK
            if filling_modes & symbol_ioc:
                return mt5.ORDER_FILLING_IOC
            if trade_exemode != market_execution:
                return mt5.ORDER_FILLING_RETURN
            return mt5.ORDER_FILLING_FOK

        policy = str(getattr(self.config, "preferred_filling_type", "broker") or "broker").strip().lower()
        if policy in {"fok", "ioc", "return"}:
            override_map = {
                "fok": mt5.ORDER_FILLING_FOK,
                "ioc": mt5.ORDER_FILLING_IOC,
                "return": mt5.ORDER_FILLING_RETURN,
            }
            requested = override_map[policy]
            if requested == mt5.ORDER_FILLING_RETURN and trade_exemode == market_execution:
                logger.warning(
                    f"{symbol}: ORDER_FILLING_RETURN requested but forbidden for market execution; "
                    "falling back to broker-supported mode"
                )
                return _broker_preferred_fill()
            return requested

        return _broker_preferred_fill()

    def get_symbol_tick(self, symbol: str) -> Optional[MT5Tick]:
        """Alias for get_tick (used by pm_main.py)."""
        return self.get_tick(symbol)

    def save_broker_specs_to_json(self, symbols: List[str], filepath: str = "broker_specs.json") -> bool:
        """
        Save MT5 symbol specifications to JSON file for offline backtesting.
        
        This creates a broker_specs.json that can be used by the backtester
        to achieve parity with live trading sizing and P&L calculation.
        
        Args:
            symbols: List of symbols to save specs for
            filepath: Output file path
            
        Returns:
            True if successful
        """
        import json
        
        if not self._check_connection():
            return False
        
        specs = {}
        for symbol in symbols:
            broker_symbol = self.find_broker_symbol(symbol)
            if broker_symbol is None:
                logger.warning(f"Symbol not found: {symbol}")
                continue
            
            info = self.get_symbol_info(broker_symbol)
            if info is None:
                continue
            
            # Store all relevant MT5 fields
            specs[symbol] = {
                'symbol': symbol,
                'broker_symbol': broker_symbol,
                'digits': info.digits,
                'point': info.point,
                'tick_size': info.trade_tick_size,
                'tick_value': info.trade_tick_value,
                'contract_size': info.trade_contract_size,
                'volume_min': info.volume_min,
                'volume_max': info.volume_max,
                'volume_step': info.volume_step,
                'spread': info.spread,
                'spread_float': info.spread_float,
                'swap_long': info.swap_long,
                'swap_short': info.swap_short,
                'stops_level': info.trade_stops_level,
                'pip_size': info.pip_size,
                'pip_value': info.pip_value,
            }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(specs, f, indent=2)
            logger.info(f"Saved broker specs for {len(specs)} symbols to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to save broker specs: {e}")
            return False


# =============================================================================
# CONTEXT MANAGER
# =============================================================================

class MT5Connection:
    """Context manager for MT5 connection."""
    
    def __init__(self, config: MT5Config = None):
        self.connector = MT5Connector(config)
    
    def __enter__(self) -> MT5Connector:
        if not self.connector.connect():
            raise ConnectionError("Failed to connect to MT5")
        return self.connector
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connector.disconnect()


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'MT5_AVAILABLE',
    'MT5Config',
    'MT5SymbolInfo',
    'MT5AccountInfo',
    'MT5Tick',
    'MT5Position',
    'MT5OrderResult',
    'OrderType',
    'FillingType',
    'MT5Connector',
    'MT5Connection',
    'RETCODE_DESCRIPTIONS',
]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print(f"MT5 Available: {MT5_AVAILABLE}")
    
    if MT5_AVAILABLE:
        connector = MT5Connector()
        if connector.connect():
            print("Connected to MT5")
            
            account = connector.get_account_info()
            if account:
                print(f"Account: {account.login}")
                print(f"Balance: {account.balance} {account.currency}")
                print(f"Equity: {account.equity} {account.currency}")
            
            connector.disconnect()
    else:
        print("Install MetaTrader5 package: pip install MetaTrader5")
    
    print("\npm_mt5.py loaded successfully!")
