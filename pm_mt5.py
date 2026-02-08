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

Version: 3.0 (Portfolio Manager)
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
    
    def to_instrument_spec(self) -> InstrumentSpec:
        """Convert to InstrumentSpec for backtesting compatibility."""
        pip_position = self.digits - 1 if self.digits in [3, 5] else self.digits
        
        return InstrumentSpec(
            symbol=self.symbol,
            pip_position=pip_position,
            pip_value=self.pip_value,
            spread_avg=(self.spread * self.point / self.pip_size) if self.pip_size > 0 else float(self.spread),
            min_lot=self.volume_min,
            max_lot=self.volume_max,
            commission_per_lot=7.0,  # Default, broker-specific
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
        return True
    
    def disconnect(self):
        """Disconnect from MT5."""
        if MT5_AVAILABLE and self._connected:
            mt5.shutdown()
            self._connected = False
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
        
        if not self._check_connection():
            return None
        
        # Enable symbol
        if not mt5.symbol_select(symbol, True):
            # Try to find broker variant
            broker_symbol = self.find_broker_symbol(symbol)
            if broker_symbol and broker_symbol != symbol:
                return self.get_symbol_info(broker_symbol)
            logger.warning(f"Failed to select symbol: {symbol}")
            return None
        
        info = mt5.symbol_info(symbol)
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
            trade_stops_level=info.trade_stops_level
        )
        
        # Cache it
        self._symbol_cache[symbol] = symbol_info
        
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
        
        # Try exact match first
        if mt5.symbol_select(base_symbol, True):
            return base_symbol
        
        # Get all symbols
        all_symbols = mt5.symbols_get()
        if all_symbols is None:
            return None
        
        # Common suffixes to try
        suffixes = ['', '.a', '.pro', 'm', '.std', '.raw', '.ecn', '#', '.i']
        
        for suffix in suffixes:
            test_symbol = base_symbol + suffix
            for sym in all_symbols:
                if sym.name.upper() == test_symbol.upper():
                    if mt5.symbol_select(sym.name, True):
                        logger.debug(f"Found broker symbol: {sym.name} for {base_symbol}")
                        return sym.name
        
        # Try prefix match
        for sym in all_symbols:
            if sym.name.upper().startswith(base_symbol.upper()):
                if mt5.symbol_select(sym.name, True):
                    logger.debug(f"Found broker symbol: {sym.name} for {base_symbol}")
                    return sym.name
        
        return None
    
    def get_tick(self, symbol: str) -> Optional[MT5Tick]:
        """Get current tick for symbol."""
        if not self._check_connection():
            return None
        
        tick = mt5.symbol_info_tick(symbol)
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
                          comment: str = "") -> MT5OrderResult:
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
        
        # Get symbol info
        symbol_info = self.get_symbol_info(symbol)
        if symbol_info is None:
            return MT5OrderResult(False, -1, f"Symbol info not available: {symbol}")
        
        # Get current price
        tick = self.get_tick(symbol)
        if tick is None:
            return MT5OrderResult(False, -1, "No tick data")
        
        # Determine price
        if order_type == OrderType.BUY:
            price = tick.ask
            mt5_type = mt5.ORDER_TYPE_BUY
        else:
            price = tick.bid
            mt5_type = mt5.ORDER_TYPE_SELL
        
        # Normalize volume
        volume = self._normalize_volume(volume, symbol_info)
        
        # Determine filling type
        filling_type = self._get_filling_type(symbol)
        
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
            "symbol": symbol,
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
        
        # Send order
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        
        retcode_desc = RETCODE_DESCRIPTIONS.get(result.retcode, f"Unknown ({result.retcode})")
        
        return MT5OrderResult(
            success=(result.retcode in [10008, 10009]),
            retcode=result.retcode,
            retcode_description=retcode_desc,
            deal=result.deal,
            order=result.order,
            volume=result.volume,
            price=result.price
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
        
        filling_type = self._get_filling_type(position.symbol)
        
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
        
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        
        retcode_desc = RETCODE_DESCRIPTIONS.get(result.retcode, f"Unknown ({result.retcode})")
        
        return MT5OrderResult(
            success=(result.retcode in [10008, 10009]),
            retcode=result.retcode,
            retcode_description=retcode_desc,
            deal=result.deal,
            order=result.order,
            volume=result.volume,
            price=result.price
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
        
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "symbol": position.symbol,
            "position": position.ticket,
            "sl": sl if sl is not None else position.sl,
            "tp": tp if tp is not None else position.tp,
        }
        
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))
        
        return MT5OrderResult(
            success=(result.retcode in [10008, 10009]),
            retcode=result.retcode,
            retcode_description=RETCODE_DESCRIPTIONS.get(result.retcode, "Unknown")
        )
    
    # =========================================================================
    # Position Management
    # =========================================================================
    
    def get_positions(self, symbol: str = None, magic: int = None) -> List[MT5Position]:
        """
        Get open positions.
        
        Args:
            symbol: Filter by symbol (optional)
            magic: Filter by magic number (optional)
            
        Returns:
            List of MT5Position
        """
        if not self._check_connection():
            return []
        
        if symbol:
            positions = mt5.positions_get(symbol=symbol)
        else:
            positions = mt5.positions_get()
        
        if positions is None:
            return []
        
        result = []
        for pos in positions:
            if magic is not None and pos.magic != magic:
                continue
            
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
                time=datetime.fromtimestamp(pos.time)
            ))
        
        return result
    
    def get_position_by_ticket(self, ticket: int) -> Optional[MT5Position]:
        """Get position by ticket number."""
        positions = self.get_positions()
        for pos in positions:
            if pos.ticket == ticket:
                return pos
        return None
    
    def get_position_by_symbol_magic(self, symbol: str, magic: int) -> Optional[MT5Position]:
        """Get position by symbol and magic number."""
        positions = self.get_positions(symbol=symbol, magic=magic)
        return positions[0] if positions else None
    
    def count_positions(self, symbol: str = None, magic: int = None) -> int:
        """Count open positions."""
        return len(self.get_positions(symbol=symbol, magic=magic))
    
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
        volume = math.floor(volume / step) * step  # floor-to-step (risk-safe)
        volume = math.floor(volume * 100) / 100.0  # avoid rounding up

        return volume
    
    
    def normalize_volume(self, volume: float, symbol_info: MT5SymbolInfo) -> float:
        """Public wrapper for volume normalization (risk-safe)."""
        return self._normalize_volume(volume, symbol_info)

    def order_calc_profit(self, order_type: int, symbol: str, volume: float,
                          price_open: float, price_close: float) -> Optional[float]:
        """Wrapper around MT5 order_calc_profit (returns profit in deposit currency)."""
        if not MT5_AVAILABLE:
            return None
        try:
            return mt5.order_calc_profit(order_type, symbol, volume, price_open, price_close)
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

    def _get_filling_type(self, symbol: str) -> int:
        """Get appropriate filling type for symbol."""
        if not MT5_AVAILABLE:
            return 0
        
        info = mt5.symbol_info(symbol)
        if info is None:
            return mt5.ORDER_FILLING_IOC
        
        # Check what filling types are allowed
        filling_modes = info.filling_mode
        
        if filling_modes & 1:  # FOK allowed
            return mt5.ORDER_FILLING_FOK
        elif filling_modes & 2:  # IOC allowed
            return mt5.ORDER_FILLING_IOC
        else:
            return mt5.ORDER_FILLING_RETURN

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
