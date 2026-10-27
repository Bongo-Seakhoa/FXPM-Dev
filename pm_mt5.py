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
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Dict, List, Mapping, Optional, Any, Tuple
from enum import Enum
import time
import math
import os
import threading
from contextlib import contextmanager
from functools import wraps
from types import SimpleNamespace

import pandas as pd
import numpy as np

# MT5 import with fallback
try:
    import MetaTrader5 as mt5

    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    mt5 = None

from pm_core import InstrumentSpec
from pm_symbol import infer_asset_class

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
    expected_login: int = 0
    expected_server: str = ""
    expected_currency: str = ""
    path: str = ""
    timeout: int = 60000
    portable: bool = False
    preferred_filling_type: str = "broker"
    # History sync: how hard to push the terminal to download history for a
    # symbol that was just added to Market Watch (stock CFDs / exotic metals are
    # not in the default watch list, so the first copy_rates_* reads an empty
    # local cache while the terminal downloads in the background).
    history_sync_max_attempts: int = 6
    history_sync_sleep_seconds: float = 0.6
    # Historical fetch pages in chunks of this size so a count larger than the
    # symbol's locally-synced depth never triggers MT5 (-2) 'Invalid params'.
    history_page_size: int = 50000


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
    swap_mode: int = -1
    swap_rollover3days: int = 3
    # The MetaTrader5 Python binding used by FxPM currently omits the seven
    # official named-day swap multipliers.  Keep them nullable and never
    # synthesize them from ``swap_rollover3days``: a complete authoritative
    # schedule is supplied by the separately authenticated MQL5 terms export.
    swap_sunday: Optional[float] = None
    swap_monday: Optional[float] = None
    swap_tuesday: Optional[float] = None
    swap_wednesday: Optional[float] = None
    swap_thursday: Optional[float] = None
    swap_friday: Optional[float] = None
    swap_saturday: Optional[float] = None
    swap_terms_provenance_status: str = "INCOMPLETE"
    filling_mode: int = 0
    trade_exemode: int = 0
    trade_freeze_level: int = 0
    path: str = ""
    asset_class: str = "unknown"
    chart_mode: Optional[int] = None

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
            self.pip_size = (
                float(10 ** (-pip_position)) if pip_position >= 0 else 0.0001
            )

        # Pip value calculation (tick-math parity when available)
        if self.trade_tick_size > 0.0 and self.trade_tick_value > 0.0:
            self.pip_value = (
                self.pip_size / self.trade_tick_size
            ) * self.trade_tick_value
        else:
            self.pip_value = float(self.trade_tick_value or 0.0)
        self.asset_class = infer_asset_class(
            self.symbol,
            broker_data={"path": self.path, "asset_class": self.asset_class},
            default="unknown",
        )

    def to_instrument_spec(self) -> InstrumentSpec:
        """Convert to InstrumentSpec for backtesting compatibility."""
        pip_position = self.digits - 1 if self.digits in [3, 5] else self.digits

        return InstrumentSpec(
            symbol=self.symbol,
            pip_position=pip_position,
            pip_value=self.pip_value,
            spread_avg=(self.spread * self.point / self.pip_size)
            if self.pip_size > 0
            else float(self.spread),
            min_lot=self.volume_min,
            max_lot=self.volume_max,
            swap_long=self.swap_long,
            swap_short=self.swap_short,
            swap_mode=self.swap_mode,
            swap_rollover3days=self.swap_rollover3days,
            swap_sunday=self.swap_sunday,
            swap_monday=self.swap_monday,
            swap_tuesday=self.swap_tuesday,
            swap_wednesday=self.swap_wednesday,
            swap_thursday=self.swap_thursday,
            swap_friday=self.swap_friday,
            swap_saturday=self.swap_saturday,
            swap_terms_provenance_status=self.swap_terms_provenance_status,
            tick_size=self.trade_tick_size,
            tick_value=self.trade_tick_value,
            contract_size=self.trade_contract_size,
            volume_step=self.volume_step,
            stops_level=self.trade_stops_level,
            point=self.point,
            digits=self.digits,
            asset_class=self.asset_class,
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
    # MT5 ACCOUNT_MARGIN_MODE_RETAIL_HEDGING == 2.  Only that accounting
    # mode permits multiple independently attributed positions per symbol.
    margin_mode: int = -1
    hedge_allowed: Optional[bool] = None
    trade_mode: int = -1


@dataclass
class MT5Tick:
    """Tick data from MT5."""

    time: datetime
    bid: float
    ask: float
    last: float
    volume: int
    time_msc: Optional[int] = None


@dataclass(frozen=True, slots=True)
class MT5QuoteTick:
    """One validated native row; equal-millisecond rows retain native order."""

    time: datetime
    time_msc: int
    same_msc_ordinal: int
    raw_ordinal: int
    bid: float
    ask: float
    last: float
    flags: int
    volume: int
    volume_real: float


@dataclass(frozen=True, slots=True)
class MT5QuoteTickPage:
    requested_symbol: Optional[str]
    broker_symbol: Optional[str]
    start_msc: Optional[int]
    end_msc: Optional[int]
    query_from_utc: Optional[datetime]
    query_to_utc: Optional[datetime]
    ticks: Tuple[MT5QuoteTick, ...]
    raw_count: int
    complete: bool
    status: str
    native_error_code: Optional[int]
    native_error_message: Optional[str]

    @property
    def diagnostics(self) -> Dict[str, Any]:
        return {name: getattr(self, name) for name in (
            "requested_symbol", "broker_symbol", "start_msc", "end_msc",
            "query_from_utc", "query_to_utc", "raw_count", "complete", "status",
            "native_error_code", "native_error_message")}


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
    identifier: int = 0  # POSITION_IDENTIFIER (stable ID for history lookups)
    reason: int = (
        0  # POSITION_REASON (0=CLIENT, 1=MOBILE, 2=WEB, 3=EXPERT, 4=SL, 5=TP, 6=SO)
    )
    time_update: Optional[datetime] = None  # Last modification time
    destination: str = "BROKER"
    runtime_namespace: str = ""
    paper_position_id: str = ""


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
    ambiguous: bool = False
    position_id: int = 0
    paper_position_id: str = ""
    destination: str = "BROKER"


def account_identity_block_reason(
    account: Any, *, connector_config: Any,
    is_current_refit_demo: bool = False, baseline_account: Any = None,
) -> Optional[str]:
    """Compare identity only; protective management must not require equity."""
    if account is None:
        return "fresh account snapshot unavailable"
    if is_current_refit_demo:
        from pm_demo_readiness import AUTHORIZED_DEMO_ACCOUNT_IDENTITY
        expected_login = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login
        expected_server = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server
        expected_currency = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency
    else:
        if type(connector_config).__module__.startswith("unittest.mock"):
            connector_config = None
        try:
            expected_login = int(getattr(connector_config, "expected_login", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return "configured expected_login is invalid"
        expected_server = str(getattr(connector_config, "expected_server", "") or "").strip()
        expected_currency = str(getattr(connector_config, "expected_currency", "") or "").strip().upper()
    try:
        fresh_login = int(getattr(account, "login", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return "fresh account login is invalid"
    if expected_login <= 0:
        return "MT5 account identity requires a nonzero expected_login pin"
    if fresh_login != expected_login:
        return f"fresh account login {fresh_login} != expected {expected_login}"
    fresh_server = str(getattr(account, "server", "") or "").strip()
    if expected_server and fresh_server.casefold() != expected_server.casefold():
        return f"fresh account server {fresh_server or '<missing>'} != expected {expected_server}"
    if not expected_server:
        return "MT5 account identity requires a nonempty expected_server pin"
    if not expected_currency:
        return "MT5 account identity requires a nonempty expected_currency pin"
    fresh_currency = str(getattr(account, "currency", "") or "").strip().upper()
    if fresh_currency != expected_currency:
        return f"fresh account currency {fresh_currency or '<missing>'} != expected {expected_currency}"
    if baseline_account is not None:
        try:
            baseline_login = int(getattr(baseline_account, "login", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            baseline_login = 0
        if baseline_login > 0 and baseline_login != fresh_login:
            return f"account login changed during decision: {baseline_login} -> {fresh_login}"
        for attr in ("server", "currency"):
            before = str(getattr(baseline_account, attr, "") or "").strip().casefold()
            after = str(getattr(account, attr, "") or "").strip().casefold()
            if before and after and before != after:
                return f"account {attr} changed during decision"
    return None


class NativeSessionError(RuntimeError):
    """Native binding or exclusion failed; no alternative account is authority."""


class MT5QuoteTickPageError(NativeSessionError):
    """Incomplete acquisition; validated records alone confer no coverage."""

    def __init__(self, code: str, page: MT5QuoteTickPage):
        self.code = code
        self.page = page
        self.partial_ticks = page.ticks
        self.diagnostics = page.diagnostics
        super().__init__(code)


class PaperProtectionDataError(NativeSessionError):
    """Other positions were protected, but this is not a complete risk snapshot."""

    def __init__(self, symbol_errors, partial_positions):
        self.symbol_errors = {symbol: list(errors) for symbol, errors in symbol_errors.items()}
        self.partial_positions = tuple(partial_positions)
        super().__init__("PAPER_PROTECTION_DATA_UNAVAILABLE:" + ",".join(sorted(symbol_errors)))


class NativeAccountBindingError(NativeSessionError):
    """Observed identity/destination differs from the immutable account binding."""


_NATIVE_API_LOCK = threading.RLock()
_NATIVE_LOCAL = threading.local()
_NATIVE_ACTIVE_CONNECTOR = None
_NATIVE_MUTEX_NAME = "Local\\FXPM.NativeTerminalWriter.v1"
_NATIVE_MUTATORS = frozenset({
    "send_market_order", "close_position", "close_position_partial", "modify_position",
})


@contextmanager
def _native_terminal_lease():
    with _NATIVE_API_LOCK:
        depth = int(getattr(_NATIVE_LOCAL, "depth", 0))
        handle = None
        api = None
        acquired = False
        abandoned = False
        if depth == 0 and os.name == "nt":
            import ctypes
            from ctypes import wintypes
            api = ctypes.WinDLL("kernel32", use_last_error=True)
            api.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
            api.CreateMutexW.restype = wintypes.HANDLE
            api.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            api.WaitForSingleObject.restype = wintypes.DWORD
            api.ReleaseMutex.argtypes = [wintypes.HANDLE]
            api.CloseHandle.argtypes = [wintypes.HANDLE]
            handle = api.CreateMutexW(None, False, _NATIVE_MUTEX_NAME)
            if not handle:
                raise NativeSessionError("NATIVE_TERMINAL_LEASE_UNAVAILABLE")
            result = api.WaitForSingleObject(handle, 60000)
            acquired = result in (0, 0x80)
            abandoned = result == 0x80
            if not acquired:
                api.CloseHandle(handle)
                raise NativeSessionError("NATIVE_TERMINAL_LEASE_BUSY_OR_FAILED")
        _NATIVE_LOCAL.depth = depth + 1
        try:
            yield abandoned
        finally:
            _NATIVE_LOCAL.depth = depth
            if acquired:
                api.ReleaseMutex(handle)
                api.CloseHandle(handle)


def _serialize_native_connector(cls):
    skip = {"native_session", "set_execution_destination", "set_runtime_identity",
            "normalize_volume", "get_last_history_range_diagnostics"}
    for name, method in tuple(vars(cls).items()):
        if name.startswith("_") or name in skip or not callable(method):
            continue

        def wrap(function, method_name):
            @wraps(function)
            def call(self, *args, **kwargs):
                global _NATIVE_ACTIVE_CONNECTOR
                mutation = method_name in _NATIVE_MUTATORS
                if mutation and self.execution_destination == "PAPER":
                    return MT5OrderResult(False, 10017, "PAPER_NATIVE_MUTATION_REFUSED", destination="PAPER")
                if mutation and method_name != "send_market_order" and self.runtime_identity is not None:
                    position = args[0] if args else kwargs.get("position")
                    if (getattr(position, "runtime_namespace", None) != self.runtime_identity.namespace
                            or getattr(position, "destination", None) != self.execution_destination):
                        return MT5OrderResult(False, 10017, "NATIVE_POSITION_NAMESPACE_MISMATCH")
                if mutation and method_name == "send_market_order" and self._native_entry_block_reason:
                    return MT5OrderResult(False, 10017, self._native_entry_block_reason)
                try:
                    with self.native_session(
                        require_identity=(mutation or (
                            self.runtime_identity is not None
                            and method_name not in {"connect", "disconnect", "get_account_info"}
                        )),
                        _activate=method_name not in {"connect", "disconnect"},
                    ):
                        if method_name == "disconnect" and _NATIVE_ACTIVE_CONNECTOR is not self:
                            self._connected = False
                            self._symbol_cache.clear()
                            return None
                        if mutation:
                            self._last_native_send_uncertain = False
                        result = function(self, *args, **kwargs)
                        if method_name == "connect":
                            _NATIVE_ACTIVE_CONNECTOR = self if result is True else None
                            if result is not True:
                                self._connected = False
                        elif method_name == "disconnect":
                            _NATIVE_ACTIVE_CONNECTOR = None
                        if mutation and self._last_native_send_uncertain:
                            result.success = False
                            result.retcode = 10012
                            result.retcode_description = self._native_entry_block_reason
                            result.error_message = self._native_entry_block_reason
                            result.ambiguous = True
                        return result
                except NativeSessionError as exc:
                    if method_name == "is_connected":
                        return False
                    if mutation:
                        return MT5OrderResult(False, 10017, str(exc), error_message=str(exc))
                    raise
            return call
        setattr(cls, name, wrap(method, name))
    return cls


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


@_serialize_native_connector
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
        "M1": mt5.TIMEFRAME_M1 if MT5_AVAILABLE else 1,
        "M5": mt5.TIMEFRAME_M5 if MT5_AVAILABLE else 5,
        "M15": mt5.TIMEFRAME_M15 if MT5_AVAILABLE else 15,
        "M30": mt5.TIMEFRAME_M30 if MT5_AVAILABLE else 30,
        "H1": mt5.TIMEFRAME_H1 if MT5_AVAILABLE else 60,
        "H4": mt5.TIMEFRAME_H4 if MT5_AVAILABLE else 240,
        "D1": mt5.TIMEFRAME_D1 if MT5_AVAILABLE else 1440,
        "W1": mt5.TIMEFRAME_W1 if MT5_AVAILABLE else 10080,
        "MN1": mt5.TIMEFRAME_MN1 if MT5_AVAILABLE else 43200,
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
        self._last_history_range_diagnostics: Dict[str, Any] = {}
        self.execution_destination = "BROKER"
        self.runtime_identity = None
        self._native_entry_block_reason = ""
        self._native_binding_failure_reason = ""
        self._native_binding_observed_identity = None
        self._native_binding_observation_sequence = 0
        self._native_binding_failure_observation = None
        self._last_native_send_uncertain = False
        self._last_tick_error = None

    def set_execution_destination(self, destination: str) -> None:
        if destination not in {"BROKER", "PAPER", "MT5_DEMO", "FUNDED"}:
            raise NativeSessionError("EXECUTION_DESTINATION_INVALID")
        if self.execution_destination == "PAPER" and destination != "PAPER":
            raise NativeSessionError("PAPER_DESTINATION_CANNOT_BECOME_BROKER")
        if self.runtime_identity is not None and self.runtime_identity.destination != destination:
            raise NativeSessionError("EXECUTION_DESTINATION_ALREADY_BOUND")
        self.execution_destination = destination

    def set_runtime_identity(self, identity) -> None:
        from pm_live_outcomes import RuntimeAccountIdentity, runtime_account_identity
        if type(identity) is not RuntimeAccountIdentity or identity.destination != self.execution_destination:
            raise NativeSessionError("RUNTIME_IDENTITY_INVALID")
        if self.runtime_identity is not None and self.runtime_identity != identity:
            raise NativeSessionError("RUNTIME_IDENTITY_ALREADY_BOUND")
        with self.native_session():
            actual = mt5.account_info()
            if runtime_account_identity(actual, self.execution_destination) != identity:
                raise NativeSessionError("RUNTIME_IDENTITY_ACCOUNT_MISMATCH")
            self.runtime_identity = identity

    def _observe_native_binding_failure(self, reason, observed_identity):
        self._native_binding_failure_reason = str(reason)
        self._native_binding_observed_identity = (dict(observed_identity) if observed_identity is not None else None)
        self._native_binding_observation_sequence += 1
        sequence = self._native_binding_observation_sequence
        self._native_binding_failure_observation = {
            "sequence": sequence, "reason": str(reason),
            "observed_identity": (dict(observed_identity) if observed_identity is not None else None),
            "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        error = NativeAccountBindingError(str(reason))
        error.native_binding_observation_sequence = sequence
        return error

    @property
    def binding_failure_observation(self):
        value = self._native_binding_failure_observation
        if value is None:
            return None
        return {**value, "observed_identity": (dict(value["observed_identity"])
                                             if value["observed_identity"] is not None else None)}

    def _assert_native_account(self) -> None:
        if not MT5_AVAILABLE or not self._connected:
            self._native_binding_failure_reason = ""
            raise NativeSessionError("NATIVE_ACCOUNT_UNAVAILABLE")
        actual = mt5.account_info()
        if actual is None:
            self._native_binding_failure_reason = ""
            raise NativeSessionError("NATIVE_ACCOUNT_UNAVAILABLE")
        login, server, currency = (getattr(actual, name, None) for name in ("login", "server", "currency"))
        observed_identity = (
            {"login": login, "server": server.strip(), "currency": currency.strip().upper()}
            if type(login) is int and login > 0 and isinstance(server, str) and server.strip()
            and isinstance(currency, str) and currency.strip() else None
        )
        reason = account_identity_block_reason(actual, connector_config=self.config)
        if reason:
            raise self._observe_native_binding_failure(reason, observed_identity)
        if self.runtime_identity is not None:
            from pm_live_outcomes import LiveOutcomeError, runtime_account_identity
            try:
                fresh = runtime_account_identity(actual, self.execution_destination)
            except (ValueError, LiveOutcomeError) as exc:
                raise self._observe_native_binding_failure("NATIVE_ACCOUNT_DESTINATION_MISMATCH", observed_identity) from exc
            if fresh != self.runtime_identity:
                raise self._observe_native_binding_failure("NATIVE_ACCOUNT_NAMESPACE_MISMATCH", observed_identity)
        self._native_binding_failure_reason = ""

    @property
    def binding_failure_reason(self):
        return self._native_binding_failure_reason

    @property
    def binding_failure_observed_identity(self):
        value = self._native_binding_observed_identity
        return dict(value) if value is not None else None

    @contextmanager
    def native_session(self, *, require_identity: bool = True, _activate: bool = True):
        global _NATIVE_ACTIVE_CONNECTOR
        with _native_terminal_lease() as abandoned:
            owner = getattr(_NATIVE_LOCAL, "connector", None)
            if owner is not None and owner is not self:
                raise NativeSessionError("NESTED_NATIVE_ACCOUNT_SWITCH_REFUSED")
            first = owner is None
            if first:
                _NATIVE_LOCAL.connector = self
            try:
                if abandoned:
                    self._native_entry_block_reason = "NATIVE_LEASE_ABANDONED_PENDING_RECONCILIATION"
                if first and _activate and self._connected and _NATIVE_ACTIVE_CONNECTOR is not self:
                    if not self.connect():
                        raise NativeSessionError("NATIVE_ACCOUNT_REBIND_FAILED")
                if require_identity:
                    self._assert_native_account()
                yield
            finally:
                if first:
                    _NATIVE_LOCAL.connector = None

    def _send_native_order(self, request):
        if self.execution_destination == "PAPER":
            raise NativeSessionError("PAPER_NATIVE_MUTATION_REFUSED")
        with self.native_session():
            self._assert_native_account()
            try:
                result = mt5.order_send(request)
            except Exception:
                self._last_native_send_uncertain = True
                self._native_entry_block_reason = "NATIVE_SEND_EXCEPTION_PENDING_RECONCILIATION"
                return None
            if result is None:
                self._last_native_send_uncertain = True
                self._native_entry_block_reason = "NATIVE_SEND_UNKNOWN_PENDING_RECONCILIATION"
            try:
                self._assert_native_account()
            except Exception:
                self._last_native_send_uncertain = True
                self._native_entry_block_reason = "NATIVE_POST_SEND_IDENTITY_MISMATCH_PENDING_RECONCILIATION"
            return result

    def get_last_history_range_diagnostics(self) -> Dict[str, Any]:
        """Return a copy of the most recent range-fetch synchronization facts."""

        return dict(self._last_history_range_diagnostics)

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(
        self,
        login: int = None,
        password: str = None,
        server: str = None,
        path: str = None,
    ) -> bool:
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
            init_kwargs["path"] = path
        if self.config.timeout:
            init_kwargs["timeout"] = self.config.timeout
        if self.config.portable:
            init_kwargs["portable"] = self.config.portable

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

        trade_mode_raw = getattr(info, "trade_mode", None)
        trade_mode = trade_mode_raw if type(trade_mode_raw) is int else -1
        margin_mode_raw = getattr(info, "margin_mode", None)
        margin_mode = margin_mode_raw if type(margin_mode_raw) is int else -1
        hedge_allowed_raw = getattr(info, "hedge_allowed", None)
        # MetaTrader5.account_info() exposes margin_mode but some package
        # versions omit the separate MQL5 ACCOUNT_HEDGE_ALLOWED property.
        # In that case the official accounting-mode contract is authoritative:
        # RETAIL_HEDGING permits independent same-symbol positions; netting and
        # exchange modes do not.
        hedge_allowed = (
            bool(hedge_allowed_raw)
            if hedge_allowed_raw is not None
            else (
                True if margin_mode == 2 else False if margin_mode in {0, 1} else None
            )
        )

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
            trade_expert=info.trade_expert,
            trade_mode=trade_mode,
            margin_mode=margin_mode,
            hedge_allowed=hedge_allowed,
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
            swap_mode=int(
                getattr(info, "swap_mode", -1)
                if getattr(info, "swap_mode", None) is not None
                else -1
            ),
            swap_rollover3days=int(
                getattr(info, "swap_rollover3days", 3)
                if getattr(info, "swap_rollover3days", None) is not None
                else 3
            ),
            swap_sunday=getattr(info, "swap_sunday", None),
            swap_monday=getattr(info, "swap_monday", None),
            swap_tuesday=getattr(info, "swap_tuesday", None),
            swap_wednesday=getattr(info, "swap_wednesday", None),
            swap_thursday=getattr(info, "swap_thursday", None),
            swap_friday=getattr(info, "swap_friday", None),
            swap_saturday=getattr(info, "swap_saturday", None),
            # Even if a future Python binding exposes these values, the
            # production evidence contract requires the nonce/identity/hash
            # checks performed by the MQL5 exporter overlay.
            swap_terms_provenance_status="INCOMPLETE",
            filling_mode=int(getattr(info, "filling_mode", 0) or 0),
            trade_exemode=int(getattr(info, "trade_exemode", 0) or 0),
            trade_freeze_level=int(getattr(info, "trade_freeze_level", 0) or 0),
            chart_mode=getattr(info, "chart_mode", None),
            path=str(getattr(info, "path", "") or ""),
            asset_class=infer_asset_class(
                getattr(info, "name", resolved_symbol),
                broker_data={"path": str(getattr(info, "path", "") or "")},
                default="unknown",
            ),
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
        suffixes = ["", ".a", ".pro", "m", ".std", ".raw", ".ecn", "#", ".i"]

        for suffix in suffixes:
            test_symbol = base_symbol + suffix
            for sym in all_symbols:
                if getattr(sym, "custom", False):
                    continue
                if sym.name.upper() == test_symbol.upper():
                    if mt5.symbol_select(sym.name, True):
                        logger.debug(
                            f"Found broker symbol: {sym.name} for {base_symbol}"
                        )
                        self._broker_symbol_cache[base_symbol] = sym.name
                        self._broker_symbol_cache[sym.name] = sym.name
                        return sym.name
        return None

    def get_tick(self, symbol: str) -> Optional[MT5Tick]:
        """Get current tick for symbol."""
        self._last_tick_error = None
        if not self._check_connection():
            return None

        resolved_symbol = self._resolve_symbol(symbol)
        if resolved_symbol is None:
            return None

        tick = mt5.symbol_info_tick(resolved_symbol)
        if tick is None:
            try:
                diagnostic = mt5.last_error()  # Capture before any later native call.
                if (isinstance(diagnostic, (tuple, list)) and len(diagnostic) == 2
                        and isinstance(diagnostic[0], (int, np.integer))
                        and not isinstance(diagnostic[0], (bool, np.bool_))
                        and type(diagnostic[1]) is str):
                    self._last_tick_error = (int(diagnostic[0]), diagnostic[1])
            except Exception:
                self._last_tick_error = None
            return None

        raw_msc = getattr(tick, "time_msc", None)
        if not hasattr(tick, "time_msc"):
            if (not isinstance(tick.time, (int, np.integer))
                    or isinstance(tick.time, (bool, np.bool_))):
                raise NativeSessionError("NATIVE_TICK_TIME_INVALID")
            raw_msc = int(tick.time) * 1000
        if (not isinstance(raw_msc, (int, np.integer))
                or isinstance(raw_msc, (bool, np.bool_)) or int(raw_msc) <= 0):
            raise NativeSessionError("NATIVE_TICK_TIME_INVALID")
        return MT5Tick(
            time=datetime.fromtimestamp(tick.time, timezone.utc),
            bid=tick.bid,
            ask=tick.ask,
            last=tick.last,
            volume=tick.volume,
            time_msc=int(raw_msc),
        )


    @property
    def last_tick_error(self):
        return self._last_tick_error

    def get_quote_tick_page(self, symbol: str, start_msc: int, end_msc: int) -> MT5QuoteTickPage:
        """Fetch the inclusive millisecond interval, at most 60 seconds, without a count cap."""
        selected = []
        broker_symbol = query_from = query_to = None
        error_code = error_message = None
        raw_count = 0

        def integer(value):
            return isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_))

        safe_symbol = (symbol if type(symbol) is str and 0 < len(symbol) <= 128
            and symbol == symbol.strip() and all(32 <= ord(char) < 127 for char in symbol) else None)
        safe_start = int(start_msc) if integer(start_msc) else None
        safe_end = int(end_msc) if integer(end_msc) else None

        def page(complete, status):
            return MT5QuoteTickPage(safe_symbol, broker_symbol, safe_start, safe_end,
                query_from, query_to, tuple(selected), raw_count, complete, status,
                error_code, error_message)

        def fail(code, cause=None):
            error = MT5QuoteTickPageError(code, page(False, code))
            if cause is not None:
                raise error from cause
            raise error

        if safe_symbol is None:
            fail("QUOTE_TICK_PAGE_SYMBOL_INVALID")
        if (safe_start is None or safe_end is None or safe_start < 0
                or safe_end < safe_start or safe_end - safe_start > 60000):
            fail("QUOTE_TICK_PAGE_BOUNDS_INVALID")
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        try:
            query_from = epoch + timedelta(seconds=safe_start // 1000)
            query_to = epoch + timedelta(seconds=(safe_end + 999) // 1000)
        except (OverflowError, ValueError) as exc:
            fail("QUOTE_TICK_PAGE_BOUNDS_INVALID", exc)
        if not self._check_connection():
            fail("QUOTE_TICK_PAGE_DISCONNECTED")
        try:
            resolved = self._resolve_symbol(symbol)
        except NativeSessionError:
            raise
        except Exception as exc:
            fail("QUOTE_TICK_PAGE_SYMBOL_UNAVAILABLE", exc)
        if (type(resolved) is not str or not 0 < len(resolved) <= 128
                or any(not 32 <= ord(char) < 127 for char in resolved)):
            fail("QUOTE_TICK_PAGE_SYMBOL_UNAVAILABLE")
        broker_symbol = resolved

        native_exception = None
        try:
            raw_ticks = mt5.copy_ticks_range(broker_symbol, query_from, query_to, mt5.COPY_TICKS_ALL)
        except Exception as exc:
            raw_ticks, native_exception = None, exc
        # Capture immediately: even a non-null result can carry a partial-copy error.
        diagnostic_error = None
        try:
            native_error = mt5.last_error()
            if (not isinstance(native_error, (tuple, list)) or len(native_error) != 2
                    or not integer(native_error[0]) or type(native_error[1]) is not str):
                diagnostic_error = "QUOTE_TICK_PAGE_ERROR_DIAGNOSTIC_INVALID"
            else:
                error_code, error_message = int(native_error[0]), native_error[1]
        except Exception as exc:
            diagnostic_error = "QUOTE_TICK_PAGE_ERROR_DIAGNOSTIC_UNAVAILABLE"
            if native_exception is None:
                native_exception = exc
        if raw_ticks is None:
            fail("QUOTE_TICK_PAGE_NATIVE_CALL_FAILED", native_exception)
        if (not isinstance(raw_ticks, (list, tuple, np.ndarray))
                or isinstance(raw_ticks, np.ndarray) and raw_ticks.ndim != 1):
            fail("QUOTE_TICK_PAGE_RESPONSE_INVALID")
        raw_count = len(raw_ticks)
        previous_msc, same_msc_ordinal = None, -1
        for raw_ordinal, row in enumerate(raw_ticks):
            try:
                values = {name: row[name] for name in (
                    "time", "time_msc", "bid", "ask", "last", "flags", "volume", "volume_real")}
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                fail("QUOTE_TICK_PAGE_RECORD_INVALID", exc)
            if (not integer(values["time"]) or not integer(values["time_msc"])
                    or not integer(values["flags"]) or not integer(values["volume"])):
                fail("QUOTE_TICK_PAGE_RECORD_INVALID")
            seconds, milliseconds = int(values["time"]), int(values["time_msc"])
            flags, volume = int(values["flags"]), int(values["volume"])
            if (milliseconds <= 0 or seconds != milliseconds // 1000
                    or not 0 <= flags <= 2**32 - 1 or not 0 <= volume <= 2**64 - 1):
                fail("QUOTE_TICK_PAGE_RECORD_INVALID")
            prices = []
            for name in ("bid", "ask", "last", "volume_real"):
                value = values[name]
                if (not isinstance(value, (int, float, np.integer, np.floating))
                        or isinstance(value, (bool, np.bool_))):
                    fail("QUOTE_TICK_PAGE_RECORD_INVALID")
                try:
                    numeric = float(value)
                except (OverflowError, ValueError, TypeError) as exc:
                    fail("QUOTE_TICK_PAGE_RECORD_INVALID", exc)
                if not math.isfinite(numeric) or numeric < 0:
                    fail("QUOTE_TICK_PAGE_RECORD_INVALID")
                prices.append(numeric)
            bid, ask, last, volume_real = prices
            if bid == 0 or ask == 0:
                fail("QUOTE_TICK_PAGE_ZERO_QUOTE")
            if bid > ask:
                fail("QUOTE_TICK_PAGE_CROSSED_QUOTE")
            if previous_msc is not None and milliseconds < previous_msc:
                fail("QUOTE_TICK_PAGE_ORDER_INVALID")
            same_msc_ordinal = same_msc_ordinal + 1 if milliseconds == previous_msc else 0
            previous_msc = milliseconds
            try:
                when = epoch + timedelta(seconds=seconds,
                    microseconds=(milliseconds % 1000) * 1000)
            except (OverflowError, ValueError) as exc:
                fail("QUOTE_TICK_PAGE_RECORD_INVALID", exc)
            if safe_start <= milliseconds <= safe_end:
                selected.append(MT5QuoteTick(when, milliseconds, same_msc_ordinal,
                    raw_ordinal, bid, ask, last, flags, volume, volume_real))
        if diagnostic_error is not None:
            fail(diagnostic_error, native_exception)
        success_code = getattr(mt5, "RES_S_OK", 1)
        if not integer(success_code) or error_code != int(success_code):
            fail("QUOTE_TICK_PAGE_NATIVE_ERROR", native_exception)
        if native_exception is not None:
            fail("QUOTE_TICK_PAGE_NATIVE_CALL_FAILED", native_exception)
        return page(True, "COMPLETE" if selected else "EMPTY")

    # =========================================================================
    # Historical Data
    # =========================================================================

    def _copy_rates_with_sync(
        self,
        broker_symbol: str,
        tf: Any,
        count: int,
        from_date: Optional[datetime] = None,
        force_full: bool = False,
    ):
        """
        Fetch rates, forcing a server-side history download so the terminal
        returns the FULL requested ``count`` rather than only its local cache.

        Two failure modes are handled, both rooted in the same cause (the
        terminal only returns history it has already downloaded):

        1. Empty result for a symbol just added to Market Watch (stock CFDs and
           exotic metals like Platinum, Palladium, XAUEUR, APPLE, TESLA are not
           in the default watch list). ``copy_rates_from_pos`` reads an empty
           local cache while the terminal downloads in the background.
        2. Short result (e.g. 266k of a requested 300k) because only part of the
           history has been synchronized locally.

        For both, a date-anchored ``copy_rates_from(symbol, tf, now, count)``
        makes the terminal synchronize and return up to ``count`` bars. We poll,
        keeping the largest result, until we reach ``count``, the available
        history plateaus (broker's true maximum), or the attempt budget is spent.

        When the first call already returns enough (the normal case for active FX
        symbols) it returns immediately with no extra work.

        Args:
            force_full: escalate even when the first call returned *some* data but
                fewer than ``count`` bars. Used by the historical fetch path,
                which needs the full window; left off for the live hot path so a
                normal short/empty live probe is not slowed.
        """

        def _len(arr) -> int:
            return 0 if arr is None else len(arr)

        def _rates(anchor: Optional[datetime]):
            # MT5 stores bar open time in UTC; time-based calls must use UTC
            # datetimes (per MetaTrader5 Python docs) or they query the wrong
            # window. copy_rates_from is also a stronger download trigger than
            # copy_rates_from_pos, which only reads the already-charted cache.
            try:
                if anchor is not None and hasattr(mt5, "copy_rates_from"):
                    return mt5.copy_rates_from(broker_symbol, tf, anchor, count)
                return mt5.copy_rates_from_pos(broker_symbol, tf, 0, count)
            except Exception as exc:
                logger.debug(f"copy_rates fetch failed for {broker_symbol}: {exc}")
                return None

        rates = _rates(from_date)
        best = rates
        best_len = _len(rates)
        if best_len >= count:
            return rates
        # Live/default path accepts whatever is cached and only escalates when the
        # cache was completely empty. The historical fetch path (force_full) keeps
        # pushing until it reaches the full requested count.
        if best_len > 0 and not force_full:
            return rates

        max_attempts = max(
            0, int(getattr(self.config, "history_sync_max_attempts", 0) or 0)
        )
        sleep_s = max(
            0.0, float(getattr(self.config, "history_sync_sleep_seconds", 0.0) or 0.0)
        )
        if max_attempts <= 0:
            return best

        # Force the terminal to (re)start / extend the history download. The
        # terminal only serves bars it has loaded on a chart, so a symbol that was
        # never charted has an empty history base until something requests it. We
        # (1) select it into Market Watch, (2) nudge a quote subscription via
        # symbol_info_tick, then (3) poll copy_rates_from with a UTC anchor while
        # the background download runs.
        try:
            mt5.symbol_select(broker_symbol, True)
        except Exception:
            pass

        stagnation = 0
        for _ in range(max_attempts):
            try:
                if hasattr(mt5, "symbol_info_tick"):
                    mt5.symbol_info_tick(broker_symbol)
            except Exception:
                pass
            if sleep_s > 0:
                time.sleep(sleep_s)
            anchor = from_date or datetime.now(timezone.utc)
            attempt_rates = _rates(anchor)
            attempt_len = _len(attempt_rates)
            if attempt_len >= count:
                return attempt_rates
            if attempt_len > best_len:
                best = attempt_rates
                best_len = attempt_len
                stagnation = 0
            else:
                stagnation += 1
                # Stop early only once we already have data and it has stopped
                # growing (the broker's true maximum < count). Keep trying the
                # full budget while still empty, to ride out a slow download.
                if best_len > 0 and stagnation >= 2:
                    break

        if best_len == 0:
            # Surface the real reason instead of a blind "no data". MT5 only
            # reports it through last_error(); without this it is unknowable.
            self._log_history_fetch_failure(broker_symbol, tf)
        return best

    def _log_history_fetch_failure(self, broker_symbol: str, tf: Any) -> None:
        """Log the authoritative reason a history fetch came back empty.

        MetaTrader5 returns None/empty from copy_rates_* on error and only exposes
        the cause via last_error(); symbol_info flags reveal whether the symbol is
        even selected/visible/synchronized on this account.
        """
        details: List[str] = []
        try:
            if hasattr(mt5, "last_error"):
                details.append(f"last_error={mt5.last_error()}")
        except Exception:
            pass
        try:
            info = (
                mt5.symbol_info(broker_symbol) if hasattr(mt5, "symbol_info") else None
            )
            if info is None:
                details.append(
                    "symbol_info=None (symbol not in broker universe / not selectable)"
                )
            else:
                for attr in ("visible", "select", "trade_mode"):
                    if hasattr(info, attr):
                        details.append(f"{attr}={getattr(info, attr)}")
                bars_attr = getattr(info, "bars", None)
                if bars_attr is not None:
                    details.append(f"chart_bars={bars_attr}")
        except Exception as exc:
            details.append(f"symbol_info_error={exc}")
        logger.warning(
            f"History fetch empty for {broker_symbol}: {'; '.join(details) if details else 'no diagnostic available'}. "
            f"Per MT5 docs the terminal only serves bars already loaded on a chart; if last_error is success the "
            f"symbol has no synchronized M5 history on this account (open it once in the terminal or confirm the "
            f"broker offers it)."
        )

    @staticmethod
    def _is_invalid_params_error() -> bool:
        try:
            err = mt5.last_error() if hasattr(mt5, "last_error") else None
        except Exception:
            return False
        return bool(
            err and isinstance(err, (tuple, list)) and len(err) and int(err[0]) == -2
        )

    @staticmethod
    def _rates_to_df(rates) -> Optional[pd.DataFrame]:
        if rates is None or len(rates) == 0:
            return None
        df = pd.DataFrame(rates)
        if df.empty or "time" not in df.columns:
            return None
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.set_index("time")
        return df.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "tick_volume": "Volume",
                "spread": "Spread",
                "real_volume": "RealVolume",
            }
        )

    def _terminal_maxbars(self) -> int:
        try:
            ti = mt5.terminal_info() if hasattr(mt5, "terminal_info") else None
            return int(getattr(ti, "maxbars", 0) or 0)
        except Exception:
            return 0

    def _paged_history(
        self, broker_symbol: str, tf: Any, count: int
    ) -> Optional[pd.DataFrame]:
        """Retrieve up to ``count`` bars by walking the history backwards by date.

        A single oversized request returns (-2, 'Invalid params') because the
        terminal serves at most 'Max bars in chart' (terminal_info().maxbars)
        bars per call. Paging by *date* (anchoring each request at the oldest bar
        retrieved so far) stays under that cap and forces the terminal to fetch
        progressively older windows from the server, accumulating well past the
        per-call limit until ``count`` is reached or the broker's history ends.
        """
        max_attempts = max(
            1, int(getattr(self.config, "history_sync_max_attempts", 6) or 6)
        )
        sleep_s = max(
            0.0, float(getattr(self.config, "history_sync_sleep_seconds", 0.6) or 0.6)
        )
        page = max(1000, int(getattr(self.config, "history_page_size", 50000) or 50000))
        maxbars = self._terminal_maxbars()
        if maxbars > 0:
            page = min(
                page, max(1000, maxbars - 1)
            )  # stay strictly under the per-call cap
        min_page = 500

        try:
            mt5.symbol_select(broker_symbol, True)
            if hasattr(mt5, "symbol_info_tick"):
                mt5.symbol_info_tick(broker_symbol)
        except Exception:
            pass

        frames: List[pd.DataFrame] = []
        total = 0
        anchor = datetime.now(timezone.utc)
        prev_oldest = None
        while total < count:
            n = min(page, count - total)
            chunk = None
            saw_invalid = False
            for _ in range(max_attempts):
                try:
                    chunk = mt5.copy_rates_from(broker_symbol, tf, anchor, n)
                except Exception as exc:
                    logger.debug(
                        f"paged copy_rates_from failed for {broker_symbol}: {exc}"
                    )
                    chunk = None
                if chunk is not None and len(chunk) > 0:
                    break
                if self._is_invalid_params_error():
                    saw_invalid = (
                        True  # window too large for the cap: shrink, don't retry
                    )
                    break
                if sleep_s > 0:
                    time.sleep(sleep_s)
            got = 0 if chunk is None else len(chunk)
            if got == 0:
                if saw_invalid and n > min_page:
                    page = max(min_page, n // 2)
                    continue
                break  # end of available history
            cdf = pd.DataFrame(chunk)
            frames.append(cdf)
            total += got
            oldest = int(cdf["time"].min())
            if prev_oldest is not None and oldest >= prev_oldest:
                break  # no older bars served -> reached start of broker history
            prev_oldest = oldest
            anchor = datetime.fromtimestamp(oldest, tz=timezone.utc) - timedelta(
                seconds=1
            )

        if not frames:
            return None
        df = self._rates_to_df(pd.concat(frames, ignore_index=True))
        if df is None:
            return None
        df = df[~df.index.duplicated(keep="last")].sort_index()
        if count > 0 and len(df) > count:
            df = df.iloc[-count:]
        return df

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        count: int = 1000,
        from_date: datetime = None,
        force_full: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Get historical bars.

        Args:
            symbol: Symbol name
            timeframe: Timeframe string (M5, H1, D1, etc.)
            count: Number of bars to fetch
            from_date: Optional start date
            force_full: keep escalating the history download until the full
                ``count`` is retrieved (historical fetch path). Off by default so
                the live hot path is not slowed by a normal short probe.

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
            logger.debug(f"Broker universe miss while fetching bars: {symbol}")
            return None

        # Historical path pages the request so a count larger than the symbol's
        # locally-synced depth does not trigger MT5 "Invalid params" (-2); the
        # live path keeps the fast single-call + empty-sync behavior.
        if force_full and from_date is None:
            df = self._paged_history(broker_symbol, tf, count)
            if df is None or df.empty:
                logger.warning(f"No data returned for {symbol} {timeframe}")
                self._log_history_fetch_failure(broker_symbol, tf)
                return None
            return df

        rates = self._copy_rates_with_sync(
            broker_symbol, tf, count, from_date=from_date, force_full=force_full
        )

        if rates is None or len(rates) == 0:
            logger.warning(f"No data returned for {symbol} {timeframe}")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.set_index("time")

        # Rename columns
        df = df.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "tick_volume": "Volume",
                "spread": "Spread",
                "real_volume": "RealVolume",
            }
        )

        return df

    def get_bars_range(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        sync_retry: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Get historical bars within date range.

        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            start_date: Start date
            end_date: End date
            sync_retry: When True, an empty or boundary-incomplete result may
                use one bounded synchronization sequence: initial exact read,
                one date-anchored warm call, and one exact reread.  Off by
                default so the live delta-sync hot path is not slowed by a
                normal "no new bars" result.

        Returns:
            DataFrame with OHLCV data or None
        """
        diagnostics: Dict[str, Any] = {
            "range_contract_version": "mt5-half-open-range-v4",
            "symbol": str(symbol),
            "broker_symbol": None,
            "timeframe": str(timeframe).upper(),
            "sync_retry": bool(sync_retry),
            "sync_status": "not_started",
            "request_start_epoch_second": None,
            "request_end_epoch_second_exclusive": None,
            "copy_rates_range_attempts": 0,
            "copy_rates_range_max_attempts": 0,
            "history_download_trigger_attempts": 0,
            "history_download_trigger_nonempty": 0,
            "history_download_trigger_max_bars": 0,
            "history_download_trigger_errors": [],
            "terminal_confirmation_consumed": False,
            "call_ledger_schema": "mt5.range-call-ledger.v1",
            "call_ledger": [],
            "hard_failure": False,
            "hard_failure_call": None,
            "last_error": None,
            "initial_raw_relation": None,
            "confirmation_raw_relation": None,
            "raw_relation": None,
            "raw_count": 0,
            "raw_extent": None,
            "admitted_count": 0,
            "admitted_extent": None,
            "boundary_span_seconds": 0,
            "boundary_slack_seconds": 0,
            "boundary_complete": False,
            "best_extent": None,
        }
        self._last_history_range_diagnostics = diagnostics
        try:
            start_epoch = int(start_date.timestamp())
            end_epoch = int(end_date.timestamp())
        except (AttributeError, OSError, OverflowError, TypeError, ValueError):
            diagnostics["sync_status"] = "invalid_datetime"
            return None
        diagnostics["request_start_epoch_second"] = int(start_epoch)
        diagnostics["request_end_epoch_second_exclusive"] = int(end_epoch)
        if end_epoch <= start_epoch:
            # An invalid half-open interval can never contain admissible bars.
            # Reject it before connection checks or any MT5 range/download call;
            # connector responses outside this contract are not evidence.
            diagnostics["sync_status"] = "invalid_interval"
            return None
        if not self._check_connection():
            diagnostics["sync_status"] = "connection_unavailable"
            return None

        tf = self.TIMEFRAME_MAP.get(timeframe)
        if tf is None:
            diagnostics["sync_status"] = "invalid_timeframe"
            return None

        broker_symbol = self.find_broker_symbol(symbol)
        if broker_symbol is None:
            diagnostics["sync_status"] = "symbol_unavailable"
            return None
        diagnostics["broker_symbol"] = str(broker_symbol)
        diagnostics["copy_rates_range_max_attempts"] = 2 if sync_retry else 1

        tf_seconds = {
            "M1": 60,
            "M5": 5 * 60,
            "M15": 15 * 60,
            "M30": 30 * 60,
            "H1": 60 * 60,
            "H4": 4 * 60 * 60,
            "D1": 24 * 60 * 60,
        }.get(str(timeframe).upper(), 5 * 60)

        def _extent_dict(values: np.ndarray) -> Optional[Dict[str, int]]:
            if values.size <= 0:
                return None
            return {
                "rows": int(values.size),
                "first_epoch_second": int(values.min()),
                "last_epoch_second": int(values.max()),
            }

        def _empty_analysis() -> Dict[str, Any]:
            return {
                "raw_relation": None,
                "raw_count": 0,
                "raw_extent": None,
                "admitted_count": 0,
                "admitted_extent": None,
                "admitted_data": None,
                "parse_error": None,
            }

        def _analyse_candidate(candidate: Any) -> Dict[str, Any]:
            """Classify raw timestamps and admit only the requested half-open range."""
            analysis = _empty_analysis()
            if candidate is None:
                return analysis
            if isinstance(candidate, Mapping):
                rows = 1
                raw_times = [candidate.get("time")]
                row_items = [candidate]
            else:
                try:
                    rows = int(len(candidate))
                except (TypeError, ValueError):
                    analysis.update(
                        raw_relation="MIXED_INVALID",
                        parse_error="response_has_no_bounded_length",
                    )
                    return analysis
                if rows <= 0:
                    return analysis
                try:
                    # MetaTrader structured arrays support this zero-copy path.
                    raw_times = candidate["time"]
                except (TypeError, ValueError, KeyError, IndexError):
                    try:
                        raw_times = [
                            item.get("time")
                            if isinstance(item, Mapping)
                            else getattr(item, "time")
                            for item in candidate
                        ]
                    except (TypeError, ValueError, AttributeError):
                        analysis.update(
                            raw_relation="MIXED_INVALID",
                            raw_count=rows,
                            parse_error="response_time_column_unreadable",
                        )
                        return analysis
                row_items = candidate

            analysis["raw_count"] = rows
            try:
                numeric_times = np.asarray(raw_times, dtype=np.float64).reshape(-1)
                if numeric_times.size != rows:
                    raise ValueError("time_count_mismatch")
                if not np.isfinite(numeric_times).all():
                    raise ValueError("non_finite_time")
                if not np.equal(numeric_times, np.floor(numeric_times)).all():
                    raise ValueError("fractional_epoch_second")
                int64 = np.iinfo(np.int64)
                if (numeric_times < int64.min).any() or (
                    numeric_times > int64.max
                ).any():
                    raise ValueError("epoch_second_out_of_int64_range")
                epochs = numeric_times.astype(np.int64)
            except (TypeError, ValueError, OverflowError) as exc:
                analysis.update(
                    raw_relation="MIXED_INVALID",
                    parse_error=str(exc)[:256] or type(exc).__name__,
                )
                return analysis

            in_range = (epochs >= start_epoch) & (epochs < end_epoch)
            analysis["raw_extent"] = _extent_dict(epochs)
            if bool(in_range.all()):
                relation = "IN_RANGE"
            elif bool((epochs >= end_epoch).all()):
                relation = "RIGHT_DISJOINT_PREFIX"
            elif bool((epochs < start_epoch).all()):
                relation = "LEFT_DISJOINT_SUFFIX"
            else:
                relation = "MIXED_INVALID"
            analysis["raw_relation"] = relation

            admitted_epochs = epochs[in_range]
            analysis["admitted_count"] = int(admitted_epochs.size)
            analysis["admitted_extent"] = _extent_dict(admitted_epochs)
            if admitted_epochs.size <= 0:
                return analysis
            indices = np.flatnonzero(in_range)
            try:
                if isinstance(row_items, pd.DataFrame):
                    admitted = row_items.iloc[indices].copy()
                elif isinstance(row_items, np.ndarray):
                    admitted = row_items[in_range]
                else:
                    admitted = [row_items[int(index)] for index in indices]
            except (IndexError, KeyError, TypeError, ValueError) as exc:
                analysis.update(
                    raw_relation="MIXED_INVALID",
                    admitted_count=0,
                    admitted_extent=None,
                    parse_error=f"response_filter_failed:{type(exc).__name__}"[:256],
                )
                return analysis
            analysis["admitted_data"] = admitted
            return analysis

        def _capture_last_error() -> Dict[str, Any]:
            getter = getattr(mt5, "last_error", None)
            if not callable(getter):
                return {"code": None, "message": "unavailable", "capture_error": None}
            try:
                raw_error = getter()
            except Exception as exc:
                return {
                    "code": None,
                    "message": "unavailable",
                    "capture_error": {
                        "type": type(exc).__name__,
                        "message": str(exc)[:512],
                    },
                }
            code: Optional[int] = None
            message = ""
            if isinstance(raw_error, (tuple, list)):
                if raw_error:
                    try:
                        code = int(raw_error[0])
                    except (TypeError, ValueError, OverflowError):
                        code = None
                if len(raw_error) > 1:
                    message = str(raw_error[1])[:512]
            elif raw_error is not None:
                message = str(raw_error)[:512]
            return {"code": code, "message": message, "capture_error": None}

        def _is_hard_error(last_error: Mapping[str, Any]) -> bool:
            if last_error.get("capture_error") is not None:
                return True
            code = last_error.get("code")
            # -4 (not found) can represent an unsynchronized/empty history page.
            # Every other negative Python-API code is invalid, resource, auth,
            # unsupported, generic, or IPC failure and must terminate the sequence.
            return isinstance(code, int) and code < 0 and code != -4

        call_ledger: List[Dict[str, Any]] = diagnostics["call_ledger"]

        def _invoke_data_call(call_type: str, call) -> Tuple[Dict[str, Any], bool]:
            started = time.perf_counter()
            result = None
            exception: Optional[Dict[str, str]] = None
            try:
                result = call()
            except Exception as exc:
                exception = {
                    "type": type(exc).__name__,
                    "message": str(exc)[:512],
                }
            # MT5 exposes the authoritative failure only via this thread-local
            # value.  Capture it before logging, sleeping, or making another call.
            last_error = _capture_last_error()
            elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
            analysis = _analyse_candidate(result)
            hard_failure = bool(exception is not None or _is_hard_error(last_error))
            entry = {
                "sequence": len(call_ledger) + 1,
                "call_type": call_type,
                "elapsed_ms": round(elapsed_ms, 3),
                "last_error": last_error,
                "exception": exception,
                "hard_failure": hard_failure,
                "raw_relation": analysis["raw_relation"],
                "raw_count": int(analysis["raw_count"]),
                "raw_extent": analysis["raw_extent"],
                "admitted_count": int(analysis["admitted_count"]),
                "admitted_extent": analysis["admitted_extent"],
                "parse_error": analysis["parse_error"],
            }
            # The range contract permits at most two exact reads and one warm.
            # This guard keeps the diagnostic bounded even if future code regresses.
            if len(call_ledger) < 3:
                call_ledger.append(entry)
            else:  # pragma: no cover - defensive invariant
                raise RuntimeError("MT5 range call ledger exceeded its fixed bound")
            return analysis, hard_failure

        def _boundary_geometry() -> Tuple[int, int]:
            try:
                start_epoch = int(start_date.timestamp())
                end_epoch = int(end_date.timestamp())
            except (AttributeError, OSError, OverflowError, ValueError):
                return 0, 0
            span = max(0, end_epoch - start_epoch)
            if span <= 0:
                return span, 0
            # Weekend/holiday and exchange-session gaps are not missing data.
            # Four calendar days covers normal weekend boundaries; slower bars
            # receive at least four nominal periods of slack.  The tolerance on
            # each edge must never overlap the other edge, however: otherwise
            # one cached bar can falsely prove both boundaries on a short
            # request and suppress the MT5 download trigger.
            base_slack = max(4 * 86400, 4 * int(tf_seconds))
            nonoverlap_budget = span - int(tf_seconds)
            if nonoverlap_budget <= 0:
                return span, 0
            nonoverlap_cap = max(0, (nonoverlap_budget - 1) // 2)
            return span, min(base_slack, nonoverlap_cap)

        def _boundary_complete(extent: Optional[Mapping[str, int]]) -> bool:
            if extent is None:
                return False
            span, slack = _boundary_geometry()
            if span <= 0:
                return False
            first_epoch = int(extent["first_epoch_second"])
            last_epoch = int(extent["last_epoch_second"])
            return bool(
                first_epoch <= start_epoch + slack
                and last_epoch >= end_epoch - int(tf_seconds) - slack
            )

        trigger_attempts = 0
        trigger_nonempty = 0
        trigger_max_bars = 0
        trigger_errors: List[Dict[str, str]] = []

        def _trigger_history_download() -> Tuple[Dict[str, Any], bool]:
            """Prompt MT5 to build/download the exact range before re-reading it.

            ``copy_rates_range`` can repeatedly expose the same incomplete local
            cache even though the terminal can load the requested history.  A
            bounded date-anchored ``copy_rates_from`` call is MT5's stronger
            synchronization trigger.  Its returned rows are deliberately not
            used as evidence; the subsequent exact range read remains the only
            result admitted by this method.
            """
            nonlocal trigger_attempts, trigger_nonempty, trigger_max_bars
            trigger = getattr(mt5, "copy_rates_from", None)
            if not callable(trigger):
                return _empty_analysis(), False
            try:
                span_seconds = max(0.0, float((end_date - start_date).total_seconds()))
            except (AttributeError, OverflowError, TypeError, ValueError):
                span_seconds = 0.0
            requested_bars = max(1, int(math.ceil(span_seconds / tf_seconds)) + 2)
            try:
                configured_cap = int(
                    getattr(self.config, "history_page_size", 50_000) or 50_000
                )
            except (TypeError, ValueError, OverflowError):
                configured_cap = 50_000
            # Resource ceiling only: acquisition pages are intentionally below
            # this value, while an accidental multi-decade intraday range cannot
            # allocate an unbounded trigger array.
            trigger_count = min(requested_bars, max(1, min(configured_cap, 50_000)))
            trigger_attempts += 1
            trigger_max_bars = max(trigger_max_bars, trigger_count)
            analysis, hard_failure = _invoke_data_call(
                "copy_rates_from_warm",
                lambda: trigger(broker_symbol, tf, end_date, trigger_count),
            )
            if analysis["raw_count"] > 0:
                trigger_nonempty += 1
            entry = call_ledger[-1]
            if entry["exception"] is not None:
                trigger_errors.append(dict(entry["exception"]))
            elif hard_failure:
                last_error = entry["last_error"]
                trigger_errors.append(
                    {
                        "type": "MT5LastError",
                        "message": (
                            f"{last_error.get('code')}: {last_error.get('message', '')}"
                        )[:512],
                    }
                )
            return analysis, hard_failure

        initial, hard_failure = _invoke_data_call(
            "copy_rates_range_initial",
            lambda: mt5.copy_rates_range(broker_symbol, tf, start_date, end_date),
        )
        exact_analyses = [initial]
        best = initial if initial["admitted_count"] > 0 else None
        hard_failure_call = "copy_rates_range_initial" if hard_failure else None

        # A wholly newer terminal prefix is the observed MT5 sentinel defect:
        # it is neither requested data nor evidence of broker absence.  The
        # historical path gets exactly one confirmation sequence; live callers
        # remain single-read because sync_retry is false there.
        initial_right_disjoint = initial["raw_relation"] == "RIGHT_DISJOINT_PREFIX"
        invalid_initial = initial["parse_error"] is not None
        should_retry = bool(
            sync_retry
            and not hard_failure
            and not invalid_initial
            and not _boundary_complete(initial["admitted_extent"])
        )
        terminal_confirmation_consumed = False

        if should_retry:
            terminal_confirmation_consumed = bool(initial_right_disjoint)
            warm, warm_hard_failure = _trigger_history_download()
            if warm["parse_error"] is not None:
                warm_hard_failure = True
            if warm_hard_failure:
                hard_failure = True
                hard_failure_call = "copy_rates_from_warm"
            else:
                sleep_s = max(
                    0.0,
                    float(
                        getattr(self.config, "history_sync_sleep_seconds", 0.0) or 0.0
                    ),
                )
                if sleep_s > 0:
                    time.sleep(sleep_s)
                retry, retry_hard_failure = _invoke_data_call(
                    "copy_rates_range_retry",
                    lambda: mt5.copy_rates_range(
                        broker_symbol, tf, start_date, end_date
                    ),
                )
                exact_analyses.append(retry)
                if retry_hard_failure or retry["parse_error"] is not None:
                    hard_failure = True
                    hard_failure_call = "copy_rates_range_retry"
                if retry["admitted_count"] > 0:
                    if best is None:
                        best = retry
                    else:
                        best_extent = best["admitted_extent"]
                        retry_extent = retry["admitted_extent"]
                        best_score = (
                            int(best_extent["last_epoch_second"])
                            - int(best_extent["first_epoch_second"]),
                            int(best["admitted_count"]),
                        )
                        retry_score = (
                            int(retry_extent["last_epoch_second"])
                            - int(retry_extent["first_epoch_second"]),
                            int(retry["admitted_count"]),
                        )
                        if retry_score > best_score:
                            best = retry

        selected = best
        if selected is None:
            selected = next(
                (
                    item
                    for item in reversed(exact_analyses)
                    if item["raw_relation"] is not None
                ),
                exact_analyses[-1],
            )
        rates = selected["admitted_data"] if best is not None else None
        final_extent = selected["admitted_extent"] if best is not None else None
        boundary_span_seconds, boundary_slack_seconds = _boundary_geometry()
        exact_attempts = sum(
            entry["call_type"].startswith("copy_rates_range") for entry in call_ledger
        )
        exact_relations = [item["raw_relation"] for item in exact_analyses]
        if hard_failure:
            sync_status = "hard_mt5_failure"
        elif best is None and initial_right_disjoint:
            sync_status = "terminal_prefix_unsynchronized"
        elif best is None and "LEFT_DISJOINT_SUFFIX" in exact_relations:
            sync_status = "terminal_suffix_unsynchronized"
        elif best is None and "RIGHT_DISJOINT_PREFIX" in exact_relations:
            sync_status = "terminal_prefix_unsynchronized"
        elif best is None and "MIXED_INVALID" in exact_relations:
            sync_status = "mixed_invalid_no_admissible_data"
        elif best is None:
            sync_status = "empty_unsynchronized" if sync_retry else "empty"
        elif _boundary_complete(final_extent):
            sync_status = "boundary_complete"
        elif selected["raw_relation"] == "MIXED_INVALID":
            sync_status = "mixed_invalid_filtered"
        else:
            sync_status = "boundary_incomplete"

        diagnostics.update(
            {
                "sync_status": sync_status,
                "copy_rates_range_attempts": int(exact_attempts),
                "history_download_trigger_attempts": int(trigger_attempts),
                "history_download_trigger_nonempty": int(trigger_nonempty),
                "history_download_trigger_max_bars": int(trigger_max_bars),
                "history_download_trigger_errors": list(trigger_errors),
                "terminal_confirmation_consumed": bool(terminal_confirmation_consumed),
                "hard_failure": bool(hard_failure),
                "hard_failure_call": hard_failure_call,
                "last_error": (
                    dict(call_ledger[-1]["last_error"]) if call_ledger else None
                ),
                "initial_raw_relation": initial["raw_relation"],
                "confirmation_raw_relation": (
                    exact_analyses[1]["raw_relation"]
                    if len(exact_analyses) > 1
                    else None
                ),
                "raw_relation": selected["raw_relation"],
                "raw_count": int(selected["raw_count"]),
                "raw_extent": selected["raw_extent"],
                "admitted_count": int(selected["admitted_count"]),
                "admitted_extent": final_extent,
                "boundary_span_seconds": int(boundary_span_seconds),
                "boundary_slack_seconds": int(boundary_slack_seconds),
                "boundary_complete": bool(_boundary_complete(final_extent)),
                # Backward-compatible name consumed by native acquisition.  Under
                # V4 it is deliberately the admitted, never the raw, extent.
                "best_extent": final_extent,
            }
        )

        if rates is None or len(rates) == 0:
            logger.warning(
                "MT5 range fetch returned no admissible bars for %s: status=%s "
                "request=[%s,%s) raw_relation=%s raw_extent=%s",
                broker_symbol,
                sync_status,
                start_epoch,
                end_epoch,
                selected["raw_relation"],
                selected["raw_extent"],
            )
            return None

        df = pd.DataFrame(rates)
        if df.empty or "time" not in df.columns:
            diagnostics["sync_status"] = "invalid_filtered_response"
            diagnostics["hard_failure"] = True
            diagnostics["hard_failure_call"] = "dataframe_conversion"
            return None
        # Defense in depth: candidate admission already enforces this mask, but
        # never trust container indexing behavior to weaken the half-open bound.
        numeric_time = pd.to_numeric(df["time"], errors="coerce")
        valid = numeric_time.ge(start_epoch) & numeric_time.lt(end_epoch)
        df = df.loc[valid].copy()
        if df.empty:
            diagnostics["sync_status"] = "invalid_filtered_response"
            diagnostics["hard_failure"] = True
            diagnostics["hard_failure_call"] = "dataframe_half_open_guard"
            diagnostics["admitted_count"] = 0
            diagnostics["admitted_extent"] = None
            diagnostics["best_extent"] = None
            diagnostics["boundary_complete"] = False
            return None
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.set_index("time")
        df = df.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "tick_volume": "Volume",
                "spread": "Spread",
                "real_volume": "RealVolume",
            }
        )
        df.attrs["mt5_history_range_diagnostics"] = dict(
            self._last_history_range_diagnostics
        )

        return df

    # =========================================================================
    # Order Execution
    # =========================================================================

    def send_market_order(
        self,
        symbol: str,
        order_type: OrderType,
        volume: float,
        sl: float = 0.0,
        tp: float = 0.0,
        deviation: int = 30,
        magic: int = 0,
        comment: str = "",
        price: Optional[float] = None,
        symbol_info: Optional[MT5SymbolInfo] = None,
    ) -> MT5OrderResult:
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
            return MT5OrderResult(
                False, -1, f"Symbol info not available: {broker_symbol}"
            )

        # A requested volume is a risk ceiling, never a target to round up to.
        # Reject volumes that cannot be represented at or above the broker
        # minimum before fetching a tick, preflighting, or submitting an order.
        requested_volume = volume
        volume = self._normalize_volume(requested_volume, symbol_info)
        if volume is None:
            description = (
                f"Invalid volume {requested_volume!r}: no broker-valid volume "
                f"at or below the request (min={symbol_info.volume_min}, "
                f"max={symbol_info.volume_max}, step={symbol_info.volume_step})"
            )
            logger.warning("Rejected %s market order: %s", broker_symbol, description)
            return MT5OrderResult(False, 10014, description)

        mt5_type = (
            mt5.ORDER_TYPE_BUY if order_type == OrderType.BUY else mt5.ORDER_TYPE_SELL
        )
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
                return MT5OrderResult(
                    False, 10015, f"Invalid price override: {price!r}"
                )
            if price <= 0.0:
                return MT5OrderResult(
                    False, 10015, f"Invalid price override: {price!r}"
                )

        if order_type == OrderType.BUY:
            mt5_type = mt5.ORDER_TYPE_BUY
        else:
            mt5_type = mt5.ORDER_TYPE_SELL

        # Determine filling type
        filling_type = self._get_filling_type(broker_symbol, symbol_info=symbol_info)

        # Validate SL/TP against minimum stop level
        min_stop_distance = symbol_info.trade_stops_level * symbol_info.point

        if sl > 0:
            sl_distance = abs(price - sl)
            if sl_distance < min_stop_distance and min_stop_distance > 0:
                return MT5OrderResult(
                    False,
                    10016,
                    f"SL too close: {sl_distance:.5f} < min {min_stop_distance:.5f} "
                    f"(stops_level={symbol_info.trade_stops_level}, point={symbol_info.point})",
                )

        if tp > 0:
            tp_distance = abs(price - tp)
            if tp_distance < min_stop_distance and min_stop_distance > 0:
                return MT5OrderResult(
                    False,
                    10016,
                    f"TP too close: {tp_distance:.5f} < min {min_stop_distance:.5f} "
                    f"(stops_level={symbol_info.trade_stops_level}, point={symbol_info.point})",
                )

        # Validate SL/TP are on correct side of entry price
        if order_type == OrderType.BUY:
            if sl > 0 and sl >= price:
                return MT5OrderResult(
                    False,
                    10016,
                    f"SL ({sl:.5f}) must be below entry ({price:.5f}) for BUY",
                )
            if tp > 0 and tp <= price:
                return MT5OrderResult(
                    False,
                    10016,
                    f"TP ({tp:.5f}) must be above entry ({price:.5f}) for BUY",
                )
        else:  # SELL
            if sl > 0 and sl <= price:
                return MT5OrderResult(
                    False,
                    10016,
                    f"SL ({sl:.5f}) must be above entry ({price:.5f}) for SELL",
                )
            if tp > 0 and tp >= price:
                return MT5OrderResult(
                    False,
                    10016,
                    f"TP ({tp:.5f}) must be below entry ({price:.5f}) for SELL",
                )

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
        result = self._send_native_order(request)

        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]), ambiguous=True)

        retcode_desc = RETCODE_DESCRIPTIONS.get(
            result.retcode, f"Unknown ({result.retcode})"
        )

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
            price=result_price,
        )

    def close_position(
        self,
        position: MT5Position,
        deviation: int = 30,
        magic: int = 0,
        comment: str = "",
    ) -> MT5OrderResult:
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

        result = self._send_native_order(request)

        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))

        retcode_desc = RETCODE_DESCRIPTIONS.get(
            result.retcode, f"Unknown ({result.retcode})"
        )

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
            price=result_price,
        )

    def close_position_partial(
        self,
        position: MT5Position,
        percent: float = 50.0,
        deviation: int = 30,
        magic: int = 0,
        comment: str = "",
    ) -> MT5OrderResult:
        """Close a percentage of an open position volume."""
        if not self._check_connection():
            return MT5OrderResult(False, -1, "Not connected")

        try:
            pct = max(0.0, min(100.0, float(percent)))
            current_volume = float(position.volume)
        except (TypeError, ValueError):
            return MT5OrderResult(False, -1, "Invalid partial close volume")
        if pct <= 0.0 or current_volume <= 0.0:
            return MT5OrderResult(
                False, -1, "Partial close percent/volume must be positive"
            )

        symbol_info = self.get_symbol_info(position.symbol)
        if symbol_info is None:
            return MT5OrderResult(
                False, -1, f"Symbol info not available: {position.symbol}"
            )

        if pct >= 99.999:
            return self.close_position(
                position,
                deviation=deviation,
                magic=magic,
                comment=comment or "Partial close full",
            )

        raw_target_volume = current_volume * pct / 100.0
        step = float(symbol_info.volume_step or 0.01)
        if step <= 0:
            step = 0.01
        target_volume = math.floor((raw_target_volume / step) + 1e-12) * step
        precision = max(
            self._volume_decimal_places(step),
            self._volume_decimal_places(symbol_info.volume_min),
        )
        target_volume = round(target_volume, precision)
        min_volume = float(symbol_info.volume_min)
        if target_volume < min_volume:
            return MT5OrderResult(
                False, -1, "Partial close below broker minimum volume"
            )
        remaining = current_volume - target_volume
        if remaining < min_volume:
            return MT5OrderResult(
                False,
                -1,
                "Partial close would leave remainder below broker minimum volume",
            )
        if target_volume <= 0.0:
            return MT5OrderResult(False, -1, "Partial close normalized to zero")

        tick = self.get_tick(position.symbol)
        if tick is None:
            return MT5OrderResult(False, -1, "No tick data")

        if position.type == 0:
            close_type = mt5.ORDER_TYPE_SELL
            price = tick.bid
        else:
            close_type = mt5.ORDER_TYPE_BUY
            price = tick.ask

        filling_type = self._get_filling_type(position.symbol, symbol_info=symbol_info)
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": position.symbol,
            "volume": target_volume,
            "type": close_type,
            "position": position.ticket,
            "price": price,
            "deviation": deviation,
            "magic": magic or position.magic,
            "comment": comment or "Partial close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }

        preflight = self._preflight_order_request(request)
        if preflight is not None:
            return preflight

        result = self._send_native_order(request)
        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]), ambiguous=True)

        retcode_desc = RETCODE_DESCRIPTIONS.get(
            result.retcode, f"Unknown ({result.retcode})"
        )
        result_price = float(getattr(result, "price", 0.0) or 0.0)
        if result_price <= 0.0:
            result_price = float(price)

        return MT5OrderResult(
            success=(result.retcode in [10008, 10009, 10010]),
            retcode=result.retcode,
            retcode_description=retcode_desc,
            deal=result.deal,
            order=result.order,
            volume=target_volume,
            price=result_price,
        )

    def modify_position(
        self, position: MT5Position, sl: float = None, tp: float = None
    ) -> MT5OrderResult:
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
            return MT5OrderResult(
                False, -1, f"Symbol info not available: {position.symbol}"
            )
        tick = self.get_tick(position.symbol)
        if tick is None:
            return MT5OrderResult(False, -1, "No tick data")

        is_long = position.type == 0
        reference_price = tick.bid if is_long else tick.ask
        min_stop_distance = float(symbol_info.trade_stops_level or 0) * float(
            symbol_info.point
        )
        freeze_distance = float(symbol_info.trade_freeze_level or 0) * float(
            symbol_info.point
        )
        required_distance = max(min_stop_distance, freeze_distance)

        new_sl = round(sl, symbol_info.digits) if sl is not None else position.sl
        new_tp = round(tp, symbol_info.digits) if tp is not None else position.tp

        if new_sl:
            if is_long and new_sl >= reference_price:
                return MT5OrderResult(
                    False, 10016, "SL must be below current price for BUY"
                )
            if (not is_long) and new_sl <= reference_price:
                return MT5OrderResult(
                    False, 10016, "SL must be above current price for SELL"
                )
            if (
                required_distance > 0
                and abs(reference_price - new_sl) < required_distance
            ):
                return MT5OrderResult(False, 10016, "SL violates stop/freeze distance")
        if new_tp:
            if is_long and new_tp <= reference_price:
                return MT5OrderResult(
                    False, 10016, "TP must be above current price for BUY"
                )
            if (not is_long) and new_tp >= reference_price:
                return MT5OrderResult(
                    False, 10016, "TP must be below current price for SELL"
                )
            if (
                required_distance > 0
                and abs(reference_price - new_tp) < required_distance
            ):
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

        result = self._send_native_order(request)

        if result is None:
            error = mt5.last_error()
            return MT5OrderResult(False, error[0], str(error[1]))

        return MT5OrderResult(
            success=(result.retcode in [10008, 10009, 10010]),
            retcode=result.retcode,
            retcode_description=RETCODE_DESCRIPTIONS.get(result.retcode, "Unknown"),
        )

    # =========================================================================
    # Position Management
    # =========================================================================

    def get_positions(
        self, symbol: str = None, magic: int = None
    ) -> Optional[List[MT5Position]]:
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

            time_update_raw = getattr(pos, "time_update", None)
            result.append(
                MT5Position(
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
                    time=datetime.fromtimestamp(pos.time, timezone.utc),
                    identifier=int(getattr(pos, "identifier", 0) or 0),
                    reason=int(getattr(pos, "reason", 0) or 0),
                    time_update=datetime.fromtimestamp(time_update_raw, timezone.utc)
                    if time_update_raw
                    else None,
                    destination=("BROKER" if self.execution_destination == "PAPER" else self.execution_destination),
                    runtime_namespace=(self.runtime_identity.namespace if self.runtime_identity is not None else ""),
                )
            )

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

    def get_position_by_symbol_magic(
        self, symbol: str, magic: int
    ) -> Optional[MT5Position]:
        """Get position by symbol and magic number."""
        positions = self.get_positions(symbol=symbol, magic=magic)
        if positions is None:
            return None
        return positions[0] if positions else None

    def count_positions(self, symbol: str = None, magic: int = None) -> int:
        """Count open positions."""
        positions = self.get_positions(symbol=symbol, magic=magic)
        return len(positions) if positions is not None else 0

    def get_position_opening_metadata(
        self, position_identifier: int
    ) -> Optional[Dict[str, Any]]:
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
                entry_type = getattr(mt5, "DEAL_ENTRY_IN", 0)
                for deal in deals:
                    if getattr(deal, "entry", None) == entry_type:
                        return {
                            "comment": getattr(deal, "comment", "") or "",
                            "magic": int(getattr(deal, "magic", 0) or 0),
                            "reason": int(getattr(deal, "reason", 0) or 0),
                        }
                # Fallback: first deal if no explicit ENTRY_IN found
                deal = deals[0]
                return {
                    "comment": getattr(deal, "comment", "") or "",
                    "magic": int(getattr(deal, "magic", 0) or 0),
                    "reason": int(getattr(deal, "reason", 0) or 0),
                }
        except Exception:
            pass

        try:
            # Fallback to orders — opening order may have the original comment
            orders = mt5.history_orders_get(position=position_identifier)
            if orders:
                order = orders[0]
                return {
                    "comment": getattr(order, "comment", "") or "",
                    "magic": int(getattr(order, "magic", 0) or 0),
                    "reason": int(getattr(order, "reason", 0) or 0),
                }
        except Exception:
            pass

        return None

    def get_position_history(
        self, position_identifier: int
    ) -> Optional[Dict[str, Any]]:
        """Return complete broker deals/orders for one stable MT5 position id.

        This is read-only accounting evidence. ``history_complete`` is true
        only when both MT5 history queries succeeded; downstream close
        accounting must otherwise retain the position in its pending outbox.
        """
        identifier = int(position_identifier or 0)
        if identifier <= 0 or not self._check_connection():
            return None
        try:
            raw_deals = mt5.history_deals_get(position=identifier)
            raw_orders = mt5.history_orders_get(position=identifier)
        except Exception:
            return None
        if raw_deals is None or raw_orders is None:
            return {
                "history_complete": False,
                "position_id": identifier,
                "deals": [],
                "orders": [],
            }

        deals: List[Dict[str, Any]] = []
        for deal in raw_deals:
            deals.append(
                {
                    "ticket": int(getattr(deal, "ticket", 0) or 0),
                    "order_ticket": int(getattr(deal, "order", 0) or 0),
                    "position_id": int(
                        getattr(deal, "position_id", identifier) or identifier
                    ),
                    "symbol": str(getattr(deal, "symbol", "") or ""),
                    "entry": getattr(deal, "entry", None),
                    "type": getattr(deal, "type", None),
                    "volume": float(getattr(deal, "volume", 0.0) or 0.0),
                    "price": float(getattr(deal, "price", 0.0) or 0.0),
                    "profit": float(getattr(deal, "profit", 0.0) or 0.0),
                    "commission": float(getattr(deal, "commission", 0.0) or 0.0),
                    "swap": float(getattr(deal, "swap", 0.0) or 0.0),
                    "fee": float(getattr(deal, "fee", 0.0) or 0.0),
                    "magic": int(getattr(deal, "magic", 0) or 0),
                    "comment": str(getattr(deal, "comment", "") or ""),
                    "reason": getattr(deal, "reason", None),
                    "time_msc": getattr(deal, "time_msc", None),
                    "time": getattr(deal, "time", None),
                }
            )

        orders: List[Dict[str, Any]] = []
        for order in raw_orders:
            orders.append(
                {
                    "ticket": int(getattr(order, "ticket", 0) or 0),
                    "position_id": int(
                        getattr(order, "position_id", 0)
                        or getattr(order, "position_by_id", 0)
                        or identifier
                    ),
                    "time_setup_msc": getattr(order, "time_setup_msc", None),
                    "time_setup": getattr(order, "time_setup", None),
                    "price_open": float(getattr(order, "price_open", 0.0) or 0.0),
                    "volume_initial": float(
                        getattr(order, "volume_initial", 0.0) or 0.0
                    ),
                    "initial_stop": float(getattr(order, "sl", 0.0) or 0.0),
                    "initial_take_profit": float(getattr(order, "tp", 0.0) or 0.0),
                    "magic": int(getattr(order, "magic", 0) or 0),
                    "comment": str(getattr(order, "comment", "") or ""),
                    "type": getattr(order, "type", None),
                }
            )
        return {
            "history_complete": True,
            "position_id": identifier,
            "deals": deals,
            "orders": orders,
        }

    def get_recent_closing_deals(
        self, lookback_hours: int = 48
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch recent closing deals with full accounting fields.

        ``None`` means broker history could not be obtained; an empty list means
        the query succeeded and found no closing deals.  Risk controls rely on
        that distinction and must not translate a transport failure into zero
        realized loss.
        """
        if not self._check_connection():
            return None

        to_time = datetime.now(timezone.utc)
        from_time = to_time - timedelta(hours=max(1, int(lookback_hours)))

        try:
            deals = mt5.history_deals_get(from_time, to_time)
        except Exception:
            return None

        if deals is None:
            return None

        result: List[Dict[str, Any]] = []
        exit_entries = {
            getattr(mt5, "DEAL_ENTRY_OUT", 1),
            getattr(mt5, "DEAL_ENTRY_OUT_BY", 3),
        }
        for deal in deals:
            entry_type = getattr(deal, "entry", None)
            if entry_type is not None and entry_type not in exit_entries:
                continue
            deal_time = getattr(deal, "time", None)
            result.append(
                {
                    "ticket": int(getattr(deal, "ticket", 0) or 0),
                    "order_ticket": int(getattr(deal, "order", 0) or 0),
                    "position_id": int(getattr(deal, "position_id", 0) or 0),
                    "symbol": getattr(deal, "symbol", ""),
                    "profit": float(getattr(deal, "profit", 0.0) or 0.0),
                    "commission": float(getattr(deal, "commission", 0.0) or 0.0),
                    "swap": float(getattr(deal, "swap", 0.0) or 0.0),
                    "fee": float(getattr(deal, "fee", 0.0) or 0.0),
                    "volume": float(getattr(deal, "volume", 0.0) or 0.0),
                    "price": float(getattr(deal, "price", 0.0) or 0.0),
                    "entry": entry_type,
                    "entry_kind": (
                        "OUT_BY"
                        if entry_type == getattr(mt5, "DEAL_ENTRY_OUT_BY", 3)
                        else "OUT"
                    ),
                    "type": getattr(deal, "type", None),
                    "comment": getattr(deal, "comment", "") or "",
                    "magic": int(getattr(deal, "magic", 0) or 0),
                    "reason": getattr(deal, "reason", None),
                    "time_msc": getattr(deal, "time_msc", None),
                    "time": datetime.fromtimestamp(deal_time, tz=timezone.utc)
                    if deal_time
                    else to_time,
                }
            )
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

    def _normalize_volume(
        self,
        volume: float,
        symbol_info: MT5SymbolInfo,
    ) -> Optional[float]:
        """Floor a risk ceiling to the broker grid, or return ``None``.

        Normalization must never increase requested exposure.  In particular,
        a positive request below ``volume_min`` is not silently promoted to the
        minimum lot: it is an invalid/no-order result.
        """
        try:
            requested = float(volume)
            min_volume = float(symbol_info.volume_min)
            max_volume = float(symbol_info.volume_max)
            step = float(symbol_info.volume_step)
        except (TypeError, ValueError, OverflowError):
            return None

        if not math.isfinite(requested) or requested <= 0.0:
            return None
        if (
            not math.isfinite(min_volume)
            or not math.isfinite(max_volume)
            or min_volume < 0.0
            or max_volume <= 0.0
            or max_volume < min_volume
        ):
            return None
        if not math.isfinite(step) or step <= 0.0:
            return None

        try:
            requested_decimal = Decimal(str(requested))
            min_decimal = Decimal(str(min_volume))
            max_decimal = Decimal(str(max_volume))
            step_decimal = Decimal(str(step))
            bounded_decimal = min(requested_decimal, max_decimal)
            step_count = (bounded_decimal / step_decimal).to_integral_value(
                rounding=ROUND_FLOOR
            )
            normalized_decimal = step_count * step_decimal
        except (InvalidOperation, ValueError, OverflowError, ZeroDivisionError):
            return None

        if normalized_decimal <= 0 or normalized_decimal < min_decimal:
            return None

        precision = max(
            self._volume_decimal_places(step),
            self._volume_decimal_places(min_volume),
        )
        normalized = round(float(normalized_decimal), precision)

        # Decimal arithmetic establishes the risk-safe invariant.  Retain a
        # final float-domain guard for unusual broker precision metadata.
        upper_bound = min(requested, max_volume)
        if normalized > upper_bound:
            normalized_decimal -= step_decimal
            if normalized_decimal <= 0 or normalized_decimal < min_decimal:
                return None
            normalized = round(float(normalized_decimal), precision)
        return normalized

    @staticmethod
    def _volume_decimal_places(value: float) -> int:
        text = f"{float(value):.10f}".rstrip("0").rstrip(".")
        if "." not in text:
            return 0
        return min(8, len(text.rsplit(".", 1)[1]))

    def normalize_volume(
        self,
        volume: float,
        symbol_info: MT5SymbolInfo,
    ) -> Optional[float]:
        """Public wrapper for risk-safe volume normalization."""
        return self._normalize_volume(volume, symbol_info)

    def order_calc_profit(
        self,
        order_type: int,
        symbol: str,
        volume: float,
        price_open: float,
        price_close: float,
    ) -> Optional[float]:
        """Wrapper around MT5 order_calc_profit (returns profit in deposit currency)."""
        if not MT5_AVAILABLE:
            return None
        try:
            resolved_symbol = self._resolve_symbol(symbol) or symbol
            return mt5.order_calc_profit(
                order_type, resolved_symbol, volume, price_open, price_close
            )
        except Exception as e:
            logger.debug(f"order_calc_profit failed for {symbol}: {e}")
            return None

    def calc_loss_amount(
        self,
        order_type: int,
        symbol: str,
        volume: float,
        entry_price: float,
        stop_price: float,
    ) -> Optional[float]:
        """Compute absolute loss at stop (deposit currency) using MT5 contract math."""
        profit = self.order_calc_profit(
            order_type, symbol, volume, entry_price, stop_price
        )
        if profit is None:
            return None
        return abs(float(profit))

    def calc_margin_required(
        self, order_type: int, symbol: str, volume: float, price: float
    ) -> Optional[float]:
        """Estimate required margin for a proposed market order."""
        if not MT5_AVAILABLE or not hasattr(mt5, "order_calc_margin"):
            return None
        try:
            resolved_symbol = self._resolve_symbol(symbol) or symbol
            if int(order_type) == int(getattr(OrderType, "BUY").value):
                mt5_type = mt5.ORDER_TYPE_BUY
            else:
                mt5_type = mt5.ORDER_TYPE_SELL
            margin = mt5.order_calc_margin(
                mt5_type, resolved_symbol, float(volume), float(price)
            )
        except Exception as e:
            logger.debug(f"order_calc_margin failed for {symbol}: {e}")
            return None
        if margin is None:
            return None
        try:
            return abs(float(margin))
        except (TypeError, ValueError):
            return None

    def _preflight_order_request(
        self, request: Dict[str, Any]
    ) -> Optional[MT5OrderResult]:
        """Validate an MT5 trade request with order_check when available."""
        if self.execution_destination == "PAPER":
            return MT5OrderResult(False, 10017, "PAPER_NATIVE_MUTATION_REFUSED", destination="PAPER")
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
        description = getattr(result, "comment", "") or RETCODE_DESCRIPTIONS.get(
            retcode, f"Order check failed ({retcode})"
        )
        return MT5OrderResult(False, retcode, str(description))

    def _get_filling_type(
        self, symbol: str, symbol_info: Optional[MT5SymbolInfo] = None
    ) -> int:
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

        policy = (
            str(getattr(self.config, "preferred_filling_type", "broker") or "broker")
            .strip()
            .lower()
        )
        if policy in {"fok", "ioc", "return"}:
            override_map = {
                "fok": mt5.ORDER_FILLING_FOK,
                "ioc": mt5.ORDER_FILLING_IOC,
                "return": mt5.ORDER_FILLING_RETURN,
            }
            requested = override_map[policy]
            if (
                requested == mt5.ORDER_FILLING_RETURN
                and trade_exemode == market_execution
            ):
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

    def save_broker_specs_to_json(
        self, symbols: List[str], filepath: str = "broker_specs.json"
    ) -> bool:
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
                "symbol": symbol,
                "broker_symbol": broker_symbol,
                "asset_class": info.asset_class,
                "path": info.path,
                "digits": info.digits,
                "point": info.point,
                "tick_size": info.trade_tick_size,
                "tick_value": info.trade_tick_value,
                "contract_size": info.trade_contract_size,
                "volume_min": info.volume_min,
                "volume_max": info.volume_max,
                "volume_step": info.volume_step,
                "spread": info.spread,
                "spread_float": info.spread_float,
                "swap_long": info.swap_long,
                "swap_short": info.swap_short,
                "swap_mode": info.swap_mode,
                "swap_rollover3days": info.swap_rollover3days,
                "swap_sunday": info.swap_sunday,
                "swap_monday": info.swap_monday,
                "swap_tuesday": info.swap_tuesday,
                "swap_wednesday": info.swap_wednesday,
                "swap_thursday": info.swap_thursday,
                "swap_friday": info.swap_friday,
                "swap_saturday": info.swap_saturday,
                "swap_terms_provenance_status": info.swap_terms_provenance_status,
                "stops_level": info.trade_stops_level,
                "pip_size": info.pip_size,
                "pip_value": info.pip_value,
            }

        try:
            with open(filepath, "w") as f:
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
    "MT5_AVAILABLE",
    "MT5Config",
    "MT5SymbolInfo",
    "MT5AccountInfo",
    "MT5Tick",
    "MT5Position",
    "MT5OrderResult",
    "OrderType",
    "FillingType",
    "MT5Connector",
    "MT5Connection",
    "RETCODE_DESCRIPTIONS",
]


class PaperExecutionConnector:
    """Observed MT5 data with local execution in the existing outcome ledger."""

    _READ_METHODS = frozenset({
        "connect", "disconnect", "is_connected", "get_last_error", "get_symbol_info",
        "find_broker_symbol", "get_tick", "get_symbol_tick", "get_bars", "get_bars_range",
        "get_last_history_range_diagnostics", "normalize_volume", "order_calc_profit",
        "calc_loss_amount", "calc_margin_required", "save_broker_specs_to_json", "get_quote_tick_page",
    })

    def __init__(self, data_connector, outcome_ledger, runtime_identity):
        from pm_live_outcomes import RuntimeAccountIdentity
        if (type(runtime_identity) is not RuntimeAccountIdentity
                or runtime_identity.destination != "PAPER"
                or outcome_ledger.runtime_identity != runtime_identity):
            raise NativeSessionError("PAPER_RUNTIME_IDENTITY_INVALID")
        data_connector.set_execution_destination("PAPER")
        data_connector.set_runtime_identity(runtime_identity)
        self._data = data_connector
        self._ledger = outcome_ledger
        self.runtime_identity = runtime_identity
        self.execution_destination = "PAPER"
        self.config = data_connector.config
        self._paper_entry_block_reason = ""
        self._paper_symbol_errors = {}
        self._paper_poll_cache = None
        self._quote_audit_capture = {}
        self._m1_mismatch_observed = {}

        self._paper_binding_ack_sequence = 0
        self._paper_pending_binding_incident = None
        try:
            with self._data.native_session():
                pass
        except NativeAccountBindingError:
            self._consume_native_binding_observation()
            raise
        observation = getattr(self._data, "binding_failure_observation", None)
        if isinstance(observation, dict) and type(observation.get("sequence")) is int:
            self._paper_binding_ack_sequence = observation["sequence"]

    def __getattr__(self, name):
        if name in self._READ_METHODS:
            method = getattr(self._data, name)
            if name in {"connect", "disconnect", "get_last_error", "get_last_history_range_diagnostics", "normalize_volume"}:
                return method
            @wraps(method)
            def observed_read(*args, **kwargs):
                with self.native_session():
                    return method(*args, **kwargs)
            return observed_read
        raise AttributeError(name)

    @property
    def binding_failure_reason(self):
        self._consume_native_binding_observation()
        reason = self._data.binding_failure_reason
        if reason:
            self._paper_entry_block_reason = "PAPER_ACCOUNT_IDENTITY_MISMATCH"
        return reason

    def is_connected(self):
        self._consume_native_binding_observation()
        connected = self._data.is_connected()
        self._consume_native_binding_observation()
        if self._data.binding_failure_reason:
            self._paper_entry_block_reason = "PAPER_ACCOUNT_IDENTITY_MISMATCH"
        return connected

    def _consume_native_binding_observation(self):
        from pm_live_outcomes import LiveOutcomeError
        # At most a previously failed save and the newest native observation.
        for _ in range(2):
            pending = self._paper_pending_binding_incident
            if pending is None:
                observation = getattr(self._data, "binding_failure_observation", None)
                if (not isinstance(observation, dict) or type(observation.get("sequence")) is not int
                        or observation["sequence"] <= self._paper_binding_ack_sequence):
                    return
                pending = dict(observation)
                self._paper_pending_binding_incident = pending
            self._paper_entry_block_reason = "PAPER_ACCOUNT_IDENTITY_MISMATCH"
            try:
                self._ledger.record_paper_binding_incident(
                    observed_identity=pending["observed_identity"],
                    bound_identity={name: getattr(self.runtime_identity, name)
                                    for name in ("login", "server", "currency", "destination")},
                    reason=pending["reason"], observed_at_utc=pending["observed_at_utc"])
            except (LiveOutcomeError, OSError, ValueError, TypeError):
                logging.getLogger("fxpm.paper").exception(
                    "PAPER identity incident could not be persisted; entries remain blocked")
                return
            if pending["sequence"] is not None:
                self._paper_binding_ack_sequence = pending["sequence"]
            self._paper_pending_binding_incident = None

    def _latch_binding_failure(self, reason):
        self._paper_entry_block_reason = "PAPER_ACCOUNT_IDENTITY_MISMATCH"
        if self._paper_pending_binding_incident is None:
            self._paper_pending_binding_incident = {
                "sequence": None, "reason": str(reason), "observed_identity": None,
                "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
        self._consume_native_binding_observation()

    @contextmanager
    def native_session(self):
        self._consume_native_binding_observation()
        try:
            with self._data.native_session():
                yield
        except NativeAccountBindingError as exc:
            self._paper_entry_block_reason = "PAPER_ACCOUNT_IDENTITY_MISMATCH"
            self._consume_native_binding_observation()
            if (getattr(exc, "native_binding_observation_sequence", None) is None
                    and not getattr(exc, "paper_binding_incident_observed", False)):
                exc.paper_binding_incident_observed = True
                self._latch_binding_failure(exc)
            raise
        finally:
            self._consume_native_binding_observation()

    @staticmethod
    def _utc(value):
        if not isinstance(value, datetime):
            raise NativeSessionError("PAPER_QUOTE_TIME_INVALID")
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        if value.utcoffset() != timedelta(0):
            raise NativeSessionError("PAPER_NON_UTC_SOURCE_TIME_REFUSED")
        return value

    @staticmethod
    def _number(value, name, *, positive=False):
        if isinstance(value, bool):
            raise NativeSessionError("PAPER_INVALID_" + name)
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise NativeSessionError("PAPER_INVALID_" + name) from exc
        if not math.isfinite(result) or (positive and result <= 0):
            raise NativeSessionError("PAPER_INVALID_" + name)
        return result

    def _tick_unavailable_error(self, code):
        error = NativeSessionError(code)
        diagnostic = getattr(self._data, "last_tick_error", None)
        if diagnostic is not None:
            error.native_error_code, error.native_error_message = diagnostic
        return error

    def _retain_unknown_upper_failure(self, row, error):
        audit = row.get("quote_audit")
        if (getattr(error, "native_error_code", None) == -4 and audit is not None
                and audit["pending_minute_msc"] is not None):
            code = getattr(error, "code", None) or str(error).split(":", 1)[0]
            self._audit_failure(row, audit["pending_minute_msc"], code, "RETRYABLE",
                                overdue=audit["overdue"])

    def _quote(self, symbol):
        cache = self._paper_poll_cache
        if cache is not None and symbol in cache["quotes"]:
            info, tick, _ = cache["quotes"][symbol]
        else:
            info = self._data.get_symbol_info(symbol)
            tick = self._data.get_tick(symbol)
        if tick is None:
            raise self._tick_unavailable_error("PAPER_OBSERVED_QUOTE_UNAVAILABLE")
        if info is None:
            raise NativeSessionError("PAPER_OBSERVED_QUOTE_UNAVAILABLE")
        bid = self._number(tick.bid, "BID", positive=True)
        ask = self._number(tick.ask, "ASK", positive=True)
        if ask < bid:
            raise NativeSessionError("PAPER_CROSSED_QUOTE_REFUSED")
        return info, tick, bid, ask, self._utc(tick.time)

    @contextmanager
    def _poll_snapshot(self):
        previous = self._paper_poll_cache
        self._paper_poll_cache = {"quotes": {}, "pages": {}, "m1": {}}
        try:
            yield
        finally:
            self._paper_poll_cache = previous

    def _tick_page(self, symbol, start_msc, end_msc):
        cache = self._paper_poll_cache
        key = symbol, start_msc, end_msc
        if cache is not None and key in cache["pages"]:
            return cache["pages"][key]
        page = self._data.get_quote_tick_page(symbol, start_msc, end_msc)
        if not page.complete or page.native_error_code != 1:
            raise MT5QuoteTickPageError("PAPER_TICK_HISTORY_INCOMPLETE", page)
        if cache is not None:
            cache["pages"][key] = page
        return page

    @staticmethod
    def _minute_signature(ticks, count=None):
        import hashlib
        import json
        if count is None:
            count = len(ticks)
        if count > len(ticks):
            raise NativeSessionError("PAPER_TICK_HISTORY_CURSOR_UNAVAILABLE")
        digest = hashlib.sha256()
        for tick in ticks[:count]:
            value = {name: getattr(tick, name) for name in (
                "time_msc", "same_msc_ordinal", "bid", "ask", "last", "flags", "volume", "volume_real")}
            digest.update(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n")
        return count, digest.hexdigest()

    def _initial_quote_audit(self, symbol):
        from pm_live_outcomes import PAPER_QUOTE_AUDIT_SCHEMA
        minute, count, digest = self._quote_audit_capture[symbol]
        return {"schema_version": PAPER_QUOTE_AUDIT_SCHEMA,
                "pending_minute_msc": minute, "processed_count": count, "prefix_sha256": digest,
                "corroborated_minute_count": 0, "last_corroborated_minute_msc": None,
                "first_failure_minute_msc": None, "first_failure_code": None, "first_failure_status": None,
                "status": "PENDING", "overdue": False,
                "terminal_failure_minute_msc": None, "terminal_failure_code": None}

    @staticmethod
    def _tick_cursor_records(ticks):
        """Hash each same-ms native prefix; query-relative raw ordinals are excluded."""
        import hashlib
        import json
        previous_ms, digest = None, None
        for tick in ticks:
            if tick.time_msc != previous_ms:
                digest = hashlib.sha256()
                previous_ms = tick.time_msc
            value = {name: getattr(tick, name) for name in (
                "time_msc", "same_msc_ordinal", "bid", "ask", "last", "flags", "volume", "volume_real")}
            digest.update(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n")
            yield tick, {"time_msc": tick.time_msc, "same_msc_ordinal": tick.same_msc_ordinal,
                         "prefix_sha256": digest.hexdigest()}

    @staticmethod
    def _execution_evidence(tick, event):
        from pm_live_outcomes import PAPER_FILL_POLICY_ID
        return {"model_id": PAPER_FILL_POLICY_ID, "bid": tick.bid, "ask": tick.ask,
                "ask_source": "NATIVE", "event": event}

    def _quote_record(self, symbol):
        """Capture one actual native history record, including its duplicate-ms rank."""
        cache = self._paper_poll_cache
        if cache is not None and symbol in cache["quotes"]:
            return cache["quotes"][symbol]
        info, latest, _, _, _ = self._quote(symbol)
        if type(getattr(info, "chart_mode", None)) is not int or info.chart_mode != 0:
            raise NativeSessionError("PAPER_TRIGGER_BASIS_UNSUPPORTED")
        milliseconds = getattr(latest, "time_msc", None)
        if type(milliseconds) is not int or milliseconds < 100_000_000_000:
            raise NativeSessionError("PAPER_NATIVE_TICK_MILLISECONDS_UNAVAILABLE")
        page = self._tick_page(symbol, (milliseconds // 60000) * 60000, milliseconds)
        if not page.complete or not page.ticks or page.ticks[-1].time_msc != milliseconds:
            raise NativeSessionError("PAPER_ENTRY_TICK_RECORD_UNAVAILABLE")
        # The page record, not an earlier symbol_info_tick snapshot, is the fill
        # observation. Later same-ms appends are consumed on the inclusive resume.
        tick, cursor = list(self._tick_cursor_records(page.ticks))[-1]
        count, digest = self._minute_signature(page.ticks)
        self._quote_audit_capture[symbol] = (milliseconds // 60000 * 60000, count, digest)
        result = info, tick, cursor
        if cache is not None:
            cache["quotes"][symbol] = result
        return result

    def _paper_row(self, identifier):
        return next((row for row in self._ledger.paper_positions()
                     if int(row["position_id"]) == int(identifier)), None)

    def get_account_info(self):
        with self.native_session():
            actual = self._data.get_account_info()
        reason = account_identity_block_reason(actual, connector_config=self.config)
        if reason:
            self._latch_binding_failure(reason)
            raise NativeAccountBindingError(reason)
        identity = self.runtime_identity
        if (int(actual.login) != identity.login
                or str(actual.server).strip().casefold() != identity.server.casefold()
                or str(actual.currency).strip().upper() != identity.currency):
            self._latch_binding_failure("PAPER_ACCOUNT_NAMESPACE_MISMATCH")
            raise NativeAccountBindingError("PAPER_ACCOUNT_NAMESPACE_MISMATCH")
        return actual

    def get_observed_broker_positions(self, symbol=None, magic=None):
        with self.native_session():
            result = self._data.get_positions(symbol=symbol, magic=magic)
            if result is None:
                raise NativeSessionError("PAPER_OBSERVED_POSITIONS_UNAVAILABLE")
            for position in result:
                if getattr(position, "destination", "BROKER") == "PAPER":
                    raise NativeSessionError("PAPER_BROKER_POSITION_ALIAS_REFUSED")
            return result

    def _terms_for_entry(self, symbol, info, when):
        import hashlib
        import json
        provider = getattr(self._data, "paper_funding_terms_provider", None)
        terms = provider(symbol, observed_at_utc=when) if callable(provider) else None
        mode = getattr(info, "swap_mode", -1)
        long_rate = self._number(getattr(info, "swap_long", None), "SWAP_LONG")
        short_rate = self._number(getattr(info, "swap_short", None), "SWAP_SHORT")
        if terms is None and (mode == 0 or (type(mode) is int and mode in range(1, 9)
                                          and long_rate == 0 and short_rate == 0)):
            public = {"symbol": symbol, "swap_mode": int(mode),
                      "swap_long": long_rate, "swap_short": short_rate}
            digest = hashlib.sha256(json.dumps(public, sort_keys=True).encode()).hexdigest()
            terms = {
                "schema_version": "fxpm.paper-funding-terms.v1", **public,
                "currency": self.runtime_identity.currency, "rollover_timezone": "UTC",
                "source_ref": "OBSERVED_MT5_DISABLED_OR_ZERO_SWAP",
                "observed_at": when.isoformat().replace("+00:00", "Z"), "provenance_sha256": digest,
            }
        if not isinstance(terms, Mapping):
            raise NativeSessionError("PAPER_FUNDING_TERMS_UNAVAILABLE")
        if (terms.get("schema_version") != "fxpm.paper-funding-terms.v1"
                or terms.get("rollover_timezone") != "UTC"
                or not terms.get("source_ref") or not terms.get("provenance_sha256")
                or str(terms.get("currency", "")).upper() != self.runtime_identity.currency):
            raise NativeSessionError("PAPER_FUNDING_TERMS_INVALID")
        packed = json.loads(json.dumps(dict(terms), allow_nan=False))
        self._funding_cash({"funding_terms": packed, "time_msc": int(when.timestamp() * 1000),
                            "type": 0, "volume": 1.0, "symbol": symbol}, when, float(info.point))
        return packed

    def _funding_cash(self, row, when, point):
        from pm_broker_terms import project_session_events
        terms = row.get("funding_terms")
        if not isinstance(terms, Mapping):
            raise NativeSessionError("PAPER_FUNDING_TERMS_UNAVAILABLE")
        mode = terms.get("swap_mode")
        long_rate = self._number(terms.get("swap_long"), "SWAP_LONG")
        short_rate = self._number(terms.get("swap_short"), "SWAP_SHORT")
        if mode == 0 or (long_rate == 0 and short_rate == 0):
            return 0.0
        if mode not in (1, 4):
            raise NativeSessionError("PAPER_FUNDING_MODE_UNSUPPORTED")
        try:
            start = datetime.fromisoformat(str(terms["coverage_start_utc"]).replace("Z", "+00:00"))
            end = datetime.fromisoformat(str(terms["coverage_end_utc"]).replace("Z", "+00:00"))
            entry = datetime.fromtimestamp(int(row["time_msc"]) / 1000, timezone.utc)
            if (start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0)
                    or not start <= entry <= when <= end
                    or not isinstance(terms["rollover_schedule"], list)):
                raise ValueError("coverage")
            if when <= entry:
                return 0.0
            frame = pd.DataFrame({"_BAR_CLOSE_TS": [when]}, index=pd.DatetimeIndex([entry]))
            projected = project_session_events(terms["rollover_schedule"], frame)
            units = int(projected.pre_open_units.sum() + projected.at_close_units.sum())
        except (KeyError, TypeError, ValueError) as exc:
            raise NativeSessionError("PAPER_FUNDING_COVERAGE_UNAVAILABLE") from exc
        if units == 0:
            return 0.0
        rate = long_rate if int(row["type"]) == 0 else short_rate
        cash_per_lot = rate
        if mode == 1:
            price = float(row.get("price_open", 1.0))
            value = self._data.order_calc_profit(0, row["symbol"], 1.0, price, price + point)
            cash_per_lot *= abs(self._number(value, "SWAP_POINT_VALUE", positive=True))
        return units * float(row["volume"]) * cash_per_lot

    def _mark(self, row):
        info, tick, bid, ask, when = self._quote(row["symbol"])
        price = bid if int(row["type"]) == 0 else ask
        value = self._data.order_calc_profit(
            int(row["type"]), row["symbol"], float(row["volume"]), float(row["price_open"]), price,
        )
        gross = self._number(value, "NATIVE_PROFIT")
        swap = self._funding_cash(row, when, float(info.point))
        return info, when, price, gross, swap

    def _position(self, row, mark):
        _, when, price, gross, swap = mark
        return MT5Position(
            ticket=int(row["ticket"]), identifier=int(row["position_id"]),
            symbol=str(row["symbol"]), type=int(row["type"]), volume=float(row["volume"]),
            price_open=float(row["price_open"]), price_current=price,
            sl=float(row["sl"]), tp=float(row["tp"]), swap=swap, profit=gross,
            magic=int(row["magic"]), comment=str(row["comment"]),
            time=datetime.fromtimestamp(int(row["time_msc"]) / 1000, timezone.utc),
            time_update=when, destination="PAPER", runtime_namespace=self.runtime_identity.namespace,
            paper_position_id=str(row["paper_position_id"]),
        )

    @staticmethod
    def _cursor_order(cursor):
        return cursor["time_msc"], cursor["same_msc_ordinal"]

    def _completed_m1(self, symbol, start_msc, end_msc):
        """Corroborate closed bid bars; this does not attest complete tick history."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        start, end = (start_msc // 60000) * 60000, (end_msc // 60000) * 60000
        if end <= start:
            return {}
        cache = self._paper_poll_cache
        if cache is not None:
            missing = [minute for minute in range(start, end, 60000) if (symbol, minute) not in cache["m1"]]
            if not missing:
                return {minute: cache["m1"][(symbol, minute)] for minute in range(start, end, 60000)
                        if cache["m1"][(symbol, minute)] is not None}
            fetch_start, fetch_end = min(missing), max(missing) + 60000
        else:
            fetch_start, fetch_end = start, end
        frame = self._data.get_bars_range(symbol, "M1", epoch + timedelta(milliseconds=fetch_start),
                                           epoch + timedelta(milliseconds=fetch_end))
        diagnostics = self._data.get_last_history_range_diagnostics()
        captured = diagnostics.get("last_error") if isinstance(diagnostics, dict) else None
        if ((isinstance(diagnostics, dict) and diagnostics.get("hard_failure"))
                or (isinstance(captured, dict) and captured.get("code") == -4)):
            error = NativeSessionError("PAPER_M1_SOURCE_UNAVAILABLE")
            if isinstance(captured, dict):
                error.native_error_code = captured.get("code")
                error.native_error_message = captured.get("message")
            raise error
        result = {}
        if frame is None:
            if cache is not None:
                for minute in range(fetch_start, fetch_end, 60000):
                    cache["m1"][(symbol, minute)] = None
                return {minute: cache["m1"][(symbol, minute)] for minute in range(start, end, 60000)
                        if cache["m1"][(symbol, minute)] is not None}
            return result
        if not isinstance(frame, pd.DataFrame) or not {"Open", "High", "Low", "Close"}.issubset(frame):
            raise NativeSessionError("PAPER_M1_SOURCE_UNAVAILABLE")
        for index, bar in frame.iterrows():
            when = self._utc(index.to_pydatetime() if isinstance(index, pd.Timestamp) else index)
            delta = when - epoch
            milliseconds = (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000
            if milliseconds % 60000 or milliseconds in result or not start <= milliseconds < end:
                raise NativeSessionError("PAPER_M1_SOURCE_UNAVAILABLE")
            result[milliseconds] = tuple(self._number(bar[name], "M1_" + name.upper(), positive=True)
                                         for name in ("Open", "High", "Low", "Close"))
        if cache is not None:
            for minute in range(fetch_start, fetch_end, 60000):
                cache["m1"][(symbol, minute)] = result.get(minute)
            return {minute: cache["m1"][(symbol, minute)] for minute in range(start, end, 60000)
                    if cache["m1"][(symbol, minute)] is not None}
        return result

    def _verify_m1_page(self, page, minute, upper_msc, observed, point):
        if minute + 60000 > (upper_msc // 60000) * 60000:
            return  # Current minute is explicitly provisional native-response data.
        bar = observed.get(minute)
        if not page.ticks:
            if bar is not None:
                raise NativeSessionError("PAPER_M1_TICK_MISMATCH")
            return
        if bar is None:
            raise NativeSessionError("PAPER_M1_SOURCE_UNAVAILABLE")
        prices = [tick.bid for tick in page.ticks]
        actual = (prices[0], max(prices), min(prices), prices[-1])
        if any(abs(left - right) > max(float(point) / 2, 1e-12) for left, right in zip(actual, bar)):
            raise NativeSessionError("PAPER_M1_TICK_MISMATCH")

    def _audit_failure(self, row, minute, code, status, *, overdue=True):
        audit = row.get("quote_audit")
        if audit is None or audit["status"] in {"DIVERGENT", "UNAVAILABLE"}:
            return row
        proposed = dict(audit)
        proposed["overdue"] = overdue
        if proposed["first_failure_minute_msc"] is None:
            proposed.update(first_failure_minute_msc=minute, first_failure_code=code,
                            first_failure_status=status)
        if status in {"DIVERGENT", "UNAVAILABLE"}:
            proposed.update(status=status, terminal_failure_minute_msc=minute, terminal_failure_code=code)
        else:
            proposed["status"] = "PENDING"
        return self._ledger.paper_update_quote_audit(int(row["position_id"]),
                    expected_audit=audit, quote_audit=proposed)

    def _audit_prefix(self, row, upper_tick):
        audit = row.get("quote_audit")
        if audit is None:
            raise NativeSessionError("PAPER_QUOTE_AUDIT_MISSING")
        if audit["status"] in {"DIVERGENT", "UNAVAILABLE"}:
            raise NativeSessionError("PAPER_QUOTE_AUDIT_" + audit["status"])
        minute = audit["pending_minute_msc"]
        if minute is None:
            return row
        if upper_tick.time_msc < minute:
            raise NativeSessionError("PAPER_TICK_HISTORY_TIME_REVERSED")
        page = None
        try:
            page = self._tick_page(row["symbol"], minute, min(minute + 59999, upper_tick.time_msc))
            if len(page.ticks) < audit["processed_count"]:
                raise NativeSessionError("PAPER_REQUIRED_PREFIX_UNAVAILABLE")
            _, prefix = self._minute_signature(page.ticks, audit["processed_count"])
            if prefix != audit["prefix_sha256"]:
                raise NativeSessionError("PAPER_MINUTE_PREFIX_CHANGED")
            count, digest = self._minute_signature(page.ticks)
            proposed = {**audit, "processed_count": count, "prefix_sha256": digest}
            row = self._ledger.paper_update_quote_audit(int(row["position_id"]),
                        expected_audit=audit, quote_audit=proposed)
            if minute + 60000 <= upper_tick.time_msc // 60000 * 60000:
                observed = self._completed_m1(row["symbol"], minute, upper_tick.time_msc)
                info = self._data.get_symbol_info(row["symbol"])
                if info is None:
                    raise NativeSessionError("PAPER_SYMBOL_INFO_UNAVAILABLE")
                self._verify_m1_page(page, minute, upper_tick.time_msc, observed, info.point)
            return row
        except NativeAccountBindingError:
            raise
        except NativeSessionError as exc:
            if getattr(exc, "page", None) is None and page is not None:
                exc.page = page
            self._audit_observed_error(row, minute, upper_tick.time_msc, exc)
            raise

    def _audit_observed_error(self, row, minute, upper_msc, error):
        code = getattr(error, "code", None) or str(error).split(":", 1)[0]
        if not code.startswith(("PAPER_", "QUOTE_TICK_PAGE_")):
            return
        status = "RETRYABLE"
        native_page = getattr(error, "page", None)
        native_code = getattr(error, "native_error_code", None)
        if native_code is None and native_page is not None:
            native_code = native_page.native_error_code
        complete_ok = (isinstance(native_page, MT5QuoteTickPage) and native_page.complete
                       and native_page.native_error_code == 1)
        if native_code == -4:
            status = "RETRYABLE"  # An API absence/error does not prove permanent history loss.
        elif complete_ok and code in {"PAPER_MINUTE_PREFIX_CHANGED", "PAPER_TICK_HISTORY_PREFIX_CHANGED"}:
            status = "DIVERGENT"
        elif complete_ok and code in {"PAPER_REQUIRED_PREFIX_UNAVAILABLE", "PAPER_TICK_HISTORY_CURSOR_UNAVAILABLE"}:
            status = "UNAVAILABLE"
        elif code == "PAPER_M1_TICK_MISMATCH":
            key = int(row["position_id"]), minute
            first_seen = self._m1_mismatch_observed.setdefault(key, upper_msc // 60000 * 60000)
            if upper_msc // 60000 * 60000 >= first_seen + 60000:
                status = "DIVERGENT"
        self._audit_failure(row, minute, code, status, overdue=upper_msc // 60000 * 60000 > minute)

    def _audit_complete(self, row, upper_tick):
        """Corroborate only the position's actual span; never infer a trade from bar extrema."""
        audit = row.get("quote_audit")
        if audit is None:
            raise NativeSessionError("PAPER_QUOTE_AUDIT_MISSING")
        if audit["status"] in {"DIVERGENT", "UNAVAILABLE"}:
            raise NativeSessionError("PAPER_QUOTE_AUDIT_" + audit["status"])
        frozen = row["status"] == "CLOSED" or row["last_execution_evidence"]["event"] in {"SL", "TP"}
        end = row["quote_cursor"]["time_msc"] if frozen else upper_tick.time_msc
        final_minute = end // 60000 * 60000
        first_minute = row["time_msc"] // 60000 * 60000
        minute = first_minute + audit["corroborated_minute_count"] * 60000
        proposed = dict(audit)
        observed = self._completed_m1(row["symbol"], minute, upper_tick.time_msc)
        info = self._data.get_symbol_info(row["symbol"])
        if info is None:
            raise NativeSessionError("PAPER_SYMBOL_INFO_UNAVAILABLE")
        while minute <= final_minute:
            page = None
            try:
                page = self._tick_page(row["symbol"], minute, min(minute + 59999, upper_tick.time_msc))
                if minute == audit["pending_minute_msc"]:
                    if len(page.ticks) < audit["processed_count"]:
                        raise NativeSessionError("PAPER_REQUIRED_PREFIX_UNAVAILABLE")
                    if self._minute_signature(page.ticks, audit["processed_count"])[1] != audit["prefix_sha256"]:
                        raise NativeSessionError("PAPER_MINUTE_PREFIX_CHANGED")
                if minute + 60000 > upper_tick.time_msc // 60000 * 60000:
                    count, digest = self._minute_signature(page.ticks)
                    proposed.update(pending_minute_msc=minute, processed_count=count, prefix_sha256=digest,
                                    status="PENDING", overdue=False)
                    break
                self._verify_m1_page(page, minute, upper_tick.time_msc, observed, info.point)
                proposed.update(corroborated_minute_count=proposed["corroborated_minute_count"] + 1,
                                last_corroborated_minute_msc=minute, pending_minute_msc=None,
                                processed_count=0, prefix_sha256=None, overdue=False)
                self._m1_mismatch_observed.pop((int(row["position_id"]), minute), None)
                minute += 60000
            except NativeAccountBindingError:
                raise
            except NativeSessionError as exc:
                if getattr(exc, "page", None) is None and page is not None:
                    exc.page = page
                # Retain the verified contiguous prefix in one bounded state update.
                if proposed["pending_minute_msc"] != minute:
                    import hashlib
                    proposed.update(pending_minute_msc=minute, processed_count=0,
                                    prefix_sha256=hashlib.sha256(b"").hexdigest())
                row = self._ledger.paper_update_quote_audit(int(row["position_id"]),
                            expected_audit=audit, quote_audit=proposed)
                self._audit_observed_error(row, minute, upper_tick.time_msc, exc)
                raise
        else:
            proposed.update(pending_minute_msc=None, processed_count=0, prefix_sha256=None,
                            status="CLEAN" if row["status"] == "CLOSED" else "PENDING", overdue=False)
        return self._ledger.paper_update_quote_audit(int(row["position_id"]),
                    expected_audit=audit, quote_audit=proposed)

    def _close_row(self, row, volume, price, when, *, operation_id="", quote_cursor=None,
                   execution_evidence=None, observed_time_msc=None):
        gross = self._number(self._data.order_calc_profit(
            int(row["type"]), row["symbol"], volume, float(row["price_open"]), price,
        ), "NATIVE_PROFIT")
        info = self._data.get_symbol_info(row["symbol"])
        if info is None:
            raise NativeSessionError("PAPER_SYMBOL_INFO_UNAVAILABLE")
        swap = self._funding_cash({**row, "volume": volume}, when, float(info.point))
        kwargs = {}
        if quote_cursor is not None or execution_evidence is not None:
            kwargs = {"expected_cursor": row["quote_cursor"], "quote_cursor": quote_cursor,
                      "execution_evidence": execution_evidence}
        return self._ledger.paper_close_position(
            int(row["position_id"]), volume, price, gross, swap,
            (quote_cursor["time_msc"] if quote_cursor is not None else observed_time_msc
             if observed_time_msc is not None else int(when.timestamp() * 1000)),
            operation_id=operation_id, **kwargs,
        )

    def _close_trigger(self, row):
        cursor, evidence = row["quote_cursor"], row["last_execution_evidence"]
        side_quote = evidence["bid"] if int(row["type"]) == 0 else evidence["ask"]
        price = side_quote if evidence["event"] == "SL" else float(row["tp"])
        when = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=cursor["time_msc"])
        self._close_row(row, float(row["volume"]), price, when, quote_cursor=cursor,
                        execution_evidence=evidence,
                        operation_id=f"protective:{row['position_id']}:{evidence['event']}:{cursor['time_msc']}:{cursor['same_msc_ordinal']}")

    def _drain_row(self, row, upper_tick, upper_cursor):
        try:
            return self._drain_row_observed(row, upper_tick, upper_cursor)
        except NativeAccountBindingError:
            raise
        except NativeSessionError as exc:
            code = getattr(exc, "code", None) or str(exc).split(":", 1)[0]
            if code.startswith(("PAPER_TICK_", "PAPER_M1_", "QUOTE_TICK_PAGE_")):
                retained = self._ledger.paper_fill_for_reservation(row["reservation_id"])
                if retained is not None:
                    minute = getattr(exc, "paper_failure_minute_msc", None)
                    if minute is None:
                        minute = retained["quote_cursor"]["time_msc"] // 60000 * 60000
                    self._audit_observed_error(retained, minute, upper_tick.time_msc, exc)
            raise

    def _drain_row_observed(self, row, upper_tick, upper_cursor):
        from pm_live_outcomes import PAPER_FILL_POLICY_ID
        cursor = row.get("quote_cursor")
        if cursor is None or row.get("fill_policy_id") != PAPER_FILL_POLICY_ID:
            raise NativeSessionError("PAPER_UNTRACKED_PROTECTION_REFUSED")
        if row["last_execution_evidence"]["event"] in {"SL", "TP"}:
            self._close_trigger(row)  # Known touch survives funding failure/history expiry.
            return None
        if self._cursor_order(upper_cursor) < self._cursor_order(cursor):
            raise NativeSessionError("PAPER_TICK_HISTORY_TIME_REVERSED")
        if cursor == upper_cursor:
            return row
        info = self._data.get_symbol_info(row["symbol"])
        if info is None:
            raise NativeSessionError("PAPER_SYMBOL_INFO_UNAVAILABLE")
        observed = self._completed_m1(row["symbol"], cursor["time_msc"], upper_tick.time_msc)
        minute = (cursor["time_msc"] // 60000) * 60000
        first = True
        while minute <= upper_tick.time_msc:
            page = None
            try:
                end = min(minute + 59999, upper_tick.time_msc)
                page = self._tick_page(row["symbol"], minute, end)
                if not page.complete:
                    raise NativeSessionError("PAPER_TICK_HISTORY_INCOMPLETE")
                pairs = list(self._tick_cursor_records(page.ticks))
                if first:
                    prior = next((seen for _, seen in pairs if self._cursor_order(seen) == self._cursor_order(cursor)), None)
                    if prior is None:
                        raise NativeSessionError("PAPER_TICK_HISTORY_CURSOR_UNAVAILABLE")
                    if prior != cursor:
                        raise NativeSessionError("PAPER_TICK_HISTORY_PREFIX_CHANGED")
                    first = False
                if end == upper_tick.time_msc:
                    boundary = next((seen for _, seen in pairs if self._cursor_order(seen) == self._cursor_order(upper_cursor)), None)
                    if boundary != upper_cursor:
                        raise NativeSessionError("PAPER_TICK_HISTORY_BOUNDARY_CHANGED")
                self._verify_m1_page(page, minute, upper_tick.time_msc, observed, info.point)
                last = None
                for tick, seen in pairs:
                    if self._cursor_order(seen) <= self._cursor_order(cursor):
                        continue
                    if self._cursor_order(seen) > self._cursor_order(upper_cursor):
                        break
                    long = int(row["type"]) == 0
                    price, sl, tp = (tick.bid if long else tick.ask), float(row["sl"]), float(row["tp"])
                    stop = sl > 0 and (price <= sl if long else price >= sl)
                    take = tp > 0 and (price >= tp if long else price <= tp)
                    if stop or take:
                        # Latch only observed evidence, then book cash/close atomically.
                        # A failed valuation cannot skip or reinterpret this first touch.
                        evidence = self._execution_evidence(tick, "SL" if stop else "TP")
                        row = self._ledger.paper_advance_quote_cursor(int(row["position_id"]),
                            expected_cursor=row["quote_cursor"], quote_cursor=seen, execution_evidence=evidence)
                        self._close_trigger(row)
                        return None
                    last = tick, seen
                if last is not None:
                    row = self._ledger.paper_advance_quote_cursor(int(row["position_id"]),
                        expected_cursor=row["quote_cursor"], quote_cursor=last[1],
                        execution_evidence=self._execution_evidence(last[0], "ADVANCE"))
                    cursor = row["quote_cursor"]
            except NativeSessionError as exc:
                if getattr(exc, "page", None) is None and page is not None:
                    exc.page = page
                exc.paper_failure_minute_msc = minute
                raise
            minute += 60000
        return row

    def paper_protection_diagnostics(self):
        return {symbol: list(errors) for symbol, errors in self._paper_symbol_errors.items()}

    def poll_paper_positions(self, symbol=None, magic=None, *, position_id=None, protection_only=False):
        with self.native_session(), self._poll_snapshot():
            touched = set(self._paper_symbol_errors) if symbol is None and magic is None and position_id is None else set()
            result, errors, closed_errors, settled = [], {}, {}, set()
            audit_pending = [row for row in self._ledger.paper_quote_audit_positions()
                             if row["status"] == "CLOSED" or (row.get("last_execution_evidence") or {}).get("event") in {"SL", "TP"}]
            for row in self._ledger.paper_positions():
                if ((symbol is not None and row["symbol"] != symbol)
                        or (magic is not None and row["magic"] != magic)
                        or (position_id is not None and row["position_id"] != position_id)):
                    continue
                origin = row
                touched.add(row["symbol"])
                try:
                    if (row.get("last_execution_evidence") or {}).get("event") in {"SL", "TP"}:
                        self._close_trigger(row)
                        settled.add(int(row["position_id"]))
                        continue
                    _, tick, cursor = self._quote_record(row["symbol"])
                    row = self._audit_prefix(row, tick)
                    retained = row
                    row = self._drain_row(row, tick, cursor)
                    if row is None:
                        row_closed = self._ledger.paper_fill_for_reservation(retained["reservation_id"])
                        self._audit_complete(row_closed, tick)
                    else:
                        row = self._audit_complete(row, tick)
                    if row is not None and not protection_only:
                        result.append(self._position(row, self._mark(row)))
                except NativeAccountBindingError:
                    raise
                except NativeSessionError as exc:
                    if (not str(exc).startswith(("PAPER_", "QUOTE_TICK_PAGE_"))
                            or "ACCOUNT" in str(exc) or not self._data.is_connected()):
                        raise
                    retained_row = self._ledger.paper_fill_for_reservation(origin["reservation_id"])
                    if retained_row is not None:
                        self._retain_unknown_upper_failure(retained_row, exc)
                    target_errors = closed_errors if retained_row is not None and retained_row["status"] == "CLOSED" else errors
                    target_errors.setdefault(origin["symbol"], []).append(str(exc))
            for closed in audit_pending:
                if int(closed["position_id"]) in settled:
                    continue  # Price settlement does not depend on rediscovering received evidence.
                closed = self._ledger.paper_fill_for_reservation(closed["reservation_id"])
                if ((symbol is not None and closed["symbol"] != symbol)
                        or (magic is not None and closed["magic"] != magic)
                        or (position_id is not None and closed["position_id"] != position_id)):
                    continue
                touched.add(closed["symbol"])
                try:
                    tick = self._data.get_tick(closed["symbol"])
                    if tick is None:
                        raise self._tick_unavailable_error("PAPER_AUDIT_SOURCE_TIME_UNAVAILABLE")
                    if type(getattr(tick, "time_msc", None)) is not int:
                        raise NativeSessionError("PAPER_AUDIT_SOURCE_TIME_UNAVAILABLE")
                    closed = self._audit_prefix(closed, tick)
                    self._audit_complete(closed, tick)
                except NativeAccountBindingError:
                    raise
                except NativeSessionError as exc:
                    if (not str(exc).startswith(("PAPER_", "QUOTE_TICK_PAGE_"))
                            or "ACCOUNT" in str(exc) or not self._data.is_connected()):
                        raise
                    latest = self._ledger.paper_fill_for_reservation(closed["reservation_id"])
                    if latest is not None:
                        self._retain_unknown_upper_failure(latest, exc)
                    audit_errors = errors if closed["status"] == "OPEN" else closed_errors
                    audit_errors.setdefault(closed["symbol"], []).append(str(exc))
            visible_errors = {name: errors.get(name, []) + closed_errors.get(name, [])
                              for name in errors.keys() | closed_errors.keys()}
            for name in touched:
                if name not in visible_errors:
                    self._paper_symbol_errors.pop(name, None)
            for name, issues in visible_errors.items():
                if self._paper_symbol_errors.get(name) != issues:
                    logging.getLogger("fxpm.paper").error("PAPER protection/valuation unavailable for %s: %s", name, issues)
                self._paper_symbol_errors[name] = issues
            if errors:
                raise PaperProtectionDataError(errors, result)
            return result

    def get_positions(self, symbol=None, magic=None):
        return self.poll_paper_positions(symbol, magic)

    def get_position_by_ticket(self, ticket):
        return next(iter(self.poll_paper_positions(position_id=int(ticket))), None)

    def get_position_by_symbol_magic(self, symbol, magic):
        positions = self.get_positions(symbol=symbol, magic=magic)
        if len(positions) > 1:
            raise NativeSessionError("PAPER_POSITION_MATCH_AMBIGUOUS")
        return positions[0] if positions else None

    def count_positions(self, symbol=None, magic=None):
        return len(self.get_positions(symbol, magic))

    def get_position_history(self, position_id, **kwargs):
        return self._ledger.paper_history(int(position_id))

    def _require_local_position(self, position):
        identifier = int(getattr(position, "identifier", 0) or getattr(position, "ticket", 0))
        expected = f"paper:{self.runtime_identity.namespace}:{identifier}"
        if (getattr(position, "destination", None) != "PAPER"
                or getattr(position, "runtime_namespace", None) != self.runtime_identity.namespace
                or getattr(position, "paper_position_id", None) != expected):
            raise NativeSessionError("PAPER_FOREIGN_POSITION_MUTATION_REFUSED")
        return identifier

    def get_position_opening_metadata(self, position_id, **kwargs):
        history = self.get_position_history(position_id)
        if history is None:
            return None
        return next((dict(deal) for deal in history["deals"] if deal["entry_kind"] == "IN"), None)

    def get_recent_closing_deals(self, lookback_hours=48):
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=max(1, int(lookback_hours)))).timestamp())
        return [dict(deal) for deal in self._ledger.paper_deals()
                if deal["entry_kind"] in {"OUT", "OUT_BY"} and int(deal["time_msc"]) >= cutoff * 1000]

    def get_risk_account_info(self, actual_account=None):
        with self.native_session():
            actual = actual_account if actual_account is not None else self.get_account_info()
            if account_identity_block_reason(actual, connector_config=self.config):
                raise NativeSessionError("PAPER_RISK_ACCOUNT_IDENTITY_MISMATCH")
            positions = self.poll_paper_positions()
            cash = self._ledger.paper_cash_adjustment()
            floating = sum(p.profit + p.swap for p in positions)
            margin = self._number(actual.margin, "BROKER_MARGIN")
            if margin < 0:
                raise NativeSessionError("PAPER_NEGATIVE_BROKER_MARGIN")
            for position in positions:
                needed = self._data.calc_margin_required(
                    position.type, position.symbol, position.volume, position.price_current,
                )
                needed = self._number(needed, "NATIVE_MARGIN")
                if needed < 0:
                    raise NativeSessionError("PAPER_NEGATIVE_NATIVE_MARGIN")
                margin += needed
            broker_equity = self._number(actual.equity, "BROKER_EQUITY")
            broker_balance = self._number(actual.balance, "BROKER_BALANCE")
            equity = broker_equity + cash + floating
            fields = dict(vars(actual))
            fields.update(balance=broker_balance + cash, equity=equity, margin=margin,
                          margin_free=equity - margin, margin_level=100 * equity / margin if margin > 0 else 0.0,
                          sampled_broker_balance=broker_balance, sampled_broker_equity=broker_equity,
                          paper_cash_adjustment=cash, paper_floating_adjustment=floating,
                          destination="PAPER", runtime_namespace=self.runtime_identity.namespace)
            return SimpleNamespace(**fields)

    def get_equity(self):
        return self.get_risk_account_info().equity

    def get_balance(self):
        return self.get_risk_account_info().balance

    @staticmethod
    def _stops_valid(side, price, sl, tp, minimum):
        if any(not math.isfinite(float(v)) or float(v) < 0 for v in (sl, tp)):
            return False
        if side == 0:
            return ((sl == 0 or sl < price) and (tp == 0 or tp > price)
                    and (sl == 0 or price - sl >= minimum) and (tp == 0 or tp - price >= minimum))
        return ((sl == 0 or sl > price) and (tp == 0 or tp < price)
                and (sl == 0 or sl - price >= minimum) and (tp == 0 or price - tp >= minimum))

    def send_market_order(self, symbol, order_type, volume, sl=0.0, tp=0.0, deviation=30,
                          magic=0, comment="", price=None, symbol_info=None, *, reservation_id=""):
        from pm_live_outcomes import LiveOutcomeError
        with self.native_session():
            if self._paper_entry_block_reason:
                return MT5OrderResult(False, 10017, self._paper_entry_block_reason, destination="PAPER")
            if not reservation_id:
                return MT5OrderResult(False, 10017, "PAPER_DURABLE_RESERVATION_REQUIRED", destination="PAPER")
            try:
                existing = self._ledger.paper_fill_for_reservation(reservation_id)
                if existing is not None:
                    return self._filled_result(existing)
                self.get_account_info()
                info, tick, cursor = self._quote_record(symbol)
                bid, ask, when = tick.bid, tick.ask, tick.time
                for held in self._ledger.paper_positions():
                    if held["symbol"] == info.symbol:
                        held = self._audit_prefix(held, tick)
                        original_reservation = held["reservation_id"]
                        held = self._drain_row(held, tick, cursor)
                        self._audit_complete(held or self._ledger.paper_fill_for_reservation(original_reservation), tick)
                for closed in self._ledger.paper_quote_audit_positions():
                    if closed["symbol"] == info.symbol and closed["status"] == "CLOSED":
                        closed = self._audit_prefix(closed, tick)
                        self._audit_complete(closed, tick)
                side = order_type.value if isinstance(order_type, OrderType) else int(order_type)
                if side not in (0, 1):
                    raise NativeSessionError("PAPER_MARKET_ORDER_TYPE_INVALID")
                normalized = self._data.normalize_volume(volume, info)
                if normalized is None:
                    return MT5OrderResult(False, 10014, "PAPER_VOLUME_OUTSIDE_RISK_CEILING", destination="PAPER")
                fill = ask if side == 0 else bid
                if price is not None and abs(fill - float(price)) > max(0, int(deviation)) * float(info.point):
                    return MT5OrderResult(False, 10020, "PAPER_OBSERVED_PRICE_CHANGED", destination="PAPER")
                if not self._stops_valid(side, bid if side == 0 else ask, float(sl), float(tp),
                                         float(info.trade_stops_level) * float(info.point)) or float(sl) <= 0:
                    return MT5OrderResult(False, 10016, "PAPER_INVALID_INITIAL_STOPS", destination="PAPER")
                risk = self._number(self._data.calc_loss_amount(side, symbol, normalized, fill, float(sl)),
                                    "ACTUAL_STOP_RISK", positive=True)
                terms = self._terms_for_entry(symbol, info, when)
            except (NativeSessionError, LiveOutcomeError, OSError, ValueError, TypeError) as exc:
                return MT5OrderResult(False, 10017, str(exc), error_message=str(exc), destination="PAPER")
            try:
                row = self._ledger.paper_open_position(reservation_id, {
                    "broker_symbol": info.symbol, "magic": int(magic), "type": side,
                    "volume": normalized, "price": fill, "sl": float(sl), "tp": float(tp),
                    "comment": str(comment), "risk_cash": risk,
                    "time_msc": tick.time_msc, "funding_terms": terms,
                    "quote_cursor": cursor, "fill_policy_id": self._execution_evidence(tick, "ENTRY")["model_id"],
                    "execution_evidence": self._execution_evidence(tick, "ENTRY"),
                    "quote_audit": self._initial_quote_audit(symbol),
                })
                return self._filled_result(row)
            except Exception:
                self._paper_entry_block_reason = "PAPER_FILL_PERSISTENCE_UNKNOWN_PENDING_RECONCILIATION"
                return MT5OrderResult(False, 10012, self._paper_entry_block_reason,
                                      ambiguous=True, destination="PAPER")

    @staticmethod
    def _filled_result(row):
        return MT5OrderResult(
            True, 10009, "PAPER_LOCAL_FILL", deal=int(row["opening_deal_ticket"]),
            order=int(row["opening_order_ticket"]), volume=float(row["volume_initial"]),
            price=float(row["price_open"]), position_id=int(row["position_id"]),
            paper_position_id=str(row["paper_position_id"]), destination="PAPER",
        )

    def close_position(self, position, deviation=30, magic=0, comment="", *, operation_id="", manual_recovery=False):
        return self.close_position_partial(position, percent=100.0, deviation=deviation,
                                           magic=magic, comment=comment, operation_id=operation_id,
                                           manual_recovery=manual_recovery)

    def _drain_for_mutation(self, identifier):
        row = self._paper_row(identifier)
        if row is None:
            raise NativeSessionError("PAPER_POSITION_UNAVAILABLE")
        if (row.get("last_execution_evidence") or {}).get("event") in {"SL", "TP"}:
            self._close_trigger(row)
            raise NativeSessionError("PAPER_POSITION_ALREADY_PROTECTED")
        info, tick, cursor = self._quote_record(row["symbol"])
        row = self._audit_prefix(row, tick)
        if self._drain_row(row, tick, cursor) is None:
            raise NativeSessionError("PAPER_POSITION_ALREADY_PROTECTED")
        row = self._paper_row(identifier)
        if row is None:
            raise NativeSessionError("PAPER_POSITION_UNAVAILABLE")
        return row, info, tick, cursor

    def close_position_partial(self, position, percent=50.0, deviation=30, magic=0, comment="", *, operation_id="", manual_recovery=False):
        from pm_live_outcomes import LiveOutcomeError
        with self.native_session():
            try:
                if type(manual_recovery) is not bool:
                    raise NativeSessionError("PAPER_MANUAL_RECOVERY_FLAG_INVALID")
                if manual_recovery and (not isinstance(operation_id, str) or not operation_id.strip()
                        or self._number(percent, "PARTIAL_PERCENT", positive=True) != 100.0):
                    raise NativeSessionError("PAPER_MANUAL_RECOVERY_REQUIRES_FULL_VOLUME_AND_OPERATION_ID")
                identifier = self._require_local_position(position)
                if operation_id:
                    previous = self._ledger.paper_close_operation(identifier, operation_id)
                    if previous is not None:
                        if manual_recovery != ((previous.get("execution_evidence") or {}).get("event") == "MANUAL_RECOVERY_CLOSE"):
                            raise NativeSessionError("PAPER_RECOVERY_OPERATION_KIND_MISMATCH")
                        return MT5OrderResult(
                            True, 10009, "PAPER_REPLAYED_LOCAL_CLOSE", deal=int(previous["ticket"]),
                            order=int(previous["order_ticket"]), volume=float(previous["volume"]),
                            price=float(previous["price"]), position_id=identifier,
                            paper_position_id=str(previous["paper_position_id"]), destination="PAPER",
                        )
                if manual_recovery:
                    row = self._paper_row(identifier)
                    if row is None:
                        raise NativeSessionError("PAPER_POSITION_UNAVAILABLE")
                    audit = row.get("quote_audit") or {}
                    if (audit.get("status") not in {"DIVERGENT", "UNAVAILABLE"}
                            or not audit.get("terminal_failure_code")
                            or (row.get("last_execution_evidence") or {}).get("event") in {"SL", "TP"}):
                        raise NativeSessionError("PAPER_MANUAL_RECOVERY_REQUIRES_TERMINAL_UNLATCHED_ROW")
                    # This explicit operator action does not reconstruct the gap,
                    # advance verified history or reinterpret an earlier touch.
                    _, tick, _ = self._quote_record(row["symbol"])
                    price = tick.bid if int(row["type"]) == 0 else tick.ask
                    evidence = {**self._execution_evidence(tick, "MANUAL_RECOVERY_CLOSE"),
                                "terminal_audit_status": audit["status"],
                                "terminal_audit_code": audit["terminal_failure_code"]}
                    deal = self._close_row(row, float(row["volume"]), price, tick.time,
                        operation_id=operation_id, execution_evidence=evidence,
                        observed_time_msc=tick.time_msc)
                else:
                    row, info, tick, cursor = self._drain_for_mutation(identifier)
                    pct = self._number(percent, "PARTIAL_PERCENT", positive=True)
                    if pct > 100:
                        raise NativeSessionError("PAPER_PARTIAL_PERCENT_INVALID")
                    volume = self._data.normalize_volume(float(row["volume"]) * pct / 100, info)
                    if volume is None:
                        return MT5OrderResult(False, 10014, "PAPER_PARTIAL_VOLUME_INVALID", destination="PAPER")
                    price = tick.bid if int(row["type"]) == 0 else tick.ask
                    deal = self._close_row(row, volume, price, tick.time, operation_id=operation_id,
                        quote_cursor=cursor,
                        execution_evidence=self._execution_evidence(tick, "MANUAL_CLOSE" if pct == 100 else "PARTIAL_CLOSE"))
                return MT5OrderResult(True, 10009, "PAPER_LOCAL_CLOSE", deal=int(deal["ticket"]),
                                      order=int(deal["order_ticket"]), volume=float(deal["volume"]),
                                      price=float(deal["price"]), position_id=int(row["position_id"]),
                                      paper_position_id=str(row["paper_position_id"]), destination="PAPER")
            except (ValueError, TypeError, OSError, NativeSessionError, LiveOutcomeError) as exc:
                return MT5OrderResult(False, 10017, str(exc), error_message=str(exc), destination="PAPER")

    def modify_position(self, position, sl=None, tp=None):
        from pm_live_outcomes import LiveOutcomeError
        with self.native_session():
            try:
                identifier = self._require_local_position(position)
                row, info, tick, cursor = self._drain_for_mutation(identifier)
                long = int(row["type"]) == 0
                next_sl = float(row["sl"] if sl is None else sl)
                next_tp = float(row["tp"] if tp is None else tp)
                if sl is not None and float(row["sl"]) > 0 and (
                    next_sl == 0 or (next_sl < row["sl"] if long else next_sl > row["sl"])
                ):
                    raise NativeSessionError("PAPER_STOP_WIDENING_REFUSED")
                if not self._stops_valid(int(row["type"]), tick.bid if long else tick.ask, next_sl, next_tp,
                                         max(info.trade_stops_level, info.trade_freeze_level) * float(info.point)):
                    raise NativeSessionError("PAPER_MODIFICATION_STOPS_INVALID")
                if next_sl != row["sl"] or next_tp != row["tp"]:
                    self._ledger.paper_modify_position(identifier, sl=sl, tp=tp,
                        expected_cursor=row["quote_cursor"], quote_cursor=cursor,
                        execution_evidence=self._execution_evidence(tick, "LEVEL_CHANGE"))
                return MT5OrderResult(True, 10009, "PAPER_LOCAL_MODIFICATION", position_id=identifier,
                                      paper_position_id=str(row["paper_position_id"]), destination="PAPER")
            except (ValueError, TypeError, NativeSessionError, LiveOutcomeError) as exc:
                return MT5OrderResult(False, 10017, str(exc), error_message=str(exc), destination="PAPER")


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
