from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class SignalEntry:
    symbol: str
    timeframe: str = ""
    regime: str = ""
    strategy_name: str = ""
    signal_direction: str = ""
    entry_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    signal_strength: Optional[float] = None
    timestamp: Optional[str] = None
    valid_now: bool = False
    reason: str = ""
    source: str = ""
    entry_id: str = ""
    secondary_trade: Optional[bool] = None
    secondary_reason: str = ""
    position_context: Dict[str, Any] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)
