"""Lightweight live-cycle drift sentinel for local trading contexts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union


@dataclass
class CycleDriftContext:
    symbol: str
    timeframe: str = ""
    regime: str = ""
    strategy_name: str = ""
    execution_slippage_mult: float = 0.0
    spread_degradation_mult: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CycleDriftDecision:
    allow_trade: bool = True
    action: str = "allow"
    risk_multiplier: float = 1.0
    reasons: List[str] = field(default_factory=list)
    observed_only: bool = False


@dataclass
class _ContextState:
    trades: int = 0
    consecutive_losses: int = 0
    last_pnl: float = 0.0
    last_execution_slippage_mult: float = 0.0
    max_execution_slippage_mult: float = 0.0
    last_spread_degradation_mult: float = 0.0
    max_spread_degradation_mult: float = 0.0
    last_evaluated_at: str = ""
    processed_event_ids: List[str] = field(default_factory=list)


class CycleDriftMonitor:
    """Track only severe local drift signals; no ranking or allocation."""

    def __init__(self, config: Any, state_path: Optional[Union[str, Path]] = None):
        self.config = config
        if state_path is None:
            raw_log_dir = getattr(config, "log_dir", "logs")
            try:
                log_dir = Path(raw_log_dir if isinstance(raw_log_dir, (str, Path)) else "logs")
            except Exception:
                log_dir = Path("logs")
            state_path = log_dir / "cycle_drift_state.json"
        self.state_path = Path(state_path)
        self._persistence_error = ""
        self._state: Dict[Tuple[str, str, str, str], _ContextState] = self._load_state()

    @staticmethod
    def _encode_key(key: Tuple[str, str, str, str]) -> str:
        return "|".join(key)

    @staticmethod
    def _decode_key(raw: str) -> Tuple[str, str, str, str]:
        parts = str(raw).split("|")
        while len(parts) < 4:
            parts.append("")
        return tuple(parts[:4])  # type: ignore[return-value]

    def _load_state(self) -> Dict[Tuple[str, str, str, str], _ContextState]:
        try:
            if self.state_path.exists():
                payload = json.loads(self.state_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    state: Dict[Tuple[str, str, str, str], _ContextState] = {}
                    for raw_key, raw_state in payload.items():
                        if not isinstance(raw_state, dict):
                            continue
                        state[self._decode_key(raw_key)] = _ContextState(
                            trades=int(raw_state.get("trades", 0) or 0),
                            consecutive_losses=int(raw_state.get("consecutive_losses", 0) or 0),
                            last_pnl=float(raw_state.get("last_pnl", 0.0) or 0.0),
                            last_execution_slippage_mult=float(
                                raw_state.get("last_execution_slippage_mult", 0.0) or 0.0
                            ),
                            max_execution_slippage_mult=float(
                                raw_state.get("max_execution_slippage_mult", 0.0) or 0.0
                            ),
                            last_spread_degradation_mult=float(
                                raw_state.get("last_spread_degradation_mult", 0.0) or 0.0
                            ),
                            max_spread_degradation_mult=float(
                                raw_state.get("max_spread_degradation_mult", 0.0) or 0.0
                            ),
                            last_evaluated_at=str(raw_state.get("last_evaluated_at", "") or ""),
                            processed_event_ids=[
                                str(value) for value in raw_state.get("processed_event_ids", []) or []
                                if str(value)
                            ][-10000:],
                        )
                    return state
        except Exception as exc:
            self._persistence_error = f"cycle drift state load failed: {type(exc).__name__}: {exc}"
        return {}

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                self._encode_key(key): {
                    "trades": value.trades,
                    "consecutive_losses": value.consecutive_losses,
                    "last_pnl": value.last_pnl,
                    "last_execution_slippage_mult": value.last_execution_slippage_mult,
                    "max_execution_slippage_mult": value.max_execution_slippage_mult,
                    "last_spread_degradation_mult": value.last_spread_degradation_mult,
                    "max_spread_degradation_mult": value.max_spread_degradation_mult,
                    "last_evaluated_at": value.last_evaluated_at,
                    "processed_event_ids": list(value.processed_event_ids[-10000:]),
                }
                for key, value in self._state.items()
            }
            temp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
            temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            temp_path.replace(self.state_path)
            self._persistence_error = ""
        except Exception as exc:
            self._persistence_error = f"cycle drift state save failed: {type(exc).__name__}: {exc}"

    @staticmethod
    def _key(symbol: str, timeframe: str, regime: str, strategy_name: str) -> Tuple[str, str, str, str]:
        return (
            str(symbol or "").upper(),
            str(timeframe or "").upper(),
            str(regime or "").upper(),
            str(strategy_name or ""),
        )

    def _mode(self) -> str:
        if not bool(getattr(self.config, "cycle_drift_monitor_enabled", True)):
            return "off"
        mode = str(getattr(self.config, "cycle_drift_mode", "lightweight") or "").lower()
        return mode if mode in {"off", "observe", "lightweight"} else "lightweight"

    def record_trade(self,
                     symbol: str,
                     timeframe: str,
                     regime: str,
                     strategy_name: str,
                     pnl_dollars: float,
                     *,
                     execution_slippage_mult: float = 0.0,
                     spread_degradation_mult: float = 0.0,
                     event_id: str = "") -> None:
        """Record a closed trade. Optional execution-quality observations widen
        the running max so the sentinel can pause a context whose live execution
        has degraded materially against expectations."""
        key = self._key(symbol, timeframe, regime, strategy_name)
        state = self._state.setdefault(key, _ContextState())
        normalized_event_id = str(event_id or "").strip()
        if normalized_event_id and normalized_event_id in state.processed_event_ids:
            return
        state.trades += 1
        state.last_pnl = float(pnl_dollars or 0.0)
        if state.last_pnl < 0.0:
            state.consecutive_losses += 1
        else:
            state.consecutive_losses = 0
        if execution_slippage_mult:
            value = float(execution_slippage_mult)
            state.last_execution_slippage_mult = value
            state.max_execution_slippage_mult = max(state.max_execution_slippage_mult, value)
        if spread_degradation_mult:
            value = float(spread_degradation_mult)
            state.last_spread_degradation_mult = value
            state.max_spread_degradation_mult = max(state.max_spread_degradation_mult, value)
        state.last_evaluated_at = datetime.now().isoformat()
        if normalized_event_id:
            state.processed_event_ids.append(normalized_event_id)
            if len(state.processed_event_ids) > 10000:
                state.processed_event_ids = state.processed_event_ids[-10000:]
        self._save_state()

    def reset_context(self,
                      symbol: str,
                      timeframe: str = "",
                      regime: str = "",
                      strategy_name: str = "") -> None:
        """Clear one context's drift state after a validated retrain/manual reset."""
        self._state.pop(self._key(symbol, timeframe, regime, strategy_name), None)
        self._save_state()

    def snapshot(self, symbol: str, timeframe: str, regime: str, strategy_name: str) -> Dict[str, Any]:
        state = self._state.get(self._key(symbol, timeframe, regime, strategy_name), _ContextState())
        return {
            "trades": state.trades,
            "consecutive_losses": state.consecutive_losses,
            "last_pnl": state.last_pnl,
            "last_execution_slippage_mult": state.last_execution_slippage_mult,
            "max_execution_slippage_mult": state.max_execution_slippage_mult,
            "last_spread_degradation_mult": state.last_spread_degradation_mult,
            "max_spread_degradation_mult": state.max_spread_degradation_mult,
            "last_evaluated_at": state.last_evaluated_at,
            "processed_event_count": len(state.processed_event_ids),
        }

    def evaluate(self, context: CycleDriftContext) -> CycleDriftDecision:
        if self._persistence_error:
            return CycleDriftDecision(
                allow_trade=False,
                action="pause_state_persistence",
                risk_multiplier=0.0,
                reasons=[self._persistence_error],
            )
        mode = self._mode()
        if mode == "off":
            return CycleDriftDecision(action="disabled")

        key = self._key(context.symbol, context.timeframe, context.regime, context.strategy_name)
        state = self._state.setdefault(key, _ContextState())
        state_changed = False
        if context.execution_slippage_mult:
            value = float(context.execution_slippage_mult)
            state.last_execution_slippage_mult = value
            state.max_execution_slippage_mult = max(state.max_execution_slippage_mult, value)
            state_changed = True
        if context.spread_degradation_mult:
            value = float(context.spread_degradation_mult)
            state.last_spread_degradation_mult = value
            state.max_spread_degradation_mult = max(state.max_spread_degradation_mult, value)
            state_changed = True
        if state_changed:
            state.last_evaluated_at = datetime.now().isoformat()
            self._save_state()
            if self._persistence_error:
                return CycleDriftDecision(
                    allow_trade=False,
                    action="pause_state_persistence",
                    risk_multiplier=0.0,
                    reasons=[self._persistence_error],
                )
        min_trades = max(1, int(getattr(self.config, "cycle_drift_min_trades", 6) or 6))
        loss_threshold = max(1, int(getattr(self.config, "cycle_drift_consecutive_losses", 5) or 5))
        slippage_threshold = float(getattr(self.config, "cycle_drift_execution_slippage_mult", 2.5) or 2.5)
        spread_threshold = float(getattr(self.config, "cycle_drift_spread_degradation_mult", 2.0) or 2.0)

        reasons: List[str] = []
        if state.trades >= min_trades and state.consecutive_losses >= loss_threshold:
            reasons.append(f"consecutive_losses {state.consecutive_losses} >= {loss_threshold}")
        if context.execution_slippage_mult and context.execution_slippage_mult >= slippage_threshold:
            reasons.append(f"execution_slippage_mult {context.execution_slippage_mult:.2f} >= {slippage_threshold:.2f}")
        if context.spread_degradation_mult and context.spread_degradation_mult >= spread_threshold:
            reasons.append(f"spread_degradation_mult {context.spread_degradation_mult:.2f} >= {spread_threshold:.2f}")

        if not reasons:
            return CycleDriftDecision(action="allow")

        action = str(getattr(self.config, "cycle_drift_action", "pause_context") or "").lower()
        if mode == "observe" or action == "observe":
            return CycleDriftDecision(action="observe", reasons=reasons, observed_only=True)
        if action == "derate_context":
            return CycleDriftDecision(action="derate_context", risk_multiplier=0.5, reasons=reasons)
        return CycleDriftDecision(allow_trade=False, action="pause_context", risk_multiplier=0.0, reasons=reasons)
