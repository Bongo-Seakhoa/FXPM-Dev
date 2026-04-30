"""Institutional hard-breakpoint supervisor for live entries.

The supervisor is deliberately narrow: it does not rank symbols, reallocate
risk, or veto local winners based on soft portfolio preferences. It only blocks
or derates when configured hard breakpoints are already breached.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


@dataclass
class SupervisorContext:
    symbol: str
    timeframe: str = ""
    regime: str = ""
    strategy_name: str = ""
    total_open_risk_pct: float = 0.0
    new_trade_risk_pct: float = 0.0
    daily_loss_pct: float = 0.0
    session_loss_pct: float = 0.0
    weekly_loss_pct: float = 0.0
    open_drawdown_pct: float = 0.0
    margin_level: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SupervisorDecision:
    allow_trade: bool = True
    action: str = "allow"
    risk_multiplier: float = 1.0
    reasons: List[str] = field(default_factory=list)
    observed_only: bool = False


class InstitutionalSupervisor:
    """Evaluate hard portfolio/account breakpoints for one proposed entry."""

    def __init__(self, config: Any, state_path: Optional[Union[str, Path]] = None):
        self.config = config
        if state_path is None:
            raw_log_dir = getattr(config, "log_dir", "logs")
            try:
                log_dir = Path(raw_log_dir if isinstance(raw_log_dir, (str, Path)) else "logs")
            except Exception:
                log_dir = Path("logs")
            state_path = log_dir / "institutional_supervisor_state.json"
        self.state_path = Path(state_path)
        self._state: Dict[str, Any] = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        try:
            if self.state_path.exists():
                payload = json.loads(self.state_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    return payload
        except Exception:
            pass
        return {"active_block": False, "block_reasons": [], "cycle_id": None, "blocked_at": None}

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(self._state, indent=2, sort_keys=True), encoding="utf-8")
        except Exception:
            pass

    def reset(self) -> None:
        self._state = {"active_block": False, "block_reasons": [], "cycle_id": None, "blocked_at": None}
        self._save_state()

    def _enabled_mode(self) -> str:
        enabled = bool(getattr(self.config, "institutional_supervisor_enabled", True))
        mode = str(getattr(self.config, "institutional_supervisor_mode", "circuit_breaker") or "").lower()
        if not enabled or mode == "off":
            return "off"
        if mode not in {"observe", "circuit_breaker"}:
            return "circuit_breaker"
        return mode

    @staticmethod
    def _threshold(config: Any, name: str) -> float:
        try:
            value = float(getattr(config, name, 0.0) or 0.0)
            return max(0.0, value)
        except Exception:
            return 0.0

    def evaluate(self, context: SupervisorContext) -> SupervisorDecision:
        mode = self._enabled_mode()
        if mode == "off":
            return SupervisorDecision(action="disabled")

        policy = str(getattr(self.config, "portfolio_control_policy", "hard_breakpoints_only") or "").lower()
        if policy == "off":
            return SupervisorDecision(action="policy_off")

        reasons: List[str] = []
        for attr, label, value in (
            ("daily_loss_hard_block_pct", "daily_loss", context.daily_loss_pct),
            ("session_loss_hard_block_pct", "session_loss", context.session_loss_pct),
            ("weekly_loss_hard_block_pct", "weekly_loss", context.weekly_loss_pct),
            ("open_drawdown_hard_block_pct", "open_drawdown", context.open_drawdown_pct),
        ):
            threshold = self._threshold(self.config, attr)
            if threshold > 0.0 and float(value or 0.0) >= threshold:
                reasons.append(f"{label} {float(value):.2f}% >= {threshold:.2f}%")

        prospective_total = max(0.0, float(context.total_open_risk_pct or 0.0)) + max(
            0.0, float(context.new_trade_risk_pct or 0.0)
        )
        max_total = self._threshold(self.config, "max_total_open_risk_pct")
        if max_total > 0.0 and prospective_total > max_total:
            reasons.append(f"total_open_risk {prospective_total:.2f}% > {max_total:.2f}%")

        elevated_open_risk = self._threshold(self.config, "max_new_entries_when_total_open_risk_above_pct")
        open_risk_action = str(getattr(self.config, "open_risk_action", "pause_new_entries") or "").lower()
        if elevated_open_risk > 0.0 and context.total_open_risk_pct >= elevated_open_risk:
            if open_risk_action == "pause_new_entries":
                reasons.append(
                    f"existing_open_risk {context.total_open_risk_pct:.2f}% >= {elevated_open_risk:.2f}%"
                )
            elif open_risk_action == "derate":
                available = max(0.0, max_total - float(context.total_open_risk_pct or 0.0)) if max_total > 0 else 0.0
                if context.new_trade_risk_pct > 0 and available > 0.0:
                    multiplier = max(0.0, min(1.0, available / float(context.new_trade_risk_pct)))
                    if multiplier < 1.0:
                        decision = SupervisorDecision(
                            allow_trade=True,
                            action="derate",
                            risk_multiplier=multiplier,
                            reasons=[
                                f"existing_open_risk {context.total_open_risk_pct:.2f}% >= {elevated_open_risk:.2f}%"
                            ],
                        )
                        if mode == "observe":
                            decision.risk_multiplier = 1.0
                            decision.action = "observe_derate"
                            decision.observed_only = True
                        return decision

        if not reasons:
            if bool(self._state.get("active_block")):
                reopen_mode = str(getattr(self.config, "hard_block_reopen_mode", "manual_or_next_cycle") or "").lower()
                current_cycle = str(context.metadata.get("cycle_id", "") or "")
                blocked_cycle = str(self._state.get("cycle_id", "") or "")
                if reopen_mode in {"manual_or_next_cycle", "next_cycle"} and current_cycle and current_cycle != blocked_cycle:
                    self.reset()
                    return SupervisorDecision(action="allow")
                if reopen_mode == "manual_or_next_cycle":
                    return SupervisorDecision(
                        allow_trade=False,
                        action="block_latched",
                        risk_multiplier=0.0,
                        reasons=["hard block latched until manual reset or next cycle"],
                    )
                if reopen_mode == "next_cycle":
                    return SupervisorDecision(
                        allow_trade=False,
                        action="block_latched",
                        risk_multiplier=0.0,
                        reasons=["hard block latched until next cycle"],
                    )
                if reopen_mode == "manual":
                    return SupervisorDecision(
                        allow_trade=False,
                        action="block_latched",
                        risk_multiplier=0.0,
                        reasons=["hard block latched until manual reset"],
                    )
            return SupervisorDecision(action="allow")

        if mode == "observe":
            return SupervisorDecision(
                allow_trade=True,
                action="observe_block",
                reasons=reasons,
                observed_only=True,
            )

        self._state = {
            "active_block": True,
            "block_reasons": list(reasons),
            "cycle_id": str(context.metadata.get("cycle_id", "") or ""),
            "blocked_at": datetime.now().isoformat(),
            "symbol": context.symbol,
            "timeframe": context.timeframe,
            "regime": context.regime,
            "strategy_name": context.strategy_name,
        }
        self._save_state()
        return SupervisorDecision(
            allow_trade=False,
            action="block_new_entry",
            risk_multiplier=0.0,
            reasons=reasons,
        )
