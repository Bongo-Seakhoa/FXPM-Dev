"""
Local order-governance helpers for FXPM.

These policies sit strictly downstream of winner discovery. The strategy
tournament still decides the local winner first; governance then competes as a
second local layer for that exact context.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence


DEFAULT_GOVERNANCE_CANDIDATES: Sequence[str] = (
    "control_fixed",
    "breakeven_1r",
    "atr_trail_capped",
    "pure_atr",
)


def normalize_policy_name(name: Any) -> str:
    """Normalize policy names onto the local governance surface."""
    normalized = str(name or "control_fixed").strip().lower()
    aliases = {
        "control": "control_fixed",
        "fixed": "control_fixed",
        "breakeven": "breakeven_1r",
        "atr_trail": "atr_trail_capped",
        "atr_runner": "pure_atr",
        "pure_atr_runner": "pure_atr",
    }
    return aliases.get(normalized, normalized or "control_fixed")


def candidate_policy_names(raw_candidates: Optional[Iterable[Any]] = None) -> List[str]:
    """Return a deduplicated candidate list in stable order."""
    seen = set()
    ordered: List[str] = []
    source = list(raw_candidates) if raw_candidates else list(DEFAULT_GOVERNANCE_CANDIDATES)
    for item in source:
        name = normalize_policy_name(item)
        if name not in {
            "control_fixed",
            "breakeven_1r",
            "atr_trail_capped",
            "pure_atr",
        }:
            continue
        if name in seen:
            continue
        ordered.append(name)
        seen.add(name)
    if "control_fixed" not in seen:
        ordered.insert(0, "control_fixed")
    return ordered


def make_policy(policy: Any, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build a normalized governance policy payload."""
    raw = dict(policy) if isinstance(policy, dict) else {}
    name = normalize_policy_name(raw.get("selected_policy") or raw.get("policy_name") or raw.get("name") or policy)
    params: Dict[str, Any] = {
        "name": name,
        "trail_activation_atr_mult": 1.5,
        "trail_atr_mult": 2.5,
        "breakeven_r": 1.0,
        "breakeven_offset_pips": 0.0,
        "release_tp_after_r": None,
    }
    if name == "pure_atr":
        params["release_tp_after_r"] = 1.0
    for payload in (raw.get("parameters"), raw, overrides or {}):
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if key in params:
                params[key] = value
    params["name"] = name
    return params


def policy_name_from_artifact(policy: Any) -> str:
    """Extract the normalized selected policy name from an artifact payload."""
    if isinstance(policy, dict):
        return normalize_policy_name(
            policy.get("selected_policy") or policy.get("policy_name") or policy.get("name")
        )
    return normalize_policy_name(policy)


@dataclass
class GovernanceContext:
    symbol: str
    timeframe: str
    regime: str
    direction: int
    entry_price: float
    current_stop_loss: float
    current_take_profit: float
    initial_stop_loss: float
    initial_take_profit: float
    current_price: float
    current_atr: float
    highest_since_entry: float
    lowest_since_entry: float
    pip_size: float = 0.0
    price_step: float = 0.0
    min_stop_distance: float = 0.0
    tp_released: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_long(self) -> bool:
        return int(self.direction) == 1

    @property
    def initial_r(self) -> float:
        return abs(float(self.entry_price) - float(self.initial_stop_loss))

    @property
    def favorable_r(self) -> float:
        initial_r = self.initial_r
        if initial_r <= 0:
            return 0.0
        if self.is_long:
            return max(0.0, (float(self.highest_since_entry) - float(self.entry_price)) / initial_r)
        return max(0.0, (float(self.entry_price) - float(self.lowest_since_entry)) / initial_r)

    @property
    def unrealized(self) -> float:
        if self.is_long:
            return float(self.current_price) - float(self.entry_price)
        return float(self.entry_price) - float(self.current_price)


@dataclass
class GovernanceDecision:
    policy_name: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    tp_released: bool = False
    notes: List[str] = field(default_factory=list)


def _tighten_stop(current_stop: float, candidate_stop: float, direction: int) -> Optional[float]:
    if not math.isfinite(float(candidate_stop)):
        return None
    if int(direction) == 1:
        if current_stop <= 0 or float(candidate_stop) > float(current_stop):
            return float(candidate_stop)
        return None
    if current_stop <= 0 or float(candidate_stop) < float(current_stop):
        return float(candidate_stop)
    return None


def _sanitize_stop_candidate(candidate_stop: float, context: GovernanceContext) -> Optional[float]:
    """Clamp candidate stops to the executable side of the current market."""
    try:
        candidate = float(candidate_stop)
        current_price = float(context.current_price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(candidate) or not math.isfinite(current_price):
        return None

    min_gap = max(
        0.0,
        float(context.min_stop_distance or 0.0),
        float(context.price_step or 0.0),
    )
    if context.is_long:
        limit = current_price - min_gap if min_gap > 0 else math.nextafter(current_price, -math.inf)
        candidate = min(candidate, limit)
    else:
        limit = current_price + min_gap if min_gap > 0 else math.nextafter(current_price, math.inf)
        candidate = max(candidate, limit)
    return candidate if math.isfinite(candidate) else None


def evaluate_policy(policy: Any, context: GovernanceContext) -> GovernanceDecision:
    """Evaluate a local order-governance policy using causal bar-close inputs."""
    normalized = make_policy(policy)
    name = normalized["name"]
    decision = GovernanceDecision(policy_name=name)

    if name == "control_fixed":
        return decision

    if context.initial_r <= 0:
        return decision

    pip_size = float(context.pip_size or 0.0)

    if name == "breakeven_1r" and context.favorable_r >= float(normalized["breakeven_r"]):
        offset_price = pip_size * float(normalized["breakeven_offset_pips"])
        candidate = context.entry_price + offset_price if context.is_long else context.entry_price - offset_price
        tightened = _tighten_stop(
            context.current_stop_loss,
            _sanitize_stop_candidate(candidate, context),
            context.direction,
        )
        if tightened is not None:
            decision.stop_loss = tightened
            decision.notes.append("breakeven")
        return decision

    if name in {"atr_trail_capped", "pure_atr"}:
        atr_value = float(context.current_atr or 0.0)
        activation = float(normalized["trail_activation_atr_mult"])
        trail_mult = float(normalized["trail_atr_mult"])
        if atr_value > 0 and context.unrealized >= activation * atr_value:
            if context.is_long:
                candidate = float(context.highest_since_entry) - trail_mult * atr_value
            else:
                candidate = float(context.lowest_since_entry) + trail_mult * atr_value
            tightened = _tighten_stop(
                context.current_stop_loss,
                _sanitize_stop_candidate(candidate, context),
                context.direction,
            )
            if tightened is not None:
                decision.stop_loss = tightened
                decision.notes.append("atr_trail")
        release_tp_after_r = normalized.get("release_tp_after_r")
        if (
            release_tp_after_r is not None
            and not context.tp_released
            and context.favorable_r >= float(release_tp_after_r)
        ):
            decision.take_profit = math.inf if context.is_long else -math.inf
            decision.tp_released = True
            decision.notes.append("tp_release")
        return decision

    return decision
