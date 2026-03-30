"""
FX Portfolio Manager - Enhancement Seams
========================================

Explicit insertion points for post-stabilization quantitative upgrades.

These seams provide real, configurable implementations for sizing overlays,
exit packs, portfolio allocators, regime model upgrades, execution-quality
overlays, option-model adaptations, and strategy extensions.
"""

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np


# ===========================================================================
# G1: Risk Scalar Stack
# ===========================================================================

@dataclass
class RiskScalarContext:
    symbol: str
    timeframe: str
    regime: str
    base_risk_pct: float
    account_equity: float = 0.0
    account_peak_equity: float = 0.0
    current_atr: float = 0.0
    current_price: float = 0.0
    target_annual_vol: float = 0.0
    open_position_count: int = 0
    open_exposure_pct: float = 0.0
    historical_win_rate: float = 0.0
    historical_avg_win: float = 0.0
    historical_avg_loss: float = 0.0
    metrics: Dict[str, Any] = field(default_factory=dict)


class RiskScalarOverlay:
    """Base risk-scalar overlay."""

    def apply(self, risk_pct: float, context: RiskScalarContext) -> float:
        return risk_pct


class VolatilityTargetScalar(RiskScalarOverlay):
    """
    Scale position risk so that dollar volatility approximates a target.

    When realized vol is high the scalar shrinks risk; when low it grows.
    Clamp prevents extreme swings.
    """

    def __init__(self, target_annual_vol: float = 0.10, min_scalar: float = 0.3, max_scalar: float = 2.0):
        self.target_annual_vol = target_annual_vol
        self.min_scalar = min_scalar
        self.max_scalar = max_scalar

    @staticmethod
    def _bars_per_year(timeframe: str) -> float:
        tf = str(timeframe or "").upper()
        minute_map = {
            "M1": 1,
            "M5": 5,
            "M15": 15,
            "M30": 30,
            "H1": 60,
            "H4": 240,
            "D1": 24 * 60,
            "W1": 7 * 24 * 60,
            "MN1": 30 * 24 * 60,
        }
        minutes = minute_map.get(tf)
        if minutes is None or minutes <= 0:
            return 252.0
        return max((252.0 * 24.0 * 60.0) / float(minutes), 1.0)

    def apply(self, risk_pct: float, context: RiskScalarContext) -> float:
        if context.current_atr <= 0 or context.current_price <= 0:
            return risk_pct
        # Use ATR as a fraction of price, annualized to the current bar frequency.
        atr_fraction = abs(context.current_atr) / abs(context.current_price)
        if atr_fraction <= 0:
            return risk_pct
        bars_per_year = self._bars_per_year(context.timeframe)
        realized_vol = atr_fraction * math.sqrt(bars_per_year)
        if realized_vol <= 0:
            return risk_pct
        target_vol = context.target_annual_vol if context.target_annual_vol > 0 else self.target_annual_vol
        scalar = target_vol / realized_vol
        scalar = max(self.min_scalar, min(self.max_scalar, scalar))
        return risk_pct * scalar


class ExposureCorrelationScalar(RiskScalarOverlay):
    """
    Reduce per-trade risk when the book already has significant open exposure.

    Simple linear taper: full risk at 0 positions, halved at max_positions.
    """

    def __init__(self, max_positions: int = 10, floor_scalar: float = 0.4):
        self.max_positions = max(max_positions, 1)
        self.floor_scalar = floor_scalar

    def apply(self, risk_pct: float, context: RiskScalarContext) -> float:
        ratio = min(context.open_position_count / self.max_positions, 1.0)
        scalar = 1.0 - ratio * (1.0 - self.floor_scalar)
        return risk_pct * scalar


class FractionalKellyCap(RiskScalarOverlay):
    """
    Cap position risk at a fraction of the Kelly criterion.

    Uses historical win rate and avg win/loss to compute full Kelly,
    then applies a conservative fraction (default 25%).
    """

    def __init__(self, kelly_fraction: float = 0.25, min_trades_for_kelly: int = 30):
        self.kelly_fraction = kelly_fraction
        self.min_trades_for_kelly = min_trades_for_kelly

    def apply(self, risk_pct: float, context: RiskScalarContext) -> float:
        wr = context.historical_win_rate
        avg_w = context.historical_avg_win
        avg_l = abs(context.historical_avg_loss) if context.historical_avg_loss != 0 else 0.0
        total_trades = context.metrics.get('total_trades', 0)

        if total_trades < self.min_trades_for_kelly or avg_l <= 0 or wr <= 0:
            return risk_pct

        win_loss_ratio = avg_w / avg_l
        kelly_pct = (wr * win_loss_ratio - (1.0 - wr)) / win_loss_ratio
        kelly_pct = max(kelly_pct, 0.0) * 100.0  # Convert to percentage
        fractional_kelly = kelly_pct * self.kelly_fraction

        return min(risk_pct, fractional_kelly) if fractional_kelly > 0 else risk_pct


class DrawdownPositionScalar(RiskScalarOverlay):
    """
    Reduce position size progressively as account drawdown deepens.

    At max_dd_pct drawdown, risk is scaled to floor_scalar of base.
    """

    def __init__(self, max_dd_pct: float = 20.0, floor_scalar: float = 0.25):
        self.max_dd_pct = max(max_dd_pct, 1.0)
        self.floor_scalar = floor_scalar

    def apply(self, risk_pct: float, context: RiskScalarContext) -> float:
        if context.account_peak_equity <= 0:
            return risk_pct
        dd_pct = ((context.account_peak_equity - context.account_equity) /
                  context.account_peak_equity) * 100.0
        if dd_pct <= 0:
            return risk_pct
        dd_ratio = min(dd_pct / self.max_dd_pct, 1.0)
        scalar = 1.0 - dd_ratio * (1.0 - self.floor_scalar)
        return risk_pct * scalar


class RiskScalarStack:
    """Composable risk-scalar stack."""

    def __init__(self, overlays: Optional[List[RiskScalarOverlay]] = None):
        self.overlays = overlays or []

    def apply(self, risk_pct: float, context: RiskScalarContext) -> float:
        current = risk_pct
        for overlay in self.overlays:
            current = float(overlay.apply(current, context))
        return current


# ===========================================================================
# G2: Market-Driven Exit Pack
# ===========================================================================

@dataclass
class ExitPackContext:
    symbol: str
    timeframe: str
    direction: str
    entry_price: float
    current_price: float
    current_atr: float = 0.0
    bars_held: int = 0
    highest_since_entry: float = 0.0
    lowest_since_entry: float = 0.0
    position_size: float = 0.0
    features: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExitPackDecision:
    exit_now: bool = False
    exit_reason: str = ""
    exit_volume_pct: float = 100.0  # % of position to close (supports partial)
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class MarketDrivenExitPack:
    """Configurable exit pack with ATR trailing, breakeven, and partial profit."""

    def __init__(self,
                 atr_trail_mult: float = 2.5,
                 atr_trail_activation_mult: float = 1.5,
                 partial_tp1_mult: float = 1.0,
                 partial_tp1_close_pct: float = 50.0,
                 breakeven_trigger_mult: float = 1.0,
                 breakeven_offset_pips: float = 1.0,
                 enabled: bool = False):
        self.atr_trail_mult = atr_trail_mult
        self.atr_trail_activation_mult = atr_trail_activation_mult
        self.partial_tp1_mult = partial_tp1_mult
        self.partial_tp1_close_pct = partial_tp1_close_pct
        self.breakeven_trigger_mult = breakeven_trigger_mult
        self.breakeven_offset_pips = breakeven_offset_pips
        self.enabled = enabled

    def evaluate(self, context: ExitPackContext) -> ExitPackDecision:
        if not self.enabled or context.current_atr <= 0:
            return ExitPackDecision()

        is_long = context.direction.upper() in ("LONG", "BUY", "1")
        atr = context.current_atr

        # ATR trailing stop
        if is_long:
            unrealized = context.current_price - context.entry_price
            trail_stop = context.highest_since_entry - self.atr_trail_mult * atr
            if unrealized >= self.atr_trail_activation_mult * atr:
                if context.current_price <= trail_stop:
                    return ExitPackDecision(
                        exit_now=True,
                        exit_reason="ATR_TRAIL_STOP",
                        stop_loss=trail_stop,
                    )
                return ExitPackDecision(stop_loss=trail_stop)
        else:
            unrealized = context.entry_price - context.current_price
            trail_stop = context.lowest_since_entry + self.atr_trail_mult * atr
            if unrealized >= self.atr_trail_activation_mult * atr:
                if context.current_price >= trail_stop:
                    return ExitPackDecision(
                        exit_now=True,
                        exit_reason="ATR_TRAIL_STOP",
                        stop_loss=trail_stop,
                    )
                return ExitPackDecision(stop_loss=trail_stop)

        # Partial profit at TP1
        if is_long:
            if context.current_price >= context.entry_price + self.partial_tp1_mult * atr:
                if not context.metadata.get('partial_tp1_taken'):
                    return ExitPackDecision(
                        exit_now=True,
                        exit_reason="PARTIAL_TP1",
                        exit_volume_pct=self.partial_tp1_close_pct,
                    )
        else:
            if context.current_price <= context.entry_price - self.partial_tp1_mult * atr:
                if not context.metadata.get('partial_tp1_taken'):
                    return ExitPackDecision(
                        exit_now=True,
                        exit_reason="PARTIAL_TP1",
                        exit_volume_pct=self.partial_tp1_close_pct,
                    )

        return ExitPackDecision()


# ===========================================================================
# G3: Portfolio Construction
# ===========================================================================

@dataclass
class PortfolioConstructionContext:
    symbol_candidates: List[str]
    candidate_scores: Dict[str, float]
    exposures: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class PortfolioAllocator:
    """Base portfolio-construction seam (equal-weight default)."""

    def allocate(self, context: PortfolioConstructionContext) -> Dict[str, float]:
        if not context.symbol_candidates:
            return {}
        equal_weight = 1.0 / len(context.symbol_candidates)
        return {symbol: equal_weight for symbol in context.symbol_candidates}


# ===========================================================================
# G4: Regime Model Adapter
# ===========================================================================

@dataclass
class RegimeModelContext:
    symbol: str
    timeframe: str
    features: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


class RegimeModelAdapter:
    """Base regime-model upgrade seam."""

    def transform(self, context: RegimeModelContext) -> Any:
        return context.features


# ===========================================================================
# G5: Execution Quality Overlay (with real spread filter logic)
# ===========================================================================

@dataclass
class ExecutionQualityContext:
    symbol: str
    timeframe: str
    spread_pips: float
    atr_pips: float = 0.0
    slippage_pips: float = 0.0
    candidate_score: float = 0.0
    rolling_spread_median: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionQualityDecision:
    allow_trade: bool = True
    score_multiplier: float = 1.0
    notes: List[str] = field(default_factory=list)


class SpreadAwareExecutionOverlay:
    """
    G5: Spread-aware signal filter + spike detection.

    - Rejects trades where ATR < min_edge_mult * spread (insufficient edge).
    - Rejects trades during spread spikes (spread > spike_mult * rolling median).
    - Penalizes score when spread is high but still above threshold.
    """

    def __init__(self,
                 min_edge_mult: float = 1.5,
                 spike_mult: float = 2.0,
                 penalty_start_mult: float = 0.5,
                 enabled: bool = True):
        self.min_edge_mult = min_edge_mult
        self.spike_mult = spike_mult
        self.penalty_start_mult = penalty_start_mult
        self.enabled = enabled

    def evaluate(self, context: ExecutionQualityContext) -> ExecutionQualityDecision:
        if not self.enabled or context.spread_pips <= 0:
            return ExecutionQualityDecision()

        notes: List[str] = []

        # Spread spike detection
        if context.rolling_spread_median > 0:
            if context.spread_pips > self.spike_mult * context.rolling_spread_median:
                return ExecutionQualityDecision(
                    allow_trade=False,
                    score_multiplier=0.0,
                    notes=[f"Spread spike: {context.spread_pips:.1f} > {self.spike_mult}x median {context.rolling_spread_median:.1f}"],
                )

        # Minimum edge filter
        if context.atr_pips > 0:
            min_edge = self.min_edge_mult * context.spread_pips
            if context.atr_pips < min_edge:
                return ExecutionQualityDecision(
                    allow_trade=False,
                    score_multiplier=0.0,
                    notes=[f"ATR {context.atr_pips:.1f} < {self.min_edge_mult}x spread {context.spread_pips:.1f}"],
                )

            # Soft penalty when spread is significant relative to ATR
            spread_ratio = context.spread_pips / context.atr_pips
            if spread_ratio > self.penalty_start_mult:
                penalty = max(0.5, 1.0 - (spread_ratio - self.penalty_start_mult))
                notes.append(f"Spread penalty: ratio={spread_ratio:.2f}, multiplier={penalty:.2f}")
                return ExecutionQualityDecision(
                    allow_trade=True,
                    score_multiplier=penalty,
                    notes=notes,
                )

        return ExecutionQualityDecision(notes=notes)


class ExecutionQualityOverlay:
    """Legacy base class for backward compatibility."""

    def evaluate(self, context: ExecutionQualityContext) -> ExecutionQualityDecision:
        return ExecutionQualityDecision()


# ===========================================================================
# G6: Options-Model Adaptations
# ===========================================================================

@dataclass
class OptionsModelContext:
    symbol: str
    timeframe: str
    features: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


class OptionsModelAdapter:
    """Base options-model adaptation seam."""

    def transform(self, context: OptionsModelContext) -> Dict[str, Any]:
        return {}


# ===========================================================================
# G7: Strategy Extension Registry
# ===========================================================================

@dataclass
class StrategyInsertionSpec:
    name: str
    strategy_cls: type
    required_features: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class StrategyExtensionRegistry:
    """Registry for future add-on strategies without editing the core roster map directly."""

    def __init__(self):
        self._registry: Dict[str, StrategyInsertionSpec] = {}

    def register(self, spec: StrategyInsertionSpec) -> None:
        self._registry[spec.name] = spec

    def list_specs(self) -> List[StrategyInsertionSpec]:
        return list(self._registry.values())


# ===========================================================================
# Composite seam bundle
# ===========================================================================

@dataclass
class EnhancementSeams:
    risk_scalar_stack: RiskScalarStack
    exit_pack: MarketDrivenExitPack
    portfolio_allocator: PortfolioAllocator
    regime_model_adapter: RegimeModelAdapter
    execution_quality_overlay: ExecutionQualityOverlay
    options_model_adapter: OptionsModelAdapter
    strategy_extension_registry: StrategyExtensionRegistry


def create_default_enhancement_seams() -> EnhancementSeams:
    """Build the default seam bundle with sensible production defaults."""
    return EnhancementSeams(
        risk_scalar_stack=RiskScalarStack([
            VolatilityTargetScalar(target_annual_vol=0.10),
            ExposureCorrelationScalar(max_positions=10),
            FractionalKellyCap(kelly_fraction=0.25),
            DrawdownPositionScalar(max_dd_pct=20.0),
        ]),
        exit_pack=MarketDrivenExitPack(enabled=False),  # Opt-in after validation
        portfolio_allocator=PortfolioAllocator(),
        regime_model_adapter=RegimeModelAdapter(),
        execution_quality_overlay=SpreadAwareExecutionOverlay(enabled=True),
        options_model_adapter=OptionsModelAdapter(),
        strategy_extension_registry=StrategyExtensionRegistry(),
    )
