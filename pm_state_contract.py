"""Declarative, fail-closed state-history contracts for FxPM.

This module is a standalone authority.  It deliberately imports neither the
pipeline nor strategy implementations.  Integration code may ask it whether a
symbol/timeframe/regime cell can be reconstructed from a bounded prefix, needs
authenticated state, or must be quarantined.

The four state classes are intentionally exhaustive:

``FINITE_WINDOW``
    Exact output is determined by a finite, parameter-specific suffix.
``RECURSIVE_CONTRACTIVE``
    Seed error contracts analytically.  A prefix is admissible only when the
    coefficient and the cell's error/tick tolerance produce a finite bound.
``PATH_STATE``
    No scalar warm-up is authoritative.  An exact-registry checkpoint or an
    authenticated replay is mandatory.
``UNKNOWN``
    Quarantined.  Missing, ambiguous, cyclic, or malformed contracts are also
    quarantined rather than guessed.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Sequence


STATE_CONTRACT_SCHEMA_VERSION = "fxpm.state-contract.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class StateKind(str, Enum):
    FINITE_WINDOW = "FINITE_WINDOW"
    RECURSIVE_CONTRACTIVE = "RECURSIVE_CONTRACTIVE"
    PATH_STATE = "PATH_STATE"
    UNKNOWN = "UNKNOWN"


class EvidenceMode(str, Enum):
    CHECKPOINT = "CHECKPOINT"
    AUTHENTICATED_REPLAY = "AUTHENTICATED_REPLAY"


class ContractionFormula(str, Enum):
    EMA_SPAN = "EMA_SPAN"
    WILDER_PERIOD = "WILDER_PERIOD"
    KAMA_SLOW = "KAMA_SLOW"
    CONSTANT = "CONSTANT"


_BARS_PER_UTC_DAY = {
    "M5": 288,
    "M15": 96,
    "M30": 48,
    "H1": 24,
    "H4": 6,
    "D1": 1,
}


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _finite_positive(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number <= 0.0:
        return None
    return number


@dataclass(frozen=True)
class WindowTerm:
    """One parameter contribution to a finite suffix requirement."""

    key: str
    multiplier: float = 1.0
    offset: int = 0
    required: bool = False


@dataclass(frozen=True)
class FiniteWindowRule:
    """Conservative deterministic finite-window rule.

    ``infer_window_parameters`` recognizes the actual FxPM parameter naming
    convention (period/lookback/window/bars/order).  Explicit terms override
    the generic multiplier and are used for structures such as fractal wings.
    Calendar bars are conservative 24-hour maxima; shorter broker sessions do
    not weaken correctness.
    """

    fixed_bars: int = 1
    terms: tuple[WindowTerm, ...] = ()
    infer_window_parameters: bool = False
    calendar_days: int = 0

    def bound(self, parameters: Mapping[str, Any], timeframe: str) -> tuple[Optional[int], tuple[str, ...]]:
        reasons: list[str] = []
        try:
            fixed = int(self.fixed_bars)
            days = int(self.calendar_days)
        except (TypeError, ValueError, OverflowError):
            return None, ("FINITE_RULE_INVALID",)
        if fixed < 0 or days < 0:
            return None, ("FINITE_RULE_INVALID",)
        bars = max(0, fixed)
        if days:
            per_day = _BARS_PER_UTC_DAY.get(str(timeframe or "").upper())
            if per_day is None:
                reasons.append(f"TIMEFRAME_UNSUPPORTED:{timeframe}")
            else:
                bars = max(bars, days * per_day + 1)

        explicit = {term.key: term for term in self.terms}
        for term in self.terms:
            raw = parameters.get(term.key)
            if raw is None:
                if term.required:
                    reasons.append(f"PARAMETER_MISSING:{term.key}")
                continue
            value = _finite_positive(raw)
            if value is None:
                reasons.append(f"PARAMETER_INVALID:{term.key}")
                continue
            candidate = int(math.ceil(value * float(term.multiplier))) + int(term.offset)
            bars = max(bars, candidate)

        if self.infer_window_parameters:
            for key, raw in sorted(parameters.items(), key=lambda item: str(item[0])):
                name = str(key).lower()
                if key in explicit or isinstance(raw, bool):
                    continue
                if name.endswith("_hour") or name in {
                    "weekday", "tom_start_day", "tom_end_day", "adx_threshold",
                    "oversold", "overbought", "signal_mode", "period_type",
                }:
                    continue
                if not any(token in name for token in ("period", "lookback", "window", "bars", "order", "age")):
                    continue
                value = _finite_positive(raw)
                if value is None:
                    continue
                multiplier = 1.0
                offset = 2
                if "swing_order" in name:
                    multiplier = 5.0
                elif "wing_bars" in name:
                    multiplier = 2.0
                bars = max(bars, int(math.ceil(value * multiplier)) + offset)
        return (None, tuple(reasons)) if reasons else (max(1, bars), ())


@dataclass(frozen=True)
class ContractionTerm:
    """One analytic contraction surface within a recursive node."""

    formula: ContractionFormula | str
    parameter_keys: tuple[str, ...] = ()
    fixed_parameters: tuple[float, ...] = ()
    fixed_coefficient: Optional[float] = None
    stages: int = 1
    required: bool = True


@dataclass(frozen=True)
class RecursiveRule:
    terms: tuple[ContractionTerm, ...]

    @staticmethod
    def _coefficient(term: ContractionTerm, parameters: Mapping[str, Any]) -> tuple[Optional[float], tuple[str, ...]]:
        try:
            formula = ContractionFormula(term.formula)
        except (TypeError, ValueError):
            return None, (f"CONTRACTION_FORMULA_UNKNOWN:{term.formula}",)
        try:
            stages = int(term.stages)
        except (TypeError, ValueError, OverflowError):
            return None, ("CONTRACTION_STAGES_INVALID",)
        if stages <= 0:
            return None, ("CONTRACTION_STAGES_INVALID",)

        values = list(term.fixed_parameters)
        for key in term.parameter_keys:
            if key not in parameters:
                continue
            value = _finite_positive(parameters.get(key))
            if value is None:
                # Zero conventionally disables optional EMA filters.
                try:
                    if float(parameters.get(key)) == 0.0 and not term.required:
                        continue
                except (TypeError, ValueError, OverflowError):
                    pass
                return None, (f"PARAMETER_INVALID:{key}",)
            values.append(value)

        coefficients: list[float] = []
        if formula == ContractionFormula.CONSTANT:
            if term.fixed_coefficient is None:
                return None, ("CONTRACTION_COEFFICIENT_MISSING",)
            coefficients.append(float(term.fixed_coefficient))
        elif formula == ContractionFormula.EMA_SPAN:
            coefficients.extend((value - 1.0) / (value + 1.0) for value in values)
        elif formula == ContractionFormula.WILDER_PERIOD:
            coefficients.extend(1.0 - 1.0 / value for value in values)
        elif formula == ContractionFormula.KAMA_SLOW:
            coefficients.extend(1.0 - (2.0 / (value + 1.0)) ** 2 for value in values)

        if not coefficients:
            if term.required:
                keys = ",".join(term.parameter_keys) or "fixed_parameters"
                return None, (f"CONTRACTION_PARAMETER_MISSING:{keys}",)
            return None, ()
        coefficient = max(coefficients)
        if not math.isfinite(coefficient) or not 0.0 <= coefficient < 1.0:
            return None, (f"CONTRACTION_COEFFICIENT_INVALID:{coefficient}",)
        return float(coefficient), ()

    def bound(
        self,
        parameters: Mapping[str, Any],
        *,
        initial_error_bound: Optional[float],
        tolerance: Optional[float],
    ) -> tuple[Optional[int], tuple[float, ...], tuple[str, ...]]:
        initial = _finite_positive(initial_error_bound)
        target = _finite_positive(tolerance)
        if initial is None:
            return None, (), ("INITIAL_ERROR_BOUND_REQUIRED",)
        if target is None:
            return None, (), ("TOLERANCE_REQUIRED",)
        already_within_tolerance = target >= initial

        bounds: list[int] = []
        coefficients: list[float] = []
        reasons: list[str] = []
        for term in self.terms:
            coefficient, term_reasons = self._coefficient(term, parameters)
            reasons.extend(term_reasons)
            if coefficient is None:
                continue
            coefficients.append(coefficient)
            stages = int(term.stages)
            stage_target = target / stages
            if already_within_tolerance:
                per_stage = 0
            elif coefficient == 0.0:
                per_stage = 1
            else:
                per_stage = max(0, int(math.ceil(math.log(stage_target / initial) / math.log(coefficient))))
            bounds.append(stages * per_stage)
        if reasons:
            return None, tuple(coefficients), tuple(dict.fromkeys(reasons))
        if not bounds:
            # Every recursive term may be explicitly disabled for this cell.
            return 0, (), ()
        return max(bounds), tuple(coefficients), ()


@dataclass(frozen=True)
class StateContract:
    name: str
    kind: StateKind | str
    dependencies: tuple[str, ...] = ()
    finite_rule: Optional[FiniteWindowRule] = None
    recursive_rule: Optional[RecursiveRule] = None
    checkpoint_schema: Optional[str] = None
    transition_schema: Optional[str] = None
    description: str = ""

    def canonical(self) -> dict[str, Any]:
        def encode(value: Any) -> Any:
            if isinstance(value, Enum):
                return value.value
            if hasattr(value, "__dataclass_fields__"):
                return {key: encode(item) for key, item in asdict(value).items()}
            if isinstance(value, Mapping):
                return {str(key): encode(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [encode(item) for item in value]
            return value

        return encode(self)


@dataclass(frozen=True)
class StateEvidence:
    node: str
    mode: EvidenceMode | str
    identity_sha256: str
    registry_hash: str
    state_schema: str
    transition_schema: str


@dataclass(frozen=True)
class ResolvedNode:
    name: str
    kind: str
    dependencies: tuple[str, ...]
    local_context_bars: Optional[int]
    cumulative_context_bars: Optional[int]
    contraction_coefficients: tuple[float, ...] = ()
    state_evidence_mode: Optional[str] = None


@dataclass(frozen=True)
class StateResolution:
    status: str
    roots: tuple[str, ...]
    bounded_context_bars: Optional[int]
    required_path_state_nodes: tuple[str, ...]
    reason_codes: tuple[str, ...]
    nodes: tuple[ResolvedNode, ...]
    registry_hash: str
    decision_hash: str
    schema_version: str = STATE_CONTRACT_SCHEMA_VERSION

    @property
    def admissible(self) -> bool:
        return self.status == "ADMISSIBLE"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StateContractAuthority:
    """Immutable registry plus deterministic fail-closed DAG resolver."""

    def __init__(self, contracts: Iterable[StateContract]):
        supplied = tuple(contracts)
        groups: dict[str, list[StateContract]] = {}
        for contract in supplied:
            groups.setdefault(str(contract.name), []).append(contract)
        self._contracts = {
            name: definitions[0]
            for name, definitions in groups.items()
            if len(definitions) == 1
        }
        self._ambiguous = frozenset(
            name for name, definitions in groups.items() if len(definitions) != 1
        )
        canonical = sorted(
            (contract.canonical() for contract in supplied),
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), allow_nan=False),
        )
        self.registry_hash = _canonical_sha256(
            {
                "schema_version": STATE_CONTRACT_SCHEMA_VERSION,
                "contracts": canonical,
                "ambiguous_names": sorted(self._ambiguous),
            }
        )

    @property
    def contract_names(self) -> tuple[str, ...]:
        return tuple(sorted(set(self._contracts) | set(self._ambiguous)))

    @property
    def strategy_names(self) -> tuple[str, ...]:
        return tuple(
            name.split(":", 1)[1]
            for name in self.contract_names
            if name.startswith("strategy:")
        )

    def _evidence_valid(self, contract: StateContract, evidence: Optional[StateEvidence]) -> tuple[bool, Optional[str], tuple[str, ...]]:
        if evidence is None:
            return False, None, (f"PATH_STATE_EVIDENCE_REQUIRED:{contract.name}",)
        reasons: list[str] = []
        try:
            mode = EvidenceMode(evidence.mode)
        except (TypeError, ValueError):
            mode = None
            reasons.append(f"STATE_EVIDENCE_MODE_INVALID:{contract.name}")
        if evidence.node != contract.name:
            reasons.append(f"STATE_EVIDENCE_NODE_MISMATCH:{contract.name}")
        if evidence.registry_hash != self.registry_hash:
            reasons.append(f"STATE_EVIDENCE_REGISTRY_MISMATCH:{contract.name}")
        if not _SHA256_RE.fullmatch(str(evidence.identity_sha256 or "")):
            reasons.append(f"STATE_EVIDENCE_IDENTITY_INVALID:{contract.name}")
        if not contract.checkpoint_schema or not contract.transition_schema:
            reasons.append(f"PATH_STATE_SCHEMA_MISSING:{contract.name}")
        if evidence.state_schema != contract.checkpoint_schema:
            reasons.append(f"STATE_SCHEMA_MISMATCH:{contract.name}")
        if evidence.transition_schema != contract.transition_schema:
            reasons.append(f"TRANSITION_SCHEMA_MISMATCH:{contract.name}")
        return not reasons, mode.value if mode else None, tuple(reasons)

    def resolve(
        self,
        roots: Sequence[str],
        *,
        parameters: Optional[Mapping[str, Any]] = None,
        timeframe: str,
        initial_error_bound: Optional[float] = None,
        tolerance: Optional[float] = None,
        state_evidence: Optional[Mapping[str, StateEvidence]] = None,
    ) -> StateResolution:
        params = dict(parameters or {})
        evidence = dict(state_evidence or {})
        normalized_roots = tuple(sorted(dict.fromkeys(str(root) for root in roots)))
        reasons: list[str] = []
        required_path: set[str] = set()
        resolved: dict[str, ResolvedNode] = {}
        visiting: list[str] = []

        def visit(name: str) -> Optional[int]:
            if name in resolved:
                return resolved[name].cumulative_context_bars
            if name in visiting:
                cycle = "->".join(visiting[visiting.index(name):] + [name])
                reasons.append(f"CONTRACT_CYCLE:{cycle}")
                return None
            if name in self._ambiguous:
                reasons.append(f"AMBIGUOUS_CONTRACT:{name}")
                return None
            contract = self._contracts.get(name)
            if contract is None:
                reasons.append(f"CONTRACT_MISSING:{name}")
                return None
            try:
                kind = StateKind(contract.kind)
            except (TypeError, ValueError):
                reasons.append(f"CONTRACT_KIND_UNKNOWN:{name}:{contract.kind}")
                return None
            if kind == StateKind.UNKNOWN:
                reasons.append(f"CONTRACT_UNKNOWN:{name}")
                return None

            visiting.append(name)
            dependency_bounds: list[int] = []
            for dependency in sorted(dict.fromkeys(contract.dependencies)):
                bound = visit(dependency)
                if bound is not None:
                    dependency_bounds.append(bound)

            local = 0
            coefficients: tuple[float, ...] = ()
            evidence_mode: Optional[str] = None
            if contract.finite_rule is not None:
                finite_bound, finite_reasons = contract.finite_rule.bound(params, timeframe)
                reasons.extend(f"{name}:{reason}" for reason in finite_reasons)
                if finite_bound is not None:
                    local = max(local, finite_bound)

            if kind == StateKind.FINITE_WINDOW:
                if contract.finite_rule is None:
                    reasons.append(f"FINITE_RULE_MISSING:{name}")
            elif kind == StateKind.RECURSIVE_CONTRACTIVE:
                if contract.recursive_rule is None:
                    reasons.append(f"RECURSIVE_RULE_MISSING:{name}")
                else:
                    recursive_bound, coefficients, recursive_reasons = contract.recursive_rule.bound(
                        params,
                        initial_error_bound=initial_error_bound,
                        tolerance=tolerance,
                    )
                    reasons.extend(f"{name}:{reason}" for reason in recursive_reasons)
                    if recursive_bound is not None:
                        local = max(local, recursive_bound)
            elif kind == StateKind.PATH_STATE:
                valid, evidence_mode, evidence_reasons = self._evidence_valid(
                    contract, evidence.get(name)
                )
                if not valid:
                    required_path.add(name)
                    reasons.extend(evidence_reasons)
                # A valid checkpoint/replay replaces only the node's unbounded
                # state. Dependencies still need their own current context.

            cumulative = local + (max(dependency_bounds) if dependency_bounds else 0)
            visiting.pop()
            resolved[name] = ResolvedNode(
                name=name,
                kind=kind.value,
                dependencies=tuple(sorted(dict.fromkeys(contract.dependencies))),
                local_context_bars=local,
                cumulative_context_bars=cumulative,
                contraction_coefficients=coefficients,
                state_evidence_mode=evidence_mode,
            )
            return cumulative

        root_bounds = [visit(root) for root in normalized_roots]
        unique_reasons = tuple(dict.fromkeys(reasons))
        admissible = bool(normalized_roots) and not unique_reasons and all(
            bound is not None for bound in root_bounds
        )
        bounded = max((bound for bound in root_bounds if bound is not None), default=0) if admissible else None
        body = {
            "schema_version": STATE_CONTRACT_SCHEMA_VERSION,
            "status": "ADMISSIBLE" if admissible else "QUARANTINED",
            "roots": list(normalized_roots),
            "bounded_context_bars": bounded,
            "required_path_state_nodes": sorted(required_path),
            "reason_codes": list(unique_reasons),
            "nodes": [asdict(resolved[name]) for name in sorted(resolved)],
            "registry_hash": self.registry_hash,
        }
        return StateResolution(
            status=body["status"],
            roots=normalized_roots,
            bounded_context_bars=bounded,
            required_path_state_nodes=tuple(body["required_path_state_nodes"]),
            reason_codes=unique_reasons,
            nodes=tuple(resolved[name] for name in sorted(resolved)),
            registry_hash=self.registry_hash,
            decision_hash=_canonical_sha256(body),
        )

    def resolve_cell(
        self,
        strategy_name: str,
        *,
        parameters: Optional[Mapping[str, Any]],
        requested_features: Sequence[str] = (),
        timeframe: str,
        include_regime_controller: bool = True,
        initial_error_bound: Optional[float] = None,
        tolerance: Optional[float] = None,
        state_evidence: Optional[Mapping[str, StateEvidence]] = None,
    ) -> StateResolution:
        roots = [f"strategy:{strategy_name}"]
        roots.extend(
            feature if str(feature).startswith("feature:") else f"feature:{feature}"
            for feature in requested_features
        )
        if include_regime_controller:
            roots.append("controller:REGIME_HYSTERESIS")
        return self.resolve(
            roots,
            parameters=parameters,
            timeframe=timeframe,
            initial_error_bound=initial_error_bound,
            tolerance=tolerance,
            state_evidence=state_evidence,
        )


# The exact 100 names in pm_strategies.StrategyRegistry at this contract's
# creation. A registry addition is quarantined until this declarative authority
# is deliberately updated and its hash changes.
ACTUAL_STRATEGY_NAMES: tuple[str, ...] = (
    "EMACrossoverStrategy", "SupertrendStrategy", "MACDTrendStrategy",
    "ADXTrendStrategy", "IchimokuStrategy", "HullMATrendStrategy",
    "EMARibbonADXStrategy", "AroonTrendStrategy", "ADXDIStrengthStrategy",
    "KeltnerPullbackStrategy", "RSIExtremesStrategy", "BollingerBounceStrategy",
    "ZScoreMRStrategy", "StochasticReversalStrategy", "CCIReversalStrategy",
    "WilliamsRStrategy", "RSITrendFilteredMRStrategy", "StochRSITrendGateStrategy",
    "FisherTransformMRStrategy", "ZScoreVWAPReversionStrategy",
    "SessionVWAPReversionStrategy", "LiquiditySweepReversalStrategy",
    "DonchianBreakoutStrategy", "VolatilityBreakoutStrategy", "MomentumBurstStrategy",
    "SqueezeBreakoutStrategy", "KeltnerBreakoutStrategy", "PivotBreakoutStrategy",
    "MACDHistogramMomentumStrategy", "RegimeTransitionBreakoutStrategy",
    "InsideBarBreakoutStrategy", "NarrowRangeBreakoutStrategy",
    "VolumeSpikeMomentumStrategy", "ATRPercentileBreakoutStrategy",
    "TurtleSoupReversalStrategy", "PinBarReversalStrategy",
    "EngulfingPatternStrategy", "RSIDivergenceStrategy", "MACDDivergenceStrategy",
    "KeltnerFadeStrategy", "ROCExhaustionReversalStrategy", "OBVDivergenceStrategy",
    "EMAPullbackContinuationStrategy", "ParabolicSARTrendStrategy",
    "KaufmanAMATrendStrategy", "VortexTrendStrategy", "TRIXSignalStrategy",
    "RelativeVigorIndexStrategy", "VIDYABandTrendStrategy",
    "ChoppinessCompressionBreakoutStrategy", "OpeningRangeBreakoutStrategy",
    "GapContinuationStrategy", "VWAPTrendContinuationStrategy",
    "RSI2TrendPullbackStrategy", "DoubleEMATrendStrategy", "HeikinAshiTrendStrategy",
    "BollingerBandWalkStrategy", "PercentileChannelReversionStrategy",
    "MorningStarEveningStarStrategy", "PiercingDarkCloudStrategy",
    "HaramiReversalStrategy", "TweezerReversalStrategy", "ThreeBarReversalStrategy",
    "DojiBreakoutStrategy", "MarubozuMomentumStrategy", "ThreeSoldiersCrowsStrategy",
    "DoubleTopBottomReversalStrategy", "TriangleCompressionBreakoutStrategy",
    "FlagPullbackContinuationStrategy", "GapFadeStrategy",
    "OpeningDriveFailureStrategy", "PreviousSessionLevelRejectionStrategy",
    "SupportResistanceRetestBreakoutStrategy", "MoneyFlowIndexReversalStrategy",
    "ChaikinMoneyFlowTrendStrategy", "AccumulationDistributionBreakoutStrategy",
    "TimeOfDayMomentumStrategy", "SessionCloseReversionStrategy",
    "DayOfWeekBiasStrategy", "TurnOfMonthStrategy", "OvernightDriftStrategy",
    "IntradayRangeFadeStrategy", "SwapCarryTrendStrategy",
    "CarryFilteredPullbackStrategy", "FractalChannelBreakoutStrategy",
    "SwingFailureReversalStrategy", "AnchoredVWAPReclaimStrategy",
    "VolumeClimaxReversalStrategy", "EfficiencyRatioTrendStrategy",
    "EfficiencyRatioMeanReversionStrategy", "OpeningRangeRetestStrategy",
    "SessionVWAPPullbackTrendStrategy", "DarvasBoxBreakoutStrategy",
    "ADRExpansionBreakoutStrategy", "PriorDayRangeReclaimStrategy",
    "WeeklyMonthlyLevelRejectionStrategy", "PreviousWeekRangeBreakoutStrategy",
    "HeadAndShouldersNecklineBreakStrategy", "VarianceRatioTrendStrategy",
    "AutocorrelationReversalStrategy",
)


def _ema_term(*keys: str, fixed: Sequence[float] = (), required: bool = False, stages: int = 1) -> ContractionTerm:
    return ContractionTerm(
        ContractionFormula.EMA_SPAN,
        parameter_keys=tuple(keys),
        fixed_parameters=tuple(float(value) for value in fixed),
        required=required,
        stages=stages,
    )


def _wilder_term(*keys: str, fixed: Sequence[float] = (), required: bool = False) -> ContractionTerm:
    return ContractionTerm(
        ContractionFormula.WILDER_PERIOD,
        parameter_keys=tuple(keys),
        fixed_parameters=tuple(float(value) for value in fixed),
        required=required,
    )


def _feature_contracts() -> list[StateContract]:
    contracts = [
        StateContract("feature:OHLCV", StateKind.FINITE_WINDOW, finite_rule=FiniteWindowRule(2)),
        # A UTC-duration scalar cannot prove that a broker/session reset is
        # present.  These nodes become finite only through an authenticated
        # replay from the relevant boundary (or an exact state checkpoint).
        StateContract(
            "feature:SESSION_RESET", StateKind.PATH_STATE,
            checkpoint_schema="fxpm.session-reset-state.v1",
            transition_schema="fxpm.session-reset-transition.v1",
        ),
        StateContract(
            "feature:PREVIOUS_DAY_LEVELS", StateKind.PATH_STATE,
            checkpoint_schema="fxpm.previous-day-level-state.v1",
            transition_schema="fxpm.previous-day-level-transition.v1",
        ),
        StateContract(
            "feature:PREVIOUS_WEEK_LEVELS", StateKind.PATH_STATE,
            checkpoint_schema="fxpm.previous-week-level-state.v1",
            transition_schema="fxpm.previous-week-level-transition.v1",
        ),
        StateContract(
            "feature:PREVIOUS_MONTH_LEVELS", StateKind.PATH_STATE,
            checkpoint_schema="fxpm.previous-month-level-state.v1",
            transition_schema="fxpm.previous-month-level-transition.v1",
        ),
        StateContract("feature:REGIME_SCORE_SURFACE", StateKind.FINITE_WINDOW, finite_rule=FiniteWindowRule(400)),
        StateContract(
            "controller:REGIME_HYSTERESIS", StateKind.PATH_STATE,
            dependencies=("feature:REGIME_SCORE_SURFACE",),
            checkpoint_schema="fxpm.regime-hysteresis-state.v1",
            transition_schema="fxpm.regime-hysteresis-transition.v1",
        ),
    ]
    for period in (7, 10, 14, 20):
        contracts.append(StateContract(f"feature:ATR_{period}", StateKind.FINITE_WINDOW, dependencies=("feature:OHLCV",), finite_rule=FiniteWindowRule(period + 1)))
    for period in (5, 8, 10, 13, 20, 21, 30, 50, 100, 200):
        contracts.append(StateContract(f"feature:SMA_{period}", StateKind.FINITE_WINDOW, dependencies=("feature:OHLCV",), finite_rule=FiniteWindowRule(period)))
        contracts.append(StateContract(f"feature:EMA_{period}", StateKind.RECURSIVE_CONTRACTIVE, dependencies=("feature:OHLCV",), recursive_rule=RecursiveRule((_ema_term(fixed=(period,), required=True),))))
    for period in (2, 7, 14, 21):
        contracts.append(StateContract(f"feature:RSI_{period}", StateKind.RECURSIVE_CONTRACTIVE, dependencies=("feature:OHLCV",), finite_rule=FiniteWindowRule(period + 1), recursive_rule=RecursiveRule((_wilder_term(fixed=(period,), required=True),))))
    for name in ("BB_MID_20", "BB_UPPER_20", "BB_LOWER_20"):
        contracts.append(StateContract(f"feature:{name}", StateKind.FINITE_WINDOW, dependencies=("feature:OHLCV",), finite_rule=FiniteWindowRule(20)))
    macd_rule = RecursiveRule((_ema_term(fixed=(12, 26, 9), required=True, stages=2),))
    for name in ("MACD", "MACD_SIGNAL", "MACD_HIST"):
        contracts.append(StateContract(f"feature:{name}", StateKind.RECURSIVE_CONTRACTIVE, dependencies=("feature:OHLCV",), recursive_rule=macd_rule))
    finite_features = {
        "STOCH_K": 14, "STOCH_D": 17, "ADX": 29, "PLUS_DI": 15, "MINUS_DI": 15,
        "CCI": 20, "WILLR": 14, "DONCHIAN_HIGH_20": 20, "DONCHIAN_LOW_20": 20,
        "DONCHIAN_HIGH_50": 50, "DONCHIAN_LOW_50": 50, "HULL_MA": 25,
        "CHANGE": 2, "CHANGE_PCT": 2, "VOLATILITY": 20,
    }
    for name, bars in finite_features.items():
        dependencies = ("feature:ATR_14",) if name in {"ADX", "PLUS_DI", "MINUS_DI"} else ("feature:OHLCV",)
        contracts.append(StateContract(f"feature:{name}", StateKind.FINITE_WINDOW, dependencies=dependencies, finite_rule=FiniteWindowRule(bars)))
    for name in ("KC_MID", "KC_UPPER", "KC_LOWER"):
        contracts.append(StateContract(f"feature:{name}", StateKind.FINITE_WINDOW, dependencies=("feature:EMA_20", "feature:ATR_20"), finite_rule=FiniteWindowRule(1)))
    for name in ("TREND_SCORE", "RANGE_SCORE", "BREAKOUT_SCORE", "CHOP_SCORE", "REGIME_RAW", "REGIME_STRENGTH", "REGIME_GAP", "REGIME_STRENGTH_LIVE"):
        contracts.append(StateContract(f"feature:{name}", StateKind.FINITE_WINDOW, dependencies=("feature:REGIME_SCORE_SURFACE",), finite_rule=FiniteWindowRule(2)))
    for name in ("REGIME", "REGIME_LIVE"):
        contracts.append(StateContract(f"feature:{name}", StateKind.PATH_STATE, dependencies=("feature:REGIME_SCORE_SURFACE",), checkpoint_schema="fxpm.regime-hysteresis-state.v1", transition_schema="fxpm.regime-hysteresis-transition.v1"))
    return contracts


_PATH_STRATEGIES = {
    "SupertrendStrategy": "fxpm.supertrend-state.v1",
    "RegimeTransitionBreakoutStrategy": "fxpm.regime-transition-state.v1",
    "OBVDivergenceStrategy": "fxpm.obv-state.v1",
    "ParabolicSARTrendStrategy": "fxpm.psar-state.v1",
    "VIDYABandTrendStrategy": "fxpm.vidya-state.v1",
    "AccumulationDistributionBreakoutStrategy": "fxpm.adl-state.v1",
    # Production veto: the confirmed pivot carried forward into the current
    # suffix must be authenticated, not inferred from an arbitrary slice.
    "FractalChannelBreakoutStrategy": "fxpm.fractal-channel-state.v1",
}

_RECURSIVE_STRATEGIES = {
    "EMACrossoverStrategy", "MACDTrendStrategy", "KeltnerPullbackStrategy",
    "RSIExtremesStrategy", "StochasticReversalStrategy", "CCIReversalStrategy",
    "WilliamsRStrategy", "RSITrendFilteredMRStrategy", "StochRSITrendGateStrategy",
    "FisherTransformMRStrategy", "SqueezeBreakoutStrategy", "KeltnerBreakoutStrategy",
    "EMARibbonADXStrategy", "MACDHistogramMomentumStrategy", "SessionVWAPReversionStrategy",
    "LiquiditySweepReversalStrategy", "RSIDivergenceStrategy", "MACDDivergenceStrategy",
    "KeltnerFadeStrategy", "EMAPullbackContinuationStrategy", "KaufmanAMATrendStrategy",
    "TRIXSignalStrategy", "OpeningRangeBreakoutStrategy", "VWAPTrendContinuationStrategy",
    "RSI2TrendPullbackStrategy", "DoubleEMATrendStrategy", "HeikinAshiTrendStrategy",
    "MorningStarEveningStarStrategy", "PiercingDarkCloudStrategy", "HaramiReversalStrategy",
    "TweezerReversalStrategy", "ThreeBarReversalStrategy", "DojiBreakoutStrategy",
    "MarubozuMomentumStrategy", "ThreeSoldiersCrowsStrategy",
    "DoubleTopBottomReversalStrategy", "FlagPullbackContinuationStrategy",
    "MoneyFlowIndexReversalStrategy", "ChaikinMoneyFlowTrendStrategy",
    "TurnOfMonthStrategy", "SwapCarryTrendStrategy", "CarryFilteredPullbackStrategy",
    "AnchoredVWAPReclaimStrategy", "EfficiencyRatioTrendStrategy",
    "SessionVWAPPullbackTrendStrategy", "PreviousWeekRangeBreakoutStrategy",
    "VarianceRatioTrendStrategy",
}

_SESSION_STRATEGIES = {
    "SessionVWAPReversionStrategy", "OpeningRangeBreakoutStrategy", "GapContinuationStrategy",
    "VWAPTrendContinuationStrategy", "GapFadeStrategy", "OpeningDriveFailureStrategy",
    "PreviousSessionLevelRejectionStrategy", "SessionCloseReversionStrategy",
    "OvernightDriftStrategy", "IntradayRangeFadeStrategy", "AnchoredVWAPReclaimStrategy",
    "OpeningRangeRetestStrategy", "SessionVWAPPullbackTrendStrategy",
}

_CALENDAR_DEPENDENCIES = {
    "ADRExpansionBreakoutStrategy": ("feature:PREVIOUS_DAY_LEVELS",),
    "PriorDayRangeReclaimStrategy": ("feature:PREVIOUS_DAY_LEVELS",),
    "WeeklyMonthlyLevelRejectionStrategy": ("feature:PREVIOUS_MONTH_LEVELS",),
    "PreviousWeekRangeBreakoutStrategy": ("feature:PREVIOUS_WEEK_LEVELS",),
}


def _strategy_recursive_rule(name: str) -> RecursiveRule:
    if name == "KaufmanAMATrendStrategy":
        return RecursiveRule((ContractionTerm(ContractionFormula.KAMA_SLOW, ("slow_period",), required=True),))
    if name == "FisherTransformMRStrategy":
        return RecursiveRule((ContractionTerm(ContractionFormula.CONSTANT, fixed_coefficient=0.5, stages=2),))
    if name == "TRIXSignalStrategy":
        return RecursiveRule((_ema_term("trix_period", "signal_period", required=True, stages=4),))
    if name == "DoubleEMATrendStrategy":
        return RecursiveRule((_ema_term("fast_period", "slow_period", required=True, stages=2),))
    if name == "HeikinAshiTrendStrategy":
        return RecursiveRule((
            ContractionTerm(ContractionFormula.CONSTANT, fixed_coefficient=0.5),
            _ema_term("trend_ema_period", required=False),
        ))
    terms = [
        _ema_term(
            "ema_period", "ema_fast", "ema_mid", "ema_slow", "fast_period", "slow_period",
            "ema_trend_period", "trend_ema_period", "ema_filter_period", "trend_ema",
            "kc_period", "period", "macd_fast", "macd_slow", "macd_signal",
            required=False,
            stages=2 if "MACD" in name else 1,
        ),
        _wilder_term("rsi_period", required=False),
    ]
    if name in {"StochasticReversalStrategy", "CCIReversalStrategy", "WilliamsRStrategy"}:
        terms.append(_ema_term(fixed=(200,), required=False))
    return RecursiveRule(tuple(terms))


def _strategy_contracts() -> list[StateContract]:
    contracts: list[StateContract] = []
    generic_window = FiniteWindowRule(
        fixed_bars=3,
        terms=(
            WindowTerm("wing_bars", multiplier=2.0, offset=2),
            WindowTerm("swing_order", multiplier=5.0, offset=2),
            WindowTerm("pattern_max_age_bars", offset=2),
            WindowTerm("pivot_max_age_bars", offset=2),
            WindowTerm("setup_max_age_bars", offset=2),
        ),
        infer_window_parameters=True,
    )
    for name in ACTUAL_STRATEGY_NAMES:
        dependencies = ["feature:ATR_14"]
        if name in _SESSION_STRATEGIES:
            dependencies.append("feature:SESSION_RESET")
        dependencies.extend(_CALENDAR_DEPENDENCIES.get(name, ()))
        if name == "RegimeTransitionBreakoutStrategy":
            dependencies.append("controller:REGIME_HYSTERESIS")
        if name in _PATH_STRATEGIES:
            schema = _PATH_STRATEGIES[name]
            contracts.append(StateContract(
                f"strategy:{name}", StateKind.PATH_STATE,
                dependencies=tuple(sorted(set(dependencies))),
                finite_rule=generic_window,
                checkpoint_schema=schema,
                transition_schema=schema.replace("state", "transition"),
                description="Registered strategy contains non-contractive path state.",
            ))
        elif name in _RECURSIVE_STRATEGIES:
            contracts.append(StateContract(
                f"strategy:{name}", StateKind.RECURSIVE_CONTRACTIVE,
                dependencies=tuple(sorted(set(dependencies))),
                finite_rule=generic_window,
                recursive_rule=_strategy_recursive_rule(name),
                description="Registered strategy contains analytically contractive recursive state.",
            ))
        else:
            contracts.append(StateContract(
                f"strategy:{name}", StateKind.FINITE_WINDOW,
                dependencies=tuple(sorted(set(dependencies))),
                finite_rule=generic_window,
                description="Registered strategy is suffix-bounded under its declared parameters.",
            ))
    return contracts


DEFAULT_AUTHORITY = StateContractAuthority((*_feature_contracts(), *_strategy_contracts()))
DEFAULT_REGISTRY_HASH = DEFAULT_AUTHORITY.registry_hash


__all__ = [
    "ACTUAL_STRATEGY_NAMES",
    "ContractionFormula",
    "ContractionTerm",
    "DEFAULT_AUTHORITY",
    "DEFAULT_REGISTRY_HASH",
    "EvidenceMode",
    "FiniteWindowRule",
    "RecursiveRule",
    "ResolvedNode",
    "STATE_CONTRACT_SCHEMA_VERSION",
    "StateContract",
    "StateContractAuthority",
    "StateEvidence",
    "StateKind",
    "StateResolution",
    "WindowTerm",
]
