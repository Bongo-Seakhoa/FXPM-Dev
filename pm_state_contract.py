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
import itertools
import json
import math
import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Sequence

from pm_current_refit_parameters import (
    iter_parameter_domain_projection,
    validate_parameter_domain_member,
)


STATE_CONTRACT_SCHEMA_VERSION = "fxpm.state-contract.v2"
STATE_CANDIDATE_DOMAIN_SCHEMA_VERSION = "fxpm.state-candidate-domain.v2"
STATE_DOMAIN_CEILING_SCHEMA_VERSION = "fxpm.state-domain-ceiling.v1"
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

# Scores, readiness and lagged views share the kernel's recursive/path state.
STATE_FEATURE_ALIASES: Mapping[str, str] = {
    name: "controller:REGIME_HYSTERESIS"
    for name in (
        "TREND_SCORE", "RANGE_SCORE", "BREAKOUT_SCORE", "CHOP_SCORE",
        "REGIME_RAW", "REGIME", "REGIME_STRENGTH", "REGIME_GAP",
        "REGIME_WARMUP", "REGIME_LIVE", "REGIME_STRENGTH_LIVE",
        "REGIME_RAW_STRENGTH", "REGIME_RAW_STRENGTH_LIVE", "REGIME_ADX",
        "REGIME_ATR", "REGIME_BB_WIDTH", "REGIME_CONFIRM_EVIDENCE",
    )
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


def _inferred_window_value(key: Any, value: Any) -> Optional[int]:
    """Return the exact generic finite-window contribution, if any.

    Keeping the name/value predicate in one place is important: domain-ceiling
    projection may omit an axis only when ``FiniteWindowRule.bound`` cannot
    read it.  A second, approximately similar predicate would turn that
    efficiency optimization into an under-bounding risk.
    """

    name = str(key).lower()
    if isinstance(value, bool):
        return None
    if name.endswith("_hour") or name in {
        "weekday",
        "tom_start_day",
        "tom_end_day",
        "adx_threshold",
        "oversold",
        "overbought",
        "signal_mode",
        "period_type",
    }:
        return None
    if not any(
        token in name
        for token in ("period", "lookback", "window", "bars", "order", "age")
    ):
        return None
    number = _finite_positive(value)
    if number is None:
        return None
    multiplier = 1.0
    offset = 2
    if "swing_order" in name:
        multiplier = 5.0
    elif "wing_bars" in name:
        multiplier = 2.0
    return int(math.ceil(number * multiplier)) + offset


@dataclass(frozen=True)
class WindowTerm:
    """One parameter contribution to a finite suffix requirement."""

    key: str
    multiplier: float = 1.0
    offset: int = 0
    required: bool = False
    allow_zero: bool = False


@dataclass(frozen=True)
class WindowExpression:
    """Additive parameter expression within a finite suffix bound.

    Independent rolling requirements combine by maximum, but nested structures
    combine by addition.  For example, a retained fractal may be
    ``pivot_max_age_bars + 2 * wing_bars`` old.  Treating those axes as two
    independent maxima understates the required prefix.
    """

    terms: tuple[WindowTerm, ...]
    offset: int = 0


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
    expressions: tuple[WindowExpression, ...] = ()
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

        all_explicit_terms = self.terms + tuple(
            term for expression in self.expressions for term in expression.terms
        )
        explicit = {term.key: term for term in all_explicit_terms}

        def term_value(term: WindowTerm) -> tuple[Optional[int], Optional[str]]:
            raw = parameters.get(term.key)
            if raw is None:
                if term.required:
                    return None, f"PARAMETER_MISSING:{term.key}"
                return None, None
            if isinstance(raw, bool):
                return None, f"PARAMETER_INVALID:{term.key}"
            try:
                value = float(raw)
            except (TypeError, ValueError, OverflowError):
                return None, f"PARAMETER_INVALID:{term.key}"
            if (
                not math.isfinite(value)
                or value < 0.0
                or (value == 0.0 and not term.allow_zero)
            ):
                return None, f"PARAMETER_INVALID:{term.key}"
            try:
                multiplier = float(term.multiplier)
                offset = int(term.offset)
                raw_offset = float(term.offset)
            except (TypeError, ValueError, OverflowError):
                return None, f"FINITE_TERM_INVALID:{term.key}"
            if (
                isinstance(term.multiplier, bool)
                or isinstance(term.offset, bool)
                or not math.isfinite(multiplier)
                or multiplier < 0.0
                or offset < 0
                or float(offset) != raw_offset
            ):
                return None, f"FINITE_TERM_INVALID:{term.key}"
            return int(math.ceil(value * multiplier)) + offset, None

        for term in self.terms:
            candidate, reason = term_value(term)
            if reason:
                reasons.append(reason)
            elif candidate is not None:
                bars = max(bars, candidate)

        for expression in self.expressions:
            try:
                candidate = int(expression.offset)
                raw_offset = float(expression.offset)
            except (TypeError, ValueError, OverflowError):
                reasons.append("FINITE_EXPRESSION_INVALID")
                continue
            if (
                isinstance(expression.offset, bool)
                or candidate < 0
                or float(candidate) != raw_offset
                or not expression.terms
            ):
                reasons.append("FINITE_EXPRESSION_INVALID")
                continue
            expression_valid = True
            for term in expression.terms:
                contribution, reason = term_value(term)
                if reason:
                    reasons.append(reason)
                    expression_valid = False
                elif contribution is not None:
                    candidate += contribution
            if expression_valid:
                bars = max(bars, candidate)

        if self.infer_window_parameters:
            for key, raw in sorted(parameters.items(), key=lambda item: str(item[0])):
                if key in explicit:
                    continue
                candidate = _inferred_window_value(key, raw)
                if candidate is not None:
                    bars = max(bars, candidate)
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


@dataclass(frozen=True)
class StateDomainCeiling:
    """DataFrame-free bound over an explicit legal domain, including coupled rows."""

    status: str
    strategy_name: str
    timeframe: str
    effective_domain_sha256: str
    authority_candidate_domain_sha256: str
    registry_hash: str
    include_regime_controller: bool
    initial_error_bound_hex: str
    tolerance_hex: str
    parameter_domain_sha256: str
    full_domain_point_count: int
    state_parameter_keys: tuple[str, ...]
    state_projection_point_count: int
    requested_features: tuple[str, ...]
    bounded_context_bars: Optional[int]
    witness_state_parameters_sha256: Optional[str]
    witness_resolution_decision_sha256: Optional[str]
    projection_roster_sha256: str
    reason_codes: tuple[str, ...]
    decision_hash: str
    schema_version: str = STATE_DOMAIN_CEILING_SCHEMA_VERSION

    @property
    def proved(self) -> bool:
        return self.status == "PROVED"

    def to_dict(self) -> dict[str, Any]:
        # Lists are deliberate here.  A ceiling is carried through canonical
        # JSON receipts and must compare byte-for-byte after a JSON reopen.
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "strategy_name": self.strategy_name,
            "timeframe": self.timeframe,
            "effective_domain_sha256": self.effective_domain_sha256,
            "authority_candidate_domain_sha256": (
                self.authority_candidate_domain_sha256
            ),
            "registry_hash": self.registry_hash,
            "include_regime_controller": self.include_regime_controller,
            "initial_error_bound_hex": self.initial_error_bound_hex,
            "tolerance_hex": self.tolerance_hex,
            "parameter_domain_sha256": self.parameter_domain_sha256,
            "full_domain_point_count": self.full_domain_point_count,
            "state_parameter_keys": list(self.state_parameter_keys),
            "state_projection_point_count": self.state_projection_point_count,
            "requested_features": list(self.requested_features),
            "bounded_context_bars": self.bounded_context_bars,
            "witness_state_parameters_sha256": (
                self.witness_state_parameters_sha256
            ),
            "witness_resolution_decision_sha256": (
                self.witness_resolution_decision_sha256
            ),
            "projection_roster_sha256": self.projection_roster_sha256,
            "reason_codes": list(self.reason_codes),
            "decision_hash": self.decision_hash,
        }


class StateContractAuthority:
    """Immutable registry plus deterministic fail-closed DAG resolver."""

    def __init__(
        self,
        contracts: Iterable[StateContract],
        *,
        candidate_domain_hash: Optional[str] = None,
    ):
        supplied = tuple(contracts)
        if candidate_domain_hash is not None and not _SHA256_RE.fullmatch(
            str(candidate_domain_hash)
        ):
            raise ValueError("candidate_domain_hash must be a lowercase SHA-256 digest")
        self.candidate_domain_hash = candidate_domain_hash
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
                "candidate_domain_hash": candidate_domain_hash,
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

    @staticmethod
    def _cell_roots(
        strategy_name: str,
        requested_features: Sequence[str],
        *,
        include_regime_controller: bool,
    ) -> tuple[str, ...]:
        roots = [f"strategy:{strategy_name}"]
        for feature in requested_features:
            feature_name = str(feature)
            bare_name = (
                feature_name.split(":", 1)[1]
                if feature_name.startswith("feature:")
                else feature_name
            )
            alias = STATE_FEATURE_ALIASES.get(bare_name)
            roots.append(
                alias
                or (
                    feature_name
                    if feature_name.startswith("feature:")
                    else f"feature:{feature_name}"
                )
            )
        if include_regime_controller:
            roots.append("controller:REGIME_HYSTERESIS")
        return tuple(sorted(dict.fromkeys(roots)))

    def _state_parameter_keys(
        self,
        roots: Sequence[str],
        parameter_domains: Mapping[str, Sequence[Any]],
    ) -> tuple[str, ...]:
        """Return exactly the domain axes the reachable resolver DAG may read."""

        keys: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            contract = self._contracts.get(name)
            if contract is None:
                return
            for dependency in contract.dependencies:
                visit(dependency)
            finite = contract.finite_rule
            if finite is not None:
                explicit_terms = finite.terms + tuple(
                    term
                    for expression in finite.expressions
                    for term in expression.terms
                )
                explicit = {term.key for term in explicit_terms}
                keys.update(explicit.intersection(parameter_domains))
                if finite.infer_window_parameters:
                    for key, choices in parameter_domains.items():
                        if key in explicit:
                            continue
                        if any(
                            _inferred_window_value(key, choice) is not None
                            for choice in choices
                        ):
                            keys.add(key)
            recursive = contract.recursive_rule
            if recursive is not None:
                for term in recursive.terms:
                    keys.update(
                        key
                        for key in term.parameter_keys
                        if key in parameter_domains
                    )

        for root in roots:
            visit(root)
        return tuple(sorted(keys))

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
        roots = self._cell_roots(
            strategy_name,
            requested_features,
            include_regime_controller=include_regime_controller,
        )
        return self.resolve(
            roots,
            parameters=parameters,
            timeframe=timeframe,
            initial_error_bound=initial_error_bound,
            tolerance=tolerance,
            state_evidence=state_evidence,
        )

    def resolve_cell_domain_ceiling(
        self,
        strategy_name: str,
        *,
        parameter_domains: Mapping[str, Sequence[Any]],
        requested_features: Sequence[str],
        timeframe: str,
        effective_domain_hash: str,
        include_regime_controller: bool = False,
        initial_error_bound: Optional[float] = None,
        tolerance: Optional[float] = None,
        state_evidence: Optional[Mapping[str, StateEvidence]] = None,
        parameter_domain: Optional[Mapping[str, Any]] = None,
    ) -> StateDomainCeiling:
        """Prove one safe state-context ceiling without reading market data.

        The complete legal domain remains hashed and counted. The resolver
        visits only distinct state-relevant rows, retaining complete legal
        witnesses for coupled parameters. The original Cartesian path retains
        its issued hashes when no structured domain is supplied.
        """

        normalized_name = str(strategy_name or "")
        normalized_timeframe = str(timeframe or "").upper()
        if not normalized_name:
            raise ValueError("strategy_name must be non-empty")
        if normalized_timeframe not in _BARS_PER_UTC_DAY:
            raise ValueError("timeframe is unsupported")
        if not _SHA256_RE.fullmatch(str(effective_domain_hash or "")):
            raise ValueError("effective_domain_hash must be a lowercase SHA-256 digest")
        if not _SHA256_RE.fullmatch(str(self.candidate_domain_hash or "")):
            raise ValueError("authority candidate domain must be a lowercase SHA-256 digest")
        if type(include_regime_controller) is not bool:
            raise ValueError("include_regime_controller must be bool")
        initial = _finite_positive(initial_error_bound)
        target = _finite_positive(tolerance)
        if initial is None or target is None:
            raise ValueError("positive initial_error_bound and tolerance are required")
        if not isinstance(parameter_domains, Mapping) or not parameter_domains:
            raise ValueError("parameter_domains must be a non-empty mapping")

        normalized_domains: dict[str, tuple[Any, ...]] = {}
        for raw_key in sorted(parameter_domains, key=lambda item: str(item)):
            if type(raw_key) is not str or not raw_key:
                raise ValueError("parameter domain keys must be non-empty strings")
            raw_choices = parameter_domains[raw_key]
            if type(raw_choices) not in {list, tuple} or not raw_choices:
                raise ValueError(f"parameter domain {raw_key} must be non-empty")
            choices = tuple(raw_choices)
            try:
                encoded = [
                    json.dumps(
                        choice,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        allow_nan=False,
                    )
                    for choice in choices
                ]
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    f"parameter domain {raw_key} is not canonical JSON"
                ) from exc
            if len(encoded) != len(set(encoded)):
                raise ValueError(f"parameter domain {raw_key} has duplicate choices")
            normalized_domains[raw_key] = choices

        if type(requested_features) not in {list, tuple}:
            raise ValueError("requested_features must be a list or tuple")
        normalized_features = tuple(
            sorted(dict.fromkeys(str(value) for value in requested_features))
        )
        if any(not value for value in normalized_features):
            raise ValueError("requested_features must be non-empty strings")
        roots = self._cell_roots(
            normalized_name,
            normalized_features,
            include_regime_controller=include_regime_controller,
        )
        state_keys = self._state_parameter_keys(roots, normalized_domains)
        if parameter_domain is None:
            full_count = math.prod(len(choices) for choices in normalized_domains.values())
            projection_count = math.prod(len(normalized_domains[key]) for key in state_keys)
            parameter_domain_sha256 = _canonical_sha256(
                {"parameter_domains": {key: list(choices) for key, choices in normalized_domains.items()}}
            )
            base_parameters = {key: choices[0] for key, choices in normalized_domains.items()}
            axes = [normalized_domains[key] for key in state_keys]
            projections = (
                ({**base_parameters, **dict(zip(state_keys, values))}, dict(zip(state_keys, values)))
                for values in itertools.product(*axes)
            )
        else:
            domain = dict(parameter_domain)
            validate_parameter_domain_member(domain, domain.get("defaults"))
            marginal = {key: [value] for key, value in domain["defaults"].items()}
            for axis in domain["axes"]:
                for column, name in enumerate(axis["names"]):
                    unique = {}
                    for row in axis["choices"]:
                        unique.setdefault(_canonical_sha256(row[column]), row[column])
                    marginal[name] = list(unique.values())
            if set(marginal) != set(normalized_domains) or any(
                {_canonical_sha256(value) for value in marginal[key]}
                != {_canonical_sha256(value) for value in normalized_domains[key]}
                for key in marginal
            ):
                raise ValueError("parameter domain marginals differ from parameter_domains")
            full_count = domain["point_count"]
            projection_count = 0
            parameter_domain_sha256 = _canonical_sha256({"parameter_domain": domain})
            projections = iter_parameter_domain_projection(domain, state_keys)
        evidence = dict(state_evidence or {})
        if any(not isinstance(item, StateEvidence) for item in evidence.values()):
            raise ValueError("state_evidence values must be StateEvidence")
        projection_roster: list[dict[str, Any]] = []
        reasons: list[str] = []
        maximum: Optional[int] = None
        witness_parameters_sha256: Optional[str] = None
        witness_decision_sha256: Optional[str] = None
        for parameters, state_parameters in projections:
            if parameter_domain is not None:
                projection_count += 1
            resolution = self.resolve_cell(
                normalized_name,
                parameters=parameters,
                requested_features=normalized_features,
                timeframe=normalized_timeframe,
                include_regime_controller=include_regime_controller,
                initial_error_bound=initial,
                tolerance=target,
                state_evidence=evidence,
            )
            state_parameters_sha256 = _canonical_sha256(state_parameters)
            projection_roster.append(
                {
                    "state_parameters_sha256": state_parameters_sha256,
                    "resolution_decision_sha256": resolution.decision_hash,
                    "status": resolution.status,
                    "bounded_context_bars": resolution.bounded_context_bars,
                    "reason_codes": list(resolution.reason_codes),
                }
            )
            if not resolution.admissible:
                reasons.extend(resolution.reason_codes)
                continue
            bound = resolution.bounded_context_bars
            if type(bound) is not int or bound < 0:
                reasons.append("DOMAIN_RESOLUTION_BOUND_INVALID")
                continue
            if maximum is None or bound > maximum:
                maximum = bound
                witness_parameters_sha256 = state_parameters_sha256
                witness_decision_sha256 = resolution.decision_hash

        unique_reasons = tuple(dict.fromkeys(reasons))
        proved = not unique_reasons and maximum is not None
        body = {
            "schema_version": STATE_DOMAIN_CEILING_SCHEMA_VERSION,
            "status": "PROVED" if proved else "QUARANTINED",
            "strategy_name": normalized_name,
            "timeframe": normalized_timeframe,
            "effective_domain_sha256": str(effective_domain_hash),
            "authority_candidate_domain_sha256": str(self.candidate_domain_hash),
            "registry_hash": self.registry_hash,
            "include_regime_controller": include_regime_controller,
            "initial_error_bound_hex": initial.hex(),
            "tolerance_hex": target.hex(),
            "parameter_domain_sha256": parameter_domain_sha256,
            "full_domain_point_count": full_count,
            "state_parameter_keys": list(state_keys),
            "state_projection_point_count": projection_count,
            "requested_features": list(normalized_features),
            "bounded_context_bars": maximum if proved else None,
            "witness_state_parameters_sha256": (
                witness_parameters_sha256 if proved else None
            ),
            "witness_resolution_decision_sha256": (
                witness_decision_sha256 if proved else None
            ),
            "projection_roster_sha256": _canonical_sha256(projection_roster),
            "reason_codes": list(unique_reasons),
        }
        # Evidence identity is receipt provenance, not a planning decision.
        # The enclosing production record binds its exact replay bytes; the
        # ceiling follows StateResolution and binds the admissible mode only.
        decision_hash = _canonical_sha256(body)
        return StateDomainCeiling(
            status=body["status"],
            strategy_name=normalized_name,
            timeframe=normalized_timeframe,
            effective_domain_sha256=str(effective_domain_hash),
            authority_candidate_domain_sha256=str(self.candidate_domain_hash),
            registry_hash=self.registry_hash,
            include_regime_controller=include_regime_controller,
            initial_error_bound_hex=initial.hex(),
            tolerance_hex=target.hex(),
            parameter_domain_sha256=parameter_domain_sha256,
            full_domain_point_count=full_count,
            state_parameter_keys=state_keys,
            state_projection_point_count=projection_count,
            requested_features=normalized_features,
            bounded_context_bars=maximum if proved else None,
            witness_state_parameters_sha256=(
                witness_parameters_sha256 if proved else None
            ),
            witness_resolution_decision_sha256=(
                witness_decision_sha256 if proved else None
            ),
            projection_roster_sha256=body["projection_roster_sha256"],
            reason_codes=unique_reasons,
            decision_hash=decision_hash,
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

# SHA-256 of the exact sorted StrategyRegistry candidate-domain document:
# schema version plus every strategy name, normalized defaults, grid axes and
# coupled_groups. The authority stays independent of strategy imports while
# tests make any candidate addition, removal, or value drift explicit.
STRATEGY_CANDIDATE_DOMAIN_SHA256 = (
    "a4305a9d256602158563c9edb8eb4ee2c857eba710825555a3f621327f0f25ef"
)


def strategy_candidate_domain_sha256(
    strategy_documents: Sequence[Mapping[str, Any]],
) -> str:
    """Hash a caller-normalized, name-sorted strategy candidate domain."""

    documents = [dict(document) for document in strategy_documents]
    names = [str(document.get("name", "")) for document in documents]
    if (
        names != sorted(names)
        or len(names) != len(set(names))
        or any(not name for name in names)
    ):
        raise ValueError("strategy candidate documents must have unique sorted names")
    return _canonical_sha256(
        {
            "schema_version": STATE_CANDIDATE_DOMAIN_SCHEMA_VERSION,
            "strategies": documents,
        }
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
        StateContract(
            "controller:REGIME_HYSTERESIS", StateKind.PATH_STATE,
            dependencies=("feature:OHLCV",),
            checkpoint_schema="fxpm.regime-stream-state.v2",
            transition_schema="fxpm.regime-stream-transition.v2",
        ),
    ]
    for period in (7, 10, 14, 20):
        contracts.append(StateContract(f"feature:ATR_{period}", StateKind.FINITE_WINDOW, dependencies=("feature:OHLCV",), finite_rule=FiniteWindowRule(period + 1)))
    for period in (5, 8, 10, 13, 20, 21, 30, 50, 100, 200):
        contracts.append(StateContract(f"feature:SMA_{period}", StateKind.FINITE_WINDOW, dependencies=("feature:OHLCV",), finite_rule=FiniteWindowRule(period)))
    for period in (5, 8, 10, 13, 15, 20, 21, 25, 26, 30, 35, 40, 50, 100, 150, 200):
        contracts.append(StateContract(f"feature:EMA_{period}", StateKind.RECURSIVE_CONTRACTIVE, dependencies=("feature:OHLCV",), recursive_rule=RecursiveRule((_ema_term(fixed=(period,), required=True),))))
    for period in (2, 3, 4, 7, 14, 21):
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
    return contracts


_PATH_STRATEGIES = {
    "SupertrendStrategy": "fxpm.supertrend-state.v1",
    "ParabolicSARTrendStrategy": "fxpm.psar-state.v1",
    "VIDYABandTrendStrategy": "fxpm.vidya-state.v1",
}


_FINITE_STRATEGY_RULES: Mapping[str, FiniteWindowRule] = {
    "AccumulationDistributionBreakoutStrategy": FiniteWindowRule(
        # The implementation deliberately suppresses its first
        # max(lookback, slope, ATR-period)+2 rows.  ATR-period is fixed at 14.
        fixed_bars=16,
        terms=(
            WindowTerm("lookback", offset=2, required=True),
            WindowTerm("adl_slope_bars", offset=2, required=True),
        ),
    ),
    "FractalChannelBreakoutStrategy": FiniteWindowRule(
        fixed_bars=3,
        expressions=(
            WindowExpression(
                terms=(
                    WindowTerm("pivot_max_age_bars", required=True),
                    WindowTerm("wing_bars", multiplier=2.0, required=True),
                ),
                offset=3,
            ),
        ),
    ),
    "OBVDivergenceStrategy": FiniteWindowRule(
        fixed_bars=3,
        expressions=(
            # The oldest eligible prior delayed swing requires its own two
            # wings: 5*order search distance + 2*order confirmation context.
            WindowExpression(
                terms=(WindowTerm("swing_order", multiplier=7.0, required=True),),
                offset=2,
            ),
            # Both smoothed OBV values must use full equal-width windows before
            # their common cumulative offset cancels.
            WindowExpression(
                terms=(
                    WindowTerm("swing_order", multiplier=5.0, required=True),
                    WindowTerm("obv_smooth", required=True, allow_zero=True),
                ),
                offset=2,
            ),
        ),
    ),
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
                finite_rule=_FINITE_STRATEGY_RULES.get(name, generic_window),
                description="Registered strategy is suffix-bounded under its declared parameters.",
            ))
    return contracts


DEFAULT_AUTHORITY = StateContractAuthority(
    (*_feature_contracts(), *_strategy_contracts()),
    candidate_domain_hash=STRATEGY_CANDIDATE_DOMAIN_SHA256,
)
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
    "STATE_CANDIDATE_DOMAIN_SCHEMA_VERSION",
    "STATE_CONTRACT_SCHEMA_VERSION",
    "STATE_DOMAIN_CEILING_SCHEMA_VERSION",
    "STATE_FEATURE_ALIASES",
    "STRATEGY_CANDIDATE_DOMAIN_SHA256",
    "StateContract",
    "StateContractAuthority",
    "StateDomainCeiling",
    "StateEvidence",
    "StateKind",
    "StateResolution",
    "WindowExpression",
    "WindowTerm",
    "strategy_candidate_domain_sha256",
]


NOISE101_STATE_SCHEMA = "fxpm.noise101-recorded-session-state.v1"
NOISE101_TRANSITION_SCHEMA = "fxpm.noise101-recorded-session-transition.v1"
NOISE101_MISSING_VALUE_SEMANTICS = "NULL_IS_MISSING_OBSERVATION_NOT_ZERO"
NOISE101_CONTEXT_FIELDS = (
    "source_edition_id", "candidate_catalogue_sha256", "timeframe",
    "domain_roster_sha256", "state_registry_sha256", "noise_state_sha256",
    "noise_transition_sha256", "state_boundary_source_sha256",
)


def build_noise101_state_authority(candidate_catalogue_sha256: str) -> StateContractAuthority:
    """Separate recorded-session authority; the captured100 authority stays exact."""
    if not _SHA256_RE.fullmatch(str(candidate_catalogue_sha256 or "")):
        raise ValueError("NOISE101_CATALOGUE_HASH_INVALID")
    noise = StateContract(
        "strategy:NoiseBandBreakoutStrategy", StateKind.PATH_STATE,
        dependencies=("feature:ATR_14", "feature:SESSION_RESET"),
        finite_rule=FiniteWindowRule(fixed_bars=1),
        checkpoint_schema=NOISE101_STATE_SCHEMA,
        transition_schema=NOISE101_TRANSITION_SCHEMA,
        description="Exact observed-session history requires authenticated full-prefix replay or a matching checkpoint.",
    )
    return StateContractAuthority(
        (*_feature_contracts(), *_strategy_contracts(), noise),
        candidate_domain_hash=candidate_catalogue_sha256,
    )


def noise101_transition_payload(*, source_edition_id: str, implementation_sha256: str,
        session_spec: Sequence[int], bar_minutes: int, check_bars: int,
        lookback: int) -> dict[str, Any]:
    if (type(source_edition_id) is not str or not source_edition_id
            or not _SHA256_RE.fullmatch(str(implementation_sha256 or ""))
            or len(session_spec) != 2
            or any(type(v) is not int for v in session_spec)
            or not 0 <= session_spec[0] < 1440 or not 1 <= session_spec[1] <= 1440
            or any(type(v) is not int or v < 1 for v in (bar_minutes, check_bars, lookback))):
        raise ValueError("NOISE101_TRANSITION_INPUT_INVALID")
    return {
        "schema_version": NOISE101_TRANSITION_SCHEMA,
        "source_edition_id": source_edition_id,
        "strategy_id": "NoiseBandBreakoutStrategy",
        "implementation_sha256": implementation_sha256,
        "session_spec": {"anchor_minutes": session_spec[0],
            "length_minutes": session_spec[1], "clock": "AS_RECORDED"},
        "bar_minutes": bar_minutes, "check_bars": check_bars, "lookback": lookback,
        "minimum_expression": "(7*lookback+9)//10",
        "history_rule": "SHIFT_WHOLE_OBSERVED_SESSION_KEYS_THEN_ROLL_LOOKBACK_SAME_SLOT_MOVES",
        "slot_rule": "RECORDED_RELATIVE_OPEN_MINUTE_PLUS_BAR_MINUTES",
        "check_rule": "ONE_BASED_IN_SESSION_OBSERVED_BAR_ORDINAL_MOD_CHECK_BARS_EQ_ZERO",
        "missing_value_semantics": NOISE101_MISSING_VALUE_SEMANTICS,
    }


def validate_noise101_source_context(context: Mapping[str, Any], *,
        allow_unproved: bool = False) -> dict[str, Any]:
    if type(context) is not dict or set(context) != set(NOISE101_CONTEXT_FIELDS):
        raise ValueError("NOISE101_SOURCE_CONTEXT_FIELDS_INVALID")
    if (type(context["source_edition_id"]) is not str or not context["source_edition_id"]
            or context["timeframe"] not in _BARS_PER_UTC_DAY):
        raise ValueError("NOISE101_SOURCE_CONTEXT_IDENTITY_INVALID")
    proof_fields = {"noise_state_sha256", "noise_transition_sha256", "state_boundary_source_sha256"}
    for name in set(NOISE101_CONTEXT_FIELDS) - {"source_edition_id", "timeframe"}:
        value = context[name]
        if allow_unproved and name in proof_fields and value is None:
            continue
        if not _SHA256_RE.fullmatch(str(value or "")):
            raise ValueError("NOISE101_SOURCE_CONTEXT_HASH_INVALID:" + name)
    return dict(context)


def encode_noise101_replay_state(*, source_context: Mapping[str, Any],
        implementation_sha256: str, symbol: str, parameters: Mapping[str, Any],
        session_spec: Sequence[int], session_keys: Sequence[int],
        in_session: Sequence[bool], slot_minutes: Sequence[int],
        open_prices: Sequence[Any], close_prices: Sequence[Any],
        recorded_opens: Sequence[str], recorded_timezone: Optional[str]) -> dict[str, Any]:
    """Encode actual replay arrays only; this function never reads source data."""
    context = validate_noise101_source_context(dict(source_context), allow_unproved=True)
    boundary = context["state_boundary_source_sha256"]
    if not _SHA256_RE.fullmatch(str(boundary or "")):
        raise ValueError("NOISE101_AUTHENTICATED_BOUNDARY_REQUIRED")
    bar_minutes, check_bars, lookback = (parameters[k] for k in ("bar_minutes", "check_bars", "lookback"))
    transition = noise101_transition_payload(source_edition_id=context["source_edition_id"],
        implementation_sha256=implementation_sha256, session_spec=session_spec,
        bar_minutes=bar_minutes, check_bars=check_bars, lookback=lookback)
    count = len(session_keys)
    if any(len(v) != count for v in (in_session, slot_minutes, open_prices, close_prices, recorded_opens)):
        raise ValueError("NOISE101_REPLAY_AXIS_INVALID")
    if recorded_timezone is not None and (type(recorded_timezone) is not str or not recorded_timezone):
        raise ValueError("NOISE101_RECORDED_TIMEZONE_INVALID")
    observations: list[dict[str, Any]] = []
    sessions: dict[int, dict[str, Any]] = {}
    order: list[int] = []
    for key, inside, slot, opened, closed, recorded_open in zip(session_keys, in_session,
            slot_minutes, open_prices, close_prices, recorded_opens, strict=True):
        if (type(key) is not int or type(inside) is not bool or type(slot) is not int
                or type(recorded_open) is not str or not recorded_open):
            raise ValueError("NOISE101_REPLAY_AXIS_INVALID")
        if not inside:
            continue
        observations.append({"session_key": key, "recorded_open": recorded_open,
            "open": float(opened) if math.isfinite(float(opened)) else None,
            "close": float(closed) if math.isfinite(float(closed)) else None})
        if key not in sessions:
            if order and key <= order[-1]:
                raise ValueError("NOISE101_REPLAY_SESSION_ORDER_INVALID")
            order.append(key)
            sessions[key] = {"first_open": float(opened), "last_close": float(closed), "moves": {}}
        item = sessions[key]
        if slot in item["moves"]:
            raise ValueError("NOISE101_REPLAY_DUPLICATE_SLOT")
        first_open = item["first_open"]
        value = abs(float(closed) / first_open - 1.0) if first_open else float("nan")
        item["moves"][slot] = value if math.isfinite(value) else None
        item["last_close"] = float(closed)
    current = order[-1] if order else None
    previous = order[-2] if len(order) > 1 else None
    history_keys = order[max(0, len(order) - 1 - lookback):-1] if order else []
    slots = sorted({slot for key in order for slot in sessions[key]["moves"]})
    def finite_or_missing(value: Any) -> Optional[float]:
        if value is None:
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    state = {
        "schema_version": NOISE101_STATE_SCHEMA,
        **{key: context[key] for key in ("source_edition_id", "candidate_catalogue_sha256",
            "timeframe", "domain_roster_sha256", "state_registry_sha256")},
        "strategy_id": "NoiseBandBreakoutStrategy", "symbol": str(symbol),
        "parameters_sha256": _canonical_sha256(dict(parameters)),
        "bar_minutes": bar_minutes, "check_bars": check_bars, "lookback": lookback,
        "session_spec": dict(transition["session_spec"]),
        "current_session_key": current,
        "current_session_first_open": finite_or_missing(sessions[current]["first_open"]) if current is not None else None,
        "previous_session_key": previous,
        "previous_session_last_close": finite_or_missing(sessions[previous]["last_close"]) if previous is not None else None,
        "history_session_keys": history_keys, "history_slot_minutes": slots,
        "history_absolute_moves": [[sessions[key]["moves"].get(slot) for slot in slots] for key in history_keys],
        "missing_value_semantics": NOISE101_MISSING_VALUE_SEMANTICS,
        "state_boundary_source_sha256": boundary,
        "recorded_timezone": recorded_timezone,
        "last_recorded_open": recorded_opens[-1] if count else None,
        "replay_observations": [item for item in observations
            if item["session_key"] in {*history_keys, current}],
    }
    context.update(noise_state_sha256=_canonical_sha256(state),
        noise_transition_sha256=_canonical_sha256(transition))
    validate_noise101_source_context(context)
    return {"state": state, "transition": transition, "source_edition_context": context}


def decode_noise101_replay_state(bundle: Mapping[str, Any]) -> dict[str, Any]:
    if type(bundle) is not dict or set(bundle) != {"state", "transition", "source_edition_context"}:
        raise ValueError("NOISE101_STATE_BUNDLE_INVALID")
    context = validate_noise101_source_context(bundle["source_edition_context"])
    state, transition = bundle["state"], bundle["transition"]
    if (type(state) is not dict or type(transition) is not dict
            or state.get("schema_version") != NOISE101_STATE_SCHEMA
            or transition.get("schema_version") != NOISE101_TRANSITION_SCHEMA
            or state.get("missing_value_semantics") != NOISE101_MISSING_VALUE_SEMANTICS
            or transition.get("missing_value_semantics") != NOISE101_MISSING_VALUE_SEMANTICS
            or _canonical_sha256(state) != context["noise_state_sha256"]
            or _canonical_sha256(transition) != context["noise_transition_sha256"]):
        raise ValueError("NOISE101_STATE_BINDING_INVALID")
    for key in ("source_edition_id", "candidate_catalogue_sha256", "timeframe",
            "domain_roster_sha256", "state_registry_sha256", "state_boundary_source_sha256"):
        if state.get(key) != context[key]:
            raise ValueError("NOISE101_STATE_CONTEXT_CROSSED")
    if (any(state.get(key) != transition.get(key) for key in ("source_edition_id", "strategy_id", "bar_minutes", "check_bars", "lookback", "session_spec"))
            or not _SHA256_RE.fullmatch(str(state.get("parameters_sha256") or ""))):
        raise ValueError("NOISE101_STATE_TRANSITION_CROSSED")
    keys, slots, matrix = (state[name] for name in ("history_session_keys",
        "history_slot_minutes", "history_absolute_moves"))
    if (type(keys) is not list or type(slots) is not list or type(matrix) is not list
            or len(keys) != len(matrix) or keys != sorted(set(keys))
            or slots != sorted(set(slots)) or len(keys) > state["lookback"]):
        raise ValueError("NOISE101_STATE_HISTORY_INVALID")
    decoded = []
    for values in matrix:
        if type(values) is not list or len(values) != len(slots):
            raise ValueError("NOISE101_STATE_HISTORY_INVALID")
        decoded.append([float("nan") if value is None else float(value) for value in values])
    observations = state.get("replay_observations")
    allowed_keys = {*keys, state.get("current_session_key")}
    if (type(observations) is not list
            or any(type(row) is not dict or set(row) != {"session_key", "recorded_open", "open", "close"}
                or row["session_key"] not in allowed_keys
                or type(row["recorded_open"]) is not str or not row["recorded_open"]
                or any(value is not None and (type(value) not in (int, float) or not math.isfinite(value))
                    for value in (row["open"], row["close"])) for row in observations)):
        raise ValueError("NOISE101_STATE_OBSERVATIONS_INVALID")
    result = dict(state)
    result["history_absolute_moves"] = decoded
    return result


__all__.extend(["NOISE101_STATE_SCHEMA", "NOISE101_TRANSITION_SCHEMA", "NOISE101_CONTEXT_FIELDS",
    "build_noise101_state_authority", "noise101_transition_payload", "validate_noise101_source_context",
    "encode_noise101_replay_state", "decode_noise101_replay_state"])
