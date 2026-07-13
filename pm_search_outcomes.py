"""Typed, fail-closed outcomes for FxPM model-search cells.

Search code must distinguish an honest trading abstention from a calculation
failure.  In particular, an exception may never be converted into a no-trade
penalty that lets an unevaluated family lose to an inferior runner-up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Tuple


SEARCH_OUTCOME_SCHEMA_VERSION = "fxpm-search-outcome-v1"
_MAX_MESSAGE_CHARS = 320
_MAX_EVIDENCE_ITEMS = 64


class SearchOutcomeKind(str, Enum):
    """The only terminal outcomes permitted for a search trial or family cell."""

    VALID = "VALID"
    EXPECTED_INELIGIBLE = "EXPECTED_INELIGIBLE"
    INVALID_PARAM = "INVALID_PARAM"
    INFRA_FAILURE = "INFRA_FAILURE"


class SearchStage(str, Enum):
    """Decision-relevant stage at which an outcome was produced."""

    PARAMETER_VALIDATION = "PARAMETER_VALIDATION"
    STRATEGY_CONSTRUCTION = "STRATEGY_CONSTRUCTION"
    SIGNAL_GENERATION = "SIGNAL_GENERATION"
    TRAIN_BACKTEST = "TRAIN_BACKTEST"
    TRAIN_EVIDENCE = "TRAIN_EVIDENCE"
    TRAIN_SCORE = "TRAIN_SCORE"
    FROZEN_VALIDATION = "FROZEN_VALIDATION"
    FAMILY_CELL = "FAMILY_CELL"


class InvalidStrategyParameterError(ValueError):
    """Explicitly identifies a parameter-domain rejection.

    Generic ``ValueError`` and ``TypeError`` are deliberately not treated as
    parameter rejections: they commonly indicate malformed data, features, or
    execution-cost calculations and must therefore fail the search cell.
    """


def _compact_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:_MAX_MESSAGE_CHARS]
    return str(value)[:_MAX_MESSAGE_CHARS]


def _compact_evidence(value: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """Return a bounded JSON-friendly evidence map.

    Outcome artifacts intentionally carry hashes, counts and reason codes, not
    raw parameters, trades, arrays, or tracebacks.
    """

    if not isinstance(value, Mapping):
        return {}
    compact: dict[str, Any] = {}
    for index, (raw_key, raw_value) in enumerate(value.items()):
        if index >= _MAX_EVIDENCE_ITEMS:
            compact["_truncated_items"] = len(value) - _MAX_EVIDENCE_ITEMS
            break
        key = str(raw_key)[:128]
        if isinstance(raw_value, Mapping):
            compact[key] = {
                str(child_key)[:128]: _compact_scalar(child_value)
                for child_key, child_value in list(raw_value.items())[:_MAX_EVIDENCE_ITEMS]
            }
        elif isinstance(raw_value, (list, tuple)):
            compact[key] = [
                _compact_scalar(item) for item in raw_value[:_MAX_EVIDENCE_ITEMS]
            ]
        else:
            compact[key] = _compact_scalar(raw_value)
    return compact


@dataclass(frozen=True)
class TrialOutcome:
    """Compact terminal record for one decision-relevant search trial."""

    kind: SearchOutcomeKind
    stage: SearchStage
    reason_code: str
    reason: str
    symbol: str = ""
    timeframe: str = ""
    regime: str = ""
    strategy_family: str = ""
    parameter_hash: str = ""
    exception_type: Optional[str] = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = SEARCH_OUTCOME_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return self.kind is SearchOutcomeKind.VALID

    @property
    def is_infrastructure_failure(self) -> bool:
        return self.kind is SearchOutcomeKind.INFRA_FAILURE

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "stage": self.stage.value,
            "reason_code": str(self.reason_code)[:128],
            "reason": str(self.reason)[:_MAX_MESSAGE_CHARS],
            "symbol": str(self.symbol)[:64],
            "timeframe": str(self.timeframe)[:16],
            "regime": str(self.regime)[:32],
            "strategy_family": str(self.strategy_family)[:128],
            "parameter_hash": str(self.parameter_hash)[:128],
            "exception_type": (
                str(self.exception_type)[:128]
                if self.exception_type not in (None, "")
                else None
            ),
            "evidence": _compact_evidence(self.evidence),
        }


@dataclass(frozen=True)
class FamilyCellOutcome:
    """Aggregate outcome for one family in one symbol/timeframe/regime cell."""

    kind: SearchOutcomeKind
    reason_code: str
    reason: str
    symbol: str
    timeframe: str
    regime: str
    strategy_family: str
    trial_count: int
    valid_trials: int
    expected_ineligible_trials: int
    invalid_param_trials: int
    infrastructure_failure_trials: int
    reason_codes: Tuple[str, ...] = ()
    schema_version: str = SEARCH_OUTCOME_SCHEMA_VERSION

    @property
    def complete(self) -> bool:
        return self.kind is not SearchOutcomeKind.INFRA_FAILURE

    @property
    def promotable_candidate_exists(self) -> bool:
        return self.kind is SearchOutcomeKind.VALID

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "reason_code": self.reason_code,
            "reason": self.reason[:_MAX_MESSAGE_CHARS],
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "regime": self.regime,
            "strategy_family": self.strategy_family,
            "trial_count": int(self.trial_count),
            "valid_trials": int(self.valid_trials),
            "expected_ineligible_trials": int(self.expected_ineligible_trials),
            "invalid_param_trials": int(self.invalid_param_trials),
            "infrastructure_failure_trials": int(
                self.infrastructure_failure_trials
            ),
            "reason_codes": list(self.reason_codes),
            "complete": self.complete,
            "promotable_candidate_exists": self.promotable_candidate_exists,
        }


class SearchInfrastructureError(RuntimeError):
    """An unresolved decision-path failure that makes a search cell incomplete."""

    def __init__(self, outcome: TrialOutcome | FamilyCellOutcome):
        self.outcome = outcome
        payload = outcome.to_dict()
        super().__init__(
            "SEARCH_CELL_INCOMPLETE:"
            f"{payload.get('symbol', '')}|{payload.get('timeframe', '')}|"
            f"{payload.get('regime', '')}|{payload.get('strategy_family', '')}:"
            f"{payload.get('reason_code', 'INFRA_FAILURE')}"
        )


def classify_search_exception(
    exc: BaseException,
    *,
    stage: SearchStage,
    symbol: str = "",
    timeframe: str = "",
    regime: str = "",
    strategy_family: str = "",
    parameter_hash: str = "",
) -> TrialOutcome:
    """Classify only the explicit parameter exception as ``INVALID_PARAM``."""

    if isinstance(exc, InvalidStrategyParameterError):
        kind = SearchOutcomeKind.INVALID_PARAM
        reason_code = "EXPLICIT_INVALID_STRATEGY_PARAMETER"
    else:
        kind = SearchOutcomeKind.INFRA_FAILURE
        reason_code = "UNEXPECTED_SEARCH_EXCEPTION"
    return TrialOutcome(
        kind=kind,
        stage=stage,
        reason_code=reason_code,
        reason=f"{type(exc).__name__}: {exc}",
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_family=strategy_family,
        parameter_hash=parameter_hash,
        exception_type=type(exc).__name__,
    )


def aggregate_family_cell_outcomes(
    outcomes: Iterable[TrialOutcome],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_family: str,
) -> FamilyCellOutcome:
    """Aggregate trials with infrastructure failure taking absolute precedence."""

    rows = tuple(outcomes)
    counts = {kind: 0 for kind in SearchOutcomeKind}
    context_mismatch = False
    for row in rows:
        if not isinstance(row, TrialOutcome):
            raise TypeError("family-cell outcomes must contain TrialOutcome records")
        counts[row.kind] += 1
        context_mismatch = context_mismatch or any((
            bool(row.symbol) and str(row.symbol) != str(symbol),
            bool(row.timeframe) and str(row.timeframe) != str(timeframe),
            bool(row.regime) and str(row.regime) != str(regime),
            bool(row.strategy_family)
            and str(row.strategy_family) != str(strategy_family),
        ))
    if not rows:
        kind = SearchOutcomeKind.INFRA_FAILURE
        reason_code = "NO_TRIAL_OUTCOMES"
        reason = "family cell produced no terminal trial outcomes"
    elif context_mismatch:
        kind = SearchOutcomeKind.INFRA_FAILURE
        reason_code = "TRIAL_CONTEXT_MISMATCH"
        reason = "trial outcomes do not all belong to the requested family cell"
    elif counts[SearchOutcomeKind.INFRA_FAILURE]:
        kind = SearchOutcomeKind.INFRA_FAILURE
        reason_code = "UNRESOLVED_INFRA_FAILURE"
        reason = "at least one trial was not validly evaluated"
    elif counts[SearchOutcomeKind.VALID]:
        kind = SearchOutcomeKind.VALID
        reason_code = "VALID_CANDIDATE_AVAILABLE"
        reason = "at least one predeclared-gate-eligible trial is available"
    elif counts[SearchOutcomeKind.EXPECTED_INELIGIBLE]:
        kind = SearchOutcomeKind.EXPECTED_INELIGIBLE
        reason_code = "ALL_EVALUATED_TRIALS_INELIGIBLE"
        reason = "all legitimately evaluated trials were ineligible"
    else:
        kind = SearchOutcomeKind.INVALID_PARAM
        reason_code = "ALL_TRIAL_PARAMETERS_INVALID"
        reason = "every planned trial was explicitly outside its parameter domain"
    reason_code_values = [row.reason_code for row in rows]
    if context_mismatch:
        reason_code_values.append("TRIAL_CONTEXT_MISMATCH")
    reason_codes = tuple(dict.fromkeys(reason_code_values))
    return FamilyCellOutcome(
        kind=kind,
        reason_code=reason_code,
        reason=reason,
        symbol=str(symbol),
        timeframe=str(timeframe),
        regime=str(regime),
        strategy_family=str(strategy_family),
        trial_count=len(rows),
        valid_trials=counts[SearchOutcomeKind.VALID],
        expected_ineligible_trials=counts[SearchOutcomeKind.EXPECTED_INELIGIBLE],
        invalid_param_trials=counts[SearchOutcomeKind.INVALID_PARAM],
        infrastructure_failure_trials=counts[SearchOutcomeKind.INFRA_FAILURE],
        reason_codes=reason_codes,
    )


__all__ = [
    "SEARCH_OUTCOME_SCHEMA_VERSION",
    "FamilyCellOutcome",
    "InvalidStrategyParameterError",
    "SearchInfrastructureError",
    "SearchOutcomeKind",
    "SearchStage",
    "TrialOutcome",
    "aggregate_family_cell_outcomes",
    "classify_search_exception",
]
