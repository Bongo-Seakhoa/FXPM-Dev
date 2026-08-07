"""Process-isolated spawn scheduling for historical symbol work.

This module owns routing and resource bounds only.  It never interprets
historical evidence and never grants authority.  Work continues until every
symbol reaches a terminal result or the operator stops the enclosing run.
"""

from __future__ import annotations

from typing import Any, Callable, Final, Mapping, Sequence

import pm_memory_governor as memory_governor


TERMINATION_GRACE_SECONDS: Final = 5.0


class HistoricalSchedulerError(RuntimeError):
    """Typed technical scheduler failure; never a market verdict."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in tuple(details)[:16])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise HistoricalSchedulerError(code, details)


def run_symbol_scheduler(
    worker: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    symbols: Sequence[str],
    requested_workers: int,
    phase: str,
) -> dict[str, dict[str, Any]]:
    """Run each exact symbol once and return results in caller order."""

    if not callable(worker):
        _fail("HISTORICAL_SCHEDULER_WORKER_INVALID")
    if type(contract) is not dict:
        _fail("HISTORICAL_SCHEDULER_CONTRACT_INVALID")
    if type(symbols) not in {list, tuple} or not symbols:
        _fail("HISTORICAL_SCHEDULER_SYMBOLS_INVALID")
    ordered = tuple(symbols)
    if (
        any(type(symbol) is not str or not symbol for symbol in ordered)
        or len(set(ordered)) != len(ordered)
        or type(requested_workers) is not int
        or type(requested_workers) is bool
        or requested_workers <= 0
    ):
        _fail("HISTORICAL_SCHEDULER_INPUT_INVALID")
    worker_limit = min(requested_workers, len(ordered))
    phase_name = str(phase or "").strip().upper()

    results: dict[str, dict[str, Any]] = {}
    try:
        governor = memory_governor.MemoryGovernor(
            requested_workers=worker_limit,
            termination_grace_seconds=TERMINATION_GRACE_SECONDS,
        )
        tasks = (
            memory_governor.MemoryTask(
                key=symbol,
                args=(contract, symbol),
            )
            for symbol in ordered
        )
        outcomes = governor.iter_completed(worker, tasks)
        try:
            for outcome in outcomes:
                value = outcome.value
                if (
                    outcome.status != "COMPLETED"
                    or outcome.technical_failure
                    or type(value) is not dict
                    or outcome.task_key not in ordered
                    or outcome.task_key in results
                    or value.get("symbol") != outcome.task_key
                ):
                    _fail(
                        "HISTORICAL_SYMBOL_WORKER_FAILED",
                        phase_name,
                        outcome.task_key,
                        outcome.status,
                        outcome.error_type,
                        outcome.error_message,
                    )
                results[outcome.task_key] = dict(value)
        finally:
            outcomes.close()
    except HistoricalSchedulerError:
        raise
    except Exception as exc:
        raise HistoricalSchedulerError(
            "HISTORICAL_SYMBOL_SCHEDULER_INFRASTRUCTURE_FAILED",
            (phase_name, type(exc).__name__, str(exc)),
        ) from exc

    if set(results) != set(ordered):
        missing = [symbol for symbol in ordered if symbol not in results]
        _fail("HISTORICAL_SYMBOL_ROSTER_INCOMPLETE", *missing[:8])
    return {symbol: results[symbol] for symbol in ordered}


__all__ = [
    "HistoricalSchedulerError",
    "TERMINATION_GRACE_SECONDS",
    "run_symbol_scheduler",
]
