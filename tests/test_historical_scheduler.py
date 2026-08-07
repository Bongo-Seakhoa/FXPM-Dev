from __future__ import annotations

from collections.abc import Iterator
import inspect
import os

import pytest

import pm_historical_scheduler as scheduler
from pm_memory_governor import TaskOutcome


def _worker(contract, symbol):
    return {"symbol": symbol, "contract": contract["value"]}


@pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)
def test_scheduler_real_windows_spawn_returns_only_compact_mapping(
) -> None:
    observed = scheduler.run_symbol_scheduler(
        _worker,
        contract={"value": "spawn-safe"},
        symbols=("EURUSD",),
        requested_workers=1,
        phase="historical_spawn_probe",
    )
    assert observed == {
        "EURUSD": {"symbol": "EURUSD", "contract": "spawn-safe"}
    }


def test_scheduler_uses_requested_workers_once_and_reassembles_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    governors = []

    class Governor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            governors.append(self)

        def iter_completed(self, worker, tasks) -> Iterator[TaskOutcome]:
            values = list(tasks)
            self.worker = worker
            self.tasks = values
            for task in reversed(values):
                yield TaskOutcome(
                    task_key=task.key,
                    status="COMPLETED",
                    value={"symbol": task.key, "contract": "ok"},
                )

    monkeypatch.setattr(scheduler.memory_governor, "MemoryGovernor", Governor)
    symbols = tuple(f"S{ordinal}" for ordinal in range(12))
    observed = scheduler.run_symbol_scheduler(
        _worker,
        contract={"value": "ok"},
        symbols=symbols,
        requested_workers=99,
        phase="historical_preplan",
    )

    assert tuple(observed) == symbols
    assert len(governors) == 1
    assert governors[0].kwargs == {
        "requested_workers": len(symbols),
        "termination_grace_seconds": scheduler.TERMINATION_GRACE_SECONDS,
    }
    assert governors[0].worker is _worker
    assert [task.key for task in governors[0].tasks] == list(symbols)


def test_scheduler_failure_is_terminal_and_never_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    class Governor:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def iter_completed(self, _worker, tasks):
            task = next(iter(tasks))
            yield TaskOutcome(
                task_key=task.key,
                status="FAILED_CHILD_OOM_OR_CRASH",
                technical_failure=True,
                error_type="WorkerProtocolError",
                error_message="child exited",
            )

    monkeypatch.setattr(scheduler.memory_governor, "MemoryGovernor", Governor)
    with pytest.raises(
        scheduler.HistoricalSchedulerError,
        match="HISTORICAL_SYMBOL_WORKER_FAILED",
    ):
        scheduler.run_symbol_scheduler(
            _worker,
            contract={"value": "ok"},
            symbols=("A", "B", "C"),
            requested_workers=5,
            phase="historical_execute",
        )
    assert len(calls) == 1


def test_scheduler_api_contains_no_elapsed_ceiling() -> None:
    parameters = inspect.signature(scheduler.run_symbol_scheduler).parameters
    assert tuple(parameters) == (
        "worker",
        "contract",
        "symbols",
        "requested_workers",
        "phase",
    )


def test_scheduler_wraps_governor_failure_as_typed_infrastructure_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Governor:
        def __init__(self, **_kwargs):
            pass

        def iter_completed(self, _worker, _tasks):
            raise RuntimeError("scheduler broke before launch")

    monkeypatch.setattr(scheduler.memory_governor, "MemoryGovernor", Governor)
    with pytest.raises(
        scheduler.HistoricalSchedulerError,
        match="HISTORICAL_SYMBOL_SCHEDULER_INFRASTRUCTURE_FAILED",
    ):
        scheduler.run_symbol_scheduler(
            _worker,
            contract={"value": "ok"},
            symbols=("A",),
            requested_workers=1,
            phase="historical_execute",
        )
