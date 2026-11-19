"""Part B against the repository's own structural parallel scenarios, with real claim, result, commit and reopen custody.

The existing tests in tests/test_declared_slot_owner_surface.py are rerun unchanged with slot_commit_overlap=True by
replacing only their executor helper.  Four of them must hold as written.  The fifth, which interrupts straight after an
outcome and expects the slot already committed, is the one place overlap differs; its overlap counterpart is written
out below: the result is durable, nothing is committed, and re-entry commits it without running the slot again."""
import inspect

import pytest

import pm_historical_search_producer as producer
import test_declared_slot_owner_surface as surface
from pm_memory_governor import TaskOutcome
from test_declared_slot_owner_surface import declared_env  # noqa: F401  (fixture)

SAME_UNDER_OVERLAP = [
    "test_declared_parallel_out_of_order_stops_close_admission_and_commit_only_the_earliest_prefix",
    "test_declared_parallel_reentry_refuses_a_durable_claim_without_a_result",
    "test_declared_parallel_reentry_commits_earlier_stop_before_resultless_later_claim",
    "test_declared_parallel_reentry_commits_a_durable_result_without_reexecuting_it",
]


def execute_with_overlap(env, dispatch):
    return producer.execute_declared_cell_owner_slots(
        context=env.context, precommit=env.precommit, process=env.process,
        frozen_pipeline_config=env.frozen, cell_id=surface.CELL_ID,
        slot_dispatch=dispatch, slot_worker_limit=4, slot_commit_overlap=True)


@pytest.mark.parametrize("name", SAME_UNDER_OVERLAP)
def test_existing_parallel_scenario_holds_unchanged_with_commit_overlap(name, request, monkeypatch):
    seen = []

    def recording(env, dispatch):
        seen.append(name)
        return execute_with_overlap(env, dispatch)

    monkeypatch.setattr(surface, "_execute_structural_parallel", recording)
    scenario = getattr(surface, name)
    scenario(**{parameter: request.getfixturevalue(parameter) for parameter in inspect.signature(scenario).parameters})
    assert seen                                                  # the scenario really ran through the overlap executor


def test_overlap_interruption_leaves_a_durable_result_that_reentry_commits_without_rerunning(declared_env, monkeypatch):
    env = declared_env
    surface._structural_readonly_profile_metadata(monkeypatch)
    state = surface._structural_driver_trace(env, monkeypatch)
    parallel_root = producer._parallel_slot_root(env.precommit.receipt_sha256, surface.CELL_ID)

    def interrupt_after_first_outcome(tasks, admission_open, task_ready):
        first = next(iter(tasks))
        assert first.key == "01" and admission_open() and task_ready(first)
        yield TaskOutcome(task_key=first.key, status="COMPLETED", value=producer.declared_slot_worker(*first.args))
        raise surface._InterruptedStructuralAttempt()            # before the dispatcher asks the gate again

    with pytest.raises(surface._InterruptedStructuralAttempt):
        execute_with_overlap(env, interrupt_after_first_outcome)
    assert surface._progress(env).slot_summaries == () and state.calls == [1]
    assert (env.context.root / producer._parallel_slot_path(parallel_root, "results", 1)).is_file()
    assert not (env.context.root / producer._declared_commit_path(env.precommit, surface.CELL_ID, 1)).exists()

    def finish_remaining(tasks, admission_open, task_ready):
        for ordinal, task in enumerate(tasks, 2):
            assert task.key == f"{ordinal:02d}" and admission_open() and task_ready(task)
            yield TaskOutcome(task_key=task.key, status="COMPLETED", value=producer.declared_slot_worker(*task.args))

    receipt = execute_with_overlap(env, finish_remaining).to_dict()
    assert receipt["completed_slots"] == receipt["declared_slots"] == 13 and not receipt["terminal_stop"]
    assert state.calls == list(range(1, 14))                     # slot 1 was not run a second time
    progress = surface._progress(env)
    assert progress.run_receipt == receipt and progress.first_uncommitted_slot is None
    paths = producer.verify_declared_parallel_slot_files(env.context, precommit=env.precommit, process=env.process,
        cell_id=surface.CELL_ID, frozen_pipeline_config=env.frozen)
    assert len(set(paths)) == 26
