"""Part B: with slot_commit_overlap the parent commits a completed slot after the free seats are refilled, in the same
order; without it the order is today's.  The dispatcher double follows pm_memory_governor.iter_completed: yield each
finished task, ask the admission gate, refill the free seats through task_ready, then ask the gate again on the next
poll.  Slot work, claims and commits are labelled stubs; only the ordering logic of _execute_parallel_declared_slots runs."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import pm_historical_search_producer as producer
from pm_memory_governor import TaskOutcome

SLOTS = [{"ordinal": i, "slot_id": f"slot-{i:02d}"} for i in range(1, 14)]


def digest(ordinal):
    return f"{ordinal:064x}"


class Harness:
    def __init__(self, monkeypatch, *, stop_at=None, commit_fails_at=None):
        self.events, self.stop_at, self.commit_fails_at, self.peak = [], stop_at, commit_fails_at, 0
        self.context = SimpleNamespace(limits=SimpleNamespace(max_json_bytes=1 << 20), root="INVENTED")
        self.precommit = SimpleNamespace(receipt_sha256="a" * 64)
        patch = monkeypatch.setattr
        patch(producer, "_parallel_slot_existing", lambda *args: ("INVENTED-ROOT", {}))
        patch(producer, "_parallel_slot_spec", lambda context, precommit, process, frozen, cell_id, slot, attempt: {"ordinal": slot["ordinal"]})
        patch(producer, "_parallel_slot_no_renewal", lambda *args: None)
        patch(producer, "_parallel_slot_path", lambda root, kind, ordinal: f"{kind}/{ordinal:02d}.json")
        patch(producer, "_parallel_slot_exists", lambda context, path: False)
        patch(producer.authority_spool, "_publish_exact_json", lambda context, path, body, bound: self.events.append(("claim", body["binding"]["ordinal"])))
        patch(producer, "_locked_parallel_slot_claim", self._null)
        patch(producer, "_bound_parallel_slot_result", self._result)
        patch(producer, "_commit_declared_slot_completion", self._commit)
        patch(producer, "_declared_commit_artifact", lambda slot, descriptor: {"artifact_id": slot["slot_id"], "descriptor": descriptor})

    @contextmanager
    def _null(self, *args):
        yield

    @contextmanager
    def _result(self, context, precommit, process, frozen, cell_id, slot, root, *, expected_sha=None):
        assert expected_sha == digest(slot["ordinal"])
        status = "STOP" if slot["ordinal"] == self.stop_at else "NO_TRADE"
        yield {"failure": None, "post_completion_failure": None, "completion": {"status": status}, "artifact_roster": []}, expected_sha

    def _commit(self, context, *, slot, previous_commit_sha256, completion, **kwargs):
        ordinal = slot["ordinal"]
        assert previous_commit_sha256 == (None if ordinal == 1 else "c" + digest(ordinal - 1)[1:])
        if ordinal == self.commit_fails_at:
            raise producer.HistoricalSearchProducerError("INVENTED_COMMIT_REFUSAL")
        self.events.append(("commit", ordinal))
        return {"status": completion["status"], "slot_id": slot["slot_id"]}, {"sha256": "c" + digest(ordinal)[1:]}

    def dispatcher(self, workers):
        def dispatch(tasks, admission_open, task_ready):
            pending, active = list(tasks), []
            while pending or active:
                for task in list(active):                      # yield before refill
                    active.remove(task)
                    yield TaskOutcome(task_key=task.key, status="COMPLETED", value=digest(int(task.key)))
                if not admission_open():
                    for task in pending:
                        yield TaskOutcome(task_key=task.key, status="NOT_RUN")
                    pending = []
                    continue
                while pending and len(active) < workers:
                    if not admission_open() or not task_ready(pending[0]):
                        break
                    self.events.append(("launch", int(pending[0].key)))
                    active.append(pending.pop(0))
                    self.peak = max(self.peak, len(active))
                admission_open()                               # the next poll, before any completion is seen
        return dispatch

    def run(self, workers, **keywords):
        completed, artifacts = [], []
        previous, failure = producer._execute_parallel_declared_slots(self.context, precommit=self.precommit,
            process=None, frozen_pipeline_config=None, cell_id="b" * 64, slots=SLOTS, completed=completed,
            commit_artifacts=artifacts, previous=None, terminal_failure=None, terminal_stop=False,
            slot_dispatch=self.dispatcher(workers), slot_worker_limit=workers, **keywords)
        return previous, failure, completed, artifacts

    def at(self, kind, ordinal):
        return self.events.index((kind, ordinal))

    def of(self, kind):
        return [ordinal for name, ordinal in self.events if name == kind]


@pytest.mark.parametrize("workers", [2, 4])
def test_overlap_commits_after_the_refill_in_ordinal_order(monkeypatch, workers):
    harness = Harness(monkeypatch)
    previous, failure, completed, artifacts = harness.run(workers, slot_commit_overlap=True)
    assert harness.of("commit") == list(range(1, 14)) == harness.of("launch") == harness.of("claim")
    assert previous == "c" + digest(13)[1:] and failure is None and len(completed) == len(artifacts) == 13
    for ordinal in range(1, 14 - workers):
        assert harness.at("commit", ordinal) > harness.at("launch", ordinal + workers)      # seats refilled first
    for ordinal in range(1, 14):                                                             # never beyond two pools
        launched_before = [o for o in harness.of("launch") if harness.at("launch", o) < harness.at("commit", ordinal)]
        assert max(launched_before) < ordinal + 2 * workers


@pytest.mark.parametrize("workers", [2, 4])
def test_without_the_keyword_every_ready_slot_is_committed_before_the_refill(monkeypatch, workers):
    harness = Harness(monkeypatch)
    previous, failure, completed, _ = harness.run(workers)
    assert harness.of("commit") == list(range(1, 14)) == harness.of("launch")
    assert previous == "c" + digest(13)[1:] and failure is None and len(completed) == 13
    for ordinal in range(1, 14 - workers):
        assert harness.at("commit", ordinal) < harness.at("launch", ordinal + workers)


def test_overlap_with_fewer_seats_than_the_limit_commits_when_the_window_is_full(monkeypatch):
    harness = Harness(monkeypatch)
    completed, artifacts = [], []
    previous, failure = producer._execute_parallel_declared_slots(harness.context, precommit=harness.precommit,
        process=None, frozen_pipeline_config=None, cell_id="b" * 64, slots=SLOTS, completed=completed,
        commit_artifacts=artifacts, previous=None, terminal_failure=None, terminal_stop=False,
        slot_dispatch=harness.dispatcher(1), slot_worker_limit=2, slot_commit_overlap=True)
    assert harness.of("commit") == list(range(1, 14)) == harness.of("launch") and previous == "c" + digest(13)[1:]
    assert harness.at("launch", 4) < harness.at("commit", 1) < harness.at("launch", 5)      # the window fills, then commits
    committed = 0
    for name, ordinal in harness.events:                                                     # no launch beyond the window
        if name == "commit":
            committed += 1
        elif name == "launch":
            assert ordinal < committed + 1 + 2 * 2


def test_overlap_never_runs_more_slots_than_the_limit_when_the_dispatcher_has_spare_seats(monkeypatch):
    harness = Harness(monkeypatch)
    completed, artifacts = [], []
    producer._execute_parallel_declared_slots(harness.context, precommit=harness.precommit,
        process=None, frozen_pipeline_config=None, cell_id="b" * 64, slots=SLOTS, completed=completed,
        commit_artifacts=artifacts, previous=None, terminal_failure=None, terminal_stop=False,
        slot_dispatch=harness.dispatcher(4), slot_worker_limit=2, slot_commit_overlap=True)
    assert harness.of("commit") == list(range(1, 14)) == harness.of("launch")
    assert harness.peak == 2                                                                 # four seats offered, two used


@pytest.mark.parametrize("overlap", [False, True])
def test_a_stop_result_closes_admission_before_any_refill_and_is_committed_at_once(monkeypatch, overlap):
    harness = Harness(monkeypatch, stop_at=2)
    previous, failure, completed, _ = harness.run(2, **({"slot_commit_overlap": True} if overlap else {}))
    assert harness.of("launch") == [1, 2] and harness.of("commit") == [1, 2]
    assert [row["status"] for row in completed] == ["NO_TRADE", "STOP"] and previous == "c" + digest(2)[1:]


def test_overlap_commit_refusal_stops_admission_and_drains_the_running_pool(monkeypatch):
    harness = Harness(monkeypatch, commit_fails_at=1)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        harness.run(2, slot_commit_overlap=True)
    assert caught.value.code == "INVENTED_COMMIT_REFUSAL"
    assert harness.of("commit") == [] and harness.of("launch") == [1, 2, 3, 4]              # the disclosed difference


def test_without_the_keyword_a_commit_refusal_starts_nothing_further(monkeypatch):
    harness = Harness(monkeypatch, commit_fails_at=1)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        harness.run(2)
    assert caught.value.code == "INVENTED_COMMIT_REFUSAL"
    assert harness.of("commit") == [] and harness.of("launch") == [1, 2]
