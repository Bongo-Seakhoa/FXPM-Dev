"""Part B under the REAL dispatcher: pm_memory_governor.MemoryGovernor.iter_completed spawns real child processes and
polls the admission gate and task_ready itself.  Slot work is a labelled stand-in worker; custody, results and commits are
labelled stubs in the parent, and the commit stub takes real time so the order against the next launches is observable.
Only orderings that hold by construction are asserted, never durations."""
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import pm_historical_search_producer as producer
from e1e3_trivial_slot_worker import trivial_slot_worker
from pm_memory_governor import MemoryGovernor

SLOTS = [{"ordinal": i, "slot_id": f"slot-{i:02d}"} for i in range(1, 14)]
COMMIT_SECONDS = 0.5


def digest(ordinal):
    return f"{ordinal:064x}"


class Harness:
    def __init__(self, monkeypatch, *, stop_at=None, commit_fails_at=None):
        self.events, self.stop_at, self.commit_fails_at = [], stop_at, commit_fails_at
        self.context = SimpleNamespace(limits=SimpleNamespace(max_json_bytes=1 << 20), root="INVENTED")
        self.precommit = SimpleNamespace(receipt_sha256="a" * 64)
        patch = monkeypatch.setattr
        patch(producer, "_parallel_slot_existing", lambda *args: ("INVENTED-ROOT", {}))
        patch(producer, "_parallel_slot_spec", lambda context, precommit, process, frozen, cell_id, slot, attempt: {"ordinal": slot["ordinal"], "seconds": 0.3})
        patch(producer, "_parallel_slot_no_renewal", lambda *args: None)
        patch(producer, "_parallel_slot_path", lambda root, kind, ordinal: f"{kind}/{ordinal:02d}.json")
        patch(producer, "_parallel_slot_exists", lambda context, path: False)
        patch(producer.authority_spool, "_publish_exact_json", lambda context, path, body, bound: self.events.append(("launch", body["binding"]["ordinal"])))
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
        time.sleep(COMMIT_SECONDS)                              # the parent's verification stands in here
        self.events.append(("commit", ordinal))
        return {"status": completion["status"], "slot_id": slot["slot_id"]}, {"sha256": "c" + digest(ordinal)[1:]}

    @staticmethod
    def dispatch(workers):
        def run(tasks, admission_open, task_ready):
            governor = MemoryGovernor(requested_workers=workers, poll_interval_seconds=0.02)
            return governor.iter_completed(trivial_slot_worker, tasks, admission_gate=admission_open, task_ready=task_ready)
        return run

    def run(self, workers, **keywords):
        completed, artifacts = [], []
        previous, failure = producer._execute_parallel_declared_slots(self.context, precommit=self.precommit,
            process=None, frozen_pipeline_config=None, cell_id="b" * 64, slots=SLOTS, completed=completed,
            commit_artifacts=artifacts, previous=None, terminal_failure=None, terminal_stop=False,
            slot_dispatch=self.dispatch(workers), slot_worker_limit=workers, **keywords)
        return previous, failure, completed

    def at(self, kind, ordinal):
        return self.events.index((kind, ordinal))

    def of(self, kind):
        return [ordinal for name, ordinal in self.events if name == kind]


def test_real_governor_overlap_launches_the_next_slots_before_each_commit(monkeypatch):
    harness = Harness(monkeypatch)
    previous, failure, completed = harness.run(2, slot_commit_overlap=True)
    assert harness.of("commit") == list(range(1, 14)) == harness.of("launch")
    assert previous == "c" + digest(13)[1:] and failure is None and len(completed) == 13
    for ordinal in range(1, 12):
        assert harness.at("launch", ordinal + 2) < harness.at("commit", ordinal)
    committed = 0
    for name, ordinal in harness.events:                        # never further ahead than two pools
        committed += name == "commit"
        assert name == "commit" or ordinal < committed + 1 + 4


def test_real_governor_without_the_keyword_commits_before_the_next_launch(monkeypatch):
    harness = Harness(monkeypatch)
    previous, failure, completed = harness.run(2)
    assert harness.of("commit") == list(range(1, 14)) == harness.of("launch")
    assert previous == "c" + digest(13)[1:] and failure is None and len(completed) == 13
    for ordinal in range(1, 12):
        assert harness.at("commit", ordinal) < harness.at("launch", ordinal + 2)


@pytest.mark.parametrize("overlap", [False, True])
def test_real_governor_stop_result_ends_admission_and_commits_through_the_stop(monkeypatch, overlap):
    harness = Harness(monkeypatch, stop_at=2)
    previous, failure, completed = harness.run(2, **({"slot_commit_overlap": True} if overlap else {}))
    assert harness.of("commit") == [1, 2] and [row["status"] for row in completed] == ["NO_TRADE", "STOP"]
    assert previous == "c" + digest(2)[1:] and failure is None
    assert set(harness.of("launch")) <= {1, 2, 3} and {1, 2} <= set(harness.of("launch"))   # 3 only if 1 finished first


def test_real_governor_overlap_commit_refusal_drains_and_is_raised(monkeypatch):
    harness = Harness(monkeypatch, commit_fails_at=1)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        harness.run(2, slot_commit_overlap=True)
    assert caught.value.code == "INVENTED_COMMIT_REFUSAL" and harness.of("commit") == []
    launched = harness.of("launch")
    assert launched[:3] == [1, 2, 3] and set(launched) <= {1, 2, 3, 4}                       # the disclosed difference


def test_real_governor_default_commit_refusal_starts_nothing_further(monkeypatch):
    harness = Harness(monkeypatch, commit_fails_at=1)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        harness.run(2)
    assert caught.value.code == "INVENTED_COMMIT_REFUSAL" and harness.of("commit") == []
    assert harness.of("launch") == [1, 2]
