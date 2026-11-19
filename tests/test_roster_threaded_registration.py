"""Part A: a roster registered through the thread pool ends in the same sink state, raises the same first refusal in
roster order, and reads every member in every call.  Invented payloads on a scratch generation; no product data."""
import threading
import time

import pytest

import pm_authority_spool as spool

N = 48


@pytest.fixture
def scratch(tmp_path):
    context = spool.freeze_generation_context(tmp_path / "generation", generation_id="e1e3-roster",
        native_universe_sha256="0" * 64, config_sha256="1" * 64, limits=spool.SpoolLimits())
    entries = [{"artifact_id": f"invented/member-{i:03d}", "artifact_type": "invented_member",
        "descriptor": spool.write_canonical_json_artifact(context, {"i": i, "pad": "x" * (40 + 7 * i)})}
        for i in range(N)]
    return context, entries


def register_each(context, entries):
    sink = spool.open_symbol_sink(context, "INVENTED")
    for entry in entries:
        sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"])
    return sink


def state(sink):
    return list(sink._artifacts.items()), sink._total_bytes


def path_of(context, entry):
    return spool._local_path(context.root, entry["descriptor"]["relative_path"])


def outcome(call):
    try:
        call()
    except spool.AuthoritySpoolError as exc:
        return str(exc)
    return None


@pytest.mark.parametrize("workers", [1, 2, 4, 8])
def test_roster_ends_in_the_same_state_as_one_by_one_registration(scratch, workers):
    context, entries = scratch
    expected = state(register_each(context, entries))
    sink = spool.open_symbol_sink(context, "INVENTED")
    assert sink.register_roster(entries, workers=workers) is None
    assert state(sink) == expected and len(expected[0]) == N


def test_every_member_is_read_in_every_call_and_reads_overlap(scratch, monkeypatch):
    context, entries = scratch
    real, seen, threads = spool._verify_artifact_file, [], set()

    def counting(ctx, descriptor):
        seen.append(descriptor["sha256"]); threads.add(threading.get_ident())
        time.sleep(0.002)                                   # let a second thread pick up work
        return real(ctx, descriptor)

    monkeypatch.setattr(spool, "_verify_artifact_file", counting)
    wanted = sorted(entry["descriptor"]["sha256"] for entry in entries)
    for _ in range(2):                                      # a second call reads everything again
        seen.clear()
        spool.open_symbol_sink(context, "INVENTED").register_roster(entries, workers=4)
        assert sorted(seen) == wanted
    assert len(threads) > 1 and threading.get_ident() not in threads


@pytest.mark.parametrize("workers", [2, 8])
def test_first_refusal_in_roster_order_is_raised_and_earlier_members_stay_admitted(scratch, workers):
    context, entries = scratch
    changed, missing = 17, 30
    target = path_of(context, entries[changed])
    raw = bytearray(target.read_bytes()); raw[-3] ^= 1      # same size, different bytes
    target.chmod(0o644); target.write_bytes(bytes(raw))
    gone = path_of(context, entries[missing]); gone.chmod(0o644); gone.unlink()
    serial = spool.open_symbol_sink(context, "INVENTED")
    expected = outcome(lambda: [serial.register_artifact(e["artifact_id"], e["artifact_type"], e["descriptor"]) for e in entries])
    threaded = spool.open_symbol_sink(context, "INVENTED")
    assert outcome(lambda: threaded.register_roster(entries, workers=workers)) == expected == "ARTIFACT_INTEGRITY_MISMATCH"
    assert state(threaded) == state(serial) and len(threaded._artifacts) == changed


@pytest.mark.parametrize("workers", [1, 4])
def test_a_bookkeeping_refusal_keeps_its_place_ahead_of_a_later_bad_file(scratch, workers):
    context, entries = scratch
    clash = dict(entries[5], artifact_id=entries[2]["artifact_id"])          # same id, different descriptor
    roster = entries[:9] + [clash] + entries[9:]
    later = path_of(context, entries[20]); later.chmod(0o644); later.unlink()
    serial = spool.open_symbol_sink(context, "INVENTED")
    expected = outcome(lambda: [serial.register_artifact(e["artifact_id"], e["artifact_type"], e["descriptor"]) for e in roster])
    threaded = spool.open_symbol_sink(context, "INVENTED")
    assert outcome(lambda: threaded.register_roster(roster, workers=workers)) == expected == "ARTIFACT_ID_CONFLICT"
    assert state(threaded) == state(serial) and len(threaded._artifacts) == 9


@pytest.mark.parametrize("workers", [0, 9, -1, True, 4.0, "4", None])
def test_worker_count_outside_the_bound_is_refused_before_any_read(scratch, monkeypatch, workers):
    context, entries = scratch
    monkeypatch.setattr(spool, "_verify_artifact_file", lambda *args: pytest.fail("read before the bound check"))
    sink = spool.open_symbol_sink(context, "INVENTED")
    assert outcome(lambda: sink.register_roster(entries, workers=workers)) == "ROSTER_VERIFY_WORKERS_INVALID"
    assert sink._artifacts == {}


@pytest.mark.parametrize("count", [0, 1])
def test_an_empty_or_single_member_roster_takes_the_one_by_one_path(scratch, count):
    context, entries = scratch
    sink = spool.open_symbol_sink(context, "INVENTED")
    sink.register_roster(entries[:count], workers=8)
    assert state(sink) == state(register_each(context, entries[:count]))


def test_register_artifact_alone_is_unchanged_for_repeats_and_conflicts(scratch):
    context, entries = scratch
    sink = spool.open_symbol_sink(context, "INVENTED")
    first = sink.register_artifact(entries[0]["artifact_id"], entries[0]["artifact_type"], entries[0]["descriptor"])
    again = sink.register_artifact(entries[0]["artifact_id"], entries[0]["artifact_type"], entries[0]["descriptor"])
    assert first == again and len(sink._artifacts) == 1
    assert outcome(lambda: sink.register_artifact(entries[0]["artifact_id"], "invented_member",
        entries[1]["descriptor"])) == "ARTIFACT_ID_CONFLICT"
