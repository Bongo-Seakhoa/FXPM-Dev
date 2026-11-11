"""Invented retirement inventories and owned software trees, without native data."""

from copy import deepcopy
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

import pm_authority_spool as spool
import pm_native_storage as storage


MAXIMUM = 64 * 1024**2
GENERATION_BYTES = 512 * 1024**3
SEGMENT_PREFIX = "generation-retirement.segment."


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _arguments(count, *, segment_max_records=100000):
    return {
        "source_fence_path": "C:/invented-retirement/retained/fence.json",
        "source_fence_sha256": "a" * 64,
        "retirement_receipt_path": "C:/invented-retirement/retained/receipt.json",
        "workspace_owner_sha256": "b" * 64,
        "protected_paths": ["C:/invented-retirement/retained"],
        "owned_direct_files": [], "unit_lock_paths": [],
        "max_paths": count, "max_nodes": 2000000, "max_bytes": GENERATION_BYTES,
        "segment_max_records": segment_max_records,
    }


def _records(count, direct_count=0):
    records = []
    for ordinal in range(count):
        if ordinal < direct_count:
            relative = ("declared-slots/" + "e" * 64 + "/requests/" + "f" * 64
                        + f"/owner-slot-{ordinal:08d}/attempt-invented.json")
        else:
            digest = f"{ordinal - direct_count:064x}"
            relative = f"objects/sha256/{digest[:2]}/{digest}.canonical-json"
        size = 512 * 1024
        records.append({"relative_path": relative,
                        "identity": [2**64 - 1, 2**64 - ordinal - 1, size, 2**64 - 1],
                        "bytes": size})
    return records


def _plan(records, *, maximum=MAXIMUM, count=100000, arguments=None):
    arguments = _arguments(len(records), segment_max_records=count) if arguments is None else arguments
    return spool.plan_segmented_retirement(records, generation_context_sha256="c" * 64,
        arguments=arguments, maximum_bytes=maximum, segment_max_records=count)


def _assert_plan(plan, records, maximum, limit):
    assert _encode(plan["intent"]) == spool._canonical_json_bytes(plan["intent"], maximum)
    assert len(_encode(plan["intent"])) == plan["intent_size_bytes"] <= maximum
    assert hashlib.sha256(_encode(plan["intent"])).hexdigest() == plan["intent_sha256"]
    cursor = 0
    for segment in plan["segments"]:
        encoded = spool._canonical_json_bytes(segment["body"], maximum)
        assert encoded == _encode(segment["body"])
        assert hashlib.sha256(encoded).hexdigest() == segment["sha256"]
        assert len(encoded) == segment["size_bytes"] <= maximum
        assert segment["relative_path"] == SEGMENT_PREFIX + segment["sha256"] + ".json"
        files = segment["body"]["files"]
        assert 0 < len(files) <= limit
        assert files == records[cursor:cursor + len(files)]
        cursor += len(files)
    assert cursor == len(records)
    assert plan["intent"]["retired_payload_files"] == len(records)
    assert plan["intent"]["retired_logical_bytes"] == sum(row["bytes"] for row in records)


@pytest.mark.parametrize("count,direct", [(313542 + 4028, 4028), (1000000, 0)],
                         ids=["case-shaped-invented", "million-invented"])
def test_real_document_limits_segment_full_scale_without_losing_records(count, direct):
    records = _records(count, direct)
    arguments = _arguments(count)
    arguments["owned_direct_files"] = [row["relative_path"] for row in records[:direct]]
    with pytest.raises(spool.AuthoritySpoolError, match="JSON_STRUCTURE_UNBOUNDED|JSON_SIZE_BOUND_EXCEEDED"):
        spool._canonical_json_bytes({"schema_version": spool.RETIREMENT_SCHEMA,
            "generation_context_sha256": "c" * 64, "arguments": arguments, "files": records}, MAXIMUM)
    plan = _plan(records, arguments=arguments)
    _assert_plan(plan, records, MAXIMUM, 100000)
    expected = (plan["intent_sha256"], tuple(s["sha256"] for s in plan["segments"]))
    del plan
    repeated = _plan(records, arguments=arguments)
    assert (repeated["intent_sha256"], tuple(s["sha256"] for s in repeated["segments"])) == expected


def test_segments_cut_at_record_and_exact_encoded_byte_boundaries():
    records = _records(5)
    assert [len(s["body"]["files"]) for s in _plan(records, count=2)["segments"]] == [2, 2, 1]
    for ordinal, row in enumerate(records):
        row["relative_path"] = f"direct-{ordinal:03d}-" + "q" * 1800 + ".json"
    full = _plan(records, maximum=65536, count=5)
    exact_bytes = full["segments"][0]["size_bytes"]
    at = _plan(records, maximum=exact_bytes, count=5)
    below = _plan(records, maximum=exact_bytes - 1, count=5)
    assert len(at["segments"]) == 1
    assert len(below["segments"]) == 2
    _assert_plan(below, records, exact_bytes - 1, 5)
    first = below["segments"][0]["body"]
    joined = deepcopy(first)
    joined["files"].append(below["segments"][1]["body"]["files"][0])
    assert len(_encode(joined)) > exact_bytes - 1


def test_node_limit_and_maximum_direct_width_are_independently_bounded():
    records = _records(181818)
    plan = _plan(records, count=181817)
    _assert_plan(plan, records, MAXIMUM, 181817)
    assert [len(segment["body"]["files"]) for segment in plan["segments"]] == [181817, 1]
    wide = _records(1)
    wide[0]["relative_path"] = "q" * 4091 + ".json"
    wide[0]["bytes"] = wide[0]["identity"][2] = 1024**3
    _assert_plan(_plan(wide, count=1), wide, MAXIMUM, 1)


def test_merge_authenticates_all_segments_before_accepting_survivor_subsets():
    records = _records(7)
    segments = _plan(records, count=2)["segments"]
    for remaining in (records, records[1::2], records[-1:], []):
        spool.verify_segmented_retirement_inventory(remaining, segments)
    changed = deepcopy(records[1::2])
    changed[0]["identity"][3] += 1
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_PAYLOAD_CHANGED"):
        spool.verify_segmented_retirement_inventory(changed, segments)
    extra = deepcopy(records[-1])
    extra["relative_path"] = "unexpected.json"
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_PAYLOAD_CHANGED"):
        spool.verify_segmented_retirement_inventory([*records, extra], segments)
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_SEGMENTS_INVALID|RETIREMENT_PAYLOAD_CHANGED"):
        spool.verify_segmented_retirement_inventory(records, tuple(reversed(segments)))
    damaged = deepcopy(segments)
    damaged[-1]["body"]["files"][0]["bytes"] += 1
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_SEGMENTS_INVALID"):
        spool.verify_segmented_retirement_inventory([], damaged)


@pytest.fixture
def owned_tree():
    root = Path(os.environ["FXPM_DERIVED_QA_ROOT"])
    assert root.is_absolute() and root.is_dir()
    spool._reject_reparse_chain(root)
    helper = importlib.import_module("tests.test_completed_generation_retirement")
    fixture = helper.CompletedGenerationRetirementTests(
        "test_native_completion_archives_owned_payload_and_retains_exact_full_grid")
    fixture.setUp()
    context, entries, arguments = fixture.fixture()
    return context, entries, {**arguments, "segment_max_records": 2}


def _inventory(context, args):
    records, _, _, _ = spool._retirement_inventory(context.root,
        frozenset(args["owned_direct_files"]), max_paths=args["max_paths"],
        max_nodes=args["max_nodes"], max_bytes=args["max_bytes"],
        retained_unit_locks=args["unit_lock_paths"])
    return records


def _payload_snapshot(context, args):
    return {r["relative_path"]: (context.root / r["relative_path"]).read_bytes()
            for r in _inventory(context, args)}


def _assert_retained(context, snapshot):
    assert {name: (context.root / name).read_bytes() for name in snapshot} == snapshot


def _archive(context, args):
    result = spool.retire_completed_generation(context.root, **args)
    assert result["status"] == "ARCHIVED_RECORD_ONLY"
    assert result["intent_segment_count"] > 0
    assert not (context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).exists()
    assert not list(context.root.glob(SEGMENT_PREFIX + "*.json"))
    for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
        assert result[key] is False
    assert spool.read_generation_retirement(context.root) == result
    assert spool.retire_completed_generation(context.root, **args) == result
    return result


class InjectedCrash(RuntimeError):
    pass


def _stop_after_marker(monkeypatch):
    original = spool._retirement_publish_v2

    def publish(root, relative, *args, **kwargs):
        answer = original(root, relative, *args, **kwargs)
        if relative == spool.RETIREMENT_RELATIVE_PATH:
            raise InjectedCrash("after marker")
        return answer

    monkeypatch.setattr(spool, "_retirement_publish_v2", publish)


@pytest.mark.parametrize("stage", ["intent", "segment-first", "segment-last", "marker", "payload-first",
    "payload-last", "receipt", "complete", "segment-cleanup", "intent-cleanup"])
def test_identical_resume_covers_each_publication_and_deletion_boundary(owned_tree, monkeypatch, stage):
    context, _, args = owned_tree
    records = _inventory(context, args)
    payloads = {context.root / r["relative_path"] for r in records}
    segment_count = (len(records) + 1) // 2
    published_segments = 0
    removed_payloads = 0
    fired = False
    original_publish = spool._retirement_publish_v2
    original_unlink = Path.unlink

    def publish(root, relative, *values, **kwargs):
        nonlocal fired, published_segments
        answer = original_publish(root, relative, *values, **kwargs)
        if relative.startswith(SEGMENT_PREFIX):
            published_segments += 1
        hit = ((stage == "intent" and relative == spool.RETIREMENT_INTENT_RELATIVE_PATH)
            or (stage == "segment-first" and relative.startswith(SEGMENT_PREFIX) and published_segments == 1)
            or (stage == "segment-last" and relative.startswith(SEGMENT_PREFIX) and published_segments == segment_count)
            or (stage == "marker" and relative == spool.RETIREMENT_RELATIVE_PATH)
            or (stage == "receipt" and Path(root) / relative == args["retirement_receipt_path"])
            or (stage == "complete" and relative == spool.RETIREMENT_COMPLETE_RELATIVE_PATH))
        if hit and not fired:
            fired = True
            raise InjectedCrash(stage)
        return answer

    def unlink(path, *values, **kwargs):
        nonlocal fired, removed_payloads
        answer = original_unlink(path, *values, **kwargs)
        if path in payloads:
            removed_payloads += 1
        hit = ((stage == "payload-first" and path in payloads and removed_payloads == 1)
            or (stage == "payload-last" and path in payloads and removed_payloads == len(payloads))
            or (stage == "segment-cleanup" and path.name.startswith(SEGMENT_PREFIX))
            or (stage == "intent-cleanup" and path == context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH))
        if hit and not fired:
            fired = True
            raise InjectedCrash(stage)
        return answer

    with monkeypatch.context() as faults:
        faults.setattr(spool, "_retirement_publish_v2", publish)
        faults.setattr(Path, "unlink", unlink)
        with pytest.raises(InjectedCrash):
            spool.retire_completed_generation(context.root, **args)
    assert fired
    result = _archive(context, args)
    assert result["retired_payload_files"] == len(records)
    assert result["retired_logical_bytes"] == sum(row["bytes"] for row in records)


@pytest.mark.parametrize("when", ["write", "publish-before", "publish-after"])
def test_control_temporary_failures_are_typed_before_payload_release(owned_tree, monkeypatch, when):
    context, _, args = owned_tree
    snapshot = _payload_snapshot(context, args)
    fired = False
    original = spool._publish_temp_exclusive

    def fail_write(*values, **kwargs):
        nonlocal fired
        fired = True
        raise OSError(5, "invented control write failure")

    def fail_publish(*values, **kwargs):
        nonlocal fired
        if when == "publish-after":
            original(*values, **kwargs)
        fired = True
        raise OSError(5, "invented control publication failure")

    with monkeypatch.context() as faults:
        faults.setattr(spool, "_write_all" if when == "write" else "_publish_temp_exclusive",
                       fail_write if when == "write" else fail_publish)
        with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_CONTROL_WRITE_FAILED"):
            spool.retire_completed_generation(context.root, **args)
    assert fired
    _assert_retained(context, snapshot)
    assert not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()


def _track_control_temporary(monkeypatch):
    state = {"path": None, "descriptor": None}
    actual_temporary = spool._temporary_path
    actual_open = spool.os.open

    def temporary(root):
        state["path"] = actual_temporary(root)
        return state["path"]

    def opened(path, flags, *args, **kwargs):
        descriptor = actual_open(path, flags, *args, **kwargs)
        if state["path"] is not None and Path(path) == state["path"]:
            state["descriptor"] = descriptor
        return descriptor

    monkeypatch.setattr(spool, "_temporary_path", temporary)
    monkeypatch.setattr(spool.os, "open", opened)
    return state


def _publish_invented_control(context):
    owner = spool.read_owned_generation_workspace(context)[0]
    return spool._retirement_publish_v2(context.root, "invented-control.json",
        {"fixture_provenance": "INVENTED_TEMPORARY_CUSTODY_ORIGINAL"},
        context.limits.max_json_bytes, root_owner=owner)


@pytest.mark.parametrize("when", ["opened", "written"])
def test_control_fstat_failure_closes_its_owned_descriptor(owned_tree, monkeypatch, when):
    context, _, _ = owned_tree
    actual_fstat = spool.os.fstat
    actual_write = spool._write_all
    state = _track_control_temporary(monkeypatch)
    written = False
    fired = False

    def write(descriptor, data):
        nonlocal written
        answer = actual_write(descriptor, data)
        if descriptor == state["descriptor"]:
            written = True
        return answer

    def fstat(descriptor):
        nonlocal fired
        if (descriptor == state["descriptor"] and not fired
                and (when == "opened" or written)):
            fired = True
            raise OSError(5, "invented descriptor metadata failure")
        return actual_fstat(descriptor)

    monkeypatch.setattr(spool, "_write_all", write)
    monkeypatch.setattr(spool.os, "fstat", fstat)
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_CONTROL_WRITE_FAILED"):
        _publish_invented_control(context)
    assert fired
    with pytest.raises(OSError):
        actual_fstat(state["descriptor"])
    assert not (context.root / "invented-control.json").exists()


def test_control_refuses_a_path_replacement_after_its_descriptor_closes(owned_tree, monkeypatch):
    context, _, args = owned_tree
    actual_close = spool.os.close
    state = _track_control_temporary(monkeypatch)
    held = args["source_fence_path"].parent / "invented-original-control.part"
    replacement = b'{"fixture_provenance":"INVENTED_FOREIGN_REPLACEMENT"}'
    replaced = False

    def close(descriptor):
        nonlocal replaced
        answer = actual_close(descriptor)
        if descriptor == state["descriptor"] and not replaced:
            state["path"].replace(held)
            state["path"].write_bytes(replacement)
            replaced = True
        return answer

    monkeypatch.setattr(spool.os, "close", close)
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_CONTROL_TEMP_CHANGED"):
        _publish_invented_control(context)
    assert replaced
    assert state["path"].read_bytes() == replacement
    assert held.exists()
    assert not (context.root / "invented-control.json").exists()


def test_control_refuses_changed_path_metadata_during_an_open_write(owned_tree, monkeypatch):
    context, _, args = owned_tree
    replacement = args["source_fence_path"].parent / "invented-foreign-control.part"
    replacement.write_bytes(b"INVENTED_FOREIGN_METADATA_IDENTITY")
    replacement_stat = replacement.stat()
    actual_stat = Path.stat
    actual_write = spool._write_all
    state = _track_control_temporary(monkeypatch)
    written = False
    publication_calls = []

    def write(descriptor, data):
        nonlocal written
        answer = actual_write(descriptor, data)
        if descriptor == state["descriptor"]:
            written = True
        return answer

    def stat(path, *args, **kwargs):
        if written and path == state["path"]:
            return replacement_stat
        return actual_stat(path, *args, **kwargs)

    def forbidden_publish(*args, **kwargs):
        publication_calls.append(args)
        raise AssertionError("changed temporary identity reached publication")

    monkeypatch.setattr(spool, "_write_all", write)
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(spool, "_publish_temp_exclusive", forbidden_publish)
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_CONTROL_TEMP_CHANGED"):
        _publish_invented_control(context)
    assert written and not publication_calls
    assert replacement.read_bytes() == b"INVENTED_FOREIGN_METADATA_IDENTITY"
    assert state["path"].exists()
    assert not (context.root / "invented-control.json").exists()


@pytest.mark.parametrize("mutation", ["payload", "unplanned-segment", "arguments", "root-intent", "segment-bytes"])
def test_resume_rejects_changed_bindings_before_any_payload_unlink(owned_tree, monkeypatch, mutation):
    context, _, args = owned_tree
    snapshot = _payload_snapshot(context, args)
    with monkeypatch.context() as faults:
        _stop_after_marker(faults)
        with pytest.raises(InjectedCrash):
            spool.retire_completed_generation(context.root, **args)
    if mutation == "payload":
        path = context.root / next(iter(snapshot))
        path.write_bytes(b"invented changed payload")
        snapshot[path.relative_to(context.root).as_posix()] = path.read_bytes()
    elif mutation == "unplanned-segment":
        (context.root / (SEGMENT_PREFIX + "f" * 64 + ".json")).write_bytes(b"{}")
    elif mutation == "arguments":
        args = {**args, "max_paths": args["max_paths"] + 1}
    else:
        path = (context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH if mutation == "root-intent"
                else next(context.root.glob(SEGMENT_PREFIX + "*.json")))
        path.write_bytes(path.read_bytes() + b" ")
    original_unlink = Path.unlink
    unlinked = []

    def watch(path, *values, **kwargs):
        if path.is_relative_to(context.root) and path.relative_to(context.root).as_posix() in snapshot:
            unlinked.append(path)
        return original_unlink(path, *values, **kwargs)

    with monkeypatch.context() as faults:
        faults.setattr(Path, "unlink", watch)
        with pytest.raises(spool.AuthoritySpoolError):
            spool.retire_completed_generation(context.root, **args)
    assert not unlinked
    _assert_retained(context, snapshot)


@pytest.mark.parametrize("obstruction", ["foreign", "git", "hardlink", "reparse"])
def test_segmented_retirement_preserves_ownership_refusals(owned_tree, monkeypatch, obstruction):
    context, entries, args = owned_tree
    snapshot = _payload_snapshot(context, args)
    if obstruction == "foreign":
        (context.root / "unowned.txt").write_text("invented foreign", encoding="ascii")
    elif obstruction == "git":
        (context.root / ".git").mkdir()
    elif obstruction == "hardlink":
        path = context.root / entries[0]["descriptor"]["relative_path"]
        os.link(path, args["source_fence_path"].parent / "retained-hardlink")
    else:
        original = spool._is_reparse
        path = context.root / "objects"
        monkeypatch.setattr(spool, "_is_reparse", lambda p: Path(p) == path or original(p))
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_(FOREIGN|HARDLINK|REPARSE)|REPARSE"):
        spool.retire_completed_generation(context.root, **args)
    _assert_retained(context, snapshot)
    assert not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()


def test_live_unit_lock_stays_exclusive_through_segmented_retirement(owned_tree):
    context, _, args = owned_tree
    relative = "software-units/units.lock"
    path = context.root / relative
    path.parent.mkdir()
    path.write_bytes(b"0")
    args.update(owned_direct_files=(relative,), unit_lock_paths=(relative,))
    before = (path.stat().st_dev, path.stat().st_ino)
    with path.open("r+b", buffering=0) as handle:
        assert storage._try_kernel_lock(handle)
        try:
            with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_UNIT_WRITER_ACTIVE"):
                spool.retire_completed_generation(context.root, **args)
        finally:
            storage._release_kernel_lock(handle)
    result = _archive(context, args)
    assert result["retained_unit_locks"] == [relative]
    assert (path.stat().st_dev, path.stat().st_ino) == before


def test_v1_marker_resume_and_archived_reader_keep_the_original_schema(owned_tree, monkeypatch):
    context, _, args = owned_tree
    args.pop("segment_max_records")
    original = spool._retirement_publish

    def interrupt(root, relative, *values, **kwargs):
        answer = original(root, relative, *values, **kwargs)
        if relative == spool.RETIREMENT_RELATIVE_PATH:
            raise InjectedCrash("invented v1 marker interruption")
        return answer

    with monkeypatch.context() as faults:
        faults.setattr(spool, "_retirement_publish", interrupt)
        with pytest.raises(InjectedCrash):
            spool.retire_completed_generation(context.root, **args)
    assert spool.read_generation_retirement(context.root)["status"] == "RETIRING"
    assert json.loads((context.root / spool.RETIREMENT_RELATIVE_PATH).read_bytes())["schema_version"] == spool.RETIREMENT_SCHEMA
    result = spool.retire_completed_generation(context.root, **args)
    assert result["status"] == "ARCHIVED_RECORD_ONLY"
    assert "intent_segment_count" not in result
    assert spool.read_generation_retirement(context.root) == result
    assert spool.retire_completed_generation(context.root, **args) == result


def test_explicit_cold_process_resumes_the_same_v2_marker(owned_tree, monkeypatch):
    context, _, args = owned_tree
    with monkeypatch.context() as faults:
        _stop_after_marker(faults)
        with pytest.raises(InjectedCrash):
            spool.retire_completed_generation(context.root, **args)
    payload = {key: ([str(p) for p in value] if key == "protected_paths" else
                     str(value) if isinstance(value, Path) else value) for key, value in args.items()}
    request = args["source_fence_path"].parent / "invented-cold-resume.json"
    request.write_bytes(_encode({"root": str(context.root), "arguments": payload}))
    candidate = Path(spool.__file__).resolve().parent
    canonical = Path(__file__).resolve().parents[3]
    script = """import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1]); sys.path.insert(1,sys.argv[2])
import pm_authority_spool as spool
body=json.loads(Path(sys.argv[3]).read_bytes()); args=body['arguments']
for key in ('protected_paths','owned_direct_files','unit_lock_paths'): args[key]=tuple(args[key])
print(json.dumps(spool.retire_completed_generation(Path(body['root']),**args),sort_keys=True))
"""
    result = subprocess.run([sys.executable, "-E", "-B", "-X", "faulthandler", "-c", script,
        str(candidate), str(canonical), str(request)], capture_output=True, text=True, check=False, timeout=120)
    assert result.returncode == 0, result.stderr
    archived = json.loads(result.stdout)
    assert archived["status"] == "ARCHIVED_RECORD_ONLY"
    assert spool.read_generation_retirement(context.root) == archived
    assert not list(context.root.glob(SEGMENT_PREFIX + "*.json"))


@pytest.mark.parametrize("when,reason", [
    ("after-open", "RETIREMENT_PAYLOAD_CHANGED"),
    ("after-link", "RETIREMENT_HARDLINK_REFUSED"),
])
def test_process_death_inside_segment_publication_retains_all_payloads(owned_tree, when, reason):
    context, _, args = owned_tree
    snapshot = _payload_snapshot(context, args)
    payload = {key: ([str(path) for path in value] if key == "protected_paths" else
                    str(value) if isinstance(value, Path) else value)
               for key, value in args.items()}
    request = args["source_fence_path"].parent / ("invented-control-death-" + when + ".json")
    request.write_bytes(_encode({"root": str(context.root), "arguments": payload}))
    script = """import json,os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1]); sys.path.insert(1,sys.argv[2])
import pm_authority_spool as spool
body=json.loads(Path(sys.argv[3]).read_bytes()); args=body['arguments']; when=sys.argv[4]
for key in ('protected_paths','owned_direct_files','unit_lock_paths'): args[key]=tuple(args[key])
active={'relative':None}
original_publish=spool._retirement_publish_v2; original_open=os.open; original_link=os.link
def publish(root,relative,*values,**kwargs):
    active['relative']=relative
    try:
        return original_publish(root,relative,*values,**kwargs)
    finally:
        active['relative']=None
def opened(path,*values,**kwargs):
    descriptor=original_open(path,*values,**kwargs)
    if when=='after-open' and (active['relative'] or '').startswith('generation-retirement.segment.'):
        os._exit(73)
    return descriptor
def linked(source,target,*values,**kwargs):
    answer=original_link(source,target,*values,**kwargs)
    if when=='after-link' and (active['relative'] or '').startswith('generation-retirement.segment.'):
        os._exit(73)
    return answer
spool._retirement_publish_v2=publish; os.open=opened; os.link=linked
spool.retire_completed_generation(Path(body['root']),**args)
raise AssertionError('synthetic death point was not reached')
"""
    child = subprocess.run(
        [sys.executable, "-E", "-B", "-X", "faulthandler", "-c", script,
         str(Path(spool.__file__).resolve().parent),
         str(Path(storage.__file__).resolve().parent), str(request), when],
        capture_output=True, text=True, check=False, timeout=120)
    assert child.returncode == 73, child.stderr
    assert (context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).exists()
    assert not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    residues = {path: path.read_bytes()
        for path in context.root.rglob("*") if path.is_file()}
    with pytest.raises(spool.AuthoritySpoolError, match="^" + reason + "$"):
        spool.retire_completed_generation(context.root, **args)
    _assert_retained(context, snapshot)
    assert not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    assert not args["retirement_receipt_path"].exists()
    assert {path: path.read_bytes()
        for path in context.root.rglob("*") if path.is_file()} == residues


def test_spool_v2_control_capacity_refuses_before_any_control_publication(owned_tree):
    context, _, args = owned_tree
    snapshot = _payload_snapshot(context, args)
    _, _, _, nodes = spool._retirement_inventory(
        context.root, frozenset(args["owned_direct_files"]),
        max_paths=args["max_paths"], max_nodes=args["max_nodes"],
        max_bytes=args["max_bytes"], retained_unit_locks=args["unit_lock_paths"])
    limited = {**args, "max_nodes": nodes + 3}
    before = {path: path.read_bytes()
        for path in context.root.rglob("*") if path.is_file()}

    with pytest.raises(spool.AuthoritySpoolError, match="^RETIREMENT_CONTROL_CAPACITY$"):
        spool.retire_completed_generation(context.root, **limited)

    _assert_retained(context, snapshot)
    assert not (context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).exists()
    assert not list(context.root.glob(SEGMENT_PREFIX + "*.json"))
    assert not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    assert not (context.root / spool.RETIREMENT_COMPLETE_RELATIVE_PATH).exists()
    assert not args["retirement_receipt_path"].exists()
    assert {path: path.read_bytes()
        for path in context.root.rglob("*") if path.is_file()} == before
