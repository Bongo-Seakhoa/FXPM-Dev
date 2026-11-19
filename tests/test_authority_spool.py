from __future__ import annotations

import copy
import errno
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import threading

import pytest

import pm_authority_spool as spool
import pm_authority_spool_quota as spool_quota


GENERATION = "generation-001"
NATIVE = "a" * 64
CONFIG = "b" * 64


def _context(tmp_path: Path, *, limits: spool.SpoolLimits | None = None):
    return spool.freeze_generation_context(
        tmp_path / "authority-spool",
        generation_id=GENERATION,
        native_universe_sha256=NATIVE,
        config_sha256=CONFIG,
        limits=limits,
    )


def _verify(
    root: Path,
    receipt: dict,
    *,
    symbol: str = "EURUSD",
    manifest_domain: str | None = None,
):
    return spool.verify_symbol_receipt(
        root,
        receipt,
        expected_generation_id=GENERATION,
        expected_symbol=symbol,
        expected_native_universe_sha256=NATIVE,
        expected_config_sha256=CONFIG,
        expected_manifest_domain=manifest_domain,
    )


def _regular_receipt(tmp_path: Path):
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    json_entry = sink.put_json(
        "anchor-0/horizon",
        "historical_horizon_evidence",
        {"schema_version": "test.horizon.v1", "values": [1, 2, 3]},
    )
    chunks = [bytes([value % 251]) * 7 for value in range(1500)]
    opaque_entry = sink.put_opaque(
        "anchor-0/events",
        "position_events",
        iter(chunks),
        format="binary-events-v1",
    )
    return context, sink.seal(), json_entry, opaque_entry, b"".join(chunks)


def _rewrite_manifest(
    root: Path,
    receipt: dict,
    mutate,
) -> dict:
    updated = copy.deepcopy(receipt)
    descriptor = updated["symbol_manifest"]
    path = root.joinpath(*Path(descriptor["relative_path"]).parts)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    mutate(manifest)
    encoded = json.dumps(
        manifest,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    path.write_bytes(encoded)
    descriptor["sha256"] = hashlib.sha256(encoded).hexdigest()
    descriptor["size_bytes"] = len(encoded)
    return updated


def test_regular_spool_preserves_complete_stream_and_exact_receipt(tmp_path: Path) -> None:
    context, receipt, json_entry, opaque_entry, expected = _regular_receipt(tmp_path)

    assert set(receipt) == {
        "schema_version",
        "generation_id",
        "symbol",
        "manifest_domain",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "symbol_manifest",
    }
    assert receipt["manifest_domain"] == spool.GENERAL_MANIFEST_DOMAIN
    assert set(receipt["symbol_manifest"]) == {
        "schema_version",
        "relative_path",
        "sha256",
        "size_bytes",
        "format",
    }
    manifest = _verify(context.root, receipt)
    assert manifest["artifact_count"] == 2
    assert [entry["artifact_id"] for entry in manifest["artifacts"]] == [
        "anchor-0/events",
        "anchor-0/horizon",
    ]

    opaque = opaque_entry["descriptor"]
    opaque_path = context.root.joinpath(*Path(opaque["relative_path"]).parts)
    assert opaque_path.read_bytes() == expected
    assert opaque["size_bytes"] == len(expected)
    assert opaque["sha256"] == hashlib.sha256(expected).hexdigest()
    assert json_entry["descriptor"]["format"] == spool.CANONICAL_JSON_FORMAT


def test_unsealed_artifact_roster_is_canonical_read_only_and_not_published(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(
        context,
        "EURUSD",
        manifest_domain=spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
    )
    second = sink.put_json("b", "test", {"value": 2})
    first = sink.put_json("a", "test", {"value": 1})

    observed = sink.artifact_roster()
    assert observed == [first, second]
    observed[0]["artifact_id"] = "tampered"
    assert sink.artifact_roster() == [first, second]
    assert not (context.root / "symbols").exists()


def test_context_and_symbol_resume_require_byte_identity(tmp_path: Path) -> None:
    context, receipt, _, _, _ = _regular_receipt(tmp_path)
    reopened = spool.freeze_generation_context(
        context.root,
        generation_id=GENERATION,
        native_universe_sha256=NATIVE,
        config_sha256=CONFIG,
    )
    sink = spool.open_symbol_sink(reopened, "EURUSD")
    sink.put_json(
        "anchor-0/horizon",
        "historical_horizon_evidence",
        {"schema_version": "test.horizon.v1", "values": [1, 2, 3]},
    )
    chunks = [bytes([value % 251]) * 7 for value in range(1500)]
    sink.put_opaque(
        "anchor-0/events",
        "position_events",
        chunks,
        format="binary-events-v1",
    )
    assert sink.seal() == receipt

    with pytest.raises(spool.AuthoritySpoolError, match="IMMUTABLE_FILE_CONFLICT"):
        spool.freeze_generation_context(
            context.root,
            generation_id="different-generation",
            native_universe_sha256=NATIVE,
            config_sha256=CONFIG,
        )

    divergent = spool.open_symbol_sink(reopened, "EURUSD")
    divergent.put_json("different", "evidence", {"different": True})
    with pytest.raises(spool.AuthoritySpoolError, match="IMMUTABLE_FILE_CONFLICT"):
        divergent.seal()


def test_duplicate_artifact_id_is_idempotent_only_for_exact_entry(tmp_path: Path) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    first = sink.put_json("cell", "winner", {"value": 1})
    descriptor = dict(first["descriptor"])
    assert sink.register_artifact("cell", "winner", descriptor) == first

    other = spool.write_canonical_json_artifact(context, {"value": 2})
    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_ID_CONFLICT"):
        sink.register_artifact("cell", "winner", other)
    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_ID_CONFLICT"):
        sink.register_artifact("cell", "different-type", descriptor)


def test_exact_opaque_replay_verifies_without_republication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    payload = b"frozen-plan-body"
    first = sink.put_opaque(
        "cell/frozen",
        "winner_attempt_process",
        (payload,),
        format="fxpm-ascii-json-v1",
    )
    monkeypatch.setattr(
        spool,
        "write_opaque_artifact",
        lambda *_args, **_kwargs: pytest.fail("duplicate body was republished"),
    )

    second = sink.put_opaque(
        "cell/frozen",
        "winner_attempt_process",
        (payload,),
        format="fxpm-ascii-json-v1",
    )

    assert second == first
    assert sink.artifact_roster() == [first]


@pytest.mark.parametrize(
    ("artifact_type", "payload"),
    [
        ("winner_attempt_process", b"changed-body"),
        ("different-type", b"frozen-plan-body"),
    ],
)
def test_opaque_replay_preserves_artifact_id_conflict(
    tmp_path: Path,
    artifact_type: str,
    payload: bytes,
) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    sink.put_opaque(
        "cell/frozen",
        "winner_attempt_process",
        (b"frozen-plan-body",),
        format="fxpm-ascii-json-v1",
    )

    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_ID_CONFLICT"):
        sink.put_opaque(
            "cell/frozen",
            artifact_type,
            (payload,),
            format="fxpm-ascii-json-v1",
        )


@pytest.mark.parametrize(
    ("damage", "error"),
    [
        ("missing", "ARTIFACT_FILE_MISSING"),
        ("corrupt", "ARTIFACT_INTEGRITY_MISMATCH"),
        ("truncated", "ARTIFACT_SIZE_MISMATCH"),
    ],
)
def test_new_sink_admission_rejects_damaged_registered_cas(
    tmp_path: Path,
    damage: str,
    error: str,
) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    payload = b"frozen-plan-body"
    entry = sink.put_opaque(
        "cell/frozen",
        "winner_attempt_process",
        (payload,),
        format="fxpm-ascii-json-v1",
    )
    path = context.root.joinpath(
        *Path(entry["descriptor"]["relative_path"]).parts
    )
    if damage == "missing":
        path.unlink()
    elif damage == "corrupt":
        path.write_bytes(b"X" * len(payload))
    else:
        path.write_bytes(payload[:-1])

    cold_sink = spool.open_symbol_sink(context, "EURUSD")
    with pytest.raises(spool.AuthoritySpoolError, match=error):
        cold_sink.register_artifact(
            "cell/frozen",
            "winner_attempt_process",
            entry["descriptor"],
        )


def test_new_sink_admission_rejects_registered_cas_reparse_point(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    payload = b"frozen-plan-body"
    entry = sink.put_opaque(
        "cell/frozen",
        "winner_attempt_process",
        (payload,),
        format="fxpm-ascii-json-v1",
    )
    path = context.root.joinpath(
        *Path(entry["descriptor"]["relative_path"]).parts
    )
    backup = tmp_path / "registered-cas-backup"
    backup.write_bytes(payload)
    path.unlink()
    try:
        os.symlink(backup, path)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation unavailable")

    cold_sink = spool.open_symbol_sink(context, "EURUSD")
    with pytest.raises(spool.AuthoritySpoolError, match="REPARSE_POINT|SAFE_REGULAR"):
        cold_sink.register_artifact(
            "cell/frozen",
            "winner_attempt_process",
            entry["descriptor"],
        )


def test_exact_opaque_replay_preserves_stream_validation(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        limits=spool.SpoolLimits(
            max_artifact_bytes=1024,
            max_symbol_bytes=2048,
            max_artifacts_per_symbol=10,
            max_manifest_bytes=1024,
            max_json_bytes=1024,
            stream_chunk_bytes=128,
            max_generation_bytes=2048,
            min_free_bytes=0,
        ),
    )
    sink = spool.open_symbol_sink(context, "EURUSD")
    sink.put_opaque("cell", "evidence", (b"valid",), format="binary-v1")

    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_EMPTY"):
        sink.put_opaque("cell", "evidence", (), format="binary-v1")
    with pytest.raises(spool.AuthoritySpoolError, match="CHUNK_TYPE_INVALID"):
        sink.put_opaque("cell", "evidence", ("invalid",), format="binary-v1")
    with pytest.raises(spool.AuthoritySpoolError, match="SIZE_BOUND_EXCEEDED"):
        sink.put_opaque("cell", "evidence", (b"x" * 1025,), format="binary-v1")


@pytest.mark.parametrize("kind", ["json", "opaque"])
@pytest.mark.parametrize("batched", [False, True])
def test_sink_reuses_admitted_bytes_and_new_sink_reads_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, batched: bool,
) -> None:
    """INVENTED software payloads: own publication is not read back for admission."""
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    payload_reads = []
    original_stream = spool._stream_file

    def counted_stream(root, relative_path, **kwargs):
        if relative_path.startswith("objects/"):
            payload_reads.append(relative_path)
        return original_stream(root, relative_path, **kwargs)

    monkeypatch.setattr(spool, "_stream_file", counted_stream)

    def put(artifact_id):
        if kind == "json":
            return sink.put_json(artifact_id, "software_fixture", {"invented": [1, "é"]})
        return sink.put_opaque(artifact_id, "software_fixture", (b"invented-", b"bytes"),
                               format="binary-v1")

    with spool.publication_batch(context) if batched else nullcontext():
        first = put("one")
        exact = copy.deepcopy(first)
        assert put("one") == exact
        assert put("two")["descriptor"] == exact["descriptor"]
        assert sink.register_artifact("three", "software_fixture", exact["descriptor"])["descriptor"] == exact["descriptor"]
    assert payload_reads == []

    # Neither an input descriptor nor any returned entry can mutate admission facts.
    detached = sink.register_artifact("one", "software_fixture", exact["descriptor"])
    detached["descriptor"]["sha256"] = "f" * 64
    first["descriptor"]["size_bytes"] = 0
    assert sink.artifact_roster()[0] == exact

    cold_sink = spool.open_symbol_sink(context, "EURUSD")
    descriptor = copy.deepcopy(exact["descriptor"])
    assert cold_sink.register_artifact("one", "software_fixture", descriptor) == exact
    descriptor["size_bytes"] = 0
    assert cold_sink.register_artifact("one", "software_fixture", exact["descriptor"]) == exact
    assert cold_sink.register_artifact("two", "software_fixture", exact["descriptor"])["descriptor"] == exact["descriptor"]
    assert payload_reads == [exact["descriptor"]["relative_path"]]


@pytest.mark.parametrize("replay", [False, True])
def test_opaque_generator_context_change_is_not_hidden_by_sink_reuse(
    tmp_path: Path, replay: bool,
) -> None:
    """INVENTED mutable generator: its real context change must still refuse."""
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    if replay:
        sink.put_opaque("one", "software_fixture", (b"invented",), format="binary-v1")

    def changing_chunks():
        yield b"invented"
        path = context.root / spool.CONTEXT_RELATIVE_PATH
        body = json.loads(path.read_bytes())
        body["config_sha256"] = "c" * 64
        path.write_bytes(spool._canonical_json_bytes(body, spool.MAX_CONTEXT_BYTES))

    with pytest.raises(spool.AuthoritySpoolError, match="GENERATION_CONTEXT_CONFIG_MISMATCH"):
        sink.put_opaque("one", "software_fixture", changing_chunks(), format="binary-v1")
    assert len(sink._artifacts) == int(replay)


def test_fresh_opaque_put_preserves_stream_before_identity_validation(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")

    with pytest.raises(spool.AuthoritySpoolError, match="CHUNK_TYPE_INVALID"):
        sink.put_opaque("", "", ("invalid",), format="binary-v1")


@pytest.mark.parametrize(
    ("field", "replacement", "error"),
    [
        ("generation_id", "generation-002", "GENERATION_MISMATCH"),
        ("symbol", "GBPUSD", "SYMBOL_MISMATCH"),
        ("native_universe_sha256", "c" * 64, "NATIVE_MISMATCH"),
        ("config_sha256", "d" * 64, "CONFIG_MISMATCH"),
    ],
)
def test_receipt_identity_splices_fail_closed(
    tmp_path: Path,
    field: str,
    replacement: str,
    error: str,
) -> None:
    context, receipt, _, _, _ = _regular_receipt(tmp_path)
    spliced = copy.deepcopy(receipt)
    spliced[field] = replacement
    with pytest.raises(spool.AuthoritySpoolError, match=error):
        _verify(context.root, spliced)


@pytest.mark.parametrize(
    ("field", "replacement", "error"),
    [
        ("generation_id", "generation-002", "GENERATION_ID_MISMATCH"),
        ("symbol", "GBPUSD", "SYMBOL_MISMATCH"),
        (
            "manifest_domain",
            spool.CURRENT_REFIT_MANIFEST_DOMAIN,
            "SYMBOL_MANIFEST_DOMAIN_MISMATCH",
        ),
        ("native_universe_sha256", "c" * 64, "NATIVE_UNIVERSE_SHA256_MISMATCH"),
        ("config_sha256", "d" * 64, "CONFIG_SHA256_MISMATCH"),
    ],
)
def test_manifest_identity_splices_fail_closed(
    tmp_path: Path,
    field: str,
    replacement: str,
    error: str,
) -> None:
    context, receipt, _, _, _ = _regular_receipt(tmp_path)
    spliced = _rewrite_manifest(
        context.root,
        receipt,
        lambda manifest: manifest.__setitem__(field, replacement),
    )
    with pytest.raises(spool.AuthoritySpoolError, match=error):
        _verify(context.root, spliced)


def test_artifact_and_manifest_tamper_fail_closed(tmp_path: Path) -> None:
    context, receipt, _, opaque_entry, _ = _regular_receipt(tmp_path)
    artifact = opaque_entry["descriptor"]
    artifact_path = context.root.joinpath(*Path(artifact["relative_path"]).parts)
    content = bytearray(artifact_path.read_bytes())
    content[len(content) // 2] ^= 1
    artifact_path.write_bytes(content)
    with pytest.raises(spool.AuthoritySpoolError, match="INTEGRITY_MISMATCH"):
        _verify(context.root, receipt)

    context2 = spool.freeze_generation_context(
        tmp_path / "other",
        generation_id=GENERATION,
        native_universe_sha256=NATIVE,
        config_sha256=CONFIG,
    )
    sink2 = spool.open_symbol_sink(context2, "EURUSD")
    sink2.put_json("cell", "winner", {"value": 1})
    receipt2 = sink2.seal()
    manifest_path = context2.root.joinpath(
        *Path(receipt2["symbol_manifest"]["relative_path"]).parts
    )
    manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
    with pytest.raises(spool.AuthoritySpoolError, match="SIZE_MISMATCH"):
        _verify(context2.root, receipt2)


def test_traversal_and_content_address_substitution_are_rejected(tmp_path: Path) -> None:
    context, receipt, _, _, _ = _regular_receipt(tmp_path)
    traversal = copy.deepcopy(receipt)
    traversal["symbol_manifest"]["relative_path"] = "../outside.json"
    with pytest.raises(spool.AuthoritySpoolError, match="RELATIVE_PATH_INVALID"):
        _verify(context.root, traversal)

    windows_traversal = copy.deepcopy(receipt)
    windows_traversal["symbol_manifest"]["relative_path"] = "..\\outside.json"
    with pytest.raises(spool.AuthoritySpoolError, match="RELATIVE_PATH_INVALID"):
        _verify(context.root, windows_traversal)

    def mutate(manifest):
        descriptor = manifest["artifacts"][0]["descriptor"]
        descriptor["relative_path"] = "objects/sha256/00/not-the-hash.binary-events-v1"

    substituted = _rewrite_manifest(context.root, receipt, mutate)
    with pytest.raises(spool.AuthoritySpoolError, match="CONTENT_ADDRESS_PATH_MISMATCH"):
        _verify(context.root, substituted)


def test_partial_temporary_file_is_never_observed_as_artifact(tmp_path: Path) -> None:
    context, receipt, _, _, _ = _regular_receipt(tmp_path)
    temp = context.root / "tmp" / "crash-left-partial.part"
    temp.write_bytes(b"partial")
    assert _verify(context.root, receipt)["artifact_count"] == 2
    assert temp.read_bytes() == b"partial"


def test_partial_preexisting_manifest_blocks_seal_without_overwrite(tmp_path: Path) -> None:
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "EURUSD")
    sink.put_json("cell", "winner", {"value": 1})
    symbol_key = hashlib.sha256(b"EURUSD").hexdigest()
    domain_key = hashlib.sha256(spool.GENERAL_MANIFEST_DOMAIN.encode("ascii")).hexdigest()
    manifest = (
        context.root
        / "symbols"
        / symbol_key
        / "domains"
        / domain_key
        / "manifest.json"
    )
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(b'{"partial":')
    with pytest.raises(spool.AuthoritySpoolError, match="IMMUTABLE_FILE_CONFLICT"):
        sink.seal()
    assert manifest.read_bytes() == b'{"partial":'


def test_linked_target_remains_reserved_when_post_link_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    relative_path = "recovery/post-link.json"
    payload = {"complete": True}

    with monkeypatch.context() as patch:
        patch.setattr(
            spool,
            "_fsync_directory",
            lambda _path: (_ for _ in ()).throw(OSError("injected-fsync")),
        )
        with pytest.raises(OSError, match="injected-fsync"):
            spool._publish_exact_json(
                context,
                relative_path,
                payload,
                1024,
            )

    target = context.root.joinpath(*Path(relative_path).parts)
    assert target.is_file()
    dirty = json.loads(
        (context.root / spool_quota.QUOTA_STATE_NAME).read_text(
            encoding="ascii"
        )
    )
    assert dirty["status"] == "DIRTY"
    assert len(dirty["reservations"]) == 1

    descriptor = spool._publish_exact_json(
        context,
        relative_path,
        payload,
        1024,
    )
    assert descriptor["size_bytes"] == target.stat().st_size
    clean = json.loads(
        (context.root / spool_quota.QUOTA_STATE_NAME).read_text(
            encoding="ascii"
        )
    )
    assert clean["status"] == "CLEAN"
    assert clean["reservations"] == []


def test_bounds_fail_instead_of_sampling_or_truncating(tmp_path: Path) -> None:
    limits = spool.SpoolLimits(
        max_artifact_bytes=1024,
        max_symbol_bytes=5,
        max_artifacts_per_symbol=2,
        max_manifest_bytes=1024,
        max_json_bytes=1024,
        stream_chunk_bytes=3,
    )
    context = _context(tmp_path, limits=limits)
    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_SIZE_BOUND_EXCEEDED"):
        spool.write_opaque_artifact(
            context,
            [b"x" * 400, b"y" * 400, b"z" * 400],
            format="binary-v1",
        )

    sink = spool.open_symbol_sink(context, "EURUSD")
    first = sink.put_opaque("one", "events", [b"12345"], format="binary-v1")
    second = sink.put_opaque("two", "events", [b"6"], format="binary-v1")
    assert context.root.joinpath(*Path(first["descriptor"]["relative_path"]).parts).read_bytes() == b"12345"
    assert context.root.joinpath(*Path(second["descriptor"]["relative_path"]).parts).read_bytes() == b"6"

    count_context = spool.freeze_generation_context(
        tmp_path / "count-bound",
        generation_id=GENERATION,
        native_universe_sha256=NATIVE,
        config_sha256=CONFIG,
        limits=spool.SpoolLimits(max_artifacts_per_symbol=1),
    )
    count_sink = spool.open_symbol_sink(count_context, "EURUSD")
    count_sink.put_json("one", "evidence", {"one": 1})
    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_COUNT_BOUND_EXCEEDED"):
        count_sink.put_json("two", "evidence", {"two": 2})


def test_real_spool_publications_remain_accounted_above_legacy_generation_cap(
    tmp_path: Path,
) -> None:
    limits = spool.SpoolLimits(
        max_artifact_bytes=1024,
        max_symbol_bytes=2048,
        max_artifacts_per_symbol=10,
        max_manifest_bytes=1024,
        max_json_bytes=1024,
        stream_chunk_bytes=128,
        max_generation_bytes=2048,
        min_free_bytes=0,
    )
    context = _context(tmp_path, limits=limits)
    first = spool.write_opaque_artifact(
        context,
        [b"a" * 1000],
        format="binary-v1",
    )
    second = spool.write_opaque_artifact(context, [b"b" * 1000], format="binary-v1")
    assert context.root.joinpath(*Path(first["relative_path"]).parts).read_bytes() == b"a" * 1000
    assert context.root.joinpath(*Path(second["relative_path"]).parts).read_bytes() == b"b" * 1000
    assert spool_quota._read_state(context.root)["committed_bytes"] > limits.max_generation_bytes
    assert spool_quota._read_state(context.root)["reservations"] == []
    assert not list((context.root / "tmp").glob("*.part"))


def test_legacy_generation_context_reopens_read_only(tmp_path: Path) -> None:
    root = (tmp_path / "legacy-spool").resolve()
    root.mkdir()
    limits = asdict(spool.SpoolLimits())
    limits.pop("max_generation_bytes")
    limits.pop("min_free_bytes")
    payload = {
        "schema_version": spool.LEGACY_GENERATION_CONTEXT_SCHEMA,
        "generation_id": GENERATION,
        "native_universe_sha256": NATIVE,
        "config_sha256": CONFIG,
        "limits": limits,
    }
    (root / spool.CONTEXT_RELATIVE_PATH).write_bytes(
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )

    context = spool.reopen_generation_context(
        root,
        expected_generation_id=GENERATION,
        expected_native_universe_sha256=NATIVE,
        expected_config_sha256=CONFIG,
    )
    assert context.writable is False
    with pytest.raises(
        spool.AuthoritySpoolError,
        match="GENERATION_CONTEXT_READ_ONLY",
    ):
        spool.write_canonical_json_artifact(context, {"forbidden": True})


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_nonfinite_json_is_rejected(tmp_path: Path, value: float) -> None:
    context = _context(tmp_path)
    with pytest.raises(spool.AuthoritySpoolError, match="JSON_NONFINITE_NUMBER"):
        spool.write_canonical_json_artifact(context, {"value": value})


def test_duplicate_and_noncanonical_json_receipts_are_rejected(tmp_path: Path) -> None:
    context, receipt, _, _, _ = _regular_receipt(tmp_path)
    encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    duplicate = encoded[:-1] + ',"symbol":"EURUSD"}'
    with pytest.raises(spool.AuthoritySpoolError, match="JSON_DUPLICATE_KEY"):
        _verify(context.root, duplicate)
    with pytest.raises(spool.AuthoritySpoolError, match="JSON_NOT_CANONICAL"):
        _verify(context.root, " " + encoded)


def test_opaque_writer_rejects_json_alias_and_bad_chunks(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        limits=spool.SpoolLimits(stream_chunk_bytes=2),
    )
    with pytest.raises(spool.AuthoritySpoolError, match="COLLIDES"):
        spool.write_opaque_artifact(
            context,
            [b"{}"],
            format=spool.CANONICAL_JSON_FORMAT,
        )
    with spool.reserve_transient_publication(context) as held:
        with pytest.raises(spool.AuthoritySpoolError, match="CHUNK_TYPE_INVALID"):
            spool.write_opaque_artifact(
                context,
                [b"ok", "bad"],
                format="binary-v1",
            )
        state = json.loads(
            (context.root / spool_quota.QUOTA_STATE_NAME).read_text(
                encoding="ascii"
            )
        )
        assert [value["token"] for value in state["reservations"]] == [
            held.token
        ]
        held.cancel_after_cleanup()


def test_root_and_artifact_links_are_rejected(tmp_path: Path) -> None:
    real_root = tmp_path / "real-root"
    real_root.mkdir()
    linked_root = tmp_path / "linked-root"
    try:
        os.symlink(real_root, linked_root, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation unavailable")
    with pytest.raises(spool.AuthoritySpoolError, match="REPARSE_POINT|SAFE_DIRECTORY"):
        spool.freeze_generation_context(
            linked_root,
            generation_id=GENERATION,
            native_universe_sha256=NATIVE,
            config_sha256=CONFIG,
        )

    artifact_case = tmp_path / "artifact-case"
    artifact_case.mkdir()
    context, receipt, _, opaque_entry, _ = _regular_receipt(artifact_case)
    descriptor = opaque_entry["descriptor"]
    artifact_path = context.root.joinpath(*Path(descriptor["relative_path"]).parts)
    backup = tmp_path / "artifact-backup"
    backup.write_bytes(artifact_path.read_bytes())
    artifact_path.unlink()
    os.symlink(backup, artifact_path)
    with pytest.raises(spool.AuthoritySpoolError, match="REPARSE_POINT|SAFE_REGULAR"):
        _verify(context.root, receipt)


def test_relative_root_and_invalid_limit_relations_fail(tmp_path: Path) -> None:
    with pytest.raises(spool.AuthoritySpoolError, match="ROOT_NOT_ABSOLUTE"):
        spool.freeze_generation_context(
            Path("relative-spool"),
            generation_id=GENERATION,
            native_universe_sha256=NATIVE,
            config_sha256=CONFIG,
        )
    with pytest.raises(spool.AuthoritySpoolError, match="MAX_JSON_BYTES_EXCEEDS"):
        _context(
            tmp_path,
            limits=spool.SpoolLimits(
                max_artifact_bytes=10,
                max_manifest_bytes=10,
                max_json_bytes=11,
            ),
        )


def test_concurrent_publication_is_exclusive_and_identity_safe(tmp_path: Path) -> None:
    context = _context(tmp_path)

    def publish_object():
        return spool.write_opaque_artifact(
            context,
            [b"complete", b"-stream"],
            format="binary-v1",
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        descriptors = list(pool.map(lambda _: publish_object(), range(8)))
    assert all(value == descriptors[0] for value in descriptors)

    def seal(value: int):
        sink = spool.open_symbol_sink(context, "EURUSD")
        sink.put_json("cell", "evidence", {"value": value})
        return sink.seal()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(seal, value) for value in (1, 2)]
    outcomes = []
    for future in futures:
        try:
            outcomes.append(future.result())
        except spool.AuthoritySpoolError as exc:
            outcomes.append(str(exc))
    assert sum(type(value) is dict for value in outcomes) == 1
    assert sum("IMMUTABLE_FILE_CONFLICT" in str(value) for value in outcomes) == 1


def test_stream_iterators_run_concurrently_outside_quota_lock(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    entered = threading.Barrier(2)

    def publish(payload: bytes):
        def chunks():
            entered.wait(timeout=5)
            yield payload

        return spool.write_opaque_artifact(
            context,
            chunks(),
            format="binary-v1",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        descriptors = list(pool.map(publish, (b"first", b"second")))
    assert {descriptor["size_bytes"] for descriptor in descriptors} == {5, 6}
    assert {
        context.root.joinpath(
            *Path(descriptor["relative_path"]).parts
        ).read_bytes()
        for descriptor in descriptors
    } == {b"first", b"second"}


def test_distinct_authenticated_manifest_domains_coexist_for_one_symbol(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    historical = spool.open_symbol_sink(
        context,
        "EURUSD",
        manifest_domain=spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
    )
    historical.put_json(
        "run/precommit",
        "historical_anchor_precommit",
        {"phase": "historical"},
    )
    historical_receipt = historical.seal()

    current = spool.open_symbol_sink(
        context,
        "EURUSD",
        manifest_domain=spool.CURRENT_REFIT_MANIFEST_DOMAIN,
    )
    current.put_json(
        "current-refit/cell",
        "production_search_cell_plan",
        {"phase": "current-refit"},
    )
    current_receipt = current.seal()

    assert historical_receipt["symbol_manifest"]["relative_path"] != (
        current_receipt["symbol_manifest"]["relative_path"]
    )
    historical_manifest = _verify(
        context.root,
        historical_receipt,
        manifest_domain=spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
    )
    current_manifest = _verify(
        context.root,
        current_receipt,
        manifest_domain=spool.CURRENT_REFIT_MANIFEST_DOMAIN,
    )
    assert historical_manifest["manifest_domain"] == (
        spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN
    )
    assert current_manifest["manifest_domain"] == spool.CURRENT_REFIT_MANIFEST_DOMAIN
    assert [item["artifact_id"] for item in historical_manifest["artifacts"]] == [
        "run/precommit"
    ]
    assert [item["artifact_id"] for item in current_manifest["artifacts"]] == [
        "current-refit/cell"
    ]

    with pytest.raises(
        spool.AuthoritySpoolError,
        match="COMPACT_RECEIPT_MANIFEST_DOMAIN_MISMATCH",
    ):
        _verify(
            context.root,
            historical_receipt,
            manifest_domain=spool.CURRENT_REFIT_MANIFEST_DOMAIN,
        )


@pytest.mark.parametrize("fault", ["partial-write", "fsync"])
def test_v1_control_failure_cleans_only_its_owned_temporary(tmp_path, monkeypatch, fault):
    """Invented control IO: an interrupted write cannot poison later completion."""
    root = tmp_path / "v1-control"
    root.mkdir()
    (root / "tmp").mkdir()
    foreign = root / "tmp" / "foreign.part"
    foreign.write_bytes(b"UNKNOWN_EXISTING_DRAFT_PRESERVE")
    body = {"fixture_provenance": "INVENTED_CONTROL_IO_ONLY", "value": 1}
    expected = hashlib.sha256(spool._canonical_json_bytes(body, 4096)).hexdigest()
    with monkeypatch.context() as controlled:
        if fault == "partial-write":
            def partial(descriptor, value):
                assert os.write(descriptor, value[:7]) == 7
                raise OSError(errno.ENOSPC, "INVENTED_DISK_FULL")
            controlled.setattr(spool, "_write_all", partial)
        else:
            def fail_fsync(_descriptor):
                raise OSError(errno.ENOSPC, "INVENTED_FSYNC_DISK_FULL")
            controlled.setattr(spool.os, "fsync", fail_fsync)
        with pytest.raises(OSError):
            spool._retirement_publish(root, "control.json", body, 4096)
    assert not (root / "control.json").exists()
    assert list((root / "tmp").iterdir()) == [foreign]
    assert foreign.read_bytes() == b"UNKNOWN_EXISTING_DRAFT_PRESERVE"
    assert spool._retirement_publish(root, "control.json", body, 4096) == expected
    assert spool._retirement_control(root, "control.json", 4096) == (body, expected)


@pytest.mark.parametrize("same", [True, False], ids=["identical-final", "conflicting-final"])
def test_v1_control_exclusive_publication_preserves_concurrent_final(tmp_path, monkeypatch, same):
    """A concurrently appeared final is verified, never replaced or removed."""
    root = tmp_path / "v1-race"
    root.mkdir()
    body = {"fixture_provenance": "INVENTED_CONTROL_PUBLICATION_RACE", "value": 1}
    incoming = spool._canonical_json_bytes(body if same else {"foreign": "PRESERVE"}, 4096)
    original = spool._publish_temp_exclusive
    appeared = []
    def arrive(parent, temporary, relative):
        target = parent / relative
        target.write_bytes(incoming)
        info = target.stat()
        appeared.append((info.st_dev, info.st_ino))
        return original(parent, temporary, relative)
    monkeypatch.setattr(spool, "_publish_temp_exclusive", arrive)
    if same:
        assert spool._retirement_publish(root, "control.json", body, 4096) == hashlib.sha256(incoming).hexdigest()
    else:
        with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_CONTROL_CONFLICT"):
            spool._retirement_publish(root, "control.json", body, 4096)
    target = root / "control.json"
    info = target.stat()
    assert appeared == [(info.st_dev, info.st_ino)]
    assert target.read_bytes() == incoming
    assert not list((root / "tmp").iterdir())


def test_exclusive_temp_fstat_failure_closes_the_owned_descriptor(tmp_path, monkeypatch):
    target = tmp_path / "owned.part"
    opened = []
    original_open, original_fstat = spool.os.open, spool.os.fstat
    def observe_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor
    expected = OSError(errno.EIO, "INVENTED_FSTAT_FAILURE")
    with monkeypatch.context() as controlled:
        controlled.setattr(spool.os, "open", observe_open)
        controlled.setattr(spool.os, "fstat", lambda descriptor: (_ for _ in ()).throw(expected))
        with pytest.raises(OSError) as caught:
            spool._open_exclusive(target)
        assert caught.value is expected
    assert len(opened) == 1
    with pytest.raises(OSError) as closed:
        original_fstat(opened[0])
    assert closed.value.errno == errno.EBADF


@pytest.mark.parametrize("stage", ["owner", "context"])
@pytest.mark.parametrize("failure", ["disk", "memory"])
def test_failed_owned_bootstrap_removes_only_its_unpublished_controls(tmp_path, monkeypatch, stage, failure):
    """Invented bootstrap IO; a failed initial call must not strand its own workspace."""
    root = tmp_path / "owned-bootstrap"
    workspace = {"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64}
    schema = spool.WORKSPACE_OWNER_SCHEMA if stage == "owner" else spool.GENERATION_CONTEXT_SCHEMA
    original_write = spool._write_all
    with monkeypatch.context() as controlled:
        def fail_control(descriptor, value):
            body = json.loads(bytes(value).decode("utf-8"))
            if body.get("schema_version") == schema:
                assert os.write(descriptor, value[:7]) == 7
                if failure == "disk":
                    raise OSError(errno.ENOSPC, "INVENTED_BOOTSTRAP_DISK_FULL")
                raise MemoryError("INVENTED_BOOTSTRAP_ALLOCATION_FAILURE")
            return original_write(descriptor, value)
        controlled.setattr(spool, "_write_all", fail_control)
        with pytest.raises((spool.AuthoritySpoolError, OSError, MemoryError)) as caught:
            spool.freeze_generation_context(root, generation_id=GENERATION,
                native_universe_sha256=NATIVE, config_sha256=CONFIG, owned_workspace=workspace)
        assert spool.resource_capacity_reason(caught.value) == ("DISK_LOW" if failure == "disk" else "MEMORY_ERROR")
    assert list(root.iterdir()) == []
    context = spool.freeze_generation_context(root, generation_id=GENERATION,
        native_universe_sha256=NATIVE, config_sha256=CONFIG, owned_workspace=workspace)
    assert spool.read_owned_generation_workspace(context)[0]["cell_id"] == workspace["cell_id"]


def test_owned_bootstrap_first_publication_accounts_for_both_control_files(tmp_path):
    root = tmp_path / "owned-accounting"
    context = spool.freeze_generation_context(root, generation_id=GENERATION,
        native_universe_sha256=NATIVE, config_sha256=CONFIG,
        owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})
    control_bytes = sum((root / name).stat().st_size
        for name in (spool.CONTEXT_RELATIVE_PATH, spool.WORKSPACE_OWNER_RELATIVE_PATH))
    sink = spool.open_symbol_sink(context, "EURUSD")
    entry = sink.put_json("fixture", "software_fixture", {"fixture_provenance": "INVENTED_BOOTSTRAP_ACCOUNTING"})
    receipt = sink.seal()
    state = spool_quota._read_state(root)
    assert state["committed_bytes"] == spool_quota._scan_committed_bytes(root)
    assert state["committed_bytes"] >= control_bytes + entry["descriptor"]["size_bytes"]
    assert spool.verify_symbol_receipt(root, receipt, expected_generation_id=GENERATION,
        expected_symbol="EURUSD", expected_native_universe_sha256=NATIVE,
        expected_config_sha256=CONFIG)["artifacts"] == [entry]


def test_owned_bootstrap_failure_preserves_foreign_appeared_file(tmp_path, monkeypatch):
    root = tmp_path / "foreign-bootstrap"
    foreign = root / "UNKNOWN-DRAFT.txt"
    original_write = spool._write_all
    def foreign_then_fail(descriptor, value):
        body = json.loads(bytes(value).decode("utf-8"))
        if body.get("schema_version") == spool.WORKSPACE_OWNER_SCHEMA:
            foreign.write_bytes(b"UNKNOWN_DRAFT_PRESERVE")
            raise OSError(errno.ENOSPC, "INVENTED_BOOTSTRAP_FAILURE")
        return original_write(descriptor, value)
    monkeypatch.setattr(spool, "_write_all", foreign_then_fail)
    with pytest.raises((spool.AuthoritySpoolError, OSError)):
        spool.freeze_generation_context(root, generation_id=GENERATION,
            native_universe_sha256=NATIVE, config_sha256=CONFIG,
            owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})
    assert foreign.read_bytes() == b"UNKNOWN_DRAFT_PRESERVE"
    with pytest.raises(spool.AuthoritySpoolError, match="WORKSPACE_ROOT_NOT_EMPTY"):
        spool.freeze_generation_context(root, generation_id=GENERATION,
            native_universe_sha256=NATIVE, config_sha256=CONFIG,
            owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})


def test_late_bootstrap_error_preserves_pair_already_observed_by_consumer(tmp_path, monkeypatch):
    """A visible context must never be rolled back from under an actual reader."""
    root = tmp_path / "visible-bootstrap"
    original_publish = spool._publish_temp_exclusive
    observed = []
    def publish_then_fail(parent, temporary, relative):
        original_publish(parent, temporary, relative)
        if relative == spool.CONTEXT_RELATIVE_PATH:
            consumer = spool.reopen_generation_context(root, expected_generation_id=GENERATION,
                expected_native_universe_sha256=NATIVE, expected_config_sha256=CONFIG)
            observed.append(spool.read_owned_generation_workspace(consumer)[0])
            raise OSError(errno.ENOSPC, "INVENTED_FAILURE_AFTER_CONTEXT_VISIBILITY")
    monkeypatch.setattr(spool, "_publish_temp_exclusive", publish_then_fail)
    with pytest.raises((spool.AuthoritySpoolError, OSError)):
        spool.freeze_generation_context(root, generation_id=GENERATION,
            native_universe_sha256=NATIVE, config_sha256=CONFIG,
            owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})
    assert len(observed) == 1
    reopened = spool.reopen_generation_context(root, expected_generation_id=GENERATION,
        expected_native_universe_sha256=NATIVE, expected_config_sha256=CONFIG)
    assert spool.read_owned_generation_workspace(reopened)[0] == observed[0]


def test_held_bootstrap_lock_refuses_without_waiting_or_publishing(tmp_path):
    """A live bootstrap owner is BUSY; a later explicit software call can succeed."""
    root = tmp_path / "busy-bootstrap"
    lock_path = root.parent / f".{root.name}.generation-bootstrap.lock"
    entered, release = threading.Event(), threading.Event()
    def hold():
        with spool_quota._exclusive_file_lock(lock_path, timeout_seconds=0):
            entered.set()
            assert release.wait(timeout=5)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(hold)
        assert entered.wait(timeout=5)
        before = lock_path.stat()
        try:
            with pytest.raises(spool.AuthoritySpoolError, match="^GENERATION_BOOTSTRAP_BUSY$"):
                spool.freeze_generation_context(root, generation_id=GENERATION,
                    native_universe_sha256=NATIVE, config_sha256=CONFIG,
                    owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})
            assert not list(root.iterdir())
        finally:
            release.set()
        future.result(timeout=5)
    context = spool.freeze_generation_context(root, generation_id=GENERATION,
        native_universe_sha256=NATIVE, config_sha256=CONFIG,
        owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})
    after = lock_path.stat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert spool.read_owned_generation_workspace(context)[0]["cell_id"] == "c" * 64


def test_existing_context_freeze_reopen_creates_no_bootstrap_neighbor(tmp_path, monkeypatch):
    """An already issued context never acquires or creates the new parent lock."""
    issued = _context(tmp_path)
    root = tmp_path / "existing-context"
    root.mkdir()
    (root / spool.CONTEXT_RELATIVE_PATH).write_bytes(
        (issued.root / spool.CONTEXT_RELATIVE_PATH).read_bytes())
    lock_path = root.parent / f".{root.name}.generation-bootstrap.lock"
    original_lock = spool_quota._exclusive_file_lock
    observed = []
    def inside_root_only(path, **kwargs):
        path = Path(path)
        assert path.parent == root
        observed.append(path)
        return original_lock(path, **kwargs)
    monkeypatch.setattr(spool_quota, "_exclusive_file_lock", inside_root_only)
    reopened = spool.freeze_generation_context(root, generation_id=GENERATION,
        native_universe_sha256=NATIVE, config_sha256=CONFIG)
    assert reopened.context_sha256 == issued.context_sha256
    assert not lock_path.exists()
    assert all(path.parent == root for path in observed)
