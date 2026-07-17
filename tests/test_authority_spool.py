from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path

import pytest

import pm_authority_spool as spool


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


def _verify(root: Path, receipt: dict, *, symbol: str = "EURUSD"):
    return spool.verify_symbol_receipt(
        root,
        receipt,
        expected_generation_id=GENERATION,
        expected_symbol=symbol,
        expected_native_universe_sha256=NATIVE,
        expected_config_sha256=CONFIG,
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
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "symbol_manifest",
    }
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
    manifest = context.root / "symbols" / symbol_key / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(b'{"partial":')
    with pytest.raises(spool.AuthoritySpoolError, match="IMMUTABLE_FILE_CONFLICT"):
        sink.seal()
    assert manifest.read_bytes() == b'{"partial":'


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
    sink.put_opaque("one", "events", [b"12345"], format="binary-v1")
    with pytest.raises(spool.AuthoritySpoolError, match="SYMBOL_BYTE_BOUND_EXCEEDED"):
        sink.put_opaque("two", "events", [b"6"], format="binary-v1")

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
    context = _context(tmp_path)
    with pytest.raises(spool.AuthoritySpoolError, match="COLLIDES"):
        spool.write_opaque_artifact(
            context,
            [b"{}"],
            format=spool.CANONICAL_JSON_FORMAT,
        )
    with pytest.raises(spool.AuthoritySpoolError, match="CHUNK_TYPE_INVALID"):
        spool.write_opaque_artifact(context, [b"ok", "bad"], format="binary-v1")


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
