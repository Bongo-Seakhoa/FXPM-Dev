"""INVENTED wrapper wiring with real Spool/quota IO, not full frozen-plan acceptance.

No fits, market data, source authority, account execution, or financial evidence.
The original Raw freeze body/signature retention is separate static assurance.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_authority_spool_quota as quota
import pm_winner_raw_evidence as raw


LABEL = "INVENTED_PLAN_FREEZE_WRAPPER_WIRING_NO_AUTHORITY"
OBJECT_COUNT = 6


def _context(root):
    return spool.freeze_generation_context(root, generation_id="invented-freeze-batch",
        native_universe_sha256="a" * 64, config_sha256="b" * 64)


def _payload(ordinal):
    return {"fixture_provenance": LABEL, "ordinal": ordinal, "payload": "invented-small-object"}


def _invented_publish(sink):
    """Exercise only the real Raw publication path used by the batch wrapper."""
    entries = [raw._put_ascii_json(sink, f"invented/freeze/{ordinal:02d}",
        "invented_plan_freeze_wiring", _payload(ordinal)) for ordinal in range(OBJECT_COUNT)]
    return {"fixture_provenance": LABEL, "artifacts": entries}


def test_plan_freeze_wrapper_batches_real_quota_io_without_changing_payloads(tmp_path, monkeypatch):
    contexts = [_context(tmp_path / name) for name in ("ordinary", "batched")]
    sinks = [spool.open_symbol_sink(context, "INVENTED") for context in contexts]
    counts = {context.root: {"writes": 0, "consumes": []} for context in contexts}
    original_write = quota._write_state
    original_consume = quota.PublicationLease.consume

    def write_state(root, value):
        if Path(root) in counts:
            counts[Path(root)]["writes"] += 1
        return original_write(root, value)

    def consume(lease, byte_count):
        if lease._root in counts:
            counts[lease._root]["consumes"].append(byte_count)
        return original_consume(lease, byte_count)

    monkeypatch.setattr(quota, "_write_state", write_state)
    monkeypatch.setattr(quota.PublicationLease, "consume", consume)
    wrapped = raw._plan_freeze_publication_batch(_invented_publish)
    assert wrapped.__wrapped__ is _invented_publish
    ordinary = _invented_publish(sinks[0])
    ordinary_counts = (counts[contexts[0].root]["writes"], tuple(counts[contexts[0].root]["consumes"]))
    batched = wrapped(sinks[1])
    batched_counts = (counts[contexts[1].root]["writes"], tuple(counts[contexts[1].root]["consumes"]))

    assert batched == ordinary
    assert raw._canonical_bytes(batched) == raw._canonical_bytes(ordinary)
    assert len(ordinary_counts[1]) == OBJECT_COUNT
    assert len(batched_counts[1]) == 1
    assert 0 < batched_counts[0] < ordinary_counts[0]
    receipts, manifests = [], []
    for context, sink, body in zip(contexts, sinks, (ordinary, batched), strict=True):
        for ordinal, entry in enumerate(body["artifacts"]):
            descriptor = entry["descriptor"]
            digest, size, payload = spool._stream_file(context.root, descriptor["relative_path"],
                maximum_bytes=context.limits.max_artifact_bytes,
                expected_size=descriptor["size_bytes"], collect=True)
            assert (digest, size) == (descriptor["sha256"], descriptor["size_bytes"])
            assert payload == raw._canonical_bytes(_payload(ordinal))
        receipt = sink.seal()
        manifest = spool.verify_symbol_receipt(context.root, receipt,
            expected_generation_id=context.generation_id, expected_symbol="INVENTED",
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256)
        assert manifest["artifacts"] == body["artifacts"]
        receipts.append(receipt)
        manifests.append(manifest)
    assert receipts[0] == receipts[1]
    assert manifests[0] == manifests[1]


def test_plan_freeze_wrapper_keeps_original_wrong_sink_refusal(monkeypatch):
    def forbidden_batch(*args, **kwargs):
        raise AssertionError("An invalid sink must reach the original Raw guard before batch access")

    monkeypatch.setattr(spool, "publication_batch", forbidden_batch)
    arguments = dict(symbol="INVENTED", timeframe="H1", regime="TRENDING",
        precommitted_at_utc="2026-01-01T00:00:00Z", attempts_frozen_at_utc="2026-01-01T00:00:01Z",
        fold_plan=(), attempt_artifacts=(), precommitted_evidence_roles=(), horizon_execution_contracts=(),
        source_contract_seeds={}, frozen_tier_pair_contract={}, risk_unit_contract={},
        partition_cost_precommitments={})
    for call in (
        lambda: raw._freeze_winner_search_plan(object(), **arguments),
        lambda: raw._freeze_winner_search_plan(sink=object(), **arguments),
    ):
        with pytest.raises(raw.WinnerRawEvidenceError) as caught:
            call()
        assert caught.value.code == "AUTHORITY_SINK_INVALID"


def test_plan_freeze_nested_batch_keeps_context_boundaries_and_generator_checks(tmp_path, monkeypatch):
    context = _context(tmp_path / "nested")
    sink = spool.open_symbol_sink(context, "INVENTED")
    original_reopen = spool.reopen_generation_context
    reopens, consumed = [], []

    def reopen(root, **kwargs):
        reopens.append((Path(root), dict(kwargs)))
        return original_reopen(root, **kwargs)

    def chunks():
        consumed.append("before")
        yield b"invented-first"
        consumed.append("between")
        yield b"invented-second"
        consumed.append("after")

    monkeypatch.setattr(spool, "reopen_generation_context", reopen)
    with spool.publication_batch(context):
        assert len(reopens) == 1
        with spool.publication_batch(context):
            first = sink.put_opaque("nested", "invented_wiring", chunks(), format="binary-v1")
            assert sink.put_opaque("nested", "invented_wiring", chunks(), format="binary-v1") == first
            assert consumed == ["before", "between", "after"] * 2
            assert len(reopens) == 1
        assert len(reopens) == 1
    assert len(reopens) == 2
    spool.open_symbol_sink(context, "INVENTED")
    assert len(reopens) == 3  # Outside the batch, the normal cold check remains.

    with spool.publication_batch(context):
        assert len(reopens) == 4
        changed = replace(context, context_sha256="c" * 64)
        with pytest.raises(spool.AuthoritySpoolError, match="GENERATION_CONTEXT_CHANGED"):
            spool._assert_context_current(changed)
        assert len(reopens) == 5  # A changed binding cannot borrow the admitted one.
        assert reopens[-1][0] == context.root
    assert len(reopens) == 6
