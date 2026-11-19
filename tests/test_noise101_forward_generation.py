"""INVENTED SOFTWARE METADATA ONLY; no source, owner or financial authority.

Public rejected Winner requests exercise version tuples. Reader spies isolate
dispatch and metadata joins; none represents an authenticated selected owner.
"""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

import pm_book_multiplicity as book
import pm_forward_oos_replay_producer as forward
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from tests.test_noise101_winner_generation import _v18_request
from tests.test_public_v17_terminal import _invented_v17_all_rejected_request


@pytest.fixture(scope="module")
def public_requests():
    return {17: _invented_v17_all_rejected_request(), 18: _v18_request()}


def _public_tuple(public_requests, version):
    request = public_requests[version]
    generation = winner.winner_generation_for_precommitment(request.precommitment.to_dict())
    return ({"contract_version": book.book_generation_for_winner(generation).version},
        request.to_dict(), {"contract_version": generation.selection_version})


@pytest.mark.parametrize("version", (17, 18), ids=("v17", "v18"))
def test_public_matched_request_tuple_uses_shared_book_descriptor(public_requests, version):
    observed = forward._selected_owner_generation(*_public_tuple(public_requests, version))
    assert observed is book.book_generation_for_version(f"fxpm.book-multiplicity.v{version}")


@pytest.mark.parametrize("arm", ("book", "request", "precommitment", "evidence", "decision"))
def test_matched_owner_tuple_refuses_each_crossed_identity(public_requests, arm):
    b, r, d = deepcopy(_public_tuple(public_requests, 18))
    old_b, old_r, old_d = _public_tuple(public_requests, 17)
    if arm == "book":
        b = old_b
    elif arm == "request":
        r["contract_version"] = old_r["contract_version"]
    elif arm == "precommitment":
        r["precommitment"] = old_r["precommitment"]
    elif arm == "evidence":
        r["evidence_ledger"] = old_r["evidence_ledger"]
    else:
        d = old_d
    with pytest.raises(forward.ForwardOOSReplayProductionError, match="SELECTED_OWNER_GENERATION_TUPLE_INVALID"):
        forward._selected_owner_generation(b, r, d)


def test_selected_owner_tuple_refuses_unknown_book(public_requests):
    b, r, d = _public_tuple(public_requests, 18)
    b["contract_version"] = "INVENTED_UNKNOWN_BOOK"
    with pytest.raises(forward.ForwardOOSReplayProductionError, match="SELECTED_OWNER_GENERATION_TUPLE_INVALID"):
        forward._selected_owner_generation(b, r, d)


@pytest.mark.parametrize("version", (17, 18), ids=("v17-v9", "v18-v10"))
@pytest.mark.parametrize("damage", (None, "unbound", "missing-reference", "wrong-restored", "cross-schema"),
    ids=("dispatch", "unbound-refused", "missing-reference", "wrong-restored", "cross-schema"))
def test_matched_owner_bound_reader_keeps_generation_and_cross_guards(monkeypatch, version, damage):
    generation = book.book_generation_for_version(f"fxpm.book-multiplicity.v{version}")
    expected = generation.winner_generation
    context = SimpleNamespace(generation_id="INVENTED_FORWARD_DISPATCH", context_sha256="a" * 64,
        native_universe_sha256="b" * 64, config_sha256="c" * 64)
    prefix = "INVENTED/winner-search/H1/TREND/frozen/"
    entry = {"artifact_id": prefix + "receipt", "descriptor": {"fixture": "INVENTED_RECEIPT"}}
    schema = raw.CURRENT_FROZEN_PLAN_SCHEMA if damage == "unbound" else raw.bound_frozen_plan_schema_for_generation(expected)
    receipt = json.dumps({"schema_version": schema}).encode("ascii")
    frozen = SimpleNamespace(frozen_tier_pair_contract_bytes=b"INVENTED_PAIR",
        selection_generation=winner.CURRENT_GENERATION if damage == "wrong-restored" else expected,
        bound_fitted_state_entry=None if damage == "missing-reference" else {"fixture": "INVENTED_REFERENCE"})
    cross = {"schema_version": generation.cross_binding_schema}
    if damage == "cross-schema":
        cross["schema_version"] = book.CURRENT_GENERATION.cross_binding_schema
    calls = []
    def restore(**kwargs):
        calls.append("bound")
        assert kwargs["manifest_entries"] == [entry] and kwargs["body_bytes"] == [receipt]
        assert kwargs["context_identity"]["generation_context_sha256"] == context.context_sha256
        return frozen
    def verify(encoded, *, frozen):
        calls.append("cross")
        assert frozen.selection_generation is expected
        assert json.loads(encoded) == cross
        return cross, (), {"fixture": "INVENTED_CROSS_CONTEXT"}
    monkeypatch.setattr(forward, "_read_descriptor_bytes", lambda *a, **k: receipt)
    monkeypatch.setattr(raw, "_BOUND_GENERATION_FORMATS", tuple(
        (known, {**formats, "restorer": restore} if known is expected else formats)
        for known, formats in raw._BOUND_GENERATION_FORMATS))
    monkeypatch.setattr(raw, "verify_historical_frozen_winner_cross_binding", verify)
    args = dict(context=context, entries={entry["artifact_id"]: entry}, symbol="INVENTED", timeframe="H1",
        regime="TREND", namespace="INVENTED", cross=cross, _generation=generation)
    if damage is None:
        assert forward._restore_current_owner_frozen_binding(**args) == (
            frozen, cross, (), {"fixture": "INVENTED_CROSS_CONTEXT"})
        assert calls == ["bound", "cross"]
    else:
        code = "CURRENT_OWNER_CROSS_BINDING_SCHEMA_INVALID" if damage == "cross-schema" else "CURRENT_OWNER_FROZEN_PAIR_GENERATION_MISMATCH"
        with pytest.raises(forward.ForwardOOSReplayProductionError, match=code):
            forward._restore_current_owner_frozen_binding(**args)
        assert calls == ([] if damage in ("unbound", "cross-schema") else ["bound"])


@pytest.mark.parametrize("damage", (None, "source", "horizon", "manifest", "causal"),
    ids=("joined", "source-refused", "horizon-refused", "manifest-refused", "causal-refused"))
def test_original_bound_source_metadata_join_refuses_splices(monkeypatch, damage):
    """A public reader spy supplies invented metadata, never source authority."""
    context = SimpleNamespace(context_sha256="a" * 64)
    namespace = "INVENTED_BOUND_JOIN"
    state_entry = {"artifact_id": namespace + "/fit-state", "fixture": "INVENTED_ENTRY"}
    session_entry = {"artifact_id": namespace + "/source-session", "fixture": "INVENTED_ENTRY"}
    source, fit = {"fixture": "INVENTED_SOURCE_RECEIPT"}, {"fixture": "INVENTED_FIT_RECEIPT"}
    causal_hash = forward._sha({"fixture": "INVENTED_CAUSAL_PRECOMMIT"})
    search_hash = forward._sha({"fixture": "INVENTED_SEARCH_DESCRIPTION"})
    assert causal_hash != search_hash
    frozen = SimpleNamespace(artifact_namespace=namespace, bound_fitted_state_entry={"state_entry": state_entry},
        precommitment={"causal_fold_plan": [{"fixture": "INVENTED_FOLD"}]},
        source_contract_seeds={"fixture": "INVENTED_SEEDS"}, frozen_tier_pair_contract={"fixture": "INVENTED_PAIR"})
    binding = {"plan_receipt_sha256": "b" * 64}
    metadata = {"binding": {"generation_context_sha256": context.context_sha256,
        "artifact_namespace": namespace, "producer_request_sha256": "c" * 64,
        "precommit_sha256": causal_hash, "process_sha256": search_hash,
        "horizon_plan_receipt_sha256": binding["plan_receipt_sha256"]},
        "source_session_entry": session_entry, "fold_plan": frozen.precommitment["causal_fold_plan"],
        "target_carrier": {"index_sha256": "d" * 64},
        "receipts": {"source_group_decision": source, "regime_fit_decision": fit,
            "source_contract_seeds": frozen.source_contract_seeds,
            "frozen_tier_pair_contract": frozen.frozen_tier_pair_contract}}
    entries = {state_entry["artifact_id"]: state_entry, session_entry["artifact_id"]: session_entry}
    if damage == "source":
        metadata["receipts"]["source_group_decision"] = {"fixture": "INVENTED_CROSSED_SOURCE"}
    elif damage == "horizon":
        metadata["binding"]["horizon_plan_receipt_sha256"] = "e" * 64
    elif damage == "manifest":
        entries.pop(state_entry["artifact_id"])
    elif damage == "causal":
        metadata["binding"]["precommit_sha256"] = search_hash
    reads = []
    def read(*args, **kwargs):
        reads.append(kwargs)
        return {"metadata": metadata}, None
    monkeypatch.setattr(forward.current_refit, "read_bound_fitted_state_reference", read)
    args = dict(context=context, entries=entries, frozen=frozen,
        cross={"process_precommit_receipt_sha256": causal_hash},
        index={"producer_request_sha256": "c" * 64}, binding=binding, source=source, fit=fit,
        native_index="d" * 64, cell_id="e" * 64)
    if damage is None:
        assert forward._verify_bound_owner_source_reference(**args) is None
    else:
        with pytest.raises(forward.ForwardOOSReplayProductionError, match="CURRENT_OWNER_BOUND_SOURCE_REFERENCE_SPLICE"):
            forward._verify_bound_owner_source_reference(**args)
    assert reads == [{"artifact_namespace": namespace}]


@pytest.mark.parametrize("version", (17, 18), ids=("v17", "v18"))
def test_selected_request_read_uses_shared_transport_bound(monkeypatch, public_requests, version):
    b, r, _ = _public_tuple(public_requests, version)
    cell = {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND", "cell_id": "a" * 64,
        "historical_state": "WINNER", "winner_request_sha256": "b" * 64, "winner_decision_sha256": "c" * 64}
    record = {**cell, "historical_state": "WINNER_DECISION", "raw_artifact_namespace": "INVENTED_LIMIT"}
    observed = []
    class StopBeforeRequestDecode(Exception):
        pass
    def read(*args, **kwargs):
        observed.append(kwargs["maximum"])
        raise StopBeforeRequestDecode
    monkeypatch.setattr(forward, "_manifest_body", read)
    with pytest.raises(StopBeforeRequestDecode):
        forward._selected_artifacts(context=None, entries={}, anchor={}, book_body=b,
            book_cell=cell, index={"cell_records": [record]})
    assert observed == [winner.winner_request_byte_limit(r["contract_version"])]
    assert observed[0] == winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES


@pytest.mark.parametrize("version", (17, 18), ids=("v17", "v18"))
def test_matched_owner_cutoff_normalization_keeps_original_stored_bytes(monkeypatch, version):
    cell_id = "a" * 64
    original = {"anchor_id": "INVENTED_ANCHOR", "selection_cutoff_utc": "2025-01-01T00:00:00+00:00"}
    stored = {**original, "selection_cutoff_utc": "2025-01-01T00:00:00Z"}
    body = {"contract_version": f"fxpm.book-multiplicity.v{version}",
        "cells": [{"cell_id": cell_id, "symbol": "EURUSD"}]}
    monkeypatch.setattr(forward, "_verified_manifest", lambda *a, **k: (None, {}))
    monkeypatch.setattr(forward, "_manifest_body", lambda *a, **k: {"anchor": stored})
    monkeypatch.setattr(forward, "_selected_artifacts", lambda **kwargs: kwargs["anchor"])
    result = forward._reopen_owner_capture(context=None, anchor=original, book_body=body,
        cell_id=cell_id, symbol_receipt={}, adapter_index_descriptor={})
    assert result is stored
    assert original["selection_cutoff_utc"].endswith("+00:00")
