"""Current independent-tier production and strict original archive assurance."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import pm_authority_spool as spool
import pm_forward_oos_replay_producer as forward
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from pm_core import Backtester, InstrumentSpec, PipelineConfig
from pm_winner_raw_evidence import (WinnerRawEvidenceError, WinnerRawEvidenceIncomplete,
    build_flat_daily_r_surface, build_precommitted_risk_unit_contract)

RISK_CONTRACT = build_precommitted_risk_unit_contract()


def _canonical(value):
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _reseal(value):
    body = {key: child for key, child in value.items() if key != "receipt_sha256"}
    return {**body, "receipt_sha256": _sha(body)}


def clone_current_native_case(case, tmp_path):
    """Copy original nodes and restore the ORIGINAL frozen artifact-id roster."""
    destination = tmp_path / "spool"
    assert destination.resolve().is_relative_to(tmp_path.resolve())
    shutil.copytree(case["context"].root, destination)
    original = case["context"]
    context = spool.reopen_generation_context(destination, expected_generation_id=original.generation_id,
        expected_native_universe_sha256=original.native_universe_sha256, expected_config_sha256=original.config_sha256)
    assert context.context_sha256 == original.context_sha256
    sink = spool.SymbolAuthoritySink(context, case["sink"].symbol, manifest_domain=case["sink"].manifest_domain)
    for entry in case["sink"].artifact_roster():
        assert sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"]) == entry
    return {**case, "context": context, "sink": sink, "evidence": copy.deepcopy(case["evidence"])}


# Shared session fixture retains one native fit and six-account execution set.
from tests.test_historical_production_adapter import current_native_case


def _produce_case(case, *, evidence=None, frozen=None, cross_binding_bytes=None):
    frozen = case["frozen"] if frozen is None else frozen
    return raw.produce_winner_v15_raw_evidence(case["sink"], frozen=frozen,
        attempt_evidence=case["evidence"] if evidence is None else evidence,
        evidence_access_started_at_utc=case["evidence_started_at_utc"],
        evidence_access_completed_at_utc=case["evidence_completed_at_utc"],
        provenance_cross_binding_bytes=case["cross_binding_bytes"] if cross_binding_bytes is None else cross_binding_bytes,
        artifact_namespace=frozen.artifact_namespace)


def _first_fold(case):
    return next(attempt["folds"][0] for attempt in case["evidence"] if "folds" in attempt)


def _referenced_current_native_case(original, tmp_path):
    """Reuse genuine synthetic-fixture accounts and publish their unchanged maps."""
    case = clone_current_native_case(original, tmp_path)
    case["inline_evidence"] = copy.deepcopy(case["evidence"])
    for attempt in case["evidence"]:
        for fold in attempt.get("folds", ()):
            provenance = fold["provenance"]
            replay = provenance["attempt_replay_receipt"]
            bodies = replay.pop("source_observation_receipts")
            replay["source_observation_references"] = {
                partition: raw.publish_source_observation_receipt(case["sink"], frozen=case["frozen"], receipt=body)
                for partition, body in bodies.items()}
            for role, receipt in provenance.items():
                receipt["schema_version"] = raw.REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA
                provenance[role] = _reseal(receipt)
    return case


def _record_real_source_observation_publication(monkeypatch):
    original = raw.publish_source_observation_receipt
    calls = []
    def record(*args, **kwargs):
        reference = original(*args, **kwargs)
        calls.append((kwargs["frozen"], copy.deepcopy(reference)))
        return reference
    monkeypatch.setattr(raw, "publish_source_observation_receipt", record)
    return calls


def _assert_real_referenced_production_closure(case, production, *, frozen=None, sink=None):
    """Verify writer output and public closure on its sealed, cold-reopened spool."""
    context = case["context"]
    sink = case["sink"] if sink is None else sink
    frozen = case["frozen"] if frozen is None else frozen
    production_entry = production.entries["production-receipt"]
    body = raw._source_observation_closure_body(context, production_entry)
    entries = sink.artifact_roster()
    raw.verify_source_observation_reference_closure(context, frozen=frozen,
        manifest_entries=entries, production_receipt=body)
    prefix = raw._artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)
    shared = {entry["artifact_id"]: entry for entry in entries
        if entry["artifact_id"].startswith(prefix + "/source-observations/")}
    assert shared
    assert {entry["artifact_type"] for entry in shared.values()} == {"winner_source_observation_receipt"}
    assert {key for key in body["artifact_roster"] if key.startswith("source-observation-")} == {
        "source-observation-" + entry["descriptor"]["sha256"] for entry in shared.values()}
    used = set()
    provenance = [entry for entry in entries if entry["artifact_id"].startswith(prefix + "/")
        and "/provenance/" in entry["artifact_id"]]
    assert provenance
    required_attempts = sum(attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
        for attempt in frozen.attempt_ledger["attempts"])
    assert len(provenance) == required_attempts * len(frozen.precommitment["causal_fold_plan"]) * 7
    for entry in provenance:
        receipt = raw._source_observation_closure_body(context, entry)
        assert receipt["schema_version"] == "fxpm.authoritative-search-provenance.v5"
        if receipt["receipt_role"] != "attempt_replay_receipt":
            continue
        assert "source_observation_receipts" not in receipt
        assert set(receipt["source_observation_references"]) == set(raw._PARTITIONS)
        for reference in receipt["source_observation_references"].values():
            source = reference["artifact_entry"]
            assert shared[source["artifact_id"]] == source
            assert body["artifact_roster"]["source-observation-" + source["descriptor"]["sha256"]] == source["descriptor"]
            used.add(source["artifact_id"])
    assert used == set(shared)
    symbol_receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(context.root, symbol_receipt,
        expected_generation_id=context.generation_id, expected_symbol=sink.symbol,
        expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256)
    cold = spool.reopen_generation_context(context.root, expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256)
    raw.verify_source_observation_reference_closure(cold, frozen=frozen,
        manifest_entries=manifest["artifacts"], production_receipt=body)


def _rebind_replay(fold, frozen):
    replay = fold["provenance"]["attempt_replay_receipt"]
    replay["lane_sha256"] = {name: _sha(body) for name, body in fold["lanes"].items()}
    execution = raw._independent_tier_execution_receipt(fold["lanes"], frozen.precommitment["frozen_tier_pair_contract_sha256"])
    replay["independent_tier_execution_receipt"] = execution
    replay["independent_tier_execution_receipt_sha256"] = execution["receipt_sha256"]
    fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)


def _source_observation_transport_case(tmp_path, *, generation_id="source-observation-transport"):
    """Invented transport carrier; no fitted source, search or trading authority."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    context = spool.freeze_generation_context((tmp_path / "spool").resolve(),
        generation_id=generation_id, native_universe_sha256="a" * 64, config_sha256="b" * 64,
        limits=spool.SpoolLimits(max_artifact_bytes=65536, max_symbol_bytes=1024 * 1024,
            max_artifacts_per_symbol=100, max_manifest_bytes=65536, max_json_bytes=65536,
            stream_chunk_bytes=4096,
            max_generation_bytes=2 * 1024 * 1024, min_free_bytes=0))
    sink = spool.open_symbol_sink(context, "EURUSD")
    frozen = raw.FrozenWinnerSearchPlan(symbol="EURUSD", timeframe="H1", regime="TREND",
        artifact_namespace="synthetic-observation-transport", generation_context_sha256=context.context_sha256,
        precommitment_bytes=b"{}", attempt_ledger_bytes=b"{}", source_contract_seed_bytes=b"{}",
        cost_ladder_contract_bytes=b"{}", risk_unit_contract_bytes=b"{}",
        cost_ladder_adversity_plan_bytes=b"{}", frozen_receipt_bytes=b"{}",
        frozen_receipt_entry_bytes=b"{}", horizon_execution_contracts_bytes=b"[]")
    descriptor = spool.write_opaque_artifact(context,
        [_canonical({"fixture_provenance": "SYNTHETIC_TRANSPORT_ONLY_NO_NATIVE_AUTHORITY"})],
        format=raw.ASCII_JSON_FORMAT)
    receipt = _reseal({"schema_version": raw.SOURCE_OBSERVATIONS_SCHEMA,
        "symbol": "EURUSD", "timeframe": "H1", "source_timeframe": "M5",
        "source_product": {"data_dir": str(tmp_path / "synthetic-source"),
            "index_descriptor": descriptor, "index_sha256": descriptor["sha256"],
            "symbol": "EURUSD", "timeframe": "M5", "cutoff_utc": "2020-01-02T00:00:00Z",
            "activation_manifest_sha256": "c" * 64, "production_contract_required": False},
        "observations": [{"target_open_utc": "2020-01-01T00:00:00Z",
            "expected_prior_fine_close_utc": "2020-01-01T00:00:00Z",
            "prior_fine_close_utc": "2020-01-01T00:00:00Z", "points": 9.0}],
        "entry_observations_sha256": "d" * 64})
    return {"context": context, "sink": sink, "frozen": frozen, "receipt": receipt}


def _publish_observation_transport(case, receipt=None):
    return raw.publish_source_observation_receipt(case["sink"], frozen=case["frozen"],
        receipt=case["receipt"] if receipt is None else receipt)


def _resolve_observation_transport(case, reference, *, expected_sha256=None):
    return raw.resolve_source_observation_reference(case["sink"], frozen=case["frozen"],
        reference=reference, expected_receipt_sha256=(case["receipt"]["receipt_sha256"]
            if expected_sha256 is None else expected_sha256))


def test_source_observation_reference_real_writer_resolver_preserves_exact_body(tmp_path):
    case = _source_observation_transport_case(tmp_path)
    original = _canonical(case["receipt"])
    reference = _publish_observation_transport(case)
    assert set(reference) == {"schema_version", "source_observation_receipt_sha256", "artifact_entry"}
    assert reference["schema_version"] == "fxpm.original-fine-spread-observation-reference.v1"
    entry = reference["artifact_entry"]
    assert entry["artifact_type"] == "winner_source_observation_receipt"
    descriptor = entry["descriptor"]
    physical_sha = hashlib.sha256(original).hexdigest()
    assert descriptor["sha256"] == physical_sha
    assert descriptor["sha256"] != reference["source_observation_receipt_sha256"]
    assert descriptor["size_bytes"] == len(original)
    assert descriptor["format"] == raw.ASCII_JSON_FORMAT
    assert entry["artifact_id"].endswith("/source-observations/" + physical_sha)
    assert "/frozen/" not in entry["artifact_id"]
    assert (case["context"].root / descriptor["relative_path"]).read_bytes() == original
    assert _canonical(_resolve_observation_transport(case, reference)) == original
    assert _publish_observation_transport(case) == reference
    assert [item for item in case["sink"].artifact_roster()
        if item["artifact_type"] == "winner_source_observation_receipt"] == [entry]
    assert _canonical(case["receipt"]) == original


@pytest.mark.parametrize("mutation,expected", [
    ("omitted", "SOURCE_OBSERVATION_REFERENCE_SCHEMA_INVALID"),
    ("extra_key", "SOURCE_OBSERVATION_REFERENCE_SCHEMA_INVALID"),
    ("wrong_version", "SOURCE_OBSERVATION_REFERENCE_SCHEMA_INVALID"),
    ("extra_entry_key", "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID"),
    ("extra_descriptor_key", "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID"),
    ("namespace", "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID"),
    ("type", "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID"),
    ("path", "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID"),
    ("oversized_descriptor", "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID"),
    ("logical_sha", "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"),
    ("precommit_sha", "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"),
])
def test_source_observation_reference_rejects_exact_transport_mutations(tmp_path, mutation, expected):
    case = _source_observation_transport_case(tmp_path)
    reference = copy.deepcopy(_publish_observation_transport(case))
    expected_sha = case["receipt"]["receipt_sha256"]
    if mutation == "omitted": reference = None
    elif mutation == "extra_key": reference["unexpected"] = False
    elif mutation == "wrong_version": reference["schema_version"] = raw.SOURCE_OBSERVATIONS_SCHEMA
    elif mutation == "extra_entry_key": reference["artifact_entry"]["unexpected"] = False
    elif mutation == "extra_descriptor_key": reference["artifact_entry"]["descriptor"]["unexpected"] = False
    elif mutation == "namespace": reference["artifact_entry"]["artifact_id"] = "other-cell/source-observations/" + reference["artifact_entry"]["descriptor"]["sha256"]
    elif mutation == "type": reference["artifact_entry"]["artifact_type"] = "winner_attempt_replay_receipt"
    elif mutation == "path": reference["artifact_entry"]["descriptor"]["relative_path"] = "../outside.fxpm-ascii-json-v1"
    elif mutation == "oversized_descriptor": reference["artifact_entry"]["descriptor"]["size_bytes"] = case["context"].limits.max_artifact_bytes + 1
    elif mutation == "logical_sha": reference["source_observation_receipt_sha256"] = "0" * 64
    else: expected_sha = "0" * 64
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _resolve_observation_transport(case, reference, expected_sha256=expected_sha)
    assert error.value.code == expected


@pytest.mark.parametrize("mutation", ["missing", "changed_bytes", "physical_oversize"])
def test_source_observation_reference_rejects_missing_or_changed_physical_body(tmp_path, mutation):
    case = _source_observation_transport_case(tmp_path)
    reference = _publish_observation_transport(case)
    path = case["context"].root / reference["artifact_entry"]["descriptor"]["relative_path"]
    if mutation == "missing":
        path.unlink()
    elif mutation == "changed_bytes":
        original = path.read_bytes()
        assert b'"points":9.0' in original
        path.write_bytes(original.replace(b'"points":9.0', b'"points":8.0', 1))
    else:
        original = path.read_bytes()
        path.write_bytes(original + b" " * (case["context"].limits.max_artifact_bytes - len(original) + 1))
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _resolve_observation_transport(case, reference)
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_REOPEN_FAILED"


def test_source_observation_reference_refuses_other_generation_context(tmp_path):
    first = _source_observation_transport_case(tmp_path / "first", generation_id="source-reference-first")
    second = _source_observation_transport_case(tmp_path / "second", generation_id="source-reference-second")
    reference = _publish_observation_transport(first)
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        raw.resolve_source_observation_reference(second["sink"], frozen=first["frozen"],
            reference=reference, expected_receipt_sha256=first["receipt"]["receipt_sha256"])
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_CONTEXT_INVALID"


def test_source_observation_reference_refuses_resealed_partition_body_splice(tmp_path):
    case = _source_observation_transport_case(tmp_path)
    alternate = copy.deepcopy(case["receipt"])
    alternate["observations"][0]["target_open_utc"] = "2020-01-01T01:00:00Z"
    alternate["observations"][0]["expected_prior_fine_close_utc"] = "2020-01-01T01:00:00Z"
    alternate["observations"][0]["prior_fine_close_utc"] = "2020-01-01T01:00:00Z"
    alternate = _reseal(alternate)
    reference = _publish_observation_transport(case, alternate)
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _resolve_observation_transport(case, reference)
    assert error.value.code == "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"


def test_source_observation_reference_resolver_is_read_only_before_complete_closure(tmp_path):
    case = _source_observation_transport_case(tmp_path)
    reference = _publish_observation_transport(case)
    unregistered = spool.SymbolAuthoritySink(case["context"], "EURUSD")
    assert raw.resolve_source_observation_reference(unregistered, frozen=case["frozen"],
        reference=reference, expected_receipt_sha256=case["receipt"]["receipt_sha256"]) == case["receipt"]
    assert unregistered.artifact_roster() == []


@pytest.mark.parametrize("mutation,expected", [
    ("extra_body_key", "SOURCE_OBSERVATION_RECEIPT_SCHEMA_INVALID"),
    ("extra_carrier_key", "SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID"),
    ("extra_row_key", "SOURCE_OBSERVATION_ROW_INVALID"),
    ("row_not_object", "SOURCE_OBSERVATION_ROW_INVALID"),
    ("rows_not_list", "SOURCE_OBSERVATION_ROSTER_INVALID"),
    ("empty_rows", "SOURCE_OBSERVATION_ROSTER_INVALID"),
    ("negative_points", "SOURCE_OBSERVATION_VALUE_INVALID"),
    ("noninteger_points", "SOURCE_OBSERVATION_VALUE_INVALID"),
    ("noncanonical_utc", "SOURCE_OBSERVATION_AXIS_INVALID"),
    ("stale_self_hash", "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"),
])
def test_source_observation_reference_publisher_rejects_malformed_body(tmp_path, mutation, expected):
    case = _source_observation_transport_case(tmp_path)
    body = copy.deepcopy(case["receipt"])
    if mutation == "extra_body_key": body["unexpected"] = False
    elif mutation == "extra_carrier_key": body["source_product"]["unexpected"] = False
    elif mutation == "extra_row_key": body["observations"][0]["unexpected"] = False
    elif mutation == "row_not_object": body["observations"][0] = 1
    elif mutation == "rows_not_list": body["observations"] = {}
    elif mutation == "empty_rows": body["observations"] = []
    elif mutation == "negative_points": body["observations"][0]["points"] = -1.0
    elif mutation == "noninteger_points": body["observations"][0]["points"] = 0.5
    elif mutation == "noncanonical_utc": body["observations"][0]["target_open_utc"] = "2020-01-01T00:00:00+00:00"
    else: body["observations"][0]["points"] = 8.0
    if mutation != "stale_self_hash": body = _reseal(body)
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _publish_observation_transport(case, body)
    assert error.value.code == expected
    assert case["sink"].artifact_roster() == []


@pytest.mark.parametrize("encoding", ["duplicate_key", "whitespace", "not_object", "malformed"])
def test_source_observation_reference_rejects_noncanonical_stored_body(tmp_path, encoding):
    case = _source_observation_transport_case(tmp_path)
    encoded = _canonical(case["receipt"])
    if encoding == "duplicate_key": encoded = encoded.replace(b'"symbol":"EURUSD"', b'"symbol":"EURUSD","symbol":"EURUSD"', 1)
    elif encoding == "whitespace": encoded += b" "
    elif encoding == "not_object": encoded = b"[]"
    else: encoded = b"{"
    digest = hashlib.sha256(encoded).hexdigest()
    prefix = raw._artifact_prefix(case["frozen"].artifact_namespace, "H1", "TREND")
    entry = case["sink"].put_opaque(prefix + "/source-observations/" + digest,
        "winner_source_observation_receipt", (encoded,), format=raw.ASCII_JSON_FORMAT)
    reference = {"schema_version": "fxpm.original-fine-spread-observation-reference.v1",
        "source_observation_receipt_sha256": case["receipt"]["receipt_sha256"], "artifact_entry": entry}
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _resolve_observation_transport(case, reference)
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_REOPEN_FAILED"


@pytest.mark.parametrize("mutation,expected", [
    ("mixed_version", "ANCHOR_REFIT_RECEIPT_PROVENANCE_ROLE_INVALID"),
    ("inline_field", "ATTEMPT_REPLAY_RECEIPT_PROVENANCE_SCHEMA_INVALID"),
    ("extra_key", "ATTEMPT_REPLAY_RECEIPT_PROVENANCE_SCHEMA_INVALID"),
])
def test_source_observation_reference_provenance_rejects_version_or_schema_mix(tmp_path, mutation, expected):
    from tests.test_authoritative_search_execution import _fold_provenance_transport_fixture
    case = _source_observation_transport_case(tmp_path)
    reference = _publish_observation_transport(case)
    provenance, _, _ = _fold_provenance_transport_fixture(
        source_observation_references={partition: reference for partition in raw._PARTITIONS})
    if mutation == "mixed_version":
        provenance["anchor_refit_receipt"]["schema_version"] = raw.CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA
        provenance["anchor_refit_receipt"] = _reseal(provenance["anchor_refit_receipt"])
    else:
        replay = provenance["attempt_replay_receipt"]
        if mutation == "inline_field":
            replay["source_observation_receipts"] = replay.pop("source_observation_references")
        else:
            replay["unexpected"] = False
        provenance["attempt_replay_receipt"] = _reseal(replay)
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        raw._validated_provenance(frozen=replace(case["frozen"], frozen_tier_pair_contract_bytes=b"{}"),
            attempt={}, attempt_artifacts={}, cross_binding={}, cross_binding_context={},
            cross_binding_attempt={}, planned_fold={}, lanes={}, raw=provenance, sink=case["sink"])
    assert error.value.code == expected


def _source_observation_closure_case(tmp_path, *, referenced=True, replay_mutation=None,
        provenance_mutation=None):
    from tests.test_authoritative_search_execution import _fold_provenance_transport_fixture
    case = _source_observation_transport_case(tmp_path)
    reference = _publish_observation_transport(case) if referenced else None
    provenance, _, _ = _fold_provenance_transport_fixture(source_observation_references=(
        {partition: reference for partition in raw._PARTITIONS} if referenced else None))
    replay = provenance["attempt_replay_receipt"]
    if replay_mutation is not None:
        replay_mutation(replay)
        replay = _reseal(replay)
        provenance["attempt_replay_receipt"] = replay
    if provenance_mutation is not None:
        provenance_mutation(provenance)
    frozen = replace(case["frozen"],
        attempt_ledger_bytes=_canonical({"attempts": [
            {"ordinal": 0, "precommitted_evidence_role": "EVALUATION_REQUIRED"}]}),
        precommitment_bytes=_canonical({"causal_fold_plan": [{"fold_ordinal": 0}]}),
        partition_cost_precommitments_bytes=_canonical({"rows": [
            {"attempt_ordinal": 0, "fold_ordinal": 0, "partition": partition,
                "source_observation_receipt_sha256": case["receipt"]["receipt_sha256"]}
            for partition in raw._PARTITIONS]}))
    prefix = raw._artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)
    roster = {}
    for role, body in provenance.items():
        entry = raw._put_ascii_json(case["sink"],
            prefix + "/attempt-0000/fold-000/provenance/" + role, "winner_" + role, body)
        roster["attempt-0000-fold-000-provenance-" + role] = entry["descriptor"]
    if referenced:
        descriptor = reference["artifact_entry"]["descriptor"]
        roster["source-observation-" + descriptor["sha256"]] = descriptor
    return {**case, "frozen": frozen, "reference": reference, "replay": replay,
        "production_receipt": {"artifact_roster": roster}, "entries": case["sink"].artifact_roster()}


def _verify_observation_transport_closure(case, *, context=None):
    return raw.verify_source_observation_reference_closure(case["context"] if context is None else context,
        frozen=case["frozen"], manifest_entries=case["entries"], production_receipt=case["production_receipt"])


def test_source_observation_reference_symbol_seal_and_cold_closure(tmp_path):
    case = _source_observation_closure_case(tmp_path)
    receipt = case["sink"].seal()
    manifest = spool.verify_symbol_receipt(case["context"].root, receipt,
        expected_generation_id=case["context"].generation_id, expected_symbol="EURUSD",
        expected_native_universe_sha256=case["context"].native_universe_sha256,
        expected_config_sha256=case["context"].config_sha256)
    cold_context = spool.reopen_generation_context(case["context"].root,
        expected_generation_id=case["context"].generation_id,
        expected_native_universe_sha256=case["context"].native_universe_sha256,
        expected_config_sha256=case["context"].config_sha256)
    cold_case = {**case, "entries": manifest["artifacts"]}
    assert _verify_observation_transport_closure(cold_case, context=cold_context) is None
    cold_sink = spool.SymbolAuthoritySink(cold_context, "EURUSD", manifest_domain=case["sink"].manifest_domain)
    for entry in manifest["artifacts"]:
        cold_sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"])
    assert _canonical(raw.resolve_source_observation_reference(cold_sink, frozen=case["frozen"],
        reference=case["reference"], expected_receipt_sha256=case["receipt"]["receipt_sha256"])) == _canonical(case["receipt"])


def test_source_observation_reference_closure_expands_actual_compact_cost_precommitments(tmp_path):
    case = _source_observation_closure_case(tmp_path)
    costs = _partition_transport_case(attempt_count=1, fold_count=1)
    for row in costs["rows"]:
        row["source_observation_receipt_sha256"] = case["receipt"]["receipt_sha256"]
    compact = raw.build_partition_cost_precommitments(costs["rows"], pair=costs["pair"])
    assert compact["schema_version"] == raw.PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA
    assert all("source_observation_receipt_sha256" not in row for row in compact["rows"])
    case["frozen"] = replace(case["frozen"], frozen_tier_pair_contract_bytes=_canonical(costs["pair"]),
        partition_cost_precommitments_bytes=_canonical(compact))
    assert _verify_observation_transport_closure(case) is None


@pytest.mark.parametrize("mutation", ["orphan", "omitted_manifest_source", "omitted_production_source",
    "extra_production_source", "duplicate_manifest", "omitted_replay"])
def test_source_observation_reference_closure_rejects_omitted_or_unused_objects(tmp_path, mutation):
    case = _source_observation_closure_case(tmp_path)
    source_entry = case["reference"]["artifact_entry"]
    source_key = "source-observation-" + source_entry["descriptor"]["sha256"]
    if mutation == "orphan":
        body = copy.deepcopy(case["receipt"])
        body["observations"][0]["points"] = 8.0
        reference = _publish_observation_transport(case, _reseal(body))
        case["entries"] = case["sink"].artifact_roster()
        descriptor = reference["artifact_entry"]["descriptor"]
        case["production_receipt"]["artifact_roster"]["source-observation-" + descriptor["sha256"]] = descriptor
    elif mutation == "omitted_manifest_source":
        case["entries"] = [entry for entry in case["entries"] if entry != source_entry]
    elif mutation == "omitted_production_source":
        del case["production_receipt"]["artifact_roster"][source_key]
    elif mutation == "extra_production_source":
        case["production_receipt"]["artifact_roster"]["source-observation-" + "0" * 64] = source_entry["descriptor"]
    elif mutation == "duplicate_manifest":
        case["entries"].append(copy.deepcopy(source_entry))
    else:
        case["entries"] = [entry for entry in case["entries"] if entry["artifact_type"] != "winner_attempt_replay_receipt"]
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _verify_observation_transport_closure(case)
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID"


@pytest.mark.parametrize("mutation", ["missing_partition", "extra_partition"])
def test_source_observation_reference_closure_requires_exact_partition_roster(tmp_path, mutation):
    def change(replay):
        references = replay["source_observation_references"]
        if mutation == "missing_partition": references.pop("optimizer_train")
        else: references["other"] = references["optimizer_train"]
    case = _source_observation_closure_case(tmp_path, replay_mutation=change)
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _verify_observation_transport_closure(case)
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID"


@pytest.mark.parametrize("mutation", ["omitted_sibling", "mixed_sibling"])
def test_source_observation_reference_cold_closure_requires_coherent_seven_roles(tmp_path, mutation):
    def change(provenance):
        if mutation == "omitted_sibling":
            provenance.pop("anchor_refit_receipt")
        else:
            receipt = provenance["anchor_refit_receipt"]
            receipt["schema_version"] = raw.CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA
            provenance["anchor_refit_receipt"] = _reseal(receipt)
    case = _source_observation_closure_case(tmp_path, provenance_mutation=change)
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _verify_observation_transport_closure(case)
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID"


def test_source_observation_reference_closure_keeps_inline_v4_transport_bytes(tmp_path):
    case = _source_observation_closure_case(tmp_path, referenced=False)
    original = _canonical(case["replay"])
    assert case["replay"]["schema_version"] == raw.CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA
    assert case["replay"]["schema_version"] == "fxpm.authoritative-search-provenance.v4"
    assert "source_observation_references" not in case["replay"]
    assert _verify_observation_transport_closure(case) is None
    assert _canonical(case["replay"]) == original


@pytest.mark.parametrize("referenced", [False, True])
def test_source_observation_complete_current_closure_refuses_total_provenance_omission(tmp_path, referenced):
    case = _source_observation_closure_case(tmp_path, referenced=referenced)
    case["entries"] = []
    case["production_receipt"]["artifact_roster"] = {}
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        _verify_observation_transport_closure(case)
    assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID"


def test_current_raw_v15_uses_six_real_accounts_and_original_spool(current_native_case, tmp_path):
    case = clone_current_native_case(current_native_case, tmp_path)
    result = _produce_case(case)
    assert result.decision.to_dict()["validated"] is True
    assert result.request.to_dict()["contract_version"] == winner.CURRENT_GENERATION.request_version
    assert result.evidence_ledger["contract_version"] == winner.CURRENT_GENERATION.evidence_version
    assert case["frozen"].precommitment["contract_version"] == winner.CURRENT_GENERATION.precommitment_version
    assert case["frozen"].precommitment["frozen_tier_pair_contract"] == case["fitted"].frozen_tier_pair_contract
    required = {partition + "_" + tier for partition in ("optimizer_train", "tuning_validation", "final_lockbox") for tier in ("p75", "p95")}
    for attempt in case["evidence"]:
        for fold in attempt.get("folds", ()):
            assert set(fold["lanes"]) == required
            assert fold["lanes"]["final_lockbox_p75"] is not fold["lanes"]["final_lockbox_p95"]
    for entry in result.evidence_ledger["entries"]:
        assert entry["evidence_status"] == "AUTHENTICATED"
        for fold in entry["folds"]:
            assert set(fold["tier_evidence"]) == {"P75", "P95"}
            assert "completed_trade_open_offset_seconds" not in fold and "trade_power_binding" not in fold
            for tier, data in fold["tier_evidence"].items():
                assert data["spread_tier"] == tier
                assert data["daily_surface_sha256"] and data["trade_vector_sha256"]
    receipt = case["sink"].seal()
    manifest = spool.verify_symbol_receipt(case["context"].root, receipt,
        expected_generation_id=case["context"].generation_id, expected_symbol=case["sink"].symbol,
        expected_native_universe_sha256=case["context"].native_universe_sha256, expected_config_sha256=case["context"].config_sha256)
    assert any(entry["artifact_type"] == "winner_v15_request" for entry in manifest["artifacts"])
    assert not any(entry["artifact_type"] == "winner_v14_request" for entry in manifest["artifacts"])


def test_transport_raw_v16_real_synthetic_accounts_and_cold_lineage(tmp_path, monkeypatch):
    """Small genuine engine lineage over invented bars, never a market qualification."""
    from tests.test_historical_production_adapter import build_current_native_case
    import pm_historical_production_adapter as adapter

    observed_guard = []
    original_guard = raw._check_cumulative_completed_trades

    def record_guard(total, generation):
        original_guard(total, generation)
        assert generation is winner.V16_GENERATION
        observed_guard.append(total)

    monkeypatch.setattr(raw, "_check_cumulative_completed_trades", record_guard)
    case = build_current_native_case(tmp_path / "native-case", retained_native_root=None,
        selected_owner=False, winner_generation=winner.V16_GENERATION)
    assert case["qualification_authority"] is False and case["selected_owner_case"] is False
    assert case["retained_native_root"] is None and case["evidence_deferred"] is False
    native_root = Path(case["native_product"]._data_dir) / "native"
    creation_bytes = (native_root / "synthetic_fixture_creation.json").read_bytes()
    creation = json.loads(creation_bytes)
    assert creation["fixture_provenance"] == "SYNTHETIC_NATIVE_ENGINEERING_INPUT_NO_MARKET_OR_TRADING_AUTHORITY"
    assert creation["qualification_authority"] is False
    assert creation["status"] == "GENERATION_AUDIT_AND_TERMS_COMPLETE"
    assert Path(creation["native_root"]).resolve() == native_root.resolve()
    frozen, context, sink = case["frozen"], case["context"], case["sink"]
    assert frozen.selection_generation is winner.V16_GENERATION
    assert frozen.precommitment["contract_version"] == winner.V16_GENERATION.precommitment_version
    assert json.loads(frozen.frozen_receipt_bytes)["schema_version"] == raw.V16_FROZEN_PLAN_SCHEMA
    assert len(case["evidence"]) == 1 and len(case["evidence"][0]["folds"]) == 4
    required_lanes = {partition + "_" + tier for partition in raw._PARTITIONS for tier in ("p75", "p95")}
    for fold in case["evidence"][0]["folds"]:
        assert set(fold["lanes"]) == required_lanes
    account_lane_count = sum(len(fold["lanes"]) for attempt in case["evidence"] for fold in attempt["folds"])
    assert account_lane_count == 24
    cross_entry = sink.put_opaque(
        raw._artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime) + "/horizon-winner-cross-binding",
        "historical_horizon_winner_cross_binding", (case["cross_binding_bytes"],), format=raw.ASCII_JSON_FORMAT)
    result = raw.produce_winner_v16_raw_evidence(sink, frozen=frozen, attempt_evidence=case["evidence"],
        evidence_access_started_at_utc=case["evidence_started_at_utc"],
        evidence_access_completed_at_utc=case["evidence_completed_at_utc"],
        provenance_cross_binding_bytes=case["cross_binding_bytes"], artifact_namespace=frozen.artifact_namespace)
    assert result.request.to_dict()["contract_version"] == winner.V16_GENERATION.request_version
    assert result.evidence_ledger["contract_version"] == winner.V16_GENERATION.evidence_version
    assert frozen.attempt_ledger["contract_version"] == winner.ATTEMPT_LEDGER_VERSION
    assert all(axis["shared_oos_partition"]["schema_version"] == winner.V16_GENERATION.shared_partition_version
        for axis in result.evidence_ledger["shared_fold_axis"])
    decision_body = result.decision.to_dict()
    assert decision_body["contract_version"] == winner.V16_GENERATION.selection_version
    assert decision_body["validated"] is True and result.decision.status == "NO_PROCESS_WINNER"
    for field in ("live_authorized", "capital_sizing_authorized", "profitability_claimed", "production_integrated"):
        assert decision_body[field] is False
    assert result.marginal_receipt is not None
    assert result.marginal_receipt["contract_version"] == winner.V16_GENERATION.marginal_version
    expected_guard, total = [], 0
    for entry in result.evidence_ledger["entries"]:
        assert entry["evidence_status"] == "AUTHENTICATED" and len(entry["folds"]) == 4
        for fold in entry["folds"]:
            assert set(fold["tier_evidence"]) == {"P75", "P95"}
            for tier in ("P75", "P95"):
                total += len(fold["tier_evidence"][tier]["completed_trade_open_offset_seconds"])
                expected_guard.append(total)
    assert len(observed_guard) == 8 and observed_guard == expected_guard
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(context.root, receipt, expected_generation_id=context.generation_id,
        expected_symbol=sink.symbol, expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256, expected_manifest_domain=spool.GENERAL_MANIFEST_DOMAIN)
    cold = spool.reopen_generation_context(context.root, expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256)
    assert manifest["manifest_domain"] == spool.GENERAL_MANIFEST_DOMAIN
    cross_matches = [entry for entry in manifest["artifacts"] if entry["artifact_id"] == cross_entry["artifact_id"]]
    assert cross_matches == [cross_entry]
    cold_cross_entry = cross_matches[0]
    cold_cross_bytes = (cold.root / cold_cross_entry["descriptor"]["relative_path"]).read_bytes()
    assert cold_cross_bytes == case["cross_binding_bytes"]
    cross_sha256 = hashlib.sha256(cold_cross_bytes).hexdigest()
    assert cross_sha256 == cold_cross_entry["descriptor"]["sha256"]
    assert len(cold_cross_bytes) == cold_cross_entry["descriptor"]["size_bytes"]
    production_bytes = (cold.root / result.entries["production-receipt"]["descriptor"]["relative_path"]).read_bytes()
    production = json.loads(production_bytes)
    assert production["provenance_cross_binding_sha256"] == cross_sha256
    artifact_types = {entry["artifact_type"] for entry in manifest["artifacts"]}
    assert {"winner_v16_request", "winner_v16_decision"} <= artifact_types
    assert not artifact_types.intersection({"winner_v14_request", "winner_v15_request", "winner_v15_decision"})
    frozen_prefix = raw._artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime) + "/frozen/"
    frozen_entries = [entry for entry in manifest["artifacts"] if entry["artifact_id"].startswith(frozen_prefix)]
    frozen_bodies = [(cold.root / entry["descriptor"]["relative_path"]).read_bytes() for entry in frozen_entries]
    identity = {"generation_id": cold.generation_id, "generation_context_sha256": cold.context_sha256,
        "native_universe_sha256": cold.native_universe_sha256, "config_sha256": cold.config_sha256}
    cell = {key: getattr(frozen, key) for key in ("symbol", "timeframe", "regime")}
    restored = raw.restore_frozen_winner_search_plan_v5(context_identity=identity, **cell,
        artifact_namespace=frozen.artifact_namespace, manifest_entries=frozen_entries, body_bytes=frozen_bodies)
    assert restored == frozen
    logical_entries = {entry["artifact_id"][len(frozen_prefix):].replace("/", "-"): entry
        for entry in frozen_entries if entry["artifact_id"] != frozen_prefix + "receipt"}
    rebuilt_receipt = raw.build_frozen_winner_search_plan_v5_receipt(context_identity=identity, **cell,
        artifact_namespace=restored.artifact_namespace, precommitment=restored.precommitment,
        attempt_ledger=restored.attempt_ledger, horizon_execution_contracts=json.loads(restored.horizon_execution_contracts_bytes),
        partition_cost_precommitments=restored.partition_cost_precommitments, artifact_entries=logical_entries)
    assert _canonical(rebuilt_receipt) == frozen.frozen_receipt_bytes
    cross, cross_attempts, _ = raw.verify_historical_frozen_winner_cross_binding(cold_cross_bytes, frozen=restored)
    assert cross["schema_version"] == raw.HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4
    assert len(cross_attempts) == 1
    reopened_request, reopened_decision = adapter._rebuild_winner_pair(cold,
        {"request": result.entries["winner-request"]["descriptor"], "request_sha256": result.request.request_sha256,
         "decision": result.entries["winner-decision"]["descriptor"], "decision_sha256": result.decision.receipt_sha256}, **cell)
    assert _canonical(reopened_request.to_dict()) == _canonical(result.request.to_dict())
    assert reopened_decision._body_json == result.decision._body_json
    assert winner.verify_historical_cell_winner_decision(reopened_decision, reopened_request)
    assert production["schema_version"] == raw.V16_PRODUCTION_SCHEMA
    assert production["request_sha256"] == reopened_request.request_sha256
    assert production["decision_receipt_sha256"] == reopened_decision.receipt_sha256
    assert production["provenance_cross_binding_sha256"] == cross_sha256
    assert observed_guard == expected_guard
    output = os.environ.get("FXPM_WINNER_QA_OUTPUT")
    if output:
        record = {"schema_version": "fxpm.qa177.synthetic-raw-lineage.v1",
            "fixture_provenance": creation["fixture_provenance"], "qualification_authority": False,
            "profitability_claimed": False, "live_or_order_authority": False, "selected_owner": False,
            "source_creation_sha256": hashlib.sha256(creation_bytes).hexdigest(),
            "attempt_count": len(case["evidence"]), "fold_count": len(case["evidence"][0]["folds"]),
            "independent_account_lane_count": account_lane_count,
            "raw_cumulative_guard_observations": observed_guard, "completed_tier_account_trades": total,
            "versions": {"frozen": rebuilt_receipt["schema_version"], "cross": cross["schema_version"],
                "production": production["schema_version"], "request": reopened_request.to_dict()["contract_version"],
                "decision": decision_body["contract_version"], "evidence": result.evidence_ledger["contract_version"],
                "marginal": result.marginal_receipt["contract_version"],
                "attempt_ledger": frozen.attempt_ledger["contract_version"],
                "shared_partition": winner.V16_GENERATION.shared_partition_version},
            "frozen_cold_restore_byte_exact": True, "cross_cold_reopen_byte_exact": True,
            "cross_sha256": cross_sha256, "request_and_decision_cold_replay_verified": True,
            "dense_at_cap_production_or_thirteen_owner_chain": False}
        with (Path(output) / "raw_lineage_v16.json").open("xb") as handle:
            handle.write(_canonical(record))


def test_current_referenced_raw_v15_uses_original_native_accounts_and_cold_closure(current_native_case, tmp_path):
    case = _referenced_current_native_case(current_native_case, tmp_path)
    for original, compact in zip(case["inline_evidence"], case["evidence"], strict=True):
        for original_fold, compact_fold in zip(original.get("folds", ()), compact.get("folds", ()), strict=True):
            assert compact_fold["lanes"] == original_fold["lanes"]
            references = compact_fold["provenance"]["attempt_replay_receipt"]["source_observation_references"]
            original_maps = original_fold["provenance"]["attempt_replay_receipt"]["source_observation_receipts"]
            for partition in raw._PARTITIONS:
                rebuilt = raw.resolve_source_observation_reference(case["sink"], frozen=case["frozen"],
                    reference=references[partition], expected_receipt_sha256=original_maps[partition]["receipt_sha256"])
                assert _canonical(rebuilt) == _canonical(original_maps[partition])
    result = _produce_case(case)
    assert result.request.to_dict()["contract_version"] == winner.CURRENT_GENERATION.request_version
    assert result.decision.to_dict()["validated"] is True
    assert result.decision.status == "NO_PROCESS_WINNER"
    assert case["qualification_authority"] is False
    _assert_real_referenced_production_closure(case, result)


@pytest.mark.parametrize("mutation,expected", [
    ("missing_reference", "ATTEMPT_REPLAY_PROVENANCE_SPLICE"),
    ("extra_reference", "ATTEMPT_REPLAY_PROVENANCE_SPLICE"),
    ("rebound_claim", "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"),
    ("orphan_body", "SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID"),
    ("mixed_fold_versions", "ATTEMPT_REPLAY_PROVENANCE_VERSION_MIXED"),
    ("unknown_first_schema", "FOLD_PROVENANCE_SCHEMA_INVALID"),
    ("unhashable_first_schema", "FOLD_PROVENANCE_SCHEMA_INVALID"),
    ("registered_body_splice", "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"),
], ids=["missing_reference", "extra_reference", "rebound_claim", "orphan_body", "mixed_fold_versions",
    "unknown_first_schema", "unhashable_first_schema", "registered_body_splice"])
def test_current_referenced_raw_v15_refuses_production_transport_tamper(
        current_native_case, tmp_path, monkeypatch, mutation, expected):
    case = _referenced_current_native_case(current_native_case, tmp_path)
    fold = _first_fold(case)
    replay = fold["provenance"]["attempt_replay_receipt"]
    references = replay["source_observation_references"]
    body_binding_calls = []
    original_sha = alternate_sha = None
    if mutation == "missing_reference":
        references.pop("optimizer_train")
    elif mutation == "extra_reference":
        references["other"] = copy.deepcopy(references["optimizer_train"])
    elif mutation == "rebound_claim":
        references["optimizer_train"]["source_observation_receipt_sha256"] = "0" * 64
    elif mutation in {"orphan_body", "registered_body_splice"}:
        original_reference = references["optimizer_train"]
        original_sha = original_reference["source_observation_receipt_sha256"]
        alternate = raw.resolve_source_observation_reference(case["sink"], frozen=case["frozen"],
            reference=original_reference, expected_receipt_sha256=original_sha)
        alternate = copy.deepcopy(alternate)
        point = alternate["observations"][0]["points"]
        alternate["observations"][0]["points"] = 1.0 if point is None else point + 1.0
        alternate = _reseal(alternate)
        alternate_sha = alternate["receipt_sha256"]
        assert alternate_sha != original_sha
        registered = raw.publish_source_observation_receipt(case["sink"], frozen=case["frozen"], receipt=alternate)
        assert registered["artifact_entry"] in case["sink"].artifact_roster()
        if mutation == "registered_body_splice":
            original_reference["artifact_entry"] = registered["artifact_entry"]
            assert original_reference["source_observation_receipt_sha256"] == original_sha
            original_validator = raw._source_observation_body
            def observe_body(value, **kwargs):
                body_binding_calls.append((value.get("receipt_sha256"), kwargs.get("expected_receipt_sha256")))
                return original_validator(value, **kwargs)
            monkeypatch.setattr(raw, "_source_observation_body", observe_body)
    elif mutation == "mixed_fold_versions":
        compact_attempt = next(attempt for attempt in case["evidence"] if "folds" in attempt)
        inline_attempt = next(attempt for attempt in case["inline_evidence"] if "folds" in attempt)
        assert len(compact_attempt["folds"]) > 1
        compact_attempt["folds"][1]["provenance"] = copy.deepcopy(inline_attempt["folds"][1]["provenance"])
    else:
        role = raw._PROVENANCE_KEYS[0]
        first = fold["provenance"][role]
        first["schema_version"] = "fxpm.authoritative-search-provenance.unknown" if mutation == "unknown_first_schema" else {}
        fold["provenance"][role] = _reseal(first)
    fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    if mutation == "orphan_body":
        with pytest.raises(raw.WinnerRawEvidenceError) as error:
            _produce_case(case)
        assert not isinstance(error.value, raw.WinnerRawEvidenceIncomplete)
        assert error.value.code == expected
    else:
        with pytest.raises(raw.WinnerRawEvidenceIncomplete) as error:
            _produce_case(case)
        assert error.value.code == "ATTEMPT_TECHNICAL_EVIDENCE_FAILURE"
        assert isinstance(error.value.__cause__, raw.WinnerRawEvidenceError)
        assert error.value.__cause__.code == expected
    if mutation == "registered_body_splice":
        assert (alternate_sha, original_sha) in body_binding_calls
    assert not any(entry["artifact_type"] == "winner_raw_evidence_production_receipt"
        for entry in case["sink"].artifact_roster())


@pytest.mark.parametrize("name", ["freeze_winner_search_plan_v3", "produce_winner_v14_raw_evidence"])
def test_original_v14_writer_entrypoints_refuse_before_inputs(name):
    class Forbidden:
        def __getattribute__(self, attribute):
            pytest.fail("retired writer touched an input")
    with pytest.raises(raw.WinnerRawEvidenceError, match="^ARCHIVED_WINNER_V14_WRITER_RETIRED_FROZEN_TIER_PAIR_REQUIRED$"):
        getattr(raw, name)(Forbidden(), frozen=Forbidden(), attempt_evidence=Forbidden())


@pytest.mark.parametrize("mutation,expected", [
    ("extra_base", "FOLD_000_RAW_SCHEMA_INVALID"),
    ("lane_hash", "ATTEMPT_REPLAY_PROVENANCE_SPLICE"),
    ("execution_receipt", "ATTEMPT_REPLAY_PROVENANCE_SPLICE"),
    ("legacy_replay", "ATTEMPT_REPLAY_RECEIPT_PROVENANCE_SCHEMA_INVALID"),
    ("signal_content", "SIGNAL_VECTOR_CONTENT_HASH_INVALID"),
    ("geometry", "PARTITION_GEOMETRY_0_LANE_SPLICE"),
    ("source_native", "OOS_SOURCE_PROVENANCE_SPLICE"),
    ("spec_source", "EXECUTION_SPEC_RECEIPT_BINDING_INVALID"),
    ("cost_array", "PARTITION_COST_PRECOMMITMENTS_ACTUAL_ACCOUNT_SPLICE"),
    ("admission_mask", "TIER_ADMISSION_RECONSTRUCTION_FAILED"),
    ("admission_counts", "TIER_ADMISSION_RECONSTRUCTION_FAILED"),
    ("observation", "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"),
    ("source_receipt_role", "OOS_SOURCE_RECEIPT_PROVENANCE_ROLE_INVALID"),
    ("posthoc_trial", "SELECTED_TRIAL_PROVENANCE_SPLICE"),
])
def test_current_raw_resealed_tamper_is_technical(current_native_case, tmp_path, mutation, expected):
    case = clone_current_native_case(current_native_case, tmp_path)
    fold = _first_fold(case)
    replay = fold["provenance"]["attempt_replay_receipt"]
    if mutation == "extra_base":
        fold["lanes"]["final_lockbox_base"] = copy.deepcopy(fold["lanes"]["final_lockbox_p75"])
    elif mutation == "lane_hash":
        replay["lane_sha256"]["optimizer_train_p75"] = "0" * 64
        fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    elif mutation == "execution_receipt":
        replay["independent_tier_execution_receipt"]["lanes"]["optimizer_train_p75"]["position_events_sha256"] = "0" * 64
        replay["independent_tier_execution_receipt"] = _reseal(replay["independent_tier_execution_receipt"])
        replay["independent_tier_execution_receipt_sha256"] = replay["independent_tier_execution_receipt"]["receipt_sha256"]
        fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    elif mutation == "legacy_replay":
        replay["cost_ladder_method_receipt"] = {}
        fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    elif mutation == "signal_content":
        signal = replay["signal_vector_receipts"]["optimizer_train"]
        signal["values"][0] = 1 if signal["values"][0] == 0 else 0
        replay["signal_vector_receipts"]["optimizer_train"] = _reseal(signal)
        fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    elif mutation == "geometry":
        horizon = fold["provenance"]["anchor_horizon_decision"]
        horizon["partition_geometry"][0]["scored_bars"] += 1
        horizon["partition_geometry"][0] = _reseal(horizon["partition_geometry"][0])
        horizon["partition_geometry_receipts"][0] = horizon["partition_geometry"][0]["receipt_sha256"]
        fold["provenance"]["anchor_horizon_decision"] = _reseal(horizon)
    elif mutation == "source_native":
        source = fold["provenance"]["oos_source_receipt"]
        source["native_source_sha256"] = "0" * 64
        fold["provenance"]["oos_source_receipt"] = _reseal(source)
    elif mutation == "spec_source":
        source = fold["provenance"]["oos_source_receipt"]
        source["execution_spec_receipt"]["tick_value_source"] = "UNBOUND_FALLBACK"
        source["execution_spec_receipt"] = _reseal(source["execution_spec_receipt"])
        fold["provenance"]["oos_source_receipt"] = _reseal(source)
    elif mutation == "cost_array":
        lane = fold["lanes"]["optimizer_train_p75"]
        attestation = lane["numerical_execution_cost_attestation"]
        from pm_trade_power import build_numerical_execution_cost_attestation
        changed_entry_sha = "0" * 64 if attestation["entry_half_spread_values_sha256"] != "0" * 64 else "1" * 64
        lane["numerical_execution_cost_attestation"] = build_numerical_execution_cost_attestation(
            use_spread=attestation["use_spread"], spread_mode=attestation["spread_mode"],
            instrument_spec_sha256=attestation["instrument_spec_sha256"],
            entry_half_spread_values_sha256=changed_entry_sha,
            intrabar_half_spread_values_sha256=attestation["intrabar_half_spread_values_sha256"])
        assert lane["numerical_execution_cost_attestation"] != attestation
        _rebind_replay(fold, case["frozen"])
    elif mutation == "admission_mask":
        fold["lanes"]["optimizer_train_p75"]["spread_admission"]["entry_admission_mask_sha256"] = "0" * 64
        _rebind_replay(fold, case["frozen"])
    elif mutation == "admission_counts":
        data = fold["lanes"]["optimizer_train_p75"]["spread_admission"]
        assert data["signal_opportunities"] > 0
        for key in ("signal_opportunities", "known_cap_pass_opportunities", "known_cap_refused_opportunities", "missing_refused_opportunities"):
            data[key] = 0
        _rebind_replay(fold, case["frozen"])
    elif mutation == "observation":
        original = replay["source_observation_receipts"]["optimizer_train"]
        original["observations"][0]["points"] = 12345.0
        replay["source_observation_receipts"]["optimizer_train"] = _reseal(original)
        fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    elif mutation == "source_receipt_role":
        source = fold["provenance"]["oos_source_receipt"]
        source["receipt_role"] = "anchor_refit_receipt"
        fold["provenance"]["oos_source_receipt"] = _reseal(source)
    elif mutation == "posthoc_trial":
        selected = fold["provenance"]["anchor_selected_trial"]
        selected["selection_scope"] = "POSTHOC_WINNER"
        fold["provenance"]["anchor_selected_trial"] = _reseal(selected)
    with pytest.raises(raw.WinnerRawEvidenceIncomplete, match=expected):
        _produce_case(case)


@pytest.mark.parametrize("mutation,expected", [
    ("missing", "PROVENANCE_CROSS_BINDING_BYTES_INVALID"),
    ("pair", "PROVENANCE_CROSS_BINDING_WINNER_SPLICE"),
    ("partition", "PROVENANCE_CROSS_BINDING_WINNER_SPLICE"),
    ("roster", "PROVENANCE_CROSS_BINDING_MAPPING_INVALID"),
    ("mixed_mode", "PROVENANCE_CROSS_BINDING_SCHEMA_INVALID"),
])
def test_current_raw_cross_binding_rejects_before_evidence(current_native_case, tmp_path, mutation, expected):
    case = clone_current_native_case(current_native_case, tmp_path)
    binding = json.loads(case["cross_binding_bytes"])
    encoded = case["cross_binding_bytes"]
    if mutation == "missing": encoded = b""
    elif mutation == "pair": binding["winner_roster"]["frozen_tier_pair_contract_sha256"] = "0" * 64
    elif mutation == "partition": binding["winner_roster"]["partition_cost_precommitments_sha256"] = "0" * 64
    elif mutation == "roster": binding["mapping_count"] += 1
    elif mutation == "mixed_mode":
        binding["schema_version"] = raw.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4
        binding["mode"] = "CURRENT"
    if mutation != "missing": encoded = _canonical(binding)
    def forbidden():
        pytest.fail("invalid original cross-binding consumed outcome evidence")
        yield None
    with pytest.raises(raw.WinnerRawEvidenceError, match=expected):
        _produce_case(case, evidence=forbidden(), cross_binding_bytes=encoded)


@pytest.mark.parametrize("mutation,expected", [
    ("points", "SOURCE_OBSERVATION_VALUES_CHANGED"),
    ("matched_close", "SOURCE_OBSERVATION_VALUES_CHANGED"),
    ("carrier", "SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID"),
])
def test_current_source_receipt_is_rebuilt_from_original_native_rows(current_native_case, tmp_path, mutation, expected):
    case = clone_current_native_case(current_native_case, tmp_path)
    fold = _first_fold(case)
    replay = fold["provenance"]["attempt_replay_receipt"]
    observation = replay["source_observation_receipts"]["optimizer_train"]
    if mutation == "points":
        point = observation["observations"][0]["points"]
        observation["observations"][0]["points"] = 1 if point is None else point + 1
    elif mutation == "matched_close":
        row = observation["observations"][0]
        row["prior_fine_close_utc"] = row["target_open_utc"] if row["prior_fine_close_utc"] is None else None
    else:
        observation["source_product"]["timeframe"] = "INVALID"
    observation = _reseal(observation)
    # Even a self-consistent receipt hash cannot replace original native rows.
    with pytest.raises(raw.WinnerRawEvidenceError, match=expected):
        raw._validated_source_observation_receipt(observation, sink=case["sink"], frozen=case["frozen"],
            expected_sha256=observation["receipt_sha256"], partition="optimizer_train",
            signals=replay["signal_vector_receipts"]["optimizer_train"], lanes=fold["lanes"])


@pytest.mark.parametrize("mutation,expected", [
    ("attempt_omitted", "ATTEMPT_EVIDENCE_ROSTER_OMISSION"),
    ("attempt_extra", "ATTEMPT_EVIDENCE_ROSTER_EXTRA"),
    ("attempt_identity", "ATTEMPT_0000_EVIDENCE_IDENTITY_MISMATCH"),
    ("fold_omitted", "ATTEMPT_0000_FOLD_ROSTER_INCOMPLETE"),
    ("fold_order", "FOLD_000_RAW_SCHEMA_INVALID"),
])
def test_current_raw_keeps_exact_frozen_attempt_and_fold_roster(current_native_case, tmp_path, mutation, expected):
    case = clone_current_native_case(current_native_case, tmp_path)
    evidence = case["evidence"]
    if mutation == "attempt_omitted": evidence.clear()
    elif mutation == "attempt_extra": evidence.append(copy.deepcopy(evidence[-1]))
    elif mutation == "attempt_identity": evidence[0]["attempt_id"] = "substituted"
    elif mutation == "fold_omitted": evidence[0]["folds"].pop()
    else: evidence[0]["folds"][0], evidence[0]["folds"][1] = evidence[0]["folds"][1], evidence[0]["folds"][0]
    with pytest.raises(raw.WinnerRawEvidenceError, match=expected):
        _produce_case(case)


@pytest.mark.parametrize("field", ["attempt_artifacts_bundle_bytes", "horizon_execution_contracts_bytes", "partition_cost_precommitments_bytes"])
def test_current_postfreeze_transport_tamper_fails_before_evidence(current_native_case, tmp_path, field):
    case = clone_current_native_case(current_native_case, tmp_path)
    value = json.loads(getattr(case["frozen"], field))
    if field == "attempt_artifacts_bundle_bytes": value[0]["trial_definition"]["parameters"] = {"forged": True}
    elif field == "horizon_execution_contracts_bytes": value[0]["source_bar_count"] += 1
    else:
        row = value["rows"][0]
        record = value["records"][row["partition_geometry_receipt_sha256"]] if (
            value["schema_version"] == raw.PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA) else row
        original_sha = record["source_observation_receipt_sha256"]
        record["source_observation_receipt_sha256"] = "0" * 64 if original_sha != "0" * 64 else "1" * 64
        assert record["source_observation_receipt_sha256"] != original_sha
        value = _reseal(value)
    changed = replace(case["frozen"], **{field: _canonical(value)})
    def forbidden():
        pytest.fail("post-freeze transport tamper consumed evidence")
        yield None
    with pytest.raises(raw.WinnerRawEvidenceError):
        _produce_case(case, evidence=forbidden(), frozen=changed)


def test_current_frozen_receipt_reseal_conflicts_with_original_sink(current_native_case, tmp_path):
    case = clone_current_native_case(current_native_case, tmp_path)
    precommitment = case["frozen"].precommitment
    precommitment["precommitted_at_utc"] = "2024-01-01T00:00:00Z"
    changed = replace(case["frozen"], precommitment_bytes=_canonical(precommitment))
    with pytest.raises(raw.WinnerRawEvidenceError, match="FROZEN_PARTITION_COST_BINDING_INVALID"):
        _produce_case(case, frozen=changed)


@pytest.mark.parametrize("mutation,expected", [
    ("rows_omitted", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("rows_reordered", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("pair_splice", "PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID"),
])
def test_current_partition_precommitment_has_exact_source_roster(current_native_case, mutation, expected):
    frozen = current_native_case["frozen"]
    body = frozen.partition_cost_precommitments
    if mutation == "rows_omitted": body["rows"].pop()
    elif mutation == "rows_reordered": body["rows"][0], body["rows"][1] = body["rows"][1], body["rows"][0]
    else: body["frozen_tier_pair_contract_sha256"] = "0" * 64
    body = _reseal(body)
    with pytest.raises(raw.WinnerRawEvidenceError, match=expected):
        raw._validated_partition_cost_precommitments(body, pair=frozen.frozen_tier_pair_contract,
            attempts=frozen.attempt_ledger["attempts"], folds=frozen.precommitment["causal_fold_plan"])


def _partition_transport_case(**kwargs):
    from tests.test_authoritative_search_execution import _partition_cost_transport_fixture
    return _partition_cost_transport_fixture(**kwargs)


def _validate_partition_transport(body, case):
    return raw._validated_partition_cost_precommitments(body, pair=case["pair"],
        attempts=case["attempts"], folds=case["folds"])


def test_partition_cost_v1_transport_preserves_original_bytes_and_hash():
    """An original v1 body is validated and expanded without normalization to v2."""
    case = _partition_transport_case(attempt_count=1)
    original = _reseal({"schema_version": raw.PARTITION_COST_PRECOMMITMENTS_SCHEMA,
        "frozen_tier_pair_contract_sha256": _sha(case["pair"]), "rows": copy.deepcopy(case["rows"])})
    original_bytes = _canonical(original)
    original_sha = hashlib.sha256(original_bytes).hexdigest()
    restored = _validate_partition_transport(json.loads(original_bytes), case)
    assert _canonical(restored) == original_bytes
    assert _sha(restored) == original_sha
    assert restored["schema_version"] == "fxpm.winner-partition-cost-precommitments.v1"
    assert "records" not in restored
    for row in restored["rows"]:
        assert raw.expand_partition_cost_precommitment(restored, row, pair=case["pair"]) == row


def test_compact_partition_cost_transport_roundtrips_exact_semantic_rows():
    case = _partition_transport_case()
    original_bytes = _canonical(case["rows"])
    compact = raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    encoded = _canonical(compact)
    restored = _validate_partition_transport(json.loads(encoded), case)
    assert restored["schema_version"] == raw.PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA
    assert _canonical(restored) == encoded
    assert _sha(restored) == hashlib.sha256(encoded).hexdigest()
    assert len(restored["rows"]) == 2 * 4 * 3
    assert len(restored["records"]) == 4 * 3
    for stored, expected in zip(restored["rows"], case["rows"], strict=True):
        assert set(stored) == {"attempt_ordinal", "fold_ordinal", "partition", "partition_geometry_receipt_sha256"}
        expanded = raw.expand_partition_cost_precommitment(restored, stored, pair=case["pair"])
        assert expanded == expected
        record = restored["records"][stored["partition_geometry_receipt_sha256"]]
        assert set(record) == {"partition_frame_sha256", "source_observation_receipt_sha256", "tiers"}
        for tier in ("P75", "P95"):
            full = expected["tiers"][tier]["numerical_execution_cost_attestation"]
            compact_tier = record["tiers"][tier]
            assert compact_tier["numerical_execution_cost_attestation_sha256"] == _sha(full)
            assert compact_tier["numerical_execution_cost_attestation_sha256"] != full["attestation_sha256"]
            assert expanded["tiers"][tier]["numerical_execution_cost_attestation"] == full
    assert _canonical(case["rows"]) == original_bytes


@pytest.mark.parametrize("mutation,expected", [
    ("row_missing", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("row_order", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("row_identity", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("geometry_hash", "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID"),
    ("record_missing", "PARTITION_COST_PRECOMMITMENTS_RECORD_UNAVAILABLE"),
    ("record_orphan", "PARTITION_COST_PRECOMMITMENTS_RECORD_ROSTER_INVALID"),
    ("record_schema", "PARTITION_COST_PRECOMMITMENTS_RECORD_INVALID"),
    ("tier_missing", "PARTITION_COST_PRECOMMITMENTS_TIER_INVALID"),
    ("tier_schema", "PARTITION_COST_PRECOMMITMENTS_TIER_INVALID"),
    ("frame_hash", "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID"),
    ("source_hash", "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID"),
    ("entry_array", "PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID"),
    ("intrabar_array", "PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID"),
    ("attestation_hash", "PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID"),
    ("observations_hash", "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID"),
    ("admission_hash", "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID"),
    ("pair_hash", "PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID"),
    ("receipt_hash", "PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID"),
])
def test_compact_partition_cost_transport_rejects_resealed_tamper(mutation, expected):
    case = _partition_transport_case()
    body = raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    geometry = body["rows"][0]["partition_geometry_receipt_sha256"]
    record = body["records"][geometry]
    tier = record["tiers"]["P75"]
    if mutation == "row_missing": body["rows"].pop()
    elif mutation == "row_order": body["rows"][0], body["rows"][1] = body["rows"][1], body["rows"][0]
    elif mutation == "row_identity": body["rows"][0]["attempt_ordinal"] += 1
    elif mutation == "geometry_hash": body["rows"][0]["partition_geometry_receipt_sha256"] = "invalid"
    elif mutation == "record_missing": body["records"].pop(geometry)
    elif mutation == "record_orphan": body["records"][_sha("ORPHAN_STRUCTURAL_GEOMETRY")] = copy.deepcopy(record)
    elif mutation == "record_schema": record.pop("partition_frame_sha256")
    elif mutation == "tier_missing": record["tiers"].pop("P95")
    elif mutation == "tier_schema": tier["unexpected"] = "0" * 64
    elif mutation == "frame_hash": record["partition_frame_sha256"] = "invalid"
    elif mutation == "source_hash": record["source_observation_receipt_sha256"] = "invalid"
    elif mutation == "entry_array": tier["entry_half_spread_values_sha256"] = _sha("ALTERED_ENTRY_ARRAY")
    elif mutation == "intrabar_array": tier["intrabar_half_spread_values_sha256"] = _sha("ALTERED_INTRABAR_ARRAY")
    elif mutation == "attestation_hash":
        tier["numerical_execution_cost_attestation_sha256"] = case["rows"][0]["tiers"]["P75"][
            "numerical_execution_cost_attestation"]["attestation_sha256"]
    elif mutation == "observations_hash": tier["entry_observations_sha256"] = "invalid"
    elif mutation == "admission_hash": tier["entry_admission_mask_sha256"] = "invalid"
    elif mutation == "pair_hash": body["frozen_tier_pair_contract_sha256"] = "0" * 64
    body = _reseal(body)
    if mutation == "receipt_hash": body["receipt_sha256"] = "0" * 64
    with pytest.raises(raw.WinnerRawEvidenceError, match="^" + expected + "$"):
        _validate_partition_transport(body, case)


@pytest.mark.parametrize("changed", ["frame", "source", "attestation", "observations", "admission"])
def test_compact_partition_cost_transport_rejects_conflicting_geometry(changed):
    case = _partition_transport_case()
    first, duplicate = case["rows"][0], case["rows"][4 * 3]
    assert first["partition_geometry_receipt_sha256"] == duplicate["partition_geometry_receipt_sha256"]
    original_record = copy.deepcopy({key: duplicate[key] for key in (
        "partition_frame_sha256", "source_observation_receipt_sha256", "tiers")})
    if changed in {"frame", "source"}:
        field = "partition_frame_sha256" if changed == "frame" else "source_observation_receipt_sha256"
        duplicate[field] = _sha(["ALTERED_REPEATED_GEOMETRY", field])
    elif changed == "attestation":
        duplicate["tiers"]["P75"]["numerical_execution_cost_attestation"] = copy.deepcopy(
            duplicate["tiers"]["P95"]["numerical_execution_cost_attestation"])
    else:
        field = "entry_observations_sha256" if changed == "observations" else "entry_admission_mask_sha256"
        duplicate["tiers"]["P75"][field] = _sha(["ALTERED_REPEATED_GEOMETRY", field])
    assert {key: duplicate[key] for key in original_record} != original_record
    with pytest.raises(raw.WinnerRawEvidenceError, match="^PARTITION_COST_PRECOMMITMENTS_RECORD_CONFLICT$"):
        raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])


def test_compact_partition_cost_transport_rejects_rebound_pair_policy():
    case = _partition_transport_case()
    body = raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    changed_pair = copy.deepcopy(case["pair"])
    changed_pair["tiers"][0]["numerical_execution_cost_policy"]["instrument_spec_sha256"] = _sha("ALTERED_POLICY_SPEC")
    body["frozen_tier_pair_contract_sha256"] = _sha(changed_pair)
    body = _reseal(body)
    with pytest.raises(raw.WinnerRawEvidenceError) as exc:
        raw._validated_partition_cost_precommitments(body, pair=changed_pair,
            attempts=case["attempts"], folds=case["folds"])
    assert exc.value.code == "PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID_POLICY_DRIFT"


def test_compact_partition_cost_transport_binds_actual_source_receipt_before_reopen():
    """Header-only unit: a valid reseal cannot replace the consumed source receipt hash."""
    case = _partition_transport_case(attempt_count=1, fold_count=1)
    semantic = case["rows"][0]
    receipt = _reseal({"schema_version": raw.SOURCE_OBSERVATIONS_SCHEMA, "symbol": "EURUSD", "timeframe": "D1",
        "source_timeframe": "D1", "source_product": {}, "observations": [],
        "entry_observations_sha256": semantic["tiers"]["P75"]["entry_observations_sha256"]})
    semantic["source_observation_receipt_sha256"] = receipt["receipt_sha256"]
    body = raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    geometry = body["rows"][0]["partition_geometry_receipt_sha256"]
    body["records"][geometry]["source_observation_receipt_sha256"] = _sha("ALTERED_SOURCE_RECEIPT")
    body = _validate_partition_transport(_reseal(body), case)
    expanded = raw.expand_partition_cost_precommitment(body, body["rows"][0], pair=case["pair"])
    assert expanded["source_observation_receipt_sha256"] != receipt["receipt_sha256"]
    class Forbidden:
        def __getattribute__(self, name):
            pytest.fail("source-hash mismatch reached native reopening or account evidence")
    with pytest.raises(raw.WinnerRawEvidenceError, match="^SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID$"):
        raw._validated_source_observation_receipt(receipt, sink=Forbidden(),
            frozen=SimpleNamespace(symbol="EURUSD", timeframe="D1"),
            expected_sha256=expanded["source_observation_receipt_sha256"], partition="optimizer_train",
            signals=Forbidden(), lanes=Forbidden())


def test_compact_partition_cost_transport_fits_canonical300_four_fold_shape():
    """Measured structural encoding only: 300x4x3 distinct records, no actual fits or market claim."""
    case = _partition_transport_case(attempt_count=300, fold_count=4, unique_geometry=True)
    body = raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    encoded = _canonical(body)
    assert winner.MAX_REQUEST_BYTES == 16 * 1024 * 1024
    assert len(encoded) <= winner.MAX_REQUEST_BYTES
    restored = _validate_partition_transport(json.loads(encoded), case)
    assert _canonical(restored) == encoded
    assert len(restored["rows"]) == len(restored["records"]) == 300 * 4 * 3
    assert sum(len(record["tiers"]) for record in restored["records"].values()) == 300 * 4 * 3 * 2
    original_hash, expanded_hash = hashlib.sha256(), hashlib.sha256()
    for stored, expected in zip(restored["rows"], case["rows"], strict=True):
        expanded = raw.expand_partition_cost_precommitment(restored, stored, pair=case["pair"])
        assert expanded == expected
        original_hash.update(_canonical(expected) + b"\n")
        expanded_hash.update(_canonical(expanded) + b"\n")
    assert expanded_hash.digest() == original_hash.digest()


def test_current_packed_r_transport_refuses_non_grid_values():
    quantum = Decimal(1).scaleb(-RISK_CONTRACT["r_decimal_places"])
    with pytest.raises(raw.WinnerRawEvidenceError, match="TIER_R_NOT_EXACT_E12"):
        raw._packed_r_e12_vector([float(Decimal(1) + quantum / 2)], maximum_abs_r=winner.MAX_ABS_DAILY_R, code="TIER_R_NOT_EXACT_E12")


def test_current_completed_trade_transport_keeps_own_axes_and_limits():
    start, end = datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 2, 1, tzinfo=timezone.utc)
    assert raw._tier_event_vectors([], fold_start=start, fold_end_exclusive=end) == ([], [], [], [], [], [])
    with pytest.raises(raw.WinnerRawEvidenceError, match="TIER_COMPLETED_POSITION_ROSTER_INVALID"):
        raw._tier_event_vectors([{}] * (winner.MAX_TRADES_PER_FOLD + 1), fold_start=start, fold_end_exclusive=end)


def test_public_archive_schema_aliases_preserve_original_meanings():
    assert raw.FROZEN_PLAN_SCHEMA == "fxpm.winner-search-plan-frozen.v3"
    assert raw.PRODUCTION_SCHEMA == "fxpm.winner-raw-evidence-production.v4"
    assert raw.RAW_LANE_SCHEMA == "fxpm.winner-raw-backtester-lane.v4"
    assert raw.AUTHORITATIVE_PROVENANCE_SCHEMA == "fxpm.authoritative-search-provenance.v3"
    assert raw.CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA == "fxpm.authoritative-search-provenance.v4"
    assert raw.REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA == "fxpm.authoritative-search-provenance.v5"
    assert raw.CURRENT_FROZEN_PLAN_SCHEMA == "fxpm.winner-search-plan-frozen.v4"
    assert raw.CURRENT_PRODUCTION_SCHEMA == "fxpm.winner-raw-evidence-production.v5"


@pytest.fixture(scope="module")
def original_v14_archive(tmp_path_factory):
    from tests.test_forward_oos_replay_producer import load_retained_raw_transport
    context, anchor, packet, artifact = load_retained_raw_transport(
        tmp_path_factory.mktemp("original-v14-archive"), version="v1")
    owner = packet["owner_source"]
    symbol = owner["symbol_receipt"]["symbol"]
    _, entries = forward._verified_manifest(context, owner["symbol_receipt"], symbol)
    prefix = f"historical-anchor/historical-run-001/{anchor['anchor_id']}/winner-search/D1/TREND"
    def read(suffix):
        entry = entries[prefix + suffix]
        _, data = forward.native_prefix._read_spool_descriptor(context, entry["descriptor"],
            expected_format=entry["descriptor"]["format"], collect=True)
        return json.loads(data), entry
    receipt, receipt_entry = read("/frozen/receipt")
    assert receipt["schema_version"] == raw.FROZEN_PLAN_SCHEMA
    names = {"precommitment": "precommitment_bytes", "attempt_ledger": "attempt_ledger_bytes",
        "source_contract_seed": "source_contract_seed_bytes", "cost_ladder_contract": "cost_ladder_contract_bytes",
        "risk_unit_contract": "risk_unit_contract_bytes", "cost_ladder_adversity_plan": "cost_ladder_adversity_plan_bytes",
        "horizon_execution_contracts": "horizon_execution_contracts_bytes"}
    fields = {}
    for name, field in names.items():
        body, entry = read("/frozen/" + name)
        assert entry["descriptor"] == receipt["artifact_roster"][name]
        fields[field] = _canonical(body)
    frozen = raw.FrozenWinnerSearchPlan(symbol=receipt["symbol"], timeframe=receipt["timeframe"],
        regime=receipt["regime"], artifact_namespace=receipt["artifact_namespace"],
        generation_context_sha256=context.context_sha256, frozen_receipt_bytes=_canonical(receipt),
        frozen_receipt_entry_bytes=_canonical(receipt_entry), **fields)
    request_body, request_entry = read("/authority/winner-request")
    decision_body, decision_entry = read("/authority/winner-decision")
    production_receipt, _ = read("/authority/production-receipt")
    assert production_receipt["artifact_roster"]["winner-request"] == request_entry["descriptor"]
    lanes = {name: read("/attempt-0000/fold-000/raw/" + name)[0] for name in (
        "optimizer_train_base", "tuning_validation_base", "final_lockbox_base", "final_lockbox_p75", "final_lockbox_p95")}
    process = read("/frozen/attempt-0000/winner_process")[0]
    replay = read("/attempt-0000/fold-000/provenance/attempt_replay_receipt")[0]
    return dict(context=context, frozen=frozen, lanes=lanes, process=process, replay=replay,
        request_body=request_body, request_entry=request_entry, decision_body=decision_body,
        decision_entry=decision_entry, production_receipt=production_receipt,
        packet=packet, artifact=artifact, anchor=anchor)


def test_original_v14_request_and_decision_reopen_without_old_writer(original_v14_archive):
    case = original_v14_archive
    # The request binding hashes its three payload hashes; it is not the
    # serialized wrapper artifact's content hash.
    request = forward._winner_request(case["request_body"], case["production_receipt"]["request_sha256"])
    decision = forward._winner_decision(case["decision_body"], request, case["decision_body"]["receipt_sha256"])
    assert request.to_dict()["contract_version"] == winner.ARCHIVED_GENERATION.request_version
    assert decision.to_dict()["contract_version"] == winner.ARCHIVED_GENERATION.selection_version
    assert case["frozen"].precommitment["selection_policy_sha256"] == winner.ARCHIVED_GENERATION.policy_sha256
    assert case["frozen"].frozen_tier_pair_contract_bytes == b""


def test_original_v14_method_receipt_reopens_original_five_accounts(original_v14_archive):
    case = original_v14_archive
    receipt = case["replay"]["cost_ladder_method_receipt"]
    assert raw._validated_cost_ladder_method_receipt(receipt, frozen=case["frozen"],
        attempt_artifacts={"winner_process": case["process"]}, lanes=case["lanes"]) == receipt


@pytest.mark.parametrize("mutation,expected", [
    ("policy", "WINNER_REQUEST_RAW_REPLAY_FAILED"),
    ("receipt", "COST_LADDER_METHOD_RECEIPT_BINDING_INVALID"),
    ("reprice", "COST_LADDER_METHOD_LEVEL_BINDING_INVALID"),
    ("trade_projection", "COST_LADDER_METHOD_BASE_TRADE_POSITION_RECONCILIATION_INVALID"),
])
def test_original_archive_resealed_tamper_does_not_become_new_authority(original_v14_archive, mutation, expected):
    case = original_v14_archive
    if mutation == "policy":
        request = copy.deepcopy(case["request_body"])
        wrapper = request["precommitment"]
        wrapper["payload"]["selection_policy_sha256"] = winner.CURRENT_GENERATION.policy_sha256
        wrapper["payload_sha256"] = _sha(wrapper["payload"])
        with pytest.raises(forward.ForwardOOSReplayProductionError, match=expected):
            forward._winner_request(request, _sha(request))
        return
    receipt = copy.deepcopy(case["replay"]["cost_ladder_method_receipt"])
    lanes = copy.deepcopy(case["lanes"])
    if mutation == "receipt":
        receipt["base_entry_half_spread_values_sha256"] = "0" * 64
    elif mutation == "reprice":
        receipt["levels"][0]["entry_half_spread_values_sha256"] = "0" * 64
        level = receipt["levels"][0]
        level["level_receipt_sha256"] = _sha({key: value for key, value in level.items() if key != "level_receipt_sha256"})
    else:
        assert lanes["final_lockbox_base"]["trade_identity_pnl"]
        lanes["final_lockbox_base"]["trade_identity_pnl"][0]["pnl_dollars"] += 1
    receipt = _reseal(receipt)
    with pytest.raises(raw.WinnerRawEvidenceError, match=expected):
        raw._validated_cost_ladder_method_receipt(receipt, frozen=case["frozen"],
            attempt_artifacts={"winner_process": case["process"]}, lanes=lanes)


class _OneTradeStrategy:
    name = "real-backtester-path"

    def calculate_stops(self, *_args, **_kwargs):
        return 10.0, 10.0

def test_retired_zero_spread_multiplier_replay_refuses_execution() -> None:
    """Replaces the retired zero-base-spread/full-multiplier-replay success case."""
    backtester = Backtester(PipelineConfig(use_spread=True, swap_model_enabled=False))
    with pytest.raises(
        ValueError,
        match="^RETIRED_MULTIPLIER_COST_POLICY_INTERVAL_QUALIFICATION_REQUIRED$",
    ):
        backtester.replay_frozen_path_cost_ladder(
            None, None, "EURUSD", None, None, timeframe="H1",
        )

def test_real_backtester_position_events_feed_daily_surface() -> None:
    config = PipelineConfig(
        use_spread=False,
        swap_model_enabled=False,
        backtest_spread_mode="historical_bar",
    )
    backtester = Backtester(config)
    index = pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h")
    features = pd.DataFrame(
        {
            "Open": [100.0, 100.0, 100.0, 100.0],
            "High": [100.0, 100.2, 100.0, 100.0],
            "Low": [100.0, 99.95, 100.0, 100.0],
            "Close": [100.0, 100.1, 100.0, 100.0],
            "Volume": [100, 100, 100, 100],
            "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
        },
        index=index,
    )
    signals = pd.Series([1, 0, 0, 0], index=index)
    result = backtester.run(
        features,
        signals,
        "EURUSD",
        _OneTradeStrategy(),
        spec=InstrumentSpec(
            symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0
        ),
        timeframe="H1",
    )

    assert result["position_events"]
    surface = build_flat_daily_r_surface(
        result["position_events"],
        start_date_utc="2024-01-01",
        end_date_utc="2024-01-01",
        risk_unit_contract=RISK_CONTRACT,
    )
    assert surface["origin"] == "backtester_search_time_flat_daily_positions"
    assert sum(surface["daily_net_mtm_r"]) == pytest.approx(
        sum(
            event["pnl_dollars"] / event["risk_amount"]
            for event in result["position_events"]
        )
    )
