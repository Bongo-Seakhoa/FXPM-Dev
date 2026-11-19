"""Invented engineering inputs; no market evidence or trading qualification.

The structural tests perform no native acquisition, regime fit or account run.
Tests with ``native`` in their name reuse the existing synthetic session carrier;
they require Root's separately scheduled native cohort.
"""
from __future__ import annotations

import copy
import json
from dataclasses import replace

import pytest

import pm_winner_raw_evidence as raw
from tests.test_authoritative_search_execution import _partition_cost_transport_fixture


def _canonical(value):
    return json.dumps(value, allow_nan=False, ensure_ascii=True,
                      separators=(",", ":"), sort_keys=True).encode("ascii")


def _reseal(value):
    body = {key: child for key, child in value.items() if key != "receipt_sha256"}
    return {**body, "receipt_sha256": raw._sha_json(body)}


def _structural_case(*, attempts=2, folds=2, unique_geometry=False, version=2):
    case = _partition_cost_transport_fixture(attempt_count=attempts, fold_count=folds,
                                              unique_geometry=unique_geometry)
    if version == 1:
        table = _reseal({"schema_version": raw.PARTITION_COST_PRECOMMITMENTS_SCHEMA,
            "frozen_tier_pair_contract_sha256": raw._sha_json(case["pair"]),
            "rows": copy.deepcopy(case["rows"])})
    else:
        table = raw.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    frozen = raw.FrozenWinnerSearchPlan(
        symbol="EURUSD", timeframe="D1", regime="TREND", artifact_namespace="synthetic-local-index",
        generation_context_sha256="a" * 64,
        precommitment_bytes=_canonical({"causal_fold_plan": case["folds"]}),
        attempt_ledger_bytes=_canonical({"attempts": case["attempts"]}),
        source_contract_seed_bytes=b"{}", cost_ladder_contract_bytes=b"{}",
        risk_unit_contract_bytes=b"{}", cost_ladder_adversity_plan_bytes=b"{}",
        frozen_receipt_bytes=b"{}", frozen_receipt_entry_bytes=b"{}",
        horizon_execution_contracts_bytes=b"[]",
        frozen_tier_pair_contract_bytes=_canonical(case["pair"]),
        partition_cost_precommitments_bytes=_canonical(table))
    return case, table, frozen


def _validate(case, table):
    return raw._validated_partition_cost_precommitments(table, pair=case["pair"],
        attempts=case["attempts"], folds=case["folds"])


def _old_filter(validated, pair, attempt, fold):
    return [raw.expand_partition_cost_precommitment(validated, row, pair=pair)
        for row in validated["rows"] if row["attempt_ordinal"] == attempt["ordinal"]
        and row["fold_ordinal"] == fold["fold_ordinal"]]


def test_structural_300_four_fold_index_matches_every_unique_geometry_in_order():
    case, table, frozen = _structural_case(attempts=300, folds=4, unique_geometry=True)
    validated = _validate(case, table)
    index = raw._index_partition_cost_precommitments(frozen, validated)
    assert len(validated["rows"]) == len(validated["records"]) == 3600
    assert len(index.rows_by_fold) == 1200
    for attempt in case["attempts"]:
        for fold in case["folds"]:
            selected = raw._selected_partition_cost_precommitments(frozen=frozen,
                attempt=attempt, planned_fold=fold, partition_cost_index=index)
            expected = _old_filter(validated, case["pair"], attempt, fold)
            assert _canonical(selected) == _canonical(expected)
            assert [row["partition"] for row in selected] == list(raw._PARTITIONS)
    assert _canonical(table) == frozen.partition_cost_precommitments_bytes


def test_structural_index_removes_repeated_whole_table_validation(monkeypatch):
    case, table, frozen = _structural_case()
    original = raw._validated_partition_cost_precommitments
    calls = []

    def record(*args, **kwargs):
        calls.append(None)
        return original(*args, **kwargs)

    monkeypatch.setattr(raw, "_validated_partition_cost_precommitments", record)
    validated = _validate(case, table)
    index = raw._index_partition_cost_precommitments(frozen, validated)
    indexed = [raw._selected_partition_cost_precommitments(frozen=frozen, attempt=attempt,
        planned_fold=fold, partition_cost_index=index)
        for attempt in case["attempts"] for fold in case["folds"]]
    assert len(calls) == 1
    calls.clear()
    _validate(case, table)
    old = [raw._selected_partition_cost_precommitments(frozen=frozen, attempt=attempt,
        planned_fold=fold) for attempt in case["attempts"] for fold in case["folds"]]
    assert len(calls) == 1 + len(case["attempts"]) * len(case["folds"])
    assert _canonical(indexed) == _canonical(old)


def test_structural_none_fallback_revalidates_and_refuses_changed_table(monkeypatch):
    case, table, frozen = _structural_case()
    original = raw._validated_partition_cost_precommitments
    seen = []

    def record(*args, **kwargs):
        seen.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(raw, "_validated_partition_cost_precommitments", record)
    selected = raw._selected_partition_cost_precommitments(frozen=frozen,
        attempt=case["attempts"][0], planned_fold=case["folds"][0], partition_cost_index=None)
    assert len(seen) == 1 and seen[0] == table
    assert _canonical(selected) == _canonical(case["rows"][:3])
    changed = copy.deepcopy(table)
    changed["rows"].pop()
    changed = _reseal(changed)
    broken = replace(frozen, partition_cost_precommitments_bytes=_canonical(changed))
    with pytest.raises(raw.WinnerRawEvidenceError, match="PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"):
        raw._selected_partition_cost_precommitments(frozen=broken,
            attempt=case["attempts"][0], planned_fold=case["folds"][0])
    assert len(seen) == 2


@pytest.mark.parametrize("change", ["other_plan", "changed_bytes", "foreign_cache"])
def test_structural_index_refuses_wrong_frozen_binding(change):
    case, table, frozen = _structural_case()
    index = raw._index_partition_cost_precommitments(frozen, _validate(case, table))
    supplied = frozen
    if change == "other_plan":
        supplied = replace(frozen, generation_context_sha256="b" * 64)
    elif change == "changed_bytes":
        object.__setattr__(frozen, "partition_cost_precommitments_bytes", frozen.partition_cost_precommitments_bytes + b" ")
    else:
        index = object()
    with pytest.raises(raw.WinnerRawEvidenceError, match="PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID"):
        raw._selected_partition_cost_precommitments(frozen=supplied,
            attempt=case["attempts"][0], planned_fold=case["folds"][0], partition_cost_index=index)
    for getter in (raw._owned_precommitment, raw._owned_pair_contract, raw._owned_generation,
            raw._owned_precommitment_sha, raw._owned_partition_cost_receipt_sha):
        with pytest.raises(raw.WinnerRawEvidenceError, match="PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID"):
            getter(supplied, index)


def test_structural_index_retains_duplicate_rows_and_missing_key_semantics():
    case, table, frozen = _structural_case(attempts=1, folds=1)
    case["attempts"] += copy.deepcopy(case["attempts"])
    table["rows"] += copy.deepcopy(table["rows"])
    table = _reseal(table)
    frozen = replace(frozen, attempt_ledger_bytes=_canonical({"attempts": case["attempts"]}),
        partition_cost_precommitments_bytes=_canonical(table))
    validated = _validate(case, table)
    index = raw._index_partition_cost_precommitments(frozen, validated)
    selected = raw._selected_partition_cost_precommitments(frozen=frozen,
        attempt=case["attempts"][0], planned_fold=case["folds"][0], partition_cost_index=index)
    assert [row["partition"] for row in selected] == list(raw._PARTITIONS) * 2
    assert _canonical(selected) == _canonical(_old_filter(validated, case["pair"],
        case["attempts"][0], case["folds"][0]))
    assert raw._selected_partition_cost_precommitments(frozen=frozen,
        attempt={"ordinal": 99}, planned_fold={"fold_ordinal": 0}, partition_cost_index=index) == []


def test_structural_indices_are_per_call_and_preserve_v1_bytes():
    case, table, frozen = _structural_case(version=1)
    original = _canonical(table)
    one = raw._index_partition_cost_precommitments(frozen, _validate(case, table))
    two = raw._index_partition_cost_precommitments(frozen, _validate(case, table))
    assert one is not two and one.rows_by_fold is not two.rows_by_fold
    assert one.precommitments is not two.precommitments
    one.rows_by_fold[(0, 0)].append(copy.deepcopy(one.rows_by_fold[(0, 0)][0]))
    assert len(one.rows_by_fold[(0, 0)]) == 4 and len(two.rows_by_fold[(0, 0)]) == 3
    for attempt in case["attempts"]:
        for fold in case["folds"]:
            selected = raw._selected_partition_cost_precommitments(frozen=frozen,
                attempt=attempt, planned_fold=fold, partition_cost_index=two)
            assert _canonical(selected) == _canonical(_old_filter(table, case["pair"], attempt, fold))
    assert _canonical(table) == original == frozen.partition_cost_precommitments_bytes


@pytest.mark.parametrize("mutation,expected", [
    ("row_missing", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("record_missing", "PARTITION_COST_PRECOMMITMENTS_RECORD_UNAVAILABLE"),
    ("row_order", "PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"),
    ("record_tamper", "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID"),
])
def test_structural_malformed_table_still_refuses_initial_validation(mutation, expected):
    case, table, _frozen = _structural_case()
    broken = copy.deepcopy(table)
    if mutation == "row_missing":
        broken["rows"].pop()
    elif mutation == "record_missing":
        del broken["records"][next(iter(broken["records"]))]
    elif mutation == "row_order":
        broken["rows"][0], broken["rows"][1] = broken["rows"][1], broken["rows"][0]
    else:
        broken["records"][next(iter(broken["records"]))]["partition_frame_sha256"] = "not-a-hash"
    with pytest.raises(raw.WinnerRawEvidenceError, match=expected):
        _validate(case, _reseal(broken))


# Reuse the existing cached synthetic carrier only in Root's native cohort.
from tests.test_historical_production_adapter import current_native_case


def _native_case_copy(case, path):
    from tests.test_winner_raw_evidence import clone_current_native_case
    assert case["selected_owner_case"] is False and case["retained_native_root"] is None
    assert case["qualification_authority"] is False
    return clone_current_native_case(case, path)


def _produce_native(case, **kwargs):
    from tests.test_winner_raw_evidence import _produce_case
    return _produce_case(case, **kwargs)


def test_native_public_producer_one_validation_and_byte_identical_old_path(current_native_case, tmp_path, monkeypatch):
    case = _native_case_copy(current_native_case, tmp_path / "indexed")
    other = _native_case_copy(current_native_case, tmp_path / "old")
    original = raw._validated_partition_cost_precommitments
    calls = []

    def record(*args, **kwargs):
        calls.append(None)
        return original(*args, **kwargs)

    monkeypatch.setattr(raw, "_validated_partition_cost_precommitments", record)
    indexed = _produce_native(case)
    assert len(calls) == 1
    calls.clear()
    monkeypatch.setattr(raw, "_index_partition_cost_precommitments", lambda *_args, **_kwargs: None)
    old = _produce_native(other)
    folds = sum(len(attempt.get("folds", [])) for attempt in other["evidence"])
    assert len(calls) == 1 + folds
    assert _canonical(indexed.evidence_ledger) == _canonical(old.evidence_ledger)
    assert _canonical(indexed.request.to_dict()) == _canonical(old.request.to_dict())
    assert indexed.decision._body_json == old.decision._body_json
    assert _canonical(indexed.entries) == _canonical(old.entries)
    assert _canonical(case["sink"].artifact_roster()) == _canonical(other["sink"].artifact_roster())


@pytest.mark.parametrize("mutation", ["row_missing", "record_missing", "receipt_tamper"])
def test_native_postfreeze_tamper_refuses_before_consuming_evidence(current_native_case, mutation):
    case = current_native_case
    assert case["selected_owner_case"] is False and case["retained_native_root"] is None
    table = case["frozen"].partition_cost_precommitments
    if mutation == "row_missing":
        table["rows"].pop()
        table = _reseal(table)
    elif mutation == "record_missing":
        del table["records"][next(iter(table["records"]))]
        table = _reseal(table)
    else:
        table["receipt_sha256"] = "0" * 64
    frozen = replace(case["frozen"], partition_cost_precommitments_bytes=_canonical(table))
    consumed = []

    def forbidden_evidence():
        consumed.append(True)
        raise AssertionError("tampered frozen input consumed evidence")
        yield

    with pytest.raises(raw.WinnerRawEvidenceError, match="PARTITION_COST_PRECOMMITMENTS_"):
        _produce_native(case, frozen=frozen, evidence=forbidden_evidence())
    assert consumed == []


def test_native_producer_retains_partition_roster_refusal(current_native_case, tmp_path, monkeypatch):
    case = _native_case_copy(current_native_case, tmp_path / "duplicate")
    factory = raw._index_partition_cost_precommitments

    def duplicate(frozen, validated, **kwargs):
        index = factory(frozen, validated, **kwargs)
        key = next(iter(index.rows_by_fold))
        index.rows_by_fold[key].append(copy.deepcopy(index.rows_by_fold[key][0]))
        return index

    monkeypatch.setattr(raw, "_index_partition_cost_precommitments", duplicate)
    with pytest.raises(raw.WinnerRawEvidenceIncomplete, match="PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID"):
        _produce_native(case)


def _owned_projection_case():
    case, table, frozen = _structural_case()
    precommitment = {"contract_version": raw.winner.V17_GENERATION.precommitment_version,
        "causal_fold_plan": case["folds"], "cell_id": "SYNTHETIC-OWNED-VIEWS",
        "source_contract_sha256": "1" * 64, "cost_contract_sha256": "2" * 64,
        "risk_unit_contract_sha256": "3" * 64, "frozen_tier_pair_contract_sha256": "4" * 64}
    return case, table, replace(frozen, precommitment_bytes=_canonical(precommitment))


def _shared_source_fixture():
    return {"execution_spec_receipt": {"receipt_sha256": "5" * 64},
        **{name: "6" * 64 for name in ("native_prefix_index_sha256", "native_prefix_content_sha256",
            "source_contract_sha256", "native_source_sha256", "activation_manifest_sha256",
            "source_group_decision_sha256", "regime_fit_decision_sha256")}}


def test_owned_projections_decode_once_across_repeated_fold_and_witness_reads(monkeypatch):
    """Invented two-attempt owner; actual decoding/index expansion, no native or model run."""
    from collections import Counter
    case, table, frozen = _owned_projection_case()
    source = _shared_source_fixture()
    expected = {fold["fold_ordinal"]: raw._shared_oos_partition_receipt(
        frozen=frozen, planned_fold=fold, source_receipt=source) for fold in case["folds"]}
    original, decodes = raw._decode_object, Counter()
    def decode(encoded, code):
        decodes[code] += 1
        return original(encoded, code)
    monkeypatch.setattr(raw, "_decode_object", decode)
    precommitment, pair = frozen.precommitment, frozen.frozen_tier_pair_contract
    ledger, costs = frozen.attempt_ledger, frozen.partition_cost_precommitments
    index = raw._index_partition_cost_precommitments(frozen, _validate(case, costs),
        attempt_ledger=ledger, precommitment=precommitment, pair_contract=pair,
        generation=raw.winner.V17_GENERATION, precommitment_sha256=raw._sha_json(precommitment))
    for _ in range(3):
        for attempt in case["attempts"]:
            for fold in case["folds"]:
                assert raw._owned_precommitment(frozen, index) is precommitment
                assert raw._owned_pair_contract(frozen, index) is pair
                assert raw._owned_generation(frozen, index) is raw.winner.V17_GENERATION
                assert raw._owned_precommitment_sha(frozen, index) == raw._sha_json(precommitment)
                assert raw._owned_partition_cost_receipt_sha(frozen, index) == table["receipt_sha256"]
                assert table["receipt_sha256"] != raw._sha_json(table)
                assert raw._selected_partition_cost_precommitments(frozen=frozen, attempt=attempt,
                    planned_fold=fold, partition_cost_index=index) == _old_filter(table, pair, attempt, fold)
                assert raw._shared_oos_partition_receipt(frozen=frozen, planned_fold=fold,
                    source_receipt=source, partition_cost_index=index) == expected[fold["fold_ordinal"]]
    assert {name: decodes[name] for name in ("PRECOMMITMENT_BYTES_INVALID",
        "FROZEN_TIER_PAIR_CONTRACT_BYTES_INVALID", "ATTEMPT_LEDGER_BYTES_INVALID",
        "PARTITION_COST_PRECOMMITMENTS_BYTES_INVALID")} == {
            "PRECOMMITMENT_BYTES_INVALID": 1, "FROZEN_TIER_PAIR_CONTRACT_BYTES_INVALID": 1,
            "ATTEMPT_LEDGER_BYTES_INVALID": 1, "PARTITION_COST_PRECOMMITMENTS_BYTES_INVALID": 1}


def test_public_nested_projections_and_replace_stay_independent_of_owned_views():
    case, table, frozen = _owned_projection_case()
    precommitment, pair = frozen.precommitment, frozen.frozen_tier_pair_contract
    index = raw._index_partition_cost_precommitments(frozen, _validate(case, table),
        precommitment=precommitment, pair_contract=pair)
    projection, costs, pair_projection = frozen.precommitment, frozen.partition_cost_precommitments, frozen.frozen_tier_pair_contract
    assert type(projection) is dict and type(projection["causal_fold_plan"]) is list
    assert type(costs) is dict and type(costs["rows"]) is list
    projection["causal_fold_plan"][0]["fold_ordinal"] = 99
    costs["rows"].clear()
    pair_projection["tiers"][0]["tier"] = "SYNTHETIC_SPLICE"
    assert raw._owned_precommitment(frozen, index)["causal_fold_plan"][0]["fold_ordinal"] == 0
    assert index.precommitments["rows"] and frozen.partition_cost_precommitments["rows"]
    assert raw._owned_pair_contract(frozen, index)["tiers"][0]["tier"] == "P75"
    assert frozen.precommitment["causal_fold_plan"][0]["fold_ordinal"] == 0
    changed = replace(frozen, precommitment_bytes=_canonical(projection))
    assert raw._owned_precommitment(changed, None) == projection
    with pytest.raises(raw.WinnerRawEvidenceError, match="PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID"):
        raw._owned_precommitment(changed, index)
