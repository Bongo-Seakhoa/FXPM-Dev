"""INVENTED structural commitments and callback stand-ins; no market/model authority."""
import copy
import sys
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as execution
from test_authoritative_search_execution import (
    _attempt_boundary_invented_continuation,
    _partition_cost_transport_fixture,
)


def _case(*, attempts=2):
    fixture = _partition_cost_transport_fixture(attempt_count=attempts, fold_count=4)
    precommitments = execution.raw_winner.build_partition_cost_precommitments(
        fixture["rows"], pair=fixture["pair"])
    return fixture, precommitments


def _joined_arguments(fixture, precommitments):
    row = fixture["rows"][0]
    partition = SimpleNamespace(role=row["partition"], geometry_receipt={
        "receipt_sha256": row["partition_geometry_receipt_sha256"],
        "frame_sha256": row["partition_frame_sha256"]})
    expected = row["tiers"]["P75"]
    result = {"numerical_execution_cost_attestation": copy.deepcopy(
        expected["numerical_execution_cost_attestation"]),
        "spread_admission": {key: expected[key] for key in (
            "entry_observations_sha256", "entry_admission_mask_sha256")}}
    reference = {"source_observation_receipt_sha256": row["source_observation_receipt_sha256"]}
    common = dict(precommitments=precommitments, pair=fixture["pair"],
        attempt=SimpleNamespace(ordinal=0), fold_ordinal=0, partition=partition)
    return common, result, {partition.geometry_receipt["receipt_sha256"]: reference}


def _row_get_count(rows, call):
    row_ids, calls = {id(row) for row in rows}, []
    previous = sys.getprofile()

    def observe(_frame, event, function):
        if (event == "c_call" and getattr(function, "__name__", None) == "get"
                and id(getattr(function, "__self__", None)) in row_ids):
            calls.append(None)

    sys.setprofile(observe)
    try:
        result = call()
    finally:
        sys.setprofile(previous)
    return result, len(calls)


def test_private_lookup_avoids_repeated_row_scans_and_keeps_detached_identity():
    fixture, precommitments = _case()
    before = execution._canonical_bytes(precommitments)
    lookup = execution._index_partition_precommitment_rows(precommitments)
    assert len(lookup) == len(precommitments["rows"])

    def identities(index):
        return [execution._route_a_owner_partition_identity(precommitments=precommitments,
            attempt_ordinal=row["attempt_ordinal"], fold_ordinal=row["fold_ordinal"],
            partition_role=row["partition"], _partition_row_lookup=index)
            for row in precommitments["rows"]]

    ordinary, scanned = _row_get_count(precommitments["rows"], lambda: identities(None))
    indexed, reused = _row_get_count(precommitments["rows"], lambda: identities(lookup))
    assert scanned > 0 and reused == 0
    assert indexed == ordinary
    indexed[0]["precommitment_row"]["partition"] = "INVENTED_CHANGED_PUBLIC_PROJECTION"
    assert identities(lookup) == ordinary
    assert execution._canonical_bytes(precommitments) == before

    common, result, references = _joined_arguments(fixture, precommitments)
    execution._verify_partition_precommitment(**common, tier="P75", result=result,
        _partition_row_lookup=lookup)
    reference = next(iter(references.values()))
    assert execution._partition_source_observation_reference(**common, references=references,
        _partition_row_lookup=lookup) is reference
    result["spread_admission"]["entry_observations_sha256"] = "0" * 64
    reference["source_observation_receipt_sha256"] = "0" * 64
    for index in (None, lookup):
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as cost:
            execution._verify_partition_precommitment(**common, tier="P75", result=result,
                _partition_row_lookup=index)
        assert cost.value.code == "PARTITION_COST_PRECOMMIT_RESULT_MISMATCH"
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as source:
            execution._partition_source_observation_reference(**common, references=references,
                _partition_row_lookup=index)
        assert source.value.code == "SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH"


@pytest.mark.parametrize("damage", ("missing", "duplicate", "other_attempt"))
def test_indexed_joins_keep_missing_duplicate_and_crossed_row_refusals(damage):
    fixture, precommitments = _case()
    if damage == "missing":
        precommitments["rows"].pop(0)
    elif damage == "duplicate":
        precommitments["rows"].append(copy.deepcopy(precommitments["rows"][0]))
    else:
        precommitments["rows"][0]["attempt_ordinal"] = 77
    lookup = execution._index_partition_precommitment_rows(precommitments)
    assert len(lookup.get((0, 0, "optimizer_train"), ())) == (2 if damage == "duplicate" else 0)
    common, result, references = _joined_arguments(fixture, precommitments)
    checks = (
        ("ROUTE_A_OWNER_SNAPSHOT_PRECOMMITMENT_INVALID", lambda index:
            execution._route_a_owner_partition_identity(precommitments=precommitments,
                attempt_ordinal=0, fold_ordinal=0, partition_role="optimizer_train",
                _partition_row_lookup=index)),
        ("PARTITION_COST_PRECOMMIT_ROW_UNAVAILABLE", lambda index:
            execution._verify_partition_precommitment(**common, tier="P75", result=result,
                _partition_row_lookup=index)),
        ("SOURCE_OBSERVATION_REFERENCE_GEOMETRY_MISMATCH", lambda index:
            execution._partition_source_observation_reference(**common, references=references,
                _partition_row_lookup=index)),
    )
    for code, check in checks:
        for index in (None, lookup):
            with pytest.raises(execution.AuthoritativeSearchExecutionError) as error:
                check(index)
            assert error.value.code == code


def test_real_continuation_builds_one_private_lookup_and_clears_it_on_close(tmp_path, monkeypatch):
    # The coordinator executes; authentication, geometry and kernel remain labelled stand-ins.
    _, structural = _case(attempts=1)
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch)
    indexer = execution._index_partition_precommitment_rows
    builds, lookups, initial_sizes, observed = [], [], [], {}

    def indexed(precommitments):
        assert precommitments == {}
        precommitments.update(copy.deepcopy(structural))
        builds.append(precommitments)
        value = indexer(precommitments)
        lookups.append(value)
        initial_sizes.append(len(value))
        return value

    def watch(name):
        original = getattr(execution, name)
        observed[name] = []

        def called(**kwargs):
            observed[name].append(kwargs["_partition_row_lookup"])
            return original(**kwargs)

        monkeypatch.setattr(execution, name, called)

    monkeypatch.setattr(execution, "_index_partition_precommitment_rows", indexed)
    for name in ("_route_a_owner_membership_snapshot", "_route_a_rebind_full_panel_owner_membership",
            "_verify_partition_precommitment", "_route_a_assemble_full_panel_attempt"):
        watch(name)
    result = case.run()
    assert type(result) is execution._PreparedRouteAFullPanelStage and len(case.staged) == 1
    assert len(builds) == len(lookups) == 1 and initial_sizes == [12]
    assert {name: len(rows) for name, rows in observed.items()} == {
        "_route_a_owner_membership_snapshot": 1,
        "_route_a_rebind_full_panel_owner_membership": 48,
        "_verify_partition_precommitment": 48,
        "_route_a_assemble_full_panel_attempt": 1,
    }
    assert all(value is lookups[0] for rows in observed.values() for value in rows)
    assert lookups[0] == {}
    assert sum(row[0] == "probe" for row in case.events) == 24
    assert sum(row[0] == "run" for row in case.events) == 24
    assert case.reads == {"config": 2, "stream": 4}
