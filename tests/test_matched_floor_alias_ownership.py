"""INVENTED transport authority; real tiny kernel facts, record builder and request ownership.

Geometry/cost/source admission are labelled stand-ins. This is not an issued
search, production selection or financial result.
"""
import copy
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as execution
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from test_authoritative_search_execution import _exact_floor_fractional_invented_fixture


def _record_inputs(monkeypatch):
    _, original, frame, _, _, _, _, _ = _exact_floor_fractional_invented_fixture(monkeypatch, False, 1)
    precommitment = {"contract_version": winner.V17_GENERATION.precommitment_version,
        "fixture_provenance": "INVENTED_TRANSPORT_AUTHORITY_ONLY",
        "causal_fold_plan": [{"ordinal": ordinal} for ordinal in range(4)]}
    attempt = {"ordinal": 0, "attempt_id": "INVENTED-RAW", "process_candidate_id": "INVENTED-CANDIDATE"}
    frozen = SimpleNamespace(precommitment=precommitment,
        partition_cost_precommitments={"receipt_sha256": "a" * 64},
        source_contract_seeds={"P75": {"fixture_provenance": "INVENTED_SOURCE_ADMISSION"}})
    witnesses, folds, axes, costs = [], [], [], []
    for fold_ordinal in range(4):
        geometries, lanes, cost_rows = [], {}, []
        for role in raw._PARTITIONS:
            facts = copy.deepcopy(original)
            facts["partition_role"] = role
            geometry = {"role": role, "frame_sha256": execution._frame_sha256(frame),
                "receipt_sha256": raw._sha_json(["INVENTED_GEOMETRY", fold_ordinal, role])}
            geometries.append(geometry)
            for tier in raw._TIERS:
                lanes[role + "_" + tier.lower()] = {
                    "trade_power_partition_axis": copy.deepcopy(facts["trade_power_partition_axis"]),
                    "position_events": copy.deepcopy(facts["base_position_events"]),
                    "swap_accrual_contract": copy.deepcopy(facts["exact_swap_accrual_contract"]),
                    "trade_identity_pnl": [copy.deepcopy(row["base_trade_identity"]) for row in facts["trades"]],
                    "numerical_execution_cost_attestation": copy.deepcopy(
                        facts["numerical_execution_cost_attestations"][tier])}
            cost_rows.append({**geometry, "partition_geometry_receipt_sha256": geometry["receipt_sha256"],
                "partition_frame_sha256": geometry["frame_sha256"],
                "tiers": {tier: {"numerical_execution_cost_attestation": copy.deepcopy(
                    facts["numerical_execution_cost_attestations"][tier])} for tier in raw._TIERS}})
            witnesses.append({"attempt_ordinal": 0, "raw_attempt_id": attempt["attempt_id"],
                "horizon_attempt_id": "INVENTED-HORIZON", "fold_ordinal": fold_ordinal,
                "partition_role": role, "partition_geometry_receipt_sha256": geometry["receipt_sha256"],
                "partition_frame_sha256": geometry["frame_sha256"],
                "partition_axis_sha256": raw._sha_json(facts["trade_power_partition_axis"]),
                "cost_precommitment_receipt_sha256": "a" * 64, "facts": facts})
        folds.append({"fold_ordinal": fold_ordinal, "lanes": lanes,
            "provenance": {"anchor_horizon_decision": {"partition_geometry": geometries}}})
        costs.append(cost_rows)
        axes.append({"oos_start_date_utc": frame.index[0].date().isoformat(),
            "oos_end_date_utc": frame.index[-1].date().isoformat(),
            "shared_oos_partition": {"fixture_provenance": "INVENTED_AXIS", "ordinal": fold_ordinal}})
    monkeypatch.setattr(raw, "_validated_partition_geometry", lambda value, **kwargs: copy.deepcopy(value))
    monkeypatch.setattr(raw, "_selected_partition_cost_precommitments", lambda **kwargs:
        copy.deepcopy(costs[kwargs["planned_fold"]["ordinal"]]))
    monkeypatch.setattr(raw, "_validated_source_identity", lambda value:
        original["initial_risk_source_binding"]["backtest_source_contract_sha256"])
    origin = {"precommitment_sha256": raw._sha_json(precommitment),
        "matched_base_facts_sha256": raw._sha_json(witnesses),
        "raw_body_sha256": raw._sha_json({"attempt_id": attempt["attempt_id"], "folds": folds}),
        "unit": {"ordinal": 0, "raw_attempt_id": attempt["attempt_id"]}}
    return dict(frozen=frozen, attempt=attempt, witnesses=witnesses, origin=origin,
        raw_folds=folds, shared_axis=axes, partition_cost_index=object(),
        cross_binding_attempt={"horizon_attempt_id": "INVENTED-HORIZON"})


def test_real_matched_record_is_accepted_by_public_v17_request_ownership(monkeypatch):
    arguments = _record_inputs(monkeypatch)
    before = raw._canonical_bytes({key: arguments[key] for key in (
        "attempt", "witnesses", "origin", "raw_folds", "shared_axis")})
    record = raw._matched_floor_attempt_record(**arguments)
    ledger = {"selection_addendum": {"entries": [record]}}
    request = winner.create_winner_selection_request_v17(
        arguments["frozen"].precommitment, {"attempts": [copy.deepcopy(arguments["attempt"])]}, ledger)
    assert request.evidence_ledger.to_dict() == ledger
    assert len(record["partition_statuses"]) == 12 and len(record["final_folds"]) == 4
    for final in record["final_folds"]:
        status = record["partition_statuses"][final["fold_ordinal"] * 3 + 2]
        assert status["floor_status"] == final["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"
        assert status["floor_reason_codes"] == final["floor_reason_codes"] == []
        assert status["floor_reason_codes"] is not final["floor_reason_codes"]
    assert raw._canonical_bytes({key: arguments[key] for key in (
        "attempt", "witnesses", "origin", "raw_folds", "shared_axis")}) == before


@pytest.mark.parametrize("damage", ("alias", "cycle"))
def test_public_v17_request_still_refuses_aliased_or_cyclic_inputs(damage):
    shared = []
    evidence = {"first": shared, "second": shared} if damage == "alias" else {"cycle": shared}
    if damage == "cycle":
        shared.append(evidence)
    with pytest.raises(ValueError, match="CYCLIC_OR_ALIASED_CONTAINER"):
        winner.create_winner_selection_request_v17(
            {"contract_version": winner.V17_GENERATION.precommitment_version}, {}, evidence)
