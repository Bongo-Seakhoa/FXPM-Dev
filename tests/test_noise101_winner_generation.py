"""INVENTED SOFTWARE METADATA ONLY: no process, source, account or market authority.

These requests reuse the accepted public terminal fixture. Their rejected attempts
exercise transport and generation contracts; they are not the actual 101 roster.
"""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import MappingProxyType

import pytest

import pm_winner_selection as winner
import pm_winner_raw_evidence as raw
from tests.test_public_v17_terminal import _invented_v17_all_rejected_request, legacy


def _payloads(generation, attempt_count):
    original = _invented_v17_all_rejected_request()
    policy = original.precommitment.to_dict()
    attempts = original.attempt_ledger.to_dict()
    evidence = original.evidence_ledger.to_dict()
    attempt_template = attempts["attempts"][0]
    entry_template = evidence["entries"][0]
    attempts["attempts"], evidence["entries"] = [], []
    complexity, floors = [], []
    for ordinal in range(attempt_count):
        attempt = deepcopy(attempt_template)
        attempt.update(ordinal=ordinal,
            attempt_id=hashlib.sha1(f"INVENTED-V18-ATTEMPT:{ordinal}".encode()).hexdigest(),
            process_candidate_id=hashlib.sha1(f"INVENTED-V18-CANDIDATE:{ordinal}".encode()).hexdigest())
        for field in ("process_sha256", "horizon_process_sha256", "trial_definition_sha256", "refit_recipe_sha256"):
            attempt[field] = legacy._sha(f"INVENTED-V18:{field}:{ordinal}")
        entry = deepcopy(entry_template)
        for field in entry.keys() & attempt.keys():
            entry[field] = deepcopy(attempt[field])
        attempts["attempts"].append(attempt)
        evidence["entries"].append(entry)
        row = {key: attempt[key] for key in ("ordinal", "process_candidate_id", "strategy_id",
            "trial_definition_sha256", "refit_recipe_sha256")}
        row.update(source_strategy_domain_sha256=None, parameter_domain=None,
            parameter_domain_sha256=None, finite_domain_cardinality=None, active_free_parameter_count=None)
        complexity.append(row)
        floors.append({**{key: attempt[key] for key in ("ordinal", "attempt_id", "process_candidate_id")},
            "stage_origin_binding": None, "partition_witnesses_descriptor": None,
            "full_witnesses_sha256": None, "partition_statuses": [], "final_folds": []})
    policy.update(contract_version=generation.precommitment_version,
        selection_policy_sha256=generation.policy_sha256,
        selection_complexity_roster=complexity,
        selection_complexity_roster_sha256=legacy._sha_json(complexity))
    evidence["contract_version"] = generation.evidence_version
    for axis in evidence["shared_fold_axis"]:
        axis["shared_oos_partition"]["schema_version"] = generation.shared_partition_version
    payloads = {"precommitment_payload": policy, "attempt_ledger_payload": attempts,
        "evidence_ledger_payload": evidence}
    legacy._refresh_complete_chain(payloads)
    legacy._refresh_current_request(payloads)
    evidence["selection_addendum"] = {"contract_version": "fxpm.matched-floor-selection-evidence.v1",
        "precommitment_payload_sha256": legacy._sha_json(policy),
        "attempt_ledger_payload_sha256": legacy._sha_json(attempts),
        "selection_complexity_roster_sha256": policy["selection_complexity_roster_sha256"],
        "entries": floors, "entries_sha256": legacy._sha_json(floors)}
    return payloads


def _v18_request(attempt_count=2):
    """Return a genuine public request containing only invented rejected metadata."""
    return winner.create_winner_selection_request_v18(**_payloads(winner.V18_GENERATION, attempt_count))


def _old_v13_policy():
    # Frozen policy from bc5c023e994470ee5e8ba34e381349212c4ab3c1, before Noise101.
    path = Path(__file__).with_name("fixtures") / "winner_v13_policy.json"
    return json.loads(path.read_bytes()), "84f4f22073d4b331ca671636138b9761445181e0e904fc626cf734dfb8761922"


def test_v18_policy_changes_exactly_two_keys_and_preserves_actual_v13_bytes():
    old, old_sha = _old_v13_policy()
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    assert canonical(dict(winner.V17_SELECTION_POLICY)) == canonical(old)
    assert hashlib.sha256(canonical(old)).hexdigest() == old_sha == winner.V17_GENERATION.policy_sha256
    new = dict(winner.V18_SELECTION_POLICY)
    assert new.keys() == old.keys()
    changed = {key for key in new if canonical(new[key]) != canonical(old[key])}
    assert changed == {"policy_version", "moving_block_bootstrap_operation_limit"}
    assert new["policy_version"] == "fxpm.winner-statistical-policy.v14"
    assert new["moving_block_bootstrap_operation_limit"] == 141484032
    assert winner.V18_GENERATION.statistical_policy_version == new["policy_version"]


def test_public_v18_request_evaluates_and_verifies_expected_v14_policy():
    request = _v18_request()
    decision = winner.evaluate_historical_cell_winner(request)
    body = decision.to_dict()
    assert body["validated"] is True, body["reason_codes"]
    assert body["contract_version"] == "fxpm.winner-selection.v18"
    assert body["request_contract_version"] == "fxpm.winner-selection-request.v6"
    assert body["status"] == "NO_PROCESS_WINNER"
    assert body["historical_book_marginal_receipt"]["contract_version"] == "fxpm.winner-book-marginal.v5"
    assert winner.verify_historical_cell_winner_decision(decision, request)
    assert all(body[flag] is False for flag in ("live_authorized", "capital_sizing_authorized",
        "profitability_claimed", "production_integrated"))


def test_public_v18_validator_rejects_coherently_hashed_wrong_policy_version(monkeypatch):
    wrong = {**winner.V18_SELECTION_POLICY, "policy_version": "fxpm.winner-statistical-policy.v13"}
    wrong_generation = replace(winner.V18_GENERATION, policy=MappingProxyType(wrong),
        policy_sha256=legacy._sha_json(wrong))
    monkeypatch.setattr(winner, "V18_GENERATION", wrong_generation)
    request = _v18_request()
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.to_dict()["validated"] is False
    assert decision.to_dict()["reason_codes"] == ["PRIMARY_SELECTION_POLICY_IMPLEMENTATION_MISMATCH"]
    assert not winner.verify_historical_cell_winner_decision(decision, request)


def test_public_v18_303_codec_owns_detached_payloads():
    payloads = _payloads(winner.V18_GENERATION, 303)
    request = winner.create_winner_selection_request_v18(**payloads)
    frozen = request.to_dict()
    payloads["attempt_ledger_payload"]["attempts"][0]["strategy_id"] = "INVENTED_MUTATED_INPUT"
    payloads["evidence_ledger_payload"]["selection_addendum"]["entries"].clear()
    assert request.to_dict() == frozen
    decoded = winner.create_winner_selection_request(**{
        "precommitment_payload": request.precommitment.to_dict(),
        "attempt_ledger_payload": request.attempt_ledger.to_dict(),
        "evidence_ledger_payload": request.evidence_ledger.to_dict()})
    assert decoded.to_dict() == frozen
    decision = winner.evaluate_historical_cell_winner(decoded)
    assert decision.to_dict()["validated"] is True, decision.to_dict()["reason_codes"]
    assert decision.to_dict()["attempted_trials_exact"] == 303
    assert winner.verify_historical_cell_winner_decision(decision, decoded)


@pytest.mark.parametrize("generation_name,count", [("V17_GENERATION", 303), ("V18_GENERATION", 304)])
def test_public_attempt_bounds_remain_generation_specific(generation_name, count):
    generation = getattr(winner, generation_name)
    request = winner.create_winner_selection_request(**_payloads(generation, count))
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.to_dict()["validated"] is False
    assert decision.to_dict()["reason_codes"] == ["PLANNED_ATTEMPT_COUNT_INVALID"]


def test_raw_format_selectors_share_explicit_known_generations_and_refuse_unknown():
    expected = [(winner.CURRENT_GENERATION, "v6", "v5", raw.freeze_winner_search_plan_v6,
        raw.restore_frozen_winner_search_plan_v6, raw.produce_winner_v15_raw_evidence),
        (winner.V16_GENERATION, "v7", "v6", raw.freeze_winner_search_plan_v7,
        raw.restore_frozen_winner_search_plan_v7, raw.produce_winner_v16_raw_evidence),
        (winner.V17_GENERATION, "v9", "v7", raw.freeze_winner_search_plan_v9,
        raw.restore_frozen_winner_search_plan_v9, raw.produce_winner_v17_raw_evidence),
        (winner.V18_GENERATION, "v10", "v8", raw.freeze_winner_search_plan_v10,
        raw.restore_frozen_winner_search_plan_v10, raw.produce_winner_v18_raw_evidence)]
    for generation, frozen, production, freezer, restorer, producer in expected:
        schema = "fxpm.winner-search-plan-frozen." + frozen
        assert winner.is_supported_current_generation(generation)
        assert raw.bound_frozen_plan_schema_for_generation(generation) == schema
        assert raw.raw_production_schema_for_generation(generation) == "fxpm.winner-raw-evidence-production." + production
        assert raw.bound_frozen_plan_freezer_for_generation(generation) is freezer
        assert raw.bound_frozen_plan_restorer_for_generation(generation) is restorer
        assert raw.raw_evidence_producer_for_generation(generation) is producer
        assert raw.frozen_plan_restorer_for_schema(schema, bound_only=True) is restorer
    assert raw.horizon_winner_cross_binding_schemas_for_generation(winner.V18_GENERATION) == (
        "fxpm.historical-horizon-winner-cross-binding.v6", "fxpm.horizon-winner-cross-binding.v7")
    assert raw.frozen_plan_restorer_for_schema(raw.CURRENT_FROZEN_PLAN_SCHEMA) is raw.restore_frozen_winner_search_plan_v4
    assert raw.frozen_plan_restorer_for_schema(raw.V16_FROZEN_PLAN_SCHEMA) is raw.restore_frozen_winner_search_plan_v5
    assert raw.frozen_plan_restorer_for_schema(raw.CURRENT_FROZEN_PLAN_SCHEMA, bound_only=True) is None
    assert raw.frozen_plan_restorer_for_schema("INVENTED_UNKNOWN_SCHEMA") is None
    assert not winner.is_supported_current_generation(replace(winner.V18_GENERATION))
    for selector in (raw.bound_frozen_plan_schema_for_generation, raw.raw_production_schema_for_generation,
            raw.bound_frozen_plan_freezer_for_generation, raw.bound_frozen_plan_restorer_for_generation,
            raw.raw_evidence_producer_for_generation, raw.horizon_winner_cross_binding_schemas_for_generation):
        with pytest.raises(raw.WinnerRawEvidenceError, match="WINNER_GENERATION_INVALID"):
            selector(replace(winner.V18_GENERATION))


def test_raw_303_horizon_codec_detaches_values_and_retains_legacy_bound():
    # This incomplete metadata carrier is never admitted as a fitted/source plan.
    request = _v18_request(303)
    contracts = [{"lookback_kind": "NATIVE_OBSERVATIONS", "lookback_value": 2,
        "source_bar_count": 2} for _ in range(303)]
    normalized = raw._normalized_horizon_execution_contracts(contracts,
        expected_count=303, _generation=winner.V18_GENERATION)
    frozen = raw.FrozenWinnerSearchPlan(symbol="AUDUSD", timeframe="M5", regime="TREND",
        artifact_namespace="invented-v18-codec-only", generation_context_sha256="0" * 64,
        precommitment_bytes=raw._canonical_bytes(request.precommitment.to_dict()),
        attempt_ledger_bytes=raw._canonical_bytes(request.attempt_ledger.to_dict()),
        source_contract_seed_bytes=b"{}", cost_ladder_contract_bytes=b"{}",
        risk_unit_contract_bytes=b"{}", cost_ladder_adversity_plan_bytes=b"{}",
        frozen_receipt_bytes=b"{}", frozen_receipt_entry_bytes=b"{}",
        horizon_execution_contracts_bytes=raw._canonical_bytes(normalized))
    contracts[0]["source_bar_count"] = 3
    assert normalized[0]["source_bar_count"] == 2
    decoded = frozen.horizon_execution_contracts
    assert len(decoded) == 303
    decoded[0]["source_bar_count"] = 4
    assert frozen.horizon_execution_contracts[0]["source_bar_count"] == 2
    with pytest.raises(raw.WinnerRawEvidenceError, match="HORIZON_EXECUTION_CONTRACT_ROSTER_INVALID"):
        raw._normalized_horizon_execution_contracts(normalized,
            expected_count=303, _generation=winner.V17_GENERATION)
    legacy_header = {**request.precommitment.to_dict(),
        "contract_version": winner.V17_GENERATION.precommitment_version}
    with pytest.raises(raw.WinnerRawEvidenceError, match="HORIZON_EXECUTION_CONTRACT_BYTES_INVALID"):
        replace(frozen, precommitment_bytes=raw._canonical_bytes(legacy_header)).horizon_execution_contracts
