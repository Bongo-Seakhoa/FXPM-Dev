"""INVENTED SOFTWARE TRANSPORT ONLY: no market result or source/owner authority.

This prospective public V17 terminal fixture does not reproduce the q303 request.
No Backtester, profile loader, producer/context or declared-origin capability is used.
"""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys

import pm_core as core
import pm_spread_profile as spread
import pm_winner_selection as winner


_fixture_path = Path(__file__).with_name("test_winner_selection.py")
if not _fixture_path.is_file():
    _fixture_path = Path.cwd() / "tests" / "test_winner_selection.py"
_spec = spec_from_file_location("fxpm_invented_public_v17_golden", _fixture_path)
legacy = module_from_spec(_spec)
sys.modules[_spec.name] = legacy
_spec.loader.exec_module(legacy)


def _invented_pair_contract():
    tiers = []
    for name, points in (("P75", 1), ("P95", 2)):
        descriptor = {
            "schema_version": spread.TIER_SCHEMA,
            "symbol": "AUDUSD", "timeframe": "M5", "source_timeframe": "M5",
            "tier": name, "point_price": 0.00001, "spread_points": points,
            "full_spread_price": points * 0.00001, "qualification_authority": False,
            "provenance": "INVENTED_SOFTWARE_TRANSPORT_ONLY",
        }
        model = {
            "mode": "conservative_max", "source": "INVENTED_SOFTWARE_TRANSPORT_ONLY",
            "frozen_spread_tier": descriptor,
            "entry_admission_policy": spread.ADMISSION_POLICY,
            "entry_admission_schema": spread.ADMISSION_SCHEMA,
        }
        policy = core.build_numerical_execution_cost_policy(
            use_spread=True, spread_mode=core.build_backtest_spread_mode(model),
            instrument_spec_sha256=legacy._sha("INVENTED-SOFTWARE-INSTRUMENT-SPEC"),
        )
        tiers.append({"tier": name, "frozen_spread_tier": descriptor,
                      "numerical_execution_cost_policy": policy})
    return core.validate_backtest_frozen_tier_pair_contract({
        "schema_version": core.BACKTEST_FROZEN_TIER_PAIR_CONTRACT_SCHEMA_VERSION,
        "admission_schema": spread.ADMISSION_SCHEMA,
        "admission_policy": spread.ADMISSION_POLICY,
        "qualification_authority": False, "tiers": tiers,
    })


def _invented_v17_all_rejected_request():
    request = legacy._current_payload_request(pair_contract=_invented_pair_contract())
    policy = request["precommitment_payload"]
    attempts = request["attempt_ledger_payload"]
    evidence = request["evidence_ledger_payload"]
    for attempt, entry in zip(attempts["attempts"], evidence["entries"], strict=True):
        attempt["precommitted_evidence_role"] = "PRESEARCH_PROCESS_REJECTED"
        entry.update(evidence_status="PROCESS_REJECTED",
                     status_reason="PRESEARCH_PROCESS_REJECTED", folds=[])
    legacy._refresh_complete_chain(request)
    policy.update(
        contract_version=winner.V17_GENERATION.precommitment_version,
        selection_policy_sha256=winner.V17_GENERATION.policy_sha256,
        selection_addendum_contract=dict(winner.V17_SELECTION_ADDENDUM_CONTRACT),
    )
    complexity = []
    for attempt in attempts["attempts"]:
        row = {key: attempt[key] for key in (
            "ordinal", "process_candidate_id", "strategy_id",
            "trial_definition_sha256", "refit_recipe_sha256")}
        row.update(source_strategy_domain_sha256=None, parameter_domain=None,
                   parameter_domain_sha256=None, finite_domain_cardinality=None,
                   active_free_parameter_count=None)
        complexity.append(row)
    policy["selection_complexity_roster"] = complexity
    policy["selection_complexity_roster_sha256"] = legacy._sha_json(complexity)
    evidence["contract_version"] = winner.V17_GENERATION.evidence_version
    for axis in evidence["shared_fold_axis"]:
        axis["shared_oos_partition"]["schema_version"] = winner.V17_GENERATION.shared_partition_version
    legacy._refresh_current_request(request)
    floor_entries = []
    for attempt in attempts["attempts"]:
        floor_entries.append({
            **{key: attempt[key] for key in ("ordinal", "attempt_id", "process_candidate_id")},
            "stage_origin_binding": None, "partition_witnesses_descriptor": None,
            "full_witnesses_sha256": None, "partition_statuses": [], "final_folds": [],
        })
    evidence["selection_addendum"] = {
        "contract_version": "fxpm.matched-floor-selection-evidence.v1",
        "precommitment_payload_sha256": legacy._sha_json(policy),
        "attempt_ledger_payload_sha256": legacy._sha_json(attempts),
        "selection_complexity_roster_sha256": policy["selection_complexity_roster_sha256"],
        "entries": floor_entries, "entries_sha256": legacy._sha_json(floor_entries),
    }
    return winner.create_winner_selection_request_v17(policy, attempts, evidence)


def test_public_v17_all_presearch_rejected_evaluates_and_verifies():
    request = _invented_v17_all_rejected_request()
    decision = winner.evaluate_historical_cell_winner(request)
    body = decision.to_dict()
    assert body["validated"] is True, body["reason_codes"]
    assert body["contract_version"] == winner.V17_GENERATION.selection_version
    assert body["status"] == "NO_PROCESS_WINNER"
    assert body["reason_codes"] == ["NO_P75_ADMISSIBLE_PROCESS_CANDIDATE"]
    assert body["winner"] is None
    assert body["reporting_candidate"] is not None
    assert body["attempted_trials_exact"] == len(request.attempt_ledger.to_dict()["attempts"])
    marginal = body["historical_book_marginal_receipt"]
    assert marginal["contract_version"] == winner.V17_GENERATION.marginal_version
    assert marginal["local_admissibility_passed"] is False
    assert marginal["marginal_p_value"] == 1.0
    assert winner.verify_historical_cell_winner_decision(decision, request)


def test_public_v17_rejects_wrong_pinned_policy_even_with_coherent_bindings():
    original = _invented_v17_all_rejected_request()
    policy = original.precommitment.to_dict()
    attempts = original.attempt_ledger.to_dict()
    evidence = original.evidence_ledger.to_dict()
    policy["selection_policy_sha256"] = winner.CURRENT_GENERATION.policy_sha256
    policy_sha = legacy._sha_json(policy)
    attempts["precommitment_payload_sha256"] = policy_sha
    attempt_sha = legacy._sha_json(attempts)
    evidence["precommitment_payload_sha256"] = policy_sha
    evidence["attempt_ledger_payload_sha256"] = attempt_sha
    evidence["selection_addendum"]["precommitment_payload_sha256"] = policy_sha
    evidence["selection_addendum"]["attempt_ledger_payload_sha256"] = attempt_sha
    request = winner.create_winner_selection_request_v17(policy, attempts, evidence)
    decision = winner.evaluate_historical_cell_winner(request)
    body = decision.to_dict()
    assert body["status"] == "REJECTED"
    assert body["validated"] is False
    assert body["reason_codes"] == ["SELECTION_POLICY_MISMATCH"]
    assert not winner.verify_historical_cell_winner_decision(decision, request)


def test_body_marginal_adapter_matches_public_verified_receipt():
    request = _invented_v17_all_rejected_request()
    decision = winner.evaluate_historical_cell_winner(request)
    body = decision.to_dict()
    adapted = winner._historical_book_marginal_from_body(body, decision.receipt_sha256)
    public = winner.historical_book_marginal_receipt(decision, request)
    assert adapted is not None and adapted == public
    assert winner._historical_book_marginal_from_body({}, decision.receipt_sha256) is None
    assert winner._historical_book_marginal_from_body(
        {"historical_book_marginal_receipt": None}, decision.receipt_sha256) is None


def test_public_marginal_retains_real_cold_evaluation(monkeypatch):
    request = _invented_v17_all_rejected_request()
    decision = winner.evaluate_historical_cell_winner(request)
    actual_evaluate = winner._evaluate_body
    evaluations = []

    def measured_evaluate(*args, **kwargs):
        evaluations.append(1)
        return actual_evaluate(*args, **kwargs)

    monkeypatch.setattr(winner, "_evaluate_body", measured_evaluate)
    assert winner._historical_book_marginal_from_body(decision.to_dict(), decision.receipt_sha256) is not None
    assert evaluations == []
    assert winner.historical_book_marginal_receipt(decision, request) is not None
    assert evaluations == [1]
