"""INVENTED metadata: Book generation binding only, never a real2400-cell study."""
import pytest
import pm_book_multiplicity as book
import pm_winner_selection as winner
from test_book_multiplicity import _insufficient_cell, _terminal_family


@pytest.fixture(scope="module")
def presearch_metadata():
    return _insufficient_cell(postsearch=False, current=True)


def test_noise101_presearch_uses_explicit_released_generation(presearch_metadata):
    legacy = presearch_metadata
    plan = legacy.insufficient_horizon_plan
    state = book.create_matched_floor_historical_process_state_decision(
        winner_generation=winner.V18_GENERATION,
        symbol=legacy.symbol, timeframe=legacy.timeframe, regime=legacy.regime,
        state_kind=book.INSUFFICIENT_EVIDENCE,
        reason_code=book.INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
        source_group_decision=legacy.source_group_decision,
        regime_fit_decision=legacy.regime_fit_decision,
        attempted_trials_exact=0, horizon_plan=plan)
    assert state is not None
    assert state.to_dict()["insufficient_evidence_binding"]["schema_version"] == (
        "fxpm.historical-insufficient-evidence-binding.v6")
    evidence = book.process_state_cell_evidence(process_state_decision=state,
        source_group_decision=legacy.source_group_decision,
        regime_fit_decision=legacy.regime_fit_decision, horizon_plan=plan)
    assert evidence is not None
    assert book._infer_input_generation(horizon_plan=plan, process_state=state) is book.V18_GENERATION
    assert legacy.process_state_decision.to_dict()["insufficient_evidence_binding"]["schema_version"] == (
        book.CURRENT_GENERATION.insufficient_schema)


def test_noise101_policy_changes_only_generation_identities():
    old = book.matched_floor_policy_snapshot()
    new = book.matched_floor_policy_snapshot(winner_generation=winner.V18_GENERATION)
    assert old == book.matched_floor_policy_snapshot(winner_generation=winner.V17_GENERATION)
    assert old.keys() == new.keys()
    assert {key for key in old if old[key] != new[key]} == {
        "contract_version", "selection_rule", "family_precommitment_sha256",
        "winner_selection_contract_version", "winner_selection_request_version",
        "winner_book_marginal_contract_version", "winner_evidence_ledger_version",
        "insufficient_evidence_binding_schema_version", "horizon_winner_cross_binding_schema_version"}
    assert new["family_size"] == 2400
    assert new["selection_rule"] == (
        "WINNER_V18_MATCHED_FLOOR_LOCAL_ADMISSIBILITY_PENDING_CELL_CAMPAIGN_VALIDATION")


def test_noise101_complete_invented_terminal_metadata_family_verifies():
    family = _terminal_family()
    decision = book.evaluate_matched_floor_historical_book_multiplicity(
        family, winner_generation=winner.V18_GENERATION)
    assert decision.valid, decision.reason_codes
    assert decision.to_dict()["contract_version"] == "fxpm.book-multiplicity.v18"
    assert book.verify_historical_book_multiplicity_decision(decision, family)


def test_noise101_unknown_generation_refuses_without_falling_back(presearch_metadata):
    plan = presearch_metadata.insufficient_horizon_plan
    with pytest.raises(ValueError, match="BOOK_MIXED_GENERATIONS"):
        book._infer_input_generation(horizon_plan=plan, preferred=object())
    with pytest.raises(ValueError, match="WINNER_GENERATION_INVALID"):
        book.book_generation_for_winner(object())
    with pytest.raises(ValueError, match="INSUFFICIENT_BINDING_VERSION_INVALID"):
        book.book_generation_for_insufficient_schema("unrecognized")
    assert book.create_matched_floor_historical_process_state_decision(
        winner_generation=object()) is None
    assert book._infer_input_generation(horizon_plan=plan,
        preferred=book.V18_GENERATION) is book.V18_GENERATION


def test_noise101_winner_reaches_released_versions_before_marginal_replay(monkeypatch):
    from test_noise101_winner_generation import _v18_request
    request = _v18_request()
    decision = winner.evaluate_historical_cell_winner(request)
    assert winner.verify_historical_cell_winner_decision(decision, request)
    cell = decision.to_dict()["cell"]
    # No source authority is supplied: stop before marginal or source validation.
    evidence = book.HistoricalBookCellEvidence(symbol=cell["symbol"],
        timeframe=cell["timeframe"], regime=cell["regime"],
        source_group_decision=None, regime_fit_decision=None,
        winner_decision=decision, winner_request=request)

    class InventedMarginalBoundary(RuntimeError):
        pass

    def stop_at_marginal(actual_decision, actual_request):
        assert actual_decision is decision and actual_request is request
        raise InventedMarginalBoundary("INVENTED_STOP_BEFORE_MARGINAL_AND_SOURCE_VALIDATION")

    monkeypatch.setattr(winner, "historical_book_marginal_receipt", stop_at_marginal)
    with pytest.raises(InventedMarginalBoundary):
        book._winner_record(evidence, 0, cell["cell_id"], {}, _generation=book.V18_GENERATION)
