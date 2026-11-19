"""INVENTED incomplete transport requests; public rejection/verification, no model or market run."""
import pytest

import pm_winner_raw_evidence as raw
import pm_winner_selection as winner


@pytest.mark.parametrize("generation,builder,code,details", (
    (winner.V17_GENERATION, winner.create_winner_selection_request_v17,
        "WINNER_V17_DECISION_VERIFICATION_FAILED", True),
    (winner.V16_GENERATION, winner.create_winner_selection_request_v16,
        "WINNER_V16_DECISION_VERIFICATION_FAILED", True),
    (winner.CURRENT_GENERATION, winner.create_winner_selection_request,
        "WINNER_V15_DECISION_VERIFICATION_FAILED", False),
), ids=("v17-reasons", "v16-unchanged-reasons", "v15-legacy-message"))
def test_produced_decision_reports_actual_public_rejection(generation, builder, code, details):
    # Public ownership succeeds; omitted policy fields deliberately yield a real rejection.
    request = builder({"contract_version": generation.precommitment_version,
        "fixture_provenance": "INVENTED_INCOMPLETE_POLICY_FOR_DIAGNOSTIC_ONLY"},
        {"attempts": []}, {"entries": []})
    assert request is not None
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.status == "REJECTED" and decision.reason_codes
    assert winner.verify_historical_cell_winner_decision(decision, request) is False

    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        raw._verify_produced_winner_decision(decision, request, generation)
    assert error.value.code == code
    assert str(error.value) == (code + ": " + ",".join(decision.reason_codes) if details else code)
