"""INVENTED public transport fixtures; no market, owner, or trading authority."""
from dataclasses import replace
import hashlib
import json

import pytest

import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from tests.test_public_v17_terminal import _invented_v17_all_rejected_request


@pytest.fixture(scope="module")
def terminal_case():
    request = _invented_v17_all_rejected_request()
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.to_dict()["validated"] is True, decision.reason_codes
    return request, decision


def test_fresh_producer_evaluates_once_preserves_result_and_public_cold_replay(
        terminal_case, monkeypatch):
    request, expected = terminal_case
    expected_marginal = winner.historical_book_marginal_receipt(expected, request)
    assert expected_marginal is not None
    original = winner._evaluate_body
    calls = []

    def counted(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(winner, "_evaluate_body", counted)
    decision, marginal = raw._evaluate_produced_winner_decision(request, winner.V17_GENERATION)
    assert calls == [request]
    assert decision._body_json == expected._body_json
    assert decision._receipt_sha256 == expected._receipt_sha256
    assert marginal == expected_marginal
    assert winner.verify_historical_cell_winner_decision(decision, request)
    assert calls == [request, request]
    assert winner.historical_book_marginal_receipt(decision, request) == expected_marginal
    assert calls == [request, request, request]


def test_fresh_result_check_uses_owned_hashes_without_decoding_payloads(
        terminal_case, monkeypatch):
    request, decision = terminal_case

    def forbidden(*args, **kwargs):
        pytest.fail("fresh result check decoded a large immutable request payload")

    for payload_type in (winner.WinnerPrecommitmentPayload,
                         winner.WinnerAttemptLedgerPayload,
                         winner.WinnerEvidenceLedgerPayload):
        monkeypatch.setattr(payload_type, "to_dict", forbidden)
    body = raw._verify_produced_winner_decision(decision, request, winner.V17_GENERATION)
    assert body["request_sha256"] == request.request_sha256


@pytest.mark.parametrize("field,value", (
    ("validated", False),
    ("selection_scope", "INVENTED_OTHER_SCOPE"),
    ("evidence_scope", "INVENTED_OTHER_SCOPE"),
    ("parameter_evidence_inherited", True),
    ("contract_version", "INVENTED_OTHER_VERSION"),
    ("request_contract_version", "INVENTED_OTHER_VERSION"),
    ("live_authorized", True),
    ("capital_sizing_authorized", True),
    ("profitability_claimed", True),
    ("production_integrated", True),
    ("request_sha256", "0" * 64),
    ("precommitment_payload_sha256", "0" * 64),
    ("attempt_ledger_payload_sha256", "0" * 64),
    ("evidence_ledger_payload_sha256", "0" * 64),
))
def test_fresh_result_refuses_resealed_binding_or_authority_tamper(
        terminal_case, field, value):
    request, decision = terminal_case
    body = json.loads(decision._body_json.decode("ascii"))
    body[field] = value
    encoded = raw._canonical_bytes(body)
    changed = winner.WinnerSelectionDecision(encoded, hashlib.sha256(encoded).hexdigest())
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        raw._verify_produced_winner_decision(changed, request, winner.V17_GENERATION)
    assert error.value.code == "WINNER_V17_DECISION_VERIFICATION_FAILED"


@pytest.mark.parametrize("mutation", ("checksum", "noncanonical"))
def test_fresh_result_refuses_changed_decision_bytes(terminal_case, mutation):
    request, decision = terminal_case
    if mutation == "checksum":
        changed = replace(decision, _receipt_sha256="0" * 64)
    else:
        encoded = json.dumps(json.loads(decision._body_json.decode("ascii")),
                             indent=2, sort_keys=True).encode("ascii")
        changed = winner.WinnerSelectionDecision(encoded, hashlib.sha256(encoded).hexdigest())
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        raw._verify_produced_winner_decision(changed, request, winner.V17_GENERATION)
    assert error.value.code == "WINNER_V17_DECISION_VERIFICATION_FAILED"


@pytest.mark.parametrize("mutation", ("request_hash", "payload_hash"))
def test_fresh_result_refuses_crossed_owned_request(terminal_case, mutation):
    request, decision = terminal_case
    if mutation == "request_hash":
        request = replace(request, request_sha256="0" * 64)
    else:
        request = replace(request, precommitment=replace(
            request.precommitment, payload_sha256="0" * 64))
    with pytest.raises(raw.WinnerRawEvidenceError) as error:
        raw._verify_produced_winner_decision(decision, request, winner.V17_GENERATION)
    assert error.value.code == "WINNER_V17_DECISION_VERIFICATION_FAILED"
