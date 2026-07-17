from __future__ import annotations

import base64
import hashlib
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import pm_cap_calibration as cap


START = datetime(2020, 1, 1, tzinfo=timezone.utc)
END = datetime(2020, 3, 13, tzinfo=timezone.utc)


class Poison:
    def __iter__(self):
        raise AssertionError("legacy candidates were consumed")

    def __str__(self):
        raise AssertionError("legacy input was stringified")


def _sha(label: str) -> str:
    return cap.canonical_json_sha256({"label": label})


def _constraints() -> cap.CapCalibrationConstraints:
    return cap.CapCalibrationConstraints(
        minimum_accepted_positions=2,
        maximum_mtm_drawdown_pct=40.0,
        maximum_initial_stop_stress_drawdown_pct=45.0,
        maximum_joint_gap_stress_loss_pct=40.0,
        maximum_factor_gap_stress_loss_pct=25.0,
        maximum_margin_utilization_pct=60.0,
        minimum_outer_anchors=4,
        maximum_outer_anchors=6,
        maximum_candidate_policies=8,
        minimum_exposure_effective_observations=2.0,
        minimum_near_cap_observations=2,
        minimum_paired_policy_observations=2,
        minimum_profitable_anchor_fraction=0.75,
        familywise_alpha=0.10,
        minimum_inner_calendar_days=8,
        minimum_outer_calendar_days=8,
        minimum_current_calendar_days=8,
        minimum_hac_effective_observations=2.0,
        hac_lag_days=1,
        minimum_margin_level_pct=150.0,
        minimum_free_margin_pct=25.0,
    )


def _policies() -> list[dict[str, object]]:
    return [
        {
            "policy_id": "high",
            "max_total_stop_risk_pct": 20.0,
            "max_factor_stop_risk_pct": 12.0,
            "max_same_symbol_stop_risk_pct": 8.0,
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 50.0,
            "gap_stress_multiple": 1.5,
        },
        {
            "policy_id": "low",
            "max_total_stop_risk_pct": 10.0,
            "max_factor_stop_risk_pct": 6.0,
            "max_same_symbol_stop_risk_pct": 4.0,
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 50.0,
            "gap_stress_multiple": 1.5,
        },
    ]


def _contract_lineage() -> dict[str, str]:
    return {
        "book_contract_version": "book-v11",
        "refit_contract_version": "refit-v2",
        "cost_contract_sha256": _sha("cost-contract"),
        "native_source_contract_sha256": _sha("native-contract"),
        "replay_contract_sha256": _sha("replay-contract"),
        "margin_contract_sha256": _sha("margin-contract"),
    }


def _lineage(anchor_id: str, phase: str, *, current: bool = False) -> dict[str, object]:
    contract = _contract_lineage()
    stable = "CURRENT" if current else anchor_id
    source = f"{stable}|{phase}"
    return {
        **contract,
        "book_role": (
            "PROCESS_VALIDATED_CURRENT_REFIT" if current else "ANCHOR_LOCAL_REFIT"
        ),
        "book_decision_sha256": _sha(f"book-decision:{stable}"),
        "book_generation_receipt_sha256": _sha(f"book-generation:{stable}"),
        "refit_receipt_sha256": _sha(f"refit-receipt:{stable}"),
        "refit_execution_sha256": _sha(f"refit-execution:{stable}"),
        "output_book_sha256": _sha(f"output-book:{stable}"),
        "winner_roster_sha256": _sha(f"winner-roster:{stable}"),
        "native_activation_id": f"activation-{stable}",
        "native_activation_manifest_sha256": _sha(f"activation-manifest:{stable}"),
        "native_activation_roster_sha256": _sha(f"activation-roster:{stable}"),
        "native_bars_sha256_by_source": {"EURUSD|H1": _sha(f"bars:{source}")},
        "native_sidecar_sha256_by_source": {
            "EURUSD|H1": _sha(f"sidecar:{source}")
        },
        "replay_output_sha256": _sha(f"placeholder-replay:{source}"),
        "margin_terms_sha256": _sha(f"margin-terms:{stable}"),
        "broker_terms_sha256": _sha(f"broker-terms:{stable}"),
    }


def _current_lineage() -> dict[str, object]:
    result = _lineage("CURRENT", "CURRENT_SELECTION", current=True)
    result.pop("replay_output_sha256")
    return result


def _window(start: date, days: int = 8) -> dict[str, str]:
    return {
        "start_date": start.isoformat(),
        "end_date": (start + timedelta(days=days)).isoformat(),
    }


def _anchors() -> tuple[list[dict[str, object]], dict[str, str]]:
    cursor = date(2020, 1, 1)
    anchors: list[dict[str, object]] = []
    for index in range(4):
        inner = _window(cursor)
        cursor += timedelta(days=8)
        outer = _window(cursor)
        cursor += timedelta(days=8)
        anchors.append(
            {
                "anchor_id": f"A{index + 1}",
                "inner_window": inner,
                "outer_window": outer,
            }
        )
    return anchors, _window(cursor)


def _daily(
    window: dict[str, str],
    policy: dict[str, object],
    *,
    return_base: float,
    utilization_scale: float = 0.90,
) -> list[dict[str, object]]:
    start = date.fromisoformat(window["start_date"])
    end = date.fromisoformat(window["end_date"])
    rows: list[dict[str, object]] = []
    for index in range((end - start).days):
        margin = 20.0
        rows.append(
            {
                "date": (start + timedelta(days=index)).isoformat(),
                "stress_net_return": return_base + (0.0005 if index % 2 else -0.0005),
                "mtm_drawdown_pct": 5.0,
                "initial_stop_drawdown_pct": 6.0,
                "total_stop_risk_pct": utilization_scale
                * float(policy["max_total_stop_risk_pct"]),
                "factor_stop_risk_pct": utilization_scale
                * float(policy["max_factor_stop_risk_pct"]),
                "same_symbol_stop_risk_pct": utilization_scale
                * float(policy["max_same_symbol_stop_risk_pct"]),
                "joint_gap_loss_pct": 7.0,
                "factor_gap_loss_pct": 4.0,
                "margin_utilization_pct": margin,
                "margin_level_pct": 10_000.0 / margin,
                "free_margin_pct": 100.0 - margin,
                "admission_events": 1,
            }
        )
    return rows


def _attempt(
    *,
    anchor_id: str,
    phase: str,
    window: dict[str, str],
    policy: dict[str, object],
    return_base: float,
    utilization_scale: float = 0.90,
    status: str = "COMPLETE",
) -> dict[str, object]:
    current = anchor_id == "CURRENT"
    lineage = _lineage(anchor_id, phase, current=current)
    lineage["replay_output_sha256"] = _sha(
        f"replay:{anchor_id}:{phase}:{policy['policy_id']}"
    )
    body: dict[str, object] = {
        "attempt_id": f"{anchor_id}|{phase}|{policy['policy_id']}",
        "anchor_id": anchor_id,
        "phase": phase,
        "policy_id": policy["policy_id"],
        "policy_sha256": cap.canonical_json_sha256(policy),
        "status": status,
        "failure_code": None if status == "COMPLETE" else "REPLAY_OOM",
        "window": deepcopy(window),
        "lineage": lineage,
        "daily_observations": (
            _daily(
                window,
                policy,
                return_base=return_base,
                utilization_scale=utilization_scale,
            )
            if status == "COMPLETE"
            else []
        ),
        "accepted_positions": 8 if status == "COMPLETE" else 0,
    }
    body["attempt_sha256"] = cap.canonical_json_sha256(body)
    return body


def _private_key() -> tuple[Ed25519PrivateKey, bytes]:
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return private, public


def _sign(
    private: Ed25519PrivateKey,
    key_id: str,
    kind: str,
    body: dict[str, object],
) -> dict[str, object]:
    signature = private.sign(cap.capital_authority_signature_payload(kind, key_id, body))
    return {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": key_id,
        "body": body,
        "signature_base64": base64.b64encode(signature).decode("ascii"),
    }


def _bundle(
    *,
    outer_returns: dict[str, float] | None = None,
    current_returns: dict[str, float] | None = None,
    utilization: dict[str, float] | None = None,
) -> dict[str, object]:
    constraints = _constraints()
    policies = _policies()
    anchors, current_window = _anchors()
    precommit_body: dict[str, object] = {
        "schema_version": cap.CAP_PRECOMMIT_SCHEMA_VERSION,
        "precommitment_id": "capital-v8-test",
        "created_at_utc": "2026-01-01T00:00:00Z",
        "evidence_access_not_before_utc": "2026-01-02T00:00:00Z",
        "candidate_policies": policies,
        "anchor_plan": anchors,
        "current_selection_window": current_window,
        "selection_rule": deepcopy(cap._FROZEN_SELECTION_RULE),
        "constraints_sha256": cap.canonical_json_sha256(constraints.__dict__),
        "contract_lineage": _contract_lineage(),
    }
    pre_private, pre_public = _private_key()
    replay_private, replay_public = _private_key()
    precommit = _sign(
        pre_private,
        "pre-key",
        cap.PRECOMMITMENT_SIGNATURE_KIND,
        precommit_body,
    )
    attempts: list[dict[str, object]] = []
    outer_returns = outer_returns or {"low": 0.008, "high": 0.015}
    current_returns = current_returns or {"low": 0.009, "high": 0.018}
    utilization = utilization or {"low": 0.90, "high": 0.90}
    for anchor in anchors:
        for policy in policies:
            policy_id = str(policy["policy_id"])
            attempts.append(
                _attempt(
                    anchor_id=str(anchor["anchor_id"]),
                    phase="INNER_SELECTION",
                    window=anchor["inner_window"],
                    policy=policy,
                    return_base=0.010 if policy_id == "low" else 0.020,
                    utilization_scale=utilization[policy_id],
                )
            )
            attempts.append(
                _attempt(
                    anchor_id=str(anchor["anchor_id"]),
                    phase="OUTER_EVALUATION",
                    window=anchor["outer_window"],
                    policy=policy,
                    return_base=outer_returns[policy_id],
                    utilization_scale=utilization[policy_id],
                )
            )
    for policy in policies:
        policy_id = str(policy["policy_id"])
        attempts.append(
            _attempt(
                anchor_id="CURRENT",
                phase="CURRENT_SELECTION",
                window=current_window,
                policy=policy,
                return_base=current_returns[policy_id],
                utilization_scale=utilization[policy_id],
            )
        )
    replay_body: dict[str, object] = {
        "schema_version": cap.CAP_REPLAY_SCHEMA_VERSION,
        "precommitment_sha256": cap.canonical_json_sha256(precommit_body),
        "precommitment_envelope_sha256": cap.canonical_json_sha256(precommit),
        "generated_at_utc": "2026-01-03T00:00:00Z",
        "attempts": attempts,
    }
    replay = _sign(
        replay_private,
        "replay-key",
        cap.REPLAY_EVIDENCE_SIGNATURE_KIND,
        replay_body,
    )
    return {
        "constraints": constraints,
        "precommit": precommit,
        "replay": replay,
        "pre_private": pre_private,
        "replay_private": replay_private,
        "pre_keys": {"pre-key": pre_public},
        "replay_keys": {"replay-key": replay_public},
        "lineage": _current_lineage(),
    }


def _calibrate(
    bundle: dict[str, object], *, replay_root: Path | None = None
) -> dict[str, object]:
    return cap.calibrate_total_stop_risk_cap(
        candidates=Poison(),
        base_policies=Poison(),
        evaluation_start=START,
        evaluation_end=END,
        lineage=bundle["lineage"],
        constraints=bundle["constraints"],
        replay_evidence_root=replay_root,
        precommitment_receipt=bundle["precommit"],
        replay_evidence_receipt=bundle["replay"],
        trusted_precommitment_public_keys=bundle["pre_keys"],
        trusted_replay_public_keys=bundle["replay_keys"],
    )


def _authorization_evidence(
    bundle: dict[str, object], *, replay_root: Path = Path(".")
) -> dict[str, object]:
    return {
        "replay_evidence_root": replay_root,
        "trusted_replay_public_keys": bundle["replay_keys"],
        "precommitment_receipt": bundle["precommit"],
        "replay_evidence_receipt": bundle["replay"],
        "trusted_precommitment_public_keys": bundle["pre_keys"],
    }


def _resign_replay(bundle: dict[str, object]) -> None:
    body = bundle["replay"]["body"]
    bundle["replay"] = _sign(
        bundle["replay_private"],
        "replay-key",
        cap.REPLAY_EVIDENCE_SIGNATURE_KIND,
        body,
    )


def _resign_precommit(bundle: dict[str, object]) -> None:
    body = bundle["precommit"]["body"]
    bundle["precommit"] = _sign(
        bundle["pre_private"],
        "pre-key",
        cap.PRECOMMITMENT_SIGNATURE_KIND,
        body,
    )
    replay_body = bundle["replay"]["body"]
    replay_body["precommitment_sha256"] = cap.canonical_json_sha256(body)
    replay_body["precommitment_envelope_sha256"] = cap.canonical_json_sha256(
        bundle["precommit"]
    )
    _resign_replay(bundle)


def test_legacy_inputs_are_blocked_without_being_consumed() -> None:
    artifact = cap.calibrate_total_stop_risk_cap(
        candidates=Poison(),
        base_policies=Poison(),
        evaluation_start=START,
        evaluation_end=END,
        lineage={"diagnostic": "legacy"},
        constraints=_constraints(),
    )
    assert artifact["selection"]["status"] == cap.BLOCKED_NO_NUMERIC_AUTHORITY
    assert artifact["selection"]["selected_capital_envelope"] is None
    assert "CAP_PRECOMMITMENT_MISSING" in artifact["selection"][
        "identification_failures"
    ]


def test_signed_complete_nested_process_selects_evidence_winner() -> None:
    bundle = _bundle()
    artifact = _calibrate(bundle)
    assert artifact["selection"]["status"] == (
        cap.EVIDENCE_SELECTED_PENDING_AUTHORIZATION
    )
    assert artifact["selection"]["selected_policy_point_id"] == "high"
    assert artifact["selection"]["selected_capital_envelope"] == {
        "max_total_stop_risk_pct": 20.0,
        "max_factor_stop_risk_pct": 12.0,
        "max_same_symbol_stop_risk_pct": 8.0,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 1.5,
    }
    assert [row["inner_selected_policy_id"] for row in artifact["cap_records"]] == [
        "high"
    ] * 4
    assert artifact["outer_process"]["outer_anchor_count"] == 4
    default_errors = cap.validate_cap_calibration_artifact(
        artifact,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
        expected_constraints=bundle["constraints"],
    )
    assert "CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_REQUIRED" in default_errors
    assert cap.validate_cap_calibration_artifact(
        artifact,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
        expected_constraints=bundle["constraints"],
        require_external_authorization=False,
        **_authorization_evidence(bundle),
    ) == []


def test_outer_outcomes_cannot_select_the_anchor_policy() -> None:
    baseline = _calibrate(_bundle(outer_returns={"low": 0.020, "high": 0.010}))
    reversed_outcomes = _calibrate(
        _bundle(outer_returns={"low": 0.010, "high": 0.020})
    )
    assert [row["inner_selected_policy_id"] for row in baseline["cap_records"]] == [
        "high"
    ] * 4
    assert [
        row["inner_selected_policy_id"] for row in reversed_outcomes["cap_records"]
    ] == ["high"] * 4


def test_missing_or_extra_attempt_blocks_the_whole_ledger() -> None:
    bundle = _bundle()
    bundle["replay"]["body"]["attempts"].pop()
    _resign_replay(bundle)
    artifact = _calibrate(bundle)
    assert artifact["selection"]["status"] == cap.BLOCKED_NO_NUMERIC_AUTHORITY
    assert "CAP_ATTEMPT_LEDGER_INCOMPLETE" in artifact["selection"][
        "identification_failures"
    ]


def test_technical_attempt_blocks_instead_of_becoming_no_trade() -> None:
    bundle = _bundle()
    attempt = bundle["replay"]["body"]["attempts"][0]
    attempt["status"] = "TECHNICAL_FAILURE"
    attempt["failure_code"] = "REPLAY_OOM"
    attempt["daily_observations"] = []
    attempt["accepted_positions"] = 0
    unsigned = dict(attempt)
    unsigned.pop("attempt_sha256")
    attempt["attempt_sha256"] = cap.canonical_json_sha256(unsigned)
    _resign_replay(bundle)
    artifact = _calibrate(bundle)
    assert any(
        "CAP_ATTEMPT_NOT_COMPLETE:REPLAY_OOM" in reason
        for reason in artifact["selection"]["identification_failures"]
    )
    assert artifact["selection"]["selected_policy_point_id"] is None


def test_unselected_outer_attempt_is_still_fully_validated() -> None:
    bundle = _bundle()
    unselected = next(
        item
        for item in bundle["replay"]["body"]["attempts"]
        if item["anchor_id"] == "A1"
        and item["phase"] == "OUTER_EVALUATION"
        and item["policy_id"] == "low"
    )
    unselected["daily_observations"][0]["margin_level_pct"] = 999.0
    unsigned = dict(unselected)
    unsigned.pop("attempt_sha256")
    unselected["attempt_sha256"] = cap.canonical_json_sha256(unsigned)
    _resign_replay(bundle)
    artifact = _calibrate(bundle)
    assert "CAP_DAILY_MARGIN_LEVEL_RECONCILIATION_FAILED" in artifact["selection"][
        "identification_failures"
    ]


def test_anchor_overlap_and_current_outer_reuse_are_rejected() -> None:
    bundle = _bundle()
    first = bundle["precommit"]["body"]["anchor_plan"][0]
    first["inner_window"]["end_date"] = (
        date.fromisoformat(first["outer_window"]["start_date"]) + timedelta(days=1)
    ).isoformat()
    _resign_precommit(bundle)
    artifact = _calibrate(bundle)
    assert "CAP_PRECOMMITMENT_ANCHOR_WINDOW_LEAKAGE" in artifact["selection"][
        "identification_failures"
    ]

    bundle = _bundle()
    last_outer = bundle["precommit"]["body"]["anchor_plan"][-1]["outer_window"]
    bundle["precommit"]["body"]["current_selection_window"] = deepcopy(last_outer)
    _resign_precommit(bundle)
    artifact = _calibrate(bundle)
    assert "CAP_CURRENT_SELECTION_REUSES_OUTER_EVIDENCE" in artifact["selection"][
        "identification_failures"
    ]


def test_current_book_refit_or_native_lineage_mismatch_blocks() -> None:
    bundle = _bundle()
    bundle["lineage"]["refit_execution_sha256"] = _sha("forged")
    artifact = _calibrate(bundle)
    assert "CAP_CURRENT_LINEAGE_MISMATCH" in artifact["selection"][
        "identification_failures"
    ]


def test_signature_tamper_and_constraint_rewrite_block() -> None:
    bundle = _bundle()
    bundle["replay"]["body"]["attempts"][0]["policy_id"] = "tampered"
    artifact = _calibrate(bundle)
    assert "CAP_REPLAY_EVIDENCE_SIGNATURE_INVALID" in artifact["selection"][
        "identification_failures"
    ]

    bundle = _bundle()
    bundle["precommit"]["body"]["constraints_sha256"] = "0" * 64
    _resign_precommit(bundle)
    artifact = _calibrate(bundle)
    assert "CAP_PRECOMMITMENT_CONSTRAINTS_MISMATCH" in artifact["selection"][
        "identification_failures"
    ]


def test_untested_high_cap_cannot_be_selected() -> None:
    artifact = _calibrate(_bundle(utilization={"low": 0.90, "high": 0.20}))
    assert artifact["selection"]["selected_policy_point_id"] == "low"
    assert artifact["selection"]["selected_max_total_stop_risk_pct"] == 10.0
    high = next(row for row in artifact["policy_records"] if row["policy_id"] == "high")
    assert high["near_cap_observations"]["total"] == 0


def test_joint_gap_and_margin_reconciliation_fail_closed() -> None:
    bundle = _bundle()
    current_high = next(
        item
        for item in bundle["replay"]["body"]["attempts"]
        if item["anchor_id"] == "CURRENT" and item["policy_id"] == "high"
    )
    for row in current_high["daily_observations"]:
        row["joint_gap_loss_pct"] = 60.0
    unsigned = dict(current_high)
    unsigned.pop("attempt_sha256")
    current_high["attempt_sha256"] = cap.canonical_json_sha256(unsigned)
    _resign_replay(bundle)
    artifact = _calibrate(bundle)
    assert artifact["selection"]["selected_policy_point_id"] == "low"

    bundle = _bundle()
    attempt = bundle["replay"]["body"]["attempts"][0]
    attempt["daily_observations"][0]["margin_level_pct"] = 999.0
    unsigned = dict(attempt)
    unsigned.pop("attempt_sha256")
    attempt["attempt_sha256"] = cap.canonical_json_sha256(unsigned)
    _resign_replay(bundle)
    artifact = _calibrate(bundle)
    assert "CAP_DAILY_MARGIN_LEVEL_RECONCILIATION_FAILED" in artifact["selection"][
        "identification_failures"
    ]


def test_numeric_selection_requires_fresh_external_authorization() -> None:
    bundle = _bundle()
    pending = _calibrate(bundle)
    errors = cap.validate_cap_calibration_artifact(
        pending,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=20.0,
        validation_time_utc="2026-08-01T00:00:00Z",
    )
    assert "CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_REQUIRED" in errors

    assert cap.validate_cap_calibration_artifact(
        pending,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=20.0,
        expected_selected_policy_point_id="high",
        expected_constraints=bundle["constraints"],
        trusted_replay_public_keys=bundle["replay_keys"],
        precommitment_receipt=bundle["precommit"],
        replay_evidence_receipt=bundle["replay"],
        trusted_precommitment_public_keys=bundle["pre_keys"],
        require_external_authorization=False,
    ) == []

    missing_reverification = cap.validate_cap_calibration_artifact(
        pending,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=20.0,
        require_external_authorization=False,
    )
    assert "CAP_CALIBRATION_EVIDENCE_REVERIFICATION_FAILED" in (
        missing_reverification
    )

    private, public = _private_key()
    body = cap.build_capital_envelope_authorization_body(
        pending,
        authority_key_id="risk-committee",
        selected_policy_point_id="high",
        selected_cap_pct=20.0,
        issued_at_utc="2026-07-15T00:00:00Z",
        expires_at_utc="2026-09-01T00:00:00Z",
        expected_deployment_lineage=bundle["lineage"],
        expected_constraints=bundle["constraints"],
        **_authorization_evidence(bundle),
    )
    receipt = _sign(
        private,
        "risk-committee",
        cap.AUTHORIZATION_SIGNATURE_KIND,
        body,
    )
    authorized = cap.attach_capital_envelope_authorization(
        pending,
        receipt,
        trusted_public_keys={"risk-committee": public},
        validation_time_utc="2026-08-01T00:00:00Z",
        expected_deployment_lineage=bundle["lineage"],
        expected_constraints=bundle["constraints"],
        **_authorization_evidence(bundle),
    )
    assert authorized["selection"]["status"] == cap.AUTHORIZED_NUMERIC_ENVELOPE
    assert cap.validate_cap_calibration_artifact(
        authorized,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=20.0,
        expected_selected_policy_point_id="high",
        expected_constraints=bundle["constraints"],
        trusted_authority_public_keys={"risk-committee": public},
        validation_time_utc="2026-08-01T00:00:00Z",
    ) == []
    assert cap.validate_cap_calibration_artifact(
        authorized,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
        trusted_authority_public_keys={"risk-committee": public},
        validation_time_utc="2026-08-01T00:00:00Z",
    ) == []
    expired = cap.validate_cap_calibration_artifact(
        authorized,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
        trusted_authority_public_keys={"risk-committee": public},
        validation_time_utc="2026-10-01T00:00:00Z",
    )
    assert "CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_INVALID" in expired


def test_none_cap_cannot_bypass_auth_or_raw_reverification() -> None:
    bundle = _bundle()
    pending = _calibrate(bundle)
    forged = deepcopy(pending)
    forged.pop("content_seal")
    forged["selection"]["selected_max_total_stop_risk_pct"] = 90.0
    forged["selection"]["selected_capital_envelope"][
        "max_total_stop_risk_pct"
    ] = 90.0
    forged["decision_sha256"] = cap.canonical_json_sha256(
        cap._decision_material(forged)
    )
    forged = cap._seal_artifact(forged)

    funded = cap.validate_cap_calibration_artifact(
        forged,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
    )
    assert "CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_REQUIRED" in funded
    demo = cap.validate_cap_calibration_artifact(
        forged,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
        require_external_authorization=False,
        **_authorization_evidence(bundle),
    )
    assert "CAP_CALIBRATION_EVIDENCE_REVERIFICATION_FAILED" in demo


def test_authorization_tamper_and_expiry_are_rejected() -> None:
    bundle = _bundle()
    pending = _calibrate(bundle)
    private, public = _private_key()
    body = cap.build_capital_envelope_authorization_body(
        pending,
        authority_key_id="risk",
        issued_at_utc="2026-07-15T00:00:00Z",
        expires_at_utc="2026-07-16T00:00:00Z",
        **_authorization_evidence(bundle),
    )
    receipt = _sign(private, "risk", cap.AUTHORIZATION_SIGNATURE_KIND, body)
    with pytest.raises(ValueError, match="NOT_CURRENT"):
        cap.attach_capital_envelope_authorization(
            pending,
            receipt,
            trusted_public_keys={"risk": public},
            validation_time_utc="2026-07-17T00:00:00Z",
            **_authorization_evidence(bundle),
        )
    receipt["body"]["selected_max_total_stop_risk_pct"] = 90.0
    with pytest.raises(cap._Blocked, match="SIGNATURE_INVALID"):
        cap.attach_capital_envelope_authorization(
            pending,
            receipt,
            trusted_public_keys={"risk": public},
            validation_time_utc="2026-07-15T12:00:00Z",
            **_authorization_evidence(bundle),
        )


def test_human_authorization_builder_recomputes_signed_evidence() -> None:
    bundle = _bundle()
    pending = _calibrate(bundle)
    forged = deepcopy(pending)
    forged.pop("content_seal")
    forged["selection"]["selected_max_total_stop_risk_pct"] = 90.0
    forged["selection"]["selected_capital_envelope"][
        "max_total_stop_risk_pct"
    ] = 90.0
    forged["decision_sha256"] = cap.canonical_json_sha256(
        cap._decision_material(forged)
    )
    forged = cap._seal_artifact(forged)

    with pytest.raises(ValueError, match="EVIDENCE_REVERIFICATION_FAILED"):
        cap.build_capital_envelope_authorization_body(
            forged,
            authority_key_id="risk",
            issued_at_utc="2026-07-15T00:00:00Z",
            expires_at_utc="2026-07-16T00:00:00Z",
            **_authorization_evidence(bundle),
        )


def test_unused_legacy_constraints_are_not_claimed() -> None:
    fields = set(cap.CapCalibrationConstraints.__dataclass_fields__)
    assert not fields.intersection(
        {
            "minimum_profit_factor",
            "minimum_return_pct",
            "maximum_bootstrap_ruin_upper_95pct",
            "maximum_bootstrap_p95_drawdown_pct",
            "minimum_mtm_coverage_pct",
            "maximum_gross_notional_to_equity_pct",
            "maximum_abs_net_notional_to_equity_pct",
            "minimum_exposure_duration_days",
            "initial_stop_gap_multiple",
            "maximum_gap_stress_loss_pct",
        }
    )


def test_artifact_tamper_and_immutable_writer(tmp_path: Path) -> None:
    bundle = _bundle()
    artifact = _calibrate(bundle)
    path = cap.write_immutable_cap_calibration(tmp_path, artifact)
    assert path == cap.write_immutable_cap_calibration(tmp_path, artifact)
    hostile = deepcopy(artifact)
    hostile["selection"]["selected_max_total_stop_risk_pct"] = 90.0
    assert cap.validate_cap_calibration_artifact(
        hostile,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=None,
    ) == ["CAP_CALIBRATION_ARTIFACT_SEAL_OR_SCHEMA_INVALID"]
    with pytest.raises(ValueError, match="ARTIFACT_INVALID"):
        cap.write_immutable_cap_calibration(tmp_path, hostile)

    invalid_cap = cap.validate_cap_calibration_artifact(
        artifact,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct="20.0",
    )
    assert "CAP_CALIBRATION_EXPECTED_CAP_INVALID" in invalid_cap


def test_authorization_binds_window_and_authority_scope() -> None:
    bundle = _bundle()
    pending = _calibrate(bundle)
    private, public = _private_key()
    body = cap.build_capital_envelope_authorization_body(
        pending,
        authority_key_id="risk",
        issued_at_utc="2026-07-15T00:00:00Z",
        expires_at_utc="2026-07-16T00:00:00Z",
        **_authorization_evidence(bundle),
    )
    receipt = _sign(private, "risk", cap.AUTHORIZATION_SIGNATURE_KIND, body)
    authorized = cap.attach_capital_envelope_authorization(
        pending,
        receipt,
        trusted_public_keys={"risk": public},
        validation_time_utc="2026-07-15T12:00:00Z",
        **_authorization_evidence(bundle),
    )

    hostile = deepcopy(authorized)
    hostile.pop("content_seal")
    hostile["window"]["evaluation_start"] = "2019-01-01T00:00:00Z"
    hostile = cap._seal_artifact(hostile)
    errors = cap.validate_cap_calibration_artifact(
        hostile,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=20.0,
        trusted_authority_public_keys={"risk": public},
        validation_time_utc="2026-07-15T12:00:00Z",
    )
    assert "CAP_CALIBRATION_DECISION_SHA256_INVALID" in errors
    assert "CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_INVALID" in errors

    hostile = deepcopy(authorized)
    hostile.pop("content_seal")
    hostile["authority_scope"] = cap.BLOCKED_NO_NUMERIC_AUTHORITY
    hostile = cap._seal_artifact(hostile)
    assert "CAP_CALIBRATION_AUTHORITY_SCOPE_MISMATCH" in (
        cap.validate_cap_calibration_artifact(
            hostile,
            expected_lineage=bundle["lineage"],
            expected_selected_cap_pct=None,
        )
    )

    hostile = deepcopy(pending)
    hostile.pop("content_seal")
    hostile["profitability_guarantee"] = True
    hostile["decision_sha256"] = cap.canonical_json_sha256(
        cap._decision_material(hostile)
    )
    hostile = cap._seal_artifact(hostile)
    assert "CAP_CALIBRATION_PROFITABILITY_GUARANTEE_INVALID" in (
        cap.validate_cap_calibration_artifact(
            hostile,
            expected_lineage=bundle["lineage"],
            expected_selected_cap_pct=None,
        )
    )


def test_content_addressed_shards_preserve_full_evidence_outside_artifact(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    replay_body = bundle["replay"]["body"]
    attempts = replay_body.pop("attempts")
    replay_body.update(
        cap.write_capital_replay_attempt_shards(
            tmp_path,
            attempts,
            shard_attempt_limit=2,
        )
    )
    _resign_replay(bundle)
    artifact = _calibrate(bundle, replay_root=tmp_path)
    assert artifact["selection"]["selected_policy_point_id"] == "high"
    shard_bytes = sum(
        path.stat().st_size for path in (tmp_path / "attempt_shards").iterdir()
    )
    artifact_bytes = len(cap._canonical_json_bytes(artifact))
    assert artifact_bytes < cap.MAX_ARTIFACT_BYTES
    assert shard_bytes > artifact_bytes
    assert "attempts" not in artifact["evidence_receipts"]
    assert cap.validate_cap_calibration_artifact(
        artifact,
        expected_lineage=bundle["lineage"],
        expected_selected_cap_pct=20.0,
        replay_evidence_root=tmp_path,
        trusted_replay_public_keys=bundle["replay_keys"],
        precommitment_receipt=bundle["precommit"],
        replay_evidence_receipt=bundle["replay"],
        trusted_precommitment_public_keys=bundle["pre_keys"],
        require_external_authorization=False,
    ) == []


def test_shard_tamper_blocks_without_falling_back_to_partial_evidence(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    replay_body = bundle["replay"]["body"]
    attempts = replay_body.pop("attempts")
    replay_body.update(
        cap.write_capital_replay_attempt_shards(
            tmp_path,
            attempts,
            shard_attempt_limit=2,
        )
    )
    _resign_replay(bundle)
    first = next((tmp_path / "attempt_shards").iterdir())
    payload = first.read_bytes()
    first.write_bytes(payload[:-2] + b" \n")
    artifact = _calibrate(bundle, replay_root=tmp_path)
    assert artifact["selection"]["status"] == cap.BLOCKED_NO_NUMERIC_AUTHORITY
    assert "CAP_REPLAY_SHARD_SHA256_MISMATCH" in artifact["selection"][
        "identification_failures"
    ]


def test_shard_writer_bounds_chunks_and_existing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cap, "MAX_REPLAY_SHARD_BYTES", 1024)
    tiny = {"attempt_id": "x", "daily_observations": []}
    shard = {
        "schema_version": cap.CAP_REPLAY_SHARD_SCHEMA_VERSION,
        "shard_index": 0,
        "attempts": [tiny],
    }
    encoded = cap._canonical_json_bytes(shard) + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    shard_dir = tmp_path / "attempt_shards"
    shard_dir.mkdir()
    target = shard_dir / f"attempts_0000_{digest}.json"
    with target.open("wb") as handle:
        handle.truncate(1025)
    with pytest.raises(ValueError, match="SIZE_INVALID"):
        cap.write_capital_replay_attempt_shards(tmp_path, [tiny])

    with pytest.raises(ValueError, match="shard byte bound"):
        cap.write_capital_replay_attempt_shards(
            tmp_path / "second",
            [{"attempt_id": "y", "padding": "x" * 2048}],
        )


def test_cap_neutral_hashes_exclude_only_calibrated_axes() -> None:
    first = {
        "pipeline": {
            "max_total_open_risk_pct": 3.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_combined_risk_pct": 1.0,
            "unrelated": 7,
        }
    }
    second = deepcopy(first)
    second["pipeline"].update(
        {
            "max_total_open_risk_pct": 90.0,
            "max_factor_group_open_risk_pct": 50.0,
            "max_combined_risk_pct": 33.0,
        }
    )
    assert cap.cap_neutral_config_sha256(first) == cap.cap_neutral_config_sha256(
        second
    )
    second["pipeline"]["unrelated"] = 8
    assert cap.cap_neutral_config_sha256(first) != cap.cap_neutral_config_sha256(
        second
    )


def test_method_contract_is_research_bound_not_a_profit_guarantee() -> None:
    artifact = _calibrate(_bundle())
    assert artifact["profitability_guarantee"] is False
    assert artifact["places_trades"] is False
    assert artifact["content_seal_is_authority"] is False
    assert artifact["method_contract"]["inner_selection_data"] == (
        "INNER_SELECTION_ONLY"
    )
    assert set(artifact["method_references"]) == {
        "hac",
        "nested_selection",
        "multiple_testing",
        "mt5_margin",
        "mt5_order_margin",
    }
