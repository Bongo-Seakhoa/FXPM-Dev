from __future__ import annotations

import base64
import json
import os
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import pm_cap_calibration as cap
import pm_capital_authority_factory as factory
import pm_book_multiplicity as book
import pm_current_refit_execution as current_execution
import pm_portfolio_replay as replay
import pm_production_authority_coordinator as coordinator
from pm_production_authority_coordinator import CapitalAuthorityInputs
from tools.provision_capital_authority_keys import (
    KeyProvisioningError,
    provision_key_pair,
)


def _sha(label: str) -> str:
    return cap.canonical_json_sha256({"label": label})


def _points() -> list[dict[str, float]]:
    result = []
    for total in cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT:
        factor = min(total, 2.0)
        symbol = min(factor, 1.0)
        result.append(
            {
                "max_total_stop_risk_pct": total,
                "max_factor_stop_risk_pct": factor,
                "max_same_symbol_stop_risk_pct": symbol,
                "max_margin_utilization_pct": 50.0,
                "min_margin_level_pct": 200.0,
                "min_free_margin_pct": 50.0,
                "gap_stress_multiple": 2.5,
            }
        )
    return result


def _constraints() -> cap.CapCalibrationConstraints:
    return cap.CapCalibrationConstraints(
        minimum_outer_anchors=2,
        maximum_outer_anchors=2,
        maximum_candidate_policies=16,
        minimum_inner_calendar_days=1,
        minimum_outer_calendar_days=1,
        minimum_current_calendar_days=1,
    )


def _config(*, funded: bool = False) -> dict:
    scopes = [factory.DEMO_PROBATION_SCOPE]
    if funded:
        scopes.append(factory.FUNDED_LIVE_SCOPE)
    return {
        "pipeline": {
            "production_capital_authority": {
                "schema_version": factory.CAPITAL_AUTHORITY_POLICY_SCHEMA,
                "constraints": asdict(_constraints()),
                "candidate_policy_points": _points(),
                "permitted_deployment_scopes": scopes,
                "authorization_max_ttl_seconds": 86_400,
            }
        }
    }


def _raw_key(private: Ed25519PrivateKey) -> tuple[bytes, bytes]:
    return (
        private.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        ),
        private.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        ),
    )


def _write_key_pair(
    root: Path, name: str
) -> tuple[Ed25519PrivateKey, factory.SigningKeySource]:
    private = Ed25519PrivateKey.generate()
    private_bytes, public_bytes = _raw_key(private)
    private_path = root / f"{name}.private"
    public_path = root / f"{name}.public"
    private_path.write_bytes(private_bytes)
    public_path.write_bytes(public_bytes)
    if os.name != "nt":
        private_path.chmod(0o600)
    return private, factory.SigningKeySource(
        private_key=factory.KeyMaterialSource(path=private_path),
        public_key=factory.KeyMaterialSource(path=public_path),
    )


def _contract() -> dict[str, str]:
    return {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(_current_cost()),
        "native_source_contract_sha256": cap.canonical_json_sha256(_current_native()),
        "replay_contract_sha256": coordinator._capital_replay_contract_sha256(),
        "margin_contract_sha256": _sha("margin"),
    }


def _current_cost() -> dict:
    return {"cost": "exact"}


def _current_native() -> dict:
    return {
        "native_activation_id": "activation-CURRENT",
        "native_activation_manifest_sha256": _sha("manifest:CURRENT"),
        "native_activation_roster_sha256": _sha("roster:CURRENT"),
        "native_bars_sha256_by_source": {
            "EURUSD|H1": _sha("bars:CURRENT|CURRENT_SELECTION")
        },
        "native_sidecar_sha256_by_source": {
            "EURUSD|H1": _sha("sidecar:CURRENT|CURRENT_SELECTION")
        },
    }


def _precommit_body() -> dict:
    return {
        "schema_version": cap.CAP_PRECOMMIT_SCHEMA_VERSION,
        "precommitment_id": "test-precommitment",
        "created_at_utc": "2025-01-01T00:00:00Z",
        "evidence_access_not_before_utc": "2025-01-02T00:00:00Z",
        "candidate_policies": cap.build_frozen_production_candidate_roster(_points()),
        "anchor_plan": [
            {
                "anchor_id": "A1",
                "inner_window": {
                    "start_date": "2020-01-01",
                    "end_date": "2020-01-03",
                },
                "outer_window": {
                    "start_date": "2020-01-03",
                    "end_date": "2020-01-05",
                },
            },
            {
                "anchor_id": "A2",
                "inner_window": {
                    "start_date": "2020-01-05",
                    "end_date": "2020-01-07",
                },
                "outer_window": {
                    "start_date": "2020-01-07",
                    "end_date": "2020-01-09",
                },
            },
        ],
        "current_selection_window": {
            "start_date": "2020-01-09",
            "end_date": "2020-01-11",
        },
        "selection_rule": cap._FROZEN_SELECTION_RULE,
        "constraints_sha256": cap.canonical_json_sha256(asdict(_constraints())),
        "contract_lineage": _contract(),
    }


def _write_precommitment(root: Path) -> tuple[Path, factory.KeyMaterialSource]:
    private = Ed25519PrivateKey.generate()
    _, public_bytes = _raw_key(private)
    key_id = factory.canonical_ed25519_key_id(public_bytes)
    body = _precommit_body()
    signature = private.sign(
        cap.capital_authority_signature_payload(
            cap.PRECOMMITMENT_SIGNATURE_KIND, key_id, body
        )
    )
    receipt = {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": key_id,
        "body": body,
        "signature_base64": base64.b64encode(signature).decode("ascii"),
    }
    receipt_path = root / "precommitment.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    public_path = root / "precommitment.public"
    public_path.write_bytes(public_bytes)
    return receipt_path, factory.KeyMaterialSource(path=public_path)


def _phase_ids() -> tuple[str, ...]:
    return (
        "A1|INNER_SELECTION",
        "A1|OUTER_EVALUATION",
        "A2|INNER_SELECTION",
        "A2|OUTER_EVALUATION",
        "CURRENT|CURRENT_SELECTION",
    )


def _candidate(phase_id: str) -> replay.CandidateTrade:
    entry = datetime(2020, 1, 1, 1)
    return replay.CandidateTrade(
        candidate_id=f"{phase_id}|candidate",
        cell_id="EURUSD|H1|TREND",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="TestStrategy",
        promotion_status=(
            "PROCESS_VALIDATED_CURRENT_REFIT"
            if phase_id == "CURRENT|CURRENT_SELECTION"
            else "PROMOTED"
        ),
        asset_class="fx",
        risk_groups=(),
        entry_time=entry.isoformat(),
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=100.0,
        volume_min=0.01,
        volume_max=1.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(replay.ExitLeg((entry + timedelta(days=1)).isoformat(), 1.0, 10.0),),
        source_schema=replay.PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
        margin_model="test-margin-v1",
    ).normalized()


def _lineage(phase_id: str) -> dict:
    stable = "CURRENT" if phase_id.startswith("CURRENT|") else phase_id[:2]
    return {
        **_contract(),
        "book_role": (
            "PROCESS_VALIDATED_CURRENT_REFIT"
            if phase_id == "CURRENT|CURRENT_SELECTION"
            else "ANCHOR_LOCAL_REFIT"
        ),
        "book_decision_sha256": _sha(f"decision:{stable}"),
        "book_generation_receipt_sha256": _sha(f"generation:{stable}"),
        "refit_receipt_sha256": _sha(f"refit:{stable}"),
        "refit_execution_sha256": _sha(f"execution:{stable}"),
        "output_book_sha256": _sha(f"book:{stable}"),
        "winner_roster_sha256": _sha(f"winners:{stable}"),
        "native_activation_id": f"activation-{stable}",
        "native_activation_manifest_sha256": _sha(f"manifest:{stable}"),
        "native_activation_roster_sha256": _sha(f"roster:{stable}"),
        "native_bars_sha256_by_source": {"EURUSD|H1": _sha(f"bars:{phase_id}")},
        "native_sidecar_sha256_by_source": {"EURUSD|H1": _sha(f"sidecar:{phase_id}")},
        "margin_terms_sha256": _sha(f"margin-terms:{stable}"),
        "broker_terms_sha256": _sha(f"broker-terms:{stable}"),
    }


def _dummy_output() -> dict:
    return {
        "input_receipt": {},
        "summary": {},
        "policy": {},
        "closed_positions": [],
        "blocked_positions": [],
        "path": [],
    }


def _authority_request() -> coordinator.CapitalAuthorityRequest:
    historical_phases = tuple(
        phase_id for phase_id in _phase_ids() if phase_id != "CURRENT|CURRENT_SELECTION"
    )
    historical = coordinator._HistoricalState(
        run_receipt=SimpleNamespace(receipt_sha256=_sha("historical-run")),
        run_body={
            "anchors": [
                {
                    "anchor_id": anchor_id,
                    "book_decision_sha256": _sha(f"decision:{anchor_id}"),
                    "book_generation_sha256": _sha(f"generation:{anchor_id}"),
                }
                for anchor_id in ("A1", "A2")
            ]
        },
        process_decision={"decision": "OFFLINE_PROCESS_VALIDATION_PASS"},
        precommit_payload={},
        evidence_payload={},
        reconciliation_payload={},
        historical_book_decision={"receipt_sha256": _sha("decision:A2")},
        capital_bootstrap_identity_sha256=_sha("bootstrap"),
        capital_candidates_by_phase={
            phase_id: (_candidate(phase_id),) for phase_id in historical_phases
        },
        capital_lineage_by_phase={
            phase_id: _lineage(phase_id) for phase_id in historical_phases
        },
    )
    current_phase = "CURRENT|CURRENT_SELECTION"
    current_lineage = {
        "historical_process_decision_sha256": _sha("historical-process"),
        "accepted_book_generation_receipt_sha256": _sha("generation:CURRENT"),
        "current_refit_receipt_sha256": _sha("refit:CURRENT"),
        "current_refit_execution_sha256": _sha("execution:CURRENT"),
        "current_refit_cell_count": 1,
        "current_selected_winner_roster_sha256": _sha("winners:CURRENT"),
        "current_refit_output_book_sha256": _sha("book:CURRENT"),
    }
    ledger = {"EURUSD": {"symbol": "EURUSD", "regime_configs": {}}}
    product = coordinator.CurrentRefitProduct(
        current_book_decision={"receipt_sha256": _sha("decision:CURRENT")},
        accepted_book_generation_receipt={},
        current_refit_receipt={},
        raw_manifest_path=Path("raw.json"),
        artifact_root=Path("artifacts"),
        detector_ledger_records=(),
        replay_candidates=(_candidate(current_phase),),
        replayed_cell_ids=("EURUSD|H1|TREND",),
        portfolio_evidence_producer=lambda _request: {},
        staged_ledger_path=Path("current-ledger.json"),
        staged_ledger=ledger,
        staged_ledger_content_sha256=_sha("ledger-content"),
        physical_ledger_sha256=replay.canonical_json_sha256(ledger),
    )
    current = coordinator._CurrentState(
        product=product,
        roster={"current_refit_lineage": current_lineage},
        candidates=(_candidate(current_phase),),
        portfolio_evidence={},
        portfolio_lineage={},
        cost_contract=_current_cost(),
        policies={},
        validation_policy={},
        native_identity=_current_native(),
        margin_contract_sha256=_contract()["margin_contract_sha256"],
        margin_terms_sha256=_sha("margin-terms:CURRENT"),
        broker_terms_sha256=_sha("broker-terms:CURRENT"),
    )
    return coordinator.CapitalAuthorityRequest(
        _historical_state=historical,
        _current_state=current,
        validation_time_utc="2026-07-14T00:00:01Z",
        authorization_scope="DEMO_PROBATION",
    )


def _factory_bundle(tmp_path: Path, *, funded: bool = False) -> dict:
    receipt_path, precommit_public = _write_precommitment(tmp_path)
    _, replay_source = _write_key_pair(tmp_path, "replay")
    _, authorization_source = _write_key_pair(tmp_path, "authorization")
    calls: list[dict] = []

    def loader(request: dict) -> dict:
        calls.append(request)
        return _dummy_output()

    return {
        "kwargs": {
            "authority_request": _authority_request(),
            "config_payload": _config(funded=funded),
            "output_root": tmp_path / "capital-output",
            "precommitment_receipt_path": receipt_path,
            "precommitment_public_key_source": precommit_public,
            "replay_signing_key_source": replay_source,
            "authorization_signing_key_source": authorization_source,
            "margin_terms_sha256": _sha("margin-terms:CURRENT"),
            "broker_terms_sha256": _sha("broker-terms:CURRENT"),
            "replay_output_loader": loader,
            "generated_at_utc": "2026-07-14T00:00:00Z",
            "authorization_issued_at_utc": "2026-07-14T00:00:00Z",
            "authorization_expires_at_utc": "2026-07-15T00:00:00Z",
        },
        "calls": calls,
    }


def test_factory_builds_exact_keys_rosters_transport_and_signers(
    tmp_path: Path,
) -> None:
    bundle = _factory_bundle(tmp_path)
    inputs = factory.build_capital_authority_inputs(**bundle["kwargs"])

    assert type(inputs) is CapitalAuthorityInputs
    assert inputs.output_root.is_absolute()
    assert inputs.constraints == _constraints()
    assert inputs.authority_key_id.startswith("ed25519-sha256:")
    assert set(inputs.trusted_authority_public_keys) == {inputs.authority_key_id}
    replay_key_id = next(iter(inputs.trusted_replay_public_keys))
    assert replay_key_id != inputs.authority_key_id
    assert "private" not in repr(inputs.replay_evidence_signer).lower()
    current_phase = "CURRENT|CURRENT_SELECTION"
    current_candidate = bundle["kwargs"]["authority_request"]._current_state.candidates
    assert inputs.frozen_candidate_rosters_by_phase[current_phase] == (
        replay.portfolio_replay_candidate_roster_receipt(current_candidate)
    )
    assert (
        inputs.lineage_by_phase[current_phase]["book_decision_sha256"]
        == (
            bundle["kwargs"][
                "authority_request"
            ]._current_state.product.current_book_decision["receipt_sha256"]
        )
    )

    replay_body = {
        "schema_version": cap.CAP_REPLAY_SCHEMA_VERSION,
        "precommitment_sha256": cap.canonical_json_sha256(
            inputs.precommitment_receipt["body"]
        ),
        "precommitment_envelope_sha256": cap.canonical_json_sha256(
            inputs.precommitment_receipt
        ),
        "generated_at_utc": inputs.generated_at_utc,
        "attempts": [],
    }
    signed_replay = inputs.replay_evidence_signer(replay_body)
    verified_replay, _ = cap._verified_envelope(
        signed_replay,
        kind=cap.REPLAY_EVIDENCE_SIGNATURE_KIND,
        trusted_public_keys=inputs.trusted_replay_public_keys,
        missing_reason="missing",
    )
    assert verified_replay == replay_body
    with pytest.raises(ValueError, match="SIGNATURE_INVALID"):
        cap._verified_envelope(
            signed_replay,
            kind=cap.AUTHORIZATION_SIGNATURE_KIND,
            trusted_public_keys={
                replay_key_id: inputs.trusted_replay_public_keys[replay_key_id]
            },
            missing_reason="missing",
        )

    authorization_body = {
        "schema_version": cap.CAP_AUTHORIZATION_SCHEMA_VERSION,
        "authority_key_id": inputs.authority_key_id,
        "pending_artifact_content_sha256": _sha("pending"),
        "calibration_decision_sha256": _sha("decision"),
        "selected_policy_point_id": "selected",
        "selected_max_total_stop_risk_pct": 3.0,
        "selected_capital_envelope": {},
        "source_lineage_sha256": _sha("lineage"),
        "constraints_sha256": cap.canonical_json_sha256(asdict(_constraints())),
        "issued_at_utc": inputs.authorization_issued_at_utc,
        "expires_at_utc": inputs.authorization_expires_at_utc,
    }
    signed_authorization = inputs.authorization_signer(authorization_body)
    verified_authorization, _ = cap._verified_envelope(
        signed_authorization,
        kind=cap.AUTHORIZATION_SIGNATURE_KIND,
        trusted_public_keys=inputs.trusted_authority_public_keys,
        missing_reason="missing",
    )
    assert verified_authorization == authorization_body

    first_policy = min(
        inputs.precommitment_receipt["body"]["candidate_policies"],
        key=lambda row: row["policy_id"],
    )
    request = {
        "attempt_id": f"A1|INNER_SELECTION|{first_policy['policy_id']}",
        "anchor_id": "A1",
        "phase": "INNER_SELECTION",
        "window": {"start_date": "2020-01-01", "end_date": "2020-01-03"},
        "policy": first_policy,
        "frozen_candidate_roster": inputs.frozen_candidate_rosters_by_phase[
            "A1|INNER_SELECTION"
        ],
    }
    changed = json.loads(json.dumps(request))
    changed["window"]["end_date"] = "2020-01-04"
    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="CAPITAL_REPLAY_REQUEST_NOT_PRECOMMITTED",
    ):
        inputs.replay_output_loader(changed)
    assert inputs.replay_output_loader(request) == _dummy_output()
    assert bundle["calls"] == [request]
    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="CAPITAL_REPLAY_REQUEST_REPEATED",
    ):
        inputs.replay_output_loader(request)


def test_factory_rejects_caller_supplied_current_phase_before_replay(
    tmp_path: Path,
) -> None:
    bundle = _factory_bundle(tmp_path)
    kwargs = bundle["kwargs"]
    request = kwargs["authority_request"]
    candidates = dict(request._historical_state.capital_candidates_by_phase)
    candidates["CURRENT|CURRENT_SELECTION"] = (_candidate("CURRENT|CURRENT_SELECTION"),)
    historical = replace(
        request._historical_state,
        capital_candidates_by_phase=candidates,
    )
    kwargs["authority_request"] = coordinator.CapitalAuthorityRequest(
        _historical_state=historical,
        _current_state=request._current_state,
        validation_time_utc=request.validation_time_utc,
        authorization_scope=request.authorization_scope,
    )

    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="CAPITAL_CURRENT_CANDIDATES_CALLER_SUPPLIED",
    ):
        factory.build_capital_authority_inputs(**kwargs)
    assert bundle["calls"] == []


def test_factory_rejects_key_mismatch_and_missing_policy(tmp_path: Path) -> None:
    bundle = _factory_bundle(tmp_path)
    kwargs = bundle["kwargs"]
    other = Ed25519PrivateKey.generate()
    _, other_public = _raw_key(other)
    wrong_public = tmp_path / "wrong.public"
    wrong_public.write_bytes(other_public)
    replay_source = kwargs["replay_signing_key_source"]
    kwargs["replay_signing_key_source"] = factory.SigningKeySource(
        private_key=replay_source.private_key,
        public_key=factory.KeyMaterialSource(path=wrong_public),
    )
    with pytest.raises(factory.CapitalAuthorityFactoryError, match="KEY_PAIR_MISMATCH"):
        factory.build_capital_authority_inputs(**kwargs)

    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="POLICY_CONFIG_MISSING_OR_INVALID",
    ):
        factory.load_capital_authority_policy({"pipeline": {}})


def test_funded_scope_needs_config_and_explicit_runtime_enable(tmp_path: Path) -> None:
    bundle = _factory_bundle(tmp_path, funded=True)
    kwargs = bundle["kwargs"]
    kwargs["authorization_scope"] = factory.FUNDED_LIVE_SCOPE
    kwargs["authority_request"] = replace(
        kwargs["authority_request"],
        authorization_scope=factory.FUNDED_LIVE_SCOPE,
    )
    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="FUNDED_CAPITAL_AUTHORIZATION_NOT_EXPLICITLY_ENABLED",
    ):
        factory.build_capital_authority_inputs(**kwargs)

    kwargs["allow_funded_authorization"] = True
    assert (
        type(factory.build_capital_authority_inputs(**kwargs)) is CapitalAuthorityInputs
    )


def test_factory_rejects_expired_or_overlong_authorization(tmp_path: Path) -> None:
    bundle = _factory_bundle(tmp_path)
    kwargs = bundle["kwargs"]
    kwargs["authorization_expires_at_utc"] = "2026-07-14T00:00:01Z"
    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="CAPITAL_AUTHORIZATION_NOT_CURRENT",
    ):
        factory.build_capital_authority_inputs(**kwargs)

    kwargs["authorization_expires_at_utc"] = "2026-07-16T00:00:00Z"
    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="CAPITAL_AUTHORIZATION_TTL_EXCEEDED",
    ):
        factory.build_capital_authority_inputs(**kwargs)


def test_environment_key_material_is_supported_without_config_secrets(
    tmp_path: Path,
) -> None:
    bundle = _factory_bundle(tmp_path)
    kwargs = bundle["kwargs"]
    private = Ed25519PrivateKey.generate()
    private_bytes, public_bytes = _raw_key(private)
    kwargs["replay_signing_key_source"] = factory.SigningKeySource(
        private_key=factory.KeyMaterialSource(base64_env_var="FXPM_REPLAY_PRIVATE"),
        public_key=factory.KeyMaterialSource(base64_env_var="FXPM_REPLAY_PUBLIC"),
    )
    kwargs["environment"] = {
        "FXPM_REPLAY_PRIVATE": base64.b64encode(private_bytes).decode("ascii"),
        "FXPM_REPLAY_PUBLIC": base64.b64encode(public_bytes).decode("ascii"),
    }
    inputs = factory.build_capital_authority_inputs(**kwargs)

    config_text = json.dumps(kwargs["config_payload"])
    assert base64.b64encode(private_bytes).decode("ascii") not in config_text
    assert private_bytes.hex() not in repr(inputs)


def test_authorization_signer_refuses_nonprecommitted_cap(tmp_path: Path) -> None:
    inputs = factory.build_capital_authority_inputs(
        **_factory_bundle(tmp_path)["kwargs"]
    )
    body = {
        "schema_version": cap.CAP_AUTHORIZATION_SCHEMA_VERSION,
        "authority_key_id": inputs.authority_key_id,
        "pending_artifact_content_sha256": _sha("pending"),
        "calibration_decision_sha256": _sha("decision"),
        "selected_policy_point_id": "forged",
        "selected_max_total_stop_risk_pct": 99.0,
        "selected_capital_envelope": {},
        "source_lineage_sha256": _sha("lineage"),
        "constraints_sha256": cap.canonical_json_sha256(asdict(_constraints())),
        "issued_at_utc": inputs.authorization_issued_at_utc,
        "expires_at_utc": inputs.authorization_expires_at_utc,
    }
    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="SELECTED_CAP_NOT_PRECOMMITTED",
    ):
        inputs.authorization_signer(body)


def test_operator_provisioning_is_non_overwriting_and_secret_free(
    tmp_path: Path,
) -> None:
    receipt = provision_key_pair(tmp_path, "replay")
    role_root = tmp_path / "replay"
    private_path = role_root / "private.ed25519"
    public_path = role_root / "public.ed25519"

    assert len(private_path.read_bytes()) == 32
    assert len(public_path.read_bytes()) == 32
    assert receipt["key_id"] == factory.canonical_ed25519_key_id(
        public_path.read_bytes()
    )
    assert "private" not in json.dumps(receipt).lower()
    if os.name != "nt":
        assert private_path.stat().st_mode & 0o777 == 0o600
        assert role_root.stat().st_mode & 0o777 == 0o700
    with pytest.raises(KeyProvisioningError, match="ALREADY_EXISTS"):
        provision_key_pair(tmp_path, "replay")


def test_lazy_factory_freezes_precommitment_before_current_evidence(
    tmp_path: Path,
) -> None:
    bundle = _factory_bundle(tmp_path)
    kwargs = dict(bundle["kwargs"])
    request = kwargs.pop("authority_request")
    lazy = factory.build_capital_authority_factory(**kwargs)
    receipt_path = kwargs["precommitment_receipt_path"]
    body = json.loads(receipt_path.read_text("utf-8"))
    body["signature_base64"] = body["signature_base64"][::-1]
    receipt_path.write_text(json.dumps(body), encoding="utf-8")

    with pytest.raises(
        factory.CapitalAuthorityFactoryError,
        match="CAPITAL_PRECOMMITMENT_CHANGED_AFTER_FACTORY_FREEZE",
    ):
        lazy(request)
