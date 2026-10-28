import base64
import hashlib
import json
import os
import stat
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import pm_book_multiplicity as book
import pm_cap_calibration as cap
import pm_capital_authority_factory as authority_factory
import pm_authority_spool as authority_spool
import pm_causal_process as causal
import pm_current_refit_execution as current_execution
import pm_portfolio_replay as replay
import pm_production_authority_coordinator as coordinator
import pm_production_capital_bootstrap as bootstrap


NOW = datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc)
NOT_BEFORE = datetime(2026, 7, 16, 0, 0, 0, tzinfo=timezone.utc)
DEADLINE = datetime(2026, 7, 20, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _trusted_clock(monkeypatch) -> None:
    monkeypatch.setattr(bootstrap, "_utc_now", lambda: NOW)


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value) -> str:
    import hashlib

    return hashlib.sha256(_canonical(value)).hexdigest()


def _points() -> list[dict[str, float]]:
    return [
        {
            "max_total_stop_risk_pct": total,
            "max_factor_stop_risk_pct": min(total, 2.0),
            "max_same_symbol_stop_risk_pct": min(total, 1.0),
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 50.0,
            "gap_stress_multiple": 2.5,
        }
        for total in cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
    ]


def _constraints() -> cap.CapCalibrationConstraints:
    return cap.CapCalibrationConstraints(
        minimum_outer_anchors=4,
        maximum_outer_anchors=4,
        maximum_candidate_policies=16,
        minimum_inner_calendar_days=1,
        minimum_outer_calendar_days=1,
        minimum_current_calendar_days=1,
    )


def _config(*, funded: bool = False, funded_trust: dict | None = None) -> dict:
    scopes = [bootstrap.DEMO_PROBATION_SCOPE]
    if funded:
        scopes.append(bootstrap.FUNDED_LIVE_SCOPE)
    config = {
        "pipeline": {
            "production_capital_authority": {
                "schema_version": (authority_factory.CAPITAL_AUTHORITY_POLICY_SCHEMA),
                "constraints": asdict(_constraints()),
                "candidate_policy_points": _points(),
                "permitted_deployment_scopes": scopes,
                "authorization_max_ttl_seconds": 3600,
            }
        }
    }
    if funded_trust is not None:
        config["pipeline"]["funded_authorization_trust"] = funded_trust
    return config


def _native_identity() -> dict:
    return {
        "native_activation_id": "activation-production-001",
        "native_activation_manifest_sha256": _sha({"native": "manifest"}),
        "native_activation_roster_sha256": _sha({"native": "roster"}),
        "native_bars_sha256_by_source": {"EURUSD|H1": _sha({"native": "bars"})},
        "native_sidecar_sha256_by_source": {"EURUSD|H1": _sha({"native": "sidecar"})},
    }


def _current_cost() -> dict:
    return {"cost": "exact"}


def _contract() -> dict[str, str]:
    return {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(_current_cost()),
        "native_source_contract_sha256": cap.canonical_json_sha256(_native_identity()),
        "replay_contract_sha256": coordinator._capital_replay_contract_sha256(),
        "margin_contract_sha256": _sha({"margin": "contract"}),
    }


def _process_anchor_plan() -> list[dict]:
    return [
        {
            "anchor_id": "anchor-2019",
            "selection_cutoff_utc": "2018-11-27T00:00:00Z",
            "forward_start_date": "2019-01-01",
            "forward_end_date": "2019-12-31",
            "purge_calendar_days": 35,
        },
        {
            "anchor_id": "anchor-2021",
            "selection_cutoff_utc": "2020-11-27T00:00:00Z",
            "forward_start_date": "2021-01-01",
            "forward_end_date": "2021-12-31",
            "purge_calendar_days": 35,
        },
        {
            "anchor_id": "anchor-2023",
            "selection_cutoff_utc": "2022-11-27T00:00:00Z",
            "forward_start_date": "2023-01-01",
            "forward_end_date": "2023-12-31",
            "purge_calendar_days": 35,
        },
        {
            "anchor_id": "anchor-2024",
            "selection_cutoff_utc": "2024-01-02T00:00:00Z",
            "forward_start_date": "2024-02-06",
            "forward_end_date": "2025-02-04",
            "purge_calendar_days": 35,
        },
    ]


def _process_precommitment() -> dict:
    regime_fit = json.loads(json.dumps(causal._FROZEN_REGIME_FIT_POLICY))
    risk = json.loads(json.dumps(causal._FROZEN_RISK_POLICY))
    cost = json.loads(json.dumps(causal._FROZEN_COST_POLICY))
    definition = {
        "schema": "fxpm-frozen-historical-institution-process-v10",
        "symbols": list(causal.SYMBOLS),
        "timeframes": list(causal.TIMEFRAMES),
        "regimes": list(causal.REGIMES),
        "cell_count": causal.CELL_COUNT,
        "book_multiplicity_contract_version": (
            causal.PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION
        ),
        "winner_selection_contract_version": (
            causal.PINNED_WINNER_SELECTION_CONTRACT_VERSION
        ),
        "winner_evidence_ledger_version": (
            causal.PINNED_WINNER_EVIDENCE_LEDGER_VERSION
        ),
        "regime_fit_contract_version": causal.PINNED_REGIME_FIT_CONTRACT_VERSION,
        "regime_detector_record_schema_version": (
            causal.REGIME_DETECTOR_RECORD_SCHEMA_VERSION
        ),
        "regime_detector_ledger_record_schema_version": (
            causal.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
        ),
        "component_identities": {
            name: _sha({"component": name}) for name in causal.COMPONENT_IDENTITY_KEYS
        },
        "policy_identities": {
            name: (
                _sha(risk)
                if name == "risk_policy_sha256"
                else _sha(cost)
                if name == "cost_model_policy_sha256"
                else _sha(regime_fit)
                if name == "regime_fit_policy_sha256"
                else _sha({"policy": name})
            )
            for name in causal.POLICY_IDENTITY_KEYS
        },
        "regime_fit_policy": regime_fit,
        "risk_policy": risk,
        "cost_policy": cost,
        "anchor_plan": _process_anchor_plan(),
        "scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
        "forbidden_authorities": [
            "CURRENT_REFIT",
            "MODEL_PROMOTION",
            "CAPITAL_ALLOCATION",
            "FUNDED_LIVE_TRADING",
            "LIVE_TRADING",
            "PROFITABILITY_CLAIM",
        ],
    }
    policy = causal.pinned_process_policy()
    return {
        "schema": causal.PRECOMMIT_SCHEMA,
        "process_id": "production-process-20260715",
        "authorized_at_utc": "2026-07-15T00:00:00Z",
        "evidence_access_not_before_utc": "2026-07-16T00:00:00Z",
        "evidence_access_deadline_utc": "2026-07-20T00:00:00Z",
        "policy": policy,
        "policy_sha256": _sha(policy),
        "process_definition": definition,
        "process_definition_sha256": _sha(definition),
    }


def _inner_windows() -> dict[str, dict[str, str]]:
    return {
        "anchor-2019": {"start_date": "2018-01-01", "end_date": "2019-01-01"},
        "anchor-2021": {"start_date": "2020-01-01", "end_date": "2021-01-01"},
        "anchor-2023": {"start_date": "2022-01-01", "end_date": "2023-01-01"},
        "anchor-2024": {"start_date": "2023-02-06", "end_date": "2024-02-06"},
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


def _bootstrap_kwargs(
    tmp_path: Path,
    *,
    funded: bool = False,
    funded_trust: dict | None = None,
    generation_id: str = "generation-20260715-001",
    genuine_replay: bool = False,
) -> dict:
    config = _config(funded=funded, funded_trust=funded_trust)
    if genuine_replay:
        config["pipeline"].update(
            {
                "initial_capital": 10_000.0,
                "live_allowed_promotion_statuses": ["PROCESS_VALIDATED_CURRENT_REFIT"],
            }
        )
    context = authority_spool.freeze_generation_context(
        tmp_path / "generation-context",
        generation_id=generation_id,
        native_universe_sha256=_sha({"native": "universe"}),
        config_sha256=_sha(config),
    )
    return {
        "bootstrap_root": tmp_path / "bootstrap",
        "output_root": tmp_path / "capital-output",
        "authorization_scope": (
            bootstrap.FUNDED_LIVE_SCOPE if funded else bootstrap.DEMO_PROBATION_SCOPE
        ),
        "generation_context": context,
        "config_payload": config,
        "native_source_identity": _native_identity(),
        "process_precommitment_payload": _process_precommitment(),
        "inner_selection_windows_by_anchor": _inner_windows(),
        "current_selection_window": {
            "start_date": "2025-02-05",
            "end_date": "2026-01-01",
        },
        "contract_lineage": _contract(),
        "margin_terms_sha256": _sha({"margin": "CURRENT"}),
        "broker_terms_sha256": _sha({"broker": "CURRENT"}),
        "replay_output_loader": lambda _request: _dummy_output(),
    }


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
        margin_model=coordinator.PRODUCTION_MARGIN_MODEL,
    ).normalized()


def _lineage(phase_id: str) -> dict:
    stable = "CURRENT" if phase_id.startswith("CURRENT|") else phase_id.split("|")[0]
    return {
        **_contract(),
        "book_role": (
            "PROCESS_VALIDATED_CURRENT_REFIT"
            if stable == "CURRENT"
            else "ANCHOR_LOCAL_REFIT"
        ),
        "book_decision_sha256": _sha({"decision": stable}),
        "book_generation_receipt_sha256": _sha({"generation": stable}),
        "refit_receipt_sha256": _sha({"refit": stable}),
        "refit_execution_sha256": _sha({"execution": stable}),
        "output_book_sha256": _sha({"book": stable}),
        "winner_roster_sha256": _sha({"winner": stable}),
        "native_activation_id": f"activation-{stable}",
        "native_activation_manifest_sha256": _sha({"manifest": stable}),
        "native_activation_roster_sha256": _sha({"roster": stable}),
        "native_bars_sha256_by_source": {"EURUSD|H1": _sha({"bars": phase_id})},
        "native_sidecar_sha256_by_source": {"EURUSD|H1": _sha({"sidecar": phase_id})},
        "margin_terms_sha256": _sha({"margin": stable}),
        "broker_terms_sha256": _sha({"broker": stable}),
    }


def _authority_request(
    authorization_scope: str = bootstrap.DEMO_PROBATION_SCOPE,
) -> coordinator.CapitalAuthorityRequest:
    anchor_ids = [item["anchor_id"] for item in _process_anchor_plan()]
    historical_phases = tuple(
        f"{anchor_id}|{phase}"
        for anchor_id in anchor_ids
        for phase in ("INNER_SELECTION", "OUTER_EVALUATION")
    )
    historical = coordinator._HistoricalState(
        run_receipt=SimpleNamespace(receipt_sha256=_sha({"historical": "run"})),
        run_body={
            "anchors": [
                {
                    "anchor_id": anchor_id,
                    "book_decision_sha256": _sha({"decision": anchor_id}),
                    "book_generation_sha256": _sha({"generation": anchor_id}),
                }
                for anchor_id in anchor_ids
            ]
        },
        process_decision={"decision": "OFFLINE_PROCESS_VALIDATION_PASS"},
        precommit_payload=_process_precommitment(),
        evidence_payload={},
        reconciliation_payload={},
        historical_book_decision={"receipt_sha256": _sha({"decision": anchor_ids[-1]})},
        capital_bootstrap_identity_sha256=_sha({"bootstrap": "test-fixture"}),
        capital_candidates_by_phase={
            phase_id: (_candidate(phase_id),) for phase_id in historical_phases
        },
        capital_lineage_by_phase={
            phase_id: _lineage(phase_id) for phase_id in historical_phases
        },
    )
    current_phase = "CURRENT|CURRENT_SELECTION"
    current_refit_lineage = {
        "historical_process_decision_sha256": _sha({"historical": "process"}),
        "accepted_book_generation_receipt_sha256": _sha({"generation": "CURRENT"}),
        "current_refit_receipt_sha256": _sha({"refit": "CURRENT"}),
        "current_refit_execution_sha256": _sha({"execution": "CURRENT"}),
        "current_refit_cell_count": 1,
        "current_selected_winner_roster_sha256": _sha({"winner": "CURRENT"}),
        "current_refit_output_book_sha256": _sha({"book": "CURRENT"}),
    }
    ledger = {"EURUSD": {"symbol": "EURUSD", "regime_configs": {}}}
    product = coordinator.CurrentRefitProduct(
        current_book_decision={"receipt_sha256": _sha({"decision": "CURRENT"})},
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
        staged_ledger_content_sha256=_sha({"ledger": "content"}),
        physical_ledger_sha256=replay.canonical_json_sha256(ledger),
    )
    current = coordinator._CurrentState(
        product=product,
        roster={"current_refit_lineage": current_refit_lineage},
        candidates=(_candidate(current_phase),),
        portfolio_evidence={},
        portfolio_lineage={},
        cost_contract=_current_cost(),
        policies={},
        validation_policy={},
        native_identity=_native_identity(),
        margin_contract_sha256=_contract()["margin_contract_sha256"],
        margin_terms_sha256=_sha({"margin": "CURRENT"}),
        broker_terms_sha256=_sha({"broker": "CURRENT"}),
    )
    return coordinator.CapitalAuthorityRequest(
        _historical_state=historical,
        _current_state=current,
        validation_time_utc="2026-07-16T00:00:01Z",
        authorization_scope=authorization_scope,
    )


def _funded_authority_request() -> coordinator.CapitalAuthorityRequest:
    return _authority_request(bootstrap.FUNDED_LIVE_SCOPE)


def _genuine_candidate(phase_id: str, window: dict[str, str]) -> replay.CandidateTrade:
    entry = datetime.combine(
        datetime.fromisoformat(window["start_date"]).date(),
        datetime.min.time(),
        tzinfo=timezone.utc,
    ) + timedelta(hours=1)
    status = (
        "PROCESS_VALIDATED_CURRENT_REFIT"
        if phase_id == "CURRENT|CURRENT_SELECTION"
        else "HISTORICAL_OOS_REPLAY"
    )
    return replace(
        _candidate(phase_id),
        promotion_status=status,
        entry_time=entry,
        legs=(replay.ExitLeg(entry + timedelta(days=1), 1.0, 10.0),),
    ).normalized()


def _genuine_authority_request(config: dict) -> coordinator.CapitalAuthorityRequest:
    request = _authority_request()
    windows: dict[str, dict[str, str]] = {}
    for anchor in _process_anchor_plan():
        anchor_id = anchor["anchor_id"]
        windows[f"{anchor_id}|INNER_SELECTION"] = _inner_windows()[anchor_id]
        windows[f"{anchor_id}|OUTER_EVALUATION"] = {
            "start_date": anchor["forward_start_date"],
            "end_date": (
                datetime.fromisoformat(anchor["forward_end_date"]).date()
                + timedelta(days=1)
            ).isoformat(),
        }
    windows["CURRENT|CURRENT_SELECTION"] = {
        "start_date": "2025-02-05",
        "end_date": "2026-01-01",
    }
    historical_candidates = {
        phase_id: (_genuine_candidate(phase_id, window),)
        for phase_id, window in windows.items()
        if not phase_id.startswith("CURRENT|")
    }
    current_candidate = _genuine_candidate(
        "CURRENT|CURRENT_SELECTION", windows["CURRENT|CURRENT_SELECTION"]
    )
    historical = replace(
        request._historical_state,
        capital_candidates_by_phase=historical_candidates,
    )
    product = replace(
        request._current_state.product,
        replay_candidates=(current_candidate,),
    )
    base_policy = replay.portfolio_policy_from_config_payload(config, 10_000.0)
    current = replace(
        request._current_state,
        product=product,
        roster={
            **request._current_state.roster,
            "symbols": ["EURUSD"],
            "authoritative": True,
        },
        candidates=(current_candidate,),
        policies={10_000.0: asdict(base_policy)},
    )
    return replace(
        request,
        _historical_state=historical,
        _current_state=current,
    )


def _write_key_pair(
    root: Path, name: str, private: Ed25519PrivateKey | None = None
) -> tuple[Ed25519PrivateKey, authority_factory.SigningKeySource]:
    key = Ed25519PrivateKey.generate() if private is None else private
    private_bytes = key.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    public_bytes = key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    private_path = root / f"{name}.private"
    public_path = root / f"{name}.public"
    private_path.write_bytes(private_bytes)
    public_path.write_bytes(public_bytes)
    if os.name != "nt":
        private_path.chmod(0o600)
    return key, authority_factory.SigningKeySource(
        private_key=authority_factory.KeyMaterialSource(path=private_path),
        public_key=authority_factory.KeyMaterialSource(path=public_path),
    )


def _authorization_body(inputs: coordinator.CapitalAuthorityInputs) -> dict:
    policy = inputs.precommitment_receipt["body"]["candidate_policies"][0]
    return {
        "schema_version": cap.CAP_AUTHORIZATION_SCHEMA_VERSION,
        "authority_key_id": inputs.authority_key_id,
        "pending_artifact_content_sha256": _sha({"pending": "artifact"}),
        "calibration_decision_sha256": _sha({"calibration": "decision"}),
        "selected_policy_point_id": policy["policy_id"],
        "selected_max_total_stop_risk_pct": policy["max_total_stop_risk_pct"],
        "selected_capital_envelope": {},
        "source_lineage_sha256": _sha({"source": "lineage"}),
        "constraints_sha256": cap.canonical_json_sha256(asdict(inputs.constraints)),
        "issued_at_utc": inputs.authorization_issued_at_utc,
        "expires_at_utc": inputs.authorization_expires_at_utc,
    }


def _test_signed_envelope(
    private_key: Ed25519PrivateKey, kind: str, body: dict
) -> dict:
    public = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    key_id = authority_factory.canonical_ed25519_key_id(public)
    payload = (
        bootstrap.funded_authorization_response_signature_payload(key_id, body)
        if kind == bootstrap.FUNDED_AUTHORIZATION_RESPONSE_SIGNATURE_KIND
        else cap.capital_authority_signature_payload(kind, key_id, body)
    )
    signature = private_key.sign(payload)
    return {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": key_id,
        "body": body,
        "signature_base64": base64.b64encode(signature).decode("ascii"),
    }


def _write_external_authorization_response(
    path: Path, request: dict, private_key: Ed25519PrivateKey
) -> tuple[dict, dict]:
    authorization_envelope = _test_signed_envelope(
        private_key,
        cap.AUTHORIZATION_SIGNATURE_KIND,
        request["authorization_body"],
    )
    response_body = {
        "schema_version": bootstrap.FUNDED_AUTHORIZATION_RESPONSE_SCHEMA,
        "request_sha256": _sha(request),
        "generation_id": request["generation_id"],
        "generation_context_sha256": request["generation_context_sha256"],
        "bootstrap_identity_sha256": request["bootstrap_identity_sha256"],
        "authorization_body_sha256": request["authorization_body_sha256"],
        "authorization_envelope": authorization_envelope,
    }
    response = _test_signed_envelope(
        private_key,
        bootstrap.FUNDED_AUTHORIZATION_RESPONSE_SIGNATURE_KIND,
        response_body,
    )
    path.write_bytes(_canonical(response))
    return response, authorization_envelope


def _funded_trust(public: bytes, key_id: str, exchange_root: Path) -> dict:
    return {
        "schema_version": bootstrap.FUNDED_AUTHORIZATION_TRUST_SCHEMA,
        "authorization_key_id": key_id,
        "authorization_public_key_sha256": hashlib.sha256(public).hexdigest(),
        "authorization_exchange_root": str(exchange_root),
        "channel_policy": bootstrap.FUNDED_AUTHORIZATION_CHANNEL_POLICY,
    }


def _funded_bootstrap_case(
    root: Path, *, generation_id: str
) -> tuple[bootstrap.ProductionCapitalBootstrap, Ed25519PrivateKey]:
    root.mkdir()
    _, precommit_source = _write_key_pair(root, "precommit")
    _, replay_source = _write_key_pair(root, "replay")
    authorization_private = Ed25519PrivateKey.generate()
    authorization_public = authorization_private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    authorization_key_id = authority_factory.canonical_ed25519_key_id(
        authorization_public
    )
    authorization_public_path = root / "authorization.public"
    authorization_public_path.write_bytes(authorization_public)
    exchange_root = root / "exchange"
    exchange_root.mkdir()
    kwargs = _bootstrap_kwargs(
        root,
        funded=True,
        funded_trust=_funded_trust(
            authorization_public, authorization_key_id, exchange_root
        ),
        generation_id=generation_id,
    )
    kwargs["funded_boundary"] = bootstrap.FundedAuthorityBoundary(
        precommitment_signing_key_source=precommit_source,
        replay_signing_key_source=replay_source,
        authorization_public_key_source=authority_factory.KeyMaterialSource(
            path=authorization_public_path
        ),
    )
    return (
        bootstrap.bootstrap_production_capital_authority(**kwargs),
        authorization_private,
    )


def _hardlink_or_skip(source: Path, alias: Path) -> None:
    try:
        os.link(source, alias)
    except OSError as exc:
        pytest.skip(f"hardlinks unavailable: {exc}")


def _request_artifact(exchange_root: Path) -> Path:
    matches = [
        item
        for item in exchange_root.rglob("*.json")
        if not {"incoming", "accepted", "issued"}.intersection(item.parts)
    ]
    assert len(matches) == 1
    return matches[0]


def test_demo_bootstrap_freezes_full_roster_and_builds_coordinator_inputs(
    tmp_path: Path,
) -> None:
    frozen = bootstrap.bootstrap_production_capital_authority(
        **_bootstrap_kwargs(tmp_path)
    )

    assert frozen.authorization_scope == bootstrap.DEMO_PROBATION_SCOPE
    assert len(list(frozen.generation_root.rglob("*.json"))) == 2
    receipt_path = frozen.generation_root.joinpath(
        *PurePosixPath(frozen.bootstrap_receipt.relative_path).parts
    )
    receipt = json.loads(receipt_path.read_text("utf-8"))
    assert receipt["confers_numeric_capital_authority"] is False
    assert receipt["confers_funded_live_authority"] is False
    assert receipt["identity"]["generation_id"] == frozen.generation_id
    assert receipt["identity"]["native_source_identity"] == _native_identity()
    assert receipt["identity"]["process_precommitment_sha256"] == _sha(
        _process_precommitment()
    )
    assert set(receipt["identity"]["required_total_stop_risk_candidates_pct"]) == set(
        cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
    )

    inputs = frozen.capital_factory(_authority_request())
    assert type(inputs) is coordinator.CapitalAuthorityInputs
    assert frozen.capital_factory.authorization_scope == bootstrap.DEMO_PROBATION_SCOPE
    assert (
        inputs.authorization_signer.scope_binding
        == frozen.capital_factory.scope_binding
    )
    inputs.authorization_signer.require_authorization_scope(
        bootstrap.DEMO_PROBATION_SCOPE
    )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH",
    ):
        inputs.authorization_signer.require_authorization_scope(
            bootstrap.FUNDED_LIVE_SCOPE
        )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH",
    ):
        frozen.capital_factory(_funded_authority_request())
    totals = {
        row["max_total_stop_risk_pct"]
        for row in inputs.precommitment_receipt["body"]["candidate_policies"]
    }
    assert {33.25, 50.0, 75.0, 90.0}.issubset(totals)
    assert {3.0, 5.0, 8.0, 10.0, 12.0, 20.0}.issubset(totals)
    assert set(inputs.frozen_candidate_rosters_by_phase) == {
        *(f"{item['anchor_id']}|INNER_SELECTION" for item in _process_anchor_plan()),
        *(f"{item['anchor_id']}|OUTER_EVALUATION" for item in _process_anchor_plan()),
        "CURRENT|CURRENT_SELECTION",
    }
    signed = inputs.authorization_signer(_authorization_body(inputs))
    verified, _ = cap._verified_envelope(
        signed,
        kind=cap.AUTHORIZATION_SIGNATURE_KIND,
        trusted_public_keys=inputs.trusted_authority_public_keys,
        missing_reason="missing",
    )
    assert verified == _authorization_body(inputs)
    frozen.require_authorization_scope(bootstrap.DEMO_PROBATION_SCOPE)
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH",
    ):
        frozen.require_authorization_scope(bootstrap.FUNDED_LIVE_SCOPE)


def test_historical_capital_replay_uses_coordinator_candidates_not_legacy_loader(
    tmp_path: Path,
) -> None:
    legacy_calls: list[dict] = []
    kwargs = _bootstrap_kwargs(tmp_path, genuine_replay=True)
    kwargs["replay_output_loader"] = lambda request: (
        legacy_calls.append(dict(request)) or _dummy_output()
    )
    frozen = bootstrap.bootstrap_production_capital_authority(**kwargs)
    authority_request = _genuine_authority_request(kwargs["config_payload"])
    inputs = frozen.capital_factory(authority_request)
    phase_prefix = "anchor-2019|INNER_SELECTION|"
    attempt_id = next(
        key
        for key in inputs.replay_output_loader._expected
        if key.startswith(phase_prefix)
    )
    exact_request = inputs.replay_output_loader._expected[attempt_id]

    output = inputs.replay_output_loader(exact_request)

    genuine = authority_request._historical_state.capital_candidates_by_phase[
        "anchor-2019|INNER_SELECTION"
    ][0]
    assert legacy_calls == []
    assert (
        output["input_receipt"]["candidate_roster_sha256"]
        == (
            replay.portfolio_replay_candidate_roster_receipt([genuine])[
                "candidate_roster_sha256"
            ]
        )
    )
    assert output["input_receipt"]["execution_authority_mode"] == (
        "STANDARD_SYNCHRONIZED_REPLAY"
    )
    assert {
        row["candidate_id"]
        for row in [*output["closed_positions"], *output["blocked_positions"]]
    } == {genuine.candidate_id}
    assert tuple(output["policy"]["allowed_promotion_statuses"]) == (
        "HISTORICAL_OOS_REPLAY",
    )

    fresh_inputs = frozen.capital_factory(authority_request)
    substituted = json.loads(_canonical(exact_request).decode("utf-8"))
    substituted["frozen_candidate_roster"]["candidate_roster_sha256"] = _sha(
        {"forged": "roster"}
    )
    with pytest.raises(
        authority_factory.CapitalAuthorityFactoryError,
        match="CAPITAL_REPLAY_REQUEST_NOT_PRECOMMITTED",
    ):
        fresh_inputs.replay_output_loader(substituted)
    assert legacy_calls == []


def test_production_replay_is_constructible_without_pre_evidence_loader(
    tmp_path: Path,
) -> None:
    kwargs = _bootstrap_kwargs(tmp_path, genuine_replay=True)
    kwargs.pop("replay_output_loader")
    frozen = bootstrap.bootstrap_production_capital_authority(**kwargs)
    authority_request = _genuine_authority_request(kwargs["config_payload"])
    inputs = frozen.capital_factory(authority_request)
    attempt_id = next(
        key
        for key in inputs.replay_output_loader._expected
        if key.startswith("anchor-2021|OUTER_EVALUATION|")
    )

    output = inputs.replay_output_loader(
        inputs.replay_output_loader._expected[attempt_id]
    )

    genuine = authority_request._historical_state.capital_candidates_by_phase[
        "anchor-2021|OUTER_EVALUATION"
    ][0]
    assert (
        output["input_receipt"]["candidate_roster_sha256"]
        == (
            replay.portfolio_replay_candidate_roster_receipt([genuine])[
                "candidate_roster_sha256"
            ]
        )
    )


def test_current_capital_replay_reverifies_then_uses_current_genuine_records(
    tmp_path: Path, monkeypatch
) -> None:
    legacy_calls: list[dict] = []
    kwargs = _bootstrap_kwargs(tmp_path, genuine_replay=True)
    kwargs["replay_output_loader"] = lambda request: (
        legacy_calls.append(dict(request)) or _dummy_output()
    )
    frozen = bootstrap.bootstrap_production_capital_authority(**kwargs)
    authority_request = _genuine_authority_request(kwargs["config_payload"])
    current = authority_request._current_state
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        coordinator,
        "_owned_book_decision",
        lambda value, *, code_prefix: (
            captured.update(book=value, code_prefix=code_prefix) or ({}, None)
        ),
    )

    def rebuild(ledger, symbols, **arguments):
        captured["ledger"] = ledger
        captured["symbols"] = list(symbols)
        captured["raw_manifest"] = arguments["current_refit_raw_manifest"]
        captured["artifact_root"] = arguments["current_refit_artifact_root"]
        return current.roster, []

    def validate(candidates, roster, *, replayed_cell_ids):
        observed = tuple(candidates)
        captured["validated_candidates"] = observed
        captured["validated_roster"] = roster
        captured["replayed_cell_ids"] = tuple(replayed_cell_ids)
        return []

    monkeypatch.setattr(replay, "build_current_refit_replay_roster", rebuild)
    monkeypatch.setattr(replay, "validate_current_refit_replay_candidates", validate)
    inputs = frozen.capital_factory(authority_request)
    phase_prefix = "CURRENT|CURRENT_SELECTION|"
    attempt_id = next(
        key
        for key in inputs.replay_output_loader._expected
        if key.startswith(phase_prefix)
    )

    output = inputs.replay_output_loader(
        inputs.replay_output_loader._expected[attempt_id]
    )

    genuine = current.candidates[0]
    assert legacy_calls == []
    assert captured["ledger"] == current.product.staged_ledger
    assert captured["symbols"] == ["EURUSD"]
    assert captured["raw_manifest"] == current.product.raw_manifest_path
    assert captured["artifact_root"] == current.product.artifact_root
    assert captured["validated_candidates"] == (genuine,)
    assert captured["validated_roster"] == current.roster
    assert captured["replayed_cell_ids"] == current.product.replayed_cell_ids
    assert output["input_receipt"]["execution_authority_mode"] == (
        "CURRENT_REFIT_REVERIFIED"
    )
    assert {
        row["candidate_id"]
        for row in [*output["closed_positions"], *output["blocked_positions"]]
    } == {genuine.candidate_id}
    assert tuple(output["policy"]["allowed_promotion_statuses"]) == (
        "PROCESS_VALIDATED_CURRENT_REFIT",
    )


def test_filtered_precommit_roster_cannot_replace_content_addressed_freeze(
    tmp_path: Path,
) -> None:
    frozen = bootstrap.bootstrap_production_capital_authority(
        **_bootstrap_kwargs(tmp_path)
    )
    precommitment_path = frozen.generation_root.joinpath(
        *PurePosixPath(frozen.precommitment.relative_path).parts
    )
    envelope = json.loads(precommitment_path.read_text("utf-8"))
    envelope["body"]["candidate_policies"] = envelope["body"]["candidate_policies"][1:]
    precommitment_path.write_bytes(_canonical(envelope))

    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_ARTIFACT_HASH_MISMATCH",
    ):
        frozen.capital_factory(_authority_request())


def test_generation_context_rejects_config_payload_substitution(tmp_path: Path) -> None:
    kwargs = _bootstrap_kwargs(tmp_path)
    kwargs["config_payload"]["pipeline"]["production_capital_authority"][
        "authorization_max_ttl_seconds"
    ] = 3599
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_CONFIG_PAYLOAD_CONTEXT_MISMATCH",
    ):
        bootstrap.bootstrap_production_capital_authority(**kwargs)


def test_factory_rejects_coordinator_process_precommitment_substitution(
    tmp_path: Path,
) -> None:
    frozen = bootstrap.bootstrap_production_capital_authority(
        **_bootstrap_kwargs(tmp_path)
    )
    request = _authority_request()
    substituted = _process_precommitment()
    substituted["process_id"] = "substituted-process"
    historical = replace(request._historical_state, precommit_payload=substituted)
    substituted_request = replace(request, _historical_state=historical)
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_COORDINATOR_PROCESS_MISMATCH",
    ):
        frozen.capital_factory(substituted_request)


def test_bootstrap_created_after_process_evidence_deadline_fails_closed(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(bootstrap, "_utc_now", lambda: DEADLINE)
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_CREATED_AFTER_PROCESS_EVIDENCE_DEADLINE",
    ):
        bootstrap.bootstrap_production_capital_authority(**_bootstrap_kwargs(tmp_path))


def test_bootstrap_can_follow_process_freeze_without_sleep(tmp_path: Path) -> None:
    kwargs = _bootstrap_kwargs(tmp_path)
    process = kwargs["process_precommitment_payload"]
    process["evidence_access_not_before_utc"] = process["authorized_at_utc"]

    frozen = bootstrap.bootstrap_production_capital_authority(**kwargs)
    binding = bootstrap.verify_capital_forward_phase_binding(
        kwargs["generation_context"], frozen.forward_phase_binding
    )

    assert binding["bootstrap_identity"]["created_at_utc"] == (
        "2026-07-15T00:00:00Z"
    )
    assert binding["bootstrap_identity"][
        "process_evidence_access_not_before_utc"
    ] == "2026-07-15T00:00:00Z"
    assert binding["capital_precommitment"][
        "evidence_access_not_before_utc"
    ] == "2026-07-15T00:00:01Z"


def test_funded_key_role_substitution_is_rejected_before_artifacts(
    tmp_path: Path,
) -> None:
    _, shared = _write_key_pair(tmp_path, "shared")
    authorization = Ed25519PrivateKey.generate()
    authorization_public_bytes = authorization.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    authorization_key_id = authority_factory.canonical_ed25519_key_id(
        authorization_public_bytes
    )
    authorization_public = tmp_path / "authorization.public"
    authorization_public.write_bytes(authorization_public_bytes)
    exchange_root = tmp_path / "funded-exchange"
    exchange_root.mkdir()
    kwargs = _bootstrap_kwargs(
        tmp_path,
        funded=True,
        funded_trust=_funded_trust(
            authorization_public_bytes, authorization_key_id, exchange_root
        ),
    )
    kwargs["funded_boundary"] = bootstrap.FundedAuthorityBoundary(
        precommitment_signing_key_source=shared,
        replay_signing_key_source=shared,
        authorization_public_key_source=authority_factory.KeyMaterialSource(
            path=authorization_public
        ),
    )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_KEY_ROLES_NOT_SEPARATED",
    ):
        bootstrap.bootstrap_production_capital_authority(**kwargs)
    assert not (tmp_path / "bootstrap").exists()


def test_funded_two_stage_request_pending_then_external_response_is_frozen(
    tmp_path: Path,
) -> None:
    missing_root = tmp_path / "missing-boundary"
    missing_root.mkdir()
    kwargs = _bootstrap_kwargs(missing_root, funded=True)
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_BOUNDARY_MISSING",
    ):
        bootstrap.bootstrap_production_capital_authority(**kwargs)

    actual = tmp_path / "actual"
    actual.mkdir()
    precommit_private, precommit_source = _write_key_pair(actual, "precommit")
    del precommit_private
    replay_private, replay_source = _write_key_pair(actual, "replay")
    del replay_private
    authorization_private = Ed25519PrivateKey.generate()
    authorization_public_bytes = authorization_private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    authorization_key_id = authority_factory.canonical_ed25519_key_id(
        authorization_public_bytes
    )
    authorization_public = actual / "funded-authorization.public"
    authorization_public.write_bytes(authorization_public_bytes)
    exchange_base = actual / "funded-exchange"
    exchange_base.mkdir()
    trust = _funded_trust(
        authorization_public_bytes, authorization_key_id, exchange_base
    )
    kwargs = _bootstrap_kwargs(actual, funded=True, funded_trust=trust)
    kwargs["funded_boundary"] = bootstrap.FundedAuthorityBoundary(
        precommitment_signing_key_source=precommit_source,
        replay_signing_key_source=replay_source,
        authorization_public_key_source=authority_factory.KeyMaterialSource(
            path=authorization_public
        ),
    )
    frozen = bootstrap.bootstrap_production_capital_authority(**kwargs)
    assert (
        "authorization_envelope_provider"
        not in bootstrap.FundedAuthorityBoundary.__dataclass_fields__
    )
    assert (
        "authorization_envelope_path"
        not in bootstrap.FundedAuthorityBoundary.__dataclass_fields__
    )
    assert not isinstance(
        frozen.capital_factory._authorization_key,
        authority_factory._LoadedSigningKey,
    )
    inputs = frozen.capital_factory(_funded_authority_request())
    assert "two-stage-external" in repr(inputs.authorization_signer)
    with pytest.raises(coordinator.ProductionAuthorityBlocked) as pending:
        inputs.authorization_signer(_authorization_body(inputs))
    assert pending.value.code == "PENDING_EXTERNAL_AUTHORIZATION"

    exchange_root = frozen.capital_factory._authorization_exchange_root
    assert exchange_root is not None
    request_files = [
        item
        for item in exchange_root.rglob("*.json")
        if not {"incoming", "accepted", "issued"}.intersection(item.parts)
    ]
    assert len(request_files) == 1
    request = json.loads(request_files[0].read_text("utf-8"))
    assert request["authorization_body"] == _authorization_body(inputs)
    assert request_files[0].name == f"{_sha(request)}.json"
    response_path = exchange_root / "incoming" / f"{_sha(request)}.json"
    response, expected_authorization = _write_external_authorization_response(
        response_path, request, authorization_private
    )

    authorized_inputs = frozen.capital_factory(_funded_authority_request())
    signed = authorized_inputs.authorization_signer(
        _authorization_body(authorized_inputs)
    )
    assert signed == expected_authorization
    verified, _ = cap._verified_envelope(
        signed,
        kind=cap.AUTHORIZATION_SIGNATURE_KIND,
        trusted_public_keys=authorized_inputs.trusted_authority_public_keys,
        missing_reason="missing",
    )
    assert verified == _authorization_body(authorized_inputs)
    accepted = [item for item in (exchange_root / "accepted").rglob("*.json")]
    assert len(accepted) == 1
    assert accepted[0].name == f"{_sha(response)}.json"
    assert json.loads(accepted[0].read_text("utf-8")) == response

    response_path.write_bytes(response_path.read_bytes() + b" ")
    changed_inputs = frozen.capital_factory(_funded_authority_request())
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_RESPONSE_MISSING_OR_INVALID",
    ):
        changed_inputs.authorization_signer(_authorization_body(changed_inputs))

    request_files[0].write_bytes(request_files[0].read_bytes() + b" ")
    mutated_request_inputs = frozen.capital_factory(_funded_authority_request())
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_CONTENT_ADDRESS_COLLISION",
    ):
        mutated_request_inputs.authorization_signer(
            _authorization_body(mutated_request_inputs)
        )


def test_funded_prestaged_valid_response_still_requires_issued_pending_pass(
    tmp_path: Path,
) -> None:
    frozen, authorization_private = _funded_bootstrap_case(
        tmp_path / "prestaged",
        generation_id="generation-20260715-prestaged",
    )
    inputs = frozen.capital_factory(_funded_authority_request())
    exact = _authorization_body(inputs)
    transport = inputs.authorization_signer._delegate
    request = transport._request(exact)
    request_sha256 = _sha(request)
    exchange_root = frozen.capital_factory._authorization_exchange_root
    assert exchange_root is not None
    response_path = exchange_root / "incoming" / f"{request_sha256}.json"
    response_path.parent.mkdir()
    _, expected_authorization = _write_external_authorization_response(
        response_path, request, authorization_private
    )

    with pytest.raises(coordinator.ProductionAuthorityBlocked) as pending:
        inputs.authorization_signer(exact)
    assert pending.value.code == "PENDING_EXTERNAL_AUTHORIZATION"
    issued = list((exchange_root / "issued").rglob("*.json"))
    assert len(issued) == 1

    rerun_inputs = frozen.capital_factory(_funded_authority_request())
    assert rerun_inputs.authorization_signer(_authorization_body(rerun_inputs)) == (
        expected_authorization
    )


def test_funded_mode_rejects_missing_exchange_root_and_unpinned_public_key(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing-exchange"
    missing.mkdir()
    _, missing_precommit = _write_key_pair(missing, "precommit")
    _, missing_replay = _write_key_pair(missing, "replay")
    missing_authorization = Ed25519PrivateKey.generate()
    missing_public = missing_authorization.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    missing_key_id = authority_factory.canonical_ed25519_key_id(missing_public)
    missing_public_path = missing / "authorization.public"
    missing_public_path.write_bytes(missing_public)
    absent_exchange = missing / "absent-exchange-root"
    missing_trust = _funded_trust(missing_public, missing_key_id, absent_exchange)
    missing_kwargs = _bootstrap_kwargs(missing, funded=True, funded_trust=missing_trust)
    missing_kwargs["funded_boundary"] = bootstrap.FundedAuthorityBoundary(
        precommitment_signing_key_source=missing_precommit,
        replay_signing_key_source=missing_replay,
        authorization_public_key_source=authority_factory.KeyMaterialSource(
            path=missing_public_path
        ),
    )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_EXCHANGE_ROOT_INVALID",
    ):
        bootstrap.bootstrap_production_capital_authority(**missing_kwargs)
    assert not (missing / "bootstrap").exists()

    mismatch = tmp_path / "unpinned-key"
    mismatch.mkdir()
    _, mismatch_precommit = _write_key_pair(mismatch, "precommit")
    _, mismatch_replay = _write_key_pair(mismatch, "replay")
    pinned_authorization = Ed25519PrivateKey.generate()
    pinned_public = pinned_authorization.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    pinned_key_id = authority_factory.canonical_ed25519_key_id(pinned_public)
    exchange_root = mismatch / "funded-exchange"
    exchange_root.mkdir()
    injected_authorization = Ed25519PrivateKey.generate()
    injected_public = injected_authorization.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    injected_public_path = mismatch / "injected-authorization.public"
    injected_public_path.write_bytes(injected_public)
    mismatch_kwargs = _bootstrap_kwargs(
        mismatch,
        funded=True,
        funded_trust=_funded_trust(pinned_public, pinned_key_id, exchange_root),
    )
    mismatch_kwargs["funded_boundary"] = bootstrap.FundedAuthorityBoundary(
        precommitment_signing_key_source=mismatch_precommit,
        replay_signing_key_source=mismatch_replay,
        authorization_public_key_source=authority_factory.KeyMaterialSource(
            path=injected_public_path
        ),
    )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_MISMATCH",
    ):
        bootstrap.bootstrap_production_capital_authority(**mismatch_kwargs)
    assert not (mismatch / "bootstrap").exists()


def test_funded_factory_reverifies_config_pinned_authority_key_and_scope(
    tmp_path: Path,
) -> None:
    frozen, _ = _funded_bootstrap_case(
        tmp_path / "funded-reverify",
        generation_id="generation-20260715-funded-reverify",
    )
    binding = frozen.capital_factory.scope_binding
    assert binding.authorization_scope == bootstrap.FUNDED_LIVE_SCOPE
    frozen.capital_factory.require_authorization_scope(bootstrap.FUNDED_LIVE_SCOPE)
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH",
    ):
        frozen.capital_factory.require_authorization_scope(
            bootstrap.DEMO_PROBATION_SCOPE
        )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH",
    ):
        frozen.capital_factory(_authority_request())

    injected = (
        Ed25519PrivateKey.generate()
        .public_key()
        .public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
    )
    frozen.capital_factory._authorization_key = (
        authority_factory.canonical_ed25519_key_id(injected),
        injected,
    )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match=(
            "CAPITAL_BOOTSTRAP_(?:RECEIPT_BINDING_INVALID|"
            "FUNDED_AUTHORIZATION_TRUST_MISMATCH)"
        ),
    ):
        frozen.capital_factory(_funded_authority_request())


def test_funded_response_cannot_splice_across_generation_identity(
    tmp_path: Path,
) -> None:
    key_root = tmp_path / "keys"
    key_root.mkdir()
    _, precommit_source = _write_key_pair(key_root, "precommit")
    _, replay_source = _write_key_pair(key_root, "replay")
    authorization_private = Ed25519PrivateKey.generate()
    authorization_public = authorization_private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    authorization_key_id = authority_factory.canonical_ed25519_key_id(
        authorization_public
    )
    authorization_public_path = key_root / "authorization.public"
    authorization_public_path.write_bytes(authorization_public)
    exchange_base = tmp_path / "exchange"
    exchange_base.mkdir()
    trust = _funded_trust(authorization_public, authorization_key_id, exchange_base)
    boundary = bootstrap.FundedAuthorityBoundary(
        precommitment_signing_key_source=precommit_source,
        replay_signing_key_source=replay_source,
        authorization_public_key_source=authority_factory.KeyMaterialSource(
            path=authorization_public_path
        ),
    )

    first_root = tmp_path / "first"
    first_root.mkdir()
    first_kwargs = _bootstrap_kwargs(
        first_root,
        funded=True,
        funded_trust=trust,
        generation_id="generation-20260715-101",
    )
    first_kwargs["funded_boundary"] = boundary
    first = bootstrap.bootstrap_production_capital_authority(**first_kwargs)
    first_inputs = first.capital_factory(_funded_authority_request())
    with pytest.raises(coordinator.ProductionAuthorityBlocked):
        first_inputs.authorization_signer(_authorization_body(first_inputs))
    first_exchange = first.capital_factory._authorization_exchange_root
    assert first_exchange is not None
    first_request_path = next(
        item
        for item in first_exchange.rglob("*.json")
        if not {"incoming", "accepted", "issued"}.intersection(item.parts)
    )
    first_request = json.loads(first_request_path.read_text("utf-8"))
    first_response_path = first_exchange / "incoming" / f"{_sha(first_request)}.json"
    _write_external_authorization_response(
        first_response_path, first_request, authorization_private
    )

    second_root = tmp_path / "second"
    second_root.mkdir()
    second_kwargs = _bootstrap_kwargs(
        second_root,
        funded=True,
        funded_trust=trust,
        generation_id="generation-20260715-102",
    )
    second_kwargs["funded_boundary"] = boundary
    second = bootstrap.bootstrap_production_capital_authority(**second_kwargs)
    second_inputs = second.capital_factory(_funded_authority_request())
    with pytest.raises(coordinator.ProductionAuthorityBlocked):
        second_inputs.authorization_signer(_authorization_body(second_inputs))
    second_exchange = second.capital_factory._authorization_exchange_root
    assert second_exchange is not None
    second_request_path = next(
        item
        for item in second_exchange.rglob("*.json")
        if not {"incoming", "accepted", "issued"}.intersection(item.parts)
    )
    second_request = json.loads(second_request_path.read_text("utf-8"))
    assert _sha(second_request) != _sha(first_request)
    second_response_path = second_exchange / "incoming" / f"{_sha(second_request)}.json"
    second_response_path.write_bytes(first_response_path.read_bytes())
    replayed_inputs = second.capital_factory(_funded_authority_request())
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_RESPONSE_BINDING_MISMATCH",
    ):
        replayed_inputs.authorization_signer(_authorization_body(replayed_inputs))


def test_bootstrap_request_and_ingress_hardlinks_fail_closed(tmp_path: Path) -> None:
    demo_root = tmp_path / "demo"
    demo_root.mkdir()
    demo = bootstrap.bootstrap_production_capital_authority(
        **_bootstrap_kwargs(demo_root)
    )
    bootstrap_path = demo.generation_root.joinpath(
        *PurePosixPath(demo.bootstrap_receipt.relative_path).parts
    )
    _hardlink_or_skip(bootstrap_path, tmp_path / "bootstrap-hardlink.json")
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_ARTIFACT_INVALID",
    ):
        demo.capital_factory(_authority_request())

    request_case, _ = _funded_bootstrap_case(
        tmp_path / "request-hardlink",
        generation_id="generation-20260715-request-hardlink",
    )
    request_inputs = request_case.capital_factory(_funded_authority_request())
    with pytest.raises(coordinator.ProductionAuthorityBlocked):
        request_inputs.authorization_signer(_authorization_body(request_inputs))
    request_exchange = request_case.capital_factory._authorization_exchange_root
    assert request_exchange is not None
    request_path = _request_artifact(request_exchange)
    _hardlink_or_skip(request_path, tmp_path / "request-hardlink.json")
    request_rerun = request_case.capital_factory(_funded_authority_request())
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_ARTIFACT_INVALID",
    ):
        request_rerun.authorization_signer(_authorization_body(request_rerun))

    ingress_case, authorization_private = _funded_bootstrap_case(
        tmp_path / "ingress-hardlink",
        generation_id="generation-20260715-ingress-hardlink",
    )
    ingress_inputs = ingress_case.capital_factory(_funded_authority_request())
    with pytest.raises(coordinator.ProductionAuthorityBlocked):
        ingress_inputs.authorization_signer(_authorization_body(ingress_inputs))
    ingress_exchange = ingress_case.capital_factory._authorization_exchange_root
    assert ingress_exchange is not None
    ingress_request = json.loads(_request_artifact(ingress_exchange).read_text("utf-8"))
    external_response = tmp_path / "external-response.json"
    _write_external_authorization_response(
        external_response, ingress_request, authorization_private
    )
    ingress_path = ingress_exchange / "incoming" / f"{_sha(ingress_request)}.json"
    _hardlink_or_skip(external_response, ingress_path)
    ingress_rerun = ingress_case.capital_factory(_funded_authority_request())
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_EXCHANGE_HARDLINK_FORBIDDEN",
    ):
        ingress_rerun.authorization_signer(_authorization_body(ingress_rerun))


def test_accepted_response_is_distinct_inode_and_hardlink_fails_closed(
    tmp_path: Path,
) -> None:
    frozen, authorization_private = _funded_bootstrap_case(
        tmp_path / "accepted-hardlink",
        generation_id="generation-20260715-accepted-hardlink",
    )
    inputs = frozen.capital_factory(_funded_authority_request())
    with pytest.raises(coordinator.ProductionAuthorityBlocked):
        inputs.authorization_signer(_authorization_body(inputs))
    exchange_root = frozen.capital_factory._authorization_exchange_root
    assert exchange_root is not None
    request = json.loads(_request_artifact(exchange_root).read_text("utf-8"))
    ingress_path = exchange_root / "incoming" / f"{_sha(request)}.json"
    _write_external_authorization_response(ingress_path, request, authorization_private)
    rerun = frozen.capital_factory(_funded_authority_request())
    rerun.authorization_signer(_authorization_body(rerun))
    accepted = list((exchange_root / "accepted").rglob("*.json"))
    assert len(accepted) == 1
    ingress_info = ingress_path.stat()
    accepted_info = accepted[0].stat()
    assert ingress_info.st_nlink == accepted_info.st_nlink == 1
    assert (ingress_info.st_dev, ingress_info.st_ino) != (
        accepted_info.st_dev,
        accepted_info.st_ino,
    )

    _hardlink_or_skip(accepted[0], tmp_path / "accepted-hardlink.json")
    hardlinked = frozen.capital_factory(_funded_authority_request())
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_EXCHANGE_HARDLINK_FORBIDDEN",
    ):
        hardlinked.authorization_signer(_authorization_body(hardlinked))


def test_exchange_bounds_directories_depth_and_path_length(
    tmp_path: Path, monkeypatch
) -> None:
    count_case, _ = _funded_bootstrap_case(
        tmp_path / "directory-count",
        generation_id="generation-20260715-directory-count",
    )
    count_root = count_case.capital_factory._authorization_exchange_root
    assert count_root is not None
    for index in range(bootstrap.MAX_AUTHORIZATION_EXCHANGE_DIRECTORIES):
        (count_root / f"extra-{index:02d}").mkdir()
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_EXCHANGE_DIRECTORY_BOUND_EXCEEDED",
    ):
        bootstrap._bounded_exchange_files(count_root)

    depth_case, _ = _funded_bootstrap_case(
        tmp_path / "directory-depth",
        generation_id="generation-20260715-directory-depth",
    )
    depth_root = depth_case.capital_factory._authorization_exchange_root
    assert depth_root is not None
    nested = depth_root
    for index in range(bootstrap.MAX_AUTHORIZATION_EXCHANGE_DEPTH + 1):
        nested = nested / f"depth-{index}"
        nested.mkdir()
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_EXCHANGE_DEPTH_EXCEEDED",
    ):
        bootstrap._bounded_exchange_files(depth_root)

    path_case, _ = _funded_bootstrap_case(
        tmp_path / "path-length",
        generation_id="generation-20260715-path-length",
    )
    path_root = path_case.capital_factory._authorization_exchange_root
    assert path_root is not None
    child = path_root / "child"
    child.mkdir()
    monkeypatch.setattr(
        bootstrap, "MAX_AUTHORIZATION_EXCHANGE_PATH_CHARS", len(str(path_root))
    )
    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="FUNDED_AUTHORIZATION_EXCHANGE_PATH_LENGTH_EXCEEDED",
    ):
        bootstrap._bounded_exchange_files(path_root)


@pytest.mark.parametrize(
    ("scanner", "limit", "error"),
    (
        (
            bootstrap._bounded_exchange_files,
            bootstrap.MAX_AUTHORIZATION_EXCHANGE_DIRECTORIES,
            "FUNDED_AUTHORIZATION_EXCHANGE_DIRECTORY_BOUND_EXCEEDED",
        ),
        (
            bootstrap._existing_regular_files,
            bootstrap.MAX_BOOTSTRAP_DIRECTORIES,
            "CAPITAL_BOOTSTRAP_STORAGE_DIRECTORY_BOUND_EXCEEDED",
        ),
    ),
)
def test_directory_discovery_is_bounded_before_stack_growth(
    tmp_path: Path, monkeypatch, scanner, limit: int, error: str
) -> None:
    consumed = 0
    scandir_calls = 0
    listdir_calls = 0
    root = tmp_path / f"lazy-scan-{limit}"
    root.mkdir()

    class FakeEntry:
        def __init__(self, name: str) -> None:
            self.name = name

    class FakeScandir:
        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback) -> None:
            return None

        def __iter__(self):
            nonlocal consumed
            for index in range(limit + 2):
                consumed += 1
                if consumed > limit:
                    raise AssertionError("directory iterator over-consumed")
                yield FakeEntry(f"child-{index:04d}")

    def lazy_scandir(path):
        nonlocal scandir_calls
        assert Path(path) == root
        scandir_calls += 1
        return FakeScandir()

    def forbidden_listdir(_path):
        nonlocal listdir_calls
        listdir_calls += 1
        raise AssertionError("eager os.listdir must not be used")

    real_lstat = bootstrap.os.lstat

    def no_follow_lstat(path):
        if Path(path) == root:
            return real_lstat(path)
        return SimpleNamespace(
            st_mode=stat.S_IFDIR,
            st_nlink=1,
            st_size=0,
            st_file_attributes=0,
        )

    with monkeypatch.context() as scoped:
        scoped.setattr(bootstrap.os, "scandir", lazy_scandir)
        scoped.setattr(bootstrap.os, "listdir", forbidden_listdir)
        scoped.setattr(bootstrap.os, "lstat", no_follow_lstat)
        with pytest.raises(bootstrap.ProductionCapitalBootstrapError, match=error):
            scanner(root)
    assert consumed == limit
    assert scandir_calls == 1
    assert listdir_calls == 0


def test_storage_is_content_addressed_bounded_and_non_overwriting(
    tmp_path: Path,
) -> None:
    kwargs = _bootstrap_kwargs(tmp_path)
    frozen = bootstrap.bootstrap_production_capital_authority(**kwargs)
    files = [item for item in frozen.generation_root.rglob("*") if item.is_file()]
    assert len(files) == bootstrap.MAX_BOOTSTRAP_ARTIFACTS
    assert sum(item.stat().st_size for item in files) < (
        bootstrap.MAX_BOOTSTRAP_GENERATION_BYTES
    )
    assert all(
        item.name.startswith(_sha(json.loads(item.read_text("utf-8"))))
        for item in files
    )

    with pytest.raises(
        bootstrap.ProductionCapitalBootstrapError,
        match="CAPITAL_BOOTSTRAP_GENERATION_ALREADY_POPULATED",
    ):
        bootstrap.bootstrap_production_capital_authority(**kwargs)
    assert (
        len([item for item in frozen.generation_root.rglob("*") if item.is_file()]) == 2
    )
