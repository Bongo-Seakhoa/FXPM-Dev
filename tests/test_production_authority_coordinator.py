from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_cap_calibration as cap
import pm_causal_evidence_producer as causal_producer
import pm_causal_process as causal
import pm_current_refit_execution as current_execution
import pm_production_authority_coordinator as coordinator


VALIDATION_TIME_UTC = "2026-07-14T00:00:00Z"


def _sha(character: str) -> str:
    return character * 64


def _anchor(anchor_id: str = "A1") -> dict:
    return {
        "anchor_id": anchor_id,
        "selection_cutoff_utc": "2020-01-01T00:00:00Z",
        "forward_start_date": "2020-02-05",
        "forward_end_date": "2021-02-03",
        "purge_calendar_days": 35,
        "horizon_binding_count": book.FAMILY_SIZE,
        "horizon_binding_roster_sha256": _sha("a"),
    }


def _causal_precommit(anchor: dict) -> dict:
    fields = (
        "anchor_id",
        "selection_cutoff_utc",
        "forward_start_date",
        "forward_end_date",
        "purge_calendar_days",
    )
    return {
        "process_definition": {"anchor_plan": [{name: anchor[name] for name in fields}]}
    }


def _current_product(
    tmp_path: Path | None = None,
    *,
    evidence_producer=lambda _request: {},
) -> coordinator.CurrentRefitProduct:
    ledger = {"EURUSD": {"symbol": "EURUSD", "regime_configs": {}}}
    raw = json.dumps(ledger, sort_keys=True, separators=(",", ":")).encode()
    ledger_path = (
        (tmp_path / "derived-current.json")
        if tmp_path
        else Path("derived-current.json")
    )
    if tmp_path is not None:
        ledger_path.write_bytes(raw)
    return coordinator.CurrentRefitProduct(
        current_book_decision={"receipt_sha256": _sha("b")},
        accepted_book_generation_receipt={"payload": {"current_cutoff_utc": "x"}},
        current_refit_receipt={},
        raw_manifest_path=Path("raw.json"),
        artifact_root=Path("artifacts"),
        detector_ledger_records=(),
        replay_candidates=(),
        replayed_cell_ids=(),
        portfolio_evidence_producer=evidence_producer,
        staged_ledger_path=ledger_path,
        staged_ledger=ledger,
        staged_ledger_content_sha256=hashlib.sha256(raw).hexdigest(),
        physical_ledger_sha256=coordinator.replay.canonical_json_sha256(ledger),
    )


def _historical_state() -> coordinator._HistoricalState:
    return coordinator._HistoricalState(
        run_receipt=SimpleNamespace(receipt_sha256=_sha("1")),
        run_body={
            "anchors": [
                {
                    "anchor_id": "A1",
                    "book_decision_sha256": _sha("2"),
                    "book_generation_sha256": _sha("3"),
                }
            ]
        },
        process_decision={"decision": "OFFLINE_PROCESS_VALIDATION_PASS"},
        precommit_payload={},
        evidence_payload={},
        reconciliation_payload={},
        historical_book_decision={"receipt_sha256": _sha("2")},
        capital_bootstrap_identity_sha256=_sha("d"),
        capital_candidates_by_phase={},
        capital_lineage_by_phase={},
    )


def _assembly_inputs(tmp_path: Path) -> coordinator.AssemblyInputs:
    return coordinator.AssemblyInputs(
        deployment_root=tmp_path,
        target_name="authority-test",
        generation_id="generation-test",
        authorization_scope="DEMO_PROBATION",
        active_config_path=tmp_path / "config.json",
        config=SimpleNamespace(data_dir="data"),
        config_payload={"pipeline": {}},
        requested_symbols=("EURUSD",),
        native_universe=SimpleNamespace(),
        current_selection_window={
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
        },
        validation_time_utc=None,
    )


def test_current_management_window_is_anchor_plan_bound_not_legacy_730_days() -> None:
    payload = {
        "pipeline": {
            "production_capital_authority": {
                "constraints": {"minimum_current_calendar_days": 252}
            }
        }
    }
    window, days = coordinator._current_management_window_contract(  # noqa: SLF001
        {"start_date": "2025-07-15", "end_date": "2026-07-15"},
        current_cutoff="2026-07-15T00:00:00Z",
        config_payload=payload,
    )
    assert days == 365.0
    assert window == {
        "mode": "common_intersection",
        "partition": "current_refit_management_replay",
        "evaluation_start": "2025-07-15T00:00:00+00:00",
        "evaluation_end": "2026-07-15T00:00:00+00:00",
    }
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CURRENT_SELECTION_WINDOW_INVALID",
    ):
        coordinator._current_management_window_contract(  # noqa: SLF001
            {"start_date": "2026-01-01", "end_date": "2026-07-15"},
            current_cutoff="2026-07-15T00:00:00Z",
            config_payload=payload,
        )


def _stub_environment_reopen(monkeypatch) -> None:
    monkeypatch.setattr(
        coordinator,
        "_active_production_environment",
        lambda *_args, **_kwargs: {"native_universe_sha256": _sha("1")},
    )


def _scope_binding() -> SimpleNamespace:
    authority_public = b"a" * 32
    return SimpleNamespace(
        authorization_scope="DEMO_PROBATION",
        bootstrap_identity_sha256=_sha("d"),
        config_sha256=hashlib.sha256(
            coordinator._canonical_bytes({"pipeline": {}})
        ).hexdigest(),
        authority_key_id="authority",
        authority_public_key_sha256=hashlib.sha256(authority_public).hexdigest(),
    )


def _derived_environment(tmp_path: Path, monkeypatch) -> tuple[dict, dict]:
    broker_path = tmp_path / "broker.json"
    broker_payload = {"EURUSD": {"tick_size": 0.00001, "tick_value": 1.0}}
    broker_path.write_text(
        json.dumps(broker_payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    config_payload = {"broker_specs_path": "broker.json", "pipeline": {}}
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(config_payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    native_contract = {
        "activation_id": "activation",
        "activation_manifest_sha256": _sha("1"),
        "activation_roster_sha256": _sha("2"),
        "bars_sha256_by_source": {"EURUSD|H1": _sha("3")},
        "sidecar_sha256_by_source": {"EURUSD|H1": _sha("4")},
        "symbols": ["EURUSD"],
        "timeframes": list(coordinator.replay.PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES),
    }
    monkeypatch.setattr(
        coordinator.replay,
        "active_native_portfolio_contract",
        lambda *_args, **_kwargs: (native_contract, []),
    )
    inputs = {
        "active_config_path": config_path,
        "config": SimpleNamespace(
            data_dir="data", _deployment_config_path=str(config_path)
        ),
        "config_payload": config_payload,
        "requested_symbols": ("EURUSD",),
        "native_universe": SimpleNamespace(
            activation_id="activation",
            activation_manifest_sha256=_sha("1"),
        ),
    }
    return coordinator.derive_production_environment_authority(**inputs), inputs


class _ScopeBoundSigner:
    def __init__(self, binding=None) -> None:
        self.scope_binding = _scope_binding() if binding is None else binding

    def require_authorization_scope(self, value: str) -> None:
        assert value == self.scope_binding.authorization_scope

    def __call__(self, _body):
        return {}


class _ScopeBoundFactory:
    def __init__(self, callback, binding=None) -> None:
        self._callback = callback
        self.scope_binding = _scope_binding() if binding is None else binding

    def require_authorization_scope(self, value: str) -> None:
        assert value == self.scope_binding.authorization_scope

    def __call__(self, request):
        return self._callback(request)


def _capital_inputs(
    tmp_path: Path, binding: SimpleNamespace | None = None
) -> coordinator.CapitalAuthorityInputs:
    binding = _scope_binding() if binding is None else binding
    return coordinator.CapitalAuthorityInputs(
        output_root=tmp_path / "capital",
        constraints=cap.CapCalibrationConstraints(),
        precommitment_receipt={},
        trusted_precommitment_public_keys={},
        frozen_candidate_rosters_by_phase={},
        lineage_by_phase={},
        replay_output_loader=lambda _spec: {},
        generated_at_utc="2026-07-14T00:00:00Z",
        replay_evidence_signer=lambda _body: {},
        trusted_replay_public_keys={},
        authority_key_id="authority",
        authorization_issued_at_utc="2026-07-14T00:00:00Z",
        authorization_expires_at_utc="2026-07-15T00:00:00Z",
        authorization_signer=_ScopeBoundSigner(binding),
        trusted_authority_public_keys={"authority": b"a" * 32},
        authorization_scope=binding.authorization_scope,
        bootstrap_identity_sha256=binding.bootstrap_identity_sha256,
        capital_config_sha256=binding.config_sha256,
        authority_public_key_sha256=binding.authority_public_key_sha256,
    )


def _capital_candidate(
    candidate_id: str, *, minute: int
) -> coordinator.replay.CandidateTrade:
    return coordinator.replay.CandidateTrade(
        candidate_id=candidate_id,
        cell_id=f"EURUSD|H1|TREND|{candidate_id}",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="TrendStrategy",
        promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
        asset_class="fx",
        risk_groups=("USD",),
        entry_time=f"2020-03-01T00:{minute:02d}:00Z",
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=1_000.0,
        volume_min=0.01,
        volume_max=1.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(
            coordinator.replay.ExitLeg(
                exit_time=f"2020-03-01T01:{minute:02d}:00Z",
                fraction=1.0,
                net_pnl_per_entry_lot=10.0,
                price_pnl_per_entry_lot=10.0,
                swap_per_entry_lot=0.0,
                exit_reason="test",
            ),
        ),
        source_schema="position-events-v2",
        margin_model=coordinator.PRODUCTION_MARGIN_MODEL,
    )


def test_causal_anchor_geometry_cannot_be_relabelled() -> None:
    anchor = _anchor()
    precommit = _causal_precommit(anchor)
    changed = dict(anchor)
    changed["forward_end_date"] = "2021-02-04"

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="HISTORICAL_CAUSAL_ANCHOR_GEOMETRY_MISMATCH",
    ):
        coordinator._assert_anchor_plan(precommit, [changed])


def test_external_signature_callback_cannot_replace_signed_body() -> None:
    expected = {"schema_version": "raw", "value": 1}
    envelope = {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": "external",
        "body": {"schema_version": "raw", "value": 2},
        "signature_base64": "signed-elsewhere",
    }

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="EXTERNAL_BODY_CHANGED",
    ):
        coordinator._signed_exact_body(envelope, expected, code="EXTERNAL_BODY_CHANGED")


def test_current_book_mapping_is_reconstructed_as_owned_decision() -> None:
    from tests.test_book_multiplicity import _terminal_family

    source = book.evaluate_historical_book_multiplicity(_terminal_family())
    decision, body = coordinator._owned_book_decision(
        source.to_dict(), code_prefix="CURRENT"
    )

    assert type(decision) is book.BookMultiplicityDecision
    assert body == source.to_dict()


def test_raw_current_refit_failure_blocks_before_portfolio_claim(
    tmp_path: Path, monkeypatch
) -> None:
    evidence_calls: list[object] = []
    current_requests: list[dict] = []
    product = _current_product(
        tmp_path, evidence_producer=lambda request: evidence_calls.append(request) or {}
    )
    monkeypatch.setattr(
        current_execution,
        "verify_raw_current_refit_execution",
        lambda *_args, **_kwargs: {
            "verified": False,
            "errors": ["RAW_OUTPUT_MISMATCH"],
        },
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CURRENT_REFIT_RAW_REVERIFY_FAILED",
    ):
        coordinator._run_current(
            _historical_state(),
            _assembly_inputs(tmp_path),
            lambda request: current_requests.append(request.to_dict()) or product,
        )

    assert evidence_calls == []
    assert current_requests == [
        {
            "schema_version": coordinator.COORDINATOR_SCHEMA,
            "historical_run_receipt_sha256": _sha("1"),
            "historical_process_decision_sha256": causal.canonical_sha256(
                _historical_state().process_decision
            ),
            "historical_process_decision": _historical_state().process_decision,
            "historical_book_receipt_sha256": _sha("2"),
            "causal_precommit_payload": {},
            "causal_evidence_payload": {},
            "causal_reconciliation_payload": {},
            "authority_claimed": False,
        }
    ]


def test_current_callback_cannot_mutate_coordinator_window_or_config_facts(
    tmp_path: Path, monkeypatch
) -> None:
    inputs = replace(
        _assembly_inputs(tmp_path),
        config_payload={
            "pipeline": {
                "production_capital_authority": {
                    "constraints": {"minimum_current_calendar_days": 252}
                }
            }
        },
        current_selection_window={
            "start_date": "2025-07-15",
            "end_date": "2026-07-15",
        },
    )
    product = replace(
        _current_product(tmp_path),
        accepted_book_generation_receipt={
            "payload": {"current_cutoff_utc": "2026-07-15T00:00:00Z"}
        },
    )
    monkeypatch.setattr(
        current_execution,
        "verify_raw_current_refit_execution",
        lambda *_args, **_kwargs: {"verified": True, "errors": []},
    )
    monkeypatch.setattr(
        coordinator, "_owned_book_decision", lambda *_args, **_kwargs: ({}, {})
    )
    captured: dict[str, object] = {}

    def capture_window(selection_window, *, current_cutoff, config_payload):
        captured["selection_window"] = json.loads(json.dumps(selection_window))
        captured["config_payload"] = json.loads(json.dumps(config_payload))
        captured["current_cutoff"] = current_cutoff
        raise coordinator.ProductionAuthorityBlocked("STOP_AFTER_WINDOW_CAPTURE")

    monkeypatch.setattr(
        coordinator, "_current_management_window_contract", capture_window
    )

    def mutating_producer(_request):
        inputs.current_selection_window["start_date"] = "2025-09-01"
        inputs.config_payload["pipeline"]["production_capital_authority"][
            "constraints"
        ]["minimum_current_calendar_days"] = 300
        return product

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="STOP_AFTER_WINDOW_CAPTURE",
    ):
        coordinator._run_current(
            _historical_state(),
            inputs,
            mutating_producer,
        )

    assert captured == {
        "selection_window": {
            "start_date": "2025-07-15",
            "end_date": "2026-07-15",
        },
        "config_payload": {
            "pipeline": {
                "production_capital_authority": {
                    "constraints": {"minimum_current_calendar_days": 252}
                }
            }
        },
        "current_cutoff": "2026-07-15T00:00:00Z",
    }


def test_historical_causal_fail_cannot_reach_current_phase(
    tmp_path: Path, monkeypatch
) -> None:
    anchor = _anchor()
    precommit = _causal_precommit(anchor)
    context = spool.freeze_generation_context(
        (tmp_path / "causal-factory-spool").resolve(),
        generation_id="coordinator-causal-factory-test",
        native_universe_sha256=_sha("1"),
        config_sha256=_sha("2"),
    )
    fake_precommit = SimpleNamespace(receipt_sha256=_sha("4"))
    run_body = {
        "anchors": [
            {
                "anchor_id": "A1",
                "book_decision_sha256": _sha("5"),
                "book_generation_sha256": _sha("6"),
            }
        ]
    }
    fake_run = SimpleNamespace(receipt_sha256=_sha("7"), to_dict=lambda: run_body)
    monkeypatch.setattr(
        coordinator.historical,
        "precommit_historical_anchor_run",
        lambda *_args, **_kwargs: fake_precommit,
    )
    monkeypatch.setattr(
        coordinator.historical,
        "execute_historical_anchor_run",
        lambda *_args, **_kwargs: fake_run,
    )
    monkeypatch.setattr(
        coordinator.historical,
        "verify_historical_anchor_run_receipt",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        coordinator,
        "_verified_book_decision",
        lambda *_args, **_kwargs: {"receipt_sha256": _sha("5")},
    )

    class FailedDecision:
        @staticmethod
        def body():
            return {
                "decision": "OFFLINE_PROCESS_VALIDATION_FAIL",
                "failure_reasons": ["negative_stress_growth"],
                "insufficiency_reasons": [],
            }

    monkeypatch.setattr(
        causal, "evaluate_historical_process", lambda *_args: FailedDecision()
    )
    monkeypatch.setattr(
        causal, "verify_historical_process_decision", lambda *_args: True
    )
    fabricated_product = coordinator.CausalEvidenceProduct(
        evidence_payload={},
        reconciliation_payload={},
        historical_book_decision={"receipt_sha256": _sha("5")},
    )
    inputs = coordinator.HistoricalAuthorityInputs(
        context=context,
        run_id="run",
        causal_precommit_payload=precommit,
        anchors=[anchor],
        optimizer_producer_release_sha256=_sha("8"),
        anchor_producers={"A1": lambda _request: ()},
        causal_evidence_factory=lambda _request: fabricated_product,
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAUSAL_EVIDENCE_FACTORY_NOT_CANONICAL",
    ):
        coordinator._run_historical(inputs)

    monkeypatch.setattr(
        causal_producer.BoundedCausalEvidenceProducer,
        "__call__",
        lambda _self, _request: fabricated_product,
    )
    canonical_producer = causal_producer.BoundedCausalEvidenceProducer(
        context=None,
        historical_precommit=None,
        historical_run_receipt=None,
        causal_precommit_payload={},
        forward_replay_product=None,
    )
    monkeypatch.setattr(
        causal_producer.BoundedCausalEvidenceFactory,
        "build_after_historical_run",
        lambda _self, **_kwargs: canonical_producer,
    )
    capital_phase_binding = object()
    monkeypatch.setattr(
        causal_producer.capital_bootstrap,
        "verify_capital_forward_phase_binding",
        lambda observed_context, observed_binding: (
            {
                "bootstrap_identity_sha256": _sha("d"),
                "bootstrap_identity": {
                    "process_precommitment_sha256": causal.canonical_sha256(precommit),
                    "process_definition_sha256": precommit.get(
                        "process_definition_sha256"
                    ),
                },
            }
            if observed_context is context and observed_binding is capital_phase_binding
            else pytest.fail("capital phase binding was substituted")
        ),
    )
    canonical_factory = causal_producer.BoundedCausalEvidenceFactory(
        context=context,
        causal_precommit_payload=precommit,
        capital_phase_binding=capital_phase_binding,
        execution_products_by_anchor={"A1": {}},
        valuation_products_by_anchor={"A1": {}},
        config=object(),
    )
    monkeypatch.setattr(
        causal_producer.BoundedCausalEvidenceProducer,
        "__call__",
        lambda _self, _request: (_ for _ in ()).throw(RuntimeError("producer failed")),
    )
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAUSAL_EVIDENCE_PRODUCTION_FAILED",
    ):
        coordinator._run_historical(
            replace(inputs, causal_evidence_factory=canonical_factory)
        )

    canonical_factory = causal_producer.BoundedCausalEvidenceFactory(
        context=context,
        causal_precommit_payload=precommit,
        capital_phase_binding=capital_phase_binding,
        execution_products_by_anchor={"A1": {}},
        valuation_products_by_anchor={"A1": {}},
        config=object(),
    )
    monkeypatch.setattr(
        causal_producer.BoundedCausalEvidenceProducer,
        "__call__",
        lambda _self, _request: fabricated_product,
    )
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked, match="CAUSAL_PROCESS_NOT_PASS"
    ):
        coordinator._run_historical(
            replace(inputs, causal_evidence_factory=canonical_factory)
        )


def _current_state_for_lineage(
    tmp_path: Path | None = None,
) -> coordinator._CurrentState:
    refit = {
        "historical_process_decision_sha256": _sha("a"),
        "accepted_book_generation_receipt_sha256": _sha("c"),
        "current_refit_receipt_sha256": _sha("d"),
        "current_refit_execution_sha256": _sha("e"),
        "current_refit_cell_count": 1,
        "current_selected_winner_roster_sha256": _sha("f"),
        "current_refit_output_book_sha256": _sha("0"),
    }
    native = {
        "native_activation_id": "activation",
        "native_activation_manifest_sha256": _sha("1"),
        "native_activation_roster_sha256": _sha("2"),
        "native_bars_sha256_by_source": {"EURUSD|H1": _sha("3")},
        "native_sidecar_sha256_by_source": {"EURUSD|H1": _sha("4")},
    }
    return coordinator._CurrentState(
        product=_current_product(tmp_path),
        roster={"current_refit_lineage": refit},
        candidates=(),
        portfolio_evidence={},
        portfolio_lineage={},
        cost_contract={"cost": "exact"},
        policies={},
        validation_policy={},
        native_identity=native,
        margin_contract_sha256=_sha("5"),
        margin_terms_sha256=_sha("6"),
        broker_terms_sha256=_sha("7"),
        current_selection_window={
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
        },
    )


def test_self_consistent_capital_callback_lineage_cannot_override_current_book() -> (
    None
):
    current = _current_state_for_lineage()
    contract = {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(current.cost_contract),
        "native_source_contract_sha256": cap.canonical_json_sha256(
            current.native_identity
        ),
        "replay_contract_sha256": coordinator._capital_replay_contract_sha256(),
        "margin_contract_sha256": _sha("5"),
    }
    refit = current.roster["current_refit_lineage"]
    supplied = {
        **contract,
        "book_role": "PROCESS_VALIDATED_CURRENT_REFIT",
        "book_decision_sha256": _sha("9"),
        "book_generation_receipt_sha256": refit[
            "accepted_book_generation_receipt_sha256"
        ],
        "refit_receipt_sha256": refit["current_refit_receipt_sha256"],
        "refit_execution_sha256": refit["current_refit_execution_sha256"],
        "output_book_sha256": refit["current_refit_output_book_sha256"],
        "winner_roster_sha256": refit["current_selected_winner_roster_sha256"],
        **current.native_identity,
        "margin_terms_sha256": _sha("6"),
        "broker_terms_sha256": _sha("7"),
    }

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_CURRENT_LINEAGE_MISMATCH",
    ):
        coordinator._expected_current_capital_lineage(current, contract, supplied)


def test_environment_authority_derives_exact_native_broker_and_margin_terms(
    tmp_path: Path, monkeypatch
) -> None:
    environment, inputs = _derived_environment(tmp_path, monkeypatch)

    assert environment["config_sha256"] == cap.canonical_json_sha256(
        inputs["config_payload"]
    )
    assert environment[
        "native_universe_sha256"
    ] == coordinator.replay.canonical_json_sha256(environment["native_contract"])
    assert environment["capital_contract"]["native_source_contract_sha256"] == (
        cap.canonical_json_sha256(environment["native_identity"])
    )
    assert environment["capital_contract"]["margin_contract_sha256"] == (
        cap.canonical_json_sha256(environment["margin_contract"])
    )
    assert environment["margin_terms_sha256"] == cap.canonical_json_sha256(
        environment["margin_terms"]
    )
    assert (
        environment["broker_terms_sha256"]
        == hashlib.sha256((tmp_path / "broker.json").read_bytes()).hexdigest()
    )
    assert environment["margin_contract"]["candidate_margin_model"] == (
        coordinator.PRODUCTION_MARGIN_MODEL
    )
    assert environment["margin_contract"]["forward_replay_contract_version"] == (
        "fxpm.forward-oos-replay-producer.v5"
    )


def test_environment_authority_preserves_requested_symbol_spelling(
    tmp_path: Path, monkeypatch
) -> None:
    environment, inputs = _derived_environment(tmp_path, monkeypatch)
    observed: dict[str, object] = {}

    def native_contract(data_dir, symbols, *, timeframes):
        observed["data_dir"] = data_dir
        observed["symbols"] = symbols
        observed["timeframes"] = timeframes
        return environment["native_contract"], []

    monkeypatch.setattr(
        coordinator.replay,
        "active_native_portfolio_contract",
        native_contract,
    )
    coordinator.derive_production_environment_authority(
        **{
            **inputs,
            "requested_symbols": (" Platinum ", "Palladium"),
        }
    )

    assert observed["symbols"] == ("Platinum", "Palladium")
    assert observed["timeframes"] == (
        coordinator.replay.PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES
    )


def test_environment_authority_rejects_casefold_duplicate_symbols(
    tmp_path: Path, monkeypatch
) -> None:
    _, inputs = _derived_environment(tmp_path, monkeypatch)
    monkeypatch.setattr(
        coordinator.replay,
        "active_native_portfolio_contract",
        lambda *_args, **_kwargs: pytest.fail("duplicate roster reached contract"),
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="PRODUCTION_SYMBOL_ROSTER_INVALID",
    ):
        coordinator.derive_production_environment_authority(
            **{
                **inputs,
                "requested_symbols": ("Platinum", "pLaTiNuM"),
            }
        )


def test_environment_authority_rejects_runtime_data_root_not_declared_by_config(
    tmp_path: Path, monkeypatch
) -> None:
    _, inputs = _derived_environment(tmp_path, monkeypatch)
    forged_config = SimpleNamespace(
        data_dir="alternate-data",
        _deployment_config_path=str(tmp_path / "config.json"),
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="PIPELINE_CONFIG_DATA_DIR_MISMATCH",
    ):
        coordinator.derive_production_environment_authority(
            **{**inputs, "config": forged_config}
        )


def test_environment_authority_rejects_native_object_and_generation_hash_splices(
    tmp_path: Path, monkeypatch
) -> None:
    environment, inputs = _derived_environment(tmp_path, monkeypatch)
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="NATIVE_UNIVERSE_OBJECT_MISMATCH",
    ):
        coordinator.derive_production_environment_authority(
            **{
                **inputs,
                "native_universe": SimpleNamespace(
                    activation_id="other",
                    activation_manifest_sha256=_sha("1"),
                ),
            }
        )

    assembly = coordinator.AssemblyInputs(
        deployment_root=tmp_path,
        target_name="authority",
        generation_id="generation",
        authorization_scope="DEMO_PROBATION",
        current_selection_window={
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
        },
        validation_time_utc=None,
        **inputs,
    )
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="GENERATION_NATIVE_UNIVERSE_MISMATCH",
    ):
        coordinator._active_production_environment(
            assembly,
            expected_native_universe_sha256=_sha("f"),
        )
    assert environment["native_universe_sha256"] != _sha("f")

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="GENERATION_CONFIG_MISMATCH",
    ):
        coordinator._active_production_environment(
            assembly,
            expected_native_universe_sha256=environment["native_universe_sha256"],
            expected_config_sha256=_sha("e"),
        )


def test_pre_evidence_margin_and_broker_terms_cannot_override_reopened_state() -> None:
    current = _current_state_for_lineage()
    contract = {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(current.cost_contract),
        "native_source_contract_sha256": cap.canonical_json_sha256(
            current.native_identity
        ),
        "replay_contract_sha256": coordinator._capital_replay_contract_sha256(),
        "margin_contract_sha256": current.margin_contract_sha256,
    }

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_ENVIRONMENT_TERMS_MISMATCH",
    ):
        coordinator.derive_current_capital_lineage(
            current,
            contract,
            margin_terms_sha256=_sha("8"),
            broker_terms_sha256=current.broker_terms_sha256,
        )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_RELEASE_CONTRACT_MISMATCH",
    ):
        coordinator.derive_current_capital_lineage(
            current,
            {**contract, "margin_contract_sha256": _sha("9")},
            margin_terms_sha256=current.margin_terms_sha256,
            broker_terms_sha256=current.broker_terms_sha256,
        )


def test_noncanonical_candidate_margin_model_is_rejected() -> None:
    candidate = replace(
        _capital_candidate("candidate", minute=0),
        margin_model="CALLER_ASSERTED_MARGIN",
    )
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_CANDIDATE_MARGIN_MODEL_MISMATCH",
    ):
        coordinator._require_canonical_margin_candidates(
            (candidate,), phase_id="CURRENT|CURRENT_SELECTION"
        )


def test_historical_capital_lineage_allows_phase_sources_but_not_activation_or_terms() -> (
    None
):
    current = _current_state_for_lineage()
    historical = _historical_state()
    contract = {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(current.cost_contract),
        "native_source_contract_sha256": cap.canonical_json_sha256(
            current.native_identity
        ),
        "replay_contract_sha256": coordinator._capital_replay_contract_sha256(),
        "margin_contract_sha256": current.margin_contract_sha256,
    }
    lineage = {
        **contract,
        "book_role": "ANCHOR_LOCAL_REFIT",
        "book_decision_sha256": _sha("2"),
        "book_generation_receipt_sha256": _sha("3"),
        "refit_receipt_sha256": _sha("a"),
        "refit_execution_sha256": _sha("b"),
        "output_book_sha256": _sha("c"),
        "winner_roster_sha256": _sha("d"),
        **current.native_identity,
        # Anchor-local prefix rosters legitimately differ from the full current
        # activation contract while remaining under the same activation.
        "native_activation_roster_sha256": _sha("e"),
        "native_bars_sha256_by_source": {"EURUSD|H1": _sha("f")},
        "native_sidecar_sha256_by_source": {"EURUSD|H1": _sha("0")},
        "margin_terms_sha256": current.margin_terms_sha256,
        "broker_terms_sha256": current.broker_terms_sha256,
    }
    lineages = {
        "A1|INNER_SELECTION": lineage,
        "A1|OUTER_EVALUATION": lineage,
    }
    historical = replace(historical, capital_lineage_by_phase=lineages)

    coordinator._validate_capital_anchor_lineage(
        historical, current, contract, lineages
    )
    hostile = {
        **lineages,
        "A1|OUTER_EVALUATION": {
            **lineage,
            "native_activation_id": "other-activation",
        },
    }
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_HISTORICAL_ENVIRONMENT_MISMATCH",
    ):
        coordinator._validate_capital_anchor_lineage(
            replace(historical, capital_lineage_by_phase=hostile),
            current,
            contract,
            hostile,
        )

    source_splice = {
        **lineages,
        "A1|OUTER_EVALUATION": {
            **lineage,
            "refit_receipt_sha256": _sha("9"),
        },
    }
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_HISTORICAL_SOURCE_LINEAGE_MISMATCH",
    ):
        coordinator._validate_capital_anchor_lineage(
            historical, current, contract, source_splice
        )


def test_untrusted_capital_factory_cannot_filter_anchor_roster_with_matching_loader(
    tmp_path: Path, monkeypatch
) -> None:
    phase_id = "A1|INNER_SELECTION"
    current_phase = "CURRENT|CURRENT_SELECTION"
    first = _capital_candidate("anchor-candidate-1", minute=0)
    second = _capital_candidate("anchor-candidate-2", minute=1)
    current_candidate = _capital_candidate("current-candidate-1", minute=2)
    historical = replace(
        _historical_state(),
        capital_candidates_by_phase={phase_id: (first, second)},
        capital_lineage_by_phase={phase_id: {"phase": phase_id}},
    )
    current = replace(
        _current_state_for_lineage(tmp_path), candidates=(current_candidate,)
    )
    filtered_anchor_roster = coordinator.cap_replay._frozen_roster(
        coordinator.replay.portfolio_replay_candidate_roster_receipt((first,))
    )
    exact_current_roster = coordinator.cap_replay._frozen_roster(
        coordinator.replay.portfolio_replay_candidate_roster_receipt(
            (current_candidate,)
        )
    )
    loader_calls: list[object] = []
    hostile_inputs = replace(
        _capital_inputs(tmp_path),
        precommitment_receipt={"signed": "externally"},
        trusted_precommitment_public_keys={"precommit": b"p" * 32},
        trusted_replay_public_keys={"replay": b"r" * 32},
        trusted_authority_public_keys={"authority": b"a" * 32},
        frozen_candidate_rosters_by_phase={
            phase_id: filtered_anchor_roster,
            current_phase: exact_current_roster,
        },
        lineage_by_phase={
            phase_id: {"phase": phase_id},
            current_phase: {"phase": current_phase},
        },
        replay_output_loader=lambda spec: (
            loader_calls.append(spec) or {"matches": "filtered-roster"}
        ),
    )

    monkeypatch.setattr(
        coordinator.cap,
        "_verified_envelope",
        lambda *_args, **_kwargs: (
            {
                "anchor_plan": [{"anchor_id": "A1"}],
                "contract_lineage": {},
                "current_selection_window": current.current_selection_window,
            },
            {},
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_expected_current_capital_lineage",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        coordinator,
        "_validate_capital_anchor_lineage",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_HISTORICAL_CANDIDATE_ROSTER_MISMATCH",
    ):
        coordinator._run_capital(
            historical,
            current,
            hostile_inputs,
            "2026-07-14T00:00:00Z",
        )

    assert loader_calls == []


def test_capital_precommit_window_must_equal_verified_current_window(
    tmp_path: Path, monkeypatch
) -> None:
    current = _current_state_for_lineage(tmp_path)
    inputs = replace(
        _capital_inputs(tmp_path),
        precommitment_receipt={"signed": "externally"},
        trusted_precommitment_public_keys={"precommit": b"p" * 32},
        trusted_replay_public_keys={"replay": b"r" * 32},
        trusted_authority_public_keys={"authority": b"a" * 32},
    )
    monkeypatch.setattr(
        coordinator.cap,
        "_verified_envelope",
        lambda *_args, **_kwargs: (
            {
                "anchor_plan": [{"anchor_id": "A1"}],
                "contract_lineage": {},
                "current_selection_window": {
                    "start_date": "2025-09-01",
                    "end_date": "2026-07-15",
                },
            },
            {},
        ),
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_CURRENT_SELECTION_WINDOW_MISMATCH",
    ):
        coordinator._run_capital(
            _historical_state(),
            current,
            inputs,
            VALIDATION_TIME_UTC,
        )


def test_coordinator_returns_only_verified_pre_cas_assembly(
    tmp_path: Path, monkeypatch
) -> None:
    _stub_environment_reopen(monkeypatch)
    historical_state = _historical_state()
    current = _current_state_for_lineage(tmp_path)
    current = coordinator._CurrentState(
        **{
            **asdict(current),
            "product": _current_product(tmp_path),
            "roster": {
                **current.roster,
                "roster_sha256": _sha("8"),
            },
        }
    )
    artifact = {
        "decision_sha256": _sha("9"),
        "selection": {
            "selected_policy_point_id": "selected-policy",
        },
    }
    capital_state = coordinator._CapitalState(
        artifact=artifact,
        expected_lineage={"replay_output_sha256": _sha("a")},
        signed_replay_evidence={},
        current_lineage={},
        precommitment_receipt={},
        trusted_authority_public_keys={},
        trusted_replay_public_keys={},
        trusted_precommitment_public_keys={},
    )
    monkeypatch.setattr(
        coordinator,
        "_run_historical",
        lambda _value, **_kwargs: historical_state,
    )
    monkeypatch.setattr(coordinator, "_run_current", lambda *_args, **_kwargs: current)
    monkeypatch.setattr(
        coordinator, "_run_capital", lambda *_args, **_kwargs: capital_state
    )
    observed = {}
    sentinel = SimpleNamespace(directory=tmp_path / "authority-test")

    def assemble(**kwargs):
        observed.update(kwargs)
        return sentinel

    monkeypatch.setattr(
        coordinator.authority_assembly, "assemble_authority_directory", assemble
    )
    historical_inputs = coordinator.HistoricalAuthorityInputs(
        context=SimpleNamespace(
            native_universe_sha256=_sha("1"),
            config_sha256=_scope_binding().config_sha256,
        ),
        run_id="run",
        causal_precommit_payload={},
        anchors=(),
        optimizer_producer_release_sha256=_sha("b"),
        anchor_producers={},
        causal_evidence_factory=lambda _request: None,
    )
    result = coordinator.coordinate_production_authority(
        historical_inputs=historical_inputs,
        current_refit_factory=lambda _request: _current_product(tmp_path),
        capital_factory=_ScopeBoundFactory(lambda _request: _capital_inputs(tmp_path)),
        assembly_inputs=_assembly_inputs(tmp_path),
    )

    assert result.assembly is sentinel
    assert result.staged_ledger_path == (tmp_path / "derived-current.json").resolve()
    assert (
        result.staged_ledger_content_sha256
        == hashlib.sha256(result.staged_ledger_path.read_bytes()).hexdigest()
    )
    assert result.physical_ledger_sha256 == coordinator.replay.canonical_json_sha256(
        json.loads(result.staged_ledger_path.read_text(encoding="utf-8"))
    )
    assert result.current_roster_sha256 == _sha("8")
    assert result.selected_policy_point_id == "selected-policy"
    assert observed["raw"].capital_expected_lineage == {
        "replay_output_sha256": _sha("a")
    }
    assert observed["raw"].detector_ledger_records == []


@pytest.mark.parametrize(
    "mutation",
    ("authorization_scope", "capital_config_sha256", "authority_public_key_sha256"),
)
def test_capital_scope_binding_rejects_factory_input_splice(
    tmp_path: Path, mutation: str
) -> None:
    assembly = _assembly_inputs(tmp_path)
    request = coordinator.CapitalAuthorityRequest(
        _historical_state=_historical_state(),
        _current_state=_current_state_for_lineage(tmp_path),
        validation_time_utc=VALIDATION_TIME_UTC,
        authorization_scope=assembly.authorization_scope,
    )
    inputs = _capital_inputs(tmp_path)
    replacement = {
        "authorization_scope": "FUNDED_LIVE",
        "capital_config_sha256": _sha("f"),
        "authority_public_key_sha256": _sha("e"),
    }[mutation]
    inputs = replace(inputs, **{mutation: replacement})
    factory = _ScopeBoundFactory(lambda _request: inputs)

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_AUTHORIZATION_SCOPE_BINDING_MISMATCH",
    ):
        coordinator._capital_scope_binding(
            historical_state=request._historical_state,
            capital_factory=factory,
            request=request,
            inputs=inputs,
            assembly_inputs=assembly,
        )


def test_funded_scope_requires_config_pinned_external_key(tmp_path: Path) -> None:
    config_payload = {
        "pipeline": {
            "funded_authorization_trust": {
                "authorization_key_id": "different-funded-key",
                "authorization_public_key_sha256": _sha("f"),
            }
        }
    }
    config_sha256 = hashlib.sha256(
        coordinator._canonical_bytes(config_payload)
    ).hexdigest()
    binding = SimpleNamespace(
        authorization_scope="FUNDED_LIVE",
        bootstrap_identity_sha256=_sha("d"),
        config_sha256=config_sha256,
        authority_key_id="authority",
        authority_public_key_sha256=hashlib.sha256(b"a" * 32).hexdigest(),
    )
    assembly = replace(
        _assembly_inputs(tmp_path),
        authorization_scope="FUNDED_LIVE",
        config_payload=config_payload,
    )
    request = coordinator.CapitalAuthorityRequest(
        _historical_state=_historical_state(),
        _current_state=_current_state_for_lineage(tmp_path),
        validation_time_utc=VALIDATION_TIME_UTC,
        authorization_scope="FUNDED_LIVE",
    )
    inputs = _capital_inputs(tmp_path, binding)
    factory = _ScopeBoundFactory(lambda _request: inputs, binding)

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_AUTHORIZATION_FUNDED_TRUST_MISMATCH",
    ):
        coordinator._capital_scope_binding(
            historical_state=request._historical_state,
            capital_factory=factory,
            request=request,
            inputs=inputs,
            assembly_inputs=assembly,
        )


def test_capital_scope_binding_rejects_different_historical_bootstrap(
    tmp_path: Path,
) -> None:
    assembly = _assembly_inputs(tmp_path)
    historical_state = replace(
        _historical_state(), capital_bootstrap_identity_sha256=_sha("0")
    )
    request = coordinator.CapitalAuthorityRequest(
        _historical_state=historical_state,
        _current_state=_current_state_for_lineage(tmp_path),
        validation_time_utc=VALIDATION_TIME_UTC,
        authorization_scope=assembly.authorization_scope,
    )
    inputs = _capital_inputs(tmp_path)
    factory = _ScopeBoundFactory(lambda _request: inputs)

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CAPITAL_BOOTSTRAP_PHASE_BINDING_MISMATCH",
    ):
        coordinator._capital_scope_binding(
            historical_state=historical_state,
            capital_factory=factory,
            request=request,
            inputs=inputs,
            assembly_inputs=assembly,
        )


def test_causal_failure_in_top_level_never_invokes_current_or_capital(
    tmp_path: Path, monkeypatch
) -> None:
    _stub_environment_reopen(monkeypatch)
    calls: list[str] = []

    def fail(_inputs, **_kwargs):
        calls.append("historical")
        raise coordinator.ProductionAuthorityBlocked("CAUSAL_PROCESS_NOT_PASS")

    monkeypatch.setattr(coordinator, "_run_historical", fail)
    monkeypatch.setattr(
        coordinator,
        "_run_current",
        lambda *_args: calls.append("current"),
    )
    monkeypatch.setattr(
        coordinator,
        "_run_capital",
        lambda *_args: calls.append("capital"),
    )
    historical_inputs = coordinator.HistoricalAuthorityInputs(
        context=SimpleNamespace(
            native_universe_sha256=_sha("1"),
            config_sha256=_scope_binding().config_sha256,
        ),
        run_id="run",
        causal_precommit_payload={},
        anchors=(),
        optimizer_producer_release_sha256=_sha("b"),
        anchor_producers={},
        causal_evidence_factory=lambda _request: None,
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked, match="CAUSAL_PROCESS_NOT_PASS"
    ):
        coordinator.coordinate_production_authority(
            historical_inputs=historical_inputs,
            current_refit_factory=lambda _request: _current_product(tmp_path),
            capital_factory=_ScopeBoundFactory(
                lambda _request: (
                    calls.append("capital_factory") or _capital_inputs(tmp_path)
                )
            ),
            assembly_inputs=_assembly_inputs(tmp_path),
        )

    assert calls == ["historical"]


def test_current_failure_in_top_level_never_invokes_capital_factory(
    tmp_path: Path, monkeypatch
) -> None:
    _stub_environment_reopen(monkeypatch)
    calls: list[str] = []
    monkeypatch.setattr(
        coordinator,
        "_run_historical",
        lambda _inputs, **_kwargs: _historical_state(),
    )
    monkeypatch.setattr(
        current_execution,
        "verify_raw_current_refit_execution",
        lambda *_args, **_kwargs: {
            "verified": False,
            "errors": ["RAW_OUTPUT_MISMATCH"],
        },
    )

    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CURRENT_REFIT_RAW_REVERIFY_FAILED",
    ):
        coordinator.coordinate_production_authority(
            historical_inputs=coordinator.HistoricalAuthorityInputs(
                context=SimpleNamespace(
                    native_universe_sha256=_sha("1"),
                    config_sha256=_scope_binding().config_sha256,
                ),
                run_id="run",
                causal_precommit_payload={},
                anchors=(),
                optimizer_producer_release_sha256=_sha("b"),
                anchor_producers={},
                causal_evidence_factory=lambda _request: None,
            ),
            current_refit_factory=lambda _request: (
                calls.append("current_factory") or _current_product(tmp_path)
            ),
            capital_factory=_ScopeBoundFactory(
                lambda _request: (
                    calls.append("capital_factory") or _capital_inputs(tmp_path)
                )
            ),
            assembly_inputs=_assembly_inputs(tmp_path),
        )

    assert calls == ["current_factory"]


def test_top_level_rejects_caller_authored_validation_time_before_work(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        coordinator,
        "_run_historical",
        lambda _inputs, **_kwargs: calls.append("historical"),
    )
    assembly = replace(
        _assembly_inputs(tmp_path), validation_time_utc=VALIDATION_TIME_UTC
    )
    with pytest.raises(
        coordinator.ProductionAuthorityBlocked,
        match="CALLER_VALIDATION_TIME_FORBIDDEN",
    ):
        coordinator.coordinate_production_authority(
            historical_inputs=coordinator.HistoricalAuthorityInputs(
                context=SimpleNamespace(
                    native_universe_sha256=_sha("1"),
                    config_sha256=_scope_binding().config_sha256,
                ),
                run_id="run",
                causal_precommit_payload={},
                anchors=(),
                optimizer_producer_release_sha256=_sha("b"),
                anchor_producers={},
                causal_evidence_factory=lambda _request: None,
            ),
            current_refit_factory=lambda _request: _current_product(tmp_path),
            capital_factory=_ScopeBoundFactory(
                lambda _request: _capital_inputs(tmp_path)
            ),
            assembly_inputs=assembly,
        )
    assert calls == []


def test_top_level_invokes_lazy_factories_in_causal_order_and_owns_authority_inputs(
    tmp_path: Path, monkeypatch
) -> None:
    _stub_environment_reopen(monkeypatch)
    events: list[str] = []
    historical = _historical_state()
    current = _current_state_for_lineage(tmp_path)
    current = coordinator._CurrentState(
        **{
            **asdict(current),
            "product": _current_product(tmp_path),
            "roster": {**current.roster, "roster_sha256": _sha("8")},
        }
    )
    capital = coordinator._CapitalState(
        artifact={
            "decision_sha256": _sha("9"),
            "selection": {"selected_policy_point_id": "selected-policy"},
        },
        expected_lineage={},
        signed_replay_evidence={},
        current_lineage={},
        precommitment_receipt={},
        trusted_authority_public_keys={},
        trusted_replay_public_keys={},
        trusted_precommitment_public_keys={},
    )

    def run_historical(_inputs, **_kwargs):
        events.append("historical_pass")
        return historical

    def run_current(_historical, _assembly, current_factory, **_kwargs):
        events.append("current_phase")
        current_factory(coordinator.CurrentRefitRequest(b"{}"))
        return current

    def run_capital(_historical, _current, _inputs, _validation_time):
        assert _validation_time == VALIDATION_TIME_UTC
        events.append("capital_phase")
        return capital

    def validation_clock():
        events.append("validation_clock")
        return VALIDATION_TIME_UTC

    monkeypatch.setattr(coordinator, "_run_historical", run_historical)
    monkeypatch.setattr(coordinator, "_run_current", run_current)
    monkeypatch.setattr(coordinator, "_run_capital", run_capital)
    monkeypatch.setattr(coordinator, "_trusted_validation_time_utc", validation_clock)
    captured_assembly: dict[str, object] = {}

    def assemble(**kwargs):
        events.append("assembly")
        captured_assembly["data_dir"] = kwargs["config"].data_dir
        captured_assembly["config_path"] = kwargs[
            "config"
        ]._deployment_config_path
        captured_assembly["native_activation_id"] = kwargs[
            "native_universe"
        ].activation_id
        captured_assembly["native_manifest_sha256"] = kwargs[
            "native_universe"
        ].activation_manifest_sha256
        return SimpleNamespace(directory=tmp_path / "authority")

    monkeypatch.setattr(
        coordinator.authority_assembly,
        "assemble_authority_directory",
        assemble,
    )
    assembly_inputs = replace(
        _assembly_inputs(tmp_path),
        config=SimpleNamespace(
            data_dir="data",
            _deployment_config_path=str(tmp_path / "config.json"),
        ),
        native_universe=SimpleNamespace(
            activation_id="activation",
            activation_manifest_sha256=_sha("1"),
        ),
    )

    def build_capital(request):
        events.append("capital_factory")
        assembly_inputs.config.data_dir = "alternate-data"
        assembly_inputs.config._deployment_config_path = "forged-config.json"
        assembly_inputs.native_universe.activation_id = "forged-activation"
        assembly_inputs.native_universe.activation_manifest_sha256 = _sha("f")
        if (
            type(request) is coordinator.CapitalAuthorityRequest
            and request.authorization_scope == "DEMO_PROBATION"
            and request.validation_time_utc == VALIDATION_TIME_UTC
        ):
            return _capital_inputs(tmp_path)
        return None

    coordinator.coordinate_production_authority(
        historical_inputs=coordinator.HistoricalAuthorityInputs(
            context=SimpleNamespace(
                native_universe_sha256=_sha("1"),
                config_sha256=_scope_binding().config_sha256,
            ),
            run_id="run",
            causal_precommit_payload={},
            anchors=(),
            optimizer_producer_release_sha256=_sha("b"),
            anchor_producers={},
            causal_evidence_factory=lambda _request: None,
        ),
        current_refit_factory=lambda _request: (
            events.append("current_factory") or _current_product(tmp_path)
        ),
        capital_factory=_ScopeBoundFactory(build_capital),
        assembly_inputs=assembly_inputs,
    )

    assert events == [
        "historical_pass",
        "current_phase",
        "current_factory",
        "validation_clock",
        "capital_factory",
        "capital_phase",
        "assembly",
    ]
    assert captured_assembly == {
        "data_dir": "data",
        "config_path": str(tmp_path / "config.json"),
        "native_activation_id": "activation",
        "native_manifest_sha256": _sha("1"),
    }


def test_integration_contract_places_coordinator_before_cas() -> None:
    contract = coordinator.production_authority_integration_contract()

    assert contract["callback_authority_facts_permitted"] is False
    assert contract["caller_validation_time_permitted"] is False
    assert (
        contract["authorization_validation_clock"]
        == "SAMPLED_ONCE_AFTER_CURRENT_REFIT_BEFORE_CAPITAL_FACTORY"
    )
    assert contract["caller_native_broker_or_margin_hash_permitted"] is False
    assert (
        contract["production_environment_authority"]
        == "REOPENED_BEFORE_HISTORICAL_AFTER_CURRENT_AND_BEFORE_ASSEMBLY"
    )
    assert contract["production_factory_status"] == "NOT_WIRED"
    assert "before _promote_staged_generation" in contract["pipeline_callsite"]
    assert contract["activates_ledger_or_places_trades"] is False
    assert "verification_result_canonical_json" in contract["assembly_fields"]
