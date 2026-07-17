from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_causal_process as causal
import pm_current_refit_execution as current_execution
import pm_current_refit_search_producer as producer
import pm_model_lifecycle as lifecycle
import pm_native_prefix_production as native_prefix
from pm_current_refit_search_producer import (
    CurrentRefitSearchProducerError,
    verify_causal_process_pass,
)


def _canonical(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


class _Decision:
    def __init__(self, body):
        self._body = body

    def body(self):
        return dict(self._body)


class _Request:
    def __init__(self, body):
        self.body = body

    def to_dict(self):
        return dict(self.body)


def _request_body() -> dict:
    decision = {"decision": "OFFLINE_PROCESS_VALIDATION_PASS"}
    return {
        "schema_version": producer.COORDINATOR_REQUEST_SCHEMA,
        "historical_run_receipt_sha256": "1" * 64,
        "historical_process_decision_sha256": "2" * 64,
        "historical_process_decision": decision,
        "historical_book_receipt_sha256": "3" * 64,
        "causal_precommit_payload": {"precommit": True},
        "causal_evidence_payload": {"evidence": True},
        "causal_reconciliation_payload": {"reconciliation": True},
        "authority_claimed": False,
    }


def test_causal_gate_requires_rederived_pass(monkeypatch) -> None:
    observed = {}

    def fake_evaluate(precommit, evidence, reconciliation):
        observed["evaluated"] = (precommit, evidence, reconciliation)
        return _Decision({"decision": "OFFLINE_PROCESS_VALIDATION_PASS"})

    def fake_verify(decision, precommit, evidence, reconciliation):
        observed["verified"] = True
        return True

    monkeypatch.setattr(causal, "evaluate_historical_process", fake_evaluate)
    monkeypatch.setattr(causal, "verify_historical_process_decision", fake_verify)
    result = verify_causal_process_pass(
        causal_precommit_payload={"p": 1},
        causal_evidence_payload={"e": 2},
        causal_reconciliation_payload={"r": 3},
    )
    assert observed["evaluated"] == ({"p": 1}, {"e": 2}, {"r": 3})
    assert observed["verified"] is True
    assert result["decision_body"]["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    assert result["decision_sha256"] == causal.canonical_sha256(
        {"decision": "OFFLINE_PROCESS_VALIDATION_PASS"}
    )


def test_causal_gate_rejects_unverified_decision(monkeypatch) -> None:
    monkeypatch.setattr(
        causal,
        "evaluate_historical_process",
        lambda *_a: _Decision({"decision": "OFFLINE_PROCESS_VALIDATION_PASS"}),
    )
    monkeypatch.setattr(causal, "verify_historical_process_decision", lambda *_a: False)
    with pytest.raises(
        CurrentRefitSearchProducerError, match="CAUSAL_PROCESS_RAW_REVERIFY_FAILED"
    ):
        verify_causal_process_pass(
            causal_precommit_payload={},
            causal_evidence_payload={},
            causal_reconciliation_payload={},
        )


def test_causal_gate_rejects_non_pass_with_reasons(monkeypatch) -> None:
    monkeypatch.setattr(
        causal,
        "evaluate_historical_process",
        lambda *_a: _Decision(
            {
                "decision": "OFFLINE_PROCESS_VALIDATION_FAIL",
                "failure_reasons": ["FORWARD_UNDERPERFORMANCE"],
            }
        ),
    )
    monkeypatch.setattr(causal, "verify_historical_process_decision", lambda *_a: True)
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        verify_causal_process_pass(
            causal_precommit_payload={},
            causal_evidence_payload={},
            causal_reconciliation_payload={},
        )
    assert caught.value.code == "CAUSAL_PROCESS_NOT_PASS"
    assert "FORWARD_UNDERPERFORMANCE" in caught.value.details


def test_causal_gate_and_production_inputs_accept_no_outcome_callbacks() -> None:
    assert set(inspect.signature(verify_causal_process_pass).parameters) == {
        "causal_precommit_payload",
        "causal_evidence_payload",
        "causal_reconciliation_payload",
    }
    assert set(
        inspect.signature(producer.CurrentRefitSearchProductionInputs).parameters
    ) == {
        "context",
        "search_process",
        "active_config_path",
        "config_payload",
        "pipeline_config",
        "native_products",
        "current_selection_window",
        "output_root",
    }


def test_rich_request_is_exact_and_never_caller_authorized() -> None:
    body = _request_body()
    assert producer._request_body(_Request(body)) == body  # noqa: SLF001
    for mutation in (
        {**body, "authority_claimed": True},
        {**body, "schema_version": "fxpm.wrong.v1"},
        {**body, "caller_passed": True},
    ):
        with pytest.raises(
            CurrentRefitSearchProducerError, match="CURRENT_REFIT_REQUEST_INVALID"
        ):
            producer._request_body(_Request(mutation))  # noqa: SLF001


def test_current_producer_is_one_shot(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        producer,
        "_produce",
        lambda supplied, request: calls.append((supplied, request)) or "product",
    )
    value = object.__new__(producer.CurrentRefitSearchProducer)
    value._context = object()  # noqa: SLF001
    value._search_process = object()  # noqa: SLF001
    value._frozen_config = SimpleNamespace(  # noqa: SLF001
        materialize=lambda context: producer.PipelineConfig()
    )
    value._native_products = {}  # noqa: SLF001
    value._current_selection_window = {  # noqa: SLF001
        "start_date": "2025-07-15",
        "end_date": "2026-07-15",
    }
    value._output_root = Path(".")  # noqa: SLF001
    value._lock = producer.threading.Lock()  # noqa: SLF001
    value._used = False
    request = _Request(_request_body())
    assert value(request) == "product"
    assert len(calls) == 1
    supplied, supplied_request = calls[0]
    assert type(supplied) is producer._ExecutableCurrentRefitInputs  # noqa: SLF001
    assert supplied.pipeline_config.backtest_spread_mode == "fixed_spec"
    assert supplied_request == _request_body()
    with pytest.raises(
        CurrentRefitSearchProducerError, match="CURRENT_REFIT_PRODUCER_ALREADY_CONSUMED"
    ):
        value(request)


def _producer_inputs_for_binding_test(config=None):
    return producer.CurrentRefitSearchProductionInputs(
        context=object.__new__(producer.authority_spool.GenerationSpoolContext),
        search_process=object.__new__(producer.planner.FrozenProductionSearchProcess),
        active_config_path=Path("config.json"),
        config_payload={},
        pipeline_config=config or producer.PipelineConfig(),
        native_products={},
        current_selection_window={
            "start_date": "2025-07-15",
            "end_date": "2026-07-15",
        },
        output_root=Path("."),
    )


def test_current_producer_owns_materialized_config_after_construction(
    monkeypatch,
) -> None:
    supplied = producer.PipelineConfig(backtest_spread_mode="stress_p95")
    owned = producer.PipelineConfig(backtest_spread_mode="stress_p95")
    frozen = SimpleNamespace(materialize=lambda context: owned)
    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        lambda *args, **kwargs: frozen,
    )
    observed = []
    monkeypatch.setattr(
        producer,
        "_produce",
        lambda inputs, request: observed.append(inputs.pipeline_config) or "ok",
    )
    value = producer.CurrentRefitSearchProducer(
        _producer_inputs_for_binding_test(supplied)
    )
    supplied.backtest_spread_mode = "stress_max"
    assert value(_Request(_request_body())) == "ok"
    assert observed == [owned]
    assert observed[0].backtest_spread_mode == "stress_p95"


def test_current_producer_rejects_initial_effective_config_mismatch(
    monkeypatch,
) -> None:
    def reject(*args, **kwargs):
        raise producer.config_binding.ProductionConfigBindingError(
            "EFFECTIVE_PIPELINE_CONFIG_MISMATCH", ("backtest_spread_mode",)
        )

    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        reject,
    )
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        producer.CurrentRefitSearchProducer(_producer_inputs_for_binding_test())
    assert caught.value.code == "CURRENT_PRODUCTION_CONFIG_BINDING_FAILED"
    assert caught.value.details[0] == "EFFECTIVE_PIPELINE_CONFIG_MISMATCH"


def test_current_producer_rejects_active_config_drift_at_call(monkeypatch) -> None:
    class _Frozen:
        def materialize(self, context):
            raise producer.config_binding.ProductionConfigBindingError(
                "ACTIVE_CONFIG_BYTES_CHANGED"
            )

    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        lambda *args, **kwargs: _Frozen(),
    )
    value = producer.CurrentRefitSearchProducer(_producer_inputs_for_binding_test())
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        value(_Request(_request_body()))
    assert caught.value.code == "CURRENT_PRODUCTION_CONFIG_REVERIFY_FAILED"
    assert caught.value.details[0] == "ACTIVE_CONFIG_BYTES_CHANGED"


def test_book_stream_releases_timeframe_before_yield_and_on_failure(
    monkeypatch,
) -> None:
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", ("EURUSD",))
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H1",))
    monkeypatch.setattr(producer.book, "REGIME_ORDER", ("TREND",))

    class _Sink:
        def seal(self):
            return "sealed"

    monkeypatch.setattr(
        producer.authority_spool, "open_symbol_sink", lambda *args: _Sink()
    )
    decision = SimpleNamespace(to_dict=lambda: {"reason_code": "NO_SOURCE"})
    terminal = SimpleNamespace(
        source_group_decision=object(), regime_fit_decision=decision
    )
    monkeypatch.setattr(
        producer.search_execution,
        "fit_terminal_native_timeframe",
        lambda **kwargs: terminal,
    )
    monkeypatch.setattr(
        producer.planner,
        "build_current_terminal_cell_plan",
        lambda *args, **kwargs: object(),
    )
    evidence = object()
    monkeypatch.setattr(producer, "_state_evidence", lambda **kwargs: evidence)
    releases = []
    monkeypatch.setattr(
        producer, "_release_timeframe_state", lambda: releases.append("released")
    )
    inputs = SimpleNamespace(
        context=object(),
        search_process=object(),
        pipeline_config=producer.PipelineConfig(),
        native_products={
            "EURUSD|H1": SimpleNamespace(source_state=native_prefix.SOURCE_UNAVAILABLE)
        },
    )
    compact = {}
    assert list(
        producer._book_evidence_stream(  # noqa: SLF001
            inputs,
            authorization=object(),
            selected={},
            compact_receipts=compact,
        )
    ) == [evidence]
    assert releases == ["released"]
    assert compact == {"EURUSD": "sealed"}

    releases.clear()
    monkeypatch.setattr(
        producer.search_execution,
        "fit_terminal_native_timeframe",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        list(
            producer._book_evidence_stream(  # noqa: SLF001
                inputs,
                authorization=object(),
                selected={},
                compact_receipts={},
            )
        )
    assert releases == ["released"]


def test_common_cutoff_requires_exact_ordered_sealed_roster(monkeypatch) -> None:
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", ("EURUSD",))
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H1",))
    product = native_prefix.NativePrefixProduct(
        source_state=native_prefix.SOURCE_UNAVAILABLE,
        index={
            "symbol": "EURUSD",
            "timeframe": "H1",
            "cutoff_utc": "2026-07-15T00:00:00Z",
        },
        index_descriptor={},
        index_sha256="0" * 64,
        native_prefix={},
        instrument_snapshot=None,
        prefix_path=None,
        prefix_descriptor=None,
        artifact_descriptors=(),
        _seal=native_prefix._PRODUCT_SEAL,  # noqa: SLF001
    )
    inputs = SimpleNamespace(native_products={"EURUSD|H1": product})
    assert producer._current_cutoff(inputs) == "2026-07-15T00:00:00+00:00"  # noqa: SLF001
    inputs.native_products = {"EURUSD|M5": product}
    with pytest.raises(
        CurrentRefitSearchProducerError, match="CURRENT_NATIVE_PRODUCT_ROSTER_INVALID"
    ):
        producer._current_cutoff(inputs)  # noqa: SLF001


def test_current_replay_contract_projects_the_frozen_capital_window_and_native_facts() -> (
    None
):
    execution, management = producer._current_replay_windows(  # noqa: SLF001
        {"start_date": "2025-07-15", "end_date": "2026-07-15"},
        current_cutoff="2026-07-15T00:00:00+00:00",
    )
    assert execution == {"start_date": "2025-07-15", "end_date": "2026-07-15"}
    assert management == {
        "mode": "common_intersection",
        "partition": "current_refit_management_replay",
        "evaluation_start": "2025-07-15T00:00:00+00:00",
        "evaluation_end": "2026-07-15T00:00:00+00:00",
    }
    identity = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "activation_id": "activation-v5",
        "activation_manifest_sha256": "1" * 64,
        "source_bars_sha256": "2" * 64,
        "source_audit_sidecar_sha256": "3" * 64,
        "index_sha256": "4" * 64,
        "source_identity_sha256": "5" * 64,
    }
    assert producer._management_native_source_contract(identity) == {  # noqa: SLF001
        "source_kind": "broker_native",
        "provenance_valid": True,
        "timestamp_semantics": "bar_open_utc",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "activation_id": "activation-v5",
        "activation_manifest_sha256": "1" * 64,
        "source_sha256": "2" * 64,
        "sidecar_sha256": "3" * 64,
        "native_prefix_index_sha256": "4" * 64,
        "native_source_identity_sha256": "5" * 64,
    }


@pytest.mark.parametrize(
    "window,code",
    [
        ({"start_date": "2025-07-15"}, "CURRENT_REPLAY_WINDOW_SCHEMA_INVALID"),
        (
            {"start_date": "2025-07-15", "end_date": "2026-07-14"},
            "CURRENT_REPLAY_WINDOW_CUTOFF_MISMATCH",
        ),
        (
            {"start_date": "2026-07-15", "end_date": "2026-07-15"},
            "CURRENT_REPLAY_WINDOW_CUTOFF_MISMATCH",
        ),
    ],
)
def test_current_replay_contract_rejects_unbound_or_noncausal_windows(
    window, code
) -> None:
    with pytest.raises(CurrentRefitSearchProducerError, match=code):
        producer._current_replay_windows(  # noqa: SLF001
            window,
            current_cutoff="2026-07-15T00:00:00+00:00",
        )


def test_replay_candidates_emit_standalone_verifier_window_and_source_keys(
    monkeypatch,
) -> None:
    material = _selected_material()
    cell_id = "EURUSD|H1|TREND|EMACrossoverStrategy"
    material = producer._SelectedMaterial(  # noqa: SLF001
        **{
            **{name: getattr(material, name) for name in material.__dataclass_fields__},
            "book_cell": {
                **material.book_cell,
                "cell_id": cell_id,
                "adjusted_q_value": 0.1,
            },
            "winner_process": {
                **material.winner_process,
                "strategy_class": "EMACrossoverStrategy",
                "strategy_module": "pm_strategies",
            },
            "search": producer._SelectedSearch(  # noqa: SLF001
                **{
                    **{
                        name: getattr(material.search, name)
                        for name in material.search.__dataclass_fields__
                    },
                    "source_capture": {
                        **material.search.source_capture,
                        "selection_prefix_receipt": {"receipt": "selection"},
                    },
                }
            ),
        }
    )
    candidate = producer.portfolio.CandidateTrade(
        candidate_id="raw-candidate",
        cell_id=cell_id,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="EMACrossoverStrategy",
        promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
        asset_class="fx",
        risk_groups=(),
        entry_time="2026-01-02T00:00:00Z",
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=1000.0,
        volume_min=0.01,
        volume_max=10.0,
        volume_step=0.01,
        priority_score=0.9,
        legs=(
            producer.portfolio.ExitLeg(
                exit_time="2026-01-03T00:00:00Z",
                fraction=1.0,
                net_pnl_per_entry_lot=25.0,
            ),
        ),
    )
    captured = {}

    def native_identity(
        _product, *, expected_symbol, expected_timeframe, expected_cutoff_utc, role
    ):
        del expected_cutoff_utc, role
        return {
            "symbol": expected_symbol,
            "timeframe": expected_timeframe,
            "activation_id": "activation-v5",
            "activation_manifest_sha256": "1" * 64,
            "source_bars_sha256": ("2" if expected_timeframe == "H1" else "6") * 64,
            "source_audit_sidecar_sha256": ("3" if expected_timeframe == "H1" else "7")
            * 64,
            "index_sha256": ("4" if expected_timeframe == "H1" else "8") * 64,
            "source_identity_sha256": ("5" if expected_timeframe == "H1" else "9") * 64,
        }

    def execute(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            positions=(SimpleNamespace(candidate=candidate),),
        )

    monkeypatch.setattr(
        producer.forward_replay, "_native_product_identity", native_identity
    )
    monkeypatch.setattr(
        producer.forward_replay, "_deployment_config", lambda _config: object()
    )
    monkeypatch.setattr(producer.forward_replay, "_execute_anchor_replay", execute)
    inputs = SimpleNamespace(
        context=object(),
        pipeline_config=producer.PipelineConfig(),
        current_selection_window={
            "start_date": "2025-07-15",
            "end_date": "2026-07-15",
        },
        native_products={"EURUSD|H1": object(), "EURUSD|M5": object()},
    )
    current_book = {
        "regime_fits": [
            {
                "payload": {
                    "symbol": "EURUSD",
                    "timeframe": "H1",
                    "regime_detector_record": {
                        "parameters_canonical_json": "{}",
                        "parameters_sha256": "a" * 64,
                    },
                }
            }
        ]
    }
    candidates, replayed, window, native_sources, valuation_sources = (
        producer._replay_candidates(  # noqa: SLF001
            inputs=inputs,
            current_book=current_book,
            materials=(material,),
            current_cutoff="2026-07-15T00:00:00+00:00",
            current_refit_receipt={"receipt": "current"},
        )
    )
    assert len(candidates) == 1
    assert replayed == [cell_id]
    assert captured["capital_window"] == {
        "start_date": "2025-07-15",
        "end_date": "2026-07-15",
    }
    assert window["partition"] == "current_refit_management_replay"
    assert set(native_sources) == {cell_id}
    assert set(valuation_sources) == {"EURUSD"}
    assert native_sources[cell_id]["source_sha256"] == "2" * 64
    assert valuation_sources["EURUSD"]["source_sha256"] == "6" * 64


def _selected_material() -> producer._SelectedMaterial:  # noqa: SLF001
    book_entry = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "winner_decision_sha256": "4" * 64,
        "historical_book_entry_sha256": "5" * 64,
    }
    source = {
        "fit_start_utc": "2025-07-15T01:00:00+00:00",
        "fit_end_utc": "2026-07-15T00:00:00+00:00",
        "latest_closed_bar_utc": "2026-07-14T23:00:00+00:00",
    }
    selected = producer._SelectedSearch(  # noqa: SLF001
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        product=object(),
        artifacts={},
        source_capture={"source_snapshot": source},
        winner_decision_sha256="4" * 64,
    )
    roles = (
        "source_snapshot",
        "winner_process",
        "horizon_process",
        "trial_definition",
        "refit_recipe",
        "winner_book_entry",
    )
    return producer._SelectedMaterial(  # noqa: SLF001
        book_cell=book_entry,
        search=selected,
        winner_process={
            "strategy_id": "EMACrossoverStrategy",
            "process_candidate_id": "candidate",
        },
        horizon_process={"schema_version": "horizon"},
        trial_definition={"parameters": {"fast": 10, "slow": 30}},
        refit_recipe={"schema_version": "recipe"},
        descriptors={
            role: {
                "artifact_id": str(index + 1) * 64,
                "content_sha256": str(index + 1) * 64,
            }
            for index, role in enumerate(roles)
        },
        detector={
            "regime_fit_receipt_sha256": "a" * 64,
            "regime_detector_record_sha256": "b" * 64,
            "regime_detector_parameters_sha256": "c" * 64,
            "regime_detector_code_sha256": "d" * 64,
        },
    )


def test_refit_identity_uses_book_semantic_digest_not_file_digest() -> None:
    material = _selected_material()
    cells = producer._derive_refit_cells(  # noqa: SLF001
        materials=[material],
        accepted_receipt_sha256="e" * 64,
        historical_process_decision_sha256="f" * 64,
    )
    assert len(cells) == 1
    cell = cells[0]
    assert cell["winner_book_entry_sha256"] == "5" * 64
    assert cell[
        "winner_book_entry_sha256"
    ] != producer.current_artifacts.canonical_artifact_sha256(material.book_cell)
    identity = {
        name: cell[name]
        for name in lifecycle._REFIT_CELL_IDENTITY_FIELDS  # noqa: SLF001
    }
    assert cell["current_refit_cell_id"] == lifecycle.compute_current_refit_cell_id(
        identity
    )


def test_raw_manifest_emits_full_semantic_cell_request(
    tmp_path: Path, monkeypatch
) -> None:
    material = _selected_material()
    material = producer._SelectedMaterial(  # noqa: SLF001
        **{
            **{name: getattr(material, name) for name in material.__dataclass_fields__},
            "descriptors": {
                **material.descriptors,
                "bars": {
                    "artifact_id": "9" * 64,
                    "content_sha256": "9" * 64,
                    "relative_path": "objects/9.csv",
                    "size_bytes": 1,
                    "semantic_sha256": "9" * 64,
                    "format_version": "bars",
                },
            },
        }
    )
    cell = producer._derive_refit_cells(  # noqa: SLF001
        materials=[material],
        accepted_receipt_sha256="e" * 64,
        historical_process_decision_sha256="f" * 64,
    )[0]
    observed = {}

    def fake_build(**kwargs):
        observed.update(kwargs)
        return {"manifest": True}

    monkeypatch.setattr(
        current_execution, "build_raw_current_refit_manifest", fake_build
    )
    bundle = {
        role: {
            "artifact_id": character * 64,
            "content_sha256": character * 64,
            "relative_path": f"objects/{character}.json",
            "size_bytes": 1,
            "semantic_sha256": character * 64,
            "format_version": role,
        }
        for role, character in zip(
            (
                "accepted_book_generation",
                "source_snapshot_bundle",
                "regime_params_bundle",
                "horizon_decisions_bundle",
            ),
            "ghij",
            strict=True,
        )
    }
    producer._raw_manifest(  # noqa: SLF001
        output_root=tmp_path,
        current_refit_receipt={"receipt": True},
        bundle_descriptors=bundle,
        materials=[material],
        refit_cells=[cell],
    )
    request = observed["cell_requests"][0]
    assert set(request) == current_execution._CELL_REQUEST_FIELDS  # noqa: SLF001
    assert request["winner_book_entry_sha256"] == "5" * 64
    assert (
        request["artifact_ids"]["winner_book_entry"]
        == material.descriptors["winner_book_entry"]["artifact_id"]
    )
