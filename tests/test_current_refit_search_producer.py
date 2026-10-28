from __future__ import annotations

import hashlib
import inspect
import json
from datetime import datetime, timezone
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


def test_current_worker_rebuilds_explicit_v16_request_without_reviving_producer():
    """Fresh SYNTHETIC request transport, with current production still retired."""
    from tests.test_winner_transport_capacity import build_v16_payload
    typed = producer.winner.create_winner_selection_request_v16(**build_v16_payload())
    restored = producer._rebuild_winner_request(typed.to_dict(), "SYNTHETIC_REQUEST_INVALID")
    assert restored.to_dict() == typed.to_dict()
    assert restored.request_sha256 == typed.request_sha256
    assert producer.winner.MAX_REQUEST_BYTES == 16 * 1024 * 1024
    value = object.__new__(producer.CurrentRefitSearchProducer)
    value._used = False
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        value(object())
    assert caught.value.code == "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"


@pytest.mark.parametrize("decision_type", ["winner_v16_decision", "winner_v15_decision"])
def test_current_worker_manifest_requires_homogeneous_v16_artifact_labels(decision_type):
    """SYNTHETIC manifest-shape witness; no authority or market evidence."""
    entries = []
    bindings = {}
    types = {"winner_request": "winner_v16_request", "winner_decision": decision_type,
        "production_receipt": "winner_raw_evidence_production_receipt",
        "cross_binding": "current_horizon_winner_cross_binding"}
    for ordinal, (name, kind) in enumerate(types.items()):
        descriptor = {"sha256": str(ordinal + 1) * 64, "size_bytes": ordinal + 1}
        entry = {"artifact_id": f"SYNTHETIC/{name}", "artifact_type": kind, "descriptor": descriptor}
        entries.append(entry)
        bindings[name] = producer._artifact_binding(entry)
    manifest = {"artifacts": entries}
    if decision_type == "winner_v15_decision":
        with pytest.raises(CurrentRefitSearchProducerError) as caught:
            producer._resolve_raw_artifacts(manifest, bindings, required=True)
        assert caught.value.code == "CURRENT_WORKER_ARTIFACT_BINDING_INVALID"
    else:
        actual = producer._resolve_raw_artifacts(manifest, bindings, required=True)
        assert actual == dict(zip(types, entries, strict=True))


@pytest.mark.parametrize("edition,mutation", [
    ("v16", "none"), ("v16", "old_production"), ("v16", "old_cross"),
    ("v14", "none"), ("v15", "none"), ("v15", "v15_labels"),
])
def test_current_worker_v16_cold_transport_tuple(tmp_path, edition, mutation):
    """SYNTHETIC metadata/selector custody only, not a complete raw engine proof."""
    from tests import test_winner_selection as fixtures
    from tests.test_winner_transport_capacity import build_v16_payload, synthetic_pair
    context = producer.authority_spool.freeze_generation_context(tmp_path / "synthetic-v16",
        generation_id="synthetic-current-v16", native_universe_sha256="a" * 64, config_sha256="b" * 64)
    sink = producer.authority_spool.open_symbol_sink(context, "AUDUSD")
    payload = (build_v16_payload() if edition == "v16" else fixtures._current_payload_request(
        pair_contract=synthetic_pair()) if edition == "v15" else fixtures._payload_request())
    request = producer.winner.create_winner_selection_request(**payload)
    assert request is not None
    decision = producer.winner.evaluate_historical_cell_winner(request)
    entries = {}

    def put(name, kind, body):
        entry = sink.put_opaque(f"SYNTHETIC/{name}", kind, (_canonical(body),),
            format=producer.raw_winner.ASCII_JSON_FORMAT)
        entries[name] = entry
        return entry

    # The retained current reader used v14 labels/raw4 even for a v15 body.
    # Preserve that old admission/refusal behavior; do not bless a new writer.
    labels = "v16" if edition == "v16" else "v15" if mutation == "v15_labels" else "v14"
    req = put("winner_request", f"winner_{labels}_request", request.to_dict())
    dec = put("winner_decision", f"winner_{labels}_decision", decision.to_dict())
    cross = {"schema_version": (producer.raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4
        if mutation == "old_cross" else producer.raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5),
        "mode": producer.raw_winner.CURRENT_CROSS_BINDING_MODE, "fixture_provenance": "SYNTHETIC_METADATA_ONLY"}
    cross_entry = put("cross_binding", "current_horizon_winner_cross_binding", cross)
    production = {"schema_version": (producer.raw_winner.CURRENT_PRODUCTION_SCHEMA
        if mutation == "old_production" else producer.raw_winner.V16_PRODUCTION_SCHEMA if edition == "v16"
        else producer.raw_winner.PRODUCTION_SCHEMA),
        "generation_context_sha256": context.context_sha256, "symbol": "AUDUSD", "timeframe": "M5", "regime": "TREND",
        "request_sha256": request.request_sha256, "decision_receipt_sha256": decision.receipt_sha256,
        "provenance_cross_binding_sha256": cross_entry["descriptor"]["sha256"],
        "artifact_roster": {"winner-request": req["descriptor"], "winner-decision": dec["descriptor"]}}
    put("production_receipt", "winner_raw_evidence_production_receipt", production)
    receipt = sink.seal()
    manifest = producer.authority_spool.verify_symbol_receipt(context.root, receipt,
        expected_generation_id=context.generation_id, expected_symbol="AUDUSD",
        expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256)
    args = dict(context=context, manifest=manifest,
        bindings={name: producer._artifact_binding(entry) for name, entry in entries.items()},
        symbol="AUDUSD", timeframe="M5", regime="TREND", expected_decision_sha256=decision.receipt_sha256)
    if mutation != "none":
        with pytest.raises(CurrentRefitSearchProducerError) as caught:
            producer._rebuild_raw_winner_artifacts(**args)
        assert caught.value.code == ("CURRENT_WORKER_RAW_ARTIFACT_BINDING_INVALID" if mutation == "v15_labels"
                                     else "CURRENT_WORKER_CROSS_BINDING_INVALID" if mutation == "old_cross"
                                     else "CURRENT_WORKER_PRODUCTION_RECEIPT_SPLICE")
    else:
        actual_request, actual_decision, cross_bytes, production_bytes = producer._rebuild_raw_winner_artifacts(**args)
        assert actual_request.to_dict() == request.to_dict()
        assert actual_decision.to_dict() == decision.to_dict()
        assert cross_bytes == _canonical(cross)
        assert production_bytes == _canonical(production)


def test_current_worker_wraps_v16_builder_value_error():
    """Deliberately malformed deep V16 payload retains a typed consumer error."""
    from tests.test_winner_transport_capacity import build_v16_payload
    payload = build_v16_payload()
    nested = 0
    for _ in range(producer.winner.MAX_JSON_DEPTH + 1):
        nested = [nested]
    payload["precommitment_payload"]["SYNTHETIC_INVALID_DEPTH"] = nested
    body = {"contract_version": producer.winner.V16_GENERATION.request_version}
    for name in ("precommitment", "attempt_ledger", "evidence_ledger"):
        value = payload[name + "_payload"]
        body[name] = {"payload": value, "payload_sha256": _sha(value)}
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        producer._rebuild_winner_request(body, "CURRENT_WORKER_WINNER_REQUEST_INVALID")
    assert caught.value.code == "CURRENT_WORKER_WINNER_REQUEST_INVALID"
    assert caught.value.details == ("_ContractError", "INPUT_STRUCTURE_LIMIT_EXCEEDED")
    assert isinstance(caught.value.__cause__, ValueError)


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
        "symbol_scheduler",
    }


def _scheduler_unit_inputs(tmp_path: Path):
    pipeline_config = producer.PipelineConfig(optimization_max_workers=5)
    context = SimpleNamespace(
        root=tmp_path / "spool",
        generation_id="generation-scheduler",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
        context_sha256="c" * 64,
    )
    search_process = SimpleNamespace(
        receipt_sha256="d" * 64,
        to_dict=lambda: {"run_id": "run-scheduler"},
    )
    scheduler = producer.CurrentRefitSymbolSchedulerInputs(
        active_config_path=tmp_path / "config.json",
        data_dir=tmp_path / "data",
        cutoff_utc="2026-07-15T00:00:00+00:00",
        source_accessed_at_utc="2026-07-26T00:00:00+00:00",
        requested_workers=12,
    )
    inputs = SimpleNamespace(
        context=context,
        search_process=search_process,
        pipeline_config=pipeline_config,
        symbol_scheduler=scheduler,
    )
    request = {
        "causal_precommit_payload": {"precommit": True},
        "causal_evidence_payload": {"evidence": True},
        "causal_reconciliation_payload": {"reconciliation": True},
    }
    authorization = SimpleNamespace(receipt_sha256="e" * 64)
    return inputs, request, authorization


def test_symbol_scheduler_uses_requested_workers_without_ram_admission_waves(
    tmp_path: Path,
    monkeypatch,
) -> None:
    symbols = tuple(f"SYM{index:02d}" for index in range(12))
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", symbols)
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("M5",))
    inputs, request, authorization = _scheduler_unit_inputs(tmp_path)
    monkeypatch.setattr(
        producer.config_binding,
        "load_effective_pipeline_config",
        lambda _path: ({}, inputs.pipeline_config),
    )
    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            effective_config_sha256="f" * 64
        ),
    )
    governors = []
    class _Governor:
        def __init__(self, **kwargs):
            governors.append(kwargs)

        def iter_completed(self, _worker, tasks):
            for task in tasks:
                yield SimpleNamespace(
                    task_key=task.key,
                    status="COMPLETED",
                    technical_failure=False,
                    value={"symbol": task.key},
                    error_type="",
                    error_message="",
                    measured_peak_bytes=128 * 1024**2,
                )

    monkeypatch.setattr(producer.memory_governor, "MemoryGovernor", _Governor)

    results = producer._run_symbol_scheduler(  # noqa: SLF001
        inputs,
        request=request,
        authorization=authorization,
        current_cutoff="2026-07-15T00:00:00+00:00",
    )

    assert list(results) == list(symbols)
    assert governors == [
        {
            "requested_workers": len(symbols),
            "termination_grace_seconds": (
                producer.SYMBOL_SCHEDULER_TERMINATION_SECONDS
            ),
        }
    ]


def test_scheduler_parallel_completion_has_byte_identical_assembled_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    symbols = ("EURUSD", "GBPUSD", "USDJPY")
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", symbols)
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H1",))
    inputs, request, authorization = _scheduler_unit_inputs(tmp_path)
    monkeypatch.setattr(
        producer.config_binding,
        "load_effective_pipeline_config",
        lambda _path: ({}, inputs.pipeline_config),
    )
    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            effective_config_sha256="f" * 64
        ),
    )
    active_run = ""
    completion_orders = {"serial": [], "parallel": []}

    class _Governor:
        def __init__(self, **kwargs):
            self.requested_workers = kwargs["requested_workers"]

        def iter_completed(self, _worker, tasks):
            ordered = list(tasks)
            if self.requested_workers > 1:
                ordered.reverse()
            for task in ordered:
                completion_orders[active_run].append(task.key)
                yield SimpleNamespace(
                    task_key=task.key,
                    status="COMPLETED",
                    technical_failure=False,
                    value={
                        "schema_version": producer.SYMBOL_SCHEDULER_SCHEMA,
                        "symbol": task.key,
                        "index": {"kind": "symbol", "symbol": task.key},
                        "compact_receipt": {"symbol": task.key},
                    },
                    error_type="",
                    error_message="",
                    measured_peak_bytes=128 * 1024**2,
                )

    monkeypatch.setattr(producer.memory_governor, "MemoryGovernor", _Governor)
    monkeypatch.setattr(
        producer.authority_spool,
        "verify_symbol_receipt",
        lambda *_args, **kwargs: {"symbol": kwargs["expected_symbol"]},
    )
    monkeypatch.setattr(
        producer,
        "_manifest_artifact",
        lambda _manifest, binding, *, artifact_type: {
            "artifact_type": artifact_type,
            "binding": dict(binding),
        },
    )

    def read_index(_context, entry, *, maximum):
        del maximum
        binding = entry["binding"]
        if entry["artifact_type"] == "current_refit_worker_symbol_index":
            symbol = binding["symbol"]
            return {
                "schema_version": producer.SYMBOL_BUNDLE_SCHEMA,
                "generation_context_sha256": inputs.context.context_sha256,
                "search_process_receipt_sha256": (
                    inputs.search_process.receipt_sha256
                ),
                "authorization_receipt_sha256": authorization.receipt_sha256,
                "current_cutoff_utc": "2026-07-15T00:00:00+00:00",
                "symbol": symbol,
                "timeframe_order": ["H1"],
                "timeframe_artifacts": [
                    {"kind": "timeframe", "symbol": symbol, "timeframe": "H1"}
                ],
            }
        return {
            "symbol": binding["symbol"],
            "timeframe": binding["timeframe"],
        }

    monkeypatch.setattr(producer, "_read_spooled_json", read_index)
    monkeypatch.setattr(
        producer,
        "_rebuild_timeframe_evidence",
        lambda **kwargs: (
            [
                {
                    "symbol": kwargs["symbol"],
                    "timeframe": kwargs["timeframe"],
                    "evidence_sha256": _sha(
                        [kwargs["symbol"], kwargs["timeframe"]]
                    ),
                }
            ],
            [],
        ),
    )

    assembled: dict[str, tuple[list[dict], str]] = {}
    for run_name, requested_workers in (("serial", 1), ("parallel", 3)):
        active_run = run_name
        inputs.symbol_scheduler = producer.CurrentRefitSymbolSchedulerInputs(
            active_config_path=tmp_path / "config.json",
            data_dir=tmp_path / "data",
            cutoff_utc="2026-07-15T00:00:00+00:00",
            source_accessed_at_utc="2026-07-26T00:00:00+00:00",
            requested_workers=requested_workers,
        )
        evidence = list(
            producer._scheduled_book_evidence_stream(  # noqa: SLF001
                inputs,
                request=request,
                authorization=authorization,
                current_cutoff="2026-07-15T00:00:00+00:00",
                selected={},
                compact_receipts={},
            )
        )
        assembled[run_name] = (evidence, _sha(evidence))

    assert completion_orders["serial"] == list(symbols)
    assert completion_orders["parallel"] == ["USDJPY", "GBPUSD", "EURUSD"]
    assert completion_orders["serial"] != completion_orders["parallel"]
    assert [row["symbol"] for row in assembled["serial"][0]] == list(symbols)
    assert assembled["serial"] == assembled["parallel"]


def test_selected_source_capture_is_bound_to_exact_manifest_entries() -> None:
    source_spool_descriptor = {
        "relative_path": "symbols/EURUSD/source.json",
        "size_bytes": 101,
        "sha256": "1" * 64,
        "format": "canonical_json_ascii",
    }
    bars_spool_descriptor = {
        "relative_path": "symbols/EURUSD/bars.csv",
        "size_bytes": 202,
        "sha256": "2" * 64,
        "format": "csv",
    }
    source_entry = {
        "artifact_id": "source",
        "artifact_type": "current_refit_source",
        "descriptor": source_spool_descriptor,
    }
    bars_entry = {
        "artifact_id": "bars",
        "artifact_type": "current_refit_bars",
        "descriptor": bars_spool_descriptor,
    }
    bars_raw = current_execution.build_raw_artifact_descriptor(
        relative_path=bars_spool_descriptor["relative_path"],
        size_bytes=bars_spool_descriptor["size_bytes"],
        content_sha256=bars_spool_descriptor["sha256"],
        semantic_sha256="3" * 64,
        format_version=producer.planner.CURRENT_BAR_FILE_SCHEMA,
    )
    source = {
        "bars_artifact_id": bars_raw["artifact_id"],
        "bars_content_sha256": "3" * 64,
    }
    source_raw = current_execution.build_raw_artifact_descriptor(
        relative_path=source_spool_descriptor["relative_path"],
        size_bytes=source_spool_descriptor["size_bytes"],
        content_sha256=source_spool_descriptor["sha256"],
        semantic_sha256=producer.current_artifacts.canonical_artifact_sha256(
            source
        ),
        format_version=producer.planner.CURRENT_SOURCE_SNAPSHOT_SCHEMA,
    )
    capture = {
        "source_snapshot": source,
        "source_spool_entry": source_entry,
        "bars_spool_entry": bars_entry,
        "source_raw_descriptor": source_raw,
        "bars_raw_descriptor": bars_raw,
    }
    manifest = {"artifacts": [source_entry, bars_entry]}

    producer._require_source_capture_manifest_entries(  # noqa: SLF001
        manifest,
        capture,
        code="TEST_CAPTURE_UNBOUND",
    )

    tampered = {
        **capture,
        "bars_raw_descriptor": {
            **bars_raw,
            "semantic_sha256": "4" * 64,
        },
    }
    with pytest.raises(
        CurrentRefitSearchProducerError,
        match="TEST_CAPTURE_UNBOUND",
    ):
        producer._require_source_capture_manifest_entries(  # noqa: SLF001
            manifest,
            tampered,
            code="TEST_CAPTURE_UNBOUND",
        )


def test_required_valid_until_uses_authenticated_completion_and_utc_schedule() -> None:
    config = producer.PipelineConfig(production_retrain_anchor_date="2026-06-07")
    payload = {"execution": {"completed_at_utc": "2026-07-22T15:00:00+00:00"}}

    deadline = producer._required_valid_until_utc(config, payload)  # noqa: SLF001

    assert deadline == datetime(2026, 8, 2, 0, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "completed_at",
    [None, "2026-07-22T15:00:00", "not-a-datetime"],
)
def test_required_valid_until_rejects_unauthenticated_time_forms(completed_at) -> None:
    with pytest.raises(
        CurrentRefitSearchProducerError,
        match="CURRENT_REFIT_COMPLETED_AT_UTC_INVALID",
    ):
        producer._required_valid_until_utc(  # noqa: SLF001
            producer.PipelineConfig(),
            {"execution": {"completed_at_utc": completed_at}},
        )


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


def test_retired_current_producer_stops_before_one_shot_or_execution() -> None:
    value = object.__new__(producer.CurrentRefitSearchProducer)
    value._used = False  # noqa: SLF001
    for _attempt in range(2):
        with pytest.raises(CurrentRefitSearchProducerError) as caught:
            value(object())
        assert caught.value.code == (
            "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"
        )
        assert value._used is False  # noqa: SLF001


@pytest.mark.parametrize("spread_mode", (
    "fixed_spec", "historical_bar", "historical_max", "conservative_max",
))
@pytest.mark.parametrize("entry", ("public-facade", "direct-implementation"))
def test_current_refit_entry_is_retired_before_source_access(spread_mode, entry):
    config = producer.PipelineConfig(backtest_spread_mode=spread_mode)

    class UnopenedInputs:
        pipeline_config = config

        def __getattribute__(self, attribute):
            pytest.fail(f"retired current-refit route accessed input {attribute}")

    if entry == "public-facade":
        value = object.__new__(producer.CurrentRefitSearchProducer)
        value._frozen_config = UnopenedInputs()  # noqa: SLF001
        invoke = lambda: value(UnopenedInputs())
    else:
        invoke = lambda: producer._produce(UnopenedInputs(), UnopenedInputs())  # noqa: SLF001
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        invoke()
    assert caught.value.code == (
        "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"
    )


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


def test_raw_artifact_bindings_require_winner_v14_types(monkeypatch) -> None:
    observed = []

    def reopen(_manifest, binding, *, artifact_type):
        observed.append((binding, artifact_type))
        return {"artifact_type": artifact_type}

    monkeypatch.setattr(producer, "_manifest_artifact", reopen)
    bindings = {
        "winner_request": "request",
        "winner_decision": "decision",
        "production_receipt": "receipt",
        "cross_binding": "cross",
    }

    result = producer._resolve_raw_artifacts({}, bindings, required=True)

    assert result is not None
    assert observed == [
        ("request", "winner_v14_request"),
        ("decision", "winner_v14_decision"),
        ("receipt", "winner_raw_evidence_production_receipt"),
        ("cross", "current_horizon_winner_cross_binding"),
    ]


def test_current_producer_owns_materialized_config_after_construction(
    monkeypatch,
) -> None:
    supplied = producer.PipelineConfig(backtest_spread_mode="historical_bar")
    owned = producer.PipelineConfig(backtest_spread_mode="historical_bar")
    frozen = SimpleNamespace(materialize=lambda context: owned)
    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        lambda *args, **kwargs: frozen,
    )
    value = producer.CurrentRefitSearchProducer(
        _producer_inputs_for_binding_test(supplied)
    )
    supplied.backtest_spread_mode = "fixed_spec"
    assert value._frozen_config is frozen  # noqa: SLF001
    assert frozen.materialize(value._context) is owned  # noqa: SLF001
    assert owned.backtest_spread_mode == "historical_bar"
    with pytest.raises(CurrentRefitSearchProducerError) as caught:
        value(_Request(_request_body()))
    assert caught.value.code == (
        "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"
    )


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


def test_retirement_precedes_active_config_reopening(monkeypatch) -> None:
    calls = []

    class _Frozen:
        def materialize(self, context):
            calls.append(context)
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
    assert caught.value.code == (
        "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"
    )
    assert calls == []
    with pytest.raises(producer.config_binding.ProductionConfigBindingError) as drift:
        value._frozen_config.materialize(value._context)  # noqa: SLF001
    assert drift.value.code == "ACTIVE_CONFIG_BYTES_CHANGED"


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
        producer.authority_spool,
        "open_symbol_sink",
        lambda *args, **kwargs: _Sink(),
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


def test_book_stream_preserves_authenticated_terminal_technical_abstention(
    monkeypatch,
) -> None:
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", ("EURUSD",))
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H1",))
    monkeypatch.setattr(producer.book, "REGIME_ORDER", ("TREND",))

    class _Sink:
        def seal(self):
            return "sealed-terminal-technical"

    monkeypatch.setattr(
        producer.authority_spool,
        "open_symbol_sink",
        lambda *args, **kwargs: _Sink(),
    )
    fit = SimpleNamespace(
        to_dict=lambda: {
            "reason_code": "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
        }
    )
    terminal = SimpleNamespace(source_group_decision=object(), regime_fit_decision=fit)
    monkeypatch.setattr(
        producer.search_execution,
        "fit_terminal_native_timeframe",
        lambda **kwargs: terminal,
    )
    plan_calls = []
    monkeypatch.setattr(
        producer.planner,
        "build_current_terminal_cell_plan",
        lambda *args, **kwargs: plan_calls.append(kwargs) or object(),
    )
    captured = []
    evidence = object()
    monkeypatch.setattr(
        producer,
        "_state_evidence",
        lambda **kwargs: captured.append(kwargs) or evidence,
    )
    inputs = SimpleNamespace(
        context=object(),
        search_process=object(),
        pipeline_config=producer.PipelineConfig(),
        native_products={
            "EURUSD|H1": SimpleNamespace(source_state=native_prefix.TECHNICAL_TERMINAL)
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
    assert captured[0]["state_kind"] == producer.book.TERMINAL_TECHNICAL
    assert captured[0]["attempted"] == 0
    assert plan_calls[0]["product"].source_state == (native_prefix.TECHNICAL_TERMINAL)
    assert compact == {"EURUSD": "sealed-terminal-technical"}


def test_current_book_stream_coexists_with_sealed_historical_symbol_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    context = producer.authority_spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="generation-phase-domain",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    historical_sink = producer.authority_spool.open_symbol_sink(
        context,
        "EURUSD",
        manifest_domain=(
            producer.authority_spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN
        ),
    )
    historical_sink.put_json(
        "run/precommit",
        "historical_anchor_precommit",
        {"phase": "historical"},
    )
    historical_receipt = historical_sink.seal()

    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", ("EURUSD",))
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H1",))
    monkeypatch.setattr(producer.book, "REGIME_ORDER", ("TREND",))
    terminal = SimpleNamespace(
        source_group_decision=object(),
        regime_fit_decision=SimpleNamespace(
            to_dict=lambda: {"reason_code": "SOURCE_UNAVAILABLE"}
        ),
    )
    monkeypatch.setattr(
        producer.search_execution,
        "fit_terminal_native_timeframe",
        lambda **_kwargs: terminal,
    )

    def build_plan(*_args, **kwargs):
        kwargs["sink"].put_json(
            "current-refit/run/cutoff/search-cell-plan/H1/TREND",
            "production_search_cell_plan",
            {"phase": "current-refit"},
        )
        return object()

    monkeypatch.setattr(
        producer.planner,
        "build_current_terminal_cell_plan",
        build_plan,
    )
    evidence = object()
    monkeypatch.setattr(producer, "_state_evidence", lambda **_kwargs: evidence)
    monkeypatch.setattr(producer, "_release_timeframe_state", lambda: None)
    inputs = SimpleNamespace(
        context=context,
        search_process=object(),
        pipeline_config=producer.PipelineConfig(),
        native_products={
            "EURUSD|H1": SimpleNamespace(
                source_state=native_prefix.SOURCE_UNAVAILABLE
            )
        },
    )
    compact: dict[str, object] = {}

    assert list(
        producer._book_evidence_stream(  # noqa: SLF001
            inputs,
            authorization=object(),
            selected={},
            compact_receipts=compact,
        )
    ) == [evidence]
    current_receipt = compact["EURUSD"]
    assert type(current_receipt) is dict
    assert current_receipt["manifest_domain"] == (
        producer.authority_spool.CURRENT_REFIT_MANIFEST_DOMAIN
    )
    assert historical_receipt["symbol_manifest"]["relative_path"] != (
        current_receipt["symbol_manifest"]["relative_path"]
    )
    producer.authority_spool.verify_symbol_receipt(
        context.root,
        current_receipt,
        expected_generation_id=context.generation_id,
        expected_symbol="EURUSD",
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
        expected_manifest_domain=(
            producer.authority_spool.CURRENT_REFIT_MANIFEST_DOMAIN
        ),
    )


def test_exact_empty_current_book_is_typed_not_promotable() -> None:
    current_book = {
        "status": "NO_LOCAL_WINNERS",
        "reason_codes": ["COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS"],
        "historical_local_candidate_count": 0,
        "historical_local_candidate_cell_ids": [],
        "cells": [],
    }

    with pytest.raises(producer.CurrentRefitSearchNotPromotable) as caught:
        producer._selected_book_cells(current_book, {})  # noqa: SLF001

    assert caught.value.code == "CURRENT_BOOK_HAS_NO_ELIGIBLE_WINNERS"
    assert caught.value.details == (
        "NO_LOCAL_WINNERS",
        "COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS",
    )
    assert not isinstance(caught.value, CurrentRefitSearchProducerError)


@pytest.mark.parametrize(
    ("mutation", "selected"),
    [
        ({"status": "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"}, {}),
        (
            {
                "reason_codes": [
                    "LOCAL_WINNERS_REQUIRE_SEPARATE_CAUSAL_PROCESS_VALIDATION"
                ]
            },
            {},
        ),
        ({"historical_local_candidate_count": 1}, {}),
        ({"historical_local_candidate_cell_ids": ["cell-1"]}, {}),
        ({}, {"EURUSD|H1|TREND": object()}),
    ],
)
def test_inconsistent_empty_current_book_remains_technical(
    mutation: dict[str, object],
    selected: dict[str, object],
) -> None:
    current_book = {
        "status": "NO_LOCAL_WINNERS",
        "reason_codes": ["COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS"],
        "historical_local_candidate_count": 0,
        "historical_local_candidate_cell_ids": [],
        "cells": [],
    }
    current_book.update(mutation)

    with pytest.raises(
        CurrentRefitSearchProducerError,
        match="CURRENT_BOOK_EMPTY_SELECTION_CONTRACT_INVALID",
    ):
        producer._selected_book_cells(current_book, selected)  # noqa: SLF001


def test_book_stream_still_aborts_unresolved_technical_failure(monkeypatch) -> None:
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", ("EURUSD",))
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H1",))

    class _Sink:
        def seal(self):
            return "sealed"

    monkeypatch.setattr(
        producer.authority_spool,
        "open_symbol_sink",
        lambda *args, **kwargs: _Sink(),
    )
    inputs = SimpleNamespace(
        context=object(),
        search_process=object(),
        pipeline_config=producer.PipelineConfig(),
        native_products={
            "EURUSD|H1": SimpleNamespace(source_state=native_prefix.TECHNICAL_FAILURE)
        },
    )
    with pytest.raises(
        CurrentRefitSearchProducerError, match="CURRENT_NATIVE_TECHNICAL_FAILURE"
    ):
        list(
            producer._book_evidence_stream(  # noqa: SLF001
                inputs,
                authorization=object(),
                selected={},
                compact_receipts={},
            )
        )


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


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_common_cutoff_preserves_exact_mt5_symbol(monkeypatch, symbol: str) -> None:
    monkeypatch.setattr(producer.book, "PINNED_SYMBOL_ORDER", (symbol,))
    monkeypatch.setattr(producer.book, "TIMEFRAME_ORDER", ("H4",))
    product = native_prefix.NativePrefixProduct(
        source_state=native_prefix.SOURCE_UNAVAILABLE,
        index={
            "symbol": symbol,
            "timeframe": "H4",
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

    assert producer._current_cutoff(  # noqa: SLF001
        SimpleNamespace(native_products={f"{symbol}|H4": product})
    ) == "2026-07-15T00:00:00+00:00"

    product.index["symbol"] = symbol.upper()
    with pytest.raises(
        CurrentRefitSearchProducerError,
        match="CURRENT_NATIVE_PRODUCT_IDENTITY_MISMATCH",
    ):
        producer._current_cutoff(  # noqa: SLF001
            SimpleNamespace(native_products={f"{symbol}|H4": product})
        )


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


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_management_native_contract_preserves_exact_mt5_symbol(symbol: str) -> None:
    contract = producer._management_native_source_contract(  # noqa: SLF001
        {
            "symbol": symbol,
            "timeframe": "H4",
            "activation_id": "activation-v5",
            "activation_manifest_sha256": "1" * 64,
            "source_bars_sha256": "2" * 64,
            "source_audit_sidecar_sha256": "3" * 64,
            "index_sha256": "4" * 64,
            "source_identity_sha256": "5" * 64,
        }
    )

    assert contract["symbol"] == symbol


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


def test_materialize_selected_preserves_complete_winner_book_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tolerance = {
        "level_id": "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
        "single_fill_uplift_percentile": 75,
        "uniform_entry_half_spread_multiplier_float64_hex": "0x1.a000000000000p+2",
    }
    process = {
        "strategy_id": "EMACrossoverStrategy",
        "process_candidate_id": "candidate",
    }
    process_sha = producer.current_artifacts.canonical_artifact_sha256(process)
    cell = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "local_process_candidate_id": "candidate",
        "local_process_sha256": process_sha,
        "winner_decision_sha256": "4" * 64,
        "historical_book_entry_sha256": "5" * 64,
        "validated_cost_ladder_tolerance": tolerance,
    }
    source = {"schema_version": "source"}
    source_descriptor = current_execution.build_raw_artifact_descriptor(
        relative_path="source.json",
        size_bytes=1,
        content_sha256="1" * 64,
        semantic_sha256=producer.current_artifacts.canonical_artifact_sha256(source),
        format_version="source",
    )
    bars_descriptor = current_execution.build_raw_artifact_descriptor(
        relative_path="bars.csv",
        size_bytes=1,
        content_sha256="2" * 64,
        semantic_sha256="3" * 64,
        format_version="bars",
    )
    selected = producer._SelectedSearch(  # noqa: SLF001
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        artifacts={
            "winner_process": process,
            "horizon_process": {"schema_version": "horizon"},
            "trial_definition": {"parameters": {"fast": 10, "slow": 30}},
            "refit_recipe": {"schema_version": "recipe"},
        },
        source_capture={
            "source_snapshot": source,
            "source_raw_descriptor": source_descriptor,
            "bars_raw_descriptor": bars_descriptor,
        },
        winner_decision_sha256="4" * 64,
    )
    current_book = {
        "regime_fits": [
            {
                "receipt_sha256": "a" * 64,
                "payload": {
                    "symbol": "EURUSD",
                    "timeframe": "H1",
                    "regime_detector_record": {
                        "record_sha256": "b" * 64,
                        "parameters_sha256": "c" * 64,
                        "detector_code_sha256": "d" * 64,
                    },
                },
            }
        ]
    }
    captured = {}

    class CaptureSink:
        def put_json(self, path, artifact_type, body):
            encoded = _canonical(body)
            captured[artifact_type] = json.loads(encoded.decode("ascii"))
            return {
                "descriptor": {
                    "relative_path": f"{path}.json",
                    "size_bytes": len(encoded),
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                }
            }

    monkeypatch.setattr(
        current_execution,
        "build_current_regime_params_bundle",
        lambda **_kwargs: {"schema_version": "regimes"},
    )
    materials, _sources, _horizons, _regimes = producer._materialize_selected(  # noqa: SLF001
        sink=CaptureSink(),
        current_book=current_book,
        selected_cells=[cell],
        selected_search={"EURUSD|H1|TREND": selected},
        current_cutoff="2026-07-15T00:00:00+00:00",
    )

    assert captured[producer.current_production.ART_BOOK_ENTRY] == cell
    assert materials[0].book_cell == cell
    assert materials[0].book_cell["validated_cost_ladder_tolerance"] == tolerance


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
