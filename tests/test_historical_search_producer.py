from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as search_execution
import pm_authority_spool as spool
import pm_causal_evidence_producer as causal_evidence
import pm_core
import pm_historical_anchor_orchestration as historical
import pm_historical_search_producer as producer
import pm_native_prefix_production as native_prefix
import pm_production_search_planner as planner
import pm_forward_oos_replay_producer as forward_replay


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


ANCHOR = {
    "anchor_id": "A1",
    "selection_cutoff_utc": "2024-01-01T00:00:00Z",
    "forward_start_date": "2024-02-05",
    "forward_end_date": "2025-02-03",
    "purge_calendar_days": 35,
}


@dataclass
class _Product:
    symbol: str
    timeframe: str
    source_state: str = native_prefix.AVAILABLE
    native_prefix: dict | None = None

    def __post_init__(self):
        if self.native_prefix is None:
            self.native_prefix = {"source_reason": None}


class _Horizon:
    def __init__(self, symbol: str, timeframe: str, regime: str, status: str):
        self.symbol = symbol
        self.timeframe = timeframe
        self.regime = regime
        self.status = status

    def to_dict(self):
        return {"plan_status": self.status}


class _Plan:
    def __init__(self, horizon: _Horizon):
        self.horizon_plan = horizon

    def attempt_artifacts(self):
        return ({"attempt": self.horizon_plan.regime},)


class _Capture:
    def __init__(self, request, symbol):
        self.request = request
        self.symbol = symbol
        self.bound = []

    def bind_timeframe(self, timeframe, **kwargs):
        self.bound.append((timeframe, kwargs))

    def finish(self):
        return historical.AnchorSymbolProduct(
            symbol=self.symbol,
            horizon_plans=(),
            book_evidence=(),
            optimizer_payload={"captured": True},
        )


def _fake_process(run_id: str = "historical-run"):
    return planner.FrozenProductionSearchProcess(
        _canonical({"run_id": run_id}),
        _canonical({}),
        object(),
    )


def _causal_payload():
    return {
        "schema": "fixture",
        "evidence_access_deadline_utc": "2099-01-01T00:00:00Z",
        "process_definition": {"anchor_plan": [dict(ANCHOR)]},
    }


def _planning_request(context, *, run_id, anchor, symbol_sinks, **kwargs):
    body = {
        "schema_version": historical.PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA,
        "run_id": run_id,
        "generation_id": context.generation_id,
        "anchor": dict(anchor),
        "process_precommit_receipt_sha256": kwargs[
            "process_precommit_receipt_sha256"
        ],
    }
    return historical.AnchorProducerRequest(_canonical(body), symbol_sinks)


def _production_request(context, planned, sinks):
    anchor = planned.anchors[0]
    body = {
        "schema_version": historical.PRODUCER_REQUEST_SCHEMA,
        "authority_scope": historical.AUTHORITY_SCOPE,
        "run_id": "historical-run",
        "generation_id": context.generation_id,
        "precommit_receipt_sha256": "f" * 64,
        "process_precommit_receipt_sha256": (
            planned.causal_precommit_payload_sha256
        ),
        "optimizer_producer_release_sha256": (
            producer.historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        "anchor": anchor,
        "symbols": ["EURUSD"],
        "timeframes": ["M5"],
        "regimes": list(producer._REGIMES),
        "requires_exact_sparse_state_roster": True,
        "possible_regime_slots_are_forced_observations": False,
        "confers_causal_or_current_authority": False,
    }
    return historical.AnchorProducerRequest(_canonical(body), sinks)


@pytest.fixture
def bounded_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    data_dir = tmp_path / "native"
    data_dir.mkdir()
    context = spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="historical-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    monkeypatch.setattr(producer, "_SYMBOLS", ("EURUSD",))
    monkeypatch.setattr(producer, "_TIMEFRAMES", ("M5",))
    monkeypatch.setattr(producer, "_GROUPS_PER_ANCHOR", 1)
    monkeypatch.setattr(producer, "_CELLS_PER_ANCHOR", 4)
    monkeypatch.setattr(producer, "_now_utc_seconds", lambda: "2026-01-01T00:00:00Z")
    monkeypatch.setattr(producer.causal, "_validate_precommit_payload", lambda _v: None)
    monkeypatch.setattr(
        producer.search_planner,
        "verify_frozen_production_search_process",
        lambda _context, _process: True,
    )
    monkeypatch.setattr(
        producer.historical,
        "issue_historical_pre_evidence_planning_request",
        _planning_request,
    )
    monkeypatch.setattr(
        producer.native_prefix,
        "produce_activated_native_prefix",
        lambda **kwargs: _Product(kwargs["symbol"], kwargs["timeframe"]),
    )

    def binding(value):
        horizon = value.horizon_plan if isinstance(value, _Plan) else value
        return {
            "cell": {
                "symbol": horizon.symbol,
                "timeframe": horizon.timeframe,
                "regime": horizon.regime,
            },
            "anchor_id": "A1",
            "plan_status": horizon.status,
            "identity": f"{horizon.symbol}|{horizon.timeframe}|{horizon.regime}",
        }

    monkeypatch.setattr(
        producer.historical,
        "historical_horizon_plan_binding",
        binding,
    )
    monkeypatch.setattr(
        producer.historical,
        "historical_horizon_binding_roster_sha256",
        lambda values: _sha(list(values)),
    )
    releases = []
    monkeypatch.setattr(producer, "_release_timeframe_state", lambda: releases.append(1))
    return data_dir, context, releases


def _install_sparse_planner(monkeypatch, *, drift: bool = False):
    calls = {"timeframes": 0}

    def plans(**kwargs):
        calls["timeframes"] += 1
        product = kwargs["product"]
        suffix = "-DRIFT" if drift and calls["timeframes"] > 1 else ""
        authority = SimpleNamespace(
            source_group_decision=object(),
            regime_fit_decision=object(),
            train_regime_support={
                "counts": {
                    "TREND": 10,
                    "RANGE": 0,
                    "BREAKOUT": 0,
                    "CHOP": 0,
                }
            },
        )
        rows = []
        for regime in producer._REGIMES:
            status = (
                "READY_FOR_HISTORICAL_EVIDENCE"
                if regime == "TREND"
                else "HISTORICAL_REGIME_UNOBSERVED"
            )
            rows.append(
                _Plan(
                    _Horizon(
                        product.symbol,
                        product.timeframe,
                        regime + suffix,
                        status,
                    )
                )
            )
        return authority, tuple(rows)

    monkeypatch.setattr(producer, "_timeframe_plans", plans)
    return calls


def test_preplan_never_executes_returns_and_callback_replays_exact_sparse_roster(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, releases = bounded_environment
    _install_sparse_planner(monkeypatch)
    captures = []
    executions = []
    monkeypatch.setattr(
        producer.historical_adapter,
        "open_historical_symbol_capture",
        lambda _context, request, symbol: captures.append(_Capture(request, symbol))
        or captures[-1],
    )

    def execute(**kwargs):
        executions.append(kwargs)
        return search_execution.HistoricalObservedCellExecution(
            status="WINNER_RAW_EVIDENCE_COMPLETE",
            attempted_trials_exact=1,
            production=None,
            failure_code=None,
        )

    monkeypatch.setattr(
        producer.search_execution,
        "execute_historical_observed_cell",
        execute,
    )
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=_causal_payload(),
        run_id="historical-run",
        anchors=[ANCHOR],
    )

    assert executions == []
    assert planned.anchors[0]["horizon_binding_count"] == 4
    assert len(planned.anchors[0]["horizon_binding_roster_sha256"]) == 64
    sinks = {"EURUSD": spool.open_symbol_sink(context, "EURUSD")}
    request = _production_request(context, planned, sinks)
    callback = planned.anchor_producers()["A1"]
    products = list(callback(request))

    assert [value.symbol for value in products] == ["EURUSD"]
    assert len(executions) == 1
    assert executions[0]["regime"] == "TREND"
    assert len(captures) == 1
    assert len(captures[0].bound) == 1
    assert len(captures[0].bound[0][1]["horizon_plans"]) == 4
    assert len(releases) == 2  # preplanning and canonical callback
    with pytest.raises(
        producer.HistoricalSearchProducerError,
        match="ANCHOR_PRODUCER_ALREADY_CONSUMED",
    ):
        callback(request)


def test_canonical_callback_rejects_any_preplanned_horizon_drift(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _ = bounded_environment
    _install_sparse_planner(monkeypatch, drift=True)
    monkeypatch.setattr(
        producer.historical_adapter,
        "open_historical_symbol_capture",
        lambda _context, request, symbol: _Capture(request, symbol),
    )
    monkeypatch.setattr(
        producer.search_execution,
        "execute_historical_observed_cell",
        lambda **_kwargs: pytest.fail("drift reached strategy-return access"),
    )
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=_causal_payload(),
        run_id="historical-run",
        anchors=[ANCHOR],
    )
    request = _production_request(
        context,
        planned,
        {"EURUSD": spool.open_symbol_sink(context, "EURUSD")},
    )
    with pytest.raises(
        producer.HistoricalSearchProducerError,
        match="CANONICAL_HORIZON_PLAN_CHANGED_AFTER_PREPLANNING",
    ):
        list(planned.anchor_producers()["A1"](request))


def test_terminal_source_absence_builds_four_zero_evaluation_plans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = _Product(
        "EURUSD",
        "M5",
        source_state=native_prefix.SOURCE_UNAVAILABLE,
    )
    terminal = SimpleNamespace(
        source_group_decision=object(),
        regime_fit_decision=object(),
    )
    calls = []
    monkeypatch.setattr(
        producer.search_execution,
        "fit_terminal_native_timeframe",
        lambda **kwargs: calls.append(("fit", kwargs)) or terminal,
    )
    plans = tuple(
        _Plan(_Horizon("EURUSD", "M5", regime, "HISTORICAL_SOURCE_UNAVAILABLE"))
        for regime in producer._REGIMES
    )
    monkeypatch.setattr(
        producer.search_planner,
        "build_historical_terminal_timeframe_plans",
        lambda *_args, **kwargs: calls.append(("plans", kwargs)) or plans,
    )
    monkeypatch.setattr(
        producer.search_execution,
        "fit_native_timeframe_for_search",
        lambda **_kwargs: pytest.fail("terminal source was treated as available"),
    )
    authority, observed = producer._timeframe_plans(
        context=object(),
        process=object(),
        request=object(),
        causal_precommit_payload={},
        anchor_ordinal=0,
        product=product,
        config=pm_core.PipelineConfig(),
    )
    assert authority is terminal
    assert observed == plans
    assert [name for name, _ in calls] == ["fit", "plans"]


def test_technical_native_failure_cannot_become_book_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = _Product(
        "EURUSD",
        "M5",
        source_state=native_prefix.TECHNICAL_FAILURE,
    )
    monkeypatch.setattr(
        producer.search_execution,
        "fit_terminal_native_timeframe",
        lambda **_kwargs: (_ for _ in ()).throw(
            search_execution.AuthoritativeSearchExecutionError(
                "TERMINAL_NATIVE_TECHNICAL_FAILURE_REQUIRES_ABORT"
            )
        ),
    )
    monkeypatch.setattr(
        producer.search_planner,
        "build_historical_terminal_timeframe_plans",
        lambda *_args, **_kwargs: pytest.fail("technical state built Book plans"),
    )
    with pytest.raises(
        search_execution.AuthoritativeSearchExecutionError,
        match="TERMINAL_NATIVE_TECHNICAL_FAILURE_REQUIRES_ABORT",
    ):
        producer._timeframe_plans(
            context=object(),
            process=object(),
            request=object(),
            causal_precommit_payload={},
            anchor_ordinal=0,
            product=product,
            config=pm_core.PipelineConfig(),
        )


def _forward_request(planned, *, execution=None, valuation=None):
    execution = ["EURUSD|M5"] if execution is None else execution
    valuation = ["EURUSD|M5"] if valuation is None else valuation
    rows = [
        {
            "anchor_id": "A1",
            "forward_end_exclusive_utc": "2025-02-04T00:00:00Z",
            "execution_product_keys": execution,
            "valuation_m5_product_keys": valuation,
        }
    ]
    body = {
        "schema_version": causal_evidence.FORWARD_NATIVE_PRODUCT_REQUEST_SCHEMA,
        "generation_id": planned.context.generation_id,
        "generation_context_sha256": planned.context.context_sha256,
        "native_universe_sha256": planned.context.native_universe_sha256,
        "config_sha256": planned.context.config_sha256,
        "historical_precommit_receipt_sha256": "c" * 64,
        "historical_run_receipt_sha256": "d" * 64,
        "causal_precommit_payload_sha256": (
            planned.causal_precommit_payload_sha256
        ),
        "anchor_count": 1,
        "anchors": rows,
        "required_roster_sha256": _sha(rows),
        "native_products_must_be_sealed_before_forward_frame_access": True,
        "resolver_may_choose_winners_or_product_keys": False,
        "confers_causal_current_refit_capital_publication_or_trading_authority": False,
    }
    return causal_evidence.ForwardNativeProductRequest(_canonical(body))


def test_forward_resolver_materializes_only_issuer_requested_winner_keys(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _ = bounded_environment
    _install_sparse_planner(monkeypatch)
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=_causal_payload(),
        run_id="historical-run",
        anchors=[ANCHOR],
    )
    monkeypatch.setattr(
        producer.historical_adapter,
        "open_historical_symbol_capture",
        lambda _context, request, symbol: _Capture(request, symbol),
    )
    monkeypatch.setattr(
        producer.search_execution,
        "execute_historical_observed_cell",
        lambda **_kwargs: search_execution.HistoricalObservedCellExecution(
            status="WINNER_RAW_EVIDENCE_COMPLETE",
            attempted_trials_exact=1,
            production=None,
            failure_code=None,
        ),
    )
    production_request = _production_request(
        context,
        planned,
        {"EURUSD": spool.open_symbol_sink(context, "EURUSD")},
    )
    list(planned.anchor_producers()["A1"](production_request))
    monkeypatch.setattr(
        producer.historical,
        "precommit_historical_anchor_run",
        lambda *_args, **_kwargs: SimpleNamespace(receipt_sha256="c" * 64),
    )
    calls = []

    def native_product(**kwargs):
        calls.append(kwargs)
        return _Product(kwargs["symbol"], kwargs["timeframe"])

    monkeypatch.setattr(
        producer.native_prefix,
        "produce_activated_native_prefix",
        native_product,
    )
    resolution = planned(_forward_request(planned))

    assert len(calls) == 1  # M5 execution and valuation share one sealed product
    assert calls[0]["cutoff_utc"] == "2025-02-04T00:00:00Z"
    assert calls[0]["production_contract_required"] is True
    assert type(resolution) is causal_evidence.ForwardNativeProductResolution
    execution = resolution.execution_products_by_anchor["A1"]["EURUSD|M5"]
    valuation = resolution.valuation_products_by_anchor["A1"]["EURUSD|M5"]
    assert execution is valuation
    with pytest.raises(
        producer.HistoricalSearchProducerError,
        match="FORWARD_NATIVE_PRODUCT_RESOLVER_ALREADY_CONSUMED",
    ):
        planned(_forward_request(planned))


def test_forward_resolver_rejects_roster_or_context_tamper_before_source_access(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _ = bounded_environment
    _install_sparse_planner(monkeypatch)
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=_causal_payload(),
        run_id="historical-run",
        anchors=[ANCHOR],
    )
    monkeypatch.setattr(
        producer.native_prefix,
        "produce_activated_native_prefix",
        lambda **_kwargs: pytest.fail("tampered request reached source access"),
    )
    request = _forward_request(planned)
    body = request.to_dict()
    body["generation_context_sha256"] = "e" * 64
    tampered = causal_evidence.ForwardNativeProductRequest(_canonical(body))
    with pytest.raises(
        producer.HistoricalSearchProducerError,
        match="FORWARD_NATIVE_PRODUCT_REQUEST_BINDING_MISMATCH",
    ):
        planned(tampered)


def test_canonical_causal_factory_consumes_preplanned_resolver_after_book_close(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _ = bounded_environment
    _install_sparse_planner(monkeypatch)
    payload = _causal_payload()
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=payload,
        run_id="historical-run",
        anchors=[ANCHOR],
    )
    monkeypatch.setattr(
        producer.historical_adapter,
        "open_historical_symbol_capture",
        lambda _context, request, symbol: _Capture(request, symbol),
    )
    monkeypatch.setattr(
        producer.search_execution,
        "execute_historical_observed_cell",
        lambda **_kwargs: search_execution.HistoricalObservedCellExecution(
            status="WINNER_RAW_EVIDENCE_COMPLETE",
            attempted_trials_exact=1,
            production=None,
            failure_code=None,
        ),
    )
    request = _production_request(
        context,
        planned,
        {"EURUSD": spool.open_symbol_sink(context, "EURUSD")},
    )
    list(planned.anchor_producers()["A1"](request))
    historical_precommit = SimpleNamespace(receipt_sha256="c" * 64)
    historical_run = SimpleNamespace(receipt_sha256="d" * 64)
    monkeypatch.setattr(
        producer.historical,
        "precommit_historical_anchor_run",
        lambda *_args, **_kwargs: historical_precommit,
    )
    monkeypatch.setattr(
        causal_evidence.capital_bootstrap,
        "verify_capital_forward_phase_binding",
        lambda observed_context, _binding: {
            "bootstrap_identity_sha256": "e" * 64,
            "bootstrap_identity": {
                "process_precommitment_sha256": producer.causal.canonical_sha256(
                    payload
                ),
                "process_definition_sha256": None,
            },
        }
        if observed_context is context
        else pytest.fail("generation context changed"),
    )
    monkeypatch.setattr(
        forward_replay,
        "_reopen_books",
        lambda *_args: ({}, [planned.anchors[0]], {"A1": {}}, []),
    )
    monkeypatch.setattr(
        forward_replay,
        "_required_forward_product_rosters",
        lambda _plans, _books: {
            "A1": {
                "execution_product_keys": ["EURUSD|M5"],
                "valuation_m5_product_keys": ["EURUSD|M5"],
            }
        },
    )
    prefix_calls = []
    monkeypatch.setattr(
        producer.native_prefix,
        "produce_activated_native_prefix",
        lambda **kwargs: prefix_calls.append(kwargs)
        or _Product(kwargs["symbol"], kwargs["timeframe"]),
    )
    forward_precommit = object()
    forward_product = object()
    observed = []

    def precommit(**kwargs):
        observed.append("precommit")
        assert kwargs["execution_products_by_anchor"]["A1"] == {
            "EURUSD|M5": kwargs["valuation_products_by_anchor"]["A1"]["EURUSD|M5"]
        }
        return forward_precommit

    def produce(**kwargs):
        observed.append("produce")
        assert kwargs["replay_precommit"] is forward_precommit
        return forward_product

    monkeypatch.setattr(
        forward_replay,
        "precommit_forward_oos_replay_products",
        precommit,
    )
    monkeypatch.setattr(
        forward_replay,
        "produce_forward_oos_replay_artifacts",
        produce,
    )
    monkeypatch.setattr(
        forward_replay,
        "verify_forward_oos_replay_product",
        lambda observed_context, value: observed.append("verify")
        if observed_context is context and value is forward_product
        else pytest.fail("forward product changed"),
    )
    factory = causal_evidence.BoundedCausalEvidenceFactory(
        context=context,
        causal_precommit_payload=payload,
        capital_phase_binding=object(),
        forward_product_resolver=planned,
        config=object(),
    )
    callback = factory.build_after_historical_run(
        historical_precommit=historical_precommit,
        historical_run_receipt=historical_run,
    )

    assert type(callback) is causal_evidence.BoundedCausalEvidenceProducer
    assert callback.forward_replay_product is forward_product
    assert observed == ["precommit", "produce", "verify"]
    assert len(prefix_calls) == 1


def test_contract_explicitly_denies_downstream_authority() -> None:
    body = producer.historical_search_producer_contract()
    assert body["preplanning_return_or_winner_evidence_access"] is False
    assert body["technical_failure_aborts_generation"] is True
    assert body["unobserved_regimes_are_explicit"] is True
    assert (
        body["confers_causal_current_capital_publication_or_trading_authority"]
        is False
    )
    assert len(producer.HISTORICAL_SEARCH_PRODUCER_RELEASE_SHA256) == 64
