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
        "process_precommit_receipt_sha256": kwargs["process_precommit_receipt_sha256"],
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
        "process_precommit_receipt_sha256": (planned.causal_precommit_payload_sha256),
        "optimizer_producer_release_sha256": (
            producer.historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        "anchor": anchor,
        "symbols": list(producer._SYMBOLS),
        "timeframes": list(producer._TIMEFRAMES),
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
        lambda **kwargs: _Product(
            kwargs["symbol"],
            kwargs["timeframe"],
            native_prefix={
                "source_reason": None,
                "cutoff_utc": kwargs["cutoff_utc"],
            },
        ),
    )

    def produce_prefixes(**kwargs):
        return tuple(
            _Product(
                kwargs["symbol"],
                kwargs["timeframe"],
                native_prefix={
                    "source_reason": None,
                    "cutoff_utc": request["cutoff_utc"],
                },
            )
            for request in kwargs["requests"]
        )

    monkeypatch.setattr(
        producer.native_prefix,
        "produce_activated_native_prefixes",
        produce_prefixes,
    )

    monkeypatch.setattr(
        producer.native_prefix,
        "stage_activated_native_prefixes",
        lambda **kwargs: tuple(
            {
                "symbol": kwargs["symbol"],
                "timeframe": kwargs["timeframe"],
                "cutoff_utc": request["cutoff_utc"],
            }
            for request in kwargs["requests"]
        ),
    )
    monkeypatch.setattr(
        producer.native_prefix,
        "verify_activated_native_prefix_candidates",
        lambda **kwargs: tuple(
            _Product(
                kwargs["symbol"],
                kwargs["timeframe"],
                native_prefix={
                    "source_reason": None,
                    "cutoff_utc": candidate["cutoff_utc"],
                },
            )
            for candidate in kwargs["candidates"]
        ),
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
    monkeypatch.setattr(
        producer, "_release_timeframe_state", lambda: releases.append(1)
    )
    active_config_path = (tmp_path / "config.json").resolve()
    effective_config_sha256 = "e" * 64

    class FrozenConfigBinding:
        def __init__(self, supplied_config):
            self.active_config_path = active_config_path
            self.effective_config_sha256 = effective_config_sha256
            self._supplied_config = supplied_config
            self._supplied_config.data_dir = data_dir

        def materialize(self, observed_context):
            assert observed_context is context
            return self._supplied_config

    monkeypatch.setattr(
        producer.config_binding,
        "load_effective_pipeline_config",
        lambda observed_path: (
            (
                {}
                if Path(observed_path) == active_config_path
                else pytest.fail("active config path changed")
            ),
            pm_core.PipelineConfig(),
        ),
    )

    def freeze_config(
        observed_context,
        *,
        active_config_path: Path,
        config_payload,
        supplied_config,
    ):
        assert observed_context is context
        assert Path(active_config_path) == (tmp_path / "config.json").resolve()
        assert config_payload == {}
        return FrozenConfigBinding(supplied_config)

    monkeypatch.setattr(
        producer.config_binding,
        "freeze_production_pipeline_config",
        freeze_config,
    )

    def worker_authority(contract, symbol, *, phase):
        assert contract["phase"] == phase
        request = historical.AnchorProducerRequest(
            contract["request_json"].encode("ascii"),
            {symbol: spool.open_symbol_sink(context, symbol)},
        )
        return (
            dict(contract),
            context,
            _fake_process(contract["run_id"]),
            request,
            pm_core.PipelineConfig(data_dir=data_dir),
            data_dir,
            dict(contract["causal_precommit_payload"]),
        )

    monkeypatch.setattr(producer, "_worker_authority", worker_authority)
    monkeypatch.setattr(
        producer,
        "_prefix_route",
        lambda product: {
            "symbol": product.symbol,
            "timeframe": product.timeframe,
            "cutoff_utc": product.native_prefix["cutoff_utc"],
            "source_state": product.source_state,
        },
    )

    def reopen_prefix(*, route, symbol, timeframe, **_kwargs):
        assert route["symbol"] == symbol
        assert route["timeframe"] == timeframe
        return _Product(
            symbol,
            timeframe,
            source_state=route["source_state"],
            native_prefix={
                "source_reason": None,
                "cutoff_utc": route["cutoff_utc"],
            },
        )

    monkeypatch.setattr(producer, "_reopen_prefix_route", reopen_prefix)

    bridge_products = {}

    def spool_bundle(capture):
        product = capture.finish()
        descriptor = spool.write_canonical_json_artifact(
            context,
            {
                "schema_version": "fxpm.fixture-historical-worker-bundle.v1",
                "symbol": product.symbol,
                "ordinal": len(bridge_products),
            },
        )
        bridge_products[descriptor["sha256"]] = product
        return descriptor

    def reopen_bundle(
        observed_context,
        _request,
        descriptor,
    ):
        assert observed_context is context
        return bridge_products[descriptor["sha256"]]

    monkeypatch.setattr(
        producer.historical_adapter,
        "spool_historical_symbol_worker_bundle",
        spool_bundle,
    )
    monkeypatch.setattr(
        producer.historical_adapter,
        "reopen_historical_symbol_worker_bundle",
        reopen_bundle,
    )

    scheduler_calls = []

    def run_scheduler(
        worker,
        *,
        contract,
        symbols,
        requested_workers,
        phase,
    ):
        assert requested_workers == scheduler.requested_workers
        scheduler_calls.append(
            {
                "phase": phase,
                "contract_phase": contract["phase"],
            }
        )
        # Reverse completion order deliberately; the production caller must
        # still consume the result mapping in its frozen symbol order.
        return {
            symbol: worker(contract, symbol)
            for symbol in reversed(tuple(symbols))
        }

    monkeypatch.setattr(
        producer.historical_scheduler,
        "run_symbol_scheduler",
        run_scheduler,
    )
    scheduler = producer.HistoricalSymbolSchedulerInputs(
        active_config_path=active_config_path,
        requested_workers=1,
    )
    return data_dir, context, releases, scheduler, scheduler_calls


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
    data_dir, context, releases, scheduler, scheduler_calls = bounded_environment
    _install_sparse_planner(monkeypatch)
    captures = []
    executions = []
    monkeypatch.setattr(
        producer.historical_adapter,
        "open_historical_symbol_capture",
        lambda _context, request, symbol: (
            captures.append(_Capture(request, symbol)) or captures[-1]
        ),
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
        symbol_scheduler=scheduler,
    )

    assert executions == []
    assert scheduler_calls == [
        {
            "phase": "historical_preplan",
            "contract_phase": "PREPLAN_BATCH",
        }
    ]
    assert planned.anchors[0]["horizon_binding_count"] == 4
    assert len(planned.anchors[0]["horizon_binding_roster_sha256"]) == 64
    sinks = {"EURUSD": spool.open_symbol_sink(context, "EURUSD")}
    request = _production_request(context, planned, sinks)
    callback = planned.anchor_producers()["A1"]
    products = list(callback(request))

    assert [value.symbol for value in products] == ["EURUSD"]
    assert len(executions) == 1
    assert executions[0]["regime"] == "TREND"
    assert executions[0]["cell_plan"].horizon_plan is captures[0].bound[0][1][
        "horizon_plans"
    ][0]
    assert executions[0]["search_process"].receipt_sha256 == (
        planned._process.receipt_sha256
    )
    assert "horizon_plan" not in executions[0]
    assert "attempt_artifacts" not in executions[0]
    assert len(captures) == 1
    assert len(captures[0].bound) == 1
    assert len(captures[0].bound[0][1]["horizon_plans"]) == 4
    assert len(releases) == 2  # preplanning and canonical callback
    assert [call["phase"] for call in scheduler_calls] == [
        "historical_preplan",
        "historical_execute",
    ]
    with pytest.raises(
        producer.HistoricalSearchProducerError,
        match="ANCHOR_PRODUCER_ALREADY_CONSUMED",
    ):
        callback(request)


def test_scheduler_parallel_completion_has_byte_identical_assembled_evidence(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _releases, base_scheduler, _scheduler_calls = (
        bounded_environment
    )
    symbols = ("EURUSD", "GBPUSD", "USDJPY")
    monkeypatch.setattr(producer, "_SYMBOLS", symbols)
    monkeypatch.setattr(producer, "_GROUPS_PER_ANCHOR", len(symbols))
    monkeypatch.setattr(
        producer,
        "_CELLS_PER_ANCHOR",
        len(symbols) * len(producer._TIMEFRAMES) * len(producer._REGIMES),
    )
    _install_sparse_planner(monkeypatch)

    class DeterministicCapture(_Capture):
        def finish(self):
            anchor_id = self.request.to_dict()["anchor"]["anchor_id"]
            horizon_plans = tuple(
                {
                    "anchor_id": anchor_id,
                    "symbol": self.symbol,
                    "timeframe": timeframe,
                }
                for timeframe, _kwargs in self.bound
            )
            book_evidence = tuple(
                {
                    "schema_version": "fxpm.fixture-historical-book-cell.v1",
                    "anchor_id": anchor_id,
                    "symbol": self.symbol,
                    "timeframe": timeframe,
                    "evidence_sha256": _sha(
                        [anchor_id, self.symbol, timeframe]
                    ),
                }
                for timeframe, _kwargs in self.bound
            )
            return historical.AnchorSymbolProduct(
                symbol=self.symbol,
                horizon_plans=horizon_plans,
                book_evidence=book_evidence,
                optimizer_payload={
                    "anchor_id": anchor_id,
                    "symbol": self.symbol,
                    "captured": True,
                },
            )

    monkeypatch.setattr(
        producer.historical_adapter,
        "open_historical_symbol_capture",
        lambda _context, request, symbol: DeterministicCapture(request, symbol),
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

    active_run = ""
    completion_orders = {
        "serial": {"historical_preplan": [], "historical_execute": []},
        "parallel": {"historical_preplan": [], "historical_execute": []},
    }

    def run_scheduler(
        worker,
        *,
        contract,
        symbols,
        requested_workers,
        phase,
    ):
        ordered = list(symbols)
        if requested_workers > 1:
            ordered = [ordered[0], *reversed(ordered[1:])]
        completed = {}
        for symbol in ordered:
            completed[symbol] = worker(contract, symbol)
            completion_orders[active_run][phase].append(symbol)
        return completed

    monkeypatch.setattr(
        producer.historical_scheduler,
        "run_symbol_scheduler",
        run_scheduler,
    )

    assembled: dict[str, tuple[bytes, str]] = {}
    for run_name, requested_workers in (("serial", 1), ("parallel", 3)):
        active_run = run_name
        scheduler = producer.HistoricalSymbolSchedulerInputs(
            active_config_path=base_scheduler.active_config_path,
            requested_workers=requested_workers,
        )
        planned = producer.preplan_historical_search(
            data_dir=data_dir,
            context=context,
            config=pm_core.PipelineConfig(),
            process=_fake_process(),
            causal_precommit_payload=_causal_payload(),
            run_id="historical-run",
            anchors=[ANCHOR],
            symbol_scheduler=scheduler,
        )
        sinks = {
            symbol: spool.open_symbol_sink(context, symbol)
            for symbol in symbols
        }
        request = _production_request(context, planned, sinks)
        products = list(planned.anchor_producers()["A1"](request))
        payload = {
            "anchors": list(planned.anchors),
            "symbol_products": [
                {
                    "symbol": product.symbol,
                    "horizon_plans": list(product.horizon_plans),
                    "book_evidence": list(product.book_evidence),
                    "optimizer_payload": product.optimizer_payload,
                }
                for product in products
            ],
        }
        raw = _canonical(payload)
        assembled[run_name] = (raw, hashlib.sha256(raw).hexdigest())

    serial_order = list(symbols)
    parallel_order = ["EURUSD", "USDJPY", "GBPUSD"]
    assert completion_orders["serial"]["historical_preplan"] == serial_order
    assert completion_orders["serial"]["historical_execute"] == serial_order
    assert completion_orders["parallel"]["historical_preplan"] == parallel_order
    assert completion_orders["parallel"]["historical_execute"] == parallel_order
    assert completion_orders["serial"] != completion_orders["parallel"]
    assert assembled["serial"] == assembled["parallel"]


def test_four_anchor_preplan_uses_one_symbol_wave_and_one_cutoff_batch(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _releases, scheduler, scheduler_calls = bounded_environment
    _install_sparse_planner(monkeypatch)
    anchors = [
        {
            **ANCHOR,
            "anchor_id": f"A{ordinal + 1}",
            "selection_cutoff_utc": f"202{ordinal + 4}-01-01T00:00:00Z",
            "forward_start_date": f"202{ordinal + 4}-02-05",
            "forward_end_date": f"202{ordinal + 5}-02-03",
        }
        for ordinal in range(4)
    ]
    observed_batches = []

    def produce_batch(**kwargs):
        observed_batches.append(kwargs)
        return tuple(
            _Product(
                kwargs["symbol"],
                kwargs["timeframe"],
                native_prefix={
                    "source_reason": None,
                    "cutoff_utc": request["cutoff_utc"],
                },
            )
            for request in kwargs["requests"]
        )

    monkeypatch.setattr(
        producer.native_prefix,
        "produce_activated_native_prefixes",
        produce_batch,
    )
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload={
            **_causal_payload(),
            "process_definition": {"anchor_plan": anchors},
        },
        run_id="historical-run",
        anchors=anchors,
        symbol_scheduler=scheduler,
    )

    assert len(planned.anchors) == 4
    assert [call["phase"] for call in scheduler_calls] == ["historical_preplan"]
    assert len(observed_batches) == 1
    assert [
        request["cutoff_utc"]
        for request in observed_batches[0]["requests"]
    ] == [anchor["selection_cutoff_utc"] for anchor in anchors]


def test_canonical_callback_rejects_any_preplanned_horizon_drift(
    bounded_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, context, _, scheduler, scheduler_calls = bounded_environment
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
        symbol_scheduler=scheduler,
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


def test_all_zero_regime_support_builds_four_unobserved_plans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = _Product("XAUGBP", "D1")
    fitted = SimpleNamespace(
        train_regime_support={
            "counts": {regime: 0 for regime in producer._REGIMES}
        }
    )
    calls = []
    monkeypatch.setattr(
        producer.search_execution,
        "fit_native_timeframe_for_search",
        lambda **kwargs: calls.append(("fit", kwargs)) or fitted,
    )

    def unobserved(*_args, **kwargs):
        regime = kwargs["regime"]
        calls.append(("unobserved", regime))
        return _Plan(
            _Horizon("XAUGBP", "D1", regime, "HISTORICAL_REGIME_UNOBSERVED")
        )

    monkeypatch.setattr(
        producer.search_planner,
        "build_historical_unobserved_cell_plan",
        unobserved,
    )
    monkeypatch.setattr(
        producer.search_planner,
        "build_historical_observed_cell_plan",
        lambda *_args, **_kwargs: pytest.fail("zero-support regime was searched"),
    )

    authority, plans = producer._timeframe_plans(
        context=object(),
        process=object(),
        request=object(),
        causal_precommit_payload={},
        anchor_ordinal=1,
        product=product,
        config=pm_core.PipelineConfig(),
    )

    assert authority is fitted
    assert [plan.horizon_plan.status for plan in plans] == [
        "HISTORICAL_REGIME_UNOBSERVED"
    ] * len(producer._REGIMES)
    assert calls[0][0] == "fit"
    assert calls[1:] == [
        ("unobserved", regime) for regime in producer._REGIMES
    ]


def test_authenticated_terminal_technical_builds_four_abstention_plans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = _Product(
        "EURUSD",
        "M5",
        source_state=native_prefix.TECHNICAL_TERMINAL,
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
        _Plan(
            _Horizon(
                "EURUSD",
                "M5",
                regime,
                "HISTORICAL_TERMINAL_TECHNICAL",
            )
        )
        for regime in producer._REGIMES
    )
    monkeypatch.setattr(
        producer.search_planner,
        "build_historical_terminal_timeframe_plans",
        lambda *_args, **kwargs: calls.append(("plans", kwargs)) or plans,
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
        "causal_precommit_payload_sha256": (planned.causal_precommit_payload_sha256),
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
    data_dir, context, _, scheduler, scheduler_calls = bounded_environment
    _install_sparse_planner(monkeypatch)
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=_causal_payload(),
        run_id="historical-run",
        anchors=[ANCHOR],
        symbol_scheduler=scheduler,
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

    def stage_products(**kwargs):
        calls.append(kwargs)
        return tuple(
            {
                "symbol": kwargs["symbol"],
                "timeframe": kwargs["timeframe"],
                "cutoff_utc": request["cutoff_utc"],
            }
            for request in kwargs["requests"]
        )

    monkeypatch.setattr(
        producer.native_prefix,
        "stage_activated_native_prefixes",
        stage_products,
    )
    resolution = planned(_forward_request(planned))

    assert len(calls) == 1  # M5 execution and valuation share one sealed product
    assert calls[0]["requests"][0]["cutoff_utc"] == "2025-02-04T00:00:00Z"
    assert calls[0]["production_contract_required"] is True
    assert scheduler_calls[-1] == {
        "phase": "historical_forward_stage",
        "contract_phase": "FORWARD_STAGE",
    }
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
    data_dir, context, _, scheduler, _scheduler_calls = bounded_environment
    _install_sparse_planner(monkeypatch)
    planned = producer.preplan_historical_search(
        data_dir=data_dir,
        context=context,
        config=pm_core.PipelineConfig(),
        process=_fake_process(),
        causal_precommit_payload=_causal_payload(),
        run_id="historical-run",
        anchors=[ANCHOR],
        symbol_scheduler=scheduler,
    )
    monkeypatch.setattr(
        producer.native_prefix,
        "stage_activated_native_prefixes",
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
    data_dir, context, _, scheduler, _scheduler_calls = bounded_environment
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
        symbol_scheduler=scheduler,
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
        lambda observed_context, _binding: (
            {
                "bootstrap_identity_sha256": "e" * 64,
                "bootstrap_identity": {
                    "process_precommitment_sha256": producer.causal.canonical_sha256(
                        payload
                    ),
                    "process_definition_sha256": None,
                },
            }
            if observed_context is context
            else pytest.fail("generation context changed")
        ),
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
        "stage_activated_native_prefixes",
        lambda **kwargs: (
            prefix_calls.append(kwargs)
            or tuple(
                {
                    "symbol": kwargs["symbol"],
                    "timeframe": kwargs["timeframe"],
                    "cutoff_utc": request["cutoff_utc"],
                }
                for request in kwargs["requests"]
            )
        ),
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
        lambda observed_context, value: (
            observed.append("verify")
            if observed_context is context and value is forward_product
            else pytest.fail("forward product changed")
        ),
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
    assert body["ram_admission_gate"] is False
    assert (
        body["symbol_scheduler_worker_source"]
        == "PIPELINE_OPTIMIZATION_WORKERS"
    )
    assert "symbol_scheduler_max_workers" not in body
    assert "symbol_scheduler_reserve_bytes" not in body
    assert (
        body["confers_causal_current_capital_publication_or_trading_authority"] is False
    )
    assert len(producer.HISTORICAL_SEARCH_PRODUCER_RELEASE_SHA256) == 64


@pytest.mark.parametrize("edition", ["v15", "v16"])
def test_declared_verified_parent_selects_transport_capture_and_executor(tmp_path, monkeypatch, edition):
    """Real issued parent/request dispatch; native, fit and outcome are labelled stubs."""
    import pm_historical_production_adapter as adapter
    from tests.test_declared_slot_owner_surface import (
        _declared_transport_environment, _request, _structural_completion,
        _structural_native_snapshot, _structural_snapshot_backend,
    )

    snapshot = _structural_native_snapshot()
    env = _declared_transport_environment(tmp_path, monkeypatch, edition, native_snapshot=snapshot)
    request = _request(env)
    request_before = request._body_json
    source_calls = _structural_snapshot_backend(env, monkeypatch,
        observed=snapshot, state=native_prefix.AVAILABLE)
    fitted = SimpleNamespace(
        source_group_decision=SimpleNamespace(receipt_sha256="3" * 64,
            to_dict=lambda: {"availability": producer.book.AVAILABLE}),
        regime_fit_decision=SimpleNamespace(receipt_sha256="4" * 64,
            to_dict=lambda: {"fit_state": producer.book.REGIME_FITTED}),
        train_regime_support={"counts": {regime: 1 for regime in producer.book.REGIME_ORDER}},
        _spread_tiers={tier: SimpleNamespace(descriptor=lambda: {"profile_sha256": "e" * 64})
                       for tier in ("P75", "P95")},
    )
    monkeypatch.setattr(search_execution, "fit_native_timeframe_for_search", lambda **kwargs: fitted)
    plan = SimpleNamespace(classification="STRUCTURAL_DISPATCH_PLAN_NOT_OWNER_EVIDENCE")
    monkeypatch.setattr(planner, "build_declared_slot_observed_cell_plan", lambda *args, **kwargs: plan)
    observed = []

    def open_capture(context, *, request, process, plan, source_session):
        assert context is env.context and process is env.process
        assert producer._verified_declared_slot_source_session(context, request=request,
            process=process, source_session=source_session) is source_session
        observed.append(("capture", edition))
        return SimpleNamespace(request=request, plan=plan, _completion=None, completion=None)

    def execute_cell(**kwargs):
        assert kwargs["fitted"] is fitted and kwargs["cell_plan"] is plan
        capture = kwargs["capture"]
        completion = _structural_completion(capture.request, reason="STRUCTURAL_DISPATCH_NOT_OWNER_EVIDENCE")
        capture._completion = capture.completion = completion
        observed.append(("executor", edition))

    def forbidden(*args, **kwargs):
        pytest.fail("Verified parent dispatched to the opposite transport edition")

    monkeypatch.setattr(adapter, "open_declared_slot_capture", open_capture if edition == "v15" else forbidden)
    monkeypatch.setattr(adapter, "open_transport_declared_slot_capture", open_capture if edition == "v16" else forbidden)
    monkeypatch.setattr(search_execution, "execute_historical_observed_cell", execute_cell if edition == "v15" else forbidden)
    monkeypatch.setattr(search_execution, "execute_transport_historical_observed_cell", execute_cell if edition == "v16" else forbidden)
    completion = producer.execute_declared_slot_owner_search(env.context,
        request=request, process=env.process, frozen_pipeline_config=env.frozen)
    expected = historical.DECLARED_WINNER_TRANSPORT_V16 if edition == "v16" else historical.DECLARED_WINNER_TRANSPORT_V15
    assert historical.declared_slot_transport_edition(env.precommit.to_dict()) == expected
    assert historical.declared_slot_transport_edition(completion["request_body"]) == expected
    assert observed == [("capture", edition), ("executor", edition)]
    assert source_calls == ["observe", "produce", "reverify"]
    assert completion["classification"] == "STRUCTURAL_STUB_NOT_OWNER_EVIDENCE"
    assert completion["status"] == "NO_TRADE" and request._body_json == request_before


@pytest.fixture
def parallel_capacity_store(tmp_path, monkeypatch):
    """INVENTED metadata-only parallel custody; no source, fit or market model."""
    context = spool.freeze_generation_context(tmp_path / "parallel-spool",
        generation_id="INVENTED-capacity-reentry-software", native_universe_sha256="a" * 64,
        config_sha256="b" * 64)
    slots = tuple({"slot_id": f"INVENTED-slot-{ordinal:02d}", "ordinal": ordinal}
        for ordinal in range(1, 14))
    parent = {"source_access_window_utc": {"start_utc": "2025-01-01T00:00:00Z",
        "end_exclusive_utc": "2027-01-01T00:00:00Z"},
        "cells": [{"symbol": "EURUSD", "cell_id": "f" * 64}]}
    precommit = SimpleNamespace(receipt_sha256="c" * 64, to_dict=lambda: parent)
    process = SimpleNamespace(receipt_sha256="d" * 64, spool_receipt={"INVENTED": True})
    frozen = SimpleNamespace(active_config_path=tmp_path / "config.json", effective_config_sha256="e" * 64)
    frozen.active_config_path.write_text("{}", encoding="ascii")
    cell_id = "f" * 64
    root = producer._parallel_slot_root(precommit.receipt_sha256, cell_id)
    monkeypatch.setattr(producer, "_now_utc_seconds", lambda: "2026-01-01T00:00:00Z")
    monkeypatch.setattr(historical, "verify_declared_slot_owner_run_precommit", lambda *args, **kwargs: parent)
    monkeypatch.setattr(historical, "declared_slot_selected_slots", lambda body: slots)
    monkeypatch.setattr(historical, "declared_slot_source_kind", lambda body: "broker_native")

    def spec(ordinal, *, prior=None, attempt=None):
        return producer._parallel_slot_spec(context, precommit, process, frozen, cell_id,
            slots[ordinal - 1], attempt or f"INVENTED-attempt-{ordinal:02d}", prior_capacity=prior)

    def completion(binding):
        return {"status": "NO_TRADE", "request_body": {key: binding[key]
            for key in ("precommit_sha256", "cell_id", "slot_id", "attempt_id")}}

    def persist_preunit(binding):
        body = {"schema_version": "INVENTED_REQUEST_METADATA_ONLY_NO_ISSUANCE_AUTHORITY",
            **{key: binding[key] for key in ("precommit_sha256", "cell_id", "slot_id", "attempt_id")}}
        request_path = (f"declared-slots/{precommit.receipt_sha256}/requests/"
            f"{cell_id}/{binding['slot_id']}/{binding['attempt_id']}.json")
        start_path = (f"declared-slots/{precommit.receipt_sha256}/{cell_id}/attempts/"
            f"{binding['slot_id']}/{binding['attempt_id']}.json")
        spool._publish_exact_json(context, request_path, body, context.limits.max_json_bytes)
        spool._publish_exact_json(context, start_path,
            {"schema_version": "fxpm.declared-slot-attempt-start.v1", "slot_id": binding["slot_id"],
                "request_sha256": _sha(body)}, context.limits.max_json_bytes)
        return body, (request_path, start_path)

    def publish(binding, *, failure=None, completed=False, legacy=False):
        selected = producer._parallel_slot_spec_root(binding)
        claim_path = producer._parallel_slot_path(selected, "claims", binding["ordinal"])
        schema = (producer._PARALLEL_RECOVERY_CLAIM_SCHEMA
            if producer._PARALLEL_RECOVERY_KEY in binding else producer._PARALLEL_CLAIM_SCHEMA)
        claim = {"schema_version": schema, "binding": binding}
        claim_record = spool._publish_exact_json(context, claim_path, claim, context.limits.max_json_bytes)
        result = {"schema_version": (producer._PARALLEL_RESULT_SCHEMA if completed or legacy
                else producer._PARALLEL_FAILURE_RESULT_SCHEMA),
            "claim_sha256": claim_record["sha256"], "completion": completion(binding) if completed else None,
            "artifact_roster": [], "post_completion_failure": None, "failure": failure}
        relative = producer._parallel_slot_path(selected, "results", binding["ordinal"])
        saved = spool._publish_exact_json(context, relative, result, context.limits.max_json_bytes)
        return result, saved["sha256"], (claim_path, relative)

    def commit(_context, *, slot, completion, artifact_roster, previous_commit_sha256, **kwargs):
        body = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
            "generation_context_sha256": context.context_sha256, "precommit_sha256": precommit.receipt_sha256,
            "cell_id": cell_id, "slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
            "previous_commit_sha256": previous_commit_sha256, "completion": completion,
            "artifact_roster": artifact_roster}
        descriptor = spool.write_canonical_json_artifact(context, body)
        pointer = {"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA, "commit_descriptor": descriptor}
        spool._publish_exact_json(context, producer._declared_commit_path(precommit, cell_id, slot["ordinal"]),
            pointer, context.limits.max_json_bytes)
        summary = {"slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
            "status": completion["status"], "commit_sha256": descriptor["sha256"]}
        return summary, descriptor

    monkeypatch.setattr(producer, "_commit_declared_slot_completion", commit)
    return SimpleNamespace(context=context, precommit=precommit, process=process, frozen=frozen,
        slots=slots, parent=parent, cell_id=cell_id, root=root, spec=spec,
        completion=completion, publish=publish, persist_preunit=persist_preunit)


@pytest.mark.parametrize("kind", ["disk", "memory", "wrapped-disk", "wrapped-memory"])
def test_parallel_failure_preserves_actual_capacity_kind(kind):
    import errno

    original = MemoryError("INVENTED") if "memory" in kind else OSError(errno.ENOSPC, "INVENTED")
    error = original
    if kind.startswith("wrapped-"):
        error = RuntimeError("INVENTED wrapper")
        error.__cause__ = original
    failure = producer._parallel_slot_failure(error)
    expected = "MEMORY_ERROR" if "memory" in kind else "DISK_LOW"
    assert failure["resource_capacity_reason"] == failure["failure_code"] == expected
    assert producer._parallel_slot_capacity({"failure": failure}) == (True, expected)


@pytest.mark.parametrize("error", [ValueError("MEMORY_ERROR"),
    RuntimeError("DISK_LOW"), producer.HistoricalSearchProducerError("MEMORY_ERROR")])
def test_parallel_capacity_is_not_inferred_from_error_text(error):
    failure = producer._parallel_slot_failure(error)
    assert failure["resource_capacity_reason"] is None
    assert producer._parallel_slot_capacity({"failure": failure}) == (False, None)


@pytest.mark.parametrize("exception_type,reason,proven", [
    ("pm_authority_spool.AuthoritySpoolError", None, False),
    ("builtins.MemoryError", "MEMORY_ERROR", True),
    ("pm_authority_spool_quota.AuthoritySpoolQuotaError", "DISK_LOW", True),
    ("builtins.RuntimeError", None, False), ("builtins.OSError", None, False),
    ("INVENTED.CustomTechnicalCodeError", None, False)])
def test_parallel_legacy_collapsed_capacity_requires_independent_type_evidence(exception_type, reason, proven):
    failure = {"exception_type": exception_type, "failure_code": producer._PARALLEL_CAPACITY_FAILURE,
        "failure_observed_at_utc": "2026-01-01T00:00:00Z"}
    assert producer._parallel_slot_capacity({"failure": failure}) == (proven, reason)


@pytest.mark.parametrize("defect", ["reason-type", "reason-value", "code-disagrees"])
def test_parallel_capacity_reason_is_validated_in_saved_transport(parallel_capacity_store, defect):
    env = parallel_capacity_store
    failure = producer._parallel_slot_failure(MemoryError("INVENTED"))
    failure["resource_capacity_reason"] = {"reason-type": [], "reason-value": "CAPACITY", "code-disagrees": "DISK_LOW"}[defect]
    _, digest, paths = env.publish(env.spec(1), failure=failure)
    claim = producer._read_declared_record(env.context, paths[0])
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_RESULT_INVALID"):
        producer._parallel_slot_read_result(env.context, paths[1], claim[1], env.spec(1), expected_sha=digest)


def test_parallel_capacity_reentry_is_one_later_entry_and_preserves_all_history(parallel_capacity_store, monkeypatch):
    env = parallel_capacity_store
    completed, artifacts, admitted = [], [], []

    def failed_dispatch(tasks, gate, task_ready):
        for task in tasks:
            if gate():
                assert task_ready(task)
                spec, = task.args
                admitted.append(spec)
                _, digest, _ = env.publish(spec, failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
                yield SimpleNamespace(task_key=task.key, status="COMPLETED", value=digest)
            else:
                yield SimpleNamespace(task_key=task.key, status="NOT_RUN")

    def execute(dispatch):
        return producer._execute_parallel_declared_slots(env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, cell_id=env.cell_id,
            slots=env.slots, completed=completed, commit_artifacts=artifacts,
            previous=completed[-1]["commit_sha256"] if completed else None,
            terminal_failure=None, terminal_stop=False, slot_dispatch=dispatch, slot_worker_limit=1)

    with pytest.raises(producer.DeclaredParallelSlotCapacityError) as caught:
        execute(failed_dispatch)
    assert caught.value.capacity_reason == "MEMORY_ERROR"
    assert len(admitted) == 1 and completed == [] and artifacts == []
    old_paths = tuple(env.context.root / producer._parallel_slot_path(env.root, kind, 1)
        for kind in ("claims", "results"))
    old_bytes = {path: path.read_bytes() for path in old_paths}
    prior = producer._read_declared_record(env.context, producer._parallel_slot_path(env.root, "results", 1))[1]

    def success_dispatch(tasks, gate, task_ready):
        for task in tasks:
            assert gate() and task_ready(task)
            spec, = task.args
            admitted.append(spec)
            if spec["ordinal"] == 1:
                assert spec[producer._PARALLEL_RECOVERY_KEY] == prior
            else:
                assert producer._PARALLEL_RECOVERY_KEY not in spec
            _, digest, _ = env.publish(spec, completed=True)
            yield SimpleNamespace(task_key=task.key, status="COMPLETED", value=digest)

    from contextlib import contextmanager
    lock_paths = []
    locked_claim = producer._locked_parallel_slot_claim

    @contextmanager
    def observed_claim(context, relative):
        lock_paths.append(relative)
        with locked_claim(context, relative) as value:
            yield value

    monkeypatch.setattr(producer, "_locked_parallel_slot_claim", observed_claim)
    last, failure = execute(success_dispatch)
    assert lock_paths[-13:] == [producer._parallel_slot_path(
        producer._parallel_slot_spec_root(spec), "claims", spec["ordinal"]) for spec in admitted[1:]]
    assert failure is None and last == completed[-1]["commit_sha256"] and len(completed) == 13
    assert len(admitted) == 14
    assert {path: path.read_bytes() for path in old_paths} == old_bytes
    snapshot = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    execute(lambda *args: pytest.fail("A complete prefix was dispatched again"))
    assert {path: path.read_bytes() for path in snapshot} == snapshot

    run = {"terminal_stop": False, "cell_id": env.cell_id, "precommit_sha256": env.precommit.receipt_sha256,
        "generation_context_sha256": env.context.context_sha256, "completed_slots": 13, "declared_slots": 13,
        "last_commit_sha256": last, "slot_commits": [{"slot_id": item["slot_id"], "status": item["status"]} for item in completed]}
    spool._publish_exact_json(env.context, producer._declared_run_path(env.precommit, env.cell_id),
        run, env.context.limits.max_json_bytes)
    retained = producer.verify_declared_parallel_slot_files(env.context, precommit=env.precommit,
        process=env.process, cell_id=env.cell_id, frozen_pipeline_config=env.frozen)
    assert len(retained) == 28 and len(set(retained)) == 28
    assert all(str(path.relative_to(env.context.root)).replace("\\", "/") in retained for path in old_paths)


@pytest.mark.parametrize("prior_kind", ["success", "technical"])
def test_parallel_reentry_cannot_follow_a_noncapacity_result(parallel_capacity_store, prior_kind):
    env = parallel_capacity_store
    _, prior, _ = env.publish(env.spec(1), completed=prior_kind == "success",
        failure=producer._parallel_slot_failure(ValueError("INVENTED")) if prior_kind == "technical" else None)
    env.publish(env.spec(1, prior=prior, attempt="INVENTED-next-attempt"), completed=True)
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_CUSTODY_INVALID"):
        with producer._bound_parallel_slot_result(env.context, env.precommit, env.process, env.frozen,
                env.cell_id, env.slots[0], env.root):
            pytest.fail("A noncapacity predecessor authorized another attempt")


def test_parallel_reentry_refuses_an_active_original_claim(parallel_capacity_store):
    env = parallel_capacity_store
    env.publish(env.spec(1), failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
    path = producer._parallel_slot_path(env.root, "claims", 1)
    with producer._locked_parallel_slot_claim(env.context, path):
        with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_CLAIM_ACTIVE"):
            producer._parallel_slot_existing(env.context, env.precommit, env.process, env.frozen,
                env.cell_id, env.slots, [])


def test_parallel_capacity_reentry_cannot_extend_the_original_window(parallel_capacity_store):
    env = parallel_capacity_store
    env.publish(env.spec(1), failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
    env.parent["source_access_window_utc"]["end_exclusive_utc"] = "2025-12-31T23:59:59Z"
    before = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_SOURCE_WINDOW_EXPIRED"):
        producer._execute_parallel_declared_slots(env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, cell_id=env.cell_id,
            slots=env.slots, completed=[], commit_artifacts=[], previous=None, terminal_failure=None,
            terminal_stop=False, slot_dispatch=lambda *args: pytest.fail("Expired recovery dispatched"), slot_worker_limit=1)
    assert {path: path.read_bytes() for path in before} == before
    assert not (env.context.root / (env.root + "/reentries")).exists()


def test_parallel_capacity_worker_reuses_the_original_pending_attempt(parallel_capacity_store, monkeypatch):
    """Actual parent/worker selection over stored metadata; issuance/completion are finite doubles."""
    from contextlib import contextmanager

    env = parallel_capacity_store
    original = env.spec(1, attempt="INVENTED-pending-preunit-attempt")
    request, original_paths = env.persist_preunit(original)
    _, prior, failure_paths = env.publish(original, failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
    namespace = spool._local_path(env.context.root, search_execution._declared_unit_relative(request, "header.json")).parent
    assert not namespace.exists()
    monkeypatch.setattr(producer.search_planner, "reopen_production_search_process", lambda *args, **kwargs: env.process)
    monkeypatch.setattr(producer.config_binding, "load_effective_pipeline_config", lambda *args: ({}, object()))
    monkeypatch.setattr(producer.config_binding, "freeze_production_pipeline_config", lambda *args, **kwargs: env.frozen)
    monkeypatch.setattr(historical, "reopen_declared_slot_owner_run_precommit", lambda *args, **kwargs: env.precommit)
    observed = []

    @contextmanager
    def attempt(*args, **kwargs):
        observed.append(kwargs)
        body = {"precommit_sha256": env.precommit.receipt_sha256, "cell_id": env.cell_id,
            "slot_id": kwargs["slot"]["slot_id"], "attempt_id": kwargs["attempt_id"]}
        yield env.completion(body), [], None

    monkeypatch.setattr(producer, "_declared_slot_attempt", attempt)
    protected = tuple(env.context.root / path for path in (*original_paths, *failure_paths))
    before = {path: path.read_bytes() for path in protected}
    admitted = []

    def dispatch(tasks, gate, task_ready):
        for task in tasks:
            assert gate() and task_ready(task)
            spec, = task.args
            admitted.append(spec)
            if spec["ordinal"] == 1:
                assert spec[producer._PARALLEL_RECOVERY_KEY] == prior
                assert spec["attempt_id"] == original["attempt_id"]
            digest = producer.declared_slot_worker(spec)
            yield SimpleNamespace(task_key=task.key, status="COMPLETED", value=digest)

    completed = []
    producer._execute_parallel_declared_slots(env.context, precommit=env.precommit,
        process=env.process, frozen_pipeline_config=env.frozen, cell_id=env.cell_id,
        slots=env.slots, completed=completed, commit_artifacts=[], previous=None,
        terminal_failure=None, terminal_stop=False, slot_dispatch=dispatch, slot_worker_limit=1)
    assert len(admitted) == len(completed) == len(observed) == 13
    assert observed[0]["resumed"] is True
    assert observed[0]["attempt_id"] == original["attempt_id"]
    assert {path: path.read_bytes() for path in before} == before
    assert len(list((env.context.root / original_paths[0]).parent.glob("*.json"))) == 1
    assert len(list((env.context.root / original_paths[1]).parent.glob("*.json"))) == 1
    assert not namespace.exists()


def test_parallel_capacity_reentry_refuses_an_unlinked_history_branch(parallel_capacity_store):
    env = parallel_capacity_store
    env.publish(env.spec(1), failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
    unknown = env.context.root / env.root / "reentries" / "01" / ("0" * 64)
    unknown.mkdir(parents=True)
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_CUSTODY_INVALID"):
        with producer._bound_parallel_slot_result(env.context, env.precommit, env.process, env.frozen,
                env.cell_id, env.slots[0], env.root):
            pytest.fail("Unlinked recovery history was ignored")
    assert unknown.is_dir()


def test_parallel_unfinished_reentry_claim_is_not_automatically_replayed(parallel_capacity_store):
    env = parallel_capacity_store
    _, prior, _ = env.publish(env.spec(1), failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
    spec = env.spec(1, prior=prior, attempt="INVENTED-unresolved-admission")
    relative = producer._parallel_slot_path(producer._parallel_slot_spec_root(spec), "claims", 1)
    spool._publish_exact_json(env.context, relative,
        {"schema_version": producer._PARALLEL_RECOVERY_CLAIM_SCHEMA, "binding": spec}, env.context.limits.max_json_bytes)
    before = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_ORPHAN_UNRESOLVED"):
        producer._execute_parallel_declared_slots(env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, cell_id=env.cell_id,
            slots=env.slots, completed=[], commit_artifacts=[], previous=None, terminal_failure=None,
            terminal_stop=False, slot_dispatch=lambda *args: pytest.fail("Unknown worker admission was replayed"),
            slot_worker_limit=1)
    assert {path: path.read_bytes() for path in before} == before


def test_parallel_capacity_reentry_can_reuse_only_its_unpublished_empty_target(parallel_capacity_store):
    env = parallel_capacity_store
    _, prior, paths = env.publish(env.spec(1), failure=producer._parallel_slot_failure(MemoryError("INVENTED")))
    spec = env.spec(1, prior=prior, attempt="INVENTED-after-claim-publication-failure")
    target = env.context.root / producer._parallel_slot_spec_root(spec)
    (target / "claims").mkdir(parents=True)
    before = {env.context.root / path: (env.context.root / path).read_bytes() for path in paths}
    root, ready = producer._parallel_slot_existing(env.context, env.precommit, env.process, env.frozen,
        env.cell_id, env.slots, [])
    assert root == env.root and ready == {}
    _, digest, _ = env.publish(spec, completed=True)
    with producer._bound_parallel_slot_result(env.context, env.precommit, env.process, env.frozen,
            env.cell_id, env.slots[0], env.root) as (body, observed):
        assert observed == digest and body["completion"]["status"] == "NO_TRADE"
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize("kind", ["message", "custom-code", "os-message", "spool-message"])
def test_parallel_legacy_capacity_text_collision_is_retained_unresolved(parallel_capacity_store, kind):
    env = parallel_capacity_store

    class LegacyTechnicalCodeError(RuntimeError):
        code = producer._PARALLEL_CAPACITY_FAILURE

    error = {"message": RuntimeError(producer._PARALLEL_CAPACITY_FAILURE),
        "custom-code": LegacyTechnicalCodeError("INVENTED technical failure"),
        "os-message": OSError(producer._PARALLEL_CAPACITY_FAILURE),
        "spool-message": spool.AuthoritySpoolError(producer._PARALLEL_CAPACITY_FAILURE)}[kind]
    assert spool.resource_capacity_reason(error) is None
    legacy = {"exception_type": f"{type(error).__module__}.{type(error).__name__}",
        "failure_code": str(getattr(error, "code", None) or error),
        "failure_observed_at_utc": "2026-01-01T00:00:00Z"}
    assert legacy["failure_code"] == producer._PARALLEL_CAPACITY_FAILURE
    env.publish(env.spec(1), failure=legacy, legacy=True)
    before = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_PARALLEL_SLOT_LEGACY_FAILURE_UNRESOLVED"):
        producer._execute_parallel_declared_slots(env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, cell_id=env.cell_id,
            slots=env.slots, completed=[], commit_artifacts=[], previous=None, terminal_failure=None,
            terminal_stop=False, slot_dispatch=lambda *args: pytest.fail("Unproved legacy capacity was retried"),
            slot_worker_limit=1)
    assert {path: path.read_bytes() for path in before} == before
    assert not (env.context.root / env.root / "reentries").exists()


@pytest.mark.parametrize("reason", [None, "RESOURCE_CAPACITY", "DISK_LOW_TEXT"])
def test_parallel_capacity_transport_exception_requires_observed_kind(reason):
    with pytest.raises(ValueError, match="DECLARED_PARALLEL_CAPACITY_REASON_INVALID"):
        producer.DeclaredParallelSlotCapacityError(reason, "INVENTED-slot", "INVENTED.type", "INVENTED-code")


@pytest.mark.parametrize("mode", ["serial", "parallel"])
@pytest.mark.parametrize("unit_lease", [False, True], ids=["before-unit-lease", "released-unit-lease"])
def test_preunit_capacity_reentry_keeps_the_real_issued_request_and_start(tmp_path, monkeypatch, mode, unit_lease):
    """Real issuers/parent/worker and optional unit lease; no source or fit execution."""
    from prototype_support import CELL_ID, _declared_environment

    env = _declared_environment(tmp_path, monkeypatch)
    slot = historical.declared_slot_selected_slots(env.precommit.to_dict())[0]
    observed = []

    def before_unit_capacity(context, *, request, **kwargs):
        body = request.to_dict()
        namespace = spool._local_path(context.root,
            search_execution._declared_unit_relative(body, "header.json")).parent
        if unit_lease:
            with search_execution._declared_unit_lease(context, body):
                assert {path.name for path in namespace.iterdir()} == {"units.lock"}
                observed.append(body)
                raise MemoryError("INVENTED_PREUNIT_CAPACITY_NO_SOURCE_OR_FIT_EXECUTION")
        assert not namespace.exists()
        observed.append(body)
        raise MemoryError("INVENTED_PREUNIT_CAPACITY_NO_SOURCE_OR_FIT_EXECUTION")

    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", before_unit_capacity)
    admitted = []

    def dispatch(tasks, gate, task_ready):
        for task in tasks:
            if gate():
                assert task_ready(task)
                admitted.append(task.args[0])
                digest = producer.declared_slot_worker(*task.args)
                yield SimpleNamespace(task_key=task.key, status="COMPLETED", value=digest)
            else:
                yield SimpleNamespace(task_key=task.key, status="NOT_RUN")

    options = {"slot_dispatch": dispatch, "slot_worker_limit": 1} if mode == "parallel" else {}
    expected = producer.DeclaredParallelSlotCapacityError if mode == "parallel" else MemoryError
    original = None
    for entry in range(2):
        with pytest.raises(expected) as caught:
            producer.execute_declared_cell_owner_slots(context=env.context, precommit=env.precommit,
                process=env.process, frozen_pipeline_config=env.frozen, cell_id=CELL_ID, **options)
        if mode == "parallel":
            assert caught.value.capacity_reason == "MEMORY_ERROR"
        starts = producer._declared_prior_attempt_starts(env.context, env.precommit, CELL_ID, slot)
        assert len(starts) == 1
        attempt = producer._declared_pending_unit_attempt(env.context, env.precommit, CELL_ID, slot)
        assert attempt is not None and attempt == observed[0]["attempt_id"]
        request_path = (f"declared-slots/{env.precommit.receipt_sha256}/requests/"
            f"{CELL_ID}/{slot['slot_id']}/{attempt}.json")
        paths = (env.context.root / request_path, env.context.root / starts[0]["relative_path"])
        saved = {path: path.read_bytes() for path in paths}
        if entry == 0:
            original = saved
        else:
            assert saved == original
        assert len(list(paths[0].parent.glob("*.json"))) == 1
        namespace = spool._local_path(env.context.root,
            search_execution._declared_unit_relative(observed[0], "header.json")).parent
        if unit_lease:
            assert {path.name for path in namespace.iterdir()} == {"units.lock"}
            assert (namespace / "units.lock").read_bytes() == b"0"
        else:
            assert not namespace.exists()
    assert len(observed) == 2 and observed[0] == observed[1]
    if mode == "parallel":
        assert len(admitted) == 2 and admitted[0]["attempt_id"] == admitted[1]["attempt_id"]
        assert producer._PARALLEL_RECOVERY_KEY not in admitted[0]
        assert producer._PARALLEL_RECOVERY_KEY in admitted[1]


@pytest.mark.parametrize("partial", ["empty-directory", "empty-lock", "nul-lock", "wrong-lock",
    "long-lock", "lock-directory", "extra-file", "raw-without-header"])
def test_preunit_reentry_preserves_and_refuses_partial_unit_namespaces(parallel_capacity_store, partial):
    env = parallel_capacity_store
    binding = env.spec(1)
    request, paths = env.persist_preunit(binding)
    namespace = spool._local_path(env.context.root,
        search_execution._declared_unit_relative(request, "header.json")).parent
    namespace.mkdir(parents=True)
    if partial in {"empty-lock", "nul-lock", "wrong-lock", "long-lock", "extra-file"}:
        encoded = {"empty-lock": b"", "nul-lock": b"\0", "wrong-lock": b"x",
                   "long-lock": b"00", "extra-file": b"0"}[partial]
        (namespace / "units.lock").write_bytes(encoded)
        if partial == "extra-file":
            (namespace / "unknown.txt").write_bytes(b"INVENTED_UNKNOWN_PRESERVE")
    elif partial == "lock-directory":
        (namespace / "units.lock").mkdir()
    elif partial == "raw-without-header":
        relative = search_execution._declared_unit_relative(request, "raw-completion.json")
        spool._publish_exact_json(env.context, relative,
            {"fixture_provenance": "INVENTED_ORPHAN_RAW_METADATA_ONLY"}, env.context.limits.max_json_bytes)
    before = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_UNIT_PREUNIT_NAMESPACE_UNRESOLVED"):
        producer._declared_pending_unit_attempt(env.context, env.precommit, env.cell_id, env.slots[0])
    assert {path: path.read_bytes() for path in before} == before
    assert namespace.is_dir()


def test_preunit_reentry_refuses_a_held_real_unit_lease_and_releases_its_probe(parallel_capacity_store):
    """Actual executor kernel lease; request metadata is an explicitly invented fixture."""
    env = parallel_capacity_store
    request, paths = env.persist_preunit(env.spec(1))
    before = {name: spool._local_path(env.context.root, name).read_bytes() for name in paths}
    with search_execution._declared_unit_lease(env.context, request):
        with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_SLOT_RUN_ACTIVE"):
            producer._declared_pending_unit_attempt(env.context, env.precommit, env.cell_id, env.slots[0])
    assert producer._declared_pending_unit_attempt(env.context, env.precommit, env.cell_id, env.slots[0]) == request["attempt_id"]
    # A second actual lease must succeed after the selector's probe, including on Windows.
    with search_execution._declared_unit_lease(env.context, request):
        pass
    lock = spool._local_path(env.context.root, search_execution._declared_unit_relative(request, "units.lock"))
    assert lock.read_bytes() == b"0"
    assert {name: spool._local_path(env.context.root, name).read_bytes() for name in paths} == before
    assert len(producer._declared_prior_attempt_starts(env.context, env.precommit, env.cell_id, env.slots[0])) == 1


def test_preunit_reentry_does_not_choose_between_multiple_authenticated_starts(parallel_capacity_store):
    env = parallel_capacity_store
    env.persist_preunit(env.spec(1, attempt="INVENTED-first-unresolved-attempt"))
    env.persist_preunit(env.spec(1, attempt="INVENTED-second-unresolved-attempt"))
    before = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    with pytest.raises(producer.HistoricalSearchProducerError, match="DECLARED_UNIT_OWNER_AMBIGUOUS"):
        producer._declared_pending_unit_attempt(env.context, env.precommit, env.cell_id, env.slots[0])
    assert {path: path.read_bytes() for path in before} == before
