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
