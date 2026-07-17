from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_book_multiplicity as book
import pm_production_authority_coordinator as coordinator
import pm_production_optimization_runner as runner
from pm_core import PipelineConfig


UTC = timezone.utc


def _sha(value: bytes | str) -> str:
    raw = value if type(value) is bytes else str(value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _request(
    tmp_path: Path, *, generation_id: str
) -> runner.ProductionOptimizationRequest:
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    payload = {
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "pipeline": {"timeframes": list(book.TIMEFRAME_ORDER)},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    config = PipelineConfig(
        data_dir=data_dir,
        timeframes=list(book.TIMEFRAME_ORDER),
    )
    config._deployment_config_path = str(config_path)  # noqa: SLF001
    return runner.ProductionOptimizationRequest(
        active_config_path=config_path,
        config=config,
        config_payload=payload,
        requested_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        native_universe=SimpleNamespace(
            activation_id="activation-1",
            activation_manifest_sha256=_sha("activation"),
        ),
        generation_id=generation_id,
    )


def _install_success_chain(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[str],
    wrong_ledger_sha: bool = False,
) -> dict[str, object]:
    state: dict[str, object] = {
        "native_calls": [],
        "coordinator_calls": 0,
        "materialized_configs": [],
    }
    environment = {
        "config_sha256": _sha("config"),
        "native_universe_sha256": _sha("native-universe"),
        "native_identity": {
            "native_activation_id": "activation-1",
            "native_activation_manifest_sha256": _sha("activation"),
        },
        "capital_contract": {"contract": "exact"},
        "margin_terms_sha256": _sha("margin"),
        "broker_terms_sha256": _sha("broker"),
    }
    plan = {
        "activation_id": "activation-1",
        "activation_manifest_sha256": _sha("activation"),
        "anchor_plan": [
            {
                "anchor_id": f"anchor-{index}",
                "selection_cutoff_utc": f"202{index}-01-01T00:00:00Z",
                "forward_start_date": f"202{index}-02-05",
                "forward_end_date": f"202{index + 1}-02-04",
                "purge_calendar_days": 35,
            }
            for index in range(4)
        ],
        "inner_selection_windows_by_anchor": {
            f"anchor-{index}": {
                "start_date": f"201{index}-01-01",
                "end_date": f"202{index}-01-01",
            }
            for index in range(4)
        },
        "current_selection_window": {
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
        },
        "current_refit_cutoff_utc": "2026-01-01T00:00:00Z",
    }
    plan_receipt = SimpleNamespace(receipt_sha256=_sha("plan"))

    def derive_environment(**_kwargs: object) -> dict[str, object]:
        events.append("environment")
        return environment

    def derive_plan(_data_dir: Path) -> object:
        events.append("anchor-plan")
        return plan_receipt

    def verify_plan(_data_dir: Path, receipt: object) -> dict[str, object]:
        assert receipt is plan_receipt
        events.append("anchor-plan-verified")
        return {**plan, "receipt_sha256": plan_receipt.receipt_sha256}

    context = SimpleNamespace(
        generation_id="placeholder",
        context_sha256=_sha("context"),
        native_universe_sha256=environment["native_universe_sha256"],
        config_sha256=environment["config_sha256"],
        root=Path("placeholder"),
    )

    def freeze_context(root: Path, **kwargs: object) -> object:
        events.append("generation-context")
        root.mkdir(parents=True)
        context.root = root
        context.generation_id = kwargs["generation_id"]
        return context

    class _FrozenConfig:
        def __init__(
            self,
            supplied_config: PipelineConfig,
            config_payload: object,
        ) -> None:
            self._supplied_config = supplied_config
            self._config_payload = copy.deepcopy(config_payload)

        def materialize(self, observed_context: object) -> PipelineConfig:
            assert observed_context is context
            materialized = copy.deepcopy(self._supplied_config)
            configs = state["materialized_configs"]
            assert isinstance(configs, list)
            configs.append(materialized)
            events.append("config-materialize")
            return materialized

        def payload(self) -> dict[str, object]:
            return copy.deepcopy(self._config_payload)

    def freeze_config(
        observed_context: object,
        *,
        active_config_path: Path,
        config_payload: object,
        supplied_config: PipelineConfig,
    ) -> object:
        assert observed_context is context
        assert active_config_path.name == "config.json"
        events.append("config-binding")
        return _FrozenConfig(supplied_config, config_payload)

    search_process = SimpleNamespace(receipt_sha256=_sha("search-process"))

    def freeze_search(_context: object, *, run_id: str) -> object:
        assert run_id.startswith("production-run-")
        events.append("search-process")
        return search_process

    now = datetime.now(UTC).replace(microsecond=0)
    causal_payload = {
        "evidence_access_not_before_utc": (now - timedelta(seconds=1))
        .isoformat()
        .replace("+00:00", "Z"),
        "evidence_access_deadline_utc": (now + timedelta(days=1))
        .isoformat()
        .replace("+00:00", "Z"),
        "process_definition": {"anchor_plan": plan["anchor_plan"]},
    }
    causal_precommit = SimpleNamespace(
        receipt_sha256=_sha("causal-precommit"),
        to_dict=lambda: json.loads(json.dumps(causal_payload)),
    )

    def freeze_causal(**_kwargs: object) -> object:
        events.append("causal-precommit")
        return causal_precommit

    capital = SimpleNamespace(
        identity_sha256=_sha("capital-bootstrap"),
        forward_phase_binding=object(),
        capital_factory=lambda _request: object(),
    )

    def freeze_capital(**_kwargs: object) -> object:
        events.append("capital-bootstrap")
        return capital

    class _Preplanned:
        anchors = tuple(plan["anchor_plan"])

        @staticmethod
        def anchor_producers() -> dict[str, object]:
            return {"anchor": lambda _request: ()}

        def __call__(self, _request: object) -> None:
            return None

    preplanned = _Preplanned()

    def preplan(**kwargs: object) -> object:
        events.append("historical-preplan")
        state["historical_config"] = kwargs["config"]
        return preplanned

    def causal_factory(**_kwargs: object) -> object:
        events.append("causal-factory")
        return SimpleNamespace()

    def produce_native(**kwargs: object) -> object:
        if not events or events[-1] == "capital-bootstrap":
            raise AssertionError("current evidence accessed before coordinator")
        calls = state["native_calls"]
        assert isinstance(calls, list)
        calls.append(
            (
                kwargs["symbol"],
                kwargs["timeframe"],
                kwargs["cutoff_utc"],
                kwargs["production_contract_required"],
            )
        )
        if len(calls) == 1:
            events.append("current-native")
        return SimpleNamespace(source_state="AVAILABLE", native_prefix={})

    current_product = object()

    def build_current(inputs: object) -> object:
        events.append("current-producer")
        state["current_config"] = inputs.pipeline_config
        assert inputs.active_config_path.name == "config.json"
        assert inputs.config_payload == {
            "symbols": list(book.PINNED_SYMBOL_ORDER),
            "pipeline": {"timeframes": list(book.TIMEFRAME_ORDER)},
        }
        assert list(inputs.native_products) == [
            f"{symbol}|{timeframe}"
            for symbol in book.PINNED_SYMBOL_ORDER
            for timeframe in book.TIMEFRAME_ORDER
        ]
        return lambda _request: current_product

    def coordinate(**kwargs: object) -> coordinator.ProductionAuthorityRun:
        state["coordinator_calls"] = int(state["coordinator_calls"]) + 1
        events.append("coordinator-enter")
        assert state["native_calls"] == []
        assert (
            kwargs["current_refit_factory"](coordinator.CurrentRefitRequest(b"{}"))
            is current_product
        )
        assembly_inputs = kwargs["assembly_inputs"]
        state["assembly_config"] = assembly_inputs.config
        assembly_dir = assembly_inputs.deployment_root / assembly_inputs.target_name
        assembly_dir.mkdir()
        staged = assembly_inputs.deployment_root / ".coordinator-ledger.json"
        ledger_raw = b'{"AUDUSD":{"winner":"genuine"}}'
        staged.write_bytes(ledger_raw)
        ledger_sha = _sha(ledger_raw)
        receipt = {
            "generation_id": assembly_inputs.generation_id,
            "authorization_scope": assembly_inputs.authorization_scope,
            "requested_symbols": list(assembly_inputs.requested_symbols),
            "ledger_sha256": ledger_sha,
        }
        descriptors = {
            "causal_process": {
                "relative_path": (f"{assembly_inputs.target_name}/causal_process.json"),
                "sha256": _sha("causal-index"),
                "size_bytes": 1,
            }
        }
        assembly = SimpleNamespace(
            directory=assembly_dir,
            chain_receipt_canonical_json=json.dumps(
                receipt, separators=(",", ":"), sort_keys=True
            ).encode("ascii"),
            descriptors_canonical_json=json.dumps(
                descriptors, separators=(",", ":"), sort_keys=True
            ).encode("ascii"),
        )
        return coordinator.ProductionAuthorityRun(
            schema_version=coordinator.COORDINATOR_SCHEMA,
            assembly=assembly,
            staged_ledger_path=staged,
            staged_ledger_content_sha256=(
                _sha("wrong") if wrong_ledger_sha else ledger_sha
            ),
            physical_ledger_sha256=_sha("physical-ledger"),
            historical_run_receipt_sha256=_sha("historical-run"),
            historical_process_decision_sha256=_sha("historical-process"),
            current_roster_sha256=_sha("current-roster"),
            capital_decision_sha256=_sha("capital-decision"),
            selected_policy_point_id="policy-medium-high",
        )

    monkeypatch.setattr(
        runner.coordinator,
        "derive_production_environment_authority",
        derive_environment,
    )
    monkeypatch.setattr(
        runner.anchor_plan, "derive_production_anchor_plan", derive_plan
    )
    monkeypatch.setattr(
        runner.anchor_plan,
        "verify_production_anchor_plan_receipt",
        verify_plan,
    )
    monkeypatch.setattr(
        runner.authority_spool, "freeze_generation_context", freeze_context
    )
    monkeypatch.setattr(
        runner.config_binding,
        "freeze_production_pipeline_config",
        freeze_config,
    )
    monkeypatch.setattr(
        runner.search_planner,
        "freeze_production_search_process",
        freeze_search,
    )
    monkeypatch.setattr(
        runner.causal_evidence,
        "freeze_causal_process_precommit_v8",
        freeze_causal,
    )
    monkeypatch.setattr(
        runner.capital_bootstrap,
        "bootstrap_production_capital_authority",
        freeze_capital,
    )
    monkeypatch.setattr(
        runner.historical_search,
        "preplan_historical_search",
        preplan,
    )
    monkeypatch.setattr(
        runner.causal_evidence,
        "BoundedCausalEvidenceFactory",
        causal_factory,
    )
    monkeypatch.setattr(
        runner.native_prefix,
        "produce_activated_native_prefix",
        produce_native,
    )
    monkeypatch.setattr(
        runner.current_refit,
        "build_current_refit_search_producer",
        build_current,
    )
    monkeypatch.setattr(
        runner.coordinator, "coordinate_production_authority", coordinate
    )
    return state


def test_runner_orders_full_chain_and_returns_exact_pre_cas_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    state = _install_success_chain(monkeypatch, events=events)
    last_good = tmp_path / "pm_configs.json"
    last_good.write_bytes(b'{"last":"good"}')

    result = runner.run_production_optimization(
        _request(tmp_path, generation_id="generation-success")
    )

    assert state["coordinator_calls"] == 1
    assert events.index("generation-context") < events.index("config-binding")
    assert events.index("config-binding") < events.index("anchor-plan")
    assert events.index("capital-bootstrap") < events.index("historical-preplan")
    assert events.index("historical-preplan") < events.index("coordinator-enter")
    assert events.index("coordinator-enter") < events.index("current-native")
    calls = state["native_calls"]
    assert isinstance(calls, list)
    assert calls == [
        (symbol, timeframe, "2026-01-01T00:00:00Z", True)
        for symbol in book.PINNED_SYMBOL_ORDER
        for timeframe in book.TIMEFRAME_ORDER
    ]
    assert len(calls) == 100 * 6
    materialized_configs = state["materialized_configs"]
    assert isinstance(materialized_configs, list)
    assert len(materialized_configs) == 3
    assert len({id(config) for config in materialized_configs}) == 3
    assert state["historical_config"] is materialized_configs[0]
    assert state["current_config"] is materialized_configs[1]
    assert state["assembly_config"] is materialized_configs[2]
    assert result.staged_ledger_bytes == b'{"AUDUSD":{"winner":"genuine"}}'
    assert result.staged_ledger_content_sha256 == _sha(result.staged_ledger_bytes)
    assert result.run_root.relative_to(tmp_path).parts[0] == "fxpm-production-runs"
    assert result.authority_chain_receipt["ledger_sha256"] == (
        result.staged_ledger_content_sha256
    )
    assert result.manifest_fields()["selected_policy_point_id"] == (
        "policy-medium-high"
    )
    assert last_good.read_bytes() == b'{"last":"good"}'
    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="AUTHORITY_TARGET_ALREADY_EXISTS",
    ):
        runner.run_production_optimization(
            _request(tmp_path, generation_id="generation-success")
        )
    assert result.run_root.is_dir()
    assert result.authority_assembly_path.is_dir()
    unrelated = tmp_path / "unrelated-owned-by-operator"
    unrelated.mkdir()
    tampered = replace(result, run_root=unrelated)
    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="UNPUBLISHED_PRODUCTION_RESULT_CAPABILITY_INVALID",
    ):
        runner.discard_unpublished_production_result(tampered)
    assert unrelated.is_dir()
    invalid_receipt = replace(result, _authority_chain_receipt_json=b"not-json")
    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="UNPUBLISHED_PRODUCTION_RESULT_CAPABILITY_INVALID",
    ):
        runner.discard_unpublished_production_result(invalid_receipt)
    assert result.run_root.is_dir()
    victim_generation = "separate-valid-generation"
    victim_digest = hashlib.sha256(victim_generation.encode("utf-8")).hexdigest()
    victim_run = tmp_path / "fxpm-production-runs" / f"run-{victim_digest}"
    victim_authority = tmp_path / f"fxpm-authority-{victim_digest}"
    victim_run.mkdir()
    victim_authority.mkdir()
    coherent_tamper = replace(
        result,
        generation_id=victim_generation,
        run_root=victim_run,
        authority_assembly_path=victim_authority,
    )
    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="UNPUBLISHED_PRODUCTION_RESULT_CAPABILITY_INVALID",
    ):
        runner.discard_unpublished_production_result(coherent_tamper)
    assert victim_run.is_dir()
    assert victim_authority.is_dir()
    victim_run.rmdir()
    victim_authority.rmdir()

    class _SplitPath:
        def __init__(self, display: Path, coerced: Path) -> None:
            self._display = display
            self._coerced = coerced

        def __str__(self) -> str:
            return str(self._display)

        def __fspath__(self) -> str:
            return str(self._coerced)

    source_digest = hashlib.sha256(result.generation_id.encode("utf-8")).hexdigest()
    split_root = tmp_path / "separate-deployment"
    split_run = split_root / "fxpm-production-runs" / f"run-{source_digest}"
    split_authority = split_root / f"fxpm-authority-{source_digest}"
    split_run.mkdir(parents=True)
    split_authority.mkdir()
    split_tamper = replace(
        result,
        run_root=_SplitPath(result.run_root, split_run),
        authority_assembly_path=_SplitPath(
            result.authority_assembly_path,
            split_authority,
        ),
    )
    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="UNPUBLISHED_PRODUCTION_RESULT_INVALID",
    ):
        runner.discard_unpublished_production_result(split_tamper)
    assert split_run.is_dir()
    assert split_authority.is_dir()
    assert result.run_root.is_dir()
    assert result.authority_assembly_path.is_dir()
    split_run.rmdir()
    split_authority.rmdir()
    (split_root / "fxpm-production-runs").rmdir()
    split_root.rmdir()
    assert runner.discard_unpublished_production_result(result) is True
    assert runner.discard_unpublished_production_result(result) is True
    assert not result.run_root.exists()
    assert not result.authority_assembly_path.exists()


def test_runner_rejects_partial_symbol_roster_before_authority_access(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path, generation_id="generation-partial")
    request = runner.ProductionOptimizationRequest(
        active_config_path=request.active_config_path,
        config=request.config,
        config_payload=request.config_payload,
        requested_symbols=tuple(book.PINNED_SYMBOL_ORDER[:-1]),
        native_universe=request.native_universe,
        generation_id=request.generation_id,
    )

    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="PRODUCTION_SYMBOL_ROSTER_NOT_EXACT",
    ):
        runner.run_production_optimization(request)

    assert not (tmp_path / "fxpm-production-runs").exists()


def test_runner_does_not_sleep_or_access_evidence_before_not_before(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_success_chain(monkeypatch, events=events)
    future = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=1)

    def future_precommit(**_kwargs: object) -> object:
        events.append("causal-precommit")
        payload = {
            "evidence_access_not_before_utc": future.isoformat().replace("+00:00", "Z"),
            "evidence_access_deadline_utc": (future + timedelta(days=1))
            .isoformat()
            .replace("+00:00", "Z"),
        }
        return SimpleNamespace(
            receipt_sha256=_sha("future-precommit"),
            to_dict=lambda: payload,
        )

    monkeypatch.setattr(
        runner.causal_evidence,
        "freeze_causal_process_precommit_v8",
        future_precommit,
    )

    for ordinal in range(3):
        with pytest.raises(
            runner.ProductionOptimizationRunnerError,
            match="EVIDENCE_ACCESS_WINDOW_NOT_OPEN",
        ):
            runner.run_production_optimization(
                _request(tmp_path, generation_id=f"generation-future-{ordinal}")
            )

    assert "capital-bootstrap" in events
    assert "historical-preplan" not in events
    assert "coordinator-enter" not in events
    runs_root = tmp_path / "fxpm-production-runs"
    assert runs_root.is_dir()
    assert list(runs_root.iterdir()) == []
    assert list(tmp_path.glob("fxpm-authority-*")) == []


def test_runner_rejects_coordinator_ledger_byte_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_success_chain(monkeypatch, events=events, wrong_ledger_sha=True)
    last_good = tmp_path / "pm_configs.json"
    last_good.write_bytes(b'{"last":"good"}')

    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="COORDINATOR_STAGED_LEDGER_HASH_MISMATCH",
    ):
        runner.run_production_optimization(
            _request(tmp_path, generation_id="generation-substitution")
        )

    assert last_good.read_bytes() == b'{"last":"good"}'
    assert list((tmp_path / "fxpm-production-runs").iterdir()) == []
    assert list(tmp_path.glob("fxpm-authority-*")) == []


def test_lazy_current_technical_failure_is_not_a_market_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_calls: list[object] = []
    monkeypatch.setattr(
        runner.native_prefix,
        "produce_activated_native_prefix",
        lambda **_kwargs: SimpleNamespace(
            source_state=runner.native_prefix.TECHNICAL_FAILURE,
            native_prefix={"source_reason": "BROKER_OR_ARTIFACT_FAILURE"},
        ),
    )
    monkeypatch.setattr(
        runner.current_refit,
        "build_current_refit_search_producer",
        lambda inputs: build_calls.append(inputs),
    )
    producer = runner._LazyCurrentRefitProducer(  # noqa: SLF001
        context=SimpleNamespace(),
        active_config_path=tmp_path / "config.json",
        config_payload={},
        data_dir=tmp_path,
        cutoff_utc="2026-01-01T00:00:00Z",
        current_selection_window={
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
        },
        search_process=SimpleNamespace(),
        pipeline_config=PipelineConfig(),
        output_root=tmp_path,
    )

    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match=r"CURRENT_NATIVE_PREFIX_TECHNICAL_FAILURE:AUDUSD\|M5",
    ):
        producer(coordinator.CurrentRefitRequest(b"{}"))
    assert build_calls == []
    with pytest.raises(
        runner.ProductionOptimizationRunnerError,
        match="CURRENT_REFIT_FACTORY_ALREADY_CONSUMED",
    ):
        producer(coordinator.CurrentRefitRequest(b"{}"))


def test_lazy_current_binds_the_frozen_anchor_plan_current_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = []
    monkeypatch.setattr(runner.book, "PINNED_SYMBOL_ORDER", ("EURUSD",))
    monkeypatch.setattr(runner.book, "TIMEFRAME_ORDER", ("H1",))
    monkeypatch.setattr(
        runner.native_prefix,
        "produce_activated_native_prefix",
        lambda **_kwargs: SimpleNamespace(
            source_state=runner.native_prefix.AVAILABLE,
            native_prefix={},
        ),
    )

    def build(inputs):
        captured.append(inputs)
        return lambda _request: "current-product"

    monkeypatch.setattr(
        runner.current_refit,
        "build_current_refit_search_producer",
        build,
    )
    window = {"start_date": "2025-01-01", "end_date": "2026-01-01"}
    producer = runner._LazyCurrentRefitProducer(  # noqa: SLF001
        context=SimpleNamespace(),
        active_config_path=tmp_path / "config.json",
        config_payload={},
        data_dir=tmp_path,
        cutoff_utc="2026-01-01T00:00:00Z",
        current_selection_window=window,
        search_process=SimpleNamespace(),
        pipeline_config=PipelineConfig(),
        output_root=tmp_path,
    )
    window["start_date"] = "2025-12-31"

    assert producer(coordinator.CurrentRefitRequest(b"{}")) == "current-product"
    assert captured[0].current_selection_window == {
        "start_date": "2025-01-01",
        "end_date": "2026-01-01",
    }
