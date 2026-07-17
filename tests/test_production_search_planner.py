from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authority_spool as authority_spool
import pm_causal_evidence_producer as causal_producer
import pm_causal_process as causal
import pm_historical_anchor_orchestration as historical
import pm_pipeline as pipeline
import pm_production_search_planner as planner
from pm_current_refit_parameters import parameter_values_equal
from pm_strategies import StrategyRegistry


def _process_body() -> dict:
    domains = planner._strategy_domains()
    return {"semantics": planner._process_semantics(domains)}


def test_process_freeze_reopens_exact_100_strategy_domain(tmp_path) -> None:
    context = authority_spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="planner-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )

    frozen = planner.freeze_production_search_process(
        context,
        run_id="planner-run",
    )

    assert planner.verify_frozen_production_search_process(context, frozen) is True
    body = frozen.to_dict()
    assert body["candidate_domain_sha256"] == (
        "d4d8ae2deee050989bdc8e93b21d779c8d68ddb4666668a9c34c5d65177803e3"
    )
    assert body["semantics"]["strategy_domain_count"] == 100
    assert body["semantics"]["attempt_policy"][
        "attempt_count_per_observed_cell"
    ] == 128
    assert body["semantics"]["all_100_strategies_represented"] is True


def test_pre_evidence_and_post_precommit_requests_build_identical_anchor_body(
    tmp_path,
) -> None:
    context = authority_spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="planner-pre-evidence-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    process = planner.freeze_production_search_process(
        context,
        run_id="planner-pre-evidence-run",
    )
    anchors = []
    cutoff = date(2015, 1, 1)
    for index in range(4):
        start = cutoff + timedelta(days=35)
        end = start + timedelta(days=364)
        anchors.append(
            {
                "anchor_id": f"anchor-{index + 1:02d}",
                "selection_cutoff_utc": f"{cutoff.isoformat()}T00:00:00Z",
                "forward_start_date": start.isoformat(),
                "forward_end_date": end.isoformat(),
                "purge_calendar_days": 35,
            }
        )
        cutoff = end + timedelta(days=36)
    causal_precommit = causal_producer.freeze_causal_process_precommit_v8(
        context=context,
        search_process=process,
        process_id="planner-pre-evidence-process",
        anchors=anchors,
    )
    causal_payload = causal_precommit.to_dict()
    process_precommit_sha256 = causal.canonical_sha256(causal_payload)
    sinks = {
        symbol: authority_spool.open_symbol_sink(context, symbol)
        for symbol in planner.book.PINNED_SYMBOL_ORDER
    }
    release_sha256 = (
        planner.historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
    )
    planning_request = historical.issue_historical_pre_evidence_planning_request(
        context,
        run_id="planner-pre-evidence-run",
        process_precommit_receipt_sha256=process_precommit_sha256,
        optimizer_producer_release_sha256=release_sha256,
        anchor=anchors[0],
        symbol_sinks=sinks,
    )
    committed_anchors = [
        {
            **anchor,
            "horizon_binding_count": planner.book.FAMILY_SIZE,
            "horizon_binding_roster_sha256": f"{index + 1:064x}",
        }
        for index, anchor in enumerate(anchors)
    ]
    historical_precommit = historical.precommit_historical_anchor_run(
        context,
        run_id="planner-pre-evidence-run",
        process_precommit_receipt_sha256=process_precommit_sha256,
        optimizer_producer_release_sha256=release_sha256,
        anchors=committed_anchors,
    )
    production_request = historical._producer_request(  # noqa: SLF001
        historical_precommit,
        historical_precommit.to_dict(),
        committed_anchors[0],
        sinks,
    )

    planning_body = planner._validate_request(  # noqa: SLF001
        planning_request,
        process=process,
        context=context,
        causal_precommit_payload=causal_payload,
    )
    production_body = planner._validate_request(  # noqa: SLF001
        production_request,
        process=process,
        context=context,
        causal_precommit_payload=causal_payload,
    )
    evidence_accessed_at_utc = causal_payload["evidence_access_deadline_utc"]

    assert planner._anchor_body(  # noqa: SLF001
        planning_body,
        ordinal=0,
        evidence_accessed_at_utc=evidence_accessed_at_utc,
    ) == planner._anchor_body(  # noqa: SLF001
        production_body,
        ordinal=0,
        evidence_accessed_at_utc=evidence_accessed_at_utc,
    )

    tampered = planning_request.to_dict()
    tampered["return_or_winner_evidence_access_authorized"] = True
    with pytest.raises(
        planner.ProductionSearchPlannerError,
        match="ANCHOR_PRE_EVIDENCE_PLANNING_BINDING_INVALID",
    ):
        planner._validate_request(  # noqa: SLF001
            historical.AnchorProducerRequest(
                planner._canonical_bytes(tampered),  # noqa: SLF001
                sinks,
            ),
            process=process,
            context=context,
            causal_precommit_payload=causal_payload,
        )


def test_attempt_roster_contains_every_default_and_28_real_grid_variants() -> None:
    process = _process_body()
    attempts = planner._attempt_designs(
        process,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
    )

    defaults = attempts[: planner.BASELINE_ATTEMPTS]
    variants = attempts[planner.BASELINE_ATTEMPTS :]
    assert [item["strategy_id"] for item in defaults] == StrategyRegistry.list_all()
    assert len(variants) == planner.VARIANT_ATTEMPTS == 28
    assert len({item["strategy_id"] for item in variants}) == 28
    for item in variants:
        strategy = StrategyRegistry.get(item["strategy_id"])
        parameters = item["parameters"]
        changed = False
        for axis, choices in strategy.get_param_grid().items():
            assert any(parameter_values_equal(parameters[axis], value) for value in choices)
            changed = changed or not parameter_values_equal(
                parameters[axis], strategy.get_default_params()[axis]
            )
        assert changed is True


def test_variant_allocation_rotates_by_cell_without_outcome_input() -> None:
    process = _process_body()
    first = planner._attempt_designs(
        process,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
    )
    repeated = planner._attempt_designs(
        process,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
    )
    other_cell = planner._attempt_designs(
        process,
        symbol="GBPUSD",
        timeframe="H1",
        regime="TREND",
    )

    assert first == repeated
    first_variants = [item["strategy_id"] for item in first[100:]]
    other_variants = [item["strategy_id"] for item in other_cell[100:]]
    assert first_variants != other_variants


def test_dependency_purge_includes_execution_delay_and_holding_horizon() -> None:
    payload = planner._dependency_payload(
        "H1",
        feature_lookback_bars=200,
        regime_warmup_bars=150,
    )
    geometry = planner._derived_dependency_geometry("H1", payload)
    holding_days = causal.pinned_process_policy()[
        "maximum_holding_calendar_days_by_timeframe"
    ]["H1"]

    assert payload["execution_delay_bars"] == 1
    assert geometry["left_purge_seconds"] == holding_days * 86_400 + 3_600
    assert geometry["right_purge_seconds"] >= geometry["left_purge_seconds"]


def test_largest_feasible_calendar_depth_is_selected_from_frozen_ladder() -> None:
    opens = pd.date_range("2010-01-01T00:00:00Z", periods=3_000, freq="D")
    closes = opens + pd.Timedelta(days=1)
    native = {
        "prefix_start_utc": planner._utc_text(opens[0]),
        "prefix_end_exclusive_utc": planner._utc_text(closes[-1]),
        "prefix_content_sha256": "c" * 64,
    }
    payload = planner._dependency_payload(
        "D1",
        feature_lookback_bars=10,
        regime_warmup_bars=10,
    )
    geometry = planner._derived_dependency_geometry("D1", payload)

    depth, candidate_start, folds, boundary_limited = planner._build_causal_folds(
        frame=pd.DataFrame(index=opens),
        native=native,
        opens=opens,
        closes=closes,
        timeframe="D1",
        geometry=geometry,
        state_context_bars=10,
    )

    assert depth == planner.FIT_CALENDAR_DAY_LADDERS["D1"][-1]
    assert boundary_limited is False
    assert len(folds) == planner.CAUSAL_FOLD_COUNT == 4
    assert candidate_start == pd.Timestamp(folds[0]["fit_start_utc"])
    for prior, later in zip(folds, folds[1:]):
        assert pd.Timestamp(prior["validation_end_exclusive_utc"]) <= pd.Timestamp(
            later["validation_start_utc"]
        )


def test_source_boundary_shortfall_never_fabricates_folds_or_market_verdict() -> None:
    opens = pd.date_range("2024-01-01T00:00:00Z", periods=120, freq="D")
    closes = opens + pd.Timedelta(days=1)
    native = {
        "prefix_start_utc": planner._utc_text(opens[0]),
        "prefix_end_exclusive_utc": planner._utc_text(closes[-1]),
        "prefix_content_sha256": "c" * 64,
    }
    payload = planner._dependency_payload(
        "D1",
        feature_lookback_bars=10,
        regime_warmup_bars=10,
    )

    depth, candidate_start, folds, boundary_limited = planner._build_causal_folds(
        frame=pd.DataFrame(index=opens),
        native=native,
        opens=opens,
        closes=closes,
        timeframe="D1",
        geometry=planner._derived_dependency_geometry("D1", payload),
        state_context_bars=10,
    )

    assert depth == planner.FIT_CALENDAR_DAY_LADDERS["D1"][0]
    assert boundary_limited is True
    assert folds == []
    assert candidate_start == opens[0] - timedelta(days=1)


def test_current_refit_window_replays_depth_at_cutoff_not_discovery_span() -> None:
    opens = pd.date_range("2015-01-01T00:00:00Z", periods=3_000, freq="D")
    closes = opens + pd.Timedelta(days=1)
    fit_end = closes[-1]
    discovery_start = opens[250]
    depth_days = 365

    fit_start, source_count = planner._cutoff_anchored_refit_window(
        opens=opens,
        closes=closes,
        fit_end=fit_end,
        depth_days=depth_days,
        state_context_bars=10,
    )

    target = fit_end - pd.Timedelta(days=depth_days)
    assert fit_start == opens[opens.get_indexer([target], method="bfill")[0]]
    assert fit_start > discovery_start
    assert fit_end - fit_start <= pd.Timedelta(days=depth_days)
    assert source_count == int(
        ((opens >= fit_start) & (opens < fit_end) & (closes <= fit_end)).sum()
    )


def test_current_source_is_prebound_to_strict_raw_executor_before_attempt_identity(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = authority_spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="current-source-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    sink = authority_spool.open_symbol_sink(context, "EURUSD")
    opens = pd.date_range("2025-01-01T00:00:00Z", periods=500, freq="h")
    closes = opens + pd.Timedelta(hours=1)
    frame = pd.DataFrame(
        {
            "Open": 1.1000,
            "High": 1.1010,
            "Low": 1.0990,
            "Close": 1.1005,
            "Volume": 100.0,
            "_BAR_CLOSE_TS": closes,
        },
        index=opens,
    )
    native = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "activation_id": "activation-current-source",
        "activation_manifest_sha256": "1" * 64,
        "artifact_sha256": "2" * 64,
        "continuity_receipt_sha256": "3" * 64,
        "prefix_content_sha256": "4" * 64,
    }
    source_decision = SimpleNamespace(
        to_dict=lambda: {"source_contract_sha256": "5" * 64}
    )
    fitted = SimpleNamespace(
        source_group_decision=source_decision,
        product=SimpleNamespace(),
    )
    monkeypatch.setattr(
        planner.native_prefix,
        "open_verified_prefix_frame",
        lambda _product: frame.copy(deep=True),
    )
    fit_start = opens[25]
    fit_end = closes[-1]
    source_count = int(
        ((opens >= fit_start) & (opens < fit_end) & (closes <= fit_end)).sum()
    )

    capture = planner._write_current_refit_source_capture(  # noqa: SLF001
        context,
        sink=sink,
        namespace="current-refit/run/20250721T200000Z",
        fitted=fitted,
        frame=frame,
        native=native,
        opens=opens,
        closes=closes,
        current_fit_start=fit_start,
        fit_end=fit_end,
        expected_source_count=source_count,
    )
    source = capture["source_snapshot"]
    bars_descriptor = capture["bars_raw_descriptor"]
    source_descriptor = capture["source_raw_descriptor"]
    pipeline_bars_descriptor = {
        name: value
        for name, value in bars_descriptor.items()
        if name != "semantic_sha256"
    }
    request = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "fit_start_utc": source["fit_start_utc"],
        "fit_end_utc": source["fit_end_utc"],
        "latest_closed_bar_utc": source["latest_closed_bar_utc"],
    }
    source_sha256 = pipeline._current_refit_validate_source(  # noqa: SLF001
        source,
        request=request,
        root=context.root,
        descriptors={bars_descriptor["artifact_id"]: pipeline_bars_descriptor},
    )
    horizon, _ = planner.current_refit.build_horizon_process_artifact_v2(
        symbol="EURUSD",
        timeframe="H1",
        lookback_kind="CALENDAR_INTERVAL",
        lookback_value=365,
        fit_start_utc=source["fit_start_utc"],
        fit_end_utc=source["fit_end_utc"],
        latest_closed_bar_utc=source["latest_closed_bar_utc"],
        source_snapshot_sha256=source_sha256,
        source_bar_count=source_count,
    )
    verified_horizon, _ = pipeline._current_refit_validate_horizon(  # noqa: SLF001
        horizon,
        request=request,
        source_sha256=source_sha256,
        source_bar_count=source_count,
    )

    assert source_sha256 == planner.current_refit.canonical_artifact_sha256(source)
    assert source_sha256 == source_descriptor["semantic_sha256"]
    assert source_sha256 != native["prefix_content_sha256"]
    assert source["first_bar_utc"] == closes[25].isoformat()
    assert source["last_bar_utc"] == closes[-1].isoformat()
    selection = capture["selection_prefix_receipt"]
    assert selection["fit_start_utc"] == opens[25].isoformat()
    assert selection["fit_start_utc"] < source["fit_start_utc"]
    assert selection["fit_end_exclusive_utc"] == source["fit_end_utc"]
    assert selection["latest_closed_bar_utc"] == source["last_bar_utc"]
    assert selection["source_bar_count"] == source["bar_count"] == source_count
    assert source["bars_artifact_id"] == bars_descriptor["artifact_id"]
    bars_path = context.root.joinpath(*Path(bars_descriptor["relative_path"]).parts)
    bars_bytes = bars_path.read_bytes()
    assert len(bars_bytes) == bars_descriptor["size_bytes"]
    assert hashlib.sha256(bars_bytes).hexdigest() == bars_descriptor["content_sha256"]
    assert len(bars_bytes.decode("ascii").splitlines()) - 1 == source_count
    assert "/EURUSD/H1/" in capture["source_spool_entry"]["artifact_id"]
    assert verified_horizon == horizon

    built_once = planner.current_refit.build_current_refit_search_artifacts_v2(
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_id="EMACrossoverStrategy",
        parameters=StrategyRegistry.get("EMACrossoverStrategy").get_default_params(),
        lookback_kind="CALENDAR_INTERVAL",
        lookback_value=365,
        fit_start_utc=source["fit_start_utc"],
        fit_end_utc=source["fit_end_utc"],
        latest_closed_bar_utc=source["latest_closed_bar_utc"],
        source_snapshot_sha256=source_sha256,
        source_bar_count=source_count,
        seed=7,
    )
    built_again = planner.current_refit.build_current_refit_search_artifacts_v2(
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_id="EMACrossoverStrategy",
        parameters=StrategyRegistry.get("EMACrossoverStrategy").get_default_params(),
        lookback_kind="CALENDAR_INTERVAL",
        lookback_value=365,
        fit_start_utc=source["fit_start_utc"],
        fit_end_utc=source["fit_end_utc"],
        latest_closed_bar_utc=source["latest_closed_bar_utc"],
        source_snapshot_sha256=source_sha256,
        source_bar_count=source_count,
        seed=7,
    )
    assert built_once == built_again
    assert built_once.horizon_process["source_snapshot_sha256"] == source_sha256


def _terminal_native_product() -> planner.native_prefix.NativePrefixProduct:
    native = {
        "source_state": planner.native_prefix.SOURCE_UNAVAILABLE,
        "source_reason": "SOURCE_UNAVAILABLE:NATIVE_ACTIVE_CELL_SOURCE_UNAVAILABLE",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "activation_id": "activation-terminal",
        "activation_manifest_sha256": "a" * 64,
        "activation_entry_sha256": "b" * 64,
        "artifact_sha256": None,
        "provenance_sha256": None,
        "request_map_sha256": "c" * 64,
        "source_identity_sha256": "d" * 64,
        "continuity_policy_sha256": "e" * 64,
        "continuity_receipt_sha256": "f" * 64,
        "continuity_geometry_sha256": "1" * 64,
        "source_terminal_receipt_sha256": "2" * 64,
        "source_accessed_at_utc": "2025-01-01T00:00:00Z",
        "request_start_utc": "2024-01-01T00:00:00Z",
        "request_end_exclusive_utc": "2025-01-02T00:00:00Z",
        "prefix_start_utc": None,
        "prefix_end_exclusive_utc": None,
        "prefix_first_open_utc": None,
        "prefix_last_open_utc": None,
        "prefix_last_close_utc": None,
        "artifact_rows": None,
        "prefix_rows": None,
        "prefix_schema_sha256": None,
        "prefix_index_sha256": None,
        "prefix_content_sha256": None,
        "internal_status": "CONTINUOUS",
        "boundary_status": "RIGHT_TRUNCATED",
        "unexplained_discontinuity_count": 0,
        "segments_sha256": "3" * 64,
        "requires_preserved_segment_ledger": False,
    }
    return planner.native_prefix.NativePrefixProduct(
        source_state=planner.native_prefix.SOURCE_UNAVAILABLE,
        index={
            "source_state": planner.native_prefix.SOURCE_UNAVAILABLE,
            "symbol": "EURUSD",
            "timeframe": "H1",
            "cutoff_utc": "2025-01-01T00:00:00Z",
        },
        index_descriptor={
            "format": "canonical_json",
            "relative_path": "sha256/terminal.json",
            "sha256": "4" * 64,
            "size_bytes": 1,
        },
        index_sha256="4" * 64,
        native_prefix=native,
        instrument_snapshot=None,
        prefix_path=None,
        prefix_descriptor=None,
        artifact_descriptors=(),
        _seal=planner.native_prefix._PRODUCT_SEAL,
    )


def _terminal_context(tmp_path: Path):
    return authority_spool.freeze_generation_context(
        (tmp_path / "terminal-reverify-spool").resolve(),
        generation_id="terminal-reverify-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )


def test_terminal_reverify_uses_sealed_product_root_outside_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _terminal_context(tmp_path)
    source_root = (tmp_path / "native-source-root").resolve()
    source_root.mkdir()
    product = replace(_terminal_native_product(), _data_dir=source_root)
    foreign_cwd = tmp_path / "foreign-working-directory"
    foreign_cwd.mkdir()
    observed: dict[str, object] = {}

    def reopen(
        supplied_product,
        supplied_context,
        *,
        require_current_activation=False,
    ):
        observed.update(
            {
                "product": supplied_product,
                "context": supplied_context,
                "require_current_activation": require_current_activation,
                "cwd": Path.cwd(),
            }
        )
        return supplied_product

    monkeypatch.setattr(
        planner.native_prefix,
        "reverify_activated_native_prefix_product",
        reopen,
    )
    monkeypatch.chdir(foreign_cwd)

    reopened = planner._reverify_terminal_product(
        context,
        product=product,
        anchor={
            "selection_cutoff_utc": "2025-01-01T00:00:00Z",
            "evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
        },
    )

    assert reopened is product
    assert observed == {
        "product": product,
        "context": context,
        "require_current_activation": False,
        "cwd": foreign_cwd.resolve(),
    }


def test_terminal_reverify_rejects_product_without_sealed_native_root(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        planner.ProductionSearchPlannerError,
        match="TERMINAL_NATIVE_PRODUCT_REVERIFY_FAILED",
    ):
        planner._reverify_terminal_product(
            _terminal_context(tmp_path),
            product=_terminal_native_product(),
            anchor={
                "selection_cutoff_utc": "2025-01-01T00:00:00Z",
                "evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
            },
        )


def test_terminal_plan_is_zero_attempt_and_not_a_market_verdict(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = authority_spool.freeze_generation_context(
        (tmp_path / "terminal-spool").resolve(),
        generation_id="terminal-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    process = planner.FrozenProductionSearchProcess(
        planner._canonical_bytes({"run_id": "terminal-run"}),
        planner._canonical_bytes({"unused": True}),
        planner._PROCESS_SEAL,
    )
    product = _terminal_native_product()
    monkeypatch.setattr(
        planner,
        "verify_frozen_production_search_process",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        planner,
        "_reverify_terminal_product",
        lambda _context, *, product, anchor: product,
    )
    sink = authority_spool.open_symbol_sink(context, "EURUSD")
    anchor = {
        "anchor_id": "ANCHOR-01",
        "ordinal": 0,
        "selection_cutoff_utc": "2025-01-01T00:00:00Z",
        "forward_start_utc": "2025-02-05T00:00:00Z",
        "forward_end_exclusive_utc": "2026-02-05T00:00:00Z",
        "evidence_accessed_at_utc": "2026-02-05T00:00:00Z",
        "process_precommit_receipt_sha256": "f" * 64,
    }

    plan = planner._build_terminal_cell_plan(
        context,
        process=process,
        product=product,
        regime="TREND",
        anchor=anchor,
        run_id="terminal-run",
        causal_precommit_receipt_sha256="f" * 64,
        namespace="historical-anchor/terminal-run/ANCHOR-01",
        sink=sink,
    )

    assert plan.mode == "TERMINAL"
    assert plan.attempt_count == 0
    assert plan.horizon_plan.to_dict()["plan_status"] == (
        "HISTORICAL_SOURCE_UNAVAILABLE"
    )
    assert plan.cell_receipt["terminal_source_is_market_no_trade_verdict"] is False
    assert plan.cell_receipt["technical_failure_requires_generation_abort"] is False
    assert planner.verify_production_cell_search_plan(
        context, process=process, sink=sink, plan=plan
    )


def _unobserved_fitted_timeframe():
    native = {
        "source_state": planner.native_prefix.AVAILABLE,
        "source_reason": None,
        "symbol": "EURUSD",
        "timeframe": "H1",
        "activation_id": "activation-unobserved",
        "activation_manifest_sha256": "a" * 64,
        "activation_entry_sha256": "b" * 64,
        "artifact_sha256": "c" * 64,
        "provenance_sha256": "d" * 64,
        "request_map_sha256": "e" * 64,
        "source_identity_sha256": "f" * 64,
        "continuity_policy_sha256": "1" * 64,
        "continuity_receipt_sha256": "2" * 64,
        "continuity_geometry_sha256": "3" * 64,
        "source_terminal_receipt_sha256": None,
        "source_accessed_at_utc": "2025-01-01T00:00:00Z",
        "request_start_utc": "2024-12-01T00:00:00Z",
        "request_end_exclusive_utc": "2025-01-01T00:00:00Z",
        "prefix_start_utc": "2024-12-01T00:00:00Z",
        "prefix_end_exclusive_utc": "2025-01-01T00:00:00Z",
        "prefix_first_open_utc": "2024-12-01T00:00:00Z",
        "prefix_last_open_utc": "2024-12-31T23:00:00Z",
        "prefix_last_close_utc": "2025-01-01T00:00:00Z",
        "artifact_rows": 744,
        "prefix_rows": 744,
        "prefix_schema_sha256": "4" * 64,
        "prefix_index_sha256": "5" * 64,
        "prefix_content_sha256": "6" * 64,
        "internal_status": "CONTINUOUS",
        "boundary_status": "LEFT_TRUNCATED",
        "unexplained_discontinuity_count": 0,
        "segments_sha256": "7" * 64,
        "requires_preserved_segment_ledger": True,
    }
    product = planner.native_prefix.NativePrefixProduct(
        source_state=planner.native_prefix.AVAILABLE,
        index={},
        index_descriptor={
            "format": "canonical_json",
            "relative_path": "sha256/available.json",
            "sha256": "5" * 64,
            "size_bytes": 1,
        },
        index_sha256="5" * 64,
        native_prefix=native,
        instrument_snapshot={},
        prefix_path=None,
        prefix_descriptor=None,
        artifact_descriptors=(),
        _seal=planner.native_prefix._PRODUCT_SEAL,
    )
    counts = {"TREND": 0, "RANGE": 300, "BREAKOUT": 200, "CHOP": 200}
    support_body = {
        "schema_version": "fxpm.regime-train-support/v1",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "surface": "REGIME_LIVE",
        "warmup_policy": "CURRENT_AND_SOURCE_BAR_NOT_WARMUP",
        "label_order": list(planner.REGIMES),
        "eligible_rows": 700,
        "counts": counts,
        "observed_regimes": ["RANGE", "BREAKOUT", "CHOP"],
        "unobserved_regimes": ["TREND"],
        "support_state_by_regime": {
            "TREND": "UNOBSERVED",
            "RANGE": "EMPIRICALLY_OBSERVED",
            "BREAKOUT": "EMPIRICALLY_OBSERVED",
            "CHOP": "EMPIRICALLY_OBSERVED",
        },
        "first_ts": "2024-12-01T01:00:00Z",
        "last_ts": "2024-12-31T22:00:00Z",
        "vector_encoding": "fxpm.regime-live-labels-nul/v1",
        "vector_sha256": "8" * 64,
        "bindings": {
            "symbol": "EURUSD",
            "timeframe": "H1",
            "activation_id": "activation-unobserved",
            "activation_manifest_sha256": "a" * 64,
            "source_sha256": "c" * 64,
            "train_window_sha256": "9" * 64,
            "params_sha256": "0" * 64,
            "detector_code_sha256": "1" * 64,
        },
    }
    support = {**support_body, "receipt_sha256": planner._sha(support_body)}
    source = SimpleNamespace(
        receipt_sha256="1" * 64,
        to_dict=lambda: {"availability": planner.book.AVAILABLE},
    )
    fit = SimpleNamespace(
        receipt_sha256="2" * 64,
        to_dict=lambda: {
            "fit_state": planner.book.REGIME_FITTED,
            "source_group_decision_sha256": "1" * 64,
            "regime_detector_record_sha256": "3" * 64,
            "train_regime_support": support,
        },
    )
    fitted = planner.search_execution.FittedNativeSearchTimeframe(
        product=product,
        source_group_decision=source,
        regime_fit_decision=fit,
        source_contract_seed={},
        base_cost_policy={"lane": "BASE"},
        stress_cost_policy={"lane": "STRESS"},
        regime_parameters={},
        train_regime_support=support,
        instrument_spec=None,
        execution_spec_receipt={},
        frame_sha256="f" * 64,
        _frame=pd.DataFrame(),
    )
    opens = pd.date_range("2024-12-01T00:00:00Z", periods=744, freq="h")
    closes = opens + pd.Timedelta(hours=1)
    return fitted, native, opens, closes


def test_unobserved_plan_binds_exact_support_and_has_zero_attempts(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = authority_spool.freeze_generation_context(
        (tmp_path / "unobserved-spool").resolve(),
        generation_id="unobserved-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    process = planner.FrozenProductionSearchProcess(
        planner._canonical_bytes({"run_id": "unobserved-run"}),
        planner._canonical_bytes({"unused": True}),
        planner._PROCESS_SEAL,
    )
    fitted, native, opens, closes = _unobserved_fitted_timeframe()
    monkeypatch.setattr(
        planner,
        "verify_frozen_production_search_process",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        planner,
        "_validated_frame",
        lambda _fitted: (pd.DataFrame(index=opens), native, opens, closes),
    )
    sink = authority_spool.open_symbol_sink(context, "EURUSD")
    anchor = {
        "anchor_id": "ANCHOR-01",
        "ordinal": 0,
        "selection_cutoff_utc": "2025-01-01T00:00:00Z",
        "forward_start_utc": "2025-02-05T00:00:00Z",
        "forward_end_exclusive_utc": "2026-02-05T00:00:00Z",
        "evidence_accessed_at_utc": "2026-02-05T00:00:00Z",
        "process_precommit_receipt_sha256": "f" * 64,
    }

    plan = planner._build_unobserved_cell_plan(
        context,
        process=process,
        fitted=fitted,
        regime="TREND",
        anchor=anchor,
        run_id="unobserved-run",
        causal_precommit_receipt_sha256="f" * 64,
        mode="HISTORICAL_REGIME_UNOBSERVED",
        namespace="historical-anchor/unobserved-run/ANCHOR-01",
        sink=sink,
    )

    assert plan.attempt_count == 0
    assert plan.horizon_plan.to_dict()["plan_status"] == (
        "HISTORICAL_REGIME_UNOBSERVED"
    )
    assert plan.cell_receipt["regime_support_binding"]["target_regime_count"] == 0
    assert plan.cell_receipt["empirically_unobserved_is_no_trade_verdict"] is False
    assert planner.verify_production_cell_search_plan(
        context, process=process, sink=sink, plan=plan
    )


def test_cell_plan_reopen_replays_carried_rosters_and_rejects_splice(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = authority_spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="cell-plan-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    process = planner.FrozenProductionSearchProcess(
        planner._canonical_bytes({"run_id": "cell-plan-run"}),
        planner._canonical_bytes({"unused": True}),
        planner._PROCESS_SEAL,
    )
    monkeypatch.setattr(
        planner,
        "verify_frozen_production_search_process",
        lambda *_args, **_kwargs: True,
    )
    binding = {"schema_version": "test-horizon-binding", "receipt": "h" * 64}
    monkeypatch.setattr(
        planner.historical,
        "historical_horizon_plan_binding",
        lambda _plan: binding,
    )
    native = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "prefix_content_sha256": "c" * 64,
        "prefix_start_utc": "2020-01-01T00:00:00Z",
        "prefix_end_exclusive_utc": "2021-01-01T00:00:00Z",
        "prefix_last_close_utc": "2020-12-31T23:00:00Z",
        "prefix_rows": 8_784,
    }
    fitted = SimpleNamespace(
        product=SimpleNamespace(native_prefix=native, index_sha256="i" * 64),
        source_group_decision=SimpleNamespace(receipt_sha256="s" * 64),
        regime_fit_decision=SimpleNamespace(receipt_sha256="r" * 64),
        train_regime_support={"receipt_sha256": "t" * 64},
    )
    attempts = [{"ordinal": value} for value in range(planner.ATTEMPT_COUNT)]
    states = []
    for value in range(planner.ATTEMPT_COUNT):
        body = {"ordinal": value}
        states.append({**body, "requirement_sha256": planner._sha(body)})
    sink = authority_spool.open_symbol_sink(context, "EURUSD")
    receipt_json, entry_json = planner._seal_cell_plan(
        context,
        sink=sink,
        mode="HISTORICAL_ANCHOR",
        namespace="historical-anchor/run/anchor-01",
        process=process,
        causal_precommit_receipt_sha256="f" * 64,
        fitted=fitted,
        regime="TREND",
        horizon_plan=object(),
        candidate={"candidate_id": "candidate"},
        dependency_payload={"payload": True},
        dependency_geometry={"geometry": True},
        attempt_artifacts=attempts,
        state_requirements=states,
        boundary_limited=False,
    )
    plan = planner.ProductionCellSearchPlan(
        mode="HISTORICAL_ANCHOR",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        causal_precommit_receipt_sha256="f" * 64,
        search_process_receipt_sha256=process.receipt_sha256,
        horizon_plan=object(),
        _attempt_artifacts_json=tuple(
            planner._canonical_bytes(value) for value in attempts
        ),
        _state_requirements_json=tuple(
            planner._canonical_bytes(value) for value in states
        ),
        _cell_receipt_json=receipt_json,
        _spool_entry_json=entry_json,
        boundary_limited=False,
        _seal=planner._CELL_PLAN_SEAL,
    )

    assert planner.verify_production_cell_search_plan(
        context,
        process=process,
        sink=sink,
        plan=plan,
    ) is True

    changed = list(plan._attempt_artifacts_json)
    changed[0] = planner._canonical_bytes({"ordinal": 999})
    with pytest.raises(
        planner.ProductionSearchPlannerError,
        match="CELL_PLAN_ROSTER_REPLAY_INVALID",
    ):
        planner.verify_production_cell_search_plan(
            context,
            process=process,
            sink=sink,
            plan=replace(plan, _attempt_artifacts_json=tuple(changed)),
        )
