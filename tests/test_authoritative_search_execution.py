from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_production_search_planner as search_planner
import pm_winner_selection as winner


def _d1_frame(periods: int) -> pd.DataFrame:
    index = pd.date_range("2022-01-24", periods=periods, freq="D", tz="UTC")
    close = pd.Series(
        [1.0 + ordinal / 1000 for ordinal in range(periods)],
        index=index,
    )
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 0.01,
            "Low": close - 0.01,
            "Close": close,
        },
        index=index,
    )


def _prefix_product(frame: pd.DataFrame) -> SimpleNamespace:
    return SimpleNamespace(
        native_prefix={
            "activation_id": "activation-test",
            "activation_manifest_sha256": "a" * 64,
            "artifact_sha256": "b" * 64,
            "prefix_content_sha256": "c" * 64,
            "prefix_start_utc": frame.index[0].isoformat(),
            "prefix_end_exclusive_utc": (
                frame.index[-1] + pd.Timedelta(days=1)
            ).isoformat(),
            "prefix_rows": len(frame),
        }
    )


def test_all_warmup_prefix_builds_authenticated_zero_regime_support() -> None:
    frame = _d1_frame(47)
    detector = execution.pm_regime.MarketRegimeDetector(
        execution.pm_regime.DEFAULT_PARAMS_BY_TIMEFRAME["D1"]
    )
    surface = detector.compute_regime_scores(frame)
    assert surface["REGIME_WARMUP"].all()

    product = _prefix_product(frame)
    support = execution._build_train_regime_support(
        frame=frame,
        surface=surface,
        product=product,
        symbol="XAUGBP",
        timeframe="D1",
        parameters_sha256="d" * 64,
        detector_code_sha256="e" * 64,
    )

    assert support["eligible_rows"] == 0
    assert support["counts"] == {regime: 0 for regime in execution.book.REGIME_ORDER}
    assert support["observed_regimes"] == []
    assert support["unobserved_regimes"] == list(execution.book.REGIME_ORDER)
    assert support["first_ts"] is None
    assert support["last_ts"] is None
    assert support["vector_sha256"] == hashlib.sha256(
        b"fxpm.regime-live-labels-nul/v1\x00"
    ).hexdigest()

    invalid = surface.copy()
    invalid.loc[frame.index[:2], "REGIME_WARMUP"] = False
    invalid.loc[frame.index[1], "REGIME_LIVE"] = None
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="REGIME_SUPPORT_LABEL_MISSING",
    ):
        execution._build_train_regime_support(
            frame=frame,
            surface=invalid,
            product=product,
            symbol="XAUGBP",
            timeframe="D1",
            parameters_sha256="d" * 64,
            detector_code_sha256="e" * 64,
        )


def test_d1_detector_threshold_still_builds_nonempty_regime_support() -> None:
    frame = _d1_frame(110)
    detector = execution.pm_regime.MarketRegimeDetector(
        execution.pm_regime.DEFAULT_PARAMS_BY_TIMEFRAME["D1"]
    )
    surface = detector.compute_regime_scores(frame)
    support = execution._build_train_regime_support(
        frame=frame,
        surface=surface,
        product=_prefix_product(frame),
        symbol="THRESHOLD",
        timeframe="D1",
        parameters_sha256="d" * 64,
        detector_code_sha256="e" * 64,
    )

    assert support["eligible_rows"] == 9
    assert sum(support["counts"].values()) == 9
    assert support["observed_regimes"]
    assert support["first_ts"] is not None
    assert support["last_ts"] is not None


def _verified_attempt_with_folds(
    *, count: int = 4, days: int = 63, overlap_second: bool = False
) -> execution._VerifiedAttempt:
    cursor = pd.Timestamp("2018-01-01T00:00:00Z")
    folds = []
    for ordinal in range(count):
        start = cursor
        if ordinal == 1 and overlap_second:
            start -= pd.Timedelta(days=1)
        end = start + pd.Timedelta(days=days)
        folds.append(
            {
                "role": "CAUSAL_ROLLING_ORIGIN_INNER",
                "validation_start_utc": start.isoformat(),
                "validation_end_exclusive_utc": end.isoformat(),
            }
        )
        cursor = end
    return execution._VerifiedAttempt(
        ordinal=0,
        plan_attempt={"status": "PLANNED"},
        candidate={"folds": folds},
        trial={},
        artifacts=SimpleNamespace(),
    )


def test_content_addressed_attempt_stage_round_trips_and_rejects_tamper(
    tmp_path,
) -> None:
    stage = tmp_path / "stage"
    stage.mkdir()
    body = {
        "attempt_id": "attempt-001",
        "folds": [
            {
                "fold_ordinal": 0,
                "lanes": {"optimizer_train_base": {"position_events": []}},
                "provenance": {"receipt": {"value": "genuine"}},
            }
        ],
    }
    reservation = SimpleNamespace(
        consumed=0,
        materialized=0,
    )
    reservation.consume = lambda count: setattr(
        reservation, "consumed", reservation.consumed + count
    )
    reservation.materialize = lambda count: setattr(
        reservation, "materialized", reservation.materialized + count
    )
    row = execution._write_staged_attempt(
        stage=stage,
        ordinal=0,
        value=body,
        remaining_cell_bytes=1024 * 1024,
        reservation=reservation,
    )
    assert reservation.consumed == row.size_bytes
    assert reservation.materialized == row.size_bytes
    assert row.sha256 in row.path.name
    assert execution._reopen_staged_attempt(row) == body

    with row.path.open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="STAGE_FILE_CHANGED|STAGE_FILE_CONTENT_MISMATCH",
    ):
        execution._reopen_staged_attempt(row)
    execution._cleanup_stage(stage, [row])
    assert not stage.exists()


def test_stage_byte_bound_fails_without_leaving_partial_file(tmp_path) -> None:
    stage = tmp_path / "stage"
    stage.mkdir()
    reservation = SimpleNamespace(
        consume=lambda _count: pytest.fail("oversize write reserved bytes"),
        materialize=lambda _count: pytest.fail("oversize write materialized bytes"),
    )
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="ATTEMPT_EVIDENCE_BYTE_BOUND_EXCEEDED",
    ):
        execution._write_staged_attempt(
            stage=stage,
            ordinal=3,
            value={"attempt_id": "x", "folds": [{"payload": "too-large"}]},
            remaining_cell_bytes=8,
            reservation=reservation,
        )
    assert list(stage.iterdir()) == []
    execution._cleanup_stage(stage, [])


def test_stage_and_publication_share_one_generation_quota(tmp_path) -> None:
    limits = execution.authority_spool.SpoolLimits(
        max_artifact_bytes=4096,
        max_symbol_bytes=4096,
        max_artifacts_per_symbol=10,
        max_manifest_bytes=4096,
        max_json_bytes=4096,
        stream_chunk_bytes=128,
        max_generation_bytes=4096,
        min_free_bytes=0,
    )
    context = execution.authority_spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="stage-quota-test",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
        limits=limits,
    )
    stage = context.root / "stage"
    stage.mkdir()
    scope = execution.authority_spool.reserve_transient_publication(context)
    reservation = scope.__enter__()
    row = execution._write_staged_attempt(
        stage=stage,
        ordinal=0,
        value={"attempt_id": "x", "payload": "a" * 2500},
        remaining_cell_bytes=4096,
        reservation=reservation,
    )
    with pytest.raises(
        execution.authority_spool.AuthoritySpoolError,
        match="SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
    ):
        execution.authority_spool.write_opaque_artifact(
            context,
            [b"b" * 2500],
            format="binary-v1",
        )
    execution._close_reserved_stage(
        stage=stage,
        rows=[row],
        reservation=reservation,
        reservation_scope=scope,
        primary_error=None,
    )
    assert not stage.exists()
    state = execution.json.loads(
        (context.root / execution.authority_spool.spool_quota.QUOTA_STATE_NAME).read_text(
            encoding="ascii"
        )
    )
    assert state["reservations"] == []


def test_shared_fit_adequacy_preserves_train_tuning_methodological_floor() -> None:
    rejected_required = execution.minimum_fold_fit_rows(
        recipe_warmup_bars=50,
        dependency_warmup_bars=943,
    )
    admitted_required = execution.minimum_fold_fit_rows(
        recipe_warmup_bars=50,
        dependency_warmup_bars=545,
    )
    assert rejected_required == 1_890
    assert admitted_required == 1_094
    failed_canary_fit_rows = (1_296, 1_296, 1_298, 1_296)
    assert all(rows < rejected_required for rows in failed_canary_fit_rows)
    assert all(rows >= admitted_required for rows in failed_canary_fit_rows)
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="DEPENDENCY_WARMUP_INVALID",
    ):
        execution.minimum_fold_fit_rows(
            recipe_warmup_bars=50,
            dependency_warmup_bars=-1,
        )


def test_signed_dependency_warmup_controls_every_partition() -> None:
    opens = pd.date_range("2020-01-01T00:00:00Z", periods=30, freq="h")
    frame = pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.1,
            "Low": 0.9,
            "Close": 1.0,
            "_BAR_CLOSE_TS": opens + pd.Timedelta(hours=1),
            "REGIME_LIVE": "TRENDING",
        },
        index=opens,
    )
    attempt = execution._VerifiedAttempt(
        ordinal=0,
        plan_attempt={"status": "PLANNED"},
        candidate={"lookback_value": 20},
        trial={},
        artifacts=SimpleNamespace(
            refit_recipe={"data_requirements": {"warmup_bars": 2}}
        ),
    )
    fold = {
        "fit_start_utc": opens[0].isoformat(),
        "fit_end_exclusive_utc": opens[20].isoformat(),
        "fit_rows": 20,
        "validation_warmup_start_utc": opens[20].isoformat(),
        "validation_start_utc": opens[25].isoformat(),
        "validation_end_exclusive_utc": (opens[-1] + pd.Timedelta(hours=1)).isoformat(),
    }
    train, tuning, final = execution._fold_partitions(
        frame=frame,
        fold=fold,
        attempt=attempt,
        dependency_warmup_bars=5,
    )
    assert (train.warmup_bars, tuning.warmup_bars, final.warmup_bars) == (5, 5, 5)
    assert train.geometry_receipt["warmup_bars"] == 5
    assert final.scored_start_utc == "2020-01-02T01:00:00Z"


def test_path_state_is_computed_once_on_full_prefix_then_sliced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opens = pd.date_range("2020-01-01T00:00:00Z", periods=9, freq="h")
    frame = pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.1,
            "Low": 0.9,
            "Close": 1.0,
            "_BAR_CLOSE_TS": opens + pd.Timedelta(hours=1),
            "REGIME_LIVE": "TREND",
        },
        index=opens,
    )
    calls = {"features": 0, "signals": 0}

    def compute(source, _strategy, **_kwargs):
        calls["features"] += 1
        return source.copy(deep=True)

    class PathStrategy:
        def generate_signals(self, features, _symbol):
            calls["signals"] += 1
            return pd.Series(
                (pd.RangeIndex(len(features)).to_numpy() % 3) - 1,
                index=features.index,
            )

    monkeypatch.setattr(
        execution.FeatureComputer,
        "compute_for_strategy",
        compute,
    )
    surface = execution._build_full_prefix_strategy_surface(
        frame=frame,
        strategy=PathStrategy(),
        symbol="EURUSD",
        timeframe="H1",
    )
    partition_frame = frame.iloc[5:].copy(deep=True)
    partition = execution._PartitionSlice(
        role="final_lockbox",
        frame=partition_frame,
        warmup_bars=1,
        scored_start_utc=opens[6].isoformat(),
        end_exclusive_utc=(opens[-1] + pd.Timedelta(hours=1)).isoformat(),
        geometry_receipt={},
    )
    _, sliced = execution._slice_full_prefix_strategy_surface(
        partition=partition,
        full_prefix_surface=surface,
    )

    assert calls == {"features": 1, "signals": 1}
    assert sliced.tolist() == [1, -1, 0, 1]
    assert sliced.tolist() != [-1, 0, 1, -1]


def test_planned_attempt_reuses_one_backtester_across_base_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[object] = []
    observed: list[object] = []
    replayed: list[object] = []

    class MarkerBacktester:
        def __init__(self, _config):
            created.append(self)

        def replay_frozen_path_cost_stress(self, *_args, **_kwargs):
            replayed.append(self)
            return {"cost_stress_frozen_path_receipt": {"receipt_sha256": "r" * 64}}

    def run_partition(*, partition, backtester, **_kwargs):
        observed.append(backtester)
        frame = pd.DataFrame(index=pd.date_range("2020-01-01", periods=2, tz="UTC"))
        signals = pd.Series([0, 0], index=frame.index, dtype="int32")
        return {"partition": partition.role}, frame, signals, backtester

    monkeypatch.setattr(execution, "Backtester", MarkerBacktester)
    monkeypatch.setattr(execution, "_runtime_strategy", lambda _attempt: (object(), {}))
    monkeypatch.setattr(
        execution,
        "_build_full_prefix_strategy_surface",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(execution, "_causal_folds", lambda _attempt: ({}, {}))

    def partitions(**_kwargs):
        return tuple(
            SimpleNamespace(role=role, warmup_bars=0)
            for role in ("optimizer_train", "tuning_validation", "final_lockbox")
        )

    monkeypatch.setattr(execution, "_fold_partitions", partitions)
    monkeypatch.setattr(execution, "_run_base_partition", run_partition)
    monkeypatch.setattr(
        execution,
        "_expected_cost_stress_adversity_row",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        execution,
        "_verify_realized_cost_stress_adversity",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        execution,
        "_verify_result_cost_attestation",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        execution,
        "_decision_relevant_result",
        lambda _result, role: {"role": role},
    )
    monkeypatch.setattr(execution, "_fold_provenance", lambda **_kwargs: {})

    attempt = execution._VerifiedAttempt(
        ordinal=0,
        plan_attempt={"attempt_id": "attempt-0", "status": "PLANNED"},
        candidate={},
        trial={},
        artifacts=SimpleNamespace(),
    )
    fitted = SimpleNamespace(
        _frame=pd.DataFrame({"Close": [1.0]}),
        instrument_spec=object(),
        source_group_decision=SimpleNamespace(
            to_dict=lambda: {"source_contract_sha256": "s" * 64}
        ),
    )
    result = execution._build_planned_attempt_evidence(
        attempt=attempt,
        raw_attempt_id="raw-0",
        plan_body={
            "cell": {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"},
            "derived_dependency_geometry": {"warmup_bars": 0},
        },
        plan_receipt_sha256="p" * 64,
        fitted=fitted,
        config=_config(),
        cost_stress_adversity_plan={},
    )

    assert result["attempt_id"] == "raw-0"
    assert len(created) == 1
    assert len(observed) == 6
    assert all(value is created[0] for value in observed)
    assert replayed == [created[0], created[0]]


def test_cost_stress_adversity_plan_is_source_frozen_and_partition_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opens = pd.date_range("2020-01-01T00:00:00Z", periods=96, freq="h")
    frame = pd.DataFrame(
        {
            "Open": 1.1,
            "High": 1.2,
            "Low": 1.0,
            "Close": 1.1,
            "Volume": 100,
            "Spread": 8.0,
            "_BAR_CLOSE_TS": opens + pd.Timedelta(hours=1),
            "REGIME_LIVE": "TREND",
        },
        index=opens,
    )
    fold = {
        "role": "CAUSAL_ROLLING_ORIGIN_INNER",
        "fold_id": "horizon-fold",
        "validation_warmup_start_utc": opens[0].isoformat(),
        "validation_start_utc": opens[24].isoformat(),
        "validation_end_exclusive_utc": (opens[-1] + pd.Timedelta(hours=1)).isoformat(),
    }
    attempts = tuple(
        execution._VerifiedAttempt(
            ordinal=ordinal,
            plan_attempt={
                "attempt_id": f"horizon-attempt-{ordinal}",
                "status": "PLANNED",
            },
            candidate={"folds": [fold]},
            trial={},
            artifacts=SimpleNamespace(
                refit_recipe={"data_requirements": {"warmup_bars": 2}}
            ),
        )
        for ordinal in range(2)
    )
    winner_fold = {
        "anchor_as_of_utc": opens[24].isoformat(),
        "oos_start_date_utc": opens[24].date().isoformat(),
        "oos_end_date_utc": opens[-1].date().isoformat(),
    }
    fitted = SimpleNamespace(
        _frame=frame,
        instrument_spec=execution.InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            spread_avg=2.3,
            point=0.00001,
            digits=5,
        ),
    )
    calls: list[str] = []
    original = execution._partition_cost_attestation

    def counted(**kwargs):
        calls.append(str(kwargs["backtester"].config.backtest_spread_mode))
        return original(**kwargs)

    monkeypatch.setattr(execution, "_partition_cost_attestation", counted)
    plan = execution._build_cost_stress_adversity_plan(
        attempts=attempts,
        fold_plan=(winner_fold,),
        plan_body={
            "cell": {
                "symbol": "EURUSD",
                "timeframe": "H1",
                "regime": "TREND",
            },
            "derived_dependency_geometry": {"warmup_bars": 2},
        },
        fitted=fitted,
        config=execution.PipelineConfig(
            use_spread=True,
            swap_model_enabled=False,
            backtest_spread_mode="conservative_max",
            oos_evidence_execution_stress_mode="stress_p95",
        ),
    )

    assert plan["row_count"] == 2
    assert plan["rows_sha256"] == execution._sha256(plan["rows"])
    assert calls == ["conservative_max", "stress_p95"]
    first, second = plan["rows"]
    assert first["attempt_ordinal"] == 0
    assert second["attempt_ordinal"] == 1
    assert (
        first["final_partition_geometry_receipt_sha256"]
        == second["final_partition_geometry_receipt_sha256"]
    )
    assert first["base_spread_values_sha256"] == (first["stress_spread_values_sha256"])
    assert first["expected_adversity_status"] == (
        execution.raw_winner.COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK
    )


def test_cost_policy_from_seed_freezes_only_spread_economics() -> None:
    attestation = execution.trade_power.build_numerical_execution_cost_attestation(
        use_spread=True,
        spread_mode="fixed_spec",
        spread_values_sha256="1" * 64,
        instrument_spec_sha256="2" * 64,
    )

    policy = execution._cost_policy_from_seed(
        {"numerical_execution_cost_attestation": attestation},
        lane="BASE",
    )

    assert policy["lane"] == "BASE"
    assert policy["use_spread"] is True
    assert policy["spread_mode"] == "fixed_spec"
    assert policy["instrument_spec_sha256"] == "2" * 64
    assert not {
        "use_commission",
        "use_slippage",
        "commission_inputs_sha256",
        "slippage_pips_hex",
        "slippage_price_hex",
    }.intersection(policy)


@pytest.mark.parametrize(
    ("expected_status", "realized_status", "accepted"),
    (
        (
            execution.raw_winner.COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK,
            execution.pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT,
            True,
        ),
        (
            execution.raw_winner.COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK,
            execution.pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT,
            False,
        ),
        (
            execution.raw_winner.COST_STRESS_ADVERSITY_EXPECTED_STRICT,
            execution.pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT,
            True,
        ),
        (
            execution.raw_winner.COST_STRESS_ADVERSITY_EXPECTED_STRICT,
            execution.pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT,
            True,
        ),
    ),
    ids=(
        "no-shock-to-insufficient",
        "no-shock-cannot-become-strict",
        "strict-remains-strict",
        "strict-may-realize-insufficient",
    ),
)
def test_realized_cost_stress_adversity_status_matrix(
    expected_status: str,
    realized_status: str,
    accepted: bool,
) -> None:
    attempt = execution._VerifiedAttempt(
        ordinal=7,
        plan_attempt={"attempt_id": "horizon-attempt-7"},
        candidate={},
        trial={},
        artifacts=SimpleNamespace(),
    )
    final = execution._PartitionSlice(
        role="final_lockbox",
        frame=pd.DataFrame(),
        warmup_bars=0,
        scored_start_utc="2025-01-01T00:00:00+00:00",
        end_exclusive_utc="2025-01-02T00:00:00+00:00",
        geometry_receipt={
            "receipt_sha256": "g" * 64,
            "frame_sha256": "f" * 64,
        },
    )
    receipt = {
        "base_cost_attestation_sha256": "b" * 64,
        "stress_cost_attestation_sha256": "s" * 64,
        "base_spread_values_sha256": "v" * 64,
        "stress_spread_values_sha256": "w" * 64,
        "realized_adversity_status": realized_status,
    }
    expected = {
        "attempt_ordinal": attempt.ordinal,
        "horizon_attempt_id": attempt.plan_attempt["attempt_id"],
        "fold_ordinal": 2,
        "final_partition_geometry_receipt_sha256": "g" * 64,
        "final_partition_frame_sha256": "f" * 64,
        **{
            name: receipt[name]
            for name in (
                "base_cost_attestation_sha256",
                "stress_cost_attestation_sha256",
                "base_spread_values_sha256",
                "stress_spread_values_sha256",
            )
        },
        "expected_adversity_status": expected_status,
    }

    if accepted:
        execution._verify_realized_cost_stress_adversity(
            expected=expected,
            attempt=attempt,
            fold_ordinal=2,
            final=final,
            receipt=receipt,
        )
    else:
        with pytest.raises(
            execution.AuthoritativeSearchExecutionError,
            match="FROZEN_COST_STRESS_ADVERSITY_PLAN_MISMATCH",
        ):
            execution._verify_realized_cost_stress_adversity(
                expected=expected,
                attempt=attempt,
                fold_ordinal=2,
                final=final,
                receipt=receipt,
            )


def test_process_rejection_is_typed_nontechnical_abstention() -> None:
    attempt = execution._VerifiedAttempt(
        ordinal=4,
        plan_attempt={
            "attempt_id": "horizon-attempt",
            "status": "PROCESS_REJECTED",
            "reason_code": "SOURCE_BOUNDARY_SHORTFALL",
        },
        candidate={"candidate_id": "candidate"},
        trial={"trial_id": "trial", "parameter_set_sha256": "a" * 64},
        artifacts=SimpleNamespace(winner_process={"strategy_family": "EMA_CROSSOVER"}),
    )
    outcome = execution._process_rejection_outcome(
        attempt=attempt,
        symbol="EURUSD",
        timeframe="H1",
        regime="TRENDING",
    )
    assert outcome.kind is execution.SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert outcome.reason_code == "SOURCE_BOUNDARY_SHORTFALL"
    assert outcome.evidence["decision_was_pre_search"] is True


def test_state_context_rejection_evidence_carries_exact_frozen_bounds() -> None:
    disposition_body = {
        "schema_version": "fxpm.production-trial-state-context-disposition.v1",
        "candidate_id": "candidate",
        "candidate_definition_sha256": "b" * 64,
        "state_requirement_sha256": "c" * 64,
        "required_context_bars": 3_882,
        "selected_context_bars": 943,
        "status": "PROCESS_REJECTED",
        "reason_code": "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL",
    }
    disposition = {
        **disposition_body,
        "disposition_sha256": execution._sha256(disposition_body),
    }
    attempt = execution._VerifiedAttempt(
        ordinal=44,
        plan_attempt={
            "attempt_id": "horizon-attempt",
            "status": "PROCESS_REJECTED",
            "reason_code": "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL",
        },
        candidate={"candidate_id": "candidate"},
        trial={
            "trial_id": "trial",
            "parameter_set_sha256": "a" * 64,
            "state_context_disposition": disposition,
        },
        artifacts=SimpleNamespace(winner_process={"strategy_family": "KAMA_TREND"}),
    )
    outcome = execution._process_rejection_outcome(
        attempt=attempt,
        symbol="EURUSD",
        timeframe="D1",
        regime="TREND",
    )

    assert outcome.kind is execution.SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert outcome.reason_code == "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
    assert outcome.evidence["required_context_bars"] == 3_882
    assert outcome.evidence["selected_context_bars"] == 943
    assert outcome.evidence["state_requirement_sha256"] == "c" * 64


def test_canary_failure_diagnostic_preserves_exact_stress_tuple() -> None:
    exc = execution.AuthoritativeSearchExecutionError(
        "FROZEN_PATH_COST_STRESS_FAILED",
        (
            "0",
            "0",
            "FrozenPathCostStressError",
            "STRESS_NOT_NUMERICALLY_ADVERSE:stress_p95:genuine evidence required",
        ),
    )

    diagnostic = execution._bounded_canary_failure_diagnostic(
        exc,
        attempted_trials_exact=128,
    )

    assert diagnostic == {
        "exception_type": "FrozenPathCostStressError",
        "failure_details": (
            "0",
            "0",
            "FrozenPathCostStressError",
            "STRESS_NOT_NUMERICALLY_ADVERSE:stress_p95:genuine evidence required",
        ),
        "attempt_ordinal": 0,
        "fold_ordinal": 0,
    }


def test_winner_evidence_roles_are_derived_from_sealed_horizon_status() -> None:
    planned = _verified_attempt_with_folds()
    rejected = execution._VerifiedAttempt(
        ordinal=1,
        plan_attempt={"status": "PROCESS_REJECTED"},
        candidate={},
        trial={},
        artifacts=SimpleNamespace(),
    )
    assert execution._precommitted_evidence_roles((planned, rejected)) == (
        "EVALUATION_REQUIRED",
        "PRESEARCH_PROCESS_REJECTED",
    )


def test_release_contract_binds_all_execution_dependencies() -> None:
    contract = execution.authoritative_search_execution_contract()
    assert contract["schema_version"] == "fxpm.authoritative-search-execution.v3"
    assert contract["position_event_source"].endswith("position-events-v2")
    assert set(contract["dependency_source_sha256"]) == {
        "pm_authoritative_search_execution",
        "pm_core",
        "pm_current_refit_artifacts",
        "pm_historical_production_adapter",
        "pm_native_prefix_production",
        "pm_trade_power",
        "pm_winner_raw_evidence",
        "pm_winner_selection",
    }
    assert all(
        len(value) == 64 for value in contract["dependency_source_sha256"].values()
    )


def test_executor_rederives_cell_state_envelope_before_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = {"receipt_sha256": "e" * 64}
    cell_plan = SimpleNamespace(
        cell_receipt={"state_error_envelope": envelope},
    )
    fitted = SimpleNamespace()
    calls = []
    monkeypatch.setattr(
        search_planner,
        "verify_preprotected_state_error_envelope",
        lambda observed_fitted, observed_envelope: (
            calls.append((observed_fitted, observed_envelope)) or True
        ),
    )

    execution._reverify_cell_state_error_envelope(
        cell_plan=cell_plan,
        fitted=fitted,
    )
    assert calls == [(fitted, envelope)]

    monkeypatch.setattr(
        search_planner,
        "verify_preprotected_state_error_envelope",
        lambda *_args, **_kwargs: False,
    )
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="STATE_ERROR_ENVELOPE_REVERIFY_FAILED",
    ):
        execution._reverify_cell_state_error_envelope(
            cell_plan=cell_plan,
            fitted=fitted,
        )


def test_source_identity_uses_the_raw_winner_trade_power_policy_owner() -> None:
    from tests.test_trade_power import _source_contract

    assert execution.raw_winner.PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS == 8
    assert execution._source_identity(_source_contract()) == (
        "51e09d17bf77c779d3e6278bb65e062525aad9d9ccc0044bbaff90f402d3cf59"
    )
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="TRADE_POWER_SOURCE_CONTRACT_INVALID",
    ):
        execution._source_identity({})


def test_native_z_cutoff_is_adapted_to_book_canonical_utc() -> None:
    source = book.create_historical_source_group_decision(
        symbol="EURUSD",
        timeframe="D1",
        availability=book.AVAILABLE,
        reason_code="AUTHENTICATED_NATIVE_PREFIX_AVAILABLE",
        source_contract_sha256="1" * 64,
        source_group_evidence_sha256="2" * 64,
        activation_id="activation-1",
        activation_manifest_sha256="3" * 64,
        native_source_sha256="4" * 64,
        acquisition_attempts_exact=1,
    )
    assert source is not None

    cutoff = execution._book_canonical_utc(
        "2024-06-08T00:00:00Z",
        "REGIME_DETECTOR_CUTOFF_INVALID",
    )
    assert cutoff == "2024-06-08T00:00:00+00:00"
    record = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc=cutoff,
        detector_version=execution.REGIME_FIT_POLICY_VERSION,
        detector_code_sha256=execution.pm_regime.detector_code_sha256(),
        parameters=execution._default_regime_parameters("D1"),
    )
    assert record is not None
    assert record["fit_cutoff_utc"] == cutoff
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="REGIME_DETECTOR_CUTOFF_INVALID",
    ):
        execution._book_canonical_utc(
            "2024-06-08T00:00:00",
            "REGIME_DETECTOR_CUTOFF_INVALID",
        )


def test_canary_prefix_binding_uses_requested_cutoff_across_weekend_gap() -> None:
    source_accessed_at = "2026-07-22T16:59:43Z"
    request = {
        "symbol": "EURUSD",
        "timeframe": "D1",
        "activation_id": "activation-1",
        "activation_manifest_sha256": "a" * 64,
        "anchor": {"selection_cutoff_utc": "2024-06-09T00:00:00Z"},
    }
    native = {
        "source_accessed_at_utc": source_accessed_at,
        "symbol": "EURUSD",
        "timeframe": "D1",
        # Friday's authenticated D1 bar closes before the Sunday cutoff.
        "prefix_end_exclusive_utc": "2024-06-08T00:00:00Z",
        "activation_id": "activation-1",
        "activation_manifest_sha256": "a" * 64,
    }
    index = {"cutoff_utc": "2024-06-09T00:00:00Z"}

    assert execution._canary_native_prefix_matches_request(
        native,
        index=index,
        request_body=request,
        source_accessed_at_utc=source_accessed_at,
    )

    for field, replacement in {
        "source_accessed_at_utc": "2026-07-22T16:59:44Z",
        "symbol": "GBPUSD",
        "timeframe": "H1",
        "activation_id": "activation-2",
        "activation_manifest_sha256": "b" * 64,
    }.items():
        changed = dict(native)
        changed[field] = replacement
        assert not execution._canary_native_prefix_matches_request(
            changed,
            index=index,
            request_body=request,
            source_accessed_at_utc=source_accessed_at,
        )

    assert not execution._canary_native_prefix_matches_request(
        native,
        index={"cutoff_utc": native["prefix_end_exclusive_utc"]},
        request_body=request,
        source_accessed_at_utc=source_accessed_at,
    )


def test_winner_resource_boundaries_are_exactly_mirrored() -> None:
    # The point of this assertion is that the two modules mirror each other and
    # that the planner's per-cell roster fits exactly, not that the bound holds
    # any particular value. Pinning the literal made a payload limit read as a
    # design constant, which is how a 128-attempt serialization bound ended up
    # capping how much edge the search could find.
    import pm_production_search_planner as planner

    assert execution.MAX_ATTEMPTS == winner.MAX_ATTEMPTS
    assert planner.ATTEMPT_COUNT == winner.MAX_ATTEMPTS
    assert execution.MAX_FOLDS == winner.MAX_FOLDS == 12
    assert execution.MIN_FOLDS == winner.PINNED_SELECTION_POLICY["min_causal_folds"]
    assert (
        execution.MIN_DAYS_PER_FOLD
        == winner.PINNED_SELECTION_POLICY["min_days_per_fold"]
    )
    assert execution.MAX_DAYS_PER_FOLD == winner.MAX_DAYS_PER_FOLD


def test_winner_fold_plan_accepts_exact_nonoverlapping_whole_day_geometry() -> None:
    plan = execution._winner_fold_plan((_verified_attempt_with_folds(),))
    assert len(plan) == 4
    assert plan[0] == {
        "anchor_as_of_utc": "2018-01-01T00:00:00Z",
        "oos_start_date_utc": "2018-01-01",
        "oos_end_date_utc": "2018-03-04",
    }


@pytest.mark.parametrize(
    ("attempt", "reason"),
    (
        (_verified_attempt_with_folds(count=13), "WINNER_FOLD_COUNT_INVALID"),
        (_verified_attempt_with_folds(days=62), "WINNER_FOLD_DATE_SPAN_INVALID"),
        (
            _verified_attempt_with_folds(overlap_second=True),
            "WINNER_FOLD_INTERVALS_OVERLAP",
        ),
    ),
)
def test_winner_fold_plan_rejects_out_of_contract_geometry(attempt, reason) -> None:
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match=reason):
        execution._winner_fold_plan((attempt,))


def test_fold_provenance_carries_actual_stress_and_native_source_receipts() -> None:
    class Decision:
        def __init__(self, body):
            self.body = body
            self.receipt_sha256 = execution._sha256(
                {
                    name: value
                    for name, value in body.items()
                    if name != "receipt_sha256"
                }
            )

        def to_dict(self):
            return {**self.body, "receipt_sha256": self.receipt_sha256}

    source = Decision(
        {
            "source_contract_sha256": "1" * 64,
            "native_source_sha256": "2" * 64,
            "activation_manifest_sha256": "3" * 64,
        }
    )
    fit = Decision({"fit_state": "REGIME_FITTED"})
    fitted = SimpleNamespace(
        product=SimpleNamespace(
            index_sha256="4" * 64,
            native_prefix={"prefix_content_sha256": "5" * 64},
        ),
        source_group_decision=source,
        regime_fit_decision=fit,
        execution_spec_receipt={
            "schema_version": execution.EXECUTION_SPEC_RECEIPT_SCHEMA,
            "receipt_sha256": "6" * 64,
        },
    )
    artifacts = SimpleNamespace(
        horizon_process_sha256="6" * 64,
        winner_process_sha256="7" * 64,
        trial_definition_sha256="8" * 64,
        refit_recipe_sha256="9" * 64,
    )
    attempt = execution._VerifiedAttempt(
        ordinal=0,
        plan_attempt={"attempt_id": "horizon-attempt-001"},
        candidate={
            "candidate_id": "candidate-001",
            "candidate_definition_sha256": "a" * 64,
        },
        trial={"trial_id": "trial-001", "trial_definition_sha256": "b" * 64},
        artifacts=artifacts,
    )
    partitions = tuple(
        SimpleNamespace(
            geometry_receipt={"receipt_sha256": char * 64, "frame_sha256": char * 64}
        )
        for char in ("c", "d", "e")
    )
    final_signals = pd.Series([0, 1, -1], dtype="int32")
    signal_sha256 = execution.hashlib.sha256(
        final_signals.to_numpy(dtype="<i4").tobytes(order="C")
    ).hexdigest()
    stress = {
        "schema_version": "stress",
        "signal_vector_sha256": signal_sha256,
        "receipt_sha256": "0" * 64,
    }
    lanes = {name: {"lane": name} for name in execution.raw_winner._LANE_KEYS}
    produced = execution._fold_provenance(
        plan_body={
            "anchor": {
                "anchor_id": "anchor-001",
                "process_precommit_receipt_sha256": "1" * 64,
                "selection_cutoff_utc": "2020-01-01T00:00:00Z",
            },
            "cell": {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"},
            "trial_registry_sha256": "2" * 64,
            "attempt_roster_sha256": "3" * 64,
            "attempts": [{}],
        },
        plan_receipt_sha256="4" * 64,
        attempt=attempt,
        fold={
            "fold_id": "fold-001",
            "fold_definition_sha256": "5" * 64,
            "fit_start_utc": "2018-01-01T00:00:00Z",
            "fit_end_exclusive_utc": "2019-01-01T00:00:00Z",
        },
        fold_ordinal=0,
        partitions=partitions,
        lanes=lanes,
        fitted=fitted,
        stress_method_receipt=stress,
        final_signals=final_signals,
    )
    replay = produced["attempt_replay_receipt"]
    assert replay["stress_method_receipt"] == stress
    assert replay["stress_method_receipt_sha256"] == stress["receipt_sha256"]
    assert replay["signal_vector_receipt"]["signal_vector_sha256"] == signal_sha256
    assert replay["signal_vector_receipt"]["values"] == [0, 1, -1]
    oos = produced["oos_source_receipt"]
    assert oos["source_contract_sha256"] == "1" * 64
    assert oos["native_source_sha256"] == "2" * 64
    assert oos["activation_manifest_sha256"] == "3" * 64
    assert oos["execution_spec_receipt"] == fitted.execution_spec_receipt


# --- Terminal native-cell fit (seq1466/seq1468 convention) ---------------

import pm_book_multiplicity as book  # noqa: E402
import pm_native_prefix_production as native_prefix  # noqa: E402
from pm_core import PipelineConfig  # noqa: E402


def _terminal_product(state, *, symbol="EURUSD", timeframe="M5"):
    """A directly sealed terminal NativePrefixProduct for fit tests.

    Activation fields are deliberately populated to prove they cannot be
    smuggled into the terminal Book state.
    """
    native = {
        "source_state": state,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_reason": f"{state}: broker archive unavailable for cell",
        "source_terminal_receipt_sha256": "a" * 64,
        "source_identity_sha256": "b" * 64,
        "request_map_sha256": "c" * 64,
        "request_start_utc": "2018-01-01T00:00:00Z",
        "request_end_exclusive_utc": "2026-01-01T00:00:00Z",
        "activation_id": "native-activation-terminal-x",
        "activation_manifest_sha256": "d" * 64,
        "activation_entry_sha256": "e" * 64,
        "artifact_sha256": None,
        "provenance_sha256": None,
    }
    index = {"cutoff_utc": "2026-01-01T00:00:00Z", "source_state": state}
    return native_prefix.NativePrefixProduct(
        source_state=state,
        index=index,
        index_descriptor={},
        index_sha256="f" * 64,
        native_prefix=native,
        instrument_snapshot=None,
        prefix_path=None,
        prefix_descriptor=None,
        artifact_descriptors=(),
        _seal=native_prefix._PRODUCT_SEAL,
    )


def _config():
    return PipelineConfig()


def _held_canary_source_fixture():
    frame = _d1_frame(12)
    fitted_frame_sha256 = execution._frame_sha256(frame)
    product = SimpleNamespace(
        index_sha256="i" * 64,
        native_prefix={"prefix_content_sha256": "n" * 64},
    )
    fitted = execution.FittedNativeSearchTimeframe(
        product=product,
        source_group_decision=SimpleNamespace(receipt_sha256="s" * 64),
        regime_fit_decision=SimpleNamespace(receipt_sha256="r" * 64),
        source_contract_seed={},
        base_cost_policy={},
        stress_cost_policy={},
        regime_parameters={},
        train_regime_support={"receipt_sha256": "t" * 64},
        instrument_spec=None,
        execution_spec_receipt={},
        frame_sha256=fitted_frame_sha256,
        _frame=frame,
    )
    config = _config()
    session = execution.BoundedRawWinnerCanarySourceSession(
        _body_json=b"{}",
        _product=product,
        _fitted=fitted,
        _pipeline_config=config,
        _frozen_pipeline_config=object(),
        _spool_entry_json=b"{}",
        _issuance_tag="test",
    )
    body = {
        "fitted_frame_sha256": fitted_frame_sha256,
        "native_prefix_index_sha256": "i" * 64,
        "native_prefix_content_sha256": "n" * 64,
        "source_group_decision_sha256": "s" * 64,
        "regime_fit_decision_sha256": "r" * 64,
        "train_regime_support_receipt_sha256": "t" * 64,
    }
    return session, body, config


def test_canary_intermediate_source_check_uses_held_binding_not_full_refit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, body, config = _held_canary_source_fixture()
    monkeypatch.setattr(
        execution,
        "_reverify_fitted_timeframe",
        lambda *_args, **_kwargs: pytest.fail("intermediate check repeated full fit"),
    )

    execution._verify_bounded_canary_held_source_state(
        source_session=session,
        source_body=body,
        current_config=config,
    )


def test_canary_intermediate_source_check_rejects_frame_mutation() -> None:
    session, body, config = _held_canary_source_fixture()
    session._fitted._frame.iloc[0, 0] += 1.0

    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="CANARY_SOURCE_SESSION_REPLAY_MISMATCH",
    ):
        execution._verify_bounded_canary_held_source_state(
            source_session=session,
            source_body=body,
            current_config=config,
        )


def test_canary_intermediate_source_check_rejects_config_mutation() -> None:
    session, body, config = _held_canary_source_fixture()
    current_config = _config()
    session._pipeline_config.initial_capital += 1.0

    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="CANARY_SOURCE_PIPELINE_CONFIG_BINDING_INVALID",
    ):
        execution._verify_bounded_canary_held_source_state(
            source_session=session,
            source_body=body,
            current_config=current_config,
        )


def test_final_canary_replay_rejects_nested_fit_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retained full replay catches state the cheap handoff check defers."""

    session, _body, config = _held_canary_source_fixture()
    source_decision = SimpleNamespace(
        receipt_sha256="s" * 64,
        to_dict=lambda: {
            "acquisition_attempts_exact": 1,
            "receipt_sha256": "s" * 64,
        },
    )
    regime_decision = SimpleNamespace(
        receipt_sha256="r" * 64,
        to_dict=lambda: {"receipt_sha256": "r" * 64},
    )
    pristine = replace(
        session._fitted,
        source_group_decision=source_decision,
        regime_fit_decision=regime_decision,
        base_cost_policy={"spread_multiplier": 1.0},
    )
    substituted = replace(
        pristine,
        base_cost_policy={"spread_multiplier": 0.5},
    )
    monkeypatch.setattr(
        execution,
        "fit_native_timeframe_for_search",
        lambda **_kwargs: pristine,
    )

    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="FITTED_TIMEFRAME_REPLAY_MISMATCH",
    ):
        execution._reverify_fitted_timeframe(substituted, config)


def test_historical_executor_replays_before_attempt_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Late fitted-state corruption cannot reach roster or market evaluation."""

    session, _body, config = _held_canary_source_fixture()
    source_decision = SimpleNamespace(
        receipt_sha256="s" * 64,
        to_dict=lambda: {
            "acquisition_attempts_exact": 1,
            "receipt_sha256": "s" * 64,
            "symbol": "EURUSD",
            "timeframe": "D1",
        },
    )
    fitted = replace(session._fitted, source_group_decision=source_decision)
    capture = object.__new__(execution.historical_adapter.HistoricalSymbolSearchCapture)
    capture.record_search_infrastructure_failure = lambda *_args, **_kwargs: None
    calls: list[str] = []

    def reject_replay(*_args, **_kwargs):
        calls.append("replay")
        raise execution.AuthoritativeSearchExecutionError(
            "FITTED_TIMEFRAME_REPLAY_MISMATCH"
        )

    monkeypatch.setattr(execution, "_reverify_fitted_timeframe", reject_replay)
    monkeypatch.setattr(
        execution,
        "_verify_attempt_roster",
        lambda *_args, **_kwargs: pytest.fail(
            "attempt verification ran after replay rejected fitted state"
        ),
    )
    monkeypatch.setattr(
        execution,
        "_verified_historical_cell_plan_inputs",
        lambda *_args, **_kwargs: ({}, ()),
    )
    monkeypatch.setattr(
        execution,
        "_verify_capture_horizon_plan_identity",
        lambda *_args, **_kwargs: None,
    )

    result = execution.execute_historical_observed_cell(
        capture=capture,
        fitted=fitted,
        regime="TREND",
        search_process=object(),
        cell_plan=object(),
        config=config,
    )

    assert calls == ["replay"]
    assert result.status == "TECHNICAL_FAILURE"
    assert result.failure_code == "FITTED_TIMEFRAME_REPLAY_MISMATCH"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("native_prefix_index_sha256", "x" * 64),
        ("native_prefix_content_sha256", "x" * 64),
        ("source_group_decision_sha256", "x" * 64),
        ("regime_fit_decision_sha256", "x" * 64),
        ("train_regime_support_receipt_sha256", "x" * 64),
    ],
)
def test_canary_intermediate_source_check_rejects_receipt_splice(
    field: str,
    value: str,
) -> None:
    session, body, config = _held_canary_source_fixture()
    body[field] = value

    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="CANARY_SOURCE_SESSION_REPLAY_MISMATCH",
    ):
        execution._verify_bounded_canary_held_source_state(
            source_session=session,
            source_body=body,
            current_config=config,
        )


def test_terminal_source_unavailable_derives_bound_book_decisions():
    product = _terminal_product(native_prefix.SOURCE_UNAVAILABLE)
    fitted = execution.fit_terminal_native_timeframe(product=product, config=_config())

    source_body = fitted.source_group_decision.to_dict()
    fit_body = fitted.regime_fit_decision.to_dict()
    assert source_body["availability"] == book.TERMINAL_UNAVAILABLE
    assert source_body["terminal"] is True
    assert fit_body["fit_state"] == book.REGIME_NOT_APPLICABLE
    # Source contract sha is the derived non-evaluation contract, bound both
    # into the source-group decision and echoed on the fitted object.
    assert (
        source_body["source_contract_sha256"] == fitted.terminal_source_contract_sha256
    )
    contract = fitted.terminal_source_contract
    assert contract["schema_version"] == execution.TERMINAL_SOURCE_CONTRACT_SCHEMA
    assert contract["economic_evaluation"] == "NOT_APPLICABLE"
    assert contract["exact_source_group_resolution_attempts"] == 1
    assert contract["native_prefix_index_sha256"] == product.index_sha256
    assert (
        contract["source_terminal_receipt_sha256"]
        == product.native_prefix["source_terminal_receipt_sha256"]
    )


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_terminal_fit_preserves_exact_mt5_symbol(symbol: str) -> None:
    product = _terminal_product(
        native_prefix.SOURCE_UNAVAILABLE,
        symbol=symbol,
        timeframe="H4",
    )

    fitted = execution.fit_terminal_native_timeframe(product=product, config=_config())

    assert fitted.terminal_source_contract["symbol"] == symbol
    assert fitted.source_group_decision.to_dict()["symbol"] == symbol
    assert fitted.regime_fit_decision.to_dict()["symbol"] == symbol


@pytest.mark.parametrize("wrong_case", ("PLATINUM", "platinum"))
def test_terminal_fit_rejects_wrong_case_mt5_symbol(wrong_case: str) -> None:
    product = _terminal_product(
        native_prefix.SOURCE_UNAVAILABLE,
        symbol=wrong_case,
        timeframe="H4",
    )

    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="TERMINAL_NATIVE_CELL_INVALID",
    ):
        execution.fit_terminal_native_timeframe(product=product, config=_config())


def test_terminal_activation_fields_cannot_be_smuggled_into_book_state():
    product = _terminal_product(native_prefix.SOURCE_UNAVAILABLE)
    fitted = execution.fit_terminal_native_timeframe(product=product, config=_config())
    source_body = fitted.source_group_decision.to_dict()
    # The product carries activation values, but terminal Book state must bind
    # them to None (their real binding lives in the reverified receipt).
    assert source_body["activation_id"] is None
    assert source_body["activation_manifest_sha256"] is None
    assert source_body["native_source_sha256"] is None
    # And the derived contract does not launder them in either.
    assert "activation_id" not in fitted.terminal_source_contract


def test_terminal_contract_sha_is_derived_from_product_not_caller():
    base = _terminal_product(native_prefix.SOURCE_UNAVAILABLE)
    base_sha = execution.fit_terminal_native_timeframe(
        product=base, config=_config()
    ).terminal_source_contract_sha256
    # Change one reverified product field -> derived contract sha must change,
    # proving no caller-injected hash can pin the identity.
    mutated = _terminal_product(native_prefix.SOURCE_UNAVAILABLE)
    mutated.native_prefix["source_terminal_receipt_sha256"] = "9" * 64
    mutated_sha = execution.fit_terminal_native_timeframe(
        product=mutated, config=_config()
    ).terminal_source_contract_sha256
    assert base_sha != mutated_sha


def test_terminal_technical_failure_raises_and_makes_no_decision():
    product = _terminal_product(native_prefix.TECHNICAL_FAILURE)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution.fit_terminal_native_timeframe(product=product, config=_config())
    assert caught.value.code == "TERMINAL_NATIVE_TECHNICAL_FAILURE_REQUIRES_ABORT"
    assert "NO_TRADE" not in caught.value.code


def test_authenticated_terminal_technical_is_complete_distinct_abstention():
    product = _terminal_product(native_prefix.TECHNICAL_TERMINAL)
    fitted = execution.fit_terminal_native_timeframe(product=product, config=_config())
    source = fitted.source_group_decision.to_dict()
    fit = fitted.regime_fit_decision.to_dict()

    assert source["availability"] == book.TERMINAL_TECHNICAL
    assert source["reason_code"] == execution.TERMINAL_TECHNICAL_REASON_CODE
    assert source["activation_id"] == product.native_prefix["activation_id"]
    assert (
        source["activation_manifest_sha256"]
        == product.native_prefix["activation_manifest_sha256"]
    )
    assert (
        source["activation_entry_sha256"]
        == product.native_prefix["activation_entry_sha256"]
    )
    assert source["native_source_sha256"] is None
    assert (
        source["terminal_proof_sha256"]
        == (product.native_prefix["source_terminal_receipt_sha256"])
    )
    assert fit["fit_state"] == book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL
    assert fit["complete"] is True
    assert fitted.terminal_source_contract["source_state"] == (
        native_prefix.TECHNICAL_TERMINAL
    )


def test_terminal_technical_cannot_omit_real_activation_entry():
    product = _terminal_product(native_prefix.TECHNICAL_TERMINAL)
    product.native_prefix["activation_entry_sha256"] = None
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution.fit_terminal_native_timeframe(product=product, config=_config())
    assert caught.value.code == "TERMINAL_ACTIVATION_ENTRY_MISSING"


def test_terminal_fit_rejects_unsealed_and_non_terminal_product():
    unsealed = execution.replace(
        _terminal_product(native_prefix.SOURCE_UNAVAILABLE), _seal=object()
    )
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as unsealed_err:
        execution.fit_terminal_native_timeframe(product=unsealed, config=_config())
    assert unsealed_err.value.code == "NATIVE_PREFIX_PRODUCT_UNSEALED"

    available = execution.replace(
        _terminal_product(native_prefix.SOURCE_UNAVAILABLE),
        source_state=native_prefix.AVAILABLE,
    )
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as avail_err:
        execution.fit_terminal_native_timeframe(product=available, config=_config())
    assert avail_err.value.code == "TERMINAL_NATIVE_PRODUCT_NOT_TERMINAL"


# --- Latest-cutoff observed-cell executor ---------------------------------


class _PlanBody:
    def __init__(self, value):
        self.value = value

    def to_dict(self):
        return execution.json.loads(
            execution._canonical_bytes(self.value).decode("ascii")
        )


def _decision(receipt, body):
    return SimpleNamespace(receipt_sha256=receipt, to_dict=lambda: dict(body))


def test_historical_execution_rejects_capture_horizon_plan_substitution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_body = {"schema_version": "test-horizon-plan", "attempts": []}
    bound_plan = _PlanBody(plan_body)
    substituted_plan = _PlanBody(plan_body)
    binding = {"plan_receipt_sha256": execution._sha256(plan_body)}
    capture = object.__new__(execution.historical_adapter.HistoricalSymbolSearchCapture)
    capture._horizon_plans = {("D1", "TREND"): bound_plan}
    capture._horizon_plan_bodies = {("D1", "TREND"): dict(plan_body)}
    capture._bindings = {("D1", "TREND"): binding}
    capture.context = object()
    capture.sink = object()
    process = SimpleNamespace(receipt_sha256="p" * 64)
    capture.request_body = {"search_process_receipt_sha256": process.receipt_sha256}
    cell_plan = SimpleNamespace(
        mode="HISTORICAL_ANCHOR",
        symbol="EURUSD",
        timeframe="D1",
        regime="TREND",
        search_process_receipt_sha256=process.receipt_sha256,
        horizon_plan=substituted_plan,
        attempt_artifacts=lambda: ({},),
    )
    fitted = SimpleNamespace(
        source_group_decision=_decision(
            "s" * 64,
            {"symbol": "EURUSD", "timeframe": "D1"},
        )
    )
    verified = []
    monkeypatch.setattr(
        search_planner,
        "verify_production_cell_search_plan",
        lambda context, *, process, sink, plan: (
            verified.append((context, process, sink, plan)) or True
        ),
    )
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: binding,
    )
    monkeypatch.setattr(
        execution,
        "_reverify_fitted_timeframe",
        lambda *_args, **_kwargs: pytest.fail(
            "substituted plan reached native re-verification"
        ),
    )

    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="HISTORICAL_CAPTURE_HORIZON_PLAN_SUBSTITUTION",
    ):
        execution.execute_historical_observed_cell(
            capture=capture,
            fitted=fitted,
            regime="TREND",
            search_process=process,
            cell_plan=cell_plan,
            config=_config(),
        )
    assert verified == [(capture.context, process, capture.sink, cell_plan)]


def test_historical_executor_replays_envelope_before_attempt_roster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = object.__new__(
        execution.historical_adapter.HistoricalSymbolSearchCapture
    )
    failures = []
    capture.record_search_infrastructure_failure = (
        lambda timeframe, regime, *, failure, attempted_trials_exact: failures.append(
            (
                timeframe,
                regime,
                failure.outcome.reason_code,
                attempted_trials_exact,
            )
        )
    )
    fitted = SimpleNamespace(
        source_group_decision=_decision(
            "s" * 64,
            {"symbol": "EURUSD", "timeframe": "H1"},
        )
    )
    horizon_plan = object()
    attempt_artifacts = ({"attempt": 0},)
    order = []
    monkeypatch.setattr(
        execution,
        "_verified_historical_cell_plan_inputs",
        lambda *_args, **_kwargs: (horizon_plan, attempt_artifacts),
    )
    monkeypatch.setattr(
        execution,
        "_verify_capture_horizon_plan_identity",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        execution,
        "_reverify_fitted_timeframe",
        lambda _fitted, _config: (order.append("fitted") or fitted),
    )

    def reject_envelope(**_kwargs):
        order.append("envelope")
        raise execution.AuthoritativeSearchExecutionError(
            "STATE_ERROR_ENVELOPE_REVERIFY_FAILED"
        )

    monkeypatch.setattr(
        execution,
        "_reverify_cell_state_error_envelope",
        reject_envelope,
    )
    monkeypatch.setattr(
        execution,
        "_verify_attempt_roster",
        lambda **_kwargs: pytest.fail("attempt roster reached before envelope replay"),
    )
    monkeypatch.setattr(
        execution,
        "_close_reserved_stage",
        lambda **_kwargs: None,
    )

    result = execution.execute_historical_observed_cell(
        capture=capture,
        fitted=fitted,
        regime="TREND",
        search_process=SimpleNamespace(),
        cell_plan=SimpleNamespace(),
        config=_config(),
    )

    assert order == ["fitted", "envelope"]
    assert result.status == "TECHNICAL_FAILURE"
    assert result.failure_code == "STATE_ERROR_ENVELOPE_REVERIFY_FAILED"
    assert failures == [
        ("H1", "TREND", "STATE_ERROR_ENVELOPE_REVERIFY_FAILED", 1)
    ]


def _attempt_roster_fixture(*, boundary: bool, attempt_count: int):
    symbol = "EURUSD"
    timeframe = "D1"
    regime = "TREND"
    opens = pd.date_range("2021-01-01T00:00:00Z", periods=800, freq="D")
    prefix_end = pd.Timestamp(opens[-1]) + pd.Timedelta(days=1)
    current_start = pd.Timestamp(opens[opens >= prefix_end - pd.Timedelta(days=365)][0])
    source_count = int(((opens >= current_start) & (opens < prefix_end)).sum())
    native = {
        "symbol": symbol,
        "timeframe": timeframe,
        "prefix_start_utc": pd.Timestamp(opens[0]).isoformat(),
        "prefix_end_exclusive_utc": prefix_end.isoformat(),
        "prefix_last_close_utc": prefix_end.isoformat(),
        "prefix_content_sha256": "c" * 64,
    }
    frame = pd.DataFrame(
        {"_BAR_CLOSE_TS": opens + pd.Timedelta(days=1)},
        index=opens,
    )
    fitted = execution.FittedNativeSearchTimeframe(
        product=SimpleNamespace(native_prefix=native),
        source_group_decision=_decision(
            "s" * 64,
            {"symbol": symbol, "timeframe": timeframe},
        ),
        regime_fit_decision=_decision("r" * 64, {"fit_state": "REGIME_FITTED"}),
        source_contract_seed={},
        base_cost_policy={"lane": "base"},
        stress_cost_policy={"lane": "stress"},
        regime_parameters={},
        train_regime_support={},
        instrument_spec=None,
        execution_spec_receipt={},
        frame_sha256="f" * 64,
        _frame=frame,
    )
    discovery_start = (
        pd.Timestamp(opens[0]) - pd.Timedelta(days=1)
        if boundary
        else pd.Timestamp(opens[30])
    )
    candidate_body = {
        "candidate_id": "DEPTH-D1-365-regression",
        "lookback_kind": "CALENDAR_INTERVAL",
        "lookback_value": 365,
        "realized_start_utc": discovery_start.isoformat(),
    }
    candidate = {
        **candidate_body,
        "candidate_definition_sha256": execution._sha256(candidate_body),
        "folds": []
        if boundary
        else [
            {
                "fold_id": "CAUSAL-01",
                "fit_start_utc": discovery_start.isoformat(),
            }
        ],
    }
    process_body = {
        "semantics": search_planner._process_semantics(
            search_planner._strategy_domains()
        )
    }
    designs = search_planner._attempt_designs(
        process_body,
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
    )[:attempt_count]
    artifacts = []
    trials = []
    attempts = []
    for ordinal, design in enumerate(designs):
        built = execution.current_refit.build_current_refit_search_artifacts_v2(
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_id=design["strategy_id"],
            parameters=design["parameters"],
            lookback_kind=candidate["lookback_kind"],
            lookback_value=candidate["lookback_value"],
            fit_start_utc=current_start.isoformat(),
            fit_end_utc=prefix_end.isoformat(),
            latest_closed_bar_utc=prefix_end.isoformat(),
            source_snapshot_sha256=native["prefix_content_sha256"],
            source_bar_count=source_count,
            seed=ordinal + 1,
        )
        artifacts.append(
            {
                "winner_process": built.winner_process,
                "horizon_process": built.horizon_process,
                "trial_definition": built.trial_definition,
                "refit_recipe": built.refit_recipe,
            }
        )
        parameter_sha = execution._sha256(built.trial_definition["parameters"])
        trial_body = {
            "trial_id": f"TRIAL-{ordinal:03d}-{parameter_sha[:16]}",
            "strategy_id": design["strategy_id"],
            "strategy_release_sha256": built.winner_process["strategy_code_sha256"],
            "parameter_set_sha256": parameter_sha,
        }
        trial = {
            **trial_body,
            "trial_definition_sha256": execution._sha256(trial_body),
        }
        trials.append(trial)
        attempts.append(
            {
                "attempt_id": f"ATTEMPT-{ordinal:03d}",
                "candidate_id": candidate["candidate_id"],
                "trial_id": trial["trial_id"],
                "strategy_id": trial["strategy_id"],
                "status": "PROCESS_REJECTED" if boundary else "PLANNED",
                "reason_code": "SOURCE_BOUNDARY_SHORTFALL" if boundary else None,
            }
        )
    candidate_registry = [
        {
            "candidate_id": candidate["candidate_id"],
            "candidate_definition_sha256": candidate["candidate_definition_sha256"],
            "boundary_shortfall": boundary,
        }
    ]
    trial_registry = [
        {
            key: trial[key]
            for key in (
                "trial_id",
                "strategy_id",
                "strategy_release_sha256",
                "parameter_set_sha256",
                "trial_definition_sha256",
            )
        }
        for trial in trials
    ]
    source_prefix_receipt_sha256 = "e" * 64
    plan_body = {
        "cell": {"symbol": symbol, "timeframe": timeframe, "regime": regime},
        "native_prefix": native,
        "source_prefix_receipt_sha256": source_prefix_receipt_sha256,
        "plan_status": (
            "HISTORICAL_BOUNDARY_LIMITED"
            if boundary
            else "READY_FOR_HISTORICAL_EVIDENCE"
        ),
        "candidates": [candidate],
        "trials": trials,
        "attempts": attempts,
        "candidate_registry": candidate_registry,
        "candidate_registry_sha256": execution._sha256(candidate_registry),
        "trial_registry": trial_registry,
        "trial_registry_sha256": execution._sha256(trial_registry),
        "attempt_roster_sha256": execution._sha256(attempts),
        "expected_evaluations": [],
        "evidence_roster_sha256": execution._sha256([]),
        "horizon_policy": {
            "evaluator_release_sha256": (
                execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
            ),
            "cost_contract_sha256": execution._sha256(fitted.base_cost_policy),
            "cost_stress_contract_sha256": execution._sha256(fitted.stress_cost_policy),
        },
    }
    binding = {
        "cell": plan_body["cell"],
        "source_prefix_receipt_sha256": source_prefix_receipt_sha256,
    }
    return _PlanBody(plan_body), tuple(artifacts), fitted, binding


def test_boundary_roster_maps_128_cutoff_refits_and_skips_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, artifacts, fitted, binding = _attempt_roster_fixture(
        boundary=True,
        attempt_count=128,
    )
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: binding,
    )
    plan_body, verified = execution._verify_attempt_roster(
        plan=plan,
        attempt_artifacts=artifacts,
        fitted=fitted,
        regime="TREND",
    )
    assert len(verified) == 128
    assert {attempt.plan_attempt["status"] for attempt in verified} == {
        "PROCESS_REJECTED"
    }
    assert {
        attempt.artifacts.horizon_process["fit_start_utc"] for attempt in verified
    } != {verified[0].candidate["realized_start_utc"]}

    cell_plan = SimpleNamespace(
        symbol="EURUSD",
        timeframe="D1",
        regime="TREND",
        horizon_plan=plan,
        attempt_artifacts=lambda: artifacts,
    )
    monkeypatch.setattr(
        execution,
        "_verified_current_observed_inputs",
        lambda **_kwargs: (fitted, {}, {}),
    )
    monkeypatch.setattr(
        execution,
        "_verify_attempt_roster",
        lambda **_kwargs: (plan_body, verified),
    )
    monkeypatch.setattr(
        execution,
        "_winner_fold_plan",
        lambda _attempts: pytest.fail("boundary terminal reached evaluation"),
    )
    result = execution.execute_current_observed_cell(
        process=SimpleNamespace(),
        sink=SimpleNamespace(),
        cell_plan=cell_plan,
        fitted=fitted,
        config=_config(),
    )
    assert result.status == "UNSEARCHED_SOURCE_BOUNDARY_SHORTFALL"
    assert result.attempted_trials_exact == 0


def test_attempt_roster_rejects_lookback_and_cutoff_window_splices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, artifacts, fitted, binding = _attempt_roster_fixture(
        boundary=True,
        attempt_count=1,
    )
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: binding,
    )
    wrong_lookback = list(artifacts)
    wrong_lookback[0] = execution.json.loads(
        execution._canonical_bytes(wrong_lookback[0]).decode("ascii")
    )
    wrong_lookback[0]["horizon_process"]["lookback_value"] = 366
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID",
    ):
        execution._verify_attempt_roster(
            plan=plan,
            attempt_artifacts=wrong_lookback,
            fitted=fitted,
            regime="TREND",
        )

    wrong_start = list(artifacts)
    wrong_start[0] = execution.json.loads(
        execution._canonical_bytes(wrong_start[0]).decode("ascii")
    )
    observed = pd.Timestamp(wrong_start[0]["horizon_process"]["fit_start_utc"])
    wrong_start[0]["horizon_process"]["fit_start_utc"] = (
        observed + pd.Timedelta(days=1)
    ).isoformat()
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="ATTEMPT_CUTOFF_REFIT_WINDOW_INVALID",
    ):
        execution._verify_attempt_roster(
            plan=plan,
            attempt_artifacts=wrong_start,
            fitted=fitted,
            regime="TREND",
        )

    duplicated_body = plan.to_dict()
    duplicate = execution.json.loads(
        execution._canonical_bytes(duplicated_body["candidates"][0]).decode("ascii")
    )
    duplicate["candidate_id"] = "DEPTH-D1-365-duplicate"
    duplicate_definition = {
        key: duplicate[key]
        for key in (
            "candidate_id",
            "lookback_kind",
            "lookback_value",
            "realized_start_utc",
        )
    }
    duplicate["candidate_definition_sha256"] = execution._sha256(duplicate_definition)
    duplicated_body["candidates"].append(duplicate)
    duplicate_registry = {
        "candidate_id": duplicate["candidate_id"],
        "candidate_definition_sha256": duplicate["candidate_definition_sha256"],
        "boundary_shortfall": True,
    }
    duplicated_body["candidate_registry"].append(duplicate_registry)
    duplicated_body["candidate_registry_sha256"] = execution._sha256(
        duplicated_body["candidate_registry"]
    )
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="HORIZON_CANDIDATE_SEMANTIC_IDENTITY_INVALID",
    ):
        execution._verify_attempt_roster(
            plan=_PlanBody(duplicated_body),
            attempt_artifacts=artifacts,
            fitted=fitted,
            regime="TREND",
        )


def test_feasible_roster_maps_distinct_discovery_and_cutoff_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, artifacts, fitted, binding = _attempt_roster_fixture(
        boundary=False,
        attempt_count=1,
    )
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: binding,
    )
    _plan_body, verified = execution._verify_attempt_roster(
        plan=plan,
        attempt_artifacts=artifacts,
        fitted=fitted,
        regime="TREND",
    )
    assert len(verified) == 1
    assert verified[0].plan_attempt["status"] == "PLANNED"
    assert (
        verified[0].candidate["realized_start_utc"]
        != verified[0].artifacts.horizon_process["fit_start_utc"]
    )


def test_current_executor_rejects_fitted_support_receipt_splice_before_search(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = execution.authority_spool.freeze_generation_context(
        (tmp_path / "current-support-spool").resolve(),
        generation_id="current-support-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    sink = execution.authority_spool.open_symbol_sink(context, "EURUSD")
    native = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "prefix_content_sha256": "c" * 64,
        "prefix_start_utc": "2020-01-01T00:00:00Z",
        "prefix_end_exclusive_utc": "2025-01-01T00:00:00Z",
        "prefix_last_close_utc": "2024-12-31T23:00:00Z",
        "prefix_rows": 10_000,
    }
    source = _decision("s" * 64, {"symbol": "EURUSD", "timeframe": "H1"})
    fit = _decision("r" * 64, {"fit_state": "REGIME_FITTED"})
    refreshed = SimpleNamespace(
        product=SimpleNamespace(index_sha256="i" * 64, native_prefix=native),
        source_group_decision=source,
        regime_fit_decision=fit,
        train_regime_support={"receipt_sha256": "t" * 64},
    )
    cell = {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}
    plan_body = {"cell": cell, "native_prefix": native}
    receipt = {
        "mode": "CURRENT_REFIT",
        "cell": cell,
        "native_prefix": {
            "index_sha256": "i" * 64,
            "content_sha256": "c" * 64,
            "start_utc": "2020-01-01T00:00:00Z",
            "end_exclusive_utc": "2025-01-01T00:00:00Z",
            "last_close_utc": "2024-12-31T23:00:00Z",
            "rows": 10_000,
        },
        "source_group_decision_sha256": "s" * 64,
        "regime_fit_decision_sha256": "r" * 64,
        "train_regime_support_sha256": "t" * 64,
    }
    cell_plan = SimpleNamespace(
        mode="CURRENT_REFIT",
        native_prefix_product=None,
        fitted_timeframe=None,
        regime="TREND",
        cell_receipt=receipt,
        horizon_plan=_PlanBody(plan_body),
    )
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: {
            "plan_receipt_sha256": execution._sha256(plan_body),
            "train_regime_support_receipt_sha256": "x" * 64,
        },
    )
    monkeypatch.setattr(
        execution,
        "_reverify_fitted_timeframe",
        lambda _fitted, _config: refreshed,
    )
    import pm_production_search_planner as planner

    monkeypatch.setattr(
        planner,
        "verify_production_cell_search_plan",
        lambda *_args, **_kwargs: True,
    )

    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._verified_current_observed_inputs(
            process=SimpleNamespace(),
            sink=sink,
            cell_plan=cell_plan,
            fitted=SimpleNamespace(),
            config=_config(),
        )
    assert caught.value.code == "CURRENT_FITTED_PLAN_BINDING_MISMATCH"

    order = []
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: {
            "plan_receipt_sha256": execution._sha256(plan_body),
            "train_regime_support_receipt_sha256": "t" * 64,
        },
    )
    monkeypatch.setattr(
        execution,
        "_reverify_fitted_timeframe",
        lambda _fitted, _config: (order.append("fitted") or refreshed),
    )
    monkeypatch.setattr(
        execution,
        "_reverify_cell_state_error_envelope",
        lambda **_kwargs: order.append("envelope"),
    )
    observed, observed_receipt, _observed_binding = (
        execution._verified_current_observed_inputs(
            process=SimpleNamespace(),
            sink=sink,
            cell_plan=cell_plan,
            fitted=SimpleNamespace(),
            config=_config(),
        )
    )
    assert observed is refreshed
    assert observed_receipt == receipt
    assert order == ["fitted", "envelope"]


def test_current_cross_binding_v2_contains_only_current_outer_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical_body = {
        "schema_version": execution.raw_winner.HORIZON_WINNER_CROSS_BINDING_SCHEMA,
        "producer_request_sha256": "1" * 64,
        "anchor_id": "anchor",
        "selection_cutoff_utc": "2025-01-01T00:00:00Z",
        "process_precommit_receipt_sha256": "2" * 64,
        "mapping": [],
    }
    monkeypatch.setattr(
        execution.historical_adapter,
        "_cross_binding_body",
        lambda **_kwargs: dict(historical_body),
    )
    cell_plan = SimpleNamespace(
        receipt_sha256="3" * 64,
        causal_precommit_receipt_sha256="4" * 64,
    )
    process = SimpleNamespace(receipt_sha256="5" * 64)
    frozen = SimpleNamespace(artifact_namespace="current-refit/run/cutoff")
    fitted = SimpleNamespace(
        source_group_decision=SimpleNamespace(receipt_sha256="6" * 64),
        regime_fit_decision=SimpleNamespace(receipt_sha256="7" * 64),
    )
    encoded = execution._build_current_cross_binding_bytes(
        cell_plan=cell_plan,
        process=process,
        plan_body={"anchor": {"anchor_id": "anchor"}},
        plan_binding={"plan_receipt_sha256": "8" * 64},
        frozen=frozen,
        attempt_artifacts=(),
        fitted=fitted,
        horizon_execution_contracts=(),
        source_count_receipts=(),
    )
    body = execution.json.loads(encoded.decode("ascii"))
    assert body["schema_version"] == (
        execution.raw_winner.HORIZON_WINNER_CROSS_BINDING_SCHEMA_V2
    )
    assert body["mode"] == execution.raw_winner.CURRENT_CROSS_BINDING_MODE
    assert body["cell_plan_receipt_sha256"] == "3" * 64
    assert body["causal_precommit_receipt_sha256"] == "4" * 64
    assert body["search_process_receipt_sha256"] == "5" * 64
    assert not {
        "producer_request_sha256",
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
    }.intersection(body)


def test_current_cross_binding_is_content_addressed_and_immutable(tmp_path) -> None:
    context = execution.authority_spool.freeze_generation_context(
        (tmp_path / "current-binding-spool").resolve(),
        generation_id="current-binding-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    sink = execution.authority_spool.open_symbol_sink(context, "EURUSD")
    cell_plan = SimpleNamespace(
        timeframe="H1",
        regime="TREND",
        cell_receipt={"artifact_namespace": "current-refit/run/cutoff"},
    )
    encoded = execution._canonical_bytes({"mode": "CURRENT", "value": 1})
    first = execution._persist_current_cross_binding(
        sink=sink, cell_plan=cell_plan, encoded=encoded
    )
    repeated = execution._persist_current_cross_binding(
        sink=sink, cell_plan=cell_plan, encoded=encoded
    )
    assert first == repeated
    assert (
        first["descriptor"]["sha256"] == execution.hashlib.sha256(encoded).hexdigest()
    )
    with pytest.raises(
        execution.authority_spool.AuthoritySpoolError,
        match="ARTIFACT_ID_CONFLICT",
    ):
        execution._persist_current_cross_binding(
            sink=sink,
            cell_plan=cell_plan,
            encoded=execution._canonical_bytes({"mode": "CURRENT", "value": 2}),
        )


def test_current_executor_persists_binding_before_strategy_evaluation(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = execution.authority_spool.freeze_generation_context(
        (tmp_path / "current-executor-spool").resolve(),
        generation_id="current-executor-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    sink = execution.authority_spool.open_symbol_sink(context, "EURUSD")
    namespace = "current-refit/run/cutoff"
    process = SimpleNamespace(receipt_sha256="p" * 64)
    cell_plan = SimpleNamespace(
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        receipt_sha256="q" * 64,
        causal_precommit_receipt_sha256="c" * 64,
        horizon_plan=object(),
        cell_receipt={"artifact_namespace": namespace},
        attempt_artifacts=lambda: ({"attempt": 0},),
    )
    fitted = SimpleNamespace(
        product=SimpleNamespace(),
        source_contract_seed={},
        base_cost_policy={},
        stress_cost_policy={},
    )
    verified_attempt = execution._VerifiedAttempt(
        ordinal=0,
        plan_attempt={"status": "PLANNED"},
        candidate={},
        trial={},
        artifacts=SimpleNamespace(),
    )
    plan_body = {"cell": {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}}
    plan_binding = {"plan_receipt_sha256": execution._sha256(plan_body)}
    monkeypatch.setattr(
        execution,
        "_verified_current_observed_inputs",
        lambda **_kwargs: (fitted, cell_plan.cell_receipt, plan_binding),
    )
    monkeypatch.setattr(
        execution,
        "_verify_attempt_roster",
        lambda **_kwargs: (plan_body, (verified_attempt,)),
    )
    monkeypatch.setattr(
        execution,
        "_winner_fold_plan",
        lambda _attempts: tuple(
            {
                "anchor_as_of_utc": f"2020-0{ordinal + 1}-01T00:00:00Z",
                "oos_start_date_utc": f"2020-0{ordinal + 1}-01",
                "oos_end_date_utc": f"2020-0{ordinal + 1}-28",
            }
            for ordinal in range(4)
        ),
    )
    monkeypatch.setattr(
        execution.historical_adapter,
        "_strict_attempt_artifacts",
        lambda *_args, **_kwargs: (
            ({"attempt": 0},),
            ({"contract": 0},),
            ({"count": 0},),
        ),
    )
    adversity_plan = {
        "schema_version": execution.raw_winner.COST_STRESS_ADVERSITY_PLAN_SCHEMA,
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "row_count": 0,
        "rows_sha256": execution._sha256([]),
        "rows": [],
    }
    monkeypatch.setattr(
        execution,
        "_build_cost_stress_adversity_plan",
        lambda **_kwargs: adversity_plan,
    )
    frozen_bytes = b"{}"
    frozen = SimpleNamespace(
        precommitment={"latest_evidence_boundary_utc": "2020-06-01T00:00:00Z"},
        cost_stress_adversity_plan=adversity_plan,
        frozen_receipt_bytes=frozen_bytes,
        frozen_receipt_entry={
            "descriptor": {"sha256": execution.hashlib.sha256(frozen_bytes).hexdigest()}
        },
    )

    def freeze(*_args, **kwargs):
        assert kwargs["precommitted_evidence_roles"] == ("EVALUATION_REQUIRED",)
        assert kwargs["cost_stress_adversity_plan"] == adversity_plan
        return frozen

    monkeypatch.setattr(
        execution.raw_winner,
        "freeze_winner_search_plan_v3",
        freeze,
    )
    monkeypatch.setattr(execution, "_raw_attempt_ids", lambda *_args: ("raw-0",))
    binding_bytes = execution._canonical_bytes({"mode": "CURRENT"})
    monkeypatch.setattr(
        execution,
        "_build_current_cross_binding_bytes",
        lambda **_kwargs: binding_bytes,
    )
    stage = context.root / "current-stage"

    def make_stage(**_kwargs):
        stage.mkdir()
        return stage

    monkeypatch.setattr(execution, "_current_stage_directory", make_stage)
    persisted = {"value": False}
    real_persist = execution._persist_current_cross_binding

    def persist(**kwargs):
        value = real_persist(**kwargs)
        persisted["value"] = True
        return value

    monkeypatch.setattr(execution, "_persist_current_cross_binding", persist)

    def build_attempt(**kwargs):
        assert persisted["value"] is True
        assert kwargs["cost_stress_adversity_plan"] == adversity_plan
        return {"attempt_id": kwargs["raw_attempt_id"], "folds": []}

    monkeypatch.setattr(execution, "_build_planned_attempt_evidence", build_attempt)
    times = iter(("2025-01-01T00:00:00Z", "2025-01-01T00:00:03Z"))
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: next(times))
    produced = SimpleNamespace(marker="raw-current-complete")

    def produce(_sink, **kwargs):
        assert kwargs["production_cell_search_plan"] is cell_plan
        assert kwargs["production_search_process"] is process
        assert kwargs["provenance_cross_binding_bytes"] == binding_bytes
        assert list(kwargs["attempt_evidence"])[0]["attempt_id"] == "raw-0"
        return produced

    monkeypatch.setattr(
        execution.raw_winner, "produce_winner_v11_raw_evidence", produce
    )

    result = execution.execute_current_observed_cell(
        process=process,
        sink=sink,
        cell_plan=cell_plan,
        fitted=fitted,
        config=_config(),
    )
    assert result.status == "WINNER_RAW_EVIDENCE_COMPLETE"
    assert result.production is produced
    assert result.cross_binding_bytes == binding_bytes
    assert persisted["value"] is True
    assert not stage.exists()
