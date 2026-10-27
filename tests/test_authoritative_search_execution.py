from __future__ import annotations

import copy
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
    frame = _d1_frame(124)
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


def _pair_fixture(*, timeframe="D1", include_results=False):
    """Invented policy-only carrier; it grants no native-source authority."""
    spec = execution.InstrumentSpec(symbol="EURUSD", pip_position=4, point=0.00001)
    config = execution.PipelineConfig(research_mode=True, backtest_spread_mode="conservative_max")
    backtester = execution.Backtester(config)
    frame = _d1_frame(4)
    frame["Spread"] = 12
    results = {}
    for tier, points in (("P75", 9), ("P95", 16)):
        descriptor = {"schema_version": execution.spread_profile.TIER_SCHEMA,
            "symbol": "EURUSD", "timeframe": timeframe, "tier": tier,
            "source_timeframe": timeframe, "point_price": spec.point,
            "spread_points": points, "full_spread_price": points * spec.point,
            "profile_sha256": "a" * 64, "source_identity_sha256": "b" * 64,
            "source_binding": {"fixture_provenance": "SYNTHETIC_POLICY_ONLY"},
            "qualification_authority": False}
        bound = execution.spread_profile.FrozenSpreadTier(execution._canonical_bytes(descriptor).decode("ascii"))
        entry, intrabar, model = backtester._build_half_spread_arrays(
            frame, spec, spread_tier=bound, timeframe=timeframe)
        results[tier] = {"spread_model": model,
            "numerical_execution_cost_attestation": backtester._numerical_execution_cost_attestation(
                entry_half_spread=entry, intrabar_half_spread=intrabar, spread_meta=model, spec=spec)}
    pair = execution.pm_core.build_backtest_frozen_tier_pair_contract(results, spec=spec, use_spread=True)
    return (pair, results) if include_results else pair


def _partition_cost_transport_fixture(*, attempt_count=2, fold_count=4, unique_geometry=False):
    """Invented structural commitments with real policy attestations; no fits or authority."""
    pair, results = _pair_fixture(include_results=True)
    rows = []
    for attempt_ordinal in range(attempt_count):
        for fold_ordinal in range(fold_count):
            for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
                identity = ["SYNTHETIC_STRUCTURAL_ONLY", fold_ordinal, partition]
                if unique_geometry:
                    identity.append(attempt_ordinal)
                rows.append({
                    "attempt_ordinal": attempt_ordinal,
                    "fold_ordinal": fold_ordinal,
                    "partition": partition,
                    "partition_geometry_receipt_sha256": execution._sha256(["geometry", identity]),
                    "partition_frame_sha256": execution._sha256(["frame", identity]),
                    "source_observation_receipt_sha256": execution._sha256(["source", identity]),
                    "tiers": {
                        tier: {
                            "numerical_execution_cost_attestation": copy.deepcopy(
                                results[tier]["numerical_execution_cost_attestation"]),
                            "entry_observations_sha256": execution._sha256(["observations", identity, tier]),
                            "entry_admission_mask_sha256": execution._sha256(["admission", identity, tier]),
                        }
                        for tier in ("P75", "P95")
                    },
                })
    return {
        "pair": pair,
        "rows": rows,
        "attempts": [{"ordinal": ordinal, "precommitted_evidence_role": "EVALUATION_REQUIRED"}
                     for ordinal in range(attempt_count)],
        "folds": [{"fold_ordinal": ordinal} for ordinal in range(fold_count)],
    }


def test_partition_cost_precommitment_builds_source_body_once_per_geometry(monkeypatch):
    """Synthetic geometry/call-count seam with real cost-policy construction, no fit."""
    pair, results = _pair_fixture(include_results=True)
    frame = _d1_frame(4)
    frame["Spread"] = 12
    partitions = tuple(SimpleNamespace(role=partition, frame=frame,
        geometry_receipt={"receipt_sha256": execution._sha256(["geometry", partition]),
            "frame_sha256": execution._sha256(["frame", partition])})
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox"))
    monkeypatch.setattr(execution, "_fold_partitions", lambda **_kwargs: partitions)
    calls = []
    def source_receipt(_fitted, target):
        calls.append(tuple(target))
        return execution._receipt("SYNTHETIC_SOURCE_CACHE_CALL_COUNT_ONLY", {"target": [str(value) for value in target]})
    monkeypatch.setattr(execution, "_source_observation_receipt", source_receipt)
    spec = execution.InstrumentSpec(symbol="EURUSD", pip_position=4, point=0.00001)
    tiers = {tier: execution.spread_profile.FrozenSpreadTier(execution._canonical_bytes(
        results[tier]["spread_model"]["frozen_spread_tier"]).decode("ascii")) for tier in ("P75", "P95")}
    fitted = SimpleNamespace(_frame=frame, _spread_tiers=tiers,
        _entry_spread_observations=pd.Series(12.0, index=frame.index),
        frozen_tier_pair_contract=pair, instrument_spec=spec)
    attempts = tuple(replace(_verified_attempt_with_folds(count=2), ordinal=ordinal) for ordinal in range(2))
    source_cache = {}
    produced = execution._partition_cost_precommitments(attempts=attempts,
        fold_plan=[{"fold_ordinal": 0}, {"fold_ordinal": 1}],
        plan_body={"derived_dependency_geometry": {"warmup_bars": 0},
            "cell": {"symbol": "EURUSD", "timeframe": "D1"}}, fitted=fitted,
        config=execution.PipelineConfig(research_mode=True, backtest_spread_mode="conservative_max"),
        source_observation_cache=source_cache)
    assert len(produced["rows"]) == 12
    assert len(calls) == 3
    assert set(source_cache) == {partition.geometry_receipt["receipt_sha256"] for partition in partitions}
    for logical_sha, encoded in source_cache.values():
        body = execution.json.loads(encoded)
        assert body["receipt_sha256"] == logical_sha
        assert encoded == execution._canonical_bytes(body)


def test_source_observation_publication_releases_bodies_and_stage_keeps_exact_refs(tmp_path, monkeypatch):
    from tests.test_winner_raw_evidence import _source_observation_transport_case
    case = _source_observation_transport_case(tmp_path)
    receipt = case["receipt"]
    source_cache = {execution._sha256(["geometry", partition]):
        (receipt["receipt_sha256"], execution._canonical_bytes(receipt))
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox")}
    original_publish = execution.raw_winner.publish_source_observation_receipt
    publication_calls = []
    def record_publication(*args, **kwargs):
        publication_calls.append(kwargs["receipt"]["receipt_sha256"])
        return original_publish(*args, **kwargs)
    monkeypatch.setattr(execution.raw_winner, "publish_source_observation_receipt", record_publication)
    references = execution._publish_source_observation_references(case["sink"], frozen=case["frozen"],
        source_observation_cache=source_cache)
    assert source_cache == {}
    assert len(references) == 3
    assert publication_calls == [receipt["receipt_sha256"]]
    assert len(case["sink"].artifact_roster()) == 1
    by_partition = {partition: references[execution._sha256(["geometry", partition])]
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox")}
    compact, _, _ = _fold_provenance_transport_fixture(source_observation_references=by_partition)
    inline, _, _ = _fold_provenance_transport_fixture()
    inline_replay = inline["attempt_replay_receipt"]
    inline_replay["source_observation_receipts"] = {
        partition: receipt for partition in ("optimizer_train", "tuning_validation", "final_lockbox")}
    inline["attempt_replay_receipt"] = execution._receipt(execution.PROVENANCE_RECEIPT_VERSION,
        {key: value for key, value in inline_replay.items() if key != "receipt_sha256"})
    value = {"attempt_id": "SYNTHETIC_TRANSPORT_ONLY", "folds": [
        {"fold_ordinal": 0, "provenance": compact, "lanes": {}}]}
    stage = tmp_path / "stage"
    stage.mkdir()
    reservation = SimpleNamespace(consumed=0, materialized=0)
    reservation.consume = lambda count: setattr(reservation, "consumed", reservation.consumed + count)
    reservation.materialize = lambda count: setattr(reservation, "materialized", reservation.materialized + count)
    staged = execution._write_staged_attempt(stage=stage, ordinal=0, value=value,
        remaining_cell_bytes=65536, reservation=reservation)
    assert execution._reopen_staged_attempt(staged) == value
    assert b'"source_observation_references"' in staged.path.read_bytes()
    assert b'"source_observation_receipts"' not in staged.path.read_bytes()
    assert b'"observations"' not in staged.path.read_bytes()
    assert len(execution._canonical_bytes(compact)) < len(execution._canonical_bytes(inline))
    assert reservation.consumed == reservation.materialized == staged.size_bytes


@pytest.mark.parametrize("mutation,expected", [
    ("logical_sha", "SOURCE_OBSERVATION_CACHE_BINDING_MISMATCH"),
    ("noncanonical_bytes", "SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH"),
])
def test_source_observation_publication_refuses_changed_cache_and_keeps_body(tmp_path, mutation, expected):
    from tests.test_winner_raw_evidence import _source_observation_transport_case
    case = _source_observation_transport_case(tmp_path)
    logical_sha = case["receipt"]["receipt_sha256"]
    encoded = execution._canonical_bytes(case["receipt"])
    if mutation == "logical_sha": logical_sha = "0" * 64
    else: encoded += b" "
    geometry = execution._sha256(["SYNTHETIC_TRANSPORT_GEOMETRY"])
    cache = {geometry: (logical_sha, encoded)}
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as error:
        execution._publish_source_observation_references(case["sink"], frozen=case["frozen"],
            source_observation_cache=cache)
    assert error.value.code == expected
    assert cache == {geometry: (logical_sha, encoded)}


@pytest.mark.parametrize("references", [None, {}, [], {"geometry": {}}],
    ids=["missing", "empty", "non_mapping", "nonempty_mapping"])
def test_official_source_observation_reference_set_is_required(references):
    if references:
        import inspect
        assert execution._require_source_observation_references(references) is references
        parameter = inspect.signature(execution._build_planned_attempt_evidence).parameters["source_observation_references"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as error:
            execution._require_source_observation_references(references)
        assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_SET_REQUIRED"


@pytest.mark.parametrize("mutation,expected", [
    ("missing_geometry", "SOURCE_OBSERVATION_REFERENCE_GEOMETRY_MISMATCH"),
    ("duplicate_precommit", "SOURCE_OBSERVATION_REFERENCE_GEOMETRY_MISMATCH"),
    ("geometry", "SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH"),
    ("frame", "SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH"),
    ("logical_sha", "SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH"),
])
def test_source_observation_reference_selection_keeps_exact_partition_binding(mutation, expected):
    case = _partition_cost_transport_fixture(attempt_count=1, fold_count=1)
    precommitments = execution.raw_winner.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    first = case["rows"][0]
    geometry_sha = first["partition_geometry_receipt_sha256"]
    partition = SimpleNamespace(role=first["partition"], geometry_receipt={
        "receipt_sha256": geometry_sha, "frame_sha256": first["partition_frame_sha256"]})
    reference = {"source_observation_receipt_sha256": first["source_observation_receipt_sha256"]}
    references = {geometry_sha: reference}
    attempt = SimpleNamespace(ordinal=0)
    assert execution._partition_source_observation_reference(references=references,
        precommitments=precommitments, pair=case["pair"], attempt=attempt,
        fold_ordinal=0, partition=partition) is reference
    if mutation == "missing_geometry": references.clear()
    elif mutation == "duplicate_precommit": precommitments["rows"].append(copy.deepcopy(precommitments["rows"][0]))
    elif mutation == "geometry":
        partition.geometry_receipt["receipt_sha256"] = "0" * 64
        references["0" * 64] = reference
    elif mutation == "frame": partition.geometry_receipt["frame_sha256"] = "0" * 64
    else: reference["source_observation_receipt_sha256"] = "0" * 64
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as error:
        execution._partition_source_observation_reference(references=references,
            precommitments=precommitments, pair=case["pair"], attempt=attempt,
            fold_ordinal=0, partition=partition)
    assert error.value.code == expected


@pytest.mark.parametrize("fallback_value", (True, None))
def test_current_cost_pair_rejects_unbound_legacy_spread(fallback_value):
    spec = execution.InstrumentSpec(symbol="EURUSD", pip_position=4, point=0.00001)
    legacy = {"spread_model": {"mode": "conservative_max", "fallback_to_fixed": fallback_value}}
    with pytest.raises(ValueError, match="FROZEN_TIER_PAIR"):
        execution.pm_core.build_backtest_frozen_tier_pair_contract(
            {"P75": legacy, "P95": legacy}, spec=spec, use_spread=True)


@pytest.mark.parametrize("changed", [None, "attestation", "observations", "mask", "geometry", "frame"])
def test_partition_cost_precommitment_matches_each_actual_tier(changed):
    pair = _pair_fixture()
    attempt = execution._VerifiedAttempt(ordinal=7, plan_attempt={}, candidate={}, trial={}, artifacts=None)
    partition = execution._PartitionSlice(role="final_lockbox", frame=pd.DataFrame(), warmup_bars=0,
        scored_start_utc="2020-01-01T00:00:00Z", end_exclusive_utc="2020-01-02T00:00:00Z",
        geometry_receipt={"receipt_sha256": "a" * 64, "frame_sha256": "b" * 64})
    result = {"numerical_execution_cost_attestation": {"bound": "actual"},
        "spread_admission": {"entry_observations_sha256": "c" * 64,
                             "entry_admission_mask_sha256": "d" * 64}}
    row = {"attempt_ordinal": 7, "fold_ordinal": 2, "partition": "final_lockbox",
        "partition_geometry_receipt_sha256": "a" * 64, "partition_frame_sha256": "b" * 64,
        "tiers": {tier: {"numerical_execution_cost_attestation": {"bound": "actual"},
            "entry_observations_sha256": "c" * 64, "entry_admission_mask_sha256": "d" * 64}
            for tier in ("P75", "P95")}}
    field = {"attestation": "numerical_execution_cost_attestation", "observations": "entry_observations_sha256",
             "mask": "entry_admission_mask_sha256", "geometry": "partition_geometry_receipt_sha256",
             "frame": "partition_frame_sha256"}.get(changed)
    if field:
        if changed in {"geometry", "frame"}:
            row[field] = "e" * 64
        else:
            row["tiers"]["P95"][field] = "changed"
    precommitments = {"schema_version": execution.raw_winner.PARTITION_COST_PRECOMMITMENTS_SCHEMA,
                      "rows": [row]}
    if changed is None:
        execution._verify_partition_precommitment(precommitments=precommitments, attempt=attempt,
            fold_ordinal=2, partition=partition, tier="P95", result=result, pair=pair)
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError,
                match="PARTITION_COST_PRECOMMIT_RESULT_MISMATCH"):
            execution._verify_partition_precommitment(precommitments=precommitments, attempt=attempt,
                fold_ordinal=2, partition=partition, tier="P95", result=result, pair=pair)


@pytest.mark.parametrize("tier", ["P75", "P95"])
@pytest.mark.parametrize("changed", [None, "attestation", "observations", "mask", "geometry", "frame", "tier"])
def test_compact_partition_cost_precommitment_matches_each_actual_tier(tier, changed):
    case = _partition_cost_transport_fixture(attempt_count=1, fold_count=1)
    row = next(row for row in case["rows"] if row["partition"] == "final_lockbox")
    precommitments = execution.raw_winner.build_partition_cost_precommitments(case["rows"], pair=case["pair"])
    attempt = execution._VerifiedAttempt(ordinal=0, plan_attempt={}, candidate={}, trial={}, artifacts=None)
    partition = execution._PartitionSlice(role="final_lockbox", frame=pd.DataFrame(), warmup_bars=0,
        scored_start_utc="2022-01-24T00:00:00Z", end_exclusive_utc="2022-01-25T00:00:00Z",
        geometry_receipt={"receipt_sha256": row["partition_geometry_receipt_sha256"],
                          "frame_sha256": row["partition_frame_sha256"]})
    expected = row["tiers"][tier]
    result = {"numerical_execution_cost_attestation": copy.deepcopy(expected["numerical_execution_cost_attestation"]),
        "spread_admission": {key: expected[key] for key in (
            "entry_observations_sha256", "entry_admission_mask_sha256")}}
    if changed == "attestation":
        from pm_trade_power import build_numerical_execution_cost_attestation
        original = result["numerical_execution_cost_attestation"]
        result["numerical_execution_cost_attestation"] = build_numerical_execution_cost_attestation(
            use_spread=original["use_spread"], spread_mode=original["spread_mode"],
            instrument_spec_sha256=original["instrument_spec_sha256"],
            entry_half_spread_values_sha256=execution._sha256("ALTERED_STRUCTURAL_ENTRY_ARRAY"),
            intrabar_half_spread_values_sha256=original["intrabar_half_spread_values_sha256"])
        assert result["numerical_execution_cost_attestation"] != original
    elif changed in {"observations", "mask"}:
        field = "entry_observations_sha256" if changed == "observations" else "entry_admission_mask_sha256"
        result["spread_admission"][field] = execution._sha256(["ALTERED_STRUCTURAL_BINDING", field])
    elif changed in {"geometry", "frame"}:
        field = "receipt_sha256" if changed == "geometry" else "frame_sha256"
        partition.geometry_receipt[field] = execution._sha256(["ALTERED_STRUCTURAL_PARTITION", field])
    elif changed == "tier":
        other = row["tiers"]["P95" if tier == "P75" else "P75"]
        result = {"numerical_execution_cost_attestation": copy.deepcopy(other["numerical_execution_cost_attestation"]),
            "spread_admission": {key: other[key] for key in (
                "entry_observations_sha256", "entry_admission_mask_sha256")}}
        assert result != {"numerical_execution_cost_attestation": expected["numerical_execution_cost_attestation"],
            "spread_admission": {key: expected[key] for key in (
                "entry_observations_sha256", "entry_admission_mask_sha256")}}
    arguments = dict(precommitments=precommitments, attempt=attempt, fold_ordinal=0,
        partition=partition, tier=tier, result=result, pair=case["pair"])
    if changed is None:
        execution._verify_partition_precommitment(**arguments)
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as exc:
            execution._verify_partition_precommitment(**arguments)
        assert exc.value.code == "PARTITION_COST_PRECOMMIT_RESULT_MISMATCH"
        assert exc.value.details == ("0", "0", "final_lockbox", tier)


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


def test_canary_failure_diagnostic_preserves_exact_cost_ladder_tuple() -> None:
    exc = execution.AuthoritativeSearchExecutionError(
        "FROZEN_PATH_COST_LADDER_FAILED",
        (
            "0",
            "0",
            "FrozenPathCostLadderError",
            "LADDER_TRADE_PNL_IMPROVEMENT:P75:genuine evidence required",
        ),
    )

    diagnostic = execution._bounded_canary_failure_diagnostic(
        exc,
        attempted_trials_exact=128,
    )

    assert diagnostic == {
        "exception_type": "FrozenPathCostLadderError",
        "failure_details": (
            "0",
            "0",
            "FrozenPathCostLadderError",
            "LADDER_TRADE_PNL_IMPROVEMENT:P75:genuine evidence required",
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


def test_decision_relevant_result_projects_exact_minimal_trade_evidence() -> None:
    trade = {
        "entry_bar": 1,
        "exit_bar": 2,
        "signal_bar": 0,
        "entry_time": "2020-01-01T01:00:00Z",
        "exit_time": "2020-01-01T02:00:00Z",
        "direction": "LONG",
        "position_size": 1.0,
        "risk_amount": 100.0,
        "exit_reason": "TEST_EXIT",
        "pnl_dollars": -1.25,
        "untransported_diagnostic": "discarded",
    }
    result = {
        "trades": [trade],
        "position_events": [],
        "winner_daily_r_surface": {"status": "AVAILABLE"},
        "trade_power_partition_axis": {},
        "execution_cost_timing_contract": {},
        "numerical_execution_cost_attestation": {},
        "swap_accrual_contract": {},
        "spread_model": {},
        "spread_admission": {},
    }

    projected = execution._decision_relevant_result(result, "final_lockbox_base")

    assert set(projected) == set(execution._RAW_LANE_KEYS)
    assert "trades" not in projected
    assert projected["trade_identity_pnl"] == [
        {
            field: trade[field]
            for field in execution.pm_core.BACKTEST_TRADE_IDENTITY_PNL_FIELDS
        }
    ]
    assert "untransported_diagnostic" not in projected["trade_identity_pnl"][0]


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("risk_amount", 0.0),
        ("risk_amount", "100"),
        ("direction", "SIDEWAYS"),
        ("exit_reason", ""),
    ),
)
def test_decision_relevant_result_rejects_invalid_trade_projection_source(
    field: str, replacement: object
) -> None:
    trade = {
        "entry_bar": 1,
        "exit_bar": 2,
        "signal_bar": 0,
        "entry_time": "2020-01-01T01:00:00Z",
        "exit_time": "2020-01-01T02:00:00Z",
        "direction": "LONG",
        "position_size": 1.0,
        "risk_amount": 100.0,
        "exit_reason": "TEST_EXIT",
        "pnl_dollars": -1.25,
    }
    trade[field] = replacement
    result = {
        "trades": [trade],
        "position_events": [],
        "winner_daily_r_surface": {"status": "AVAILABLE"},
        "trade_power_partition_axis": {},
        "execution_cost_timing_contract": {},
        "numerical_execution_cost_attestation": {},
        "swap_accrual_contract": {},
        "spread_model": {},
        "spread_admission": {},
    }
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="BACKTEST_TRADE_IDENTITY_PNL_INVALID",
    ):
        execution._decision_relevant_result(result, "final_lockbox_base")


def test_release_contract_binds_all_execution_dependencies() -> None:
    contract = execution.authoritative_search_execution_contract()
    assert contract["schema_version"] == "fxpm.authoritative-search-execution.v5"
    assert contract["position_event_source"].endswith("position-events-v2")
    assert contract["candidate_costs"] == execution.pm_core.BACKTEST_FROZEN_TIER_PAIR_CONTRACT_SCHEMA_VERSION
    assert contract["independent_cost_accounts_per_partition"] == ["P75", "P95"]
    assert contract["outer_interval_qualification"] == "METHOD_UNADOPTED"
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
        "192d5ee48b65a8d0f0f1b371c7b9f5368ba2df6e093148cbef31278017db29b6"
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
    assert execution.MIN_FOLDS == winner.CURRENT_SELECTION_POLICY["min_causal_folds"]
    assert (
        execution.MIN_DAYS_PER_FOLD
        == winner.CURRENT_SELECTION_POLICY["min_days_per_fold"]
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


def _fold_provenance_transport_fixture(*, source_observation_references=None):
    """Synthetic receipt transport only; no native fit or backtest is executed."""
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
            role=role,
            geometry_receipt={"receipt_sha256": char * 64, "frame_sha256": char * 64}
        )
        for role, char in zip(("optimizer_train", "tuning_validation", "final_lockbox"),
            ("c", "d", "e"), strict=True)
    )
    final_signals = pd.Series([0, 1, -1], dtype="int32")
    signal_sha256 = execution.hashlib.sha256(
        final_signals.to_numpy(dtype="<i4").tobytes(order="C")
    ).hexdigest()
    execution_receipt = {
        "schema_version": "SYNTHETIC_UNIT_TRANSPORT_ONLY",
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
        independent_tier_execution_receipt=execution_receipt,
        partition_signals={name: final_signals for name in ("optimizer_train", "tuning_validation", "final_lockbox")},
        source_observation_receipts=({name: {"receipt_sha256": "a" * 64}
            for name in ("optimizer_train", "tuning_validation", "final_lockbox")}
            if source_observation_references is None else None),
        source_observation_references=source_observation_references,
    )
    return produced, execution_receipt, signal_sha256


@pytest.mark.parametrize("referenced", [False, True])
def test_fold_provenance_transports_independent_tier_and_native_source_receipts(referenced) -> None:
    references = ({partition: {"fixture_provenance": "SYNTHETIC_REFERENCE_TRANSPORT_ONLY"}
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox")}
        if referenced else None)
    produced, execution_receipt, signal_sha256 = _fold_provenance_transport_fixture(
        source_observation_references=references)
    replay = produced["attempt_replay_receipt"]
    assert replay["independent_tier_execution_receipt"] == execution_receipt
    assert replay["independent_tier_execution_receipt_sha256"] == execution_receipt["receipt_sha256"]
    assert replay["signal_vector_receipts"]["final_lockbox"]["signal_vector_sha256"] == signal_sha256
    assert replay["signal_vector_receipts"]["final_lockbox"]["values"] == [0, 1, -1]
    if referenced:
        assert replay["source_observation_references"] == references
        assert "source_observation_receipts" not in replay
        assert {body["schema_version"] for body in produced.values()} == {
            "fxpm.authoritative-search-provenance.v5"}
    else:
        assert set(replay["source_observation_receipts"]) == {"optimizer_train", "tuning_validation", "final_lockbox"}
        assert "source_observation_references" not in replay
        assert {body["schema_version"] for body in produced.values()} == {
            "fxpm.authoritative-search-provenance.v4"}
    oos = produced["oos_source_receipt"]
    assert oos["source_contract_sha256"] == "1" * 64
    assert oos["native_source_sha256"] == "2" * 64
    assert oos["activation_manifest_sha256"] == "3" * 64
    assert oos["execution_spec_receipt"] == {
        "schema_version": execution.EXECUTION_SPEC_RECEIPT_SCHEMA, "receipt_sha256": "6" * 64}


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
        cost_ladder_contract={},
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
        cost_ladder_contract={"spread_multiplier": 1.0},
    )
    substituted = replace(
        pristine,
        cost_ladder_contract={"spread_multiplier": 0.5},
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


def test_historical_capture_rejects_horizon_plan_substitution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_body = {"schema_version": "test-horizon-plan", "attempts": []}
    bound_plan = _PlanBody(plan_body)
    substituted_plan = _PlanBody(plan_body)
    binding = {"plan_receipt_sha256": execution._sha256(plan_body)}
    capture = SimpleNamespace(
        _horizon_plans={("D1", "TREND"): bound_plan},
        _horizon_plan_bodies={("D1", "TREND"): dict(plan_body)},
        _bindings={("D1", "TREND"): binding},
    )
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: binding,
    )
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="HISTORICAL_CAPTURE_HORIZON_PLAN_SUBSTITUTION",
    ):
        execution._verify_capture_horizon_plan_identity(
            capture, timeframe="D1", regime="TREND", horizon_plan=substituted_plan
        )


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
        cost_ladder_contract={},
        frozen_tier_pair_contract=_pair_fixture(),
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
            "schema_version": "fxpm.historical-horizon-policy.v3",
            "cost_contract_sha256": execution._sha256({level["tier"]: execution._sha256(level["numerical_execution_cost_policy"])
                for level in fitted.frozen_tier_pair_contract["tiers"]}),
            "frozen_tier_pair_contract_sha256": execution._sha256(fitted.frozen_tier_pair_contract),
        },
    }
    binding = {
        "cell": plan_body["cell"],
        "source_prefix_receipt_sha256": source_prefix_receipt_sha256,
    }
    return _PlanBody(plan_body), tuple(artifacts), fitted, binding


def test_boundary_roster_maps_128_cutoff_refits_without_evaluation(
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
        "train_regime_support_sha256": "x" * 64,
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
    receipt["train_regime_support_sha256"] = "t" * 64
    monkeypatch.setattr(
        execution.historical,
        "historical_horizon_plan_binding",
        lambda _plan: {
            "plan_receipt_sha256": execution._sha256(plan_body),
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


def test_current_cross_binding_v4_contains_only_current_outer_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical_body = {
        "schema_version": execution.raw_winner.HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V3,
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
    frozen = SimpleNamespace(artifact_namespace="current-refit/run/cutoff",
        selection_generation=winner.CURRENT_GENERATION)
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
        execution.raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4
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


def test_transport_current_entrypoint_is_explicit_and_legacy_default_stays_v15(monkeypatch):
    """SYNTHETIC routing witness, not an issued current-production grant."""
    observed = []
    marker = object()
    monkeypatch.setattr(execution, "_execute_current_observed_cell",
        lambda **kwargs: observed.append(kwargs) or marker)
    args = dict(process=object(), sink=object(), cell_plan=object(), fitted=object(), config=object())
    assert execution.execute_current_observed_cell(**args) is marker
    assert observed[-1]["_generation"] is winner.CURRENT_GENERATION
    assert execution.execute_transport_current_observed_cell(**args) is marker
    assert observed[-1]["_generation"] is winner.V16_GENERATION
    assert {key: value for key, value in observed[-1].items() if key != "_generation"} == args


def test_transport_historical_entrypoint_requires_explicit_capture_generation(monkeypatch):
    """The existing executor still owns source/plan/grant verification."""
    observed = []
    monkeypatch.setattr(execution, "execute_historical_observed_cell",
        lambda **kwargs: observed.append(kwargs) or "SYNTHETIC_ROUTED")
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution.execute_transport_historical_observed_cell(capture=SimpleNamespace(
            _winner_generation=winner.CURRENT_GENERATION))
    assert caught.value.code == "TRANSPORT_CAPTURE_GENERATION_REQUIRED"
    assert observed == []
    capture = SimpleNamespace(_winner_generation=winner.V16_GENERATION)
    assert execution.execute_transport_historical_observed_cell(capture=capture) == "SYNTHETIC_ROUTED"
    assert observed == [{"capture": capture}]


def test_transport_current_cross_binding_uses_v5_and_preserves_cost_bijection(monkeypatch):
    """SYNTHETIC outer-binding witness over the unchanged inner mapping schema."""
    historical = {"schema_version": execution.raw_winner.HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4,
        "producer_request_sha256": "1" * 64, "anchor_id": "synthetic-anchor",
        "selection_cutoff_utc": "2025-01-01T00:00:00Z", "process_precommit_receipt_sha256": "2" * 64,
        "mapping": [{"fixture_provenance": "SYNTHETIC_BINDING_ONLY"}], "mapping_sha256": "3" * 64}
    monkeypatch.setattr(execution.historical_adapter, "_cross_binding_body", lambda **kwargs: dict(historical))
    plan = SimpleNamespace(receipt_sha256="4" * 64, causal_precommit_receipt_sha256="5" * 64)
    fitted = SimpleNamespace(source_group_decision=SimpleNamespace(receipt_sha256="6" * 64),
        regime_fit_decision=SimpleNamespace(receipt_sha256="7" * 64))
    value = execution._build_current_cross_binding_bytes(cell_plan=plan,
        process=SimpleNamespace(receipt_sha256="8" * 64), plan_body={"anchor": {"anchor_id": "synthetic-anchor"}},
        plan_binding={"plan_receipt_sha256": "9" * 64},
        frozen=SimpleNamespace(artifact_namespace="synthetic-v16", selection_generation=winner.V16_GENERATION),
        attempt_artifacts=(), fitted=fitted, horizon_execution_contracts=(), source_count_receipts=())
    body = execution.json.loads(value)
    assert body["schema_version"] == execution.raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5
    assert body["mapping"] == historical["mapping"]
    assert body["mapping_sha256"] == historical["mapping_sha256"]
    assert body["mode"] == execution.raw_winner.CURRENT_CROSS_BINDING_MODE
    assert body["cell_plan_receipt_sha256"] == plan.receipt_sha256
    assert "anchor_id" not in body


@pytest.mark.parametrize("kind", ["plain", "declared", "canary"])
def test_historical_raw_failure_preserves_failure_without_diagnostic_typeerror(kind):
    """SYNTHETIC interface dispatch only; these unissued objects confer no authority."""
    classes = {"plain": execution.historical_adapter.HistoricalSymbolSearchCapture,
        "declared": execution.historical_adapter.DeclaredSlotSearchCapture,
        "canary": execution.historical_adapter.BoundedRawWinnerCanaryCapture}
    capture = object.__new__(classes[kind])
    calls = []
    if kind == "plain":
        def record(handle, failure):
            calls.append((handle, failure, None))
    else:
        def record(handle, failure, **diagnostic):
            calls.append((handle, failure, diagnostic))
    capture.record_raw_evidence_failure = record
    handle = object()
    failure = execution.raw_winner.WinnerRawEvidenceIncomplete("SYNTHETIC_RESOURCE_STOP", "unit witness")
    original = execution.AuthoritativeSearchExecutionError("SYNTHETIC_RESOURCE_STOP",
        ("DEPENDENCE_BLOCK_OPERATION_LIMIT_EXCEEDED",))
    execution._record_historical_raw_failure(capture, handle, failure, original, attempted_trials_exact=300)
    assert len(calls) == 1
    assert calls[0][0] is handle and calls[0][1] is failure
    assert calls[0][1].code == "SYNTHETIC_RESOURCE_STOP"
    if kind == "plain":
        assert calls[0][2] is None
    else:
        assert calls[0][2]["failure_details"] == ("DEPENDENCE_BLOCK_OPERATION_LIMIT_EXCEEDED",)
        assert calls[0][2]["exception_type"] == "AuthoritativeSearchExecutionError"


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


@pytest.mark.parametrize("name,arguments", [
    ("execute_bounded_raw_winner_canary", ["context", "request", "process", "causal_precommit_payload", "frozen_pipeline_config"]),
])
def test_retired_production_execution_fails_before_touching_inputs(name, arguments):
    class UnavailableInput:
        def __getattribute__(self, attribute):
            pytest.fail(f"retired execution accessed input attribute {attribute}")

    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        getattr(execution, name)(**dict.fromkeys(arguments, UnavailableInput()))
    assert caught.value.code == "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"


@pytest.mark.parametrize("relative,path", [
    ("../other/prefix.csv", "C:/outside/prefix.csv"),
    ("objects/prefix.csv", "C:/outside/different.csv"),
    ("C:/outside/prefix.csv", "C:/outside/prefix.csv"),
])
def test_native_spread_source_cannot_escape_its_verified_spool(relative, path):
    from pathlib import Path
    product = SimpleNamespace(prefix_descriptor={"relative_path": relative}, prefix_path=Path(path))
    with pytest.raises(execution.AuthoritativeSearchExecutionError,
                       match="FROZEN_TIER_TARGET_PREFIX_ROUTE_INVALID"):
        execution._native_spread_inputs(product=product, config=object(), frame=pd.DataFrame(), spec=object())


def test_native_candidate_emits_six_genuine_independent_accounts(current_native_case):
    case = current_native_case
    assert case["provenance"] == "UNISSUED_SYNTHETIC_CONTROL_CARRIER"
    assert case["qualification_authority"] is False
    assert case["fitted"].frozen_tier_pair_contract["qualification_authority"] is False
    counts = {"P75": 0, "P95": 0}
    for attempt in case["evidence"]:
        for fold in attempt["folds"]:
            assert set(fold["lanes"]) == {f"{partition}_{tier}" for partition in
                ("optimizer_train", "tuning_validation", "final_lockbox") for tier in ("p75", "p95")}
            for tier in counts:
                for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
                    lane = fold["lanes"][f"{partition}_{tier.lower()}"]
                    counts[tier] += len(lane["trade_identity_pnl"])
                    assert lane["spread_admission"]["missing_executed_legs"] == 0
                    assert lane["spread_model"]["frozen_spread_tier"]["tier"] == tier
            replay = fold["provenance"]["attempt_replay_receipt"]
            assert set(replay["source_observation_receipts"]) == {"optimizer_train", "tuning_validation", "final_lockbox"}
            assert replay["independent_tier_execution_receipt"]["frozen_tier_pair_contract_sha256"] == (
                execution._sha256(case["fitted"].frozen_tier_pair_contract))
    assert counts["P75"] == 0
    assert counts["P95"] > 0


def test_native_spread_source_rejects_other_data_context(tmp_path, current_native_case):
    case = current_native_case
    config = copy.deepcopy(case["config"])
    config.data_dir = tmp_path / "different-native-source"
    fitted = case["fitted"]
    with pytest.raises(execution.AuthoritativeSearchExecutionError,
                       match="FROZEN_TIER_TARGET_PREFIX_CONTEXT_MISMATCH"):
        execution._native_spread_inputs(product=fitted.product, config=config,
            frame=fitted._frame, spec=fitted.instrument_spec)


@pytest.mark.parametrize("binding,expected", [
    ("bars_sha256", "FROZEN_TIER_PROFILE_FINE_SOURCE_BINDING_MISMATCH"),
    ("source_identity", "FROZEN_TIER_SOURCE_REOPEN_FAILED"),
])
def test_native_spread_source_rejects_profile_source_splice(
    monkeypatch, current_native_case, binding, expected,
):
    case = current_native_case
    fitted = case["fitted"]
    profile = copy.deepcopy(execution.spread_profile.load_spread_profile(
        fitted.product._data_dir / "native" / "terms" / "spread_profile.json"))
    cell = profile._cells[(fitted.product.native_prefix["symbol"], "D1")]
    if binding == "source_identity":
        cell["source_identity"]["identity_sha256"] = "f" * 64
    else:
        cell["source_binding"][binding] = "f" * 64
    monkeypatch.setattr(execution.spread_profile, "load_spread_profile", lambda _path: profile)
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match=expected):
        execution._native_spread_inputs(product=fitted.product, config=case["config"],
            frame=fitted._frame, spec=fitted.instrument_spec)


# The fixture is shared across candidate/raw/adapter tests; each mutating case clones its spool.
from tests.test_historical_production_adapter import current_native_case


def _unissued_outer_control(monkeypatch, case, sink, cell_plan, process):
    """Only issuance/state-envelope verification is substituted; all numerical/source gates run."""
    marker = cell_plan.cell_receipt["state_error_envelope"]
    original = case["fitted"]
    def verify_plan(context, *, process: object, sink: object, plan: object):
        assert context is case["context"]
        assert sink is expected_sink and process is expected_process and plan is cell_plan
        assert marker["fixture_provenance"] == "UNISSUED_SYNTHETIC_CONTROL_CARRIER"
        assert marker["qualification_authority"] is False
        assert cell_plan.horizon_plan.to_dict() == case["plan"].to_dict()
        assert cell_plan.attempt_artifacts() == case["attempt_artifacts"]
        return True
    def verify_state(fitted, supplied):
        assert supplied is marker
        assert fitted.frame_sha256 == original.frame_sha256
        assert execution._frame_sha256(fitted._frame) == original.frame_sha256
        assert supplied["protected_cutoff_utc"] == fitted.product.index["cutoff_utc"]
        assert supplied["earliest_state_boundary_utc"] == pd.Timestamp(fitted._frame.index[0]).isoformat()
        assert pd.Timestamp(fitted._frame["_BAR_CLOSE_TS"].max()) <= pd.Timestamp(supplied["protected_cutoff_utc"])
        assert fitted.source_group_decision.to_dict() == original.source_group_decision.to_dict()
        return True
    expected_sink, expected_process = sink, process
    monkeypatch.setattr(search_planner, "verify_production_cell_search_plan", verify_plan)
    monkeypatch.setattr(search_planner, "verify_preprotected_state_error_envelope", verify_state)


def _control_marker(case):
    return {"fixture_provenance": "UNISSUED_SYNTHETIC_CONTROL_CARRIER", "qualification_authority": False,
        "protected_cutoff_utc": case["fitted"].product.index["cutoff_utc"],
        "earliest_state_boundary_utc": pd.Timestamp(case["fitted"]._frame.index[0]).isoformat()}


def test_historical_native_entry_executes_real_tiers_under_unissued_control(tmp_path, monkeypatch, current_native_case):
    from tests import test_historical_production_adapter as fixture
    from tests import test_winner_raw_evidence as raw_fixture
    case, request, capture, _handle, _evidence = fixture._current_capture_from_case(
        current_native_case, tmp_path, freeze=False)
    regime = case["plan"].to_dict()["cell"]["regime"]
    process = SimpleNamespace(receipt_sha256=request.to_dict()["search_process_receipt_sha256"])
    plan = SimpleNamespace(mode="HISTORICAL_ANCHOR", symbol=capture.symbol, timeframe="D1", regime=regime,
        search_process_receipt_sha256=process.receipt_sha256, horizon_plan=case["plan"],
        attempt_artifacts=lambda: case["attempt_artifacts"],
        cell_receipt={"state_error_envelope": _control_marker(case)})
    _unissued_outer_control(monkeypatch, case, capture.sink, plan, process)
    publications = raw_fixture._record_real_source_observation_publication(monkeypatch)
    result = execution.execute_historical_observed_cell(capture=capture, fitted=case["fitted"],
        regime=regime, search_process=process, cell_plan=plan, config=case["config"])
    assert result.status == "WINNER_RAW_EVIDENCE_COMPLETE", result.failure_code
    assert result.production.request.to_dict()["contract_version"] == winner.CURRENT_GENERATION.request_version
    assert result.production.decision.to_dict()["validated"] is True
    assert result.production.decision.status == "NO_PROCESS_WINNER"
    assert case["qualification_authority"] is False
    assert not list(case["context"].root.glob(".authoritative-search-stage-*"))
    assert publications
    actual_frozen = publications[0][0]
    assert all(frozen.frozen_receipt_bytes == actual_frozen.frozen_receipt_bytes for frozen, _ in publications)
    raw_fixture._assert_real_referenced_production_closure(case, result.production,
        frozen=actual_frozen, sink=capture.sink)


def _current_unissued_control_plan(case, *, artifact_namespace="current-refit/unissued-native/cutoff"):
    fitted = case["fitted"]
    horizon = case["plan"].to_dict()
    regime = horizon["cell"]["regime"]
    causal_sha = horizon["anchor"]["process_precommit_receipt_sha256"]
    process = SimpleNamespace(receipt_sha256="d" * 64)
    native = fitted.product.native_prefix
    receipt = {"mode": "CURRENT_REFIT", "cell": dict(horizon["cell"]),
        "artifact_namespace": artifact_namespace,
        "native_prefix": {"index_sha256": fitted.product.index_sha256,
            "content_sha256": native["prefix_content_sha256"], "start_utc": native["prefix_start_utc"],
            "end_exclusive_utc": native["prefix_end_exclusive_utc"], "last_close_utc": native["prefix_last_close_utc"],
            "rows": native["prefix_rows"]},
        "source_group_decision_sha256": fitted.source_group_decision.receipt_sha256,
        "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
        "train_regime_support_sha256": fitted.train_regime_support["receipt_sha256"],
        "horizon_plan_binding": execution.historical.historical_horizon_plan_binding(case["plan"]),
        "causal_process_precommit_receipt_sha256": causal_sha,
        "search_process_receipt_sha256": process.receipt_sha256,
        "state_error_envelope": _control_marker(case),
        "attempts_frozen_before_search": True, "state_replay_required_before_scored_partition": True,
        "confers_winner_causal_current_capital_publication_or_live_authority": False}
    receipt = {**receipt, "receipt_sha256": execution._sha256(receipt)}
    plan = SimpleNamespace(mode="CURRENT_REFIT", symbol=case["sink"].symbol, timeframe="D1", regime=regime,
        native_prefix_product=None, fitted_timeframe=None, horizon_plan=case["plan"], cell_receipt=receipt,
        receipt_sha256=receipt["receipt_sha256"], causal_precommit_receipt_sha256=causal_sha,
        search_process_receipt_sha256=process.receipt_sha256, attempt_artifacts=lambda: case["attempt_artifacts"])
    return plan, process


def test_current_native_entry_executes_real_tiers_under_unissued_control(tmp_path, monkeypatch, current_native_case):
    from tests import test_winner_raw_evidence as raw_fixture
    case = raw_fixture.clone_current_native_case(current_native_case, tmp_path)
    plan, process = _current_unissued_control_plan(case)
    _unissued_outer_control(monkeypatch, case, case["sink"], plan, process)
    publications = raw_fixture._record_real_source_observation_publication(monkeypatch)
    result = execution.execute_current_observed_cell(process=process, sink=case["sink"], cell_plan=plan,
        fitted=case["fitted"], config=case["config"])
    assert result.status == "WINNER_RAW_EVIDENCE_COMPLETE"
    assert result.production.request.to_dict()["contract_version"] == winner.CURRENT_GENERATION.request_version
    assert result.production.decision.to_dict()["validated"] is True
    assert result.production.decision.status == "NO_PROCESS_WINNER"
    assert execution.json.loads(result.cross_binding_bytes)["mode"] == "CURRENT"
    assert plan.cell_receipt["confers_winner_causal_current_capital_publication_or_live_authority"] is False
    assert publications
    actual_frozen = publications[0][0]
    assert all(frozen.frozen_receipt_bytes == actual_frozen.frozen_receipt_bytes for frozen, _ in publications)
    raw_fixture._assert_real_referenced_production_closure(case, result.production, frozen=actual_frozen)


@pytest.mark.parametrize("registry,identity", [
    ("candidate_registry", "candidate_id"),
    ("trial_registry", "trial_id"),
])
def test_current_raw_context_rejects_corrupted_registry(
    monkeypatch, current_native_case, registry, identity,
):
    # Isolate the raw handoff; reuse the real frozen accounts without another fit.
    horizon = search_planner.training_horizon.build_historical_horizon_plan(
        current_native_case["horizon_request"])
    case = {**current_native_case, "plan": horizon}
    plan, process = _current_unissued_control_plan(
        case, artifact_namespace=case["frozen"].artifact_namespace)
    _unissued_outer_control(monkeypatch, case, case["sink"], plan, process)
    binding = execution.json.loads(case["cross_binding_bytes"])
    binding.update({
        "cell_plan_receipt_sha256": plan.receipt_sha256,
        "causal_precommit_receipt_sha256": plan.causal_precommit_receipt_sha256,
        "search_process_receipt_sha256": process.receipt_sha256,
    })
    arguments = {
        "frozen": case["frozen"], "attempt_artifacts": case["attempt_artifacts"],
        "binding": binding, "production_cell_search_plan": plan,
        "production_search_process": process,
    }
    original = execution.raw_winner._validated_current_cell_plan_context(case["sink"], **arguments)
    assert original["plan_body"] == horizon.to_dict()

    damaged = horizon.to_dict()
    damaged[registry][0][identity] += "-corrupted"
    original_bytes = horizon._body_json
    try:
        # Corrupt transport bytes while retaining the genuine seal and declared hashes.
        object.__setattr__(horizon, "_body_json", execution._canonical_bytes(damaged))
        with pytest.raises(execution.raw_winner.WinnerRawEvidenceError) as caught:
            execution.raw_winner._validated_current_cell_plan_context(case["sink"], **arguments)
        assert caught.value.code == "PROVENANCE_CURRENT_HORIZON_ROSTER_INVALID"
    finally:
        object.__setattr__(horizon, "_body_json", original_bytes)


@pytest.mark.parametrize("empty_reference_cache", [False, True],
    ids=["complete-reference-cache", "empty-reference-cache"])
def test_current_executor_persists_binding_before_strategy_evaluation(
    tmp_path, monkeypatch: pytest.MonkeyPatch, empty_reference_cache,
) -> None:
    """SYNTHETIC ORDERING-ONLY stubs; no native cost or qualification evidence."""
    from tests.test_winner_raw_evidence import _source_observation_transport_case
    transport = _source_observation_transport_case(tmp_path, generation_id="current-executor-generation")
    context, sink = transport["context"], transport["sink"]
    namespace = "current-refit/run/cutoff"
    process = SimpleNamespace(receipt_sha256="p" * 64)
    cell_plan = SimpleNamespace(
        symbol="EURUSD", timeframe="H1", regime="TREND",
        receipt_sha256="q" * 64,
        causal_precommit_receipt_sha256="c" * 64,
        horizon_plan=object(), cell_receipt={"artifact_namespace": namespace},
        attempt_artifacts=lambda: ({"attempt": 0},),
    )
    source_seeds = {
        tier: {"fixture_provenance": "SYNTHETIC_ORDERING_ONLY", "qualification_authority": False}
        for tier in ("P75", "P95")
    }
    pair = _pair_fixture(timeframe="H1")
    fitted = SimpleNamespace(
        product=SimpleNamespace(), source_contract_seeds=source_seeds,
        frozen_tier_pair_contract=pair,
    )
    verified_attempt = execution._VerifiedAttempt(
        ordinal=0, plan_attempt={"status": "PLANNED"}, candidate={}, trial={},
        artifacts=SimpleNamespace(),
    )
    plan_body = {"cell": {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}}
    plan_binding = {"plan_receipt_sha256": execution._sha256(plan_body)}
    monkeypatch.setattr(execution, "_verified_current_observed_inputs",
        lambda **_kwargs: (fitted, cell_plan.cell_receipt, plan_binding))
    monkeypatch.setattr(execution, "_verify_attempt_roster",
        lambda **_kwargs: (plan_body, (verified_attempt,)))
    monkeypatch.setattr(execution, "_winner_fold_plan", lambda _attempts: tuple(
        {"anchor_as_of_utc": f"2020-0{ordinal + 1}-01T00:00:00Z",
         "oos_start_date_utc": f"2020-0{ordinal + 1}-01",
         "oos_end_date_utc": f"2020-0{ordinal + 1}-28"}
        for ordinal in range(4)))
    monkeypatch.setattr(execution.historical_adapter, "_strict_attempt_artifacts",
        lambda *_args, **_kwargs: (({"attempt": 0},), ({"contract": 0},), ({"count": 0},)))
    partition_costs = {
        "fixture_provenance": "SYNTHETIC_ORDERING_ONLY", "qualification_authority": False,
        "schema_version": "fxpm.winner-partition-cost-precommitments.v1", "rows": [],
    }
    order = []

    def precommit_costs(**kwargs):
        assert kwargs["attempts"] == (verified_attempt,)
        assert kwargs["fitted"] is fitted
        assert kwargs["plan_body"] is plan_body
        if not empty_reference_cache:
            receipt = transport["receipt"]
            kwargs["source_observation_cache"][execution._sha256(["SYNTHETIC_ORDERING_GEOMETRY"])] = (
                receipt["receipt_sha256"], execution._canonical_bytes(receipt))
        order.append("costs")
        return partition_costs

    monkeypatch.setattr(execution, "_partition_cost_precommitments", precommit_costs)
    frozen_bytes = b"{}"
    frozen = replace(transport["frozen"], artifact_namespace=namespace,
        precommitment_bytes=execution._canonical_bytes({"latest_evidence_boundary_utc": "2020-06-01T00:00:00Z"}),
        partition_cost_precommitments_bytes=execution._canonical_bytes(partition_costs),
        frozen_receipt_bytes=frozen_bytes,
        frozen_receipt_entry_bytes=execution._canonical_bytes({
            "descriptor": {"sha256": execution.hashlib.sha256(frozen_bytes).hexdigest()},
        }),
    )
    def freeze(actual_sink, **kwargs):
        assert actual_sink is sink
        assert kwargs["precommitted_evidence_roles"] == ("EVALUATION_REQUIRED",)
        assert kwargs["source_contract_seeds"] is source_seeds
        assert kwargs["frozen_tier_pair_contract"] is pair
        assert kwargs["partition_cost_precommitments"] is partition_costs
        order.append("freeze")
        return frozen

    monkeypatch.setattr(execution.raw_winner, "freeze_winner_search_plan_v4", freeze)
    monkeypatch.setattr(execution, "_raw_attempt_ids", lambda *_args: ("raw-0",))
    binding_bytes = execution._canonical_bytes({
        "mode": "CURRENT", "fixture_provenance": "SYNTHETIC_ORDERING_ONLY",
        "qualification_authority": False,
    })
    monkeypatch.setattr(execution, "_build_current_cross_binding_bytes", lambda **_kwargs: binding_bytes)
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
        order.append("persist")
        return value

    monkeypatch.setattr(execution, "_persist_current_cross_binding", persist)

    def build_attempt(**kwargs):
        assert persisted["value"] is True
        assert kwargs["partition_cost_precommitments"] == partition_costs
        references = kwargs["source_observation_references"]
        assert len(references) == 1
        reference = next(iter(references.values()))
        assert execution.raw_winner.resolve_source_observation_reference(sink, frozen=frozen,
            reference=reference, expected_receipt_sha256=transport["receipt"]["receipt_sha256"]) == transport["receipt"]
        order.append("evaluate")
        return {"attempt_id": kwargs["raw_attempt_id"], "folds": []}

    monkeypatch.setattr(execution, "_build_planned_attempt_evidence", build_attempt)
    times = iter(("2025-01-01T00:00:00Z", "2025-01-01T00:00:03Z"))
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: next(times))
    produced = SimpleNamespace(marker="raw-current-complete")

    def produce(actual_sink, **kwargs):
        assert actual_sink is sink
        assert kwargs["production_cell_search_plan"] is cell_plan
        assert kwargs["production_search_process"] is process
        assert kwargs["provenance_cross_binding_bytes"] == binding_bytes
        assert list(kwargs["attempt_evidence"])[0]["attempt_id"] == "raw-0"
        order.append("produce")
        return produced

    monkeypatch.setattr(execution.raw_winner, "produce_winner_v15_raw_evidence", produce)
    if empty_reference_cache:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as error:
            execution.execute_current_observed_cell(process=process, sink=sink,
                cell_plan=cell_plan, fitted=fitted, config=_config())
        assert error.value.code == "SOURCE_OBSERVATION_REFERENCE_SET_REQUIRED"
        assert persisted["value"] is True
        assert order == ["costs", "freeze", "persist"]
        assert not stage.exists()
        assert not any(entry["artifact_type"] == "winner_source_observation_receipt" for entry in sink.artifact_roster())
        return
    result = execution.execute_current_observed_cell(
        process=process, sink=sink, cell_plan=cell_plan, fitted=fitted, config=_config(),
    )
    assert result.status == "WINNER_RAW_EVIDENCE_COMPLETE"
    assert result.production is produced
    assert result.cross_binding_bytes == binding_bytes
    descriptor = result.cross_binding_entry["descriptor"]
    assert descriptor["sha256"] == execution.hashlib.sha256(binding_bytes).hexdigest()
    assert descriptor["size_bytes"] == len(binding_bytes)
    assert persisted["value"] is True
    assert order == ["costs", "freeze", "persist", "evaluate", "persist", "produce"]
    assert not stage.exists()


def test_declared_executor_empty_reference_set_records_stop_with_frozen_pointers(tmp_path, monkeypatch):
    """Actual emitter/guard/lifecycle on labelled metadata stubs, not native or full300 proof."""
    from tests import test_declared_slot_owner_surface as declared
    adapter = execution.historical_adapter
    monkeypatch.setattr(declared._TrackedStructuralFitted, "_read_source",
        lambda self: {"symbol": "AUDUSD", "timeframe": "H1"})
    env = declared._declared_environment(tmp_path, monkeypatch)
    capture, fitted, config, plan = declared._structural_live_capture(env, monkeypatch)
    fitted.product = SimpleNamespace()
    fitted.source_contract_seeds = {tier: {"fixture_provenance": "SYNTHETIC_REFERENCE_GUARD_ONLY"}
        for tier in ("P75", "P95")}
    fitted.frozen_tier_pair_contract = {"fixture_provenance": "SYNTHETIC_REFERENCE_GUARD_ONLY"}
    attempt_artifacts = ({"fixture_provenance": "SYNTHETIC_REFERENCE_GUARD_ONLY"},)
    verified = execution._VerifiedAttempt(ordinal=0, plan_attempt={"status": "PLANNED"},
        candidate={}, trial={}, artifacts=SimpleNamespace())
    plan_body = {"cell": {"symbol": "AUDUSD", "timeframe": "H1", "regime": "TREND"}}
    folds = tuple({"fold_ordinal": ordinal} for ordinal in range(4))
    monkeypatch.setattr(execution, "_verified_historical_cell_plan_inputs",
        lambda *args, **kwargs: (plan.horizon_plan, attempt_artifacts))
    monkeypatch.setattr(execution, "_verify_capture_horizon_plan_identity", lambda *args, **kwargs: None)
    monkeypatch.setattr(execution, "_reverify_fitted_timeframe", lambda actual, supplied: actual)
    monkeypatch.setattr(execution, "_reverify_cell_state_error_envelope", lambda **kwargs: None)
    monkeypatch.setattr(execution, "_verify_attempt_roster", lambda **kwargs: (plan_body, (verified,)))
    monkeypatch.setattr(execution, "_winner_fold_plan", lambda attempts: folds)
    costs = {"schema_version": "SYNTHETIC_REFERENCE_GUARD_COST_STUB", "rows": []}
    order = []
    def empty_cost_precommit(**kwargs):
        assert kwargs["source_observation_cache"] == {}
        assert kwargs["fitted"] is fitted and kwargs["attempts"] == (verified,)
        order.append("costs")
        return costs
    monkeypatch.setattr(execution, "_partition_cost_precommitments", empty_cost_precommit)
    pointers = {role: capture.sink.put_json(capture.raw_artifact_namespace + "/synthetic-reference-guard-" + role,
        "synthetic_reference_guard_marker", {"classification": "SYNTHETIC_GUARD_POINTER_NOT_RAW_AUTHORITY", "role": role})["descriptor"]
        for role in ("frozen", "graph", "cross")}
    handle = adapter.FrozenHistoricalCell(symbol=capture.symbol, timeframe="H1", regime="TREND",
        frozen=SimpleNamespace(attempt_ledger={"attempt_count": 1},
            precommitment={"latest_evidence_boundary_utc": "2020-01-01T00:00:00Z"},
            partition_cost_precommitments=costs),
        frozen_receipt_descriptor=pointers["frozen"], frozen_descriptor_graph=pointers["graph"],
        cross_binding_descriptor=pointers["cross"], artifact_namespace=capture.raw_artifact_namespace,
        _attempt_artifacts=attempt_artifacts, _horizon_execution_contracts=(), _source_count_receipts=(),
        _native_prefix_product=None, _cross_binding_bytes=b"", _capture_nonce="synthetic-reference-guard-only")
    def freeze(actual, timeframe, regime, **kwargs):
        assert actual is capture and (timeframe, regime) == ("H1", "TREND")
        assert kwargs["partition_cost_precommitments"] is costs
        capture._active = handle
        order.append("freeze")
        return handle
    monkeypatch.setattr(adapter.DeclaredSlotSearchCapture, "freeze_observed_cell", freeze)
    monkeypatch.setattr(execution, "_raw_attempt_ids", lambda frozen, attempts: ("synthetic-raw-0",))
    original_guard = execution._require_source_observation_references
    guard_inputs = []
    def observe_guard(value):
        guard_inputs.append(value)
        return original_guard(value)
    monkeypatch.setattr(execution, "_require_source_observation_references", observe_guard)
    def forbidden_evaluation(**kwargs):
        pytest.fail("Empty official reference set reached strategy evaluation")
    monkeypatch.setattr(execution, "_build_planned_attempt_evidence", forbidden_evaluation)
    def completion_body(actual, *, status, reason_code, handle, completed):
        assert actual is capture and completed is None
        assert handle is capture._active
        return {"classification": "SYNTHETIC_REFERENCE_GUARD_LIFECYCLE_NOT_FULL300_PRODUCTION",
            "status": status, "reason_code": reason_code, "owner": None,
            "frozen_plan_receipt": handle.frozen_receipt_descriptor,
            "frozen_descriptor_graph": handle.frozen_descriptor_graph,
            "horizon_winner_cross_binding": handle.cross_binding_descriptor,
            "winner_request_entry": None, "winner_decision_entry": None,
            "raw_production_receipt": None, "production_descriptor_graph": None,
            "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    monkeypatch.setattr(adapter, "_declared_capture_completion_body", completion_body)
    result = execution.execute_historical_observed_cell(capture=capture, fitted=fitted,
        regime="TREND", search_process=env.process, cell_plan=plan, config=config)
    assert guard_inputs == [{}] and order == ["costs", "freeze"]
    assert (result.status, result.failure_code) == ("TECHNICAL_FAILURE", "SOURCE_OBSERVATION_REFERENCE_SET_REQUIRED")
    assert result.production is None and result.attempted_trials_exact == 1
    observed = capture.completion
    assert (observed["status"], observed["reason_code"]) == ("STOP", "SOURCE_OBSERVATION_REFERENCE_SET_REQUIRED")
    assert capture._finished is True and capture._active is None
    assert observed["frozen_plan_receipt"] == pointers["frozen"]
    assert observed["frozen_descriptor_graph"] == pointers["graph"]
    assert observed["horizon_winner_cross_binding"] == pointers["cross"]
    failure = adapter._reopen_ascii_json_entry(capture.context, observed["technical_failure_entry"],
        expected_type="declared_slot_technical_failure", code="TEST_REFERENCE_GUARD_FAILURE_INVALID")
    assert failure["failure_code"] == "SOURCE_OBSERVATION_REFERENCE_SET_REQUIRED"
    assert failure["frozen_receipt_sha256"] == pointers["frozen"]["sha256"]
    assert failure["attempted_trials_exact"] == 1
    assert all(observed[name] is None for name in ("owner", "winner_request_entry", "winner_decision_entry",
        "raw_production_receipt", "production_descriptor_graph"))
    assert all(observed[name] is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))
