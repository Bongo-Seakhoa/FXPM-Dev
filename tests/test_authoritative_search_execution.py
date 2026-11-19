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


def test_stage_small_payload_is_not_refused_by_aggregate_cap(tmp_path, monkeypatch) -> None:
    """INVENTED bytes; the retired aggregate policy is not an admission control."""
    monkeypatch.setattr(execution, "MAX_CELL_EVIDENCE_BYTES", 1, raising=False)
    stage = tmp_path / "stage"
    stage.mkdir()
    reservation = SimpleNamespace(consumed=0, materialized=0)
    reservation.consume = lambda count: setattr(reservation, "consumed", reservation.consumed + count)
    reservation.materialize = lambda count: setattr(reservation, "materialized", reservation.materialized + count)
    body = {"attempt_id": "INVENTED", "folds": [{"payload": "small-real-file"}]}
    row = execution._write_staged_attempt(stage=stage, ordinal=3, value=body, reservation=reservation)
    assert row.size_bytes > 1
    assert reservation.consumed == reservation.materialized == row.size_bytes
    assert execution._reopen_staged_attempt(row) == body
    execution._cleanup_stage(stage, [row])
    assert not stage.exists()



def test_stage_and_publication_share_accounting_without_generation_byte_cap(tmp_path) -> None:
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
        reservation=reservation,
    )
    published = execution.authority_spool.write_opaque_artifact(
        context, [b"b" * 2500], format="binary-v1")
    assert row.size_bytes + published["size_bytes"] > limits.max_generation_bytes
    assert (context.root / published["relative_path"]).read_bytes() == b"b" * 2500
    execution.authority_spool._verify_artifact_file(context, published)
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
    unit_fitted_sentinel = object()  # Unit forwarding only; no fitted authority.

    def compute(*, frame, strategy, fitted, symbol, timeframe):
        assert fitted is unit_fitted_sentinel
        calls["features"] += 1
        return frame.copy(deep=True)

    class PathStrategy:
        def generate_signals(self, features, _symbol):
            calls["signals"] += 1
            return pd.Series(
                (pd.RangeIndex(len(features)).to_numpy() % 3) - 1,
                index=features.index,
            )

    monkeypatch.setattr(
        execution,
        "_compute_fitted_strategy_features",
        compute,
    )
    surface = execution._build_full_prefix_strategy_surface(
        frame=frame,
        strategy=PathStrategy(),
        fitted=unit_fitted_sentinel,
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
    """Invented geometry seam; real cost/Raw bytes, no strategy, fit or financial claim."""
    pair, results = _pair_fixture(include_results=True)
    frame = _d1_frame(4)
    frame["Spread"] = 12
    partitions = tuple(SimpleNamespace(role=partition, frame=frame,
        geometry_receipt={"receipt_sha256": execution._sha256(["geometry", partition]),
            "frame_sha256": execution._sha256(["frame", partition])})
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox"))
    fold_calls = []
    def derive_partitions(**kwargs):
        fold_calls.append((kwargs["attempt"].ordinal, dict(kwargs["fold"])))
        assert kwargs["frame"] is frame
        return partitions
    monkeypatch.setattr(execution, "_fold_partitions", derive_partitions)
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
    original = _verified_attempt_with_folds(count=2)
    folds = [{**fold,
        "fit_start_utc": (pd.Timestamp(fold["validation_start_utc"]) - pd.Timedelta(days=30)).isoformat(),
        "fit_end_exclusive_utc": fold["validation_start_utc"],
        "validation_warmup_start_utc": (pd.Timestamp(fold["validation_start_utc"]) - pd.Timedelta(days=1)).isoformat()}
        for fold in original.candidate["folds"]]
    attempts = tuple(replace(original, ordinal=ordinal, candidate={"folds": folds},
        artifacts=SimpleNamespace(refit_recipe={"data_requirements": {"warmup_bars": 1}}))
        for ordinal in range(2))
    config = execution.PipelineConfig(research_mode=True, backtest_spread_mode="conservative_max")
    def produce(selected, target_cache):
        return execution._partition_cost_precommitments(attempts=selected,
            fold_plan=[{"fold_ordinal": 0}, {"fold_ordinal": 1}],
            plan_body={"derived_dependency_geometry": {"warmup_bars": 0},
                "cell": {"symbol": "EURUSD", "timeframe": "D1"}}, fitted=fitted,
            config=config, source_observation_cache=target_cache)
    source_cache = {}
    produced = produce(attempts, source_cache)
    assert len(produced["rows"]) == 12
    assert len(calls) == 3
    assert len(fold_calls) == 2
    assert set(source_cache) == {partition.geometry_receipt["receipt_sha256"] for partition in partitions}
    for logical_sha, encoded in source_cache.values():
        body = execution.json.loads(encoded)
        assert body["receipt_sha256"] == logical_sha
        assert encoded == execution._canonical_bytes(body)

    # Independent calls cannot share summaries. Collect their original full
    # rows through the real Raw builder, then compare the exact combined bytes.
    raw_build = execution.raw_winner.build_partition_cost_precommitments
    unshared_rows = []
    def capture_rows(rows, *, pair):
        unshared_rows.extend(rows)
        return raw_build(rows, pair=pair)
    monkeypatch.setattr(execution.raw_winner, "build_partition_cost_precommitments", capture_rows)
    baseline_cache = {}
    fold_calls.clear()
    for attempt in attempts:
        produce((attempt,), baseline_cache)
    assert len(fold_calls) == 4
    monkeypatch.setattr(execution.raw_winner, "build_partition_cost_precommitments", raw_build)
    baseline = raw_build(unshared_rows, pair=pair)
    assert execution._canonical_bytes(produced) == execution._canonical_bytes(baseline)
    assert source_cache == baseline_cache
    assert [(row["attempt_ordinal"], row["fold_ordinal"]) for row in produced["rows"]] == [
        (attempt, fold) for attempt in range(2) for fold in range(2) for _ in range(3)]

    # Every boundary participates, even when validation axes are unchanged.
    for name in ("fit_start_utc", "fit_end_exclusive_utc", "validation_warmup_start_utc",
            "validation_start_utc", "validation_end_exclusive_utc"):
        changed_folds = [dict(fold) for fold in folds]
        changed_folds[0][name] = (pd.Timestamp(changed_folds[0][name]) + pd.Timedelta(hours=1)).isoformat()
        changed = replace(attempts[1], candidate={"folds": changed_folds})
        fold_calls.clear()
        produce((attempts[0], changed), {})
        assert len(fold_calls) == 3, name
    for warmup in (2, True, "1"):
        changed = replace(attempts[1], artifacts=SimpleNamespace(
            refit_recipe={"data_requirements": {"warmup_bars": warmup}}))
        fold_calls.clear()
        produce((attempts[0], changed), {})
        assert len(fold_calls) == 4, warmup

    # Missing/non-string boundaries reach the existing geometry seam each time;
    # a successful synthetic stub must not turn malformed raw inputs into hits.
    for value in (None, True, 1):
        changed_folds = [dict(fold) for fold in folds]
        if value is None:
            changed_folds[0].pop("fit_start_utc")
        else:
            changed_folds[0]["fit_start_utc"] = value
        malformed = tuple(replace(attempt, candidate={"folds": changed_folds}) for attempt in attempts)
        fold_calls.clear()
        produce(malformed, {})
        assert len(fold_calls) == 3, value


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
        reservation=reservation)
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
    assert contract["schema_version"] == "fxpm.authoritative-search-execution.v6"
    assert contract["regime_surface_acquisition"] == "RECOMPUTED_OR_REOPENED_FROM_ORIGINAL_BOUND_STATE"
    assert contract["bounded_canary_accepts_prebuilt_source_or_fit"] is False
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


def _attempt_roster_fixture(*, boundary: bool, attempt_count: int, process_body=None):
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
    process_body = process_body or {
        "schema_version": search_planner.PROCESS_SCHEMA,
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
    snapshot_calls = []
    snapshot_marker = {"classification": "SYNTHETIC_SNAPSHOT_PHASE_MARKER_NOT_A_BOUND_REFERENCE"}
    def structural_snapshot(actual, actual_fitted, actual_folds, issue_owner):
        # This negative guard test never consumes a fitted reference as authority.
        assert actual is capture and actual_fitted is fitted and actual_folds is folds
        assert type(issue_owner) is execution._FreshFitOwner
        assert issue_owner.fitted is fitted and issue_owner.original_fitted is fitted
        assert issue_owner.context_id == id(capture.context)
        assert issue_owner.request_sha256 == capture.request.receipt_sha256
        assert issue_owner.config is config
        snapshot_calls.append(issue_owner)
        return snapshot_marker
    monkeypatch.setattr(execution, "_publish_declared_bound_fit", structural_snapshot)
    def forbidden_unit_work(*args, **kwargs):
        pytest.fail("Empty reference guard reached F header or unit publication")
    monkeypatch.setattr(execution, "_make_declared_unit_header", forbidden_unit_work)
    monkeypatch.setattr(execution, "_continue_declared_attempt_units", forbidden_unit_work)
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
        assert kwargs["bound_fitted_state_entry"] is snapshot_marker
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
        assert handle is not None and handle is capture._active
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
    assert len(snapshot_calls) == 1
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


# Labelled synthetic store tests. These do not claim native/300-trial/Raw authority.
def _completed_unit_store_fixture(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    context = execution.authority_spool.freeze_generation_context(
        (tmp_path / "unit-store").resolve(), generation_id="synthetic-completed-units",
        native_universe_sha256="a" * 64, config_sha256="b" * 64,
        limits=execution.authority_spool.SpoolLimits(
            max_artifact_bytes=65536, max_symbol_bytes=1048576,
            max_artifacts_per_symbol=100, max_manifest_bytes=65536,
            max_json_bytes=65536, stream_chunk_bytes=1024,
            max_generation_bytes=1048576, min_free_bytes=0))
    body = {"precommit_sha256": "c" * 64, "cell_id": "synthetic-cell",
        "slot_id": "synthetic-slot", "attempt_id": "original-synthetic-attempt",
        "source_access_window_utc": {"start_utc": "2000-01-01T00:00:00Z",
                                   "end_exclusive_utc": "2099-01-01T00:00:00Z"}}
    units = [{"ordinal": ordinal, "raw_attempt_id": f"raw-{ordinal}",
        "plan_attempt_sha256": str(ordinal + 1) * 64,
        "status": "PROCESS_REJECTED" if ordinal == 1 else "PLANNED"}
        for ordinal in range(3)]
    return context, body, "d" * 64, units


def _synthetic_completed_unit_value(unit):
    return {"attempt_id": unit["raw_attempt_id"],
        "synthetic_only": True, "delta": [7, 0, -2][unit["ordinal"]],
        "outcome": {"status": "PROCESS_REJECTED"} if unit["status"] == "PROCESS_REJECTED" else None}


def test_completed_units_resume_disk_prefix_without_recalculating_and_match_uninterrupted(tmp_path):
    context, body, binding, units = _completed_unit_store_fixture(tmp_path / "resumed")
    calls = []
    def interrupted(unit):
        calls.append(unit["ordinal"])
        if unit["ordinal"] == 2:
            raise KeyboardInterrupt("labelled interruption before third unit")
        return _synthetic_completed_unit_value(unit)
    with pytest.raises(KeyboardInterrupt):
        execution._continue_declared_attempt_units(context, body, binding, units, interrupted)
    assert calls == [0, 1, 2]
    # Reopen the real generation context, not an in-memory row list.
    reopened = execution.authority_spool.reopen_generation_context(context.root,
        expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256)
    # Existing reopen is read-only; the ordinary writable freeze verifies identical context.
    reopened = execution.authority_spool.freeze_generation_context(context.root,
        generation_id=context.generation_id,
        native_universe_sha256=context.native_universe_sha256,
        config_sha256=context.config_sha256, limits=context.limits)
    resumed_calls = []
    def remaining(unit):
        resumed_calls.append(unit["ordinal"])
        return _synthetic_completed_unit_value(unit)
    records, ended = execution._continue_declared_attempt_units(reopened, body, binding, units, remaining)
    assert resumed_calls == [2]
    assert len(records) == 3 and records[1]["unit"]["status"] == "PROCESS_REJECTED"
    expected_context, expected_body, expected_binding, expected_units = _completed_unit_store_fixture(tmp_path / "whole")
    full, _ = execution._continue_declared_attempt_units(expected_context, expected_body,
        expected_binding, expected_units, _synthetic_completed_unit_value)
    actual = [execution._read_declared_completed_unit(reopened, body, binding, unit)[1] for unit in units]
    expected = [execution._read_declared_completed_unit(expected_context, expected_body,
        expected_binding, unit)[1] for unit in expected_units]
    assert actual == expected
    # Explicit synthetic carry; no skipped rejected row, and no cash reset within the chain.
    cash = [10000]
    for value in actual:
        cash.append(cash[-1] + value["delta"])
    assert cash == [10000, 10007, 10007, 10005]
    assert [row["payload"]["sha256"] for row in records] == [row["payload"]["sha256"] for row in full]
    assert max(row["completed_at_utc"] for row in records) <= ended
    def no_recalculation(_unit):
        pytest.fail("fully committed unit was recalculated")
    again, again_ended = execution._continue_declared_attempt_units(reopened, body, binding, units, no_recalculation)
    assert again == records and again_ended == ended


@pytest.mark.parametrize("phase", ["before_completion_record", "after_completion_record"])
def test_completed_unit_publication_interruption_has_one_truthful_boundary(tmp_path, monkeypatch, phase):
    context, body, binding, units = _completed_unit_store_fixture(tmp_path)
    real = execution.authority_spool._publish_exact_json
    cut = execution._declared_unit_relative(body, "000000.json")
    tripped = []
    def publish(ctx, relative, value, maximum_bytes):
        if relative == cut and not tripped:
            tripped.append(True)
            if phase == "after_completion_record":
                real(ctx, relative, value, maximum_bytes)
            raise KeyboardInterrupt("labelled metadata boundary interruption")
        return real(ctx, relative, value, maximum_bytes)
    monkeypatch.setattr(execution.authority_spool, "_publish_exact_json", publish)
    with pytest.raises(KeyboardInterrupt):
        execution._continue_declared_attempt_units(context, body, binding, units, _synthetic_completed_unit_value)
    monkeypatch.setattr(execution.authority_spool, "_publish_exact_json", real)
    calls = []
    def produce(unit):
        calls.append(unit["ordinal"])
        return _synthetic_completed_unit_value(unit)
    execution._continue_declared_attempt_units(context, body, binding, units, produce)
    assert calls == ([0, 1, 2] if phase == "before_completion_record" else [1, 2])


@pytest.mark.parametrize("kind", ["payload_tamper", "missing_payload", "wrong_binding",
                                  "wrong_unit", "prefix_hole", "promised_missing_tail"])
def test_completed_units_refuse_corrupt_or_incompatible_prefix_before_more_work(tmp_path, kind):
    context, body, binding, units = _completed_unit_store_fixture(tmp_path)
    records, _ = execution._continue_declared_attempt_units(context, body, binding, units,
        _synthetic_completed_unit_value)
    if kind in {"payload_tamper", "missing_payload"}:
        payload = context.root / records[0]["payload"]["relative_path"]
        if kind == "missing_payload":
            payload.unlink()
        else:
            payload.write_bytes(payload.read_bytes() + b" ")
    elif kind == "wrong_binding":
        binding = "e" * 64
    elif kind == "wrong_unit":
        units[0] = {**units[0], "plan_attempt_sha256": "f" * 64}
    elif kind == "prefix_hole":
        (context.root / execution._declared_unit_relative(body, "000001.json")).unlink()
    else:
        (context.root / execution._declared_unit_relative(body, "000002.json")).unlink()
    def forbidden(_unit):
        pytest.fail("incompatible completed prefix reached new unit work")
    with pytest.raises((execution.AuthoritativeSearchExecutionError,
                        execution.authority_spool.AuthoritySpoolError)):
        execution._continue_declared_attempt_units(context, body, binding, units, forbidden)


def test_completed_units_preserve_original_request_and_do_not_claim_old_orphan_recovery(tmp_path):
    context, body, binding, units = _completed_unit_store_fixture(tmp_path)
    execution._continue_declared_attempt_units(context, body, binding, units, _synthetic_completed_unit_value)
    foreign = {**body, "attempt_id": "different-synthetic-attempt"}
    # Complete objects in another request are not a unit roster for this one.
    assert execution._declared_completed_prefix(context, foreign, binding, units) == []
    observed = []
    def no_transfer(unit):
        observed.append(unit["ordinal"])
        return _synthetic_completed_unit_value(unit)
    execution._continue_declared_attempt_units(context, foreign, binding, units, no_transfer)
    assert observed == [0, 1, 2]


def test_completed_units_missing_work_respects_original_window_and_actual_completion_clock(tmp_path, monkeypatch):
    context, body, binding, units = _completed_unit_store_fixture(tmp_path)
    body["source_access_window_utc"] = {"start_utc": "2020-01-01T00:00:00Z",
                                     "end_exclusive_utc": "2020-01-02T00:00:00Z"}
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: "2020-01-03T00:00:00Z")
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="ORIGINAL_WINDOW_CLOSED"):
        execution._continue_declared_attempt_units(context, body, binding, units,
            lambda _unit: pytest.fail("expired original window admitted new work"))
    assert execution._declared_completed_prefix(context, body, binding, units) == []
    observed = ["2020-01-01T01:00:00Z"]
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: observed[0])
    def work(unit):
        observed[0] = f"2020-01-01T02:00:0{unit['ordinal']}Z"
        return _synthetic_completed_unit_value(unit)
    rows, ended = execution._continue_declared_attempt_units(context, body, binding, units, work)
    assert ended == "2020-01-01T02:00:02Z"
    assert rows[-1]["completed_at_utc"] == ended
    observed[0] = "2020-02-01T00:00:00Z"
    _, same_end = execution._continue_declared_attempt_units(context, body, binding, units,
        lambda _unit: pytest.fail("completed boundary was recomputed"))
    assert same_end == ended


@pytest.mark.parametrize("field", ["producer_request_sha256", "executor_release_sha256",
                                  "producer_source_sha256", "generation_context_sha256"])
def test_completed_unit_header_refuses_owner_or_source_edition_changes(tmp_path, field):
    context, body, _, _ = _completed_unit_store_fixture(tmp_path)
    binding = {"generation_context_sha256": context.context_sha256,
        "producer_request_sha256": execution._sha256(body),
        "executor_release_sha256": execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        "producer_source_sha256": execution._file_sha256(
            execution.Path(execution.__file__).with_name("pm_historical_search_producer.py"))}
    header = {"schema_version": execution._DECLARED_UNIT_HEADER_SCHEMA,
        "binding": {**binding, field: "f" * 64}, "request_body": body,
        "qualification_authority": False, "live_authority": False}
    execution.authority_spool._publish_exact_json(context,
        execution._declared_unit_relative(body, "header.json"),
        header, context.limits.max_json_bytes)
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="HEADER_BINDING_MISMATCH"):
        execution._read_declared_unit_header(context, body)


def test_completed_units_copied_index_cannot_cross_original_request_owner(tmp_path):
    context, body, binding, units = _completed_unit_store_fixture(tmp_path)
    records, _ = execution._continue_declared_attempt_units(context, body, binding, units,
        _synthetic_completed_unit_value)
    foreign = {**body, "attempt_id": "foreign-original-request"}
    execution.authority_spool._publish_exact_json(context,
        execution._declared_unit_relative(foreign, "000000.json"),
        records[0], context.limits.max_json_bytes)
    foreign_binding = execution._sha256({"original_request_sha256": execution._sha256(foreign)})
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="UNIT_BINDING_MISMATCH"):
        execution._continue_declared_attempt_units(context, foreign, foreign_binding, units,
            lambda _unit: pytest.fail("copied index admitted foreign-owner work"))


def _completed_unit_lease_child(root, generation, native, config, body, ready, release,
                                diagnostic_path):
    entry_clock = (__import__("time").monotonic_ns(), __import__("time").time_ns())
    import json
    import os
    import time
    import traceback

    phase = "CHILD_ENTRY"
    def record(stage, *, clock=None, **details):
        nonlocal phase
        phase = stage
        monotonic_ns, utc_ns = clock or (time.monotonic_ns(), time.time_ns())
        utc = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(utc_ns // 10**9))
        row = {"classification": "SYNTHETIC_LEASE_DIAGNOSTIC_ONLY", "phase": stage,
            "pid": os.getpid(), "parent_pid": os.getppid(),
            "monotonic_ns": monotonic_ns, "utc": utc + f".{utc_ns % 10**9:09d}Z",
            **details}
        with open(diagnostic_path, "a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(row, sort_keys=True) + "\n")

    record("CHILD_ENTRY", clock=entry_clock)
    try:
        record("BEFORE_INNER_IMPORTS")
        from pathlib import Path
        import pm_authoritative_search_execution as child_execution
        record("INNER_IMPORTS_COMPLETE")
        record("BEFORE_CONTEXT_REOPEN")
        current = child_execution.authority_spool.reopen_generation_context(Path(root),
            expected_generation_id=generation, expected_native_universe_sha256=native,
            expected_config_sha256=config)
        record("CONTEXT_REOPEN_COMPLETE")
        record("BEFORE_CONTEXT_FREEZE")
        writable = child_execution.authority_spool.freeze_generation_context(Path(root),
            generation_id=generation, native_universe_sha256=native,
            config_sha256=config, limits=current.limits)
        record("CONTEXT_FREEZE_COMPLETE")
        record("BEFORE_LEASE_ENTRY")
        with child_execution._declared_unit_lease(writable, body):
            record("LEASE_ACQUIRED")
            ready.set()
            record("READY_SET")
            record("BEFORE_RELEASE_WAIT")
            released = release.wait(15)
            record("RELEASE_WAIT_COMPLETE", released=released)
            if not released:
                raise RuntimeError("synthetic lease test release not received")
        record("LEASE_EXITED")
    except BaseException as error:
        try:
            record("CHILD_EXCEPTION", failed_phase=phase,
                exception_type=type(error).__name__, exception_message=str(error),
                traceback=traceback.format_exc())
        except OSError:
            traceback.print_exc()
        raise
    record("CHILD_RETURNING")


def test_completed_unit_os_lease_excludes_second_resumer_and_releases_after_exit(tmp_path):
    import json
    import multiprocessing
    import os
    import time

    parent_diagnostic = tmp_path / "lease-parent-diagnostic.jsonl"
    child_diagnostic = tmp_path / "lease-child-diagnostic.jsonl"
    def record(stage, **details):
        monotonic_ns, utc_ns = time.monotonic_ns(), time.time_ns()
        utc = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(utc_ns // 10**9))
        row = {"classification": "SYNTHETIC_LEASE_DIAGNOSTIC_ONLY", "phase": stage,
            "pid": os.getpid(), "monotonic_ns": monotonic_ns,
            "utc": utc + f".{utc_ns % 10**9:09d}Z", **details}
        with parent_diagnostic.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(row, sort_keys=True) + "\n")

    record("PARENT_ENTRY")
    context, body, binding, units = _completed_unit_store_fixture(tmp_path)
    spawn = multiprocessing.get_context("spawn")
    ready, release = spawn.Event(), spawn.Event()
    child = spawn.Process(target=_completed_unit_lease_child, args=(str(context.root),
        context.generation_id, context.native_universe_sha256, context.config_sha256,
        body, ready, release, str(child_diagnostic)))
    record("BEFORE_CHILD_START")
    child.start()
    record("CHILD_START_RETURNED", child_pid=child.pid)
    try:
        record("BEFORE_READY_WAIT", child_pid=child.pid)
        ready_result = ready.wait(15)
        record("READY_WAIT_COMPLETE", child_pid=child.pid, ready=ready_result,
            child_exitcode=child.exitcode, child_alive=child.is_alive())
        assert ready_result, "synthetic child never acquired the actual OS lease"
        with pytest.raises(execution.AuthoritativeSearchExecutionError, match="DECLARED_SLOT_RUN_ACTIVE"):
            with execution._declared_unit_lease(context, body):
                pytest.fail("two concurrent original-request resumers were admitted")
        assert execution._declared_completed_prefix(context, body, binding, units) == []
    finally:
        try:
            release.set()
        finally:
            try:
                child.join(20)
            finally:
                record("POST_JOIN", child_pid=child.pid, child_exitcode=child.exitcode,
                    child_alive=child.is_alive(), ready=ready.is_set(), release=release.is_set())
    assert child.exitcode == 0
    with execution._declared_unit_lease(context, body):
        rows, _ = execution._continue_declared_attempt_units(context, body, binding, units,
            _synthetic_completed_unit_value)
    assert len(rows) == 3
    lease = context.root / execution._declared_unit_relative(body, "units.lock")
    assert lease.read_bytes() == b"0"  # persistent, quota-accounted, never deleted/truncated


def test_completed_unit_os_lease_releases_on_exception(tmp_path):
    context, body, _, _ = _completed_unit_store_fixture(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        with execution._declared_unit_lease(context, body):
            raise KeyboardInterrupt("synthetic abrupt stack unwind")
    with execution._declared_unit_lease(context, body):
        pass


def test_saved_partial_units_refuse_missing_fit_loader_instead_of_rerunning_fit(tmp_path, monkeypatch):
    import pm_historical_search_producer as producer
    context, body, _, _ = _completed_unit_store_fixture(tmp_path)
    binding = {"generation_context_sha256": context.context_sha256,
        "producer_request_sha256": execution._sha256(body),
        "executor_release_sha256": execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        "producer_source_sha256": execution._file_sha256(
            execution.Path(producer.__file__))}
    header = {"schema_version": execution._DECLARED_UNIT_HEADER_SCHEMA,
        "binding": binding, "request_body": body,
        "qualification_authority": False, "live_authority": False}
    execution.authority_spool._publish_exact_json(context,
        execution._declared_unit_relative(body, "header.json"),
        header, context.limits.max_json_bytes)
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search",
        lambda **_kw: pytest.fail("completed fit was implicitly recalculated"))
    request = SimpleNamespace(to_dict=lambda: body)
    with pytest.raises(producer.HistoricalSearchProducerError,
            match="DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE"):
        producer._declared_saved_raw_completion(context, request=request,
            process=None, frozen_pipeline_config=None)


def _synthetic_bound_fit_header_locator(context, body):
    """Structural locator only: these invented bytes are NOT fit/source authority."""
    import pm_historical_search_producer as producer
    reference = {"synthetic_only": "NOT_A_VERIFIED_BOUND_FIT_SNAPSHOT"}
    return {"schema_version": execution._DECLARED_UNIT_HEADER_SCHEMA,
        "fit_state_origin_contract": "RECOMPUTED_OR_REOPENED_FROM_PRECOMMITTED_BOUND_STATE",
        "fitted_state_entry": reference, "plan_transport": {}, "horizon_plan": {},
        "binding": {"generation_context_sha256": context.context_sha256,
            "producer_request_sha256": execution._sha256(body),
            "executor_release_sha256": execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
            "producer_source_sha256": execution._file_sha256(execution.Path(producer.__file__)),
            "fitted_state_reference_sha256": execution._sha256(reference)},
        "request_body": body, "qualification_authority": False, "live_authority": False}


@pytest.mark.parametrize("mutation", ["legacy", "missing", "type", "changed_ref",
                                      "changed_hash", "contract", "plan", "horizon"])
def test_bound_fit_unit_header_refuses_incompatible_or_unbound_locators(tmp_path, monkeypatch, mutation):
    context, body, _, _ = _completed_unit_store_fixture(tmp_path)
    header = _synthetic_bound_fit_header_locator(context, body)
    expected = "DECLARED_UNIT_HEADER_BINDING_MISMATCH"
    if mutation == "legacy":
        header["schema_version"] = "fxpm.declared-attempt-unit-header.v1"
        expected = "DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE"
    elif mutation == "missing":
        del header["fitted_state_entry"]
        expected = "DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE"
    elif mutation == "type":
        header["fitted_state_entry"] = []
        expected = "DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE"
    elif mutation == "changed_ref":
        header["fitted_state_entry"]["changed"] = True
    elif mutation == "changed_hash":
        header["binding"]["fitted_state_reference_sha256"] = "f" * 64
    elif mutation == "contract":
        header["fit_state_origin_contract"] = "ALREADY_VERIFIED"
    elif mutation == "plan":
        del header["plan_transport"]
    else:
        del header["horizon_plan"]
    execution.authority_spool._publish_exact_json(context,
        execution._declared_unit_relative(body, "header.json"), header, context.limits.max_json_bytes)
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search",
        lambda **_kwargs: pytest.fail("a bad cold locator initiated fitting"))
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match=expected):
        execution._read_declared_unit_header(context, body)


def test_partial_v2_locator_is_neither_raw_completion_nor_a_source_capability(tmp_path, monkeypatch):
    import pm_historical_search_producer as producer
    context, body, _, _ = _completed_unit_store_fixture(tmp_path)
    header = _synthetic_bound_fit_header_locator(context, body)
    execution.authority_spool._publish_exact_json(context,
        execution._declared_unit_relative(body, "header.json"), header, context.limits.max_json_bytes)
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search",
        lambda **_kwargs: pytest.fail("a locator was treated as a fitted source"))
    request = SimpleNamespace(to_dict=lambda: body)
    # Only the actual Raw cold verifier can return a portable completed result.
    assert producer._declared_saved_raw_completion(context, request=request,
        process=None, frozen_pipeline_config=None) is None
    assert execution._read_declared_unit_header(context, body) == header


@pytest.mark.parametrize("changed", ["header", "validation", "actual_plan"])
def test_reopened_declared_plan_join_refuses_any_different_original_identity(changed):
    import pm_historical_search_producer as producer
    expected = "a" * 64
    cell = "c" * 64
    header = {"binding": {"horizon_plan_receipt_sha256": expected,
                          "cell_plan_receipt_sha256": cell}}
    validation = {"original_horizon_plan_receipt_sha256": expected,
                  "original_cell_plan_receipt_sha256": cell}
    plan = SimpleNamespace(receipt_sha256=cell,
                           horizon_plan=SimpleNamespace(receipt_sha256=expected))
    if changed == "header":
        header["binding"]["horizon_plan_receipt_sha256"] = "b" * 64
    elif changed == "validation":
        validation["original_cell_plan_receipt_sha256"] = "b" * 64
    else:
        plan.receipt_sha256 = "b" * 64
    with pytest.raises(producer.HistoricalSearchProducerError,
                       match="DECLARED_BOUND_FIT_ORIGINAL_PLAN_CHANGED"):
        producer._require_reopened_declared_plan(header, validation, plan)


def test_reopened_plan_join_is_a_non_authorising_equality_check():
    import pm_historical_search_producer as producer
    expected = "a" * 64
    cell = "c" * 64
    assert producer._require_reopened_declared_plan(
        {"binding": {"horizon_plan_receipt_sha256": expected,
                     "cell_plan_receipt_sha256": cell}},
        {"original_horizon_plan_receipt_sha256": expected,
         "original_cell_plan_receipt_sha256": cell},
        SimpleNamespace(receipt_sha256=cell,
                        horizon_plan=SimpleNamespace(receipt_sha256=expected))) is None


# INVENTED software wiring only. Source envelopes below are not native authentication.
def _invented_admission_executor_case():
    import numpy as np
    label = "INVENTED_ADMISSION_EXECUTOR_ENVELOPE_NOT_NATIVE_AUTHENTICATION"
    index = pd.date_range("2024-01-01T00:00:00Z", periods=7, freq="h")
    frame = pd.DataFrame({"Open": [1.1000, 1.1000, 1.0988, 1.1000, 1.1000, 1.1000, 1.1000],
                          "High": [1.1000, 1.1005, 1.0990, 1.1000, 1.1013, 1.1000, 1.1000],
                          "Low": [1.1000, 1.0995, 1.0985, 1.1000, 1.0997, 1.1000, 1.1000],
                          "Close": [1.1000, 1.1000, 1.0988, 1.1000, 1.1010, 1.1000, 1.1000],
                          "Volume": 100.0, "ATR_14": 0.0005, "SpreadOpen": 9, "SpreadMax": 12,
                          "REGIME_LIVE": "TREND", "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    frame.attrs["fixture_provenance"] = label
    observations = pd.Series(9.0, index=index)
    signals = pd.Series([1, 0, 0, 1, 0, 0, 0], index=index, dtype=np.int32)
    config = execution.PipelineConfig(initial_capital=10000.0, discovery_risk_per_trade_pct=1.0,
                                      risk_per_trade_pct=1.0, use_spread=True,
                                      backtest_spread_mode="conservative_max", swap_model_enabled=True,
                                      research_mode=True)
    spec = execution.InstrumentSpec("EURUSD", pip_position=4, pip_value=10.0, point=0.00001,
                                    tick_size=0.00001, tick_value=1.0, swap_long=-2.0, swap_short=1.0)
    tiers = {}
    for tier, points in (("P75", 9), ("P95", 16)):
        descriptor = {"schema_version": execution.spread_profile.TIER_SCHEMA, "symbol": "EURUSD",
                      "timeframe": "H1", "tier": tier, "source_timeframe": "H1", "point_price": spec.point,
                      "spread_points": points, "full_spread_price": points * spec.point,
                      "profile_sha256": execution._sha256([label, "profile"]),
                      "source_identity_sha256": execution._sha256([label, "source"]),
                      "source_binding": {"fixture_provenance": label}, "qualification_authority": False}
        tiers[tier] = execution.spread_profile.FrozenSpreadTier(execution._canonical_bytes(descriptor).decode("ascii"))
    swap = {"fixture_provenance": label, "contract_version": "causal_swap_accrual_v1",
            "status": "AVAILABLE", "provenance_status": "AUTHENTICATED",
            "source_provenance_authenticated": True, "historical_rates_authenticated": True,
            "execution_state_compounding": True, "timestamp_source": "_BAR_CLOSE_TS",
            "projection_contract": "fxpm-swap-preopen-close-projection-v1",
            "projection_sha256": execution._sha256([label, "projection"])}
    for name in ("price_activation_manifest_sha256", "terms_activation_manifest_sha256", "terms_snapshot_sha256",
                 "schedule_sha256", "target_bars_sha256", "target_sidecar_sha256", "historical_rates_sha256"):
        swap[name] = execution._sha256([label, name])

    class CountingStrategy:
        name = "INVENTED_ADMISSION_FIXED_STOPS"
        params = {}

        def __init__(self):
            self.stop_calls = 0

        def calculate_stops(self, *args, **kwargs):
            self.stop_calls += 1
            return 10.0, 10.0

    class InventedSourceBacktester(execution.pm_core.Backtester):
        def __init__(self, supplied_config):
            super().__init__(supplied_config)
            self.binding_calls = []
            self.results = []
            self.fail_run = False

        def _partition_entry_admission_reuse(self, preparation):
            self.binding_calls.append(preparation)
            return super()._partition_entry_admission_reuse(preparation)

        def run(self, *args, **kwargs):
            if self.fail_run:
                raise RuntimeError("INVENTED_BACKTEST_FAILURE")
            result = super().run(*args, **kwargs)
            # Only the test provenance envelope is invented; cost validators stay real.
            result["fixture_provenance"] = label
            result["swap_accrual_contract"] = copy.deepcopy(swap)
            self.results.append(result)
            return result

    ordinary = execution.pm_core.Backtester(config)
    seeds = {tier: ordinary.build_evidence_source_seed_result(
        frame, "EURUSD", spec=spec, timeframe="H1", spread_tier=bound,
        entry_spread_observations=observations) for tier, bound in tiers.items()}
    for seed in seeds.values():
        seed["swap_accrual_contract"] = copy.deepcopy(swap)
    sources = {tier: execution.raw_winner.build_trade_source_contract_from_backtester_result(seed)
               for tier, seed in seeds.items()}
    identities = {tier: execution._source_identity(source) for tier, source in sources.items()}
    pair = execution.pm_core.build_backtest_frozen_tier_pair_contract(seeds, spec=spec, use_spread=True)
    partitions = {}
    rows, references = [], {}
    for role in ("optimizer_train", "tuning_validation", "final_lockbox"):
        geometry = {"receipt_sha256": execution._sha256([label, role, "geometry"]),
                    "frame_sha256": execution._sha256([label, role, "frame"])}
        partitions[role] = execution._PartitionSlice(role=role, frame=frame, warmup_bars=0,
            scored_start_utc=index[0].isoformat(), end_exclusive_utc=(index[-1] + pd.Timedelta(hours=1)).isoformat(),
            geometry_receipt=geometry)
        source_observation = execution._sha256([label, role, "observation"])
        references[geometry["receipt_sha256"]] = {"source_observation_receipt_sha256": source_observation}
        for ordinal in (0, 1):
            rows.append({"attempt_ordinal": ordinal, "fold_ordinal": 0, "partition": role,
                         "partition_geometry_receipt_sha256": geometry["receipt_sha256"],
                         "partition_frame_sha256": geometry["frame_sha256"],
                         "source_observation_receipt_sha256": source_observation,
                         "tiers": {tier: {"numerical_execution_cost_attestation": seed["numerical_execution_cost_attestation"],
                                          "entry_observations_sha256": seed["spread_admission"]["entry_observations_sha256"],
                                          "entry_admission_mask_sha256": seed["spread_admission"]["entry_admission_mask_sha256"]}
                                   for tier, seed in seeds.items()}})
    precommitments = execution.raw_winner.build_partition_cost_precommitments(rows, pair=pair)
    fitted = SimpleNamespace(_frame=frame, source_contract_seeds=sources, frozen_tier_pair_contract=pair,
                             _spread_tiers=tiers, _entry_spread_observations=observations, instrument_spec=spec)
    surface = execution._FullPrefixStrategySurface(features=frame, raw_signals=signals)
    return SimpleNamespace(label=label, frame=frame, observations=observations, config=config, spec=spec,
                           tiers=tiers, identities=identities, partitions=partitions, fitted=fitted,
                           pair=pair, references=references, precommitments=precommitments, surface=surface,
                           Strategy=CountingStrategy, Backtester=InventedSourceBacktester)


def _admission_executor_counter():
    from contextlib import contextmanager
    import inspect
    import sys
    codes = {"scalar": execution.pm_core.spread_admission.__code__,
             "run": inspect.unwrap(execution.pm_core.Backtester.run).__code__,
             "cost": execution._verify_result_cost_attestation.__code__,
             "source": execution.raw_winner.build_trade_source_contract_from_backtester_result.__code__,
             "precommit": execution._verify_partition_precommitment.__code__}

    @contextmanager
    def observe():
        counts = {name: 0 for name in codes}
        previous = sys.getprofile()

        def profile(frame, event, arg):
            if event == "call":
                for name, code in codes.items():
                    if frame.f_code is code:
                        counts[name] += 1

        sys.setprofile(profile)
        try:
            yield counts
        finally:
            sys.setprofile(previous)
    return observe()


def _admission_executor_run(case, backtester, strategy, role, preparation=None):
    kwargs = dict(partition=case.partitions[role], full_prefix_surface=case.surface, backtester=backtester,
                  strategy=strategy, symbol="EURUSD", timeframe="H1", regime="TREND", config=case.config,
                  spec=case.spec, expected_source_contract_sha256=case.identities["P75"],
                  spread_tier=case.tiers["P75"], entry_spread_observations=case.observations)
    if preparation is not None:
        kwargs["_entry_admission_preparation"] = preparation
    return execution._run_tier_partition(**kwargs)


@pytest.mark.parametrize("role", ["optimizer_train", "tuning_validation"])
def test_entry_admission_partition_real_callbacks_and_validation_repeat(role):
    case = _invented_admission_executor_case()
    backtester, strategy = case.Backtester(case.config), case.Strategy()
    with execution.pm_core._EntryAdmissionPreparation(case.observations) as preparation:
        with _admission_executor_counter() as counts:
            first = _admission_executor_run(case, backtester, strategy, role, preparation)
            stop_calls = strategy.stop_calls
            second = _admission_executor_run(case, backtester, strategy, role, preparation)
        assert counts["run"] == counts["cost"] == counts["source"] == 2
        assert counts["scalar"] == 1  # Identical native bits share one prepared scalar result.
        assert stop_calls > 0 and strategy.stop_calls == 2 * stop_calls
        assert backtester.binding_calls == [preparation, preparation]
        assert "_entry_admission_preparation" not in backtester.__dict__
        assert preparation._labels_by_tier
    assert first[0]["trades"] == second[0]["trades"]
    assert first[0]["spread_admission"] == second[0]["spread_admission"]
    assert first[0]["numerical_execution_cost_attestation"] == second[0]["numerical_execution_cost_attestation"]
    assert first[0]["numerical_execution_cost_attestation"] is not second[0]["numerical_execution_cost_attestation"]


def test_entry_admission_opted_final_lockbox_suppresses_and_restores_outer_binding():
    case = _invented_admission_executor_case()
    backtester, strategy = case.Backtester(case.config), case.Strategy()
    with execution.pm_core._EntryAdmissionPreparation(case.observations) as outer:
        with execution.pm_core._EntryAdmissionPreparation(case.observations) as inner:
            execution.pm_core.Backtester._entry_spread_admission_inputs(
                case.frame, case.tiers["P75"], case.observations, _preparation=outer)
            with backtester._partition_entry_admission_reuse(outer):
                with _admission_executor_counter() as counts:
                    _admission_executor_run(case, backtester, strategy, "final_lockbox", inner)
                assert counts["scalar"] == len(case.observations)
                assert backtester._entry_admission_preparation is outer
                assert backtester.binding_calls[-1] is None
                assert inner._labels_by_tier == {}
            assert outer._labels_by_tier
    assert "_entry_admission_preparation" not in backtester.__dict__


def test_entry_admission_default_partition_does_not_require_binding_api():
    case = _invented_admission_executor_case()
    backtester, strategy = case.Backtester(case.config), case.Strategy()
    with _admission_executor_counter() as counts:
        returned = _admission_executor_run(case, backtester, strategy, "optimizer_train")
    assert returned[3] is backtester
    assert backtester.binding_calls == []
    assert counts["run"] == counts["cost"] == counts["source"] == 1
    assert counts["scalar"] == len(case.observations)


def test_entry_admission_partition_exception_restores_outer_binding():
    case = _invented_admission_executor_case()
    backtester, strategy = case.Backtester(case.config), case.Strategy()
    backtester.fail_run = True
    with execution.pm_core._EntryAdmissionPreparation(case.observations) as outer:
        with execution.pm_core._EntryAdmissionPreparation(case.observations) as inner:
            with backtester._partition_entry_admission_reuse(outer):
                with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
                    _admission_executor_run(case, backtester, strategy, "optimizer_train", inner)
                assert caught.value.code == "BACKTEST_PARTITION_EXECUTION_FAILED"
                assert backtester._entry_admission_preparation is outer
                assert inner._labels_by_tier == {}
    assert "_entry_admission_preparation" not in backtester.__dict__


def test_entry_admission_shared_pool_across_planned_attempts_keeps_real_checks(monkeypatch):
    case = _invented_admission_executor_case()
    backtesters, strategies = [], []

    def runtime_strategy(_attempt):
        strategy = case.Strategy()
        strategies.append(strategy)
        return strategy, {}

    def backtester(config):
        instance = case.Backtester(config)
        backtesters.append(instance)
        return instance

    # Only upstream feature/fold/provenance carriers are invented. Validators remain real.
    monkeypatch.setattr(execution, "_runtime_strategy", runtime_strategy)
    monkeypatch.setattr(execution, "Backtester", backtester)
    monkeypatch.setattr(execution, "_build_full_prefix_strategy_surface", lambda **kwargs: case.surface)
    monkeypatch.setattr(execution, "_causal_folds", lambda attempt: ({"fixture_provenance": case.label},))
    monkeypatch.setattr(execution, "_fold_partitions", lambda **kwargs: tuple(case.partitions.values()))
    monkeypatch.setattr(execution, "_fold_provenance", lambda **kwargs: {"fixture_provenance": case.label})
    plan = {"cell": {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"},
            "derived_dependency_geometry": {"warmup_bars": 0}}
    outputs = []
    with execution.pm_core._EntryAdmissionPreparation(case.observations) as preparation:
        with _admission_executor_counter() as counts:
            for ordinal in (0, 1):
                outputs.append(execution._build_planned_attempt_evidence(
                    attempt=SimpleNamespace(ordinal=ordinal, plan_attempt={"status": "PLANNED"}),
                    raw_attempt_id="INVENTED_ATTEMPT_" + str(ordinal), plan_body=plan,
                    plan_receipt_sha256=execution._sha256([case.label, "plan"]), fitted=case.fitted,
                    config=case.config, partition_cost_precommitments=case.precommitments,
                    source_observation_references=case.references,
                    _entry_admission_preparation=preparation))
        assert counts["run"] == counts["cost"] == counts["source"] == counts["precommit"] == 12
        assert counts["scalar"] == 2 + 4 * len(case.observations)  # Two representatives; final paths stay rowwise.
        assert len(backtesters) == len(strategies) == 2
        for instance, strategy in zip(backtesters, strategies):
            assert len(instance.results) == 6 and strategy.stop_calls > 0
            assert instance.binding_calls == [preparation, preparation, preparation, preparation, None, None]
            assert "_entry_admission_preparation" not in instance.__dict__
            assert len({id(result["numerical_execution_cost_attestation"]) for result in instance.results}) == 6
        assert preparation._labels_by_tier
    assert preparation._closed and preparation._labels_by_tier == {}
    assert [output["attempt_id"] for output in outputs] == ["INVENTED_ATTEMPT_0", "INVENTED_ATTEMPT_1"]


"""INVENTED SOFTWARE-ISSUER FIXTURE FRAGMENT; SOURCE ONLY, NOT EXECUTED.

This is the synchronous observation seam, not a completed positive fixture.
The caller must first construct the coherent owned-fresh inputs listed in the
metadata companion. No market lineage, qualification or trading authority is claimed.
"""


def _observe_invented_issued_declared_header(monkeypatch, *, owned_root,
        lifecycle_root, context, precommit, process, frozen_pipeline_config, cell_id, slot_id,
        observe):
    """Call the real issuer and assert inside its live post-header boundary.

    `observe` must make all adapter assertions synchronously and return None.
    It must not retain capture, fitted, handle or request for use after this call.
    The two observer wrappers delegate header construction and interrupt only
    the downstream attempt loop; no issuer or authentication verifier is replaced.
    """
    from pathlib import Path
    import pm_authority_spool as spool
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_historical_production_adapter as adapter
    import pm_production_search_planner as planner
    import pm_authoritative_search_execution as execution

    _require_invented_fixture_real_clocks()
    owned_root = Path(owned_root).resolve(strict=True)
    lifecycle_root = Path(lifecycle_root).resolve(strict=True)
    assert lifecycle_root != owned_root and lifecycle_root.is_relative_to(owned_root)
    assert context.root.resolve().is_relative_to(owned_root)
    assert frozen_pipeline_config.active_config_path.resolve().is_relative_to(owned_root)
    assert context.root.resolve().is_relative_to(lifecycle_root)
    assert frozen_pipeline_config.active_config_path.resolve().is_relative_to(lifecycle_root)
    materialized = frozen_pipeline_config.materialize(context)
    assert Path(materialized.data_dir).resolve().is_relative_to(owned_root)
    committed = orchestration.verify_declared_slot_owner_run_precommit(
        context, precommit, process=process)
    cells = [cell for cell in committed["cells"] if cell["cell_id"] == cell_id]
    assert len(cells) == 1
    sink = spool.open_symbol_sink(context, cells[0]["symbol"])
    request = orchestration.declared_slot_producer_request(
        context, precommit=precommit, process=process, cell_id=cell_id,
        slot_id=slot_id, attempt_id="invented-route-a-observer", sink=sink)
    assert execution._read_declared_unit_header(context, request.to_dict()) is None
    original_header = execution._make_declared_unit_header
    saved, failure, reached = {}, [], []

    class _ObservedBoundary(BaseException):
        pass

    def observe_header(*args, **kwargs):
        header = original_header(*args, **kwargs)
        assert not kwargs and len(args) == 8 and not saved
        saved.update(capture=args[0], refreshed=args[1], cell_plan=args[2],
                     attempts=args[3], fold_plan=args[4], handle=args[5],
                     constructed_header=header)
        return header

    def observe_attempt_boundary(actual_context, body, binding_sha256, units, produce):
        try:
            assert not reached and actual_context is context
            assert body == request.to_dict() and saved
            capture = saved["capture"]
            assert capture.context is context and capture.request is request
            assert capture.process is process and capture.cell_plan is saved["cell_plan"]
            assert adapter._verify_declared_slot_capture(capture) is True
            planner.verify_production_cell_search_plan(context, process=process,
                sink=capture.sink, plan=capture.cell_plan)
            header = execution._read_declared_unit_header(context, request.to_dict())
            assert header == saved["constructed_header"]
            assert execution._sha256(header["binding"]) == binding_sha256
            assert observe(capture=capture, fitted=capture.verified_fitted_timeframe,
                search_process=process, cell_plan=capture.cell_plan,
                config=capture.verified_pipeline_config, handle=saved["handle"],
                declared_header=header, attempts=saved["attempts"],
                fold_plan=saved["fold_plan"], units=units) is None
            reached.append(True)
        except Exception as exc:
            failure.append(exc)
        # Preserve assertion failures across the issuer's handled-technical-STOP
        # conversion, and stop before unrelated attempts can call produce().
        raise _ObservedBoundary()

    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(execution, "_make_declared_unit_header", observe_header)
            scoped.setattr(execution, "_continue_declared_attempt_units", observe_attempt_boundary)
            try:
                completion = producer.execute_declared_slot_owner_search(
                    context, request=request, process=process,
                    frozen_pipeline_config=frozen_pipeline_config)
            except _ObservedBoundary:
                pass
            else:
                raise AssertionError("INVENTED fixture did not reach the issued post-header boundary; "
                                     "inspect the actual completion without treating it as positive proof")
    finally:
        # The real producer has already removed its source session in finally.
        orchestration._release_declared_slot_producer_request(request, context)
    if failure:
        raise failure[0]
    assert reached == [True]
    return {"fixture_provenance": "INVENTED_SOFTWARE_ISSUER_ASSERTIONS_ONLY",
            "live_boundary_observed": True, "qualification_authority": False,
            "capital_authority": False, "live_order_authority": False}



def _prepare_owned_invented_native_source(tmp_path):
    """Generate one fresh source tree/profile; retain paths, not authority objects."""
    import json
    from pathlib import Path
    from types import SimpleNamespace
    import pm_spread_profile as spread_profile
    from tests.test_historical_production_adapter import _current_v5_native_prefixes

    def encode(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode("ascii")

    _require_invented_fixture_real_clocks()
    owned_root = Path(tmp_path).resolve(strict=True) / "invented-owned-route-a"
    assert not owned_root.exists(), "Generate a fresh owned source; never reuse old QA inputs"
    owned_root.mkdir()
    foundation, fine = _current_v5_native_prefixes(
        owned_root / "source", selected_owner=False, retained_native_root=None)
    data_dir = Path(foundation._data_dir).resolve(strict=True)
    assert data_dir.is_relative_to(owned_root)
    symbol = foundation.native_prefix["symbol"]
    assert foundation.native_prefix["timeframe"] == "D1"
    assert fine.native_prefix["symbol"] == symbol and fine.native_prefix["timeframe"] == "M5"
    fine_source = fine.index["source"]
    point = float(foundation.instrument_snapshot["point"])
    profile_body = {
        "schema_version": spread_profile.PROFILE_SCHEMA,
        "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "production_authority": False,
        "fixture_provenance": "INVENTED_SOFTWARE_ISSUER_FIXTURE_NO_MARKET_CALIBRATION_OR_TRADING_AUTHORITY",
        "activation": {"id": foundation.index["activation"]["activation_id"],
                       "manifest_sha256": foundation.index["activation"]["manifest_sha256"]},
        "protected_cutoff_utc": foundation.index["cutoff_utc"],
        "cells": [{"symbol": symbol, "timeframe": "D1", "source_timeframe": "M5",
            "status": "CALCULATED", "point_price": point,
            "source_identity": {"identity_sha256": fine_source["source_identity_sha256"]},
            "source_binding": {"bars_sha256": fine_source["bars_sha256"],
                "sidecar_sha256": fine_source["audit_sidecar_sha256"],
                "provenance_sha256": fine_source["provenance_sha256"],
                "target_bars_sha256": foundation.index["source"]["bars_sha256"]},
            "tiers": {tier: {"usability": "POSITIVE_QUANTILE", "spread_points": points,
                "full_spread_price": points * point, "half_spread_price": points * point / 2}
                for tier, points in (("P75", 9), ("P95", 16))}}],
    }
    profile_path = data_dir / "native" / "terms" / "spread_profile.json"
    assert profile_path.resolve().is_relative_to(owned_root) and not profile_path.exists()
    profile_path.write_bytes(encode(profile_body))
    profile = spread_profile.load_spread_profile(profile_path)
    # Share only newly generated immutable input paths/basic metadata. Neither
    # source products nor their generation context/receipts enter a later case.
    return SimpleNamespace(owned_root=owned_root, data_dir=data_dir, symbol=symbol,
        cutoff_utc=foundation.index["cutoff_utc"], profile_path=profile_path,
        fixture_provenance="FRESH_MODULE_OWNED_INVENTED_SOURCE_NO_RETAINED_QA_REUSE")


def _reopen_invented_native_source_for_new_issuers(tmp_path, native_root):
    """Reuse only completed synthetic raw inputs; allocate every issuer in a new root."""
    import json
    from pathlib import Path
    from types import SimpleNamespace
    from tests import test_historical_production_adapter as fixture

    _require_invented_fixture_real_clocks()
    native_root = Path(native_root).resolve(strict=True)
    creation_path = native_root / "synthetic_fixture_creation.json"
    record = json.loads(creation_path.read_bytes())
    helper_path = Path(fixture.__file__).with_name("native_v5_support.py")
    inputs = fixture._current_native_generator_inputs(selected_owner=False)
    fixture._validated_current_native_creation(record, native_root=native_root,
        helper_sha256=hashlib.sha256(helper_path.read_bytes()).hexdigest(), generator_inputs=inputs)
    data_dir = native_root.parent
    profile_path = (native_root / "terms" / "spread_profile.json").resolve(strict=True)
    assert profile_path.is_relative_to(native_root)
    owned_root = Path(tmp_path).resolve(strict=True) / "invented-new-issuers-reused-inputs"
    assert not owned_root.exists(), "Every reuse observation needs a new issuer workspace"
    owned_root.mkdir()
    return SimpleNamespace(owned_root=owned_root, data_dir=data_dir,
        symbol=inputs["write_v5_run"]["dense_symbol"],
        cutoff_utc=inputs["write_v5_run"]["information_cutoff_utc"], profile_path=profile_path,
        reused_native_root=native_root, source_inputs_reused=True,
        source_creation_record=creation_path,
        fixture_provenance="REUSED_IMMUTABLE_SYNTHETIC_RAW_INPUTS_NEW_ISSUERS_NO_FINANCIAL_AUTHORITY")


def _build_owned_invented_declared_inputs(tmp_path, *, source_access_window_utc,
        regime="TREND", native_fixture=None):
    """Compose SOFTWARE-issued inputs from explicitly classified INVENTED sources.

    The caller supplies an explicit fixture-only access window containing the
    ordinary current UTC time. This function changes no clock or issuer policy.
    It does not execute a strategy or call Backtester.run. The real producer will
    re-produce/reverify/fit against these owned bytes inside the live observer.
    The optional module fixture shares raw source bytes only; every context, process,
    precommit, producer request, fit and capture/header lifecycle remains fresh.
    """
    import json
    from pathlib import Path
    from types import SimpleNamespace
    import pm_authority_spool as spool
    import pm_historical_anchor_orchestration as orchestration
    import pm_portfolio_replay as portfolio
    import pm_production_anchor_plan as anchor_plan
    import pm_production_config_binding as binding
    import pm_production_search_planner as planner
    import pm_spread_profile as spread_profile
    import pm_book_multiplicity as book
    import pm_winner_selection as winner
    import pm_native_prefix_production as native_prefix
    from tests.prototype_support import _execution_config, _roster_arguments

    def encode(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode("ascii")

    _require_invented_fixture_real_clocks()
    if native_fixture is None:
        native_fixture = _prepare_owned_invented_native_source(tmp_path)
    owned_root = Path(native_fixture.owned_root).resolve(strict=True)
    data_dir = Path(native_fixture.data_dir).resolve(strict=True)
    if getattr(native_fixture, "source_inputs_reused", False):
        assert (data_dir / "native").resolve(strict=True) == native_fixture.reused_native_root
    else:
        assert data_dir.is_relative_to(owned_root)
    lifecycle_root = owned_root / "lifecycles" / Path(tmp_path).name
    assert not lifecycle_root.exists(), "Each observation requires a fresh lifecycle subtree"
    lifecycle_root.mkdir(parents=True)
    assert lifecycle_root.resolve(strict=True).is_relative_to(owned_root)
    symbol = native_fixture.symbol
    assert regime in book.REGIME_ORDER

    config_path = _execution_config(lifecycle_root / "config", symbol=symbol)
    payload = json.loads(config_path.read_bytes())
    payload.update(symbols=[symbol], position={}, storage={})
    payload["pipeline"] = {
        "research_mode": True, "initial_capital": 10000.0,
        "discovery_risk_per_trade_pct": 1.0, "risk_per_trade_pct": 0.25,
        "data_dir": str(data_dir), "output_dir": str(lifecycle_root / "outputs"),
        "log_dir": str(lifecycle_root / "logs"), "backtest_spread_mode": "conservative_max",
        "oos_validation_mode": "fixed_holdout", "false_discovery_symbol_count": 1,
    }
    config_path.write_bytes(encode(payload))
    owned_payload, effective = binding.load_effective_pipeline_config(config_path)
    snapshot, errors = portfolio.active_native_portfolio_contract(
        data_dir, book.PINNED_SYMBOL_ORDER, timeframes=book.TIMEFRAME_ORDER)
    assert not errors
    context = spool.freeze_generation_context(
        (lifecycle_root / "declared-spool").resolve(), generation_id="invented-issued-route-a",
        native_universe_sha256=portfolio.canonical_json_sha256(snapshot),
        config_sha256=portfolio.canonical_json_sha256(owned_payload))
    assert orchestration._declared_slot_native_snapshot_status(context, snapshot, errors) == "MATCH"
    # Fresh metadata-only spool preserves the original separation between the
    # prefix preparation context and the producer's independently owned context.
    # No generation product/receipt is reused or pre-registered in the producer spool.
    metadata_context = spool.freeze_generation_context(
        (lifecycle_root / "metadata-prefix-spool").resolve(),
        generation_id="invented-route-a-fresh-prefix-metadata",
        native_universe_sha256=portfolio.canonical_json_sha256(snapshot),
        config_sha256=portfolio.canonical_json_sha256(owned_payload))
    foundation = native_prefix.produce_activated_native_prefix(data_dir=data_dir,
        context=metadata_context, symbol=symbol, timeframe="D1", cutoff_utc=native_fixture.cutoff_utc,
        source_accessed_at_utc="2025-01-01T00:00:00Z", production_contract_required=True)
    assert foundation.source_state == native_prefix.AVAILABLE
    assert orchestration._declared_slot_consumed_native_snapshot_matches(snapshot, foundation)
    frozen = binding.freeze_production_pipeline_config(context,
        active_config_path=config_path, config_payload=owned_payload, supplied_config=effective)

    profile_path = Path(native_fixture.profile_path).resolve(strict=True)
    assert profile_path.is_relative_to(data_dir)
    profile = spread_profile.load_spread_profile(profile_path)
    run_id = "invented-issued-route-a"
    process = planner.freeze_production_search_process(context, run_id=run_id)
    roster_arguments = _roster_arguments(context.config_sha256, frozen.effective_config_sha256)
    roster = anchor_plan.derive_declared_owner_slot_roster(**roster_arguments)
    cell_id = winner.compute_cell_id(symbol, "D1", regime)
    precommit = orchestration.precommit_declared_slot_owner_run(context,
        run_id=run_id, process=process, slot_roster=roster,
        launch_activation_utc=roster_arguments["launch_activation_utc"],
        cell_roster=[{"cell_id": cell_id, "symbol": symbol, "timeframe": "D1", "regime": regime}],
        frozen_pipeline_config=frozen, spread_profile_identity={"profile_sha256": profile.sha256},
        source_access_window_utc=dict(source_access_window_utc))
    return SimpleNamespace(owned_root=owned_root, lifecycle_root=lifecycle_root, context=context, precommit=precommit,
        process=process, frozen_pipeline_config=frozen, cell_id=cell_id,
        slot_id=roster["slots"][-1]["slot_id"], profile_path=profile_path,
        fixture_provenance="INVENTED_SOFTWARE_ISSUER_INPUTS_NO_MARKET_OR_GLOBAL_AUTHORITY")



def _require_invented_fixture_real_clocks():
    from datetime import datetime
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_authoritative_search_execution as execution
    assert orchestration.datetime is producer.datetime is execution.datetime is datetime, (
        "Use ordinary UTC providers; do not invoke the fixed-clock _declared_environment fixture")


def _invented_fixture_external_io_guard(monkeypatch):
    """Block attempted external I/O, not software/data authentication."""
    from contextlib import contextmanager

    @contextmanager
    def guarded():
        import builtins
        import os
        import subprocess
        import sys
        attempts = []

        def deny(action):
            def blocked(*args, **kwargs):
                attempts.append(action)
                raise AssertionError("INVENTED fixture attempted forbidden external I/O: " + action)
            return blocked

        with monkeypatch.context() as scoped:
            original_import = builtins.__import__

            def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
                requested_imports = [name] + [name + "." + item for item in (fromlist or ())
                                                if isinstance(item, str)]
                if any("real_control" in part.casefold() for request_name in requested_imports
                       for part in request_name.split(".")):
                    return deny("real_control import")()
                if name in {"MetaTrader5", "pm_mt5"} and name not in sys.modules:
                    return deny("unloaded connector import: " + name)()
                return original_import(name, globals, locals, fromlist, level)

            mt5 = sys.modules.get("MetaTrader5")
            if mt5 is not None:
                for name in ("initialize", "login", "shutdown", "copy_rates_range", "copy_rates_from",
                             "copy_ticks_range", "symbol_info", "symbol_info_tick", "account_info",
                             "terminal_info", "symbols_get"):
                    if hasattr(mt5, name):
                        scoped.setattr(mt5, name, deny("MetaTrader5." + name))
            connector_module = sys.modules.get("pm_mt5")
            connector = getattr(connector_module, "MT5Connector", None)
            if connector is not None and hasattr(connector, "connect"):
                scoped.setattr(connector, "connect", deny("MT5Connector.connect"))
            scoped.setattr(subprocess, "Popen", deny("subprocess.Popen"))
            scoped.setattr(os, "system", deny("os.system"))
            if hasattr(os, "startfile"):
                scoped.setattr(os, "startfile", deny("os.startfile"))
            browser = sys.modules.get("webbrowser")
            if browser is not None:
                for name in ("open", "open_new", "open_new_tab"):
                    scoped.setattr(browser, name, deny("webbrowser." + name))
            scoped.setattr(builtins, "__import__", guarded_import)
            try:
                yield attempts
            finally:
                assert not attempts, "Blocked external I/O was attempted: " + repr(attempts)

    return guarded()


def _run_owned_invented_issued_observation(tmp_path, monkeypatch, *,
        source_access_window_utc, observe, regime="TREND", native_fixture=None):
    """Required guarded composition entrypoint for the proposed positive test."""
    with _invented_fixture_external_io_guard(monkeypatch) as external_io_attempts:
        env = _build_owned_invented_declared_inputs(tmp_path,
            source_access_window_utc=source_access_window_utc, regime=regime,
            native_fixture=native_fixture)
        result = _observe_invented_issued_declared_header(monkeypatch,
            owned_root=env.owned_root, lifecycle_root=env.lifecycle_root,
            context=env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen_pipeline_config,
            cell_id=env.cell_id, slot_id=env.slot_id, observe=observe)
        assert not external_io_attempts
        return result

"""INVENTED Route A software-issuer checks; no market or financial evidence."""


@pytest.fixture(scope="module")
def _route_a_owned_invented_native_source(tmp_path_factory):
    """Classify fresh or explicitly reused raw bytes; later issuers are always new."""
    import os
    with pytest.MonkeyPatch.context() as scoped:
        with _invented_fixture_external_io_guard(scoped) as external_io_attempts:
            reused = os.environ.get("FXPM_QA_REUSED_INVENTED_NATIVE_ROOT")
            if reused:
                source = _reopen_invented_native_source_for_new_issuers(
                    tmp_path_factory.mktemp("invented-route-a-reused-inputs"), reused)
            else:
                source = _prepare_owned_invented_native_source(
                    tmp_path_factory.mktemp("invented-route-a-module-source"))
            assert not external_io_attempts
            return source


def _route_a_owned_utc_window():
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    return {"start_utc": (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_exclusive_utc": (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")}


def _route_a_callback_identity_dispatch(code_pairs):
    by_identity = {}
    for name, code in code_pairs:
        by_identity.setdefault(id(code), []).append((name, code))
    by_identity = {identity: tuple(matches) for identity, matches in by_identity.items()}

    def labels(frame_code, event):
        if event != "call":
            return ()
        matches = by_identity.get(id(frame_code), ())
        if not matches:
            return ()
        return tuple(name for name, code in matches if frame_code is code)

    return labels


def _route_a_recorded_calls():
    from collections import Counter, defaultdict
    from contextlib import contextmanager
    import sys

    @contextmanager
    def recorded():
        codes = {name: function.__code__ for name, function in (
            ("model_run", execution.Backtester.run),
            ("runtime_strategy", execution._runtime_strategy),
            ("partition", execution._run_tier_partition),
            ("surface", execution._build_full_prefix_strategy_surface),
            ("probe", execution.Backtester.build_evidence_source_seed_result),
            ("cost", execution._verify_result_cost_attestation),
            ("source", execution.raw_winner.build_trade_source_contract_from_backtester_result),
            ("precommit", execution._verify_partition_precommitment),
            ("pool_lookup", execution.pm_core._EntryAdmissionPreparation._states_for))}
        callback_labels = _route_a_callback_identity_dispatch(codes.items())
        counts, lane_counts = Counter(), defaultdict(Counter)
        callbacks, surface_callbacks = defaultdict(list), []
        lanes, surfaces, outputs, instances = [], [], {}, []

        def lane_key(values):
            partition = values.get("partition")
            if partition is None:
                return None
            tier = values.get("tier")
            if tier is None:
                tier = values["spread_tier"].tier
            return partition.geometry_receipt["receipt_sha256"], tier

        def profile(frame, event, argument):
            if event not in {"call", "return"}:
                return
            if event == "call":
                for name in callback_labels(frame.f_code, event):
                    counts[name] += 1
                    current = lane_key(frame.f_locals) if name == "precommit" else (
                        lanes[-1][1] if lanes else None)
                    if current is not None:
                        lane_counts[current][name] += 1
                if frame.f_code is codes["surface"]:
                    surfaces.append((frame, frame.f_locals["strategy"]))
                if frame.f_code is codes["partition"]:
                    key = lane_key(frame.f_locals)
                    lanes.append((frame, key, frame.f_locals["strategy"]))
                    instances.append((frame.f_locals["strategy"], frame.f_locals["backtester"],
                                      frame.f_locals.get("_entry_admission_preparation")))
                if frame.f_code.co_name in {"generate_signals", "calculate_stops", "build_trade_intent"}:
                    owner = frame.f_locals.get("self")
                    if lanes and owner is lanes[-1][2]:
                        callbacks[lanes[-1][1]].append(frame.f_code.co_name)
                    elif surfaces and owner is surfaces[-1][1]:
                        surface_callbacks.append(frame.f_code.co_name)
            elif event == "return":
                if lanes and frame is lanes[-1][0]:
                    key = lanes[-1][1]
                    if argument is not None:
                        outputs[key] = {"decision": copy.deepcopy(frame.f_locals["decision_result_out"]),
                                        "signals": argument[2].copy(deep=True)}
                    lanes.pop()
                if surfaces and frame is surfaces[-1][0]:
                    surfaces.pop()

        previous = sys.getprofile()
        sys.setprofile(profile)
        try:
            yield SimpleNamespace(counts=counts, lane_counts=lane_counts, callbacks=callbacks,
                surface_callbacks=surface_callbacks, outputs=outputs, instances=instances)
        finally:
            sys.setprofile(previous)

    return recorded()


def _route_a_unambiguous_issued_attempt(witness):
    """Choose by issued metadata only, before any model result or score exists."""
    from collections import Counter
    plan = witness["cell_plan"].horizon_plan.to_dict()
    counts, candidates = Counter(), []
    roles = ("optimizer_train", "tuning_validation")
    for attempt in witness["attempts"]:
        strategy_id = attempt.artifacts.winner_process["strategy_id"]
        if attempt.plan_attempt["status"] != "PLANNED" or strategy_id != "EMACrossoverStrategy":
            continue
        parameters = execution.decode_parameter_mapping(attempt.artifacts.trial_definition["parameters"],
            execution.StrategyRegistry.get(strategy_id).get_default_params())
        folds, keys = execution._causal_folds(attempt), {}
        for ordinal, fold in enumerate(folds):
            partitions = execution._fold_partitions(frame=witness["refreshed"]._frame,
                fold=fold, attempt=attempt,
                dependency_warmup_bars=plan["derived_dependency_geometry"]["warmup_bars"])
            for partition in partitions:
                if partition.role not in roles:
                    continue
                key = execution._canonical_bytes({"strategy_id": strategy_id,
                    "parameters": parameters, "fold_ordinal": ordinal, "fold_id": fold["fold_id"],
                    "partition_geometry": partition.geometry_receipt})
                counts[key] += 1
                keys[ordinal, partition.role] = key
        candidates.append((attempt, folds, keys))
    eligible = [item for item in candidates
        if len(item[1]) >= 2 and item[1][0]["fold_id"] != item[1][1]["fold_id"]
        and all((ordinal, role) in item[2] and counts[item[2][ordinal, role]] == 1
                for ordinal in range(2) for role in roles)]
    assert eligible, "INVENTED fixture needs one unambiguous issued two-fold EMA key"
    # Canonical authenticated parameter/fold/geometry bytes, never bookkeeping or ranking.
    selected = min(eligible, key=lambda item: tuple(
        item[2][ordinal, role] for ordinal in range(2) for role in roles))
    return selected[0], selected[1]


def _route_a_batch_specification(witness, attempt, *, tier,
        role="optimizer_train", fold_ordinal=0):
    """INVENTED software input; the production owner derives every authority binding."""
    strategy_id = attempt.artifacts.winner_process["strategy_id"]
    parameters = execution.decode_parameter_mapping(attempt.artifacts.trial_definition["parameters"],
        execution.StrategyRegistry.get(strategy_id).get_default_params())
    return {"strategy_id": strategy_id,
        "parameters": parameters, "fold_ordinal": fold_ordinal,
        "partition_role": role, "spread_tier": tier,
        "bookkeeping": {"attempt_ordinal": attempt.ordinal,
            "horizon_attempt_id": attempt.plan_attempt["attempt_id"]}}


def _route_a_batch_progress(path):
    import json
    import os
    import time
    from datetime import datetime, timezone

    def progress(phase, label, **details):
        row = {"phase": phase, "label": label,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "monotonic_ns": time.perf_counter_ns(),
            "fixture_provenance": "INVENTED_SOFTWARE_ONLY_NOT_FINANCIAL_OR_PERFORMANCE_EVIDENCE",
            **details}
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    return progress


def _observe_owned_invented_route_a_consumer(tmp_path, monkeypatch, *,
        source_access_window_utc, native_fixture, observe, progress,
        restore_first=False, refuse_outer_consumer=False):
    """Prove owner control flow and finish entry, never completed financial evidence."""
    from collections import Counter
    import sys
    import time
    import pm_authority_spool as spool
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer

    class BeforeInventedOrdinaryLoop(BaseException):
        pass

    class ObservedOriginalFinishEntry(BaseException):
        pass

    with _invented_fixture_external_io_guard(monkeypatch) as external_io_attempts:
        env = _build_owned_invented_declared_inputs(tmp_path,
            source_access_window_utc=source_access_window_utc, native_fixture=native_fixture)
        committed = orchestration.verify_declared_slot_owner_run_precommit(
            env.context, env.precommit, process=env.process)
        cells = [cell for cell in committed["cells"] if cell["cell_id"] == env.cell_id]
        assert len(cells) == 1
        sink = spool.open_symbol_sink(env.context, cells[0]["symbol"])
        request = orchestration.declared_slot_producer_request(env.context,
            precommit=env.precommit, process=env.process, cell_id=env.cell_id,
            slot_id=env.slot_id, attempt_id="invented-route-a-unit-consumer", sink=sink)
        assert execution._read_declared_unit_header(env.context, request.to_dict()) is None
        saved, reached, failure, finish_entries = {}, [], [], []
        observations, consumer_entries, containment_events, normal_returns = [], [], [], []
        observation_order = 0
        callback_active = False
        diagnostic_recording_failures = []
        original_containment = execution._route_a_contain_consumer_failure
        original_header = execution._make_declared_unit_header
        original_unit_binding = execution._declared_unit_binding
        original_consumer = execution._consume_route_a_unit
        assert original_consumer is execution._route_a_noop_consumer
        entrypoints = ((execution, "_declared_fitted_for_execution"),
            (execution, "_reverify_cell_state_error_envelope"),
            (execution, "_verify_attempt_roster"),
            (execution, "_verified_historical_cell_plan_inputs"),
            (execution, "_verify_capture_horizon_plan_identity"),
            (execution.historical_adapter, "_verify_declared_slot_frozen_chain"))
        counts = {name: 0 for _, name in entrypoints}
        auth_codes = {name: getattr(owner, name).__code__ for owner, name in entrypoints}
        pool_type = execution.pm_core._EntryAdmissionPreparation
        model_codes = {name: function.__code__ for name, function in (
            ("model_run", execution.Backtester.run), ("runtime_strategy", execution._runtime_strategy),
            ("partition", execution._run_tier_partition),
            ("surface", execution._build_full_prefix_strategy_surface),
            ("pool_lookup", pool_type._states_for), ("pool_enter", pool_type.__enter__),
            ("pool_exit", pool_type.__exit__),
            ("full_capture", execution.historical_adapter._verify_declared_slot_capture),
            ("unit_binding", original_unit_binding),
            ("source_reopen", producer._reopen_declared_source_material),
            ("original_freeze", execution.historical_adapter.DeclaredSlotSearchCapture.freeze_observed_cell))}
        owner_codes = {name: function.__code__ for name, function in (
            ("owner_snapshot", execution._route_a_owner_membership_snapshot),
            ("owner_rebind", execution._route_a_rebind_owner_membership),
            ("owner_key", execution._route_a_owner_selection_bytes),
            ("owner_geometry", execution._fold_partitions))}
        dispatch = _route_a_callback_identity_dispatch(
            tuple(auth_codes.items()) + tuple(model_codes.items()) + tuple(owner_codes.items()))
        coordinator_code = execution._route_a_evaluate_prepared_request.__code__
        restore_code = execution.historical_adapter.DeclaredSlotSearchCapture.restore_completed_unit_handle.__code__
        executor_code = execution.execute_historical_observed_cell.__code__
        batch_codes = []
        def find_batch_code(code):
            for value in code.co_consts:
                if isinstance(value, type(code)):
                    if value.co_name == "evaluate_route_a_batch":
                        batch_codes.append(value)
                    find_batch_code(value)
        find_batch_code(executor_code)
        assert len(batch_codes) == 1
        working_parent_code = batch_codes[0]
        owner_spans = {}
        calls = SimpleNamespace(counts=Counter(), instances=[], unique_intervals=[], auth_snapshots=[],
            executor_admission_before=[], loop_invocations=0,
            pool_depth=0, restore_returns=0, header_creations=0, outputs={},
            owner_counts=Counter(), owner_geometry=Counter(), owner_intervals=[],
            owner_snapshots=[], default_prefix_receipts=[], owner_binding_metadata=None,
            owner_unit_roster=None)
        active_unique = []

        def observation(stage, *, error=None, returned=None):
            nonlocal observation_order
            observation_order += 1
            row = {"order": observation_order, "monotonic_ns": time.monotonic_ns(),
                "stage": stage, "error": error, "returned": returned}
            observations.append(row)
            return row

        def save_failure(stage, error):
            row = observation(stage, error=error)
            failure.append(row)
            return row

        def defer_recording_failure(stage, error):
            nonlocal observation_order
            try:
                save_failure(stage, error)
            except BaseException as recording_error:
                try:
                    observation_order += 1
                    diagnostic_recording_failures.append({"order": observation_order,
                        "monotonic_ns": time.monotonic_ns(), "stage": "diagnostic-recording:" + stage,
                        "error": recording_error, "returned": None})
                except BaseException:
                    # Preserve the delegated outcome if diagnostic storage itself is unavailable.
                    pass

        def bounded_error(error):
            frames, trace = [], error.__traceback__
            while trace is not None and len(frames) < 8:
                code = trace.tb_frame.f_code
                frames.append({"function": code.co_name[:96],
                    "first_line": code.co_firstlineno, "line": trace.tb_lineno})
                trace = trace.tb_next
            return {"type": type(error).__name__[:96],
                "code": str(getattr(error, "code", ""))[:96],
                "message": str(error)[:256], "traceback_code": frames}

        def bounded_return(result):
            summary = {"type": type(result).__name__[:96]}
            if type(result) is dict:
                for key in ("schema", "schema_version", "status", "reason_code", "failure_code"):
                    value = result.get(key)
                    if value is None or type(value) in (str, bool, int, float):
                        summary[key] = value[:256] if type(value) is str else value
                refusal = result.get("refusal")
                if type(refusal) is dict:
                    summary["refusal_code"] = str(refusal.get("code", ""))[:96]
                    summary["refusal_message"] = str(refusal.get("details", ()))[:256]
            return summary

        def record_normal_return(stage, result):
            summary = bounded_return(result)
            normal_returns.append(observation(stage, returned=summary))
            save_failure(stage, AssertionError("INVENTED producer returned before its control boundary: "
                + str(summary)))

        def record_containment(stage, error, *, returned=False, result=None):
            if returned and callback_active:
                classification = "CALLBACK_RESULT_ASSERTIONS"
            elif (returned and consumer_entries and refuse_outer_consumer and type(error) is RuntimeError
                    and error.args == ("INVENTED_OUTER_CONSUMER_REFUSAL_MUST_NOT_STOP_OWNER",)):
                classification = "INTENTIONAL_OUTER_CONSUMER_SENTINEL"
            else:
                classification = "UNEXPECTED"
            row = observation(stage, error=error,
                returned=bounded_return(result) if returned else None)
            row["classification"] = classification
            containment_events.append(row)
            if classification == "UNEXPECTED":
                failure.append(row)

        def containment_spy(error):
            try:
                result = original_containment(error)
            except BaseException:
                try:
                    record_containment("containment-raised", error)
                except BaseException as observation_error:
                    defer_recording_failure("containment-observation", observation_error)
                raise
            try:
                record_containment("containment-returned", error, returned=True, result=result)
            except BaseException as observation_error:
                defer_recording_failure("containment-observation", observation_error)
            return result

        def flush_observations():
            failed_orders = {row["order"] for row in failure + diagnostic_recording_failures}
            rows = tuple(observations) + tuple(diagnostic_recording_failures)
            for row in sorted(rows, key=lambda item: item["order"]):
                try:
                    details = {"observation_order": row["order"],
                        "observation_monotonic_ns": row["monotonic_ns"],
                        "saved_failure": row["order"] in failed_orders}
                    if "classification" in row:
                        details["classification"] = row["classification"]
                    if row["error"] is not None:
                        details["error"] = bounded_error(row["error"])
                    if row["returned"] is not None:
                        details["returned"] = row["returned"]
                    progress("BOUNDED_OWNER_OBSERVATION", row["stage"], **details)
                except Exception as observation_error:
                    defer_recording_failure("observation-progress", observation_error)

        def raise_earliest_failure():
            candidates = failure + diagnostic_recording_failures
            if candidates:
                earliest = min(candidates, key=lambda row: (row["monotonic_ns"], row["order"]))
                raise earliest["error"]

        def profile(frame, event, argument):
            if event == "call":
                if frame.f_code is executor_code:
                    calls.executor_admission_before.append(dict(counts))
                    progress("STARTED_NORMAL_OWNER_EXECUTOR", str(len(calls.executor_admission_before)))
                for name in dispatch(frame.f_code, event):
                    if name in counts:
                        counts[name] += 1
                        progress("STARTED_AUTH_ENTRY", name, interval="NORMAL_OWNER_AND_ADOPTION")
                    elif name in owner_codes:
                        calls.owner_counts[name] += 1
                        interval = name
                        if name == "owner_geometry":
                            caller = frame.f_back.f_code if frame.f_back is not None else None
                            if any(label == "owner_snapshot" for _, label in owner_spans):
                                interval = "snapshot_geometry"
                            elif caller is owner_codes["owner_rebind"]:
                                interval = "selected_rebind_geometry"
                            elif caller is working_parent_code:
                                interval = "uncached_working_geometry"
                            else:
                                interval = "fixture_reference_or_other_geometry"
                            calls.owner_geometry[interval] += 1
                        owner_spans[id(frame), name] = (interval, time.monotonic_ns())
                    else:
                        calls.counts[name] += 1
                if frame.f_code is model_codes["partition"]:
                    calls.instances.append((frame.f_locals["strategy"], frame.f_locals["backtester"],
                        frame.f_locals.get("_entry_admission_preparation")))
                if frame.f_code is coordinator_code:
                    ordinal = len(calls.unique_intervals)
                    calls.unique_intervals.append(ordinal)
                    active_unique.append((frame, ordinal))
                    calls.auth_snapshots.append(dict(counts))
                    progress("STARTED_UNIQUE_EVALUATION", str(ordinal),
                        interval="ACTUAL_SHARED_COORDINATOR_ONLY")
            elif event == "return":
                for name in dispatch(frame.f_code, "call"):
                    if name not in owner_codes:
                        continue
                    interval, started = owner_spans.pop((id(frame), name))
                    ended = time.monotonic_ns()
                    calls.owner_intervals.append({"interval": interval,
                        "started_monotonic_ns": started, "ended_monotonic_ns": ended,
                        "observed_duration_ns": ended - started,
                        "returned_nonnull": argument is not None})
                    if name == "owner_snapshot" and type(argument) is dict:
                        # Retain only returned plain JSON metadata, never execution frames.
                        calls.owner_snapshots.append(copy.deepcopy(argument))
                if frame.f_code is model_codes["partition"] and argument is not None:
                    partition = frame.f_locals["partition"]
                    tier = frame.f_locals["spread_tier"].tier
                    calls.outputs[partition.geometry_receipt["receipt_sha256"], tier] = {
                        "decision": copy.deepcopy(frame.f_locals["decision_result_out"]),
                        "signals": argument[2].copy(deep=True)}
                if frame.f_code is model_codes["pool_enter"]:
                    calls.pool_depth += 1
                if frame.f_code is model_codes["pool_exit"]:
                    calls.pool_depth -= 1
                if frame.f_code is restore_code and argument is not None:
                    calls.restore_returns += 1
                    progress("RETURNED_ACTUAL_RESTORED_HANDLE", "fresh-INVENTED-original-request")
                for name, code in auth_codes.items():
                    if frame.f_code is code:
                        progress("RETURNED_AUTH_ENTRY", name, interval="NORMAL_OWNER_AND_ADOPTION")
                if active_unique and frame is active_unique[-1][0]:
                    _, ordinal = active_unique.pop()
                    calls.auth_snapshots.append(dict(counts))
                    progress("COMPLETED_UNIQUE_EVALUATION" if argument is not None else "ABORTED_UNIQUE_EVALUATION",
                        str(ordinal), interval="ACTUAL_SHARED_COORDINATOR_ONLY")

        def header_spy(*args, **kwargs):
            header = original_header(*args, **kwargs)
            assert not kwargs and len(args) == 8 and calls.header_creations == 0
            calls.header_creations += 1
            saved["original_header_bytes"] = execution._canonical_bytes(header)
            progress("COMPLETED_ORIGINAL_HEADER", "fresh-INVENTED-owner")
            return header

        def unit_binding_spy(*args, **kwargs):
            binding = original_unit_binding(*args, **kwargs)
            assert not kwargs and len(args) == 6
            calls.owner_binding_metadata = copy.deepcopy(binding)
            capture, refreshed, cell_plan, attempts, fold_plan, handle = args
            saved["verified_binding_witness"] = {"capture": capture,
                "fitted": capture.fitted_timeframe, "refreshed": refreshed,
                "cell_plan": cell_plan, "attempts": attempts, "fold_plan": fold_plan,
                "handle": handle, "search_process": capture.process,
                "config": capture.pipeline_config, "binding": copy.deepcopy(binding)}
            return binding

        def ordinary_loop(actual_context, body, binding_sha256, units, produce):
            assert actual_context is env.context and body == request.to_dict()
            assert calls.pool_depth == 1 and callable(produce)
            current = saved["verified_binding_witness"]
            assert [unit["ordinal"] for unit in units] == list(range(len(current["attempts"])))
            calls.loop_invocations += 1
            if restore_first and calls.loop_invocations == 1:
                assert calls.counts["model_run"] == calls.counts["runtime_strategy"] == 0
                assert execution._consume_route_a_unit is original_consumer is execution._route_a_noop_consumer
                assert all(calls.owner_counts[name] == 0 for name in ("owner_snapshot", "owner_rebind", "owner_key"))
                calls.default_prefix_receipts.append({
                    "interval": "DEFAULT_PREFIX_BEFORE_ORDINARY_LOOP_ONLY",
                    "consumer_identity": "UNCHANGED_PRODUCTION_NOOP_UNWRAPPED",
                    "snapshot_rebind_key_calls": {name: calls.owner_counts[name]
                        for name in ("owner_snapshot", "owner_rebind", "owner_key")},
                    "default_post_loop_gate_and_finish": "UNPROVEN_STOPPED_BEFORE_ORDINARY_LOOP",
                    "extra_owner_preparations": 0})
                saved["first_unit_roster"] = copy.deepcopy(units)
                saved["first_binding_sha256"] = binding_sha256
                # Revoked objects are held only for identity observations, never second-call inputs.
                saved["first_revoked_objects_for_identity_only"] = tuple(current[name]
                    for name in ("capture", "fitted", "refreshed", "handle")) + (
                        current["capture"].source_session,)
                for name in ("raw-completion.json", "evidence-completed.json"):
                    assert execution._read_declared_unit_json(env.context,
                        execution._declared_unit_relative(body, name)) is None
                assert execution._declared_completed_prefix(env.context, body, binding_sha256, units) == []
                progress("CONTROLLED_BEFORE_ORDINARY_LOOP", "fresh-header-first-invocation-no-produce")
                raise BeforeInventedOrdinaryLoop()
            saved["post_loop_counts"] = dict(counts)
            calls.owner_unit_roster = copy.deepcopy(units)
            progress("RETURNED_STUBBED_ORDINARY_LOOP", "INVENTED_CONTROL_FLOW_ONLY_NO_ATTEMPTS_COMPLETED")
            header = execution._read_declared_unit_header(env.context, body)
            return [], header["evidence_access_started_at_utc"]

        def consumer(*, evaluate_batch):
            nonlocal callback_active
            entry = observation("consumer-entry")
            consumer_entries.append(entry)
            try:
                progress("OBSERVED_CONSUMER_ENTRY", "BEFORE_ASSERTIONS",
                    observation_order=entry["order"], observation_monotonic_ns=entry["monotonic_ns"])
                assert saved and not reached and calls.pool_depth == 0
                current = saved["verified_binding_witness"]
                capture = current["capture"]
                assert capture.request is request and capture.context is env.context
                header = execution._read_declared_unit_header(env.context, request.to_dict())
                assert execution._canonical_bytes(header) == saved["original_header_bytes"]
                if restore_first:
                    assert calls.restore_returns == 1
                    actual_objects = tuple(current[name] for name in ("capture", "fitted", "refreshed", "handle")) + (
                        capture.source_session,)
                    assert all(first is not second for first, second in zip(
                        saved["first_revoked_objects_for_identity_only"], actual_objects))
                reached.append(True)
                callback_active = True
                try:
                    assert observe(evaluate_batch=evaluate_batch, witness={
                        "capture": capture, "fitted": current["fitted"], "refreshed": current["refreshed"],
                        "cell_plan": current["cell_plan"], "attempts": current["attempts"],
                        "fold_plan": current["fold_plan"], "handle": current["handle"],
                        "declared_header": header, "search_process": current["search_process"], "config": current["config"]},
                        counts=counts, post_header_counts=saved["post_loop_counts"], calls=calls,
                        restored=restore_first) is None
                finally:
                    callback_active = False
            except Exception as error:
                save_failure("consumer-assertion", error)
            if refuse_outer_consumer:
                raise RuntimeError("INVENTED_OUTER_CONSUMER_REFUSAL_MUST_NOT_STOP_OWNER")
            return None

        def finish_entry(actual_capture, actual_handle, **kwargs):
            entry = observation("finish-entry")
            finish_entries.append(sorted(kwargs))
            try:
                progress("OBSERVED_ORIGINAL_CAPTURE_FINISH_ENTRY", "BEFORE_ASSERTIONS_CONTROL_FLOW_ONLY",
                    observation_order=entry["order"], observation_monotonic_ns=entry["monotonic_ns"])
                current = saved["verified_binding_witness"]
                assert actual_capture is current["capture"] and actual_handle is current["handle"]
                assert reached == [True] and calls.pool_depth == 0
                assert set(kwargs) == {"attempt_evidence", "evidence_access_started_at_utc", "evidence_access_completed_at_utc"}
                assert iter(kwargs["attempt_evidence"]) is kwargs["attempt_evidence"]
            except Exception as error:
                save_failure("finish-assertion", error)
            finally:
                raise ObservedOriginalFinishEntry()

        try:
            with monkeypatch.context() as scoped:
                scoped.setattr(execution, "_make_declared_unit_header", header_spy)
                scoped.setattr(execution, "_declared_unit_binding", unit_binding_spy)
                scoped.setattr(execution, "_continue_declared_attempt_units", ordinary_loop)
                scoped.setattr(execution, "_route_a_contain_consumer_failure", containment_spy)
                scoped.setattr(execution.historical_adapter.DeclaredSlotSearchCapture,
                    "complete_observed_cell", finish_entry)
                previous = sys.getprofile()
                sys.setprofile(profile)
                try:
                    if restore_first:
                        try:
                            first_return = producer.execute_declared_slot_owner_search(env.context, request=request,
                                process=env.process, frozen_pipeline_config=env.frozen_pipeline_config)
                        except BeforeInventedOrdinaryLoop:
                            pass
                        else:
                            record_normal_return("first-producer-normal-return", first_return)
                            flush_observations()
                            raise_earliest_failure()
                        assert calls.header_creations == 1 and not reached and calls.pool_depth == 0
                        first_body = request.to_dict()
                        for name in ("raw-completion.json", "evidence-completed.json"):
                            assert execution._read_declared_unit_json(env.context,
                                execution._declared_unit_relative(first_body, name)) is None
                        assert execution._declared_completed_prefix(env.context, first_body,
                            saved["first_binding_sha256"], saved["first_unit_roster"]) == []
                        assert execution._canonical_bytes(execution._read_declared_unit_header(
                            env.context, first_body)) == saved["original_header_bytes"]
                        saved.pop("verified_binding_witness")
                        progress("STARTED_NORMAL_EXISTING_HEADER_REOPEN", "same-live-original-request-within-original-window")
                    scoped.setattr(execution, "_consume_route_a_unit", consumer)
                    try:
                        second_return = producer.execute_declared_slot_owner_search(env.context, request=request,
                            process=env.process, frozen_pipeline_config=env.frozen_pipeline_config)
                    except ObservedOriginalFinishEntry:
                        pass
                    else:
                        record_normal_return("second-producer-normal-return", second_return)
                finally:
                    sys.setprofile(previous)
        finally:
            orchestration._release_declared_slot_producer_request(request, env.context)
        flush_observations()
        raise_earliest_failure()
        assert reached == [True] and len(finish_entries) == 1
        assert calls.header_creations == 1 and calls.pool_depth == 0
        assert calls.restore_returns == (1 if restore_first else 0)
        assert calls.counts["source_reopen"] == (1 if restore_first else 0)
        assert calls.counts["original_freeze"] == 1
        assert not active_unique and not external_io_attempts
        assert not owner_spans
    return {"fixture_provenance": "FRESH_INVENTED_NORMAL_ISSUER_POSTLOOP_FINISH_ENTRY_ONLY",
        "normal_finish_entry_observed": True, "restored_fresh_software_header": restore_first,
        "qualification_authority": False, "capital_authority": False, "live_order_authority": False}


def _route_a_invented_data_specification():
    return {"strategy_id": "INVENTED_STRATEGY_PREDICATE_ONLY",
        "parameters": {"period": 2}, "fold_ordinal": 0,
        "partition_role": "optimizer_train", "spread_tier": "P75", "bookkeeping": {}}


@pytest.mark.parametrize("damage, suffix", (
    ("container", "SPECIFICATIONS_INVALID"),
    ("shape", "REQUEST_INVALID"),
    ("ordinal", "REQUEST_INVALID"),
    ("strategy", "REQUEST_INVALID"),
    ("parameters", "REQUEST_INVALID"),
    ("fold", "REQUEST_INVALID"),
    ("bookkeeping", "REQUEST_INVALID"),
    ("authority_field", "REQUEST_INVALID"),
    ("lockbox", "LOCKBOX_REFUSED"),
    ("role", "ROLE_INVALID"),
    ("tier", "TIER_INVALID"),
), ids=("container", "shape", "ordinal", "strategy", "parameters",
    "fold", "bookkeeping", "authority_field", "lockbox", "role", "tier"))
def test_route_a_batch_actual_data_predicates_refuse_invalid_specification(damage, suffix):
    """INVENTED values prove actual predicates only; they issue no owner capability."""
    row = _route_a_invented_data_specification()
    if damage == "shape":
        del row["parameters"]
    elif damage == "ordinal":
        row["request_ordinal"] = 1
    elif damage == "strategy":
        row["strategy_id"] = ""
    elif damage == "parameters":
        row["parameters"] = []
    elif damage == "fold":
        row["fold_ordinal"] = -1
    elif damage == "bookkeeping":
        row["bookkeeping"] = []
    elif damage == "authority_field":
        row["capture"] = "INVENTED_NOT_AUTHORITY"
    elif damage == "lockbox":
        row["partition_role"] = "final_lockbox"
    elif damage == "role":
        row["partition_role"] = "INVENTED_UNKNOWN_ROLE"
    elif damage == "tier":
        row["spread_tier"] = "P99"
    specifications = {} if damage == "container" else [row]
    before = copy.deepcopy(specifications)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_batch_specifications(specifications)
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_" + suffix
    assert specifications == before


def test_route_a_batch_actual_specification_snapshot_is_copy_safe():
    row = _route_a_invented_data_specification()
    before = copy.deepcopy(row)
    assert execution._route_a_batch_specifications([]) == ()
    snapshot = execution._route_a_batch_specifications([row])
    assert type(snapshot) is tuple and len(snapshot) == 1 and snapshot[0] == {**row, "request_ordinal": 0}
    row["parameters"]["period"] = 3
    row["bookkeeping"]["attempt_ordinal"] = 100
    assert snapshot[0] == {**before, "request_ordinal": 0}


@pytest.mark.parametrize("closed,busy,suffix", (
    (False, False, None), (False, True, "BUSY"),
    (True, False, "CLOSED"), (True, True, "CLOSED"),
), ids=("open_idle", "open_busy", "closed_idle", "closed_busy"))
def test_route_a_batch_actual_idle_state_predicate(closed, busy, suffix):
    if suffix is None:
        assert execution._route_a_require_idle(closed=closed, busy=busy) is True
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._route_a_require_idle(closed=closed, busy=busy)
        assert caught.value.code == "ROUTE_A_UNIT_BATCH_" + suffix


@pytest.mark.parametrize("damage,suffix", (
    (None, None), ("closed", "CLOSED"),
    ("process", "OWNER_MISMATCH"), ("thread", "OWNER_MISMATCH"),
    ("early", "SOURCE_WINDOW_CLOSED"), ("expired", "SOURCE_WINDOW_CLOSED"),
), ids=("current", "closed", "process", "thread", "early", "expired"))
def test_route_a_batch_actual_current_owner_and_window_predicate(damage, suffix):
    from datetime import datetime, timedelta, timezone
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    arguments = {"closed": damage == "closed", "owner_process": 11, "owner_thread": 22,
        "current_process": 12 if damage == "process" else 11,
        "current_thread": 23 if damage == "thread" else 22,
        "window_start": start, "window_end": end,
        "observed": start - timedelta(seconds=1) if damage == "early" else (
            end if damage == "expired" else start)}
    if suffix is None:
        assert execution._route_a_require_scope_current(**arguments) is True
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._route_a_require_scope_current(**arguments)
        assert caught.value.code == "ROUTE_A_UNIT_BATCH_" + suffix


def test_route_a_batch_actual_nonauthorizing_lifecycle_transitions():
    lifecycle = execution._RouteABatchLifecycle()
    assert lifecycle.closed is False and lifecycle.busy is False
    lifecycle.enter()
    assert lifecycle.busy is True and lifecycle.closed is False
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        lifecycle.enter()
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_BUSY"
    lifecycle.leave()
    assert lifecycle.busy is False and lifecycle.closed is False
    lifecycle.enter()
    lifecycle.close()
    assert lifecycle.closed is True and lifecycle.busy is False
    lifecycle.leave()
    assert lifecycle.closed is True and lifecycle.busy is False
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        lifecycle.enter()
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_CLOSED"


def test_route_a_batch_actual_transfer_refusal_revokes_plain_lifecycle():
    lifecycle = execution._RouteABatchLifecycle()
    lifecycle.enter()
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_refuse_transfer(lifecycle)
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_CAPABILITY_NOT_TRANSFERABLE"
    assert lifecycle.closed is True and lifecycle.busy is False


def test_route_a_batch_actual_shared_content_comparison_representatives():
    expected = {"top": "INVENTED_BOUND", "nested": {"field": [1, 2]}}
    assert execution._route_a_require_unchanged(expected=expected, observed=copy.deepcopy(expected)) is True
    for changed in ({**expected, "top": "INVENTED_OTHER"},
            {**expected, "nested": {"field": [1, 3]}}):
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._route_a_require_unchanged(expected=expected, observed=changed)
        assert caught.value.code == "ROUTE_A_UNIT_BATCH_SHARED_CONTENT_CHANGED"


@pytest.mark.parametrize("damage", (
    "value", "dtype", "index_value", "index_name", "column_name", "column_order", "attrs",
    "series_name", "series_dtype", "categorical_order", "timezone",
), ids=("value", "dtype", "index_value", "index_name", "column_name", "column_order", "attrs",
    "series_name", "series_dtype", "categorical_order", "timezone"))
def test_route_a_batch_actual_pandas_fingerprint_covers_typed_content(damage):
    """Tiny INVENTED typed values exercise mutation detection, not market evidence."""
    frame = pd.DataFrame({"first": pd.Series([1, 2], dtype="int32"),
        "second": pd.Series([3, 4], dtype="int32")})
    if damage == "series_name":
        original = pd.Series([1, 2], name="INVENTED_ORIGINAL", dtype="int32")
        changed = original.rename("INVENTED_CHANGED")
    elif damage == "series_dtype":
        original = pd.Series([1, 2], dtype="int32")
        changed = original.astype("int64")
    elif damage == "categorical_order":
        original = pd.Series(pd.Categorical(["a", "b"], categories=["a", "b"], ordered=True))
        changed = original.cat.reorder_categories(["b", "a"], ordered=True)
    elif damage == "timezone":
        original = pd.Series([1, 2], index=pd.date_range("2020-01-01", periods=2, tz="UTC"))
        changed = original.copy(deep=True)
        changed.index = changed.index.tz_convert("Europe/Budapest")
    else:
        original, changed = frame, frame.copy(deep=True)
        if damage == "value":
            changed.iloc[0, 0] = 9
        elif damage == "dtype":
            changed["first"] = changed["first"].astype("int64")
        elif damage == "index_value":
            changed.index = pd.Index([10, 11])
        elif damage == "index_name":
            changed.index = changed.index.rename("INVENTED_INDEX")
        elif damage == "column_name":
            changed = changed.rename(columns={"first": "INVENTED_COLUMN"})
        elif damage == "column_order":
            changed = changed[["second", "first"]]
        elif damage == "attrs":
            changed.attrs["fixture"] = "INVENTED_MUTATION"
    expected = execution._route_a_pandas_content_sha256(original)
    assert execution._route_a_pandas_content_sha256(original.copy(deep=True)) == expected
    assert execution._route_a_pandas_content_sha256(changed) != expected


def test_route_a_batch_actual_pandas_fingerprint_refuses_non_pandas_values():
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_pandas_content_sha256({"fixture": "INVENTED_NOT_A_FRAME"})
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_PANDAS_INPUT_INVALID"


def _route_a_invented_fingerprint_namespace():
    """Complete inert content surface; no owner, seal or issuer authority."""
    from dataclasses import dataclass

    @dataclass
    class InventedInstrument:
        contract: str = "INVENTED_INSTRUMENT_CONTRACT"

    tier_descriptor = {
        "fixture_provenance": "INVENTED_TIER_FINGERPRINT_SURFACE_NOT_SPREAD_EVIDENCE",
        "symbol": "INVENTED_SOFTWARE_SYMBOL", "timeframe": "D1", "tier": "P75",
        "source_timeframe": "D1", "point": 0.0001, "spread_points": 1.0,
        "full_spread_price": 0.0001}
    tier_surface = SimpleNamespace(**tier_descriptor,
        descriptor=lambda: copy.deepcopy(tier_descriptor))

    return SimpleNamespace(_bound_fit_owner=None, _derived_binding=None,
        _frame=pd.DataFrame({"value": [1, 2]}), _entry_spread_observations=pd.Series([1, 2]),
        _spread_source_frame=None, _spread_source_product=None,
        source_group_decision=SimpleNamespace(to_dict=lambda: {"fixture": "INVENTED_SOURCE"}),
        regime_fit_decision=SimpleNamespace(to_dict=lambda: {"fixture": "INVENTED_FIT"}),
        source_contract_seed={"fixture": "INVENTED_SEED"}, cost_ladder_contract={},
        regime_parameters={}, train_regime_support={}, instrument_spec=InventedInstrument(),
        execution_spec_receipt={"contract": {"fixture": "INVENTED_EXECUTION_SPEC"}},
        frame_sha256="a" * 64, source_contract_seeds={"P75": {"fixture": "INVENTED_P75"}},
        frozen_tier_pair_contract={"fixture": "INVENTED_PAIR"},
        _spread_tiers={"P75": tier_surface},
        product=SimpleNamespace(index_descriptor={"sha256": "b" * 64}, index_sha256="b" * 64,
            index={"native": {"fixture": "INVENTED_INDEX"}}, native_prefix={"fixture": "INVENTED_PREFIX"}))

def test_route_a_batch_actual_fitted_fingerprint_includes_instrument_and_bound_contracts():
    """Inert namespaces exercise content hashing only; no fitted/capture capability is constructed."""
    fitted = _route_a_invented_fingerprint_namespace()
    expected = execution._route_a_fitted_shared_content(fitted)
    for target, key in ((vars(fitted.instrument_spec), "contract"),
            (fitted.execution_spec_receipt["contract"], "fixture"),
            (fitted.frozen_tier_pair_contract, "fixture"),
            (fitted.product.index["native"], "fixture")):
        previous = target[key]
        target[key] = "INVENTED_CHANGED_BOUND_CONTENT"
        assert execution._route_a_fitted_shared_content(fitted) != expected
        target[key] = previous
        assert execution._route_a_fitted_shared_content(fitted) == expected


def test_route_a_batch_actual_fitted_fingerprint_preserves_unset_owner_slots():
    """INVENTED eight-slot observation only; no real owner constructor, seal or issuer."""
    class InventedSlottedOwner:
        __slots__ = ("seal", "fitted", "context_id", "request_sha256", "header_bytes",
            "reference_bytes", "validation_receipt", "verification_only")

    owner = InventedSlottedOwner()
    owner.seal = None
    owner.fitted = None
    owner.context_id = "INVENTED_CONTEXT_OBSERVATION"
    owner.request_sha256 = "INVENTED_NOT_A_REQUEST_GRANT"
    owner.header_bytes = b"INVENTED_HEADER_OBSERVATION_ONLY"
    owner.reference_bytes = b"INVENTED_REFERENCE_OBSERVATION_ONLY"
    owner.validation_receipt = {"fixture_provenance": "INVENTED_NO_VALIDATION_AUTHORITY"}
    fitted = _route_a_invented_fingerprint_namespace()
    fitted._bound_fit_owner = owner
    assert not hasattr(owner, "verification_only")
    absent = execution._route_a_fitted_shared_content(fitted)
    assert execution._route_a_fitted_shared_content(fitted) == absent
    observed = {absent}
    for value in (None, False, ("absent",)):
        owner.verification_only = value
        present = execution._route_a_fitted_shared_content(fitted)
        assert present not in observed
        assert execution._route_a_fitted_shared_content(fitted) == present
        observed.add(present)
        del owner.verification_only
        assert not hasattr(owner, "verification_only")
        assert execution._route_a_fitted_shared_content(fitted) == absent


@pytest.mark.parametrize("failure,expected", (
    (None, ["current", "probe", "verify_probe", "current", "strategy", "run", "verify_realized", "current"]),
    ("probe", ["current", "probe", "verify_probe"]),
    ("expiry", ["current", "probe", "verify_probe", "current"]),
    ("realized", ["current", "probe", "verify_probe", "current", "strategy", "run", "verify_realized"]),
), ids=("success", "probe_refusal", "pre_strategy_expiry", "realized_refusal"))
def test_route_a_batch_actual_coordinator_order_and_pre_strategy_refusals(failure, expected):
    """Inert dependencies exercise the shared production coordinator, not authentication."""
    events, probe, strategy, realized = [], object(), object(), object()

    class InventedRefusal(BaseException):
        pass

    def current():
        events.append("current")
        if failure == "expiry" and events.count("current") == 2:
            raise InventedRefusal()

    def prepare_probe():
        events.append("probe")
        return probe

    def verify_probe(*values):
        events.append("verify_probe")
        assert probe in values
        if failure == "probe":
            raise InventedRefusal()

    def make_strategy():
        events.append("strategy")
        return strategy

    def run_strategy(*values):
        events.append("run")
        assert strategy in values
        return realized

    def verify_realized(*values):
        events.append("verify_realized")
        assert realized in values
        if failure == "realized":
            raise InventedRefusal()

    arguments = dict(prepare_probe=prepare_probe, verify_probe=verify_probe,
        require_current=current, make_strategy=make_strategy, run_strategy=run_strategy,
        verify_realized=verify_realized)
    if failure is None:
        assert execution._route_a_evaluate_prepared_request(**arguments) is realized
    else:
        with pytest.raises(InventedRefusal):
            execution._route_a_evaluate_prepared_request(**arguments)
    assert events == expected


@pytest.mark.parametrize("damage", (None, "signal", "pair", "missing_partner"),
    ids=("verified", "signal_mismatch", "pair_drift", "missing_partner"))
def test_route_a_batch_actual_pair_gate_keeps_independent_tier_checks(monkeypatch, damage):
    signals = {"P75": pd.Series([0, 1], dtype="int32"), "P95": pd.Series([0, 1], dtype="int32")}
    results = {"P75": {"fixture": "INVENTED_P75"}, "P95": {"fixture": "INVENTED_P95"}}
    frozen = {"fixture": "INVENTED_PAIR_PREDICATE_ONLY"}
    calls = []

    def build_pair(actual, *, spec, use_spread):
        assert actual is results and use_spread is True
        calls.append(True)
        return {"fixture": "INVENTED_DRIFT"} if damage == "pair" else dict(frozen)

    monkeypatch.setattr(execution.pm_core, "build_backtest_frozen_tier_pair_contract", build_pair)
    if damage == "signal":
        signals["P95"].iloc[1] = -1
    elif damage == "missing_partner":
        signals.pop("P95")
        results.pop("P95")
    arguments = dict(signals_by_tier=signals, results_by_tier=results,
        spec=object(), use_spread=True, frozen_pair=frozen)
    if damage in {"signal", "pair"}:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._route_a_pair_check(**arguments)
        assert caught.value.code == ("PARTITION_SIGNAL_VECTOR_TIER_MISMATCH" if damage == "signal"
            else "REALIZED_FROZEN_TIER_POLICY_DRIFT")
    else:
        result = execution._route_a_pair_check(**arguments)
        assert result["status"] == ("INCOMPLETE_MISSING_PARTNER" if damage == "missing_partner" else "VERIFIED")
    assert len(calls) == (1 if damage in {None, "pair"} else 0)


def _assert_route_a_batch_refusal(result, code):
    assert result["schema_version"] == "fxpm.route-a-unit-batch-diagnostic.v1"
    assert result["status"] == "REFUSED_DIAGNOSTIC_ONLY" and result["refusal"]["code"] == code
    assert result["requests"] == result["request_to_unique"] == result["unique_evaluations"] == result["pair_checks"] == []
    for name in ("execution_ready", "overall_execution_ready", "qualification_authority", "capital_authority",
            "paper_authority", "live_authority", "order_authority", "live_order_authority"):
        assert result[name] is False


def test_route_a_batch_actual_refusal_containment_preserves_finish_control_flow():
    """INVENTED finish callback outcome; this is no completed financial slot."""
    arguments = ("INVENTED_ORIGINAL_HANDLE", {"fixture": "INVENTED_ORIGINAL_EVIDENCE"})
    outcomes, calls, refusals = [], [], []

    def finish(*actual):
        calls.append(actual)
        return "INVENTED_SAME_FINISH_OUTCOME"

    def refused(*, evaluate_batch):
        raise RuntimeError("INVENTED_CONSUMER_REFUSAL")

    for consumer in (execution._route_a_noop_consumer, refused):
        try:
            consumer(evaluate_batch=lambda **_values: pytest.fail("INVENTED wiring does not evaluate"))
        except BaseException as error:
            refusals.append(execution._route_a_contain_consumer_failure(error))
        outcomes.append(finish(*arguments))
    assert calls == [arguments, arguments]
    assert outcomes == ["INVENTED_SAME_FINISH_OUTCOME"] * 2
    assert len(refusals) == 1
    _assert_route_a_batch_refusal(refusals[0], "RuntimeError")


def test_route_a_batch_actual_containment_propagates_typed_capacity_and_base_exception():
    """Typed INVENTED exceptions prove propagation, not actual disk pressure."""
    capacity = execution.authority_spool.spool_quota.AuthoritySpoolQuotaError(
        "SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED", "INVENTED_PROPAGATION_ONLY")

    class InventedBoundary(BaseException):
        pass

    boundary = InventedBoundary("INVENTED_CONTROL_FLOW_BOUNDARY")
    for error in (capacity, boundary):
        with pytest.raises(type(error)) as caught:
            execution._route_a_contain_consumer_failure(error)
        assert caught.value is error


def test_route_a_unit_batch_normal_owner_two_keys_and_duplicate(tmp_path, monkeypatch,
        _route_a_owned_invented_native_source, record_property):
    """INVENTED owner integration; no ordinary full-attempt oracle or financial experiment."""
    path = tmp_path / "route_a_unit_batch_progress.jsonl"
    progress = _route_a_batch_progress(path)
    record_property("route_a_unit_batch_progress_path", str(path))
    record_property("fixture_provenance", "INVENTED_SOFTWARE_ONLY_NO_FINANCIAL_OR_PERFORMANCE_AUTHORITY")
    record_property("synthetic_source_input_provenance", _route_a_owned_invented_native_source.fixture_provenance)
    record_property("synthetic_native_input_root", str(_route_a_owned_invented_native_source.data_dir / "native"))
    record_property("source_inputs_reused", str(getattr(_route_a_owned_invented_native_source, "source_inputs_reused", False)).lower())
    retained, receipts = [], []
    progress("STARTED", "normal-owner-two-keys-plus-duplicate")

    def observe(*, evaluate_batch, witness, counts, post_header_counts, calls, restored):
        attempt, folds = _route_a_unambiguous_issued_attempt(witness)
        assert restored and len(folds) == search_planner.CAUSAL_FOLD_COUNT == 4
        original_candidate = copy.deepcopy(attempt.candidate)
        original_header = copy.deepcopy(witness["declared_header"])
        original_roster = copy.deepcopy(witness["capture"].sink.artifact_roster())
        plan = witness["cell_plan"].horizon_plan.to_dict()
        assert len(plan["candidates"]) == 1
        assert len(witness["attempts"]) == len(plan["attempts"]) == len(plan["trials"]) == len(calls.owner_unit_roster) > 1
        assert len(calls.owner_snapshots) == calls.owner_counts["owner_snapshot"] == 1
        assert calls.owner_counts["owner_rebind"] == calls.owner_counts["owner_key"] == 0
        snapshot = calls.owner_snapshots[0]
        assert snapshot["schema_version"] == "fxpm.route-a-owner-membership-snapshot.v2"
        assert snapshot["provenance_scope"] == "CURRENT_OWNER_METADATA_PROJECTION_ONLY"
        assert snapshot["original_trial_count"] == len(snapshot["attempt_roster"]) == len(witness["attempts"])
        assert snapshot["declared_unit_binding"] == calls.owner_binding_metadata
        assert execution._sha256([row.plan_attempt for row in witness["attempts"]]) == calls.owner_binding_metadata["attempt_roster_sha256"]
        assert execution._sha256(witness["fold_plan"]) == calls.owner_binding_metadata["fold_plan_sha256"]
        for name in ("execution_ready", "overall_execution_ready", "qualification_authority", "capital_authority",
                "paper_authority", "live_authority", "order_authority", "live_order_authority"):
            assert snapshot[name] is False
        assert not {"unit_metadata", "header_reference", "new_header_digest"} & set(snapshot)
        precommitments = witness["handle"].frozen.partition_cost_precommitments
        assert precommitments["schema_version"] == execution.raw_winner.PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA
        own_rows = {(row["attempt_ordinal"], row["fold_ordinal"], row["partition"]): row
            for row in precommitments["rows"]}
        assert len(own_rows) == len(precommitments["rows"])
        planned_count = rejected_count = own_partition_count = tier_fact_count = 0
        scheduled = []
        for ordinal, (row, owner_attempt) in enumerate(zip(snapshot["attempt_roster"], witness["attempts"], strict=True)):
            source_plan, source_folds = owner_attempt.plan_attempt, owner_attempt.candidate["folds"]
            assert row["ordinal"] == owner_attempt.ordinal == ordinal
            assert row["source_plan_attempt"] == source_plan == plan["attempts"][ordinal]
            assert execution._sha256(source_plan) == row["plan_attempt_sha256"]
            assert row["horizon_attempt_id"] == source_plan["attempt_id"]
            assert row["raw_attempt_id"] == calls.owner_unit_roster[ordinal]["raw_attempt_id"]
            assert row["source_folds"] == source_folds and len(source_folds) == len(row["folds"]) == 4
            assert row["parameters"] == owner_attempt.artifacts.trial_definition["parameters"]
            assert type(source_plan["state_context_disposition_sha256"]) is str
            assert len(source_plan["state_context_disposition_sha256"]) == 64
            expected_schedule = [value for value in plan["expected_evaluations"]
                if value["attempt_id"] == source_plan["attempt_id"]]
            assert row["scheduled_evaluations"] == expected_schedule
            scheduled.extend(row["scheduled_evaluations"])
            rejected = source_plan["status"] == "PROCESS_REJECTED"
            if rejected:
                rejected_count += 1
                assert row["rejection"] == {"plan_status": "PROCESS_REJECTED",
                    "reason_code": source_plan["reason_code"],
                    "state_context_disposition_sha256": source_plan["state_context_disposition_sha256"]}
                assert len(expected_schedule) == 1
                assert expected_schedule[0]["fold_id"] == "TERMINAL"
                assert expected_schedule[0]["role"] == "TERMINAL_MECHANICAL_REJECTION"
                assert expected_schedule[0]["expected_reason_code"] == source_plan["reason_code"]
            else:
                planned_count += 1
                assert source_plan["status"] == "PLANNED" and row["rejection"] is None
                assert len(expected_schedule) == 4
            for fold_ordinal, (projected, source_fold) in enumerate(zip(row["folds"], source_folds, strict=True)):
                assert len(source_fold) == 14 and source_fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER"
                assert execution._sha256({key: value for key, value in source_fold.items()
                    if key != "fold_definition_sha256"}) == source_fold["fold_definition_sha256"]
                assert projected["fold_id"] == source_fold["fold_id"] and projected["role"] == source_fold["role"]
                if rejected:
                    assert projected["partitions"] == {"status": "UNSCHEDULED"}
                    continue
                assert set(projected["partitions"]) == {"optimizer_train", "tuning_validation", "final_lockbox"}
                for role, identity in projected["partitions"].items():
                    expected_row = own_rows[ordinal, fold_ordinal, role]
                    geometry_sha = expected_row["partition_geometry_receipt_sha256"]
                    record = precommitments["records"][geometry_sha]
                    assert identity["precommitment_row"] == expected_row
                    assert identity["partition_geometry_receipt_sha256"] == geometry_sha
                    assert identity["partition_frame_sha256"] == record["partition_frame_sha256"]
                    assert set(record["tiers"]) == set(identity["tier_refs"]) == {"P75", "P95"}
                    assert identity["tier_refs"] == {tier: "|".join((precommitments["schema_version"],
                        precommitments["receipt_sha256"], geometry_sha, tier)) for tier in ("P75", "P95")}
                    own_partition_count += 1
                    tier_fact_count += len(record["tiers"])
        assert scheduled == plan["expected_evaluations"]
        assert planned_count + rejected_count == len(witness["attempts"])
        assert own_partition_count == planned_count * 4 * 3 and tier_fact_count == own_partition_count * 2
        payload_bytes = len(execution._canonical_bytes({name: snapshot[name]
            for name in ("frozen_domains", "attempt_roster", "declared_unit_binding")}))
        wrapper_bytes = len(execution._canonical_bytes(snapshot))
        assert payload_bytes <= execution.winner.MAX_REQUEST_BYTES
        owner_snapshot_receipt = {"original_trials": len(witness["attempts"]), "horizon_candidates": 1,
            "planned_trials": planned_count, "rejected_trials": rejected_count,
            "terminal_rejection_branch": "OBSERVED" if rejected_count else "UNOBSERVED_ZERO_ACTUAL_REJECTIONS",
            "all_rejected_unsearched_entry": "UNPROVEN_NO_JUSTIFIED_NORMALLY_ISSUED_FIXTURE",
            "planned_v2_own_partition_rows": own_partition_count, "both_tier_facts": tier_fact_count,
            "validated_three_field_payload_bytes": payload_bytes, "full_snapshot_wrapper_bytes": wrapper_bytes,
            "existing_payload_limit_bytes": execution.winner.MAX_REQUEST_BYTES, "wrapper_cap": "NONE_ADDED",
            "snapshot_geometry_calls": calls.owner_geometry["snapshot_geometry"],
            "snapshot_monotonic_observations": [copy.deepcopy(row) for row in calls.owner_intervals
                if row["interval"] == "owner_snapshot"],
            "default_prefix": copy.deepcopy(calls.default_prefix_receipts),
            "default_post_loop_gate_and_finish": "UNPROVEN_NO_EXTRA_OWNER_PREPARATION_ADDED"}
        assert calls.owner_geometry["snapshot_geometry"] == 0
        candidate = copy.deepcopy(attempt.candidate)
        candidate["folds"] = copy.deepcopy(list(folds[:1]))
        candidate["fixture_provenance"] = "INVENTED_ONE_FOLD_QA_REFERENCE_NOT_ISSUED_ATTEMPT"
        reference_attempt = replace(attempt, candidate=candidate)
        plan = witness["cell_plan"].horizon_plan.to_dict()
        reference_before = dict(calls.counts)
        reference_auth_before = dict(counts)
        progress("STARTED_ORDINARY_BUILDER_REFERENCE", "INVENTED_ONE_FOLD_QA_CARRIER_SIX_LANES")
        reference = execution._build_planned_attempt_evidence(attempt=reference_attempt,
            raw_attempt_id="INVENTED_ONE_FOLD_ORDINARY_REFERENCE_NOT_ISSUED_ATTEMPT",
            plan_body=plan, plan_receipt_sha256=witness["cell_plan"].horizon_plan.receipt_sha256,
            fitted=witness["refreshed"], config=witness["config"],
            partition_cost_precommitments=witness["handle"].frozen.partition_cost_precommitments,
            source_observation_references=None)
        assert len(reference["folds"]) == 1
        assert calls.counts["model_run"] - reference_before.get("model_run", 0) == 6
        assert calls.counts["runtime_strategy"] - reference_before.get("runtime_strategy", 0) == 1
        assert calls.counts["surface"] - reference_before.get("surface", 0) == 1
        assert calls.counts["pool_lookup"] - reference_before.get("pool_lookup", 0) == 0
        assert counts == reference_auth_before
        reference_outputs = copy.deepcopy(calls.outputs)
        assert attempt.candidate == original_candidate
        assert witness["declared_header"] == original_header
        assert witness["capture"].sink.artifact_roster() == original_roster
        progress("COMPLETED_ORDINARY_BUILDER_REFERENCE", "INVENTED_ONE_FOLD_QA_CARRIER_SIX_LANES")
        reference_deltas = {name: calls.counts[name] - reference_before.get(name, 0)
            for name in ("model_run", "runtime_strategy", "surface", "pool_lookup")}
        specifications = [_route_a_batch_specification(witness, attempt, tier="P75"),
            _route_a_batch_specification(witness, attempt, tier="P95"),
            _route_a_batch_specification(witness, attempt, tier="P75")]
        specifications[2]["bookkeeping"] = {"attempt_ordinal": 999,
            "horizon_attempt_id": "INVENTED_BOOKKEEPING_NEVER_SELECTS"}
        before = copy.deepcopy(specifications)
        admitted = dict(counts)
        assert len(calls.executor_admission_before) == (2 if restored else 1)
        assert {name: admitted[name] - post_header_counts[name] for name in counts} == {
            name: 0 for name in counts}
        expected_normal_owner = {name: (int(restored) if name == "_verify_declared_slot_frozen_chain" else 1)
            for name in counts}
        assert {name: admitted[name] - calls.executor_admission_before[-1][name]
            for name in counts} == expected_normal_owner
        before_model_counts = dict(calls.counts)
        before_batch_instances = len(calls.instances)
        before_owner_counts = dict(calls.owner_counts)
        before_owner_geometry = dict(calls.owner_geometry)
        before_owner_intervals = len(calls.owner_intervals)
        batch = evaluate_batch(specifications=specifications,
            metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1")
        assert batch["status"] == "EVALUATED_DIAGNOSTIC_ONLY"
        assert calls.owner_counts["owner_snapshot"] - before_owner_counts.get("owner_snapshot", 0) == 0
        assert calls.owner_counts["owner_rebind"] - before_owner_counts.get("owner_rebind", 0) == 3
        assert calls.owner_counts["owner_key"] - before_owner_counts.get("owner_key", 0) == 3
        owner_geometry_deltas = {name: calls.owner_geometry[name] - before_owner_geometry.get(name, 0)
            for name in ("snapshot_geometry", "selected_rebind_geometry", "uncached_working_geometry")}
        assert owner_geometry_deltas == {"snapshot_geometry": 0, "selected_rebind_geometry": 3,
            "uncached_working_geometry": 2}
        first_batch_owner_intervals = copy.deepcopy(calls.owner_intervals[before_owner_intervals:])
        assert all(row["observed_duration_ns"] >= 0 for row in first_batch_owner_intervals)
        assert calls.counts["full_capture"] - before_model_counts.get("full_capture", 0) == 0
        assert calls.counts["unit_binding"] - before_model_counts.get("unit_binding", 0) == 0
        successful_commit_deltas = {name: calls.counts[name] - before_model_counts.get(name, 0)
            for name in ("full_capture", "unit_binding")}
        assert specifications == before and counts == admitted
        assert batch["schema_version"] == "fxpm.route-a-unit-batch-diagnostic.v1"
        assert batch["request_to_unique"] == [0, 1, 0]
        assert len(batch["requests"]) == 3 and len(batch["unique_evaluations"]) == 2
        assert [row["unique_evaluation_ordinal"] for row in batch["requests"]] == [0, 1, 0]
        assert [row["cache_hit"] for row in batch["requests"]] == [False, False, True]
        assert [row["request_ordinal"] for row in batch["requests"]] == [0, 1, 2]
        assert batch["requests"][2]["bookkeeping"] == specifications[2]["bookkeeping"]
        assert all(row["matched_attempt_ordinal"] == attempt.ordinal for row in batch["requests"])
        assert all(row["matched_horizon_attempt_id"] == attempt.plan_attempt["attempt_id"]
            for row in batch["requests"])
        for row, tier in zip(batch["requests"][:2], ("P75", "P95")):
            expected_lane = reference["folds"][0]["lanes"]["optimizer_train_" + tier.lower()]
            assert execution._canonical_bytes(row["decision_lane"]) == execution._canonical_bytes(expected_lane)
            geometry = row["shared_evaluation_key"]["evaluation_binding"]["partition_geometry"]["receipt_sha256"]
            assert row["signal_vector"].equals(reference_outputs[geometry, tier]["signals"])
        assert attempt.candidate == original_candidate and len(execution._causal_folds(attempt)) == 4
        assert batch["requests"][0]["shared_evaluation_key"] == batch["requests"][2]["shared_evaluation_key"]
        assert batch["requests"][0]["signal_vector"].equals(batch["requests"][2]["signal_vector"])
        assert batch["requests"][0]["signal_vector"] is not batch["requests"][2]["signal_vector"]
        original_signal = batch["requests"][2]["signal_vector"].copy(deep=True)
        batch["requests"][0]["signal_vector"].iloc[0] = 123456
        assert batch["requests"][2]["signal_vector"].equals(original_signal)
        assert len(batch["pair_checks"]) == 1 and batch["pair_checks"][0]["status"] == "VERIFIED"
        assert all(row["signal_comparison"] == "VERIFIED" for row in batch["requests"])
        assert calls.unique_intervals == [0, 1]
        assert all(snapshot == admitted for snapshot in calls.auth_snapshots)
        assert all(calls.counts[name] - before_model_counts.get(name, 0) == 2
            for name in ("model_run", "runtime_strategy", "surface"))
        assert calls.counts["pool_lookup"] == 0
        batch_instances = calls.instances[before_batch_instances:]
        assert len(batch_instances) == 2
        assert batch_instances[0][0] is not batch_instances[1][0]
        assert batch_instances[0][1] is not batch_instances[1][1]
        assert all(preparation is None for _, _, preparation in batch_instances)
        assert all(strategy is not previous_strategy and backtester is not previous_backtester
            for strategy, backtester, _ in batch_instances
            for previous_strategy, previous_backtester, _ in calls.instances[:before_batch_instances])
        for name in ("execution_ready", "overall_execution_ready", "qualification_authority", "capital_authority",
                "paper_authority", "live_authority", "order_authority", "live_order_authority"):
            assert batch[name] is False
            assert all(row[name] is False for row in batch["requests"])
        retained.append(evaluate_batch)
        terminal_specifications = copy.deepcopy(specifications[:2])
        original_clock = execution._now_utc_seconds
        original_scope = execution._route_a_require_scope_current
        terminal_window_returns, advanced = [], []
        before_terminal = dict(calls.counts)
        window_end = witness["capture"].request.to_dict()["source_access_window_utc"]["end_exclusive_utc"]

        def scope_then_advance_clock(**arguments):
            result = original_scope(**arguments)
            terminal_window_returns.append(True)
            if not advanced:
                advanced.append(True)
                progress("ADVANCED_TEST_CLOCK_AFTER_ACTUAL_WINDOW_CHECK", "ORIGINAL_WINDOW_END_NO_WAIT")
            return result

        def clock():
            return window_end if advanced else original_clock()

        with monkeypatch.context() as scoped:
            scoped.setattr(execution, "_route_a_require_scope_current", scope_then_advance_clock)
            scoped.setattr(execution, "_now_utc_seconds", clock)
            refused = evaluate_batch(specifications=terminal_specifications,
                metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1")
        _assert_route_a_batch_refusal(refused, "ROUTE_A_UNIT_BATCH_SOURCE_WINDOW_CLOSED")
        assert advanced == [True] and terminal_window_returns
        assert all(calls.counts[name] == before_terminal[name]
            for name in ("model_run", "runtime_strategy", "surface"))
        terminal_commit_deltas = {name: calls.counts[name] - before_terminal.get(name, 0)
            for name in ("full_capture", "unit_binding")}
        assert terminal_commit_deltas == {"full_capture": 0, "unit_binding": 0}
        assert counts == admitted
        _assert_route_a_batch_refusal(evaluate_batch(specifications=specifications,
            metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1"), "ROUTE_A_UNIT_BATCH_CLOSED")
        progress("COMPLETED_TERMINAL_WINDOW_EXPIRY", "ATOMIC_REFUSAL_NO_NEW_MODELS_NO_CLOCK_WAIT")
        receipts.append({"named_interval": "SECOND_NORMAL_EXECUTOR_RESTORE_THEN_POST_LOOP_CONSUMER_AND_TWO_UNIQUE_KEYS",
            "adoption_count": 1, "original_header_count": 1,
            "owner_membership_snapshot": owner_snapshot_receipt,
            "first_successful_batch_product_geometry_calls": owner_geometry_deltas,
            "first_successful_batch_monotonic_observations": first_batch_owner_intervals,
            "monotonic_observation_claim": "OBSERVED_INTERVALS_ONLY_NO_SPEEDUP_RESOURCE_OR_RUNTIME_FORECAST",
            "ordinary_reference_fixture": "INVENTED_ONE_FOLD_QA_CARRIER_NOT_ISSUED_ATTEMPT",
            "ordinary_reference_deltas": reference_deltas,
            "batch_evaluation_deltas": {name: calls.counts[name] - before_model_counts.get(name, 0)
                for name in ("model_run", "runtime_strategy", "surface", "pool_lookup")},
            "ordinary_reference_comparison": "SAME_CURRENT_RESTORED_OWNER_TRAIN_P75_P95_LANES_AND_SIGNALS",
            "normal_executor_admission_delta": {name: admitted[name] - calls.executor_admission_before[-1][name]
                for name in counts},
            "broader_issuer_counts_at_consumer": admitted,
            "post_loop_adoption_delta": {name: admitted[name] - post_header_counts[name] for name in counts},
            "successful_batch_commit_deltas": successful_commit_deltas,
            "late_hash_refused_batch_commit_deltas": terminal_commit_deltas,
            "late_hash_clock_advance_observed": bool(advanced),
            "actual_restore_returns": calls.restore_returns,
            "actual_normal_source_reopen_calls": calls.counts["source_reopen"],
            "consumer_pool_depth": calls.pool_depth,
            "after_batch_delta": {name: counts[name] - admitted[name] for name in counts},
            "request_to_unique": batch["request_to_unique"], "pool_lookup": calls.counts["pool_lookup"],
            "unique_key_sha256": [hashlib.sha256(execution._canonical_bytes(row["shared_evaluation_key"])).hexdigest()
                for row in batch["unique_evaluations"]],
            "fixture_provenance": "INVENTED_SOFTWARE_ONLY_NO_GLOBAL_CALL_OR_SPEED_CLAIM"})

    observed = _observe_owned_invented_route_a_consumer(tmp_path, monkeypatch,
        source_access_window_utc=_route_a_owned_utc_window(),
        native_fixture=_route_a_owned_invented_native_source, observe=observe, progress=progress,
        restore_first=True, refuse_outer_consumer=True)
    assert observed["normal_finish_entry_observed"] is True and observed["restored_fresh_software_header"] is True
    assert len(retained) == 1
    _assert_route_a_batch_refusal(retained[0](specifications=[],
        metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1"), "ROUTE_A_UNIT_BATCH_CLOSED")
    def observe_second_owner(*, evaluate_batch, witness, counts, post_header_counts, calls, restored):
        assert len(calls.executor_admission_before) == (2 if restored else 1)
        assert {name: counts[name] - post_header_counts[name] for name in counts} == {
            name: 0 for name in counts}
        _assert_route_a_batch_refusal(retained[0](specifications=[],
            metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1"), "ROUTE_A_UNIT_BATCH_CLOSED")
        admitted = dict(counts)
        before_commit = dict(calls.counts)
        empty = evaluate_batch(specifications=[], metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1")
        assert calls.counts["full_capture"] - before_commit.get("full_capture", 0) == 0
        assert calls.counts["unit_binding"] - before_commit.get("unit_binding", 0) == 0
        assert empty["requests"] == empty["request_to_unique"] == empty["unique_evaluations"] == empty["pair_checks"] == []
        assert counts == admitted and calls.unique_intervals == []
        assert calls.counts["model_run"] == calls.counts["runtime_strategy"] == calls.counts["surface"] == 0
        assert calls.counts["pool_lookup"] == 0 and calls.instances == []
        for name in ("execution_ready", "overall_execution_ready", "qualification_authority", "capital_authority",
                "paper_authority", "live_authority", "order_authority", "live_order_authority"):
            assert empty[name] is False
        retained.append(evaluate_batch)
        progress("COMPLETED_CROSS_OWNER_REFUSAL", "closed-original-A-inside-normally-issued-B")

    _observe_owned_invented_route_a_consumer(tmp_path / "second-owner", monkeypatch,
        source_access_window_utc=_route_a_owned_utc_window(),
        native_fixture=_route_a_owned_invented_native_source,
        observe=observe_second_owner, progress=progress)
    assert len(retained) == 2
    _assert_route_a_batch_refusal(retained[1](specifications=[],
        metadata_recipe="ROUTE_A_APPLIED_SPREAD_POLICY_V1"), "ROUTE_A_UNIT_BATCH_CLOSED")
    import pickle
    for transfer in (copy.copy, copy.deepcopy, pickle.dumps):
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            transfer(retained[1])
        assert caught.value.code == "ROUTE_A_UNIT_BATCH_CAPABILITY_NOT_TRANSFERABLE"
    record_property("route_a_unit_batch_receipts", execution._canonical_bytes(receipts).decode("ascii"))
    progress("COMPLETED", "normal-owner-two-keys-plus-duplicate")


def test_route_a_callback_identity_dispatch_preserves_legacy_predicate():
    def observed():
        return None

    def other():
        return 1

    code = observed.__code__
    separate_equal_code = code.replace()
    assert separate_equal_code is not code
    assert separate_equal_code == code
    pairs = (("observed", code), ("alias", code), ("observed", code),
             ("other", other.__code__))
    labels = _route_a_callback_identity_dispatch(pairs)
    unknown = object()
    for event in ("call", "return", "exception", "c_call", "unknown", None):
        for frame_code in (code, other.__code__, separate_equal_code, unknown, None):
            expected = tuple(name for name, candidate in pairs
                             if event == "call" and frame_code is candidate)
            assert labels(frame_code, event) == expected



# INVENTED plain mapping metadata; no authentication, model or financial evidence.
def _request_mapping_seal(body, field):
    import json
    sealed = copy.deepcopy(body)
    sealed[field] = hashlib.sha256(json.dumps(sealed, sort_keys=True,
        separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()
    return sealed


def _request_mapping_inputs():
    parameters = {"fast_period": 10, "slow_period": 20,
        "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}
    binding_fields = ("generation_context_sha256", "producer_request_sha256",
        "executor_release_sha256", "producer_source_sha256", "horizon_plan_receipt_sha256",
        "cell_plan_receipt_sha256", "source_group_sha256", "regime_fit_sha256",
        "frame_sha256", "fitted_state_reference_sha256", "execution_spec_sha256",
        "frozen_receipt_sha256", "cost_precommitments_sha256", "fold_plan_sha256",
        "attempt_roster_sha256")
    unit = {"declared_unit_binding": {name: "a" * 64 for name in binding_fields},
        "header_reference": {"schema_version": "INVENTED_HEADER_REFERENCE_ONLY", "sha256": "b" * 64},
        "source_access_window_utc": {"start_utc": "2020-01-01T00:00:00+00:00",
            "end_exclusive_utc": "2020-01-02T00:00:00+00:00"},
        "cell": {"symbol": "INVENTED_SYMBOL", "timeframe": "D1", "regime": "INVENTED_REGIME"},
        "anchor": {"anchor_id": "INVENTED_ANCHOR", "selection_cutoff_utc": "2020-01-01T00:00:00+00:00"},
        "search_process_receipt_sha256": "c" * 64, "config_sha256": "d" * 64,
        "effective_config_sha256": "e" * 64}
    domain = _request_mapping_seal({"strategy_id": "EMACrossoverStrategy",
        "strategy_family": "INVENTED_FAMILY", "parameter_domain": {
            "schema_version": "fxpm.parameter-domain.v1", "defaults": copy.deepcopy(parameters),
            "axes": [{"names": list(parameters), "choices": [[10, 20, 2.0, 2.0], [11, 21, 2.0, 2.0]]}],
            "point_count": 2}}, "domain_sha256")
    folds = []
    for ordinal in range(4):
        partitions = {}
        for role in ("optimizer_train", "tuning_validation", "final_lockbox"):
            geometry = _request_mapping_seal({"schema_version": "fxpm.winner-partition-geometry.v1",
                "role": role, "frame_start_utc": "2019-12-31T00:00:00+00:00",
                "scored_start_utc": "2020-01-01T00:00:00+00:00",
                "end_exclusive_utc": "2020-01-02T00:00:00+00:00",
                "warmup_bars": 1, "scored_bars": 2, "frame_rows": 3,
                "frame_sha256": str(ordinal + 1) * 64}, "receipt_sha256")
            partitions[role] = {"geometry": geometry,
                "tier_refs": {tier: f"INVENTED_{ordinal}_{role}_{tier}" for tier in ("P75", "P95")}}
        folds.append({"fold_id": f"INVENTED_FOLD_{ordinal}",
            "role": "CAUSAL_ROLLING_ORIGIN_INNER", "partitions": partitions})
    planned = {"ordinal": 0, "raw_attempt_id": "INVENTED_RAW_0",
        "horizon_attempt_id": "INVENTED_HORIZON_0", "plan_attempt_sha256": "1" * 64,
        "status": "PLANNED", "strategy_id": "EMACrossoverStrategy",
        "parameters": copy.deepcopy(parameters), "causal_fold_count": 4,
        "folds": copy.deepcopy(folds), "rejection": None}
    rejected = copy.deepcopy(planned)
    rejected.update(ordinal=1, raw_attempt_id="INVENTED_RAW_1", horizon_attempt_id="INVENTED_HORIZON_1",
        plan_attempt_sha256="2" * 64, status="PROCESS_REJECTED",
        rejection={"kind": "PROCESS_REJECTED", "code": "INVENTED_PRESEARCH_REJECTION",
            "details": ["INVENTED_SOFTWARE_ONLY"]})
    specification = {"strategy_id": "EMACrossoverStrategy", "parameters": copy.deepcopy(parameters),
        "fold_ordinal": 0, "partition_role": "optimizer_train", "spread_tier": "P75", "bookkeeping": {}}
    specifications = [copy.deepcopy(specification) for _ in range(3)]
    specifications[1]["spread_tier"] = "P95"
    specifications[2]["bookkeeping"] = {"attempt_ordinal": 999,
        "horizon_attempt_id": "INVENTED_NONSELECTING_BOOKKEEPING"}
    return {"unit_metadata": unit, "frozen_domains": [domain],
        "attempt_roster": [planned, rejected], "original_trial_count": 2,
        "specifications": specifications}


def _request_mapping_normalized_specification(inputs):
    return {**copy.deepcopy(inputs["specifications"][0]), "request_ordinal": 0}


def _request_mapping_add_planned(inputs, parameters=None, fold_count=4):
    row = copy.deepcopy(inputs["attempt_roster"][0])
    ordinal = len(inputs["attempt_roster"])
    row.update(ordinal=ordinal, raw_attempt_id=f"INVENTED_RAW_{ordinal}",
        horizon_attempt_id=f"INVENTED_HORIZON_{ordinal}", plan_attempt_sha256=f"{ordinal + 1:x}" * 64,
        causal_fold_count=fold_count, folds=copy.deepcopy(row["folds"][:fold_count]))
    if parameters is not None:
        row["parameters"] = copy.deepcopy(parameters)
    inputs["attempt_roster"].append(row)
    inputs["original_trial_count"] += 1
    return row


def _request_mapping_assert_no_authority(result):
    for name in ("execution_ready", "overall_execution_ready", "qualification_authority",
            "capital_authority", "paper_authority", "live_authority", "order_authority", "live_order_authority"):
        assert result[name] is False


def test_route_a_request_mapping_preserves_rejected_twin_and_request_order():
    inputs = _request_mapping_inputs()
    before = copy.deepcopy(inputs)
    result = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert result["original_trial_count"] == 2
    assert result["attempt_roster"] == before["attempt_roster"]
    assert result["counts"] == {"original_trials": 2, "planned_trials": 1,
        "rejected_trials": 1, "requests": 3, "structural_groups": 2}
    assert result["request_to_group"] == [0, 1, 0]
    assert len(result["requests"]) == 3 and len(result["structural_groups"]) == 2
    assert [row["bookkeeping"] for row in result["requests"]] == [row["bookkeeping"] for row in before["specifications"]]
    assert inputs == before
    _request_mapping_assert_no_authority(result)


def test_route_a_request_mapping_partial_coverage_keeps_lockbox_ordinary_only():
    inputs = _request_mapping_inputs()
    result = execution._compile_route_a_request_attempt_mapping(**inputs)
    planned, rejected = result["coverage_by_attempt"]
    assert len(planned["expected_batch_lanes"]) == 16
    assert len(planned["requested_unique_lanes"]) == 2
    assert len(planned["missing_batch_lanes"]) == 14
    assert len(planned["ordinary_only_lockbox_lanes"]) == 8
    for name in ("expected_batch_lanes", "requested_unique_lanes", "missing_batch_lanes", "ordinary_only_lockbox_lanes"):
        assert rejected[name] == []
    inputs["specifications"] = []
    empty = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert empty["requests"] == empty["request_to_group"] == empty["structural_groups"] == []
    assert len(empty["coverage_by_attempt"][0]["missing_batch_lanes"]) == 16
    assert empty["original_trial_count"] == 2
    _request_mapping_assert_no_authority(empty)


_REQUEST_MAPPING_SITES = {
    "M1": ("_route_a_resolve_request_membership", 7392, "ROUTE_A_MAPPING_REQUEST_INVALID"),
    "M2": ("_route_a_resolve_request_membership", 7396, "ROUTE_A_MAPPING_DOMAIN_UNAVAILABLE"),
    "M3": ("_route_a_resolve_request_membership", 7415, "ROUTE_A_MAPPING_MATCH_DATA_INVALID"),
    "M4-unavailable": ("_route_a_resolve_request_membership", 7417, "ROUTE_A_MAPPING_MEMBERSHIP_UNAVAILABLE"),
    "M4-ambiguous": ("_route_a_resolve_request_membership", 7417, "ROUTE_A_MAPPING_MEMBERSHIP_AMBIGUOUS"),
    "C1": ("_compile_route_a_request_attempt_mapping", 7718, "ROUTE_A_MAPPING_INPUT_INVALID"),
    "C2": ("_route_a_validate_mapping_snapshot", 7495, "ROUTE_A_MAPPING_DOMAINS_INVALID"),
    "C3": ("_route_a_validate_mapping_snapshot", 7647, "ROUTE_A_MAPPING_ROSTER_INVALID"),
    "C4": ("_compile_route_a_request_attempt_mapping", 7786, "ROUTE_A_MAPPING_RESULT_TOO_LARGE"),
    "R-request": ("_route_a_batch_specifications", 7217, "ROUTE_A_UNIT_BATCH_REQUEST_INVALID"),
    "R-lockbox": ("_route_a_batch_specifications", 7219, "ROUTE_A_UNIT_BATCH_LOCKBOX_REFUSED"),
    "R-role": ("_route_a_batch_specifications", 7221, "ROUTE_A_UNIT_BATCH_ROLE_INVALID"),
    "R-tier": ("_route_a_batch_specifications", 7223, "ROUTE_A_UNIT_BATCH_TIER_INVALID"),
}


def _request_mapping_expect_site(call, site_id):
    function, line, code = _REQUEST_MAPPING_SITES[site_id]
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        call()
    assert caught.value.code == code
    trace = caught.value.__traceback__
    expected_file = execution._compile_route_a_request_attempt_mapping.__code__.co_filename
    reached = False
    while trace is not None:
        current_code = trace.tb_frame.f_code
        if current_code.co_filename == expected_file and current_code.co_name == function and trace.tb_lineno == line:
            reached = True
        trace = trace.tb_next
    assert reached, f"INVENTED fixture did not reach refusal site {site_id}"


@pytest.mark.parametrize("case_id", (
    "M1-normalized-ordinal", "M2-domain-absent", "M3-not-domain-member",
    "M4-zero-match", "M4-planned-ambiguity", "M4-own-fold-bound",
    "C1-nonplain-input", "C2-domain-digest", "C3-partition-shape", "C4-result-size",
), ids=(
    "M1-normalized-ordinal", "M2-domain-absent", "M3-not-domain-member",
    "M4-zero-match", "M4-planned-ambiguity", "M4-own-fold-bound",
    "C1-nonplain-input", "C2-domain-digest", "C3-partition-shape", "C4-result-size",
))
def test_route_a_request_mapping_each_new_refusal_site(case_id):
    """INVENTED conditions; actual traceback-site evidence requires an authorized run."""
    inputs = _request_mapping_inputs()
    specification = _request_mapping_normalized_specification(inputs)
    direct_member = case_id.startswith("M")
    if case_id == "M1-normalized-ordinal":
        specification["request_ordinal"] = True
        site = "M1"
    elif case_id == "M2-domain-absent":
        inputs["frozen_domains"] = []
        site = "M2"
    elif case_id == "M3-not-domain-member":
        specification["parameters"]["fast_period"] = 999
        site = "M3"
    elif case_id == "M4-zero-match":
        specification["parameters"].update(fast_period=11, slow_period=21)
        site = "M4-unavailable"
    elif case_id == "M4-planned-ambiguity":
        _request_mapping_add_planned(inputs)
        specification["bookkeeping"] = {"attempt_ordinal": 0, "horizon_attempt_id": "INVENTED_HORIZON_0"}
        site = "M4-ambiguous"
    elif case_id == "M4-own-fold-bound":
        inputs["attempt_roster"][0]["folds"] = inputs["attempt_roster"][0]["folds"][:1]
        inputs["attempt_roster"][0]["causal_fold_count"] = 1
        _request_mapping_add_planned(inputs, {
            "fast_period": 11, "slow_period": 21, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0})
        # The other attempt has four causal folds but different parameters.
        inputs["attempt_roster"][-1]["folds"] = copy.deepcopy(inputs["attempt_roster"][1]["folds"])
        inputs["attempt_roster"][-1]["causal_fold_count"] = 4
        specification["fold_ordinal"] = 1
        site = "M4-unavailable"
    elif case_id == "C1-nonplain-input":
        inputs["unit_metadata"]["cell"]["symbol"] = object()
        site = "C1"
    elif case_id == "C2-domain-digest":
        inputs["frozen_domains"][0]["domain_sha256"] = "0" * 64
        site = "C2"
    elif case_id == "C3-partition-shape":
        del inputs["attempt_roster"][0]["folds"][0]["partitions"]["optimizer_train"]["geometry"]
        site = "C3"
    elif case_id == "C4-result-size":
        import json
        inputs["unit_metadata"]["header_reference"]["schema_version"] = (
            "INVENTED_LARGE_METADATA_ONLY_" + "x" * (winner.MAX_REQUEST_BYTES // 2))
        assert len(json.dumps({**inputs, "metadata_recipe": "ROUTE_A_APPLIED_SPREAD_POLICY_V1"},
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")) < winner.MAX_REQUEST_BYTES
        site = "C4"
    else:
        raise AssertionError("Unknown INVENTED refusal fixture")
    if direct_member:
        call = lambda: execution._route_a_resolve_request_membership(
            specification=specification, frozen_domains=inputs["frozen_domains"], attempt_roster=inputs["attempt_roster"])
    else:
        call = lambda: execution._compile_route_a_request_attempt_mapping(**inputs)
    _request_mapping_expect_site(call, site)


@pytest.mark.parametrize("case_id", (
    "unit-extra", "unit-hash", "unit-window", "nonfinite", "dict-subclass", "recipe",
    "trial-count-bool", "trial-count-mismatch", "unused-domain", "duplicate-domain",
    "ordinal", "duplicate-raw", "planned-rejection", "rejected-reason", "fold-count",
    "missing-partition", "missing-tier", "geometry-role", "geometry-count-bool",
    "geometry-row-sum", "geometry-digest", "noncausal-partitions",
    "request-lockbox", "request-ordinal", "request-fold-bool", "request-role", "request-tier",
), ids=(
    "unit-extra", "unit-hash", "unit-window", "nonfinite", "dict-subclass", "recipe",
    "trial-count-bool", "trial-count-mismatch", "unused-domain", "duplicate-domain",
    "ordinal", "duplicate-raw", "planned-rejection", "rejected-reason", "fold-count",
    "missing-partition", "missing-tier", "geometry-role", "geometry-count-bool",
    "geometry-row-sum", "geometry-digest", "noncausal-partitions",
    "request-lockbox", "request-ordinal", "request-fold-bool", "request-role", "request-tier",
))
def test_route_a_request_mapping_strict_plain_data_geometry_and_domains(case_id):
    inputs = _request_mapping_inputs()
    attempt = inputs["attempt_roster"][0]
    partition = attempt["folds"][0]["partitions"]["optimizer_train"]
    site = "C1"
    if case_id == "unit-extra":
        inputs["unit_metadata"]["execution_ready"] = True
    elif case_id == "unit-hash":
        inputs["unit_metadata"]["config_sha256"] = "INVENTED_NOT_A_HASH"
    elif case_id == "unit-window":
        inputs["unit_metadata"]["source_access_window_utc"]["start_utc"] = None
    elif case_id == "nonfinite":
        inputs["specifications"][0]["bookkeeping"]["invalid"] = float("nan")
    elif case_id == "dict-subclass":
        class InventedDict(dict):
            pass
        inputs["unit_metadata"] = InventedDict(inputs["unit_metadata"])
    elif case_id == "recipe":
        inputs["metadata_recipe"] = "INVENTED_DIFFERENT_RECIPE"
    elif case_id == "trial-count-bool":
        inputs["original_trial_count"] = True
    elif case_id == "trial-count-mismatch":
        inputs["original_trial_count"] += 1
    elif case_id == "unused-domain":
        domain = copy.deepcopy(inputs["frozen_domains"][0])
        domain.pop("domain_sha256")
        domain["strategy_id"] = "INVENTED_UNUSED_STRATEGY"
        domain["parameter_domain"]["point_count"] = 999
        inputs["frozen_domains"].append(_request_mapping_seal(domain, "domain_sha256"))
        site = "C2"
    elif case_id == "duplicate-domain":
        inputs["frozen_domains"].append(copy.deepcopy(inputs["frozen_domains"][0]))
        site = "C2"
    elif case_id == "ordinal":
        attempt["ordinal"] = 1
        site = "C3"
    elif case_id == "duplicate-raw":
        inputs["attempt_roster"][1]["raw_attempt_id"] = attempt["raw_attempt_id"]
        site = "C3"
    elif case_id == "planned-rejection":
        attempt["rejection"] = copy.deepcopy(inputs["attempt_roster"][1]["rejection"])
        site = "C3"
    elif case_id == "rejected-reason":
        inputs["attempt_roster"][1]["rejection"] = None
        site = "C3"
    elif case_id == "fold-count":
        attempt["causal_fold_count"] = 3
        site = "C3"
    elif case_id == "missing-partition":
        del attempt["folds"][0]["partitions"]["final_lockbox"]
        site = "C3"
    elif case_id == "missing-tier":
        del partition["tier_refs"]["P95"]
        site = "C3"
    elif case_id in {"geometry-role", "geometry-count-bool", "geometry-row-sum", "geometry-digest"}:
        geometry = partition["geometry"]
        geometry.pop("receipt_sha256")
        if case_id == "geometry-role":
            geometry["role"] = "tuning_validation"
        elif case_id == "geometry-count-bool":
            geometry["warmup_bars"] = True
        elif case_id == "geometry-row-sum":
            geometry["frame_rows"] = 4
        partition["geometry"] = _request_mapping_seal(geometry, "receipt_sha256")
        if case_id == "geometry-digest":
            partition["geometry"]["receipt_sha256"] = "0" * 64
        site = "C3"
    elif case_id == "noncausal-partitions":
        attempt["folds"][0]["role"] = "INVENTED_NONCAUSAL"
        attempt["causal_fold_count"] = 3
        site = "C3"
    elif case_id == "request-lockbox":
        inputs["specifications"][0]["partition_role"] = "final_lockbox"
        site = "R-lockbox"
    elif case_id == "request-ordinal":
        inputs["specifications"][0]["request_ordinal"] = 0
        site = "R-request"
    elif case_id == "request-fold-bool":
        inputs["specifications"][0]["fold_ordinal"] = True
        site = "R-request"
    elif case_id == "request-role":
        inputs["specifications"][0]["partition_role"] = "INVENTED_ROLE"
        site = "R-role"
    elif case_id == "request-tier":
        inputs["specifications"][0]["spread_tier"] = "P99"
        site = "R-tier"
    else:
        raise AssertionError("Unknown INVENTED semantic rejection")
    _request_mapping_expect_site(lambda: execution._compile_route_a_request_attempt_mapping(**inputs), site)



def test_route_a_request_mapping_decodes_registry_tuple_defaults_before_matching(monkeypatch):
    """INVENTED tuple-template provider; metadata construction is not evaluation."""
    inputs = _request_mapping_inputs()
    templates = {"pair": (1, 2)}
    reads = []
    def metadata_defaults(strategy_id):
        assert strategy_id == "INVENTED_TUPLE_DEFAULTS"
        reads.append(strategy_id)
        return SimpleNamespace(get_default_params=lambda: templates)
    monkeypatch.setattr(execution.StrategyRegistry, "get", staticmethod(metadata_defaults))
    domain = {"strategy_id": "INVENTED_TUPLE_DEFAULTS", "strategy_family": "INVENTED_TUPLE_FAMILY",
        "parameter_domain": {"schema_version": "fxpm.parameter-domain.v1", "defaults": {"pair": [1, 2]},
            "axes": [{"names": ["pair"], "choices": [[[1, 2]], [[2, 3]]]}], "point_count": 2}}
    inputs["frozen_domains"] = [_request_mapping_seal(domain, "domain_sha256")]
    for row in inputs["attempt_roster"]:
        row["strategy_id"], row["parameters"] = "INVENTED_TUPLE_DEFAULTS", {"pair": [1, 2]}
    for row in inputs["specifications"]:
        row["strategy_id"], row["parameters"] = "INVENTED_TUPLE_DEFAULTS", {"pair": [1, 2]}
    decoded = execution.decode_parameter_mapping({"pair": [1, 2]}, templates)
    normalized = search_planner.validate_parameter_domain_member(domain["parameter_domain"], {"pair": [1, 2]})
    assert decoded == {"pair": (1, 2)} and normalized == {"pair": [1, 2]} and decoded != normalized
    result = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert result["request_to_group"] == [0, 1, 0]
    assert all(row["parameters"] == {"pair": [1, 2]} for row in result["requests"])
    assert reads and templates == {"pair": (1, 2)}
    _request_mapping_assert_no_authority(result)


def test_route_a_request_mapping_reports_holes_for_each_original_attempt():
    inputs = _request_mapping_inputs()
    alternate = {"fast_period": 11, "slow_period": 21, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}
    _request_mapping_add_planned(inputs, alternate, fold_count=2)
    request = copy.deepcopy(inputs["specifications"][0])
    request.update(parameters=alternate, fold_ordinal=1, partition_role="tuning_validation",
        spread_tier="P95", bookkeeping={"attempt_ordinal": 0})
    inputs["specifications"].append(request)
    result = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert result["original_trial_count"] == 3 and result["counts"]["planned_trials"] == 2
    assert result["request_to_group"] == [0, 1, 0, 2]
    assert result["requests"][-1]["attempt_ordinal"] == 2
    coverage = result["coverage_by_attempt"]
    assert [(len(row["expected_batch_lanes"]), len(row["requested_unique_lanes"]),
        len(row["missing_batch_lanes"]), len(row["ordinary_only_lockbox_lanes"]))
        for row in coverage] == [(16, 2, 14, 8), (0, 0, 0, 0), (8, 1, 7, 4)]
    assert all(lane["attempt_ordinal"] == 2 for lane in coverage[2]["missing_batch_lanes"])
    assert all(lane["partition_role"] == "final_lockbox" for lane in coverage[2]["ordinary_only_lockbox_lanes"])
    assert result["requests"][-1]["ordinary_lane"] == "tuning_validation_p95"
    _request_mapping_assert_no_authority(result)


def test_route_a_request_mapping_ignores_noncausal_fold_positions():
    inputs = _request_mapping_inputs()
    inputs["attempt_roster"][0]["folds"].insert(0,
        {"fold_id": "INVENTED_NONCAUSAL", "role": "INVENTED_NONCAUSAL", "partitions": {}})
    result = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert all(row["fold_id"] == "INVENTED_FOLD_0" for row in result["requests"])
    assert len(result["coverage_by_attempt"][0]["expected_batch_lanes"]) == 16
    assert all(lane["fold_id"] != "INVENTED_NONCAUSAL"
        for lane in result["coverage_by_attempt"][0]["expected_batch_lanes"])


@pytest.mark.parametrize("component", (
    "unit-header", "unit-window-text", "unit-config", "raw-id", "horizon-id", "plan-receipt",
    "fold", "geometry", "tier-reference", "tier", "parameters", "role", "family-domain",
), ids=(
    "unit-header", "unit-window-text", "unit-config", "raw-id", "horizon-id", "plan-receipt",
    "fold", "geometry", "tier-reference", "tier", "parameters", "role", "family-domain",
))
def test_route_a_request_mapping_full_identity_changes_with_bound_metadata(component):
    import json
    original = _request_mapping_inputs()
    original["specifications"] = original["specifications"][:1]
    changed = copy.deepcopy(original)
    attempt = changed["attempt_roster"][0]
    specification = changed["specifications"][0]
    if component == "unit-header":
        changed["unit_metadata"]["header_reference"]["sha256"] = "f" * 64
    elif component == "unit-window-text":
        changed["unit_metadata"]["source_access_window_utc"]["start_utc"] = "2020-01-01T00:00:00Z"
    elif component == "unit-config":
        changed["unit_metadata"]["config_sha256"] = "f" * 64
    elif component == "raw-id":
        attempt["raw_attempt_id"] = "INVENTED_OTHER_RAW"
    elif component == "horizon-id":
        attempt["horizon_attempt_id"] = "INVENTED_OTHER_HORIZON"
    elif component == "plan-receipt":
        attempt["plan_attempt_sha256"] = "f" * 64
    elif component == "fold":
        specification["fold_ordinal"] = 1
    elif component == "geometry":
        partition = attempt["folds"][0]["partitions"]["optimizer_train"]
        geometry = partition["geometry"]
        geometry.pop("receipt_sha256")
        geometry["frame_sha256"] = "f" * 64
        partition["geometry"] = _request_mapping_seal(geometry, "receipt_sha256")
    elif component == "tier-reference":
        attempt["folds"][0]["partitions"]["optimizer_train"]["tier_refs"]["P75"] = "INVENTED_OTHER_TIER_REFERENCE"
    elif component == "tier":
        specification["spread_tier"] = "P95"
    elif component == "parameters":
        alternate = {"fast_period": 11, "slow_period": 21, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0}
        attempt["parameters"] = copy.deepcopy(alternate)
        specification["parameters"] = copy.deepcopy(alternate)
    elif component == "role":
        specification["partition_role"] = "tuning_validation"
    elif component == "family-domain":
        domain = changed["frozen_domains"][0]
        domain.pop("domain_sha256")
        domain["strategy_family"] = "INVENTED_OTHER_FAMILY"
        changed["frozen_domains"][0] = _request_mapping_seal(domain, "domain_sha256")
    else:
        raise AssertionError("Unknown INVENTED identity component")
    before = execution._compile_route_a_request_attempt_mapping(**original)
    after = execution._compile_route_a_request_attempt_mapping(**changed)
    left, right = before["structural_groups"][0], after["structural_groups"][0]
    assert left["structural_identity_json"] != right["structural_identity_json"]
    assert json.loads(right["structural_identity_json"])["metadata_recipe"] == "ROUTE_A_APPLIED_SPREAD_POLICY_V1"
    assert "bookkeeping" not in json.loads(right["structural_identity_json"])
    assert before["request_to_group"] == after["request_to_group"] == [0]
    _request_mapping_assert_no_authority(before)
    _request_mapping_assert_no_authority(after)


def test_route_a_request_mapping_display_digest_collision_cannot_merge_identities(monkeypatch):
    monkeypatch.setattr(execution, "_sha256", lambda _value: "f" * 64)
    result = execution._compile_route_a_request_attempt_mapping(**_request_mapping_inputs())
    assert result["request_to_group"] == [0, 1, 0]
    assert len(result["structural_groups"]) == 2
    assert {row["display_sha256"] for row in result["structural_groups"]} == {"f" * 64}
    assert result["structural_groups"][0]["structural_identity_json"] != result["structural_groups"][1]["structural_identity_json"]


def test_route_a_request_mapping_determinism_and_bidirectional_mutation_isolation():
    inputs = _request_mapping_inputs()
    inputs["specifications"][0]["bookkeeping"] = {"nested": {"labels": ["INVENTED_A"]}}
    inputs["specifications"][2]["bookkeeping"] = {"nested": {"labels": ["INVENTED_DUPLICATE"]}}
    snapshot = copy.deepcopy(inputs)
    first = execution._compile_route_a_request_attempt_mapping(**inputs)
    second = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert first == second and inputs == snapshot
    inputs["unit_metadata"]["cell"]["symbol"] = "INVENTED_CHANGED_INPUT"
    inputs["specifications"][0]["bookkeeping"]["nested"]["labels"].append("CHANGED")
    inputs["attempt_roster"][1]["rejection"]["details"].append("CHANGED")
    assert first["unit_metadata"] == snapshot["unit_metadata"]
    assert first["attempt_roster"] == snapshot["attempt_roster"]
    assert first["requests"][0]["bookkeeping"] == snapshot["specifications"][0]["bookkeeping"]
    first["requests"][0]["bookkeeping"]["nested"]["labels"][0] = "CHANGED_OUTPUT"
    first["requests"][0]["partition_geometry"]["frame_start_utc"] = "CHANGED_OUTPUT"
    first["attempt_roster"][0]["folds"][0]["partitions"]["optimizer_train"]["geometry"]["warmup_bars"] = 100
    assert first["requests"][2]["bookkeeping"]["nested"]["labels"] == ["INVENTED_DUPLICATE"]
    assert first["requests"][2]["partition_geometry"]["frame_start_utc"] == "2019-12-31T00:00:00+00:00"
    assert first["requests"][0]["partition_geometry"]["warmup_bars"] == 1
    assert second["unit_metadata"] == snapshot["unit_metadata"]
    assert second["attempt_roster"] == snapshot["attempt_roster"]
    assert inputs["specifications"][0]["bookkeeping"]["nested"]["labels"] == ["INVENTED_A", "CHANGED"]
    assert snapshot["specifications"][0]["bookkeeping"]["nested"]["labels"] == ["INVENTED_A"]


def test_route_a_request_mapping_is_metadata_only_and_does_not_route_evaluation(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("INVENTED pure compiler attempted evaluation, frame geometry, source I/O or clock access")
    for name in ("_runtime_strategy", "_fold_partitions", "_file_sha256", "_now_utc_seconds", "Backtester"):
        monkeypatch.setattr(execution, name, forbidden)
    inputs = _request_mapping_inputs()
    result = execution._compile_route_a_request_attempt_mapping(**inputs)
    assert result["status"] == "MAPPED_METADATA_ONLY"
    assert result["provenance_scope"] == "CALLER_SUPPLIED_METADATA_ONLY"
    assert result["unit_metadata"]["source_access_window_utc"] == inputs["unit_metadata"]["source_access_window_utc"]
    assert result["unit_metadata"]["anchor"]["selection_cutoff_utc"].endswith("+00:00")
    assert result["partition_responsibilities"]["final_lockbox"] == "ORDINARY_ONLY_NOT_ROUTABLE"
    assert "shared_evaluation_key" not in result and all("shared_evaluation_key" not in row for row in result["requests"])
    _request_mapping_assert_no_authority(result)


# INVENTED source carriers exercise metadata consistency, not authenticated owner issuance.
def _owner_snapshot_inputs(monkeypatch):
    literal = {'provenance': 'INVENTED_SOFTWARE_ONLY_INERT_CARRIER_NO_AUTHORITY_NO_FINANCIAL_EVIDENCE',
     'declared_unit_binding': {'generation_context_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'producer_request_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'executor_release_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'producer_source_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'horizon_plan_receipt_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'cell_plan_receipt_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'source_group_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'regime_fit_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'frame_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'fitted_state_reference_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'execution_spec_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'frozen_receipt_sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                               'cost_precommitments_sha256': '1b907d4cda6d50ee3c47fa1608f34c863501ce0d6658e25a0ead14fa32a3c881',
                               'fold_plan_sha256': '4490ba9f111d444fc0e134e572436dd58b14f9dde0c11a1c3e1c85e4a43cb0fd',
                               'attempt_roster_sha256': '5e5c6dfac887378a081c1d21dbddc739e5112a452019e1f743f100b072f3a586'},
     'frozen_domains': [{'strategy_id': 'EMACrossoverStrategy',
                         'strategy_family': 'INVENTED_FAMILY',
                         'parameter_domain': {'schema_version': 'fxpm.parameter-domain.v1',
                                              'defaults': {'fast_period': 10,
                                                           'slow_period': 20,
                                                           'sl_atr_mult': 2.0,
                                                           'tp_atr_mult': 2.0},
                                              'axes': [{'names': ['fast_period',
                                                                  'slow_period',
                                                                  'sl_atr_mult',
                                                                  'tp_atr_mult'],
                                                        'choices': [[10, 20, 2.0, 2.0], [11, 21, 2.0, 2.0]]}],
                                              'point_count': 2},
                         'domain_sha256': '2d49c19d048d757ddc3c23db2d70c7e76ffda730edfa6a19b59511e584810e48'}],
     'plan_body': {'cell': {'symbol': 'INVENTED_SYMBOL', 'timeframe': 'D1', 'regime': 'INVENTED_REGIME'},
                   'candidates': [{'candidate_id': 'INVENTED-CANDIDATE',
                                   'candidate_definition_sha256': '4d16813b7a97f88861d6357f0b4d7c937d44016e20423267a565504551bdb749',
                                   'folds': [{'fold_id': 'INVENTED-CAUSAL-01',
                                              'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                              'fit_start_utc': '2018-10-03T00:00:00+00:00',
                                              'fit_end_exclusive_utc': '2019-01-01T00:00:00+00:00',
                                              'validation_warmup_start_utc': '2018-12-31T00:00:00+00:00',
                                              'validation_start_utc': '2019-01-01T00:00:00+00:00',
                                              'validation_end_exclusive_utc': '2019-03-05T00:00:00+00:00',
                                              'right_fit_start_utc': None,
                                              'right_fit_end_exclusive_utc': None,
                                              'fit_rows': 90,
                                              'validation_rows': 63,
                                              'source_slice_sha256': 'a1cfa36be3d2a5d735dbd681babcad9c78ecf4b6eb26e52242f81f13465c42ab',
                                              'final_oos_authority': False,
                                              'fold_definition_sha256': '38e32b6ac27858f0e6f4b75efdc9a3f510fb3f9dd0393cf962995bb6caef3404'},
                                             {'fold_id': 'INVENTED-CAUSAL-02',
                                              'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                              'fit_start_utc': '2018-12-05T00:00:00+00:00',
                                              'fit_end_exclusive_utc': '2019-03-05T00:00:00+00:00',
                                              'validation_warmup_start_utc': '2019-03-04T00:00:00+00:00',
                                              'validation_start_utc': '2019-03-05T00:00:00+00:00',
                                              'validation_end_exclusive_utc': '2019-05-07T00:00:00+00:00',
                                              'right_fit_start_utc': None,
                                              'right_fit_end_exclusive_utc': None,
                                              'fit_rows': 90,
                                              'validation_rows': 63,
                                              'source_slice_sha256': '8232ee597e78ea5d1ec1a845b456e9930bfcc6a786d3e27340fa08c8f16b2563',
                                              'final_oos_authority': False,
                                              'fold_definition_sha256': '8b516df70fed00f385b16799bd29dc65cf882b196076db96512d012b5096f0c5'},
                                             {'fold_id': 'INVENTED-CAUSAL-03',
                                              'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                              'fit_start_utc': '2019-02-06T00:00:00+00:00',
                                              'fit_end_exclusive_utc': '2019-05-07T00:00:00+00:00',
                                              'validation_warmup_start_utc': '2019-05-06T00:00:00+00:00',
                                              'validation_start_utc': '2019-05-07T00:00:00+00:00',
                                              'validation_end_exclusive_utc': '2019-07-09T00:00:00+00:00',
                                              'right_fit_start_utc': None,
                                              'right_fit_end_exclusive_utc': None,
                                              'fit_rows': 90,
                                              'validation_rows': 63,
                                              'source_slice_sha256': '0fcb0e2812f61d3f1efba3cb5a3bacb1eefe758bb070c15bce6465d91ffc2157',
                                              'final_oos_authority': False,
                                              'fold_definition_sha256': '0358d91fd3b274c868280c9bf96f723207d0bb384f96dc915e095f506f5da640'},
                                             {'fold_id': 'INVENTED-CAUSAL-04',
                                              'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                              'fit_start_utc': '2019-04-10T00:00:00+00:00',
                                              'fit_end_exclusive_utc': '2019-07-09T00:00:00+00:00',
                                              'validation_warmup_start_utc': '2019-07-08T00:00:00+00:00',
                                              'validation_start_utc': '2019-07-09T00:00:00+00:00',
                                              'validation_end_exclusive_utc': '2019-09-10T00:00:00+00:00',
                                              'right_fit_start_utc': None,
                                              'right_fit_end_exclusive_utc': None,
                                              'fit_rows': 90,
                                              'validation_rows': 63,
                                              'source_slice_sha256': '46cb25bb9cdbd286092620e64d5a7da2cfc0579f4da287789a85e3910095ba9a',
                                              'final_oos_authority': False,
                                              'fold_definition_sha256': 'ce34e0242789b1be41b66f38be7a9ebdfde82226c585f6ce2dab00cdf9859f5a'}]}],
                   'trials': [{'trial_id': 'INVENTED-TRIAL-0'}, {'trial_id': 'INVENTED-TRIAL-1'}],
                   'attempts': [{'attempt_id': 'INVENTED-HORIZON-0',
                                 'candidate_id': 'INVENTED-CANDIDATE',
                                 'candidate_definition_sha256': '4d16813b7a97f88861d6357f0b4d7c937d44016e20423267a565504551bdb749',
                                 'trial_id': 'INVENTED-TRIAL-0',
                                 'trial_definition_sha256': '42d42e458c33cbc3a09c69c579b916ed4023e1cba6bac206a9d892b18ee4397b',
                                 'strategy_id': 'EMACrossoverStrategy',
                                 'strategy_release_sha256': '1111111111111111111111111111111111111111111111111111111111111111',
                                 'parameter_set_sha256': '1764b05366fdd4d87c32b565ab1fb3a21b62e6813017309b3a8d0867e2370063',
                                 'status': 'PLANNED',
                                 'reason_code': None,
                                 'state_context_disposition_sha256': '737b764327e768a6826eb1e63cd58a6dae5a0519af707f00a021e458675eef48'},
                                {'attempt_id': 'INVENTED-HORIZON-1',
                                 'candidate_id': 'INVENTED-CANDIDATE',
                                 'candidate_definition_sha256': '4d16813b7a97f88861d6357f0b4d7c937d44016e20423267a565504551bdb749',
                                 'trial_id': 'INVENTED-TRIAL-1',
                                 'trial_definition_sha256': 'faa6d8fcbbdd71a393dec04cb00564da3d04bfccf4f79552a13c37462166a1e1',
                                 'strategy_id': 'EMACrossoverStrategy',
                                 'strategy_release_sha256': '1111111111111111111111111111111111111111111111111111111111111111',
                                 'parameter_set_sha256': '1764b05366fdd4d87c32b565ab1fb3a21b62e6813017309b3a8d0867e2370063',
                                 'status': 'PROCESS_REJECTED',
                                 'reason_code': 'STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL',
                                 'state_context_disposition_sha256': '52d41ef53aa1acced85644b57a57924ce785c0fb1f38c1baf3bb185013776663'}],
                   'expected_evaluations': [{'ordinal': 0,
                                             'attempt_id': 'INVENTED-HORIZON-0',
                                             'fold_id': 'INVENTED-CAUSAL-01',
                                             'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                             'validation_start_utc': '2019-01-01T00:00:00+00:00',
                                             'validation_end_exclusive_utc': '2019-03-05T00:00:00+00:00',
                                             'expected_status': 'EVALUATION_REQUIRED',
                                             'expected_reason_code': None,
                                             'downstream_causal_eligible': True,
                                             'final_oos_authority': False,
                                             'context_sha256': '6f62a66a2e2482b45ca0f041856f8fcd81ffed5eaeb3a0b697105fa35c6c7994'},
                                            {'ordinal': 1,
                                             'attempt_id': 'INVENTED-HORIZON-0',
                                             'fold_id': 'INVENTED-CAUSAL-02',
                                             'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                             'validation_start_utc': '2019-03-05T00:00:00+00:00',
                                             'validation_end_exclusive_utc': '2019-05-07T00:00:00+00:00',
                                             'expected_status': 'EVALUATION_REQUIRED',
                                             'expected_reason_code': None,
                                             'downstream_causal_eligible': True,
                                             'final_oos_authority': False,
                                             'context_sha256': 'd01ebf458d44471095ac0ec91cce7917627cf4f6cba702678aca7939571b98da'},
                                            {'ordinal': 2,
                                             'attempt_id': 'INVENTED-HORIZON-0',
                                             'fold_id': 'INVENTED-CAUSAL-03',
                                             'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                             'validation_start_utc': '2019-05-07T00:00:00+00:00',
                                             'validation_end_exclusive_utc': '2019-07-09T00:00:00+00:00',
                                             'expected_status': 'EVALUATION_REQUIRED',
                                             'expected_reason_code': None,
                                             'downstream_causal_eligible': True,
                                             'final_oos_authority': False,
                                             'context_sha256': '11ff9669ce3caf9f906f09a02979ecb9b77d7985469610e20cdcd9c61fc30558'},
                                            {'ordinal': 3,
                                             'attempt_id': 'INVENTED-HORIZON-0',
                                             'fold_id': 'INVENTED-CAUSAL-04',
                                             'role': 'CAUSAL_ROLLING_ORIGIN_INNER',
                                             'validation_start_utc': '2019-07-09T00:00:00+00:00',
                                             'validation_end_exclusive_utc': '2019-09-10T00:00:00+00:00',
                                             'expected_status': 'EVALUATION_REQUIRED',
                                             'expected_reason_code': None,
                                             'downstream_causal_eligible': True,
                                             'final_oos_authority': False,
                                             'context_sha256': 'd61208f1a5119f85d3145657f3beb676a2b292aa4833b61100685fa51b0dc082'},
                                            {'ordinal': 4,
                                             'attempt_id': 'INVENTED-HORIZON-1',
                                             'fold_id': 'TERMINAL',
                                             'role': 'TERMINAL_MECHANICAL_REJECTION',
                                             'validation_start_utc': None,
                                             'validation_end_exclusive_utc': None,
                                             'expected_status': 'PROCESS_REJECTED',
                                             'expected_reason_code': 'STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL',
                                             'downstream_causal_eligible': False,
                                             'final_oos_authority': False,
                                             'context_sha256': 'c9feb5736da8a6bbf9776d51230a100c25c21b47fad5591a05c5338bc82d12e4'}]},
     'precommitments': {'schema_version': 'fxpm.winner-partition-cost-precommitments.v2',
                        'frozen_tier_pair_contract_sha256': '8888888888888888888888888888888888888888888888888888888888888888',
                        'rows': [{'attempt_ordinal': 0,
                                  'fold_ordinal': 0,
                                  'partition': 'optimizer_train',
                                  'partition_geometry_receipt_sha256': '78e33700ee09b2fa89b7d7ad1d62fc62c6dd2a44c4317d3f9ac93ecb3b8fecaf'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 0,
                                  'partition': 'tuning_validation',
                                  'partition_geometry_receipt_sha256': '073c55a63b1c14a6db6fbf1819f4cfd8c0273e2b9c7248a5c9b1e0e84c99fca3'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 0,
                                  'partition': 'final_lockbox',
                                  'partition_geometry_receipt_sha256': '8f23b6086ad88e0fe8c7f0aaa7b5906cdcc2a319e086a53139221fdfdcac9660'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 1,
                                  'partition': 'optimizer_train',
                                  'partition_geometry_receipt_sha256': '3fb7b82b9cbb2e7739fd8d338917430586037655efd5e33976b085dfc83e6412'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 1,
                                  'partition': 'tuning_validation',
                                  'partition_geometry_receipt_sha256': 'b90d7b9767b67d3443bb8656eee72582f4b4f5639adf8bfda6f73de362c6ebf2'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 1,
                                  'partition': 'final_lockbox',
                                  'partition_geometry_receipt_sha256': '023984080e9968adf1d455470c75ac06e2614c0bc218e59537d19611222a219c'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 2,
                                  'partition': 'optimizer_train',
                                  'partition_geometry_receipt_sha256': '5caaa46e4a8d20fd55602d076a41aeeeb5ecae5a9356936e680cb16e340a55a4'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 2,
                                  'partition': 'tuning_validation',
                                  'partition_geometry_receipt_sha256': '035a3b3772ab07a10407ca30da8480f94d363691c18fa94e3bac695d96b8c7c1'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 2,
                                  'partition': 'final_lockbox',
                                  'partition_geometry_receipt_sha256': '8abb3a001dddcabb76bb5b81abec2398ecddba10aef6bf6254f8670f9927f6d5'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 3,
                                  'partition': 'optimizer_train',
                                  'partition_geometry_receipt_sha256': 'd97393f9f468704833d41affda38fbabf48537b7344db468324d1b5b86546f0e'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 3,
                                  'partition': 'tuning_validation',
                                  'partition_geometry_receipt_sha256': '6e8736d1fcc728d8d94660652b6a7b2b44ea49b93a9b9205ea875591a8165fd1'},
                                 {'attempt_ordinal': 0,
                                  'fold_ordinal': 3,
                                  'partition': 'final_lockbox',
                                  'partition_geometry_receipt_sha256': '10eee714d437386768b6755ae3a7ec35099ec765d7d93ddba433b10daa1a70f5'}],
                        'records': {'78e33700ee09b2fa89b7d7ad1d62fc62c6dd2a44c4317d3f9ac93ecb3b8fecaf': {'partition_frame_sha256': 'cad40cf8a8b88e782eca1b3c65b97dd896ebdfd4ea210efaa3b91c83e6ed3a7a',
                                                                                                         'source_observation_receipt_sha256': '6393f584eb6f7abcb17a38e48c10620722c3b9c4d680e8cd1106630e0ae6f895',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'aa3a8346550b97912cbcc307b1fe72ec2e728d36bf745055e44f0360cf67c3be',
                                                                                                                           'intrabar_half_spread_values_sha256': '64718124eb917d98f708610321e6df0965fbb0c478f9853666049bd0587e2f03',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '94d213b8b884fd0d9ded78c5c6271d786653e67d0945d47985f2a8c7caaf6709',
                                                                                                                           'entry_observations_sha256': 'cc655a6fb3bc5a75e3cae3b3d06e8604b233db5afdab931990f1bdb1041792b0',
                                                                                                                           'entry_admission_mask_sha256': '340c606e364a2c3d1cb5c08e909ec69e7a911f68271040c9f1eba541920b8f81'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': '7d8ef5d3c262a08b9de162212a86ae7db20244631d45858d2e7273734cc330cd',
                                                                                                                           'intrabar_half_spread_values_sha256': '56bbe64ac46ad1c2e5d5499a2fed1fc91f21bc351c74b02f5743dba1e83adabc',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '8475f8fe154942d09b2937b80ba17f3f17ab6509bfb20a5f669691293920af5a',
                                                                                                                           'entry_observations_sha256': '0e6efd81d76e91519617c9f12d8601afde6ebd6065267a0cca023e62674d2ec2',
                                                                                                                           'entry_admission_mask_sha256': '582f17b4421e6a86cb820003aeb1d3b0d41027bb2345ee1e8055bfe7d53734af'}}},
                                    '073c55a63b1c14a6db6fbf1819f4cfd8c0273e2b9c7248a5c9b1e0e84c99fca3': {'partition_frame_sha256': '8117bc446d1120e5d6a79fdc76c248c88bcd498cc3ae59237a9d62a86103c183',
                                                                                                         'source_observation_receipt_sha256': '0cf35b33fe9b27530f9711f9c6baef08ee9f1523986a631eefda35f72da774a0',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': '0ab93a9cc31c8538e0de1ba960c72fdb9d9cca0d93afd97759d5aa08c0db5627',
                                                                                                                           'intrabar_half_spread_values_sha256': '920cc488c460f2836c1bd63b524aad51cb15cd1fe07895738859e4631dd12c5a',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '35932feb5d9e2d07cf798ed1d335b3f51532496c268d29a63f048873f12c7139',
                                                                                                                           'entry_observations_sha256': '8d7d48bfe0e61bd9c69bd6b72a84aa29f6d178d4902d133224a5ee93bae58a83',
                                                                                                                           'entry_admission_mask_sha256': 'fa96282775c69396b5bf4b414b09a0010d044d3685dbd86f23443e3de8f75efc'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'd0bfd79da0bf99351edb85ed8d5da2a7f3494610f48454f3f1e4fa23a2445f55',
                                                                                                                           'intrabar_half_spread_values_sha256': '2f5865397e6c1bc561c74eb8a5938fcbab08dc9876748d3303266d999c83bf18',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '1a0be63b1d4073e8e47e59bbb99c5428c5ba1f8a85f48c8df62c78d358cb752f',
                                                                                                                           'entry_observations_sha256': '587c63f847f54bec46d8ae8dccc015ff107b6b684acc7c0af0075c3e39b10d2c',
                                                                                                                           'entry_admission_mask_sha256': 'ec608319a6397b8937f6afcd69702b7f3276b4cf39b3d97ae584762324a75f54'}}},
                                    '8f23b6086ad88e0fe8c7f0aaa7b5906cdcc2a319e086a53139221fdfdcac9660': {'partition_frame_sha256': '615d741dc75a18eb71f4546a19f27770741f8ba1acebf9af8c0ac5637b93b420',
                                                                                                         'source_observation_receipt_sha256': '6f7f0d31face70b657a15a48abdad8f29ae5a9fdc199acc9279292ee3ca17c7e',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'a997677b5033e3e4347c38b92cbe297cf98fd9133e5f64e22fc553f5e2454b9a',
                                                                                                                           'intrabar_half_spread_values_sha256': '17c1af7e24d0763bc1caef04c0d6fdb9cb4cf08b2a4370205ac52b9c67aeb744',
                                                                                                                           'numerical_execution_cost_attestation_sha256': 'a518270b17711d856076bcc516e077f3c08a52f01d96f6240571adf8d294ed74',
                                                                                                                           'entry_observations_sha256': '04cdee81476af777e8ae9b57568ee7fc31a79e8c55584aa76a0e69756076d148',
                                                                                                                           'entry_admission_mask_sha256': 'f79a2362cdfb986d650884c14b22a3f5fd5e9a8c0a82f38f59a9729f06572daa'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'acef5450cfacc19a521fc6739a867c5db59c1f0d00d7541f8d77c351995dbb4e',
                                                                                                                           'intrabar_half_spread_values_sha256': '7d867f348f25ad7e030b6249b9e990857501d930e291e3a3660284c67d4cba41',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '1f7014aeea45b3705503818fc259cffdd221cded6e995e247ab7ededb929feaf',
                                                                                                                           'entry_observations_sha256': 'd5711ec0d56c411cc3200b6877b388c9ec022711e00f481b9784a9fb0927e515',
                                                                                                                           'entry_admission_mask_sha256': '9f4f7ba93cde621f130046bfb4d879af84817869bc0fdb935ea691f3b574384a'}}},
                                    '3fb7b82b9cbb2e7739fd8d338917430586037655efd5e33976b085dfc83e6412': {'partition_frame_sha256': '59fdd2920e11fc2a0a21bb847d732b23a5447359a8ab17f7384c3b565517cfc0',
                                                                                                         'source_observation_receipt_sha256': '96a4edcbd9b8e68f88c9cccc90d2c56b65f4b1bc635a8f2bf69b6c9a1c6f84c4',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'ab51275e6b2e61eb7c2c96f10cbaa06a94ad09caee17be2fe830d0e276e68726',
                                                                                                                           'intrabar_half_spread_values_sha256': 'a1bb2c01197472cdc13540b46adc213287b510390750923e2cf0d2a655cd2034',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '55a70d08fc551db3e8c454e992749f9f60a7e269870da740d2dc44f0ddbf24fa',
                                                                                                                           'entry_observations_sha256': 'f6095bae57149c434704a06c7d72fc3626550de2f9cf4ae6fc295262ecfe9b1f',
                                                                                                                           'entry_admission_mask_sha256': 'a0cc6f0380d1e52ba8a6e26579ab25bf9f9f81662e7cf4f44e78304f8e8b5564'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': '0b830979a8bc386c08163f48b5f185a324f32336489a05c9310a160d4a75d7cb',
                                                                                                                           'intrabar_half_spread_values_sha256': '587d28032b2d41c632abb669ed4467dbbb35c4542c65cf5b374b7edd81a4cdb0',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '683fe4475efb27818fbe2bbe2f28d13910ec0d921276782e7b973fade200682d',
                                                                                                                           'entry_observations_sha256': '1f37dea029d073d27a1c478e530578c7782ca77e8e87f30418c10ae37ac29575',
                                                                                                                           'entry_admission_mask_sha256': '3a47de3f5e425a13d134a707326cc57b075d6ff89a40ead36bdba3edacdfa1d0'}}},
                                    'b90d7b9767b67d3443bb8656eee72582f4b4f5639adf8bfda6f73de362c6ebf2': {'partition_frame_sha256': '7446c973160f641c59628b0ef268bbda0ecbb2bdb5b4300e7554f7e482f2f5ec',
                                                                                                         'source_observation_receipt_sha256': '05d4263fc432703782bff60484732fff7187a5098995c909f1ebe2bc72d10d12',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'fb0eecabb58c13b52822534f54fd5a653485c3e1959e51c9a1dab467c8ea39a6',
                                                                                                                           'intrabar_half_spread_values_sha256': 'f271c57df17a7fe8209d0a7577d65ea61d56c92a00e533553c7637a0ded81a8e',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '70264b79da9cdc715e08a39adc70c4ead73de109ee38740ded9cfc0683ffd92c',
                                                                                                                           'entry_observations_sha256': '7275ff4a1ae76476d506c38b8696b6ceef9b02ee9e9a12c5caaa628861c0f7f8',
                                                                                                                           'entry_admission_mask_sha256': 'eb3b65320918489d3f0992b5c777d3e74f6c8f7bb6ddee206565cce23fb20ad5'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'de2a0fb1e1e0bc8e76915d3861746b5da6069b288644322b53a69fd33e3e096f',
                                                                                                                           'intrabar_half_spread_values_sha256': '84b75fb351686e68a88973758a64dd3538c0e7594f89e63abbe2bd457de0264d',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '7c358122aacf848d91666bd6ee49e7b9d906e652081ccec9cac84d81e6275c4e',
                                                                                                                           'entry_observations_sha256': '6120143ad2f7cbd35906b96919f72cab2aa69f309c6be2d273dba5ca6a6ea120',
                                                                                                                           'entry_admission_mask_sha256': 'c8af5fd36f260b98efc909aaedca04ef302553566932dc0a4eb22d5bbe3b6973'}}},
                                    '023984080e9968adf1d455470c75ac06e2614c0bc218e59537d19611222a219c': {'partition_frame_sha256': '3a659857d5f2c4f7d94a4d5ba39a3a4942efae35d50fc533a37e7ecddbe30608',
                                                                                                         'source_observation_receipt_sha256': 'a759a8a34ad37585b8365689a693199f0f14f8a7989157655bf9501d2438ed1f',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'fa4c42779610b70a271048a7487edd69791e60ca0057b7a00ff5aa0057ae6a3c',
                                                                                                                           'intrabar_half_spread_values_sha256': '88ef1f28ee3cc0c9db17e6ca5b25afc70ac45ca105376535dcdc13f551c6aa9f',
                                                                                                                           'numerical_execution_cost_attestation_sha256': 'bc7d4d81889794fcaad49b5ec6ace91a3c971b66c81ac6a7d5d9947f5827c439',
                                                                                                                           'entry_observations_sha256': '02cca38791705a0085d8c3698bbbd39e745ff31aec0fdbbd3216807c444340a6',
                                                                                                                           'entry_admission_mask_sha256': 'acc25423bab015448a211248de629e84328bcecb20612e79bff50f93f55b65cb'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': '7543a4b8efa917e673da7eea4d3092f8081f122ba83adbd9bd907c87f9f5e225',
                                                                                                                           'intrabar_half_spread_values_sha256': '134f0d423e5a6b10be7ff86973731099a4232fb6aaa3105e548aee0babca9850',
                                                                                                                           'numerical_execution_cost_attestation_sha256': 'a2356f7a523bc52c4f22ca49a72f92c9477938b2f7e7b4de4fb34b9717f1adac',
                                                                                                                           'entry_observations_sha256': '673db9300b99d2a5ea6a5801feedebe3b217e9448d53c4233378aaece882d38b',
                                                                                                                           'entry_admission_mask_sha256': '7a16032e060d9fc32fb0ce9dd73db6c8a64aa1ef21aa0b8afd0a236e82d99bd5'}}},
                                    '5caaa46e4a8d20fd55602d076a41aeeeb5ecae5a9356936e680cb16e340a55a4': {'partition_frame_sha256': 'c132afcd2f02b5e4b9ac22d19c75bdd576a092426af884901e3844ec8e7a616d',
                                                                                                         'source_observation_receipt_sha256': '5e720e01a99b0a9933dfdaff46a978de6253e2595d8cb566c484041889b46a70',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': '75d73b9a545312a7c8cd56df7aacd5395ae9608f46bf38641c74de492c25abb0',
                                                                                                                           'intrabar_half_spread_values_sha256': '951d69284686beb87994a530c7e332d1de591c3c56af953f9fc40278695934ef',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '781895d7df6b91cabfa2eb54a1b9443f57453e4e467bf88e70f3b94c7e46fbf5',
                                                                                                                           'entry_observations_sha256': '74061eff046f1ef90e472bec0fe8d569f59a1d05122ddd5e86895c0c07a4d9c0',
                                                                                                                           'entry_admission_mask_sha256': 'a389632cb23a674a3f6957bc5b7c69890e8d509df80a642f7a769618071344a8'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': '1b21914f1ef717c68ad278f6f9df6bb46b37496fb7aa8e517507b8990fc11f6d',
                                                                                                                           'intrabar_half_spread_values_sha256': '9481bd375ff35ede3910985620544d31b66200285cd886c907d78ec72529ce67',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '966484440ee2609585b7258e603c0f376f65671bfbeb17c76f97e4bbab896be3',
                                                                                                                           'entry_observations_sha256': '70127b00192048c7e128d73d0ee63a73585bb333fb9f349ecd309806a48573de',
                                                                                                                           'entry_admission_mask_sha256': '18dee978644691ebbe48bc6cca73fd3af63ab01d5d0894b123e931b4a1e91514'}}},
                                    '035a3b3772ab07a10407ca30da8480f94d363691c18fa94e3bac695d96b8c7c1': {'partition_frame_sha256': 'ea117b41616bdca568d4fdb5450d4d573abd6a1068dd80a7235049acbb0c421a',
                                                                                                         'source_observation_receipt_sha256': '6dc5daa5765db2aa0cdb3f7d17f8b40ea260cfa9444f9020e5318a239f325224',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': '6d75632a707b72e3d6bc326e17b4db037d8335f0c5d2aad5c62318d77ab86312',
                                                                                                                           'intrabar_half_spread_values_sha256': '64b5c2111a620b2f6cf04284c1070b922e6426704e4c37bab1e1d122050da825',
                                                                                                                           'numerical_execution_cost_attestation_sha256': 'd31c30c42eb0fe8e6d8d7c22a6fde8b4ffcf967021bd3b0decd748a8cc1a54ee',
                                                                                                                           'entry_observations_sha256': 'c4efbdad7b94ae5dd37a7138988aadb583bda1c5ac784fd1fbb6dfd5ad7703fa',
                                                                                                                           'entry_admission_mask_sha256': '15dfb83e41d6d6a5d6401eb165a07eee88034cdb24e33fb1562859d73761a4eb'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'b60968a28d60ea1ab252046b5f38713c6265f727823b4dfb777d36d25eddb793',
                                                                                                                           'intrabar_half_spread_values_sha256': 'd600a5d6e1dec66f6e329214eb13ee607fd0eb196ebf4c02a8482e4085cb0aa4',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '5a6381a9b5ee6ddd9b5df8d94a083692a9f87509c1d1a4eaf9b196ab364eb6aa',
                                                                                                                           'entry_observations_sha256': '370679b3b6d339326f257d03cceadddd4da62452fadf54be4fa686c75f36bd80',
                                                                                                                           'entry_admission_mask_sha256': 'a53807540f6fa8101f351f74f2d729592ae623d8f8a85c698d2dbb7b96be8c00'}}},
                                    '8abb3a001dddcabb76bb5b81abec2398ecddba10aef6bf6254f8670f9927f6d5': {'partition_frame_sha256': 'ad46d89ffa341cad862a2240a55635f7d3002f25eef559ca4ef4a2fe5f65881e',
                                                                                                         'source_observation_receipt_sha256': '8e2bb5de17da73ffff3e74a25d9bf4f4e8fdbc3b80d712a2ac04cd757013d9fa',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'c9eaf3cd61600c84a0662768f1b64822cab1b57df4bcb90089e742ea346c47d4',
                                                                                                                           'intrabar_half_spread_values_sha256': '592c95d3e1ffa59e87aa8d9d1e14683041ddda22ba4102c891cf6ed6fd05c4ef',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '81f47e36abf463aa5e68e3e070abe5922ac96084a2bd5751f29f485f384bcc93',
                                                                                                                           'entry_observations_sha256': '2c5db67093b1c43220ce09a47c91fb944b3fe425cc1fba721de6bd90646a80f6',
                                                                                                                           'entry_admission_mask_sha256': '4484cb2f0bdfac1cb0e7660b00a4993b52f4640b5150074593869834613b72d6'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'df51957dde71fa7458ecc169e9ff4b05c15f35b7f2fe7bd37fd3e88e7b6d1945',
                                                                                                                           'intrabar_half_spread_values_sha256': '6834ae9ae50dffc37020f2fbc3932e5beaa67e3f1c5baab59031cc4093897e79',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '3fe080ff992b221308e16b44da514bf09e44a42c1b67159a271f49b4877d06d7',
                                                                                                                           'entry_observations_sha256': '961768a33dc696cc6adf3cf07239d13f97272983c44832de526553c9aeb6fc9f',
                                                                                                                           'entry_admission_mask_sha256': '1a6b237362cf9333b6bf77171341561807d9a7d48b6195707f87ba7e21b3766b'}}},
                                    'd97393f9f468704833d41affda38fbabf48537b7344db468324d1b5b86546f0e': {'partition_frame_sha256': '885a443c8e5a0e3619da7faaa9640b00cc4795516ded54e8b8c10c2d79c5f9a2',
                                                                                                         'source_observation_receipt_sha256': 'dccf38345b3066080854aa72d796ccd21e583e7f05213519f17b852d8c6aa9d0',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': '1ecd41b2b833caf411cd64016ad174dac36427838369e2167cc9cc421fb368f0',
                                                                                                                           'intrabar_half_spread_values_sha256': '80d49d1aee1cddcf33b918331cbc92bacf824e4e4f38d5d333432076d0338491',
                                                                                                                           'numerical_execution_cost_attestation_sha256': 'c7aca6848fa204e29dd6a98afb8b84c63a3d0a7b069f371252c8065b16f647a9',
                                                                                                                           'entry_observations_sha256': '6d6b07f7f7958e92c32338d26ea55c5a712639b632073ce6993fab6011d03c33',
                                                                                                                           'entry_admission_mask_sha256': '4b138b681bdb046e5dd7e8465d2165657ca7a0827295eecf5a2cf0e4db867cfc'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'f6ce403707c5ef1bae85deba570784cdb001601378fec713bf9175a93610d012',
                                                                                                                           'intrabar_half_spread_values_sha256': 'c61470109b9eb6f1d9b1b2ffc4b82a7e55059ba0c8ba839a85215e08df27e110',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '9ac5b688aee20ded498a73c6ba318883c23ef715ab7f85fd4c15cd42f7edf64b',
                                                                                                                           'entry_observations_sha256': '183a7c658c91544baff251833746a7939e663ebc9a94d2a209d999898c7a45aa',
                                                                                                                           'entry_admission_mask_sha256': 'a06f2c4cf4a03bcd9cf3180b6cb799b26ed7efa32470a0bf5c3d824601a81451'}}},
                                    '6e8736d1fcc728d8d94660652b6a7b2b44ea49b93a9b9205ea875591a8165fd1': {'partition_frame_sha256': '01339cd1eed72820a599ab1b49bcbc2fe4e723c7c0707d9351304c28a1f21694',
                                                                                                         'source_observation_receipt_sha256': 'f23036ec8d76aafcaf179e82256fc736f9dde5d7b70bc0a3c1a303124ddec367',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': '2524f3b428671c549a2a7a9c5dd6d74c0bb990f37c7137abfc5a29a0f29e3df5',
                                                                                                                           'intrabar_half_spread_values_sha256': '81d0a3ef270db29260c5fa2314c758d1ae0a25b1c26e22d2e694047fe2bfb8c1',
                                                                                                                           'numerical_execution_cost_attestation_sha256': 'eec7f10b9b3175f40fccded61531e908c83cee1432090fce14c90ea58d7fe80e',
                                                                                                                           'entry_observations_sha256': '3e61d261a2a55aeddf6f063dcc36bd5b11679b2549df7953b1e050f2e048c052',
                                                                                                                           'entry_admission_mask_sha256': '8eeadbb746e3a21ed61ae8b181e9c68cff04856df533d89d9cd82168e505598e'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': 'f19188822f5eb4f185c33bd63f2b3c6897919d1230214b15c14cda8c9e713d4b',
                                                                                                                           'intrabar_half_spread_values_sha256': 'db478cfd4f5ce2601ddb3dc5e2345ceb6a2593b103046afe83a4c8c95967d60a',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '1f5c25fa73cfae96aecc2ad1cca1f1190be18f0f5158ca5f3c5a4eff8ee9d5f9',
                                                                                                                           'entry_observations_sha256': 'd2fd88223af1b4abfd783cc1353da15f154f935cf500c5b23e47871a21a1e888',
                                                                                                                           'entry_admission_mask_sha256': '9be6de728e8cbb7e96d34e50f210d7689cc09a94ce91f03f8ff22f55df33f076'}}},
                                    '10eee714d437386768b6755ae3a7ec35099ec765d7d93ddba433b10daa1a70f5': {'partition_frame_sha256': '7f627ec606c0cad87d1b611213ca5bc1b60a3e819059f5d2361bda1e26747016',
                                                                                                         'source_observation_receipt_sha256': 'f06975ff1c3dac2563e922b35d369f1e6c34088a2c871c120522b4e378e4d1c1',
                                                                                                         'tiers': {'P75': {'entry_half_spread_values_sha256': 'd3439820909fc6587746bd6e77614f1e296014872a4260d9e109a9e860d8b6ea',
                                                                                                                           'intrabar_half_spread_values_sha256': '83c3a9bb3052b24cfc654e7d1a0162ff6758c119e590d4e342929c2c18e24697',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '4e6f552292a839358e838fb62ff798ad105900bc6d15f45cc7734cb86afba864',
                                                                                                                           'entry_observations_sha256': 'c600485f748ff65ebcd633e5bda173d2954e35e96e5a7c94624379fec26b8bbd',
                                                                                                                           'entry_admission_mask_sha256': 'b3ea09fcc945e1bbbf3a7325e92cbc40ccdbbfd92f0f5bc7510cea6e40edc91c'},
                                                                                                                   'P95': {'entry_half_spread_values_sha256': '50be81d1027f1bfea9e3beb88145a9e6b7d44f66f8dee775c422dfff194ccd1c',
                                                                                                                           'intrabar_half_spread_values_sha256': '03d66c38a1d0f4bc4ee39a9ccc4152b4fb6516f500e711d50333d902884e1591',
                                                                                                                           'numerical_execution_cost_attestation_sha256': '16c9bb94490b932893f6cb1ddafd05d948df58d08be8a6bd7b0f2b046e666f72',
                                                                                                                           'entry_observations_sha256': 'e2ed0ccdef45834382fbdef03155d2dcfd4b784c5544d2eaa9f098ea98fa62c4',
                                                                                                                           'entry_admission_mask_sha256': '8ec303e49e34f515b1b8e0df3d8f09579ce4968006f2c5a0406b390d42359e48'}}}},
                        'receipt_sha256': '1411061ab29a2c12890a0cfd308aaa0d405c87bca6ce848be1acea0b375c75eb'},
     'resolved_derive_stub_rows': [{'fold_ordinal': 0,
                                    'role': 'optimizer_train',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'optimizer_train',
                                                         'frame_start_utc': '2018-12-31T00:00:00+00:00',
                                                         'scored_start_utc': '2019-01-01T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-03-05T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': 'cad40cf8a8b88e782eca1b3c65b97dd896ebdfd4ea210efaa3b91c83e6ed3a7a',
                                                         'receipt_sha256': '78e33700ee09b2fa89b7d7ad1d62fc62c6dd2a44c4317d3f9ac93ecb3b8fecaf'}},
                                   {'fold_ordinal': 0,
                                    'role': 'tuning_validation',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'tuning_validation',
                                                         'frame_start_utc': '2018-12-31T00:00:00+00:00',
                                                         'scored_start_utc': '2019-01-01T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-03-05T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '8117bc446d1120e5d6a79fdc76c248c88bcd498cc3ae59237a9d62a86103c183',
                                                         'receipt_sha256': '073c55a63b1c14a6db6fbf1819f4cfd8c0273e2b9c7248a5c9b1e0e84c99fca3'}},
                                   {'fold_ordinal': 0,
                                    'role': 'final_lockbox',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'final_lockbox',
                                                         'frame_start_utc': '2018-12-31T00:00:00+00:00',
                                                         'scored_start_utc': '2019-01-01T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-03-05T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '615d741dc75a18eb71f4546a19f27770741f8ba1acebf9af8c0ac5637b93b420',
                                                         'receipt_sha256': '8f23b6086ad88e0fe8c7f0aaa7b5906cdcc2a319e086a53139221fdfdcac9660'}},
                                   {'fold_ordinal': 1,
                                    'role': 'optimizer_train',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'optimizer_train',
                                                         'frame_start_utc': '2019-03-04T00:00:00+00:00',
                                                         'scored_start_utc': '2019-03-05T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-05-07T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '59fdd2920e11fc2a0a21bb847d732b23a5447359a8ab17f7384c3b565517cfc0',
                                                         'receipt_sha256': '3fb7b82b9cbb2e7739fd8d338917430586037655efd5e33976b085dfc83e6412'}},
                                   {'fold_ordinal': 1,
                                    'role': 'tuning_validation',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'tuning_validation',
                                                         'frame_start_utc': '2019-03-04T00:00:00+00:00',
                                                         'scored_start_utc': '2019-03-05T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-05-07T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '7446c973160f641c59628b0ef268bbda0ecbb2bdb5b4300e7554f7e482f2f5ec',
                                                         'receipt_sha256': 'b90d7b9767b67d3443bb8656eee72582f4b4f5639adf8bfda6f73de362c6ebf2'}},
                                   {'fold_ordinal': 1,
                                    'role': 'final_lockbox',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'final_lockbox',
                                                         'frame_start_utc': '2019-03-04T00:00:00+00:00',
                                                         'scored_start_utc': '2019-03-05T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-05-07T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '3a659857d5f2c4f7d94a4d5ba39a3a4942efae35d50fc533a37e7ecddbe30608',
                                                         'receipt_sha256': '023984080e9968adf1d455470c75ac06e2614c0bc218e59537d19611222a219c'}},
                                   {'fold_ordinal': 2,
                                    'role': 'optimizer_train',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'optimizer_train',
                                                         'frame_start_utc': '2019-05-06T00:00:00+00:00',
                                                         'scored_start_utc': '2019-05-07T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-07-09T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': 'c132afcd2f02b5e4b9ac22d19c75bdd576a092426af884901e3844ec8e7a616d',
                                                         'receipt_sha256': '5caaa46e4a8d20fd55602d076a41aeeeb5ecae5a9356936e680cb16e340a55a4'}},
                                   {'fold_ordinal': 2,
                                    'role': 'tuning_validation',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'tuning_validation',
                                                         'frame_start_utc': '2019-05-06T00:00:00+00:00',
                                                         'scored_start_utc': '2019-05-07T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-07-09T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': 'ea117b41616bdca568d4fdb5450d4d573abd6a1068dd80a7235049acbb0c421a',
                                                         'receipt_sha256': '035a3b3772ab07a10407ca30da8480f94d363691c18fa94e3bac695d96b8c7c1'}},
                                   {'fold_ordinal': 2,
                                    'role': 'final_lockbox',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'final_lockbox',
                                                         'frame_start_utc': '2019-05-06T00:00:00+00:00',
                                                         'scored_start_utc': '2019-05-07T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-07-09T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': 'ad46d89ffa341cad862a2240a55635f7d3002f25eef559ca4ef4a2fe5f65881e',
                                                         'receipt_sha256': '8abb3a001dddcabb76bb5b81abec2398ecddba10aef6bf6254f8670f9927f6d5'}},
                                   {'fold_ordinal': 3,
                                    'role': 'optimizer_train',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'optimizer_train',
                                                         'frame_start_utc': '2019-07-08T00:00:00+00:00',
                                                         'scored_start_utc': '2019-07-09T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-09-10T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '885a443c8e5a0e3619da7faaa9640b00cc4795516ded54e8b8c10c2d79c5f9a2',
                                                         'receipt_sha256': 'd97393f9f468704833d41affda38fbabf48537b7344db468324d1b5b86546f0e'}},
                                   {'fold_ordinal': 3,
                                    'role': 'tuning_validation',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'tuning_validation',
                                                         'frame_start_utc': '2019-07-08T00:00:00+00:00',
                                                         'scored_start_utc': '2019-07-09T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-09-10T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '01339cd1eed72820a599ab1b49bcbc2fe4e723c7c0707d9351304c28a1f21694',
                                                         'receipt_sha256': '6e8736d1fcc728d8d94660652b6a7b2b44ea49b93a9b9205ea875591a8165fd1'}},
                                   {'fold_ordinal': 3,
                                    'role': 'final_lockbox',
                                    'geometry_receipt': {'schema_version': 'fxpm.winner-partition-geometry.v1',
                                                         'role': 'final_lockbox',
                                                         'frame_start_utc': '2019-07-08T00:00:00+00:00',
                                                         'scored_start_utc': '2019-07-09T00:00:00+00:00',
                                                         'end_exclusive_utc': '2019-09-10T00:00:00+00:00',
                                                         'warmup_bars': 1,
                                                         'scored_bars': 63,
                                                         'frame_rows': 64,
                                                         'frame_sha256': '7f627ec606c0cad87d1b611213ca5bc1b60a3e819059f5d2361bda1e26747016',
                                                         'receipt_sha256': '10eee714d437386768b6755ae3a7ec35099ec765d7d93ddba433b10daa1a70f5'}}]}
    plan = copy.deepcopy(literal["plan_body"])
    owners = tuple(SimpleNamespace(ordinal=ordinal, plan_attempt=copy.deepcopy(row),
        candidate=copy.deepcopy(plan["candidates"][0]), trial=copy.deepcopy(plan["trials"][ordinal]),
        artifacts=SimpleNamespace(winner_process={"strategy_id": row["strategy_id"], "strategy_family": "INVENTED_FAMILY"},
            trial_definition={"parameters": copy.deepcopy(literal["frozen_domains"][0]["parameter_domain"]["defaults"])}))
        for ordinal, row in enumerate(plan["attempts"]))
    case = {"provenance": literal["provenance"], "declared_unit_binding": copy.deepcopy(literal["declared_unit_binding"]),
        "frozen_domains": copy.deepcopy(literal["frozen_domains"]), "plan_body": plan,
        "owner_attempts": owners, "attempts": tuple(copy.deepcopy(owners)), "raw_ids": ("INVENTED-RAW-0", "INVENTED-RAW-1"),
        "precommitments": copy.deepcopy(literal["precommitments"]), "derive_rows": copy.deepcopy(literal["resolved_derive_stub_rows"]),
        "frame": object(), "dependency_warmup_bars": 1, "derive_calls": [],
        "specification": {"strategy_id": "EMACrossoverStrategy", "parameters": copy.deepcopy(literal["frozen_domains"][0]["parameter_domain"]["defaults"]),
            "fold_ordinal": 0, "partition_role": "optimizer_train", "spread_tier": "P75", "bookkeeping": {}, "request_ordinal": 0}}
    def derive(*, frame, fold, attempt, dependency_warmup_bars):
        assert frame is case["frame"] and attempt is case["attempts"][0] and dependency_warmup_bars == 1
        matches = [(index, candidate) for index, candidate in enumerate(attempt.candidate["folds"])
            if candidate["fold_id"] == fold["fold_id"]]
        assert len(matches) == 1
        fold_ordinal, source_fold = matches[0]
        assert fold == source_fold
        case["derive_calls"].append((attempt.ordinal, fold_ordinal))
        return tuple(SimpleNamespace(role=row["role"], geometry_receipt=copy.deepcopy(row["geometry_receipt"]))
            for row in case["derive_rows"] if row["fold_ordinal"] == fold_ordinal)
    monkeypatch.setattr(execution, "_fold_partitions", derive)
    return case


def _owner_snapshot_sha(value):
    import json
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _owner_snapshot_build(case):
    return execution._route_a_owner_membership_snapshot(**{key: case[key] for key in (
        "declared_unit_binding", "frozen_domains", "plan_body", "attempts", "raw_ids", "precommitments")})


def _owner_snapshot_rebind(case, snapshot, specification=None):
    return execution._route_a_rebind_owner_membership(
        specification=case["specification"] if specification is None else specification, snapshot=snapshot,
        **{key: case[key] for key in ("attempts", "owner_attempts", "raw_ids", "frame", "dependency_warmup_bars", "precommitments")})


def _owner_snapshot_validate(snapshot):
    return execution._route_a_validate_mapping_snapshot(frozen_domains=snapshot["frozen_domains"],
        attempt_roster=snapshot["attempt_roster"], owner_projection=True,
        declared_unit_binding=snapshot["declared_unit_binding"])


_OWNER_SNAPSHOT_SITES = {
    "V0": ("_route_a_validate_mapping_snapshot", "ROUTE_A_MAPPING_SNAPSHOT_INVALID"),
    "OS-SOURCE": ("_route_a_owner_membership_snapshot", "ROUTE_A_OWNER_SNAPSHOT_SOURCE_INVALID"),
    "OS-BINDING": ("_route_a_owner_membership_snapshot", "ROUTE_A_OWNER_SNAPSHOT_SOURCE_INVALID"),
    "OS-PRECOMMIT": ("_route_a_owner_partition_identity", "ROUTE_A_OWNER_SNAPSHOT_PRECOMMITMENT_INVALID"),
    "OS-REBIND": ("_route_a_rebind_owner_membership", "ROUTE_A_OWNER_SNAPSHOT_REBIND_INVALID"),
    "OS-GEOMETRY": ("_route_a_rebind_owner_membership", "ROUTE_A_OWNER_SNAPSHOT_GEOMETRY_MISMATCH"),
    "PROPAGATED-BOUNDARY": ("_winner_fold_plan", "PLANNED_ATTEMPT_HAS_NO_CAUSAL_FOLDS"),
}


def _owner_snapshot_expect_site(call, site_id):
    function, code = _OWNER_SNAPSHOT_SITES[site_id]
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        call()
    assert caught.value.code == code
    trace = caught.value.__traceback__
    filename = execution._route_a_owner_membership_snapshot.__code__.co_filename
    reached = False
    while trace is not None:
        current = trace.tb_frame.f_code
        reached |= current.co_filename == filename and current.co_name == function
        trace = trace.tb_next
    assert reached, f"INVENTED fixture did not reach owner refusal site {site_id}"


def test_route_a_owner_snapshot_sparse_roster_preserves_full_source_and_terminal_rejection(monkeypatch):
    case = _owner_snapshot_inputs(monkeypatch)
    plans = [copy.deepcopy(row.plan_attempt) for row in case["attempts"]]
    folds = [copy.deepcopy(row.candidate["folds"]) for row in case["attempts"]]
    snapshot = _owner_snapshot_build(case)
    assert snapshot["schema_version"] == "fxpm.route-a-owner-membership-snapshot.v2"
    assert snapshot["provenance_scope"] == "CURRENT_OWNER_METADATA_PROJECTION_ONLY"
    assert snapshot["original_trial_count"] == 2 and len(snapshot["attempt_roster"]) == 2
    assert len(case["plan_body"]["candidates"]) == 1
    assert [row["source_plan_attempt"] for row in snapshot["attempt_roster"]] == plans
    assert [row["source_folds"] for row in snapshot["attempt_roster"]] == folds
    assert all(len(fold) == 14 for attempt_folds in folds for fold in attempt_folds)
    for row in snapshot["attempt_roster"]:
        assert _owner_snapshot_sha(row["source_plan_attempt"]) == row["plan_attempt_sha256"]
        assert row["parameters"] == case["attempts"][row["ordinal"]].artifacts.trial_definition["parameters"]
    assert _owner_snapshot_sha(plans) == snapshot["declared_unit_binding"]["attempt_roster_sha256"]
    assert snapshot["frozen_domains"] == case["frozen_domains"]
    assert not {"unit_metadata", "header_reference", "new_header_digest"} & set(snapshot)
    planned, rejected = snapshot["attempt_roster"]
    assert planned["rejection"] is None and plans[0]["reason_code"] is None
    assert rejected["rejection"] == {"plan_status": "PROCESS_REJECTED", "reason_code": plans[1]["reason_code"],
        "state_context_disposition_sha256": plans[1]["state_context_disposition_sha256"]}
    assert all(fold["partitions"] == {"status": "UNSCHEDULED"} for fold in rejected["folds"])
    assert len(planned["scheduled_evaluations"]) == 4 and len(rejected["scheduled_evaluations"]) == 1
    assert rejected["scheduled_evaluations"] == [case["plan_body"]["expected_evaluations"][-1]]
    assert rejected["scheduled_evaluations"][0]["fold_id"] == "TERMINAL"
    assert rejected["scheduled_evaluations"][0]["role"] == "TERMINAL_MECHANICAL_REJECTION"
    assert "kind" not in rejected["rejection"] and "details" not in rejected["rejection"]
    identity = planned["folds"][0]["partitions"]["optimizer_train"]
    assert set(identity) == {"precommitment_row", "partition_geometry_receipt_sha256", "partition_frame_sha256", "tier_refs"}
    assert identity["precommitment_row"] == case["precommitments"]["rows"][0]
    assert "geometry" not in identity
    assert case["derive_calls"] == []
    _request_mapping_assert_no_authority(snapshot)


@pytest.mark.parametrize("case_id", (
    "rejected-reason-missing", "rejected-reason-empty", "planned-reason-nonnull", "planned-reason-missing",
    "disposition-missing", "planned-disposition-missing", "fold-id-missing", "fold-id-duplicate", "fold-definition-hash",
    "non-state-context-reason", "regime-null", "two-candidates",
), ids=(
    "rejected-reason-missing", "rejected-reason-empty", "planned-reason-nonnull", "planned-reason-missing",
    "disposition-missing", "planned-disposition-missing", "fold-id-missing", "fold-id-duplicate", "fold-definition-hash",
    "non-state-context-reason", "regime-null", "two-candidates",
))
def test_route_a_owner_snapshot_refuses_unsupported_real_owner_sources(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    if case_id == "rejected-reason-missing":
        del case["attempts"][1].plan_attempt["reason_code"]
    elif case_id == "rejected-reason-empty":
        case["attempts"][1].plan_attempt["reason_code"] = ""
    elif case_id == "planned-reason-nonnull":
        case["attempts"][0].plan_attempt["reason_code"] = "INVENTED_INVALID_PLANNED_REASON"
    elif case_id == "planned-reason-missing":
        del case["attempts"][0].plan_attempt["reason_code"]
    elif case_id == "disposition-missing":
        del case["attempts"][1].plan_attempt["state_context_disposition_sha256"]
    elif case_id == "planned-disposition-missing":
        del case["attempts"][0].plan_attempt["state_context_disposition_sha256"]
    elif case_id == "non-state-context-reason":
        case["attempts"][1].plan_attempt["reason_code"] = "SOURCE_BOUNDARY_SHORTFALL"
        case["plan_body"]["expected_evaluations"][-1]["expected_reason_code"] = "SOURCE_BOUNDARY_SHORTFALL"
    elif case_id in {"fold-id-missing", "fold-id-duplicate", "fold-definition-hash"}:
        for attempt in case["attempts"]:
            if case_id == "fold-id-missing":
                del attempt.candidate["folds"][0]["fold_id"]
            elif case_id == "fold-id-duplicate":
                duplicate = attempt.candidate["folds"][1]
                duplicate.pop("fold_definition_sha256")
                duplicate["fold_id"] = attempt.candidate["folds"][0]["fold_id"]
                attempt.candidate["folds"][1] = _request_mapping_seal(duplicate, "fold_definition_sha256")
            elif case_id == "fold-definition-hash":
                attempt.candidate["folds"][0]["fit_rows"] += 1
        case["plan_body"]["candidates"][0] = copy.deepcopy(case["attempts"][0].candidate)
        if case_id == "fold-id-duplicate":
            case["plan_body"]["expected_evaluations"][1]["fold_id"] = case["attempts"][0].candidate["folds"][1]["fold_id"]
            for attempt in case["attempts"]:
                duplicate = attempt.candidate["folds"][1]
                assert _owner_snapshot_sha({key: value for key, value in duplicate.items()
                    if key != "fold_definition_sha256"}) == duplicate["fold_definition_sha256"]
    elif case_id == "regime-null":
        case["plan_body"]["cell"]["regime"] = None
    elif case_id == "two-candidates":
        case["plan_body"]["candidates"].append(copy.deepcopy(case["plan_body"]["candidates"][0]))
    else:
        raise AssertionError("Unknown INVENTED real-owner source case")
    case["plan_body"]["attempts"] = [copy.deepcopy(row.plan_attempt) for row in case["attempts"]]
    case["declared_unit_binding"]["attempt_roster_sha256"] = _owner_snapshot_sha(case["plan_body"]["attempts"])
    if case_id in {"fold-id-duplicate", "fold-definition-hash", "planned-disposition-missing"}:
        _request_mapping_expect_site(lambda: _owner_snapshot_build(case), "C3")
    else:
        _owner_snapshot_expect_site(lambda: _owner_snapshot_build(case), "OS-SOURCE")
    assert case["derive_calls"] == []


@pytest.mark.parametrize("case_id", ("roster-hash", "fold-plan-hash"), ids=("roster-hash", "fold-plan-hash"))
def test_route_a_owner_snapshot_refuses_existing_binding_mismatch(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    field = "attempt_roster_sha256" if case_id == "roster-hash" else "fold_plan_sha256"
    case["declared_unit_binding"][field] = "0" * 64
    _owner_snapshot_expect_site(lambda: _owner_snapshot_build(case), "OS-BINDING")
    assert case["derive_calls"] == []


@pytest.mark.parametrize("case_id", ("rejected-per-fold", "terminal-reason", "planned-own-fold"),
    ids=("rejected-per-fold", "terminal-reason", "planned-own-fold"))
def test_route_a_owner_snapshot_refuses_schedule_projection_drift(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    rows = case["plan_body"]["expected_evaluations"]
    if case_id == "rejected-per-fold":
        rows.append(copy.deepcopy(rows[-1]))
    elif case_id == "terminal-reason":
        rows[-1]["expected_reason_code"] = "INVENTED_OTHER_REASON"
    else:
        rows[0]["fold_id"] = "INVENTED_FOREIGN_FOLD"
    _request_mapping_expect_site(lambda: _owner_snapshot_build(case), "C3")


@pytest.mark.parametrize("case_id", ("source-plan-hash", "source-fold-order", "rejected-scheduled", "rejection-kind", "interpolation-fold"),
    ids=("source-plan-hash", "source-fold-order", "rejected-scheduled", "rejection-kind", "interpolation-fold"))
def test_route_a_owner_snapshot_shared_validator_checks_sparse_source_joins(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    planned, rejected = snapshot["attempt_roster"]
    if case_id == "source-plan-hash":
        planned["source_plan_attempt"]["trial_id"] = "INVENTED_OTHER_TRIAL"
    elif case_id == "source-fold-order":
        planned["source_folds"].reverse()
    elif case_id == "rejected-scheduled":
        rejected["folds"][0]["partitions"] = copy.deepcopy(planned["folds"][0]["partitions"])
    elif case_id == "interpolation-fold":
        fold = planned["source_folds"][0]
        fold.pop("fold_definition_sha256")
        fold["role"] = "INTERPOLATION_ONLY"
        planned["source_folds"][0] = _request_mapping_seal(fold, "fold_definition_sha256")
        planned["folds"][0]["role"] = "INTERPOLATION_ONLY"
    else:
        rejected["rejection"] = {"kind": "EXPECTED_INELIGIBLE", "reason_code": "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"}
    _request_mapping_expect_site(lambda: _owner_snapshot_validate(snapshot), "C3")


@pytest.mark.parametrize("case_id", (
    "v2-schema", "receipt-format", "row-missing", "row-duplicate", "row-other-attempt",
    "record-missing", "p75-missing", "p95-missing", "geometry-hash-format", "frame-hash-format",
), ids=(
    "v2-schema", "receipt-format", "row-missing", "row-duplicate", "row-other-attempt",
    "record-missing", "p75-missing", "p95-missing", "geometry-hash-format", "frame-hash-format",
))
def test_route_a_owner_snapshot_refuses_missing_or_crossed_v2_identity(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    bundle = case["precommitments"]
    digest = bundle["rows"][0]["partition_geometry_receipt_sha256"]
    if case_id == "v2-schema":
        bundle["schema_version"] = "INVENTED_NOT_V2"
    elif case_id == "receipt-format":
        bundle["receipt_sha256"] = "INVENTED_NOT_A_RECEIPT"
    elif case_id == "row-missing":
        bundle["rows"] = bundle["rows"][1:]
    elif case_id == "row-duplicate":
        bundle["rows"].append(copy.deepcopy(bundle["rows"][0]))
    elif case_id == "row-other-attempt":
        # Geometry records can be shared; the required OWN row cannot.
        bundle["rows"][0]["attempt_ordinal"] = 1
    elif case_id == "record-missing":
        del bundle["records"][digest]
    elif case_id == "p75-missing":
        del bundle["records"][digest]["tiers"]["P75"]
    elif case_id == "p95-missing":
        del bundle["records"][digest]["tiers"]["P95"]
    elif case_id == "geometry-hash-format":
        bundle["rows"][0]["partition_geometry_receipt_sha256"] = "INVENTED_NOT_A_GEOMETRY_HASH"
    else:
        bundle["records"][digest]["partition_frame_sha256"] = "INVENTED_NOT_A_FRAME_HASH"
    _owner_snapshot_expect_site(lambda: execution._route_a_owner_partition_identity(
        precommitments=bundle, attempt_ordinal=0, fold_ordinal=0, partition_role="optimizer_train"), "OS-PRECOMMIT")


@pytest.mark.parametrize("tier", ("P75", "P95"), ids=("P75", "P95"))
def test_route_a_owner_snapshot_rebind_derives_only_resolved_fold_and_uses_shared_matcher(monkeypatch, tier):
    case = _owner_snapshot_inputs(monkeypatch)
    def forbidden(*_args, **_kwargs):
        pytest.fail("INVENTED owner matching attempted compiler, evaluation, source I/O or clock access")
    for name in ("_compile_route_a_request_attempt_mapping", "_runtime_strategy", "Backtester", "_file_sha256", "_now_utc_seconds"):
        monkeypatch.setattr(execution, name, forbidden)
    snapshot = _owner_snapshot_build(case)
    assert case["derive_calls"] == []
    member, calls = execution._route_a_resolve_request_membership, []
    def resolve(**kwargs):
        calls.append(copy.deepcopy(kwargs["specification"]))
        assert kwargs["owner_projection"] is True
        return member(**kwargs)
    monkeypatch.setattr(execution, "_route_a_resolve_request_membership", resolve)
    specification = copy.deepcopy(case["specification"])
    specification.update(spread_tier=tier, fold_ordinal=2, bookkeeping={"attempt_ordinal": 999, "horizon_attempt_id": "INVENTED_NONSELECTING"})
    for ordinal in (0, 1):
        specification["request_ordinal"] = ordinal
        selected, fold, partition, parameters = _owner_snapshot_rebind(case, snapshot, specification)
        assert selected is case["attempts"][0] and fold == selected.candidate["folds"][2]
        assert fold["fold_id"] == selected.candidate["folds"][2]["fold_id"]
        assert partition.role == "optimizer_train" and parameters == case["specification"]["parameters"]
        assert partition.geometry_receipt == next(row["geometry_receipt"] for row in case["derive_rows"]
            if row["fold_ordinal"] == 2 and row["role"] == "optimizer_train")
    assert len(calls) == 2 and case["derive_calls"] == [(0, 2), (0, 2)]
    _request_mapping_assert_no_authority(snapshot)


@pytest.mark.parametrize("case_id", (
    "private-ordinal", "raw-id", "horizon-id", "plan-content", "planned-status", "original-plan",
    "source-fold-content", "original-owner-order", "original-strategy", "original-parameters",
), ids=(
    "private-ordinal", "raw-id", "horizon-id", "plan-content", "planned-status", "original-plan",
    "source-fold-content", "original-owner-order", "original-strategy", "original-parameters",
))
def test_route_a_owner_snapshot_rebind_refuses_current_or_original_identity_drift(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    if case_id == "private-ordinal":
        case["attempts"][0].ordinal = 7
    elif case_id == "raw-id":
        case["raw_ids"] = ("INVENTED_OTHER_RAW", case["raw_ids"][1])
    elif case_id == "horizon-id":
        case["attempts"][0].plan_attempt["attempt_id"] = "INVENTED_OTHER_HORIZON"
    elif case_id == "plan-content":
        case["attempts"][0].plan_attempt["invented_new_field"] = "INVENTED_CHANGED"
    elif case_id == "planned-status":
        case["attempts"][0].plan_attempt.update(status="PROCESS_REJECTED", reason_code="INVENTED_CHANGED")
    elif case_id == "original-plan":
        case["owner_attempts"][0].plan_attempt["invented_new_field"] = "INVENTED_CHANGED"
    elif case_id == "source-fold-content":
        case["attempts"][0].candidate["folds"][0]["fit_rows"] += 1
    elif case_id == "original-owner-order":
        case["owner_attempts"] = tuple(reversed(case["owner_attempts"]))
    elif case_id == "original-strategy":
        case["owner_attempts"][0].artifacts.winner_process["strategy_id"] = "INVENTED_OTHER_STRATEGY"
    elif case_id == "original-parameters":
        case["owner_attempts"][0].artifacts.trial_definition["parameters"]["fast_period"] = 11
    else:
        raise AssertionError("Unknown INVENTED original-owner join case")
    _owner_snapshot_expect_site(lambda: _owner_snapshot_rebind(case, snapshot), "OS-REBIND")
    assert case["derive_calls"] == []


@pytest.mark.parametrize("index", (-1, 2), ids=("negative-index", "past-original-roster"))
def test_route_a_owner_snapshot_rebind_refuses_untrusted_helper_index(monkeypatch, index):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    member = execution._route_a_resolve_request_membership
    def invalid_index(**kwargs):
        result = member(**kwargs)
        result["attempt_index"] = index
        return result
    monkeypatch.setattr(execution, "_route_a_resolve_request_membership", invalid_index)
    _owner_snapshot_expect_site(lambda: _owner_snapshot_rebind(case, snapshot), "OS-REBIND")
    assert case["derive_calls"] == []


@pytest.mark.parametrize("case_id", ("geometry-receipt", "frame-hash", "tier-reference", "geometry-body"),
    ids=("geometry-receipt", "frame-hash", "tier-reference", "geometry-body"))
def test_route_a_owner_snapshot_rebind_refuses_selected_geometry_or_reference_drift(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    identity = snapshot["attempt_roster"][0]["folds"][0]["partitions"]["optimizer_train"]
    geometry = case["derive_rows"][0]["geometry_receipt"]
    if case_id == "geometry-receipt":
        # Equivalent UTC text keeps the frame identity while changing committed body bytes.
        assert geometry["frame_start_utc"].endswith("+00:00")
        geometry["frame_start_utc"] = geometry["frame_start_utc"].replace("+00:00", "Z")
        geometry["receipt_sha256"] = _owner_snapshot_sha({key: value for key, value in geometry.items() if key != "receipt_sha256"})
        assert geometry["receipt_sha256"] != identity["partition_geometry_receipt_sha256"]
        assert geometry["frame_sha256"] == identity["partition_frame_sha256"]
    elif case_id == "frame-hash":
        old_digest, expected_frame = identity["partition_geometry_receipt_sha256"], identity["partition_frame_sha256"]
        geometry["frame_sha256"] = "9" * 64
        geometry["receipt_sha256"] = _owner_snapshot_sha({key: value for key, value in geometry.items() if key != "receipt_sha256"})
        bundle = case["precommitments"]
        own_row = next(row for row in bundle["rows"] if (row["attempt_ordinal"], row["fold_ordinal"], row["partition"]) == (0, 0, "optimizer_train"))
        own_row["partition_geometry_receipt_sha256"] = geometry["receipt_sha256"]
        bundle["records"][geometry["receipt_sha256"]] = copy.deepcopy(bundle["records"][old_digest])
        if not any(row["partition_geometry_receipt_sha256"] == old_digest for row in bundle["rows"]):
            del bundle["records"][old_digest]
        bundle.pop("receipt_sha256")
        case["precommitments"] = _request_mapping_seal(bundle, "receipt_sha256")
        bundle = case["precommitments"]
        own_row = next(row for row in bundle["rows"] if (row["attempt_ordinal"], row["fold_ordinal"], row["partition"]) == (0, 0, "optimizer_train"))
        identity.update(precommitment_row=copy.deepcopy(own_row), partition_geometry_receipt_sha256=geometry["receipt_sha256"],
            tier_refs={tier: "|".join((bundle["schema_version"], bundle["receipt_sha256"], geometry["receipt_sha256"], tier)) for tier in ("P75", "P95")})
        case["declared_unit_binding"]["cost_precommitments_sha256"] = _owner_snapshot_sha(bundle)
        snapshot["declared_unit_binding"]["cost_precommitments_sha256"] = _owner_snapshot_sha(bundle)
        assert geometry["receipt_sha256"] == identity["partition_geometry_receipt_sha256"]
        assert geometry["frame_sha256"] != identity["partition_frame_sha256"] == expected_frame
        assert bundle["records"][geometry["receipt_sha256"]]["partition_frame_sha256"] == expected_frame
    elif case_id == "geometry-body":
        assert geometry["frame_start_utc"].endswith("+00:00")
        geometry["frame_start_utc"] = geometry["frame_start_utc"].replace("+00:00", "Z")
        assert geometry["receipt_sha256"] == identity["partition_geometry_receipt_sha256"]
        assert geometry["frame_sha256"] == identity["partition_frame_sha256"]
    else:
        identity["tier_refs"]["P75"] = "|".join((case["precommitments"]["schema_version"], "9" * 64,
            identity["partition_geometry_receipt_sha256"], "P75"))
    actual_selfhash = _owner_snapshot_sha({key: value for key, value in geometry.items() if key != "receipt_sha256"})
    assert (actual_selfhash == geometry["receipt_sha256"]) is (case_id != "geometry-body")
    _owner_snapshot_expect_site(lambda: _owner_snapshot_rebind(case, snapshot), "OS-GEOMETRY")


def test_route_a_owner_snapshot_rebind_preserves_typed_tuple_parameters(monkeypatch):
    """INVENTED metadata provider; canonical wire equality must preserve runtime tuple type."""
    case = _owner_snapshot_inputs(monkeypatch)
    templates = {"pair": (1, 2)}
    def defaults(strategy_id):
        assert strategy_id == "INVENTED_TUPLE_DEFAULTS"
        return SimpleNamespace(get_default_params=lambda: templates)
    monkeypatch.setattr(execution.StrategyRegistry, "get", staticmethod(defaults))
    domain = {"strategy_id": "INVENTED_TUPLE_DEFAULTS", "strategy_family": "INVENTED_TUPLE_FAMILY",
        "parameter_domain": {"schema_version": "fxpm.parameter-domain.v1", "defaults": {"pair": [1, 2]},
            "axes": [{"names": ["pair"], "choices": [[[1, 2]], [[2, 3]]]}], "point_count": 2}}
    case["frozen_domains"] = [_request_mapping_seal(domain, "domain_sha256")]
    for row in (*case["attempts"], *case["owner_attempts"]):
        row.plan_attempt["strategy_id"] = "INVENTED_TUPLE_DEFAULTS"
        row.plan_attempt["parameter_set_sha256"] = _owner_snapshot_sha({"pair": [1, 2]})
        row.artifacts.winner_process.update(strategy_id="INVENTED_TUPLE_DEFAULTS", strategy_family="INVENTED_TUPLE_FAMILY")
        row.artifacts.trial_definition["parameters"] = {"pair": [1, 2]}
    case["plan_body"]["attempts"] = [copy.deepcopy(row.plan_attempt) for row in case["attempts"]]
    case["declared_unit_binding"]["attempt_roster_sha256"] = _owner_snapshot_sha(case["plan_body"]["attempts"])
    case["specification"].update(strategy_id="INVENTED_TUPLE_DEFAULTS", parameters={"pair": [1, 2]})
    snapshot = _owner_snapshot_build(case)
    selected, _fold, _partition, parameters = _owner_snapshot_rebind(case, snapshot)
    assert selected is case["attempts"][0] and parameters == {"pair": (1, 2)} and type(parameters["pair"]) is tuple
    assert snapshot["attempt_roster"][0]["parameters"] == {"pair": [1, 2]}
    parameters["pair"] = (9, 9)
    assert templates == {"pair": (1, 2)} and selected.artifacts.trial_definition["parameters"] == {"pair": [1, 2]}


def test_route_a_owner_snapshot_pair_selection_bytes_preserve_old_formula(monkeypatch):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    selected, fold, partition, parameters = _owner_snapshot_rebind(case, snapshot)
    common = dict(strategy_id=selected.artifacts.winner_process["strategy_id"], parameters=parameters,
        attempt_ordinal=selected.ordinal, fold_ordinal=0, fold_id=fold["fold_id"],
        partition_geometry=partition.geometry_receipt, declared_unit_binding=case["declared_unit_binding"])
    low, low_group = execution._route_a_owner_selection_bytes(**common, spread_tier="P75")
    high, high_group = execution._route_a_owner_selection_bytes(**common, spread_tier="P95")
    assert low != high and low_group == high_group
    assert hashlib.sha256(low_group).hexdigest() == hashlib.sha256(high_group).hexdigest()
    import json
    expected_group = {"strategy_id": selected.artifacts.winner_process["strategy_id"], "parameters": parameters,
        "attempt_ordinal": selected.ordinal, "fold_ordinal": 0, "fold_id": fold["fold_id"],
        "partition_geometry": partition.geometry_receipt, "declared_unit_binding": case["declared_unit_binding"]}
    encode = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    assert low_group == encode(expected_group)
    assert low == encode({**expected_group, "spread_tier": "P75", "metadata_recipe": "ROUTE_A_APPLIED_SPREAD_POLICY_V1"})
    assert high == encode({**expected_group, "spread_tier": "P95", "metadata_recipe": "ROUTE_A_APPLIED_SPREAD_POLICY_V1"})
    assert "spread_tier" not in json.loads(low_group) and "metadata_recipe" not in json.loads(low_group)


def test_route_a_owner_snapshot_copies_source_and_projection_without_aliases(monkeypatch):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    pristine = copy.deepcopy(snapshot)
    case["attempts"][1].plan_attempt["reason_code"] = "INVENTED_CHANGED_SOURCE"
    case["attempts"][0].candidate["folds"][0]["fit_rows"] += 1
    case["frozen_domains"][0]["strategy_family"] = "INVENTED_CHANGED_SOURCE"
    assert snapshot == pristine
    snapshot["attempt_roster"][0]["source_folds"][0]["fit_rows"] += 1
    snapshot["attempt_roster"][1]["rejection"]["reason_code"] = "INVENTED_CHANGED_OUTPUT"
    assert snapshot["attempt_roster"][1]["source_folds"] == pristine["attempt_roster"][1]["source_folds"]
    assert snapshot["attempt_roster"][1]["source_plan_attempt"] == pristine["attempt_roster"][1]["source_plan_attempt"]
    assert case["attempts"][1].plan_attempt["reason_code"] == "INVENTED_CHANGED_SOURCE"


def test_route_a_owner_snapshot_identity_names_receipt_without_authenticating_it(monkeypatch):
    """INVENTED consistency only; authenticated full-content guards remain external."""
    case = _owner_snapshot_inputs(monkeypatch)
    case["precommitments"]["receipt_sha256"] = "9" * 64
    identity = execution._route_a_owner_partition_identity(precommitments=case["precommitments"],
        attempt_ordinal=0, fold_ordinal=0, partition_role="optimizer_train")
    assert identity["tier_refs"] == {tier: "|".join((case["precommitments"]["schema_version"], "9" * 64,
        identity["partition_geometry_receipt_sha256"], tier)) for tier in ("P75", "P95")}


@pytest.mark.parametrize("case_id", ("nonplain", "binding-shape", "owner-flag"), ids=("nonplain", "binding-shape", "owner-flag"))
def test_route_a_owner_snapshot_shared_input_guard_refuses_invalid_projection(monkeypatch, case_id):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    kwargs = {"frozen_domains": snapshot["frozen_domains"], "attempt_roster": snapshot["attempt_roster"],
        "owner_projection": True, "declared_unit_binding": snapshot["declared_unit_binding"]}
    if case_id == "nonplain":
        kwargs["attempt_roster"][0]["source_plan_attempt"]["invented_nonplain"] = object()
    elif case_id == "binding-shape":
        del kwargs["declared_unit_binding"]["attempt_roster_sha256"]
    else:
        kwargs["owner_projection"] = 1
    _owner_snapshot_expect_site(lambda: execution._route_a_validate_mapping_snapshot(**kwargs), "V0")


def test_route_a_owner_snapshot_membership_owner_flag_is_exact_bool(monkeypatch):
    case = _owner_snapshot_inputs(monkeypatch)
    snapshot = _owner_snapshot_build(case)
    _request_mapping_expect_site(lambda: execution._route_a_resolve_request_membership(
        specification=case["specification"], frozen_domains=snapshot["frozen_domains"],
        attempt_roster=snapshot["attempt_roster"], owner_projection=1), "M1")


def test_route_a_owner_snapshot_boundary_refuses_at_existing_fold_plan_callee(monkeypatch):
    """INVENTED unsupported real-route input; this is existing-callee propagation, not a new owner site."""
    case = _owner_snapshot_inputs(monkeypatch)
    for attempt in case["attempts"]:
        attempt.candidate["folds"] = []
    case["plan_body"]["candidates"][0] = copy.deepcopy(case["attempts"][0].candidate)
    _owner_snapshot_expect_site(lambda: _owner_snapshot_build(case), "PROPAGATED-BOUNDARY")
    assert case["derive_calls"] == []


def test_route_a_owner_snapshot_direct_projection_refuses_all_rejected_roster(monkeypatch):
    """Direct helper refusal does not establish the outer UNSEARCHED bypass."""
    case = _owner_snapshot_inputs(monkeypatch)
    case["attempts"][0].plan_attempt.update(status="PROCESS_REJECTED", reason_code="STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL")
    case["plan_body"]["attempts"] = [copy.deepcopy(row.plan_attempt) for row in case["attempts"]]
    case["declared_unit_binding"]["attempt_roster_sha256"] = _owner_snapshot_sha(case["plan_body"]["attempts"])
    _owner_snapshot_expect_site(lambda: _owner_snapshot_build(case), "OS-SOURCE")
    assert case["derive_calls"] == []


def _full_panel_preparation_invented_inputs(monkeypatch):
    """INVENTED metadata only; not an issued process or financial evidence."""
    case = _owner_snapshot_inputs(monkeypatch)
    case["declared_unit_binding"]["cost_precommitments_sha256"] = execution._sha256(case["precommitments"])
    return case, _owner_snapshot_build(case)


def test_route_a_full_panel_preparation_contract_covers_all_planned_folds_and_lanes(monkeypatch):
    case, snapshot = _full_panel_preparation_invented_inputs(monkeypatch)
    contract = search_planner._route_a_full_panel_preparation_contract(
        owner_snapshot=snapshot, partition_cost_precommitments=case["precommitments"])
    planned = [row for row in snapshot["attempt_roster"] if row["status"] == "PLANNED"]
    rejected = [row for row in snapshot["attempt_roster"] if row["status"] == "PROCESS_REJECTED"]
    assert contract["original_trial_count"] == len(case["attempts"])
    assert len(contract["requests"]) == len(planned) * 4 * 6
    assert len(contract["process_rejections"]) == len(rejected)
    assert {row["raw_attempt_id"] for row in contract["requests"]} == {row["raw_attempt_id"] for row in planned}
    identities = {(row["attempt_ordinal"], row["fold_ordinal"], row["partition_role"], row["spread_tier"])
        for row in contract["requests"]}
    assert len(identities) == len(contract["requests"])
    for row in planned:
        assert {(fold, role, tier) for attempt, fold, role, tier in identities if attempt == row["ordinal"]} == {
            (fold, role, tier) for fold in range(4)
            for role in ("optimizer_train", "tuning_validation", "final_lockbox") for tier in ("P75", "P95")}
    public = {key: value for key, value in case["specification"].items() if key != "request_ordinal"}
    public["partition_role"] = "final_lockbox"
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_batch_specifications([public])
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_LOCKBOX_REFUSED"


def test_route_a_full_panel_preparation_contract_refuses_crossed_precommitment(monkeypatch):
    case, snapshot = _full_panel_preparation_invented_inputs(monkeypatch)
    snapshot["declared_unit_binding"]["cost_precommitments_sha256"] = "0" * 64
    with pytest.raises(search_planner.ProductionSearchPlannerError) as caught:
        search_planner._route_a_full_panel_preparation_contract(
            owner_snapshot=snapshot, partition_cost_precommitments=case["precommitments"])
    assert caught.value.code == "ROUTE_A_FULL_PANEL_PRECOMMITMENT_BINDING_INVALID"


@pytest.mark.parametrize("drift", ["own-row", "frame", "tier-ref"])
def test_route_a_full_panel_preparation_contract_refuses_partition_join_drift(monkeypatch, drift):
    case, snapshot = _full_panel_preparation_invented_inputs(monkeypatch)
    row = next(row for row in snapshot["attempt_roster"] if row["status"] == "PLANNED")
    identity = row["folds"][0]["partitions"]["optimizer_train"]
    if drift == "own-row":
        identity["precommitment_row"]["attempt_ordinal"] = 999
    elif drift == "frame":
        identity["partition_frame_sha256"] = "0" * 64
    else:
        identity["tier_refs"]["P95"] = "INVENTED-CROSSED-TIER-REFERENCE"
    with pytest.raises(search_planner.ProductionSearchPlannerError) as caught:
        search_planner._route_a_full_panel_preparation_contract(
            owner_snapshot=snapshot, partition_cost_precommitments=case["precommitments"])
    assert caught.value.code == "ROUTE_A_FULL_PANEL_PARTITION_INVALID"


def test_route_a_full_panel_lane_capture_refuses_unmatched_stress_metadata():
    """INVENTED collector inputs exercise joins, never kernel authentication."""
    capture = execution._FullPanelLaneCapture(lambda: None)
    facts = {"partition_role": "optimizer_train",
        "numerical_execution_cost_attestations": {"P95": {"INVENTED": "expected"}}}
    capture.record_lane(0, "optimizer_train", "P75", {}, facts)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        capture.record_lane(0, "optimizer_train", "P95",
            {"numerical_execution_cost_attestation": {"INVENTED": "crossed"}}, {})
    assert caught.value.code == "ROUTE_A_FULL_PANEL_STRESS_ATTESTATION_DRIFT"
    capture.close()


def test_route_a_full_panel_lane_capture_closes_without_retaining_owner_guard():
    capture = execution._FullPanelLaneCapture(lambda: None)
    capture.close()
    assert capture._require_current is None
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        capture.require_current()
    assert caught.value.code == "ROUTE_A_FULL_PANEL_CAPTURE_CLOSED"


def _full_panel_preparation_invented_stage(tmp_path):
    """INVENTED codec envelope; its empty folds are not authentic Raw evidence."""
    unit = {"ordinal": 0, "raw_attempt_id": "INVENTED-RAW-UNIT",
        "plan_attempt_sha256": "a" * 64, "status": "PLANNED"}
    raw_body = {"attempt_id": unit["raw_attempt_id"], "folds": []}
    facts = [{"fixture_provenance": "INVENTED_METADATA_ONLY_NOT_KERNEL_EVIDENCE"}]
    envelope = {"schema_version": "fxpm.route-a-prepared-full-panel-unit.v1",
        "declared_unit_binding_sha256": "b" * 64, "preparation_contract_sha256": "c" * 64,
        "attempt_evidence": raw_body, "matched_base_facts": facts}
    encoded = execution._canonical_bytes(envelope)
    path = tmp_path / "invented-prepared-envelope.json"
    path.write_bytes(encoded)
    row = execution._StagedAttemptEvidence(0, "RAW_FOLDS", path, len(encoded), hashlib.sha256(encoded).hexdigest())
    prepared = execution._PreparedRouteAFullPanelStage((row,), "b" * 64, "c" * 64,
        execution._canonical_bytes([unit]))
    return prepared, unit, raw_body, facts


def test_route_a_full_panel_prepared_stage_separates_facts_from_legacy_raw_body(tmp_path):
    prepared, unit, raw_body, facts = _full_panel_preparation_invented_stage(tmp_path)
    body = prepared.unit_body(unit)
    assert body == raw_body and set(body) == {"attempt_id", "folds"}
    assert not {"matched_base_facts", "stress_cost_scalar", "actual_initial_risk_dollars"} & set(body)
    assert prepared.matched_base_facts(0) == facts


def test_route_a_full_panel_prepared_stage_refuses_same_raw_id_with_crossed_unit_identity(tmp_path):
    prepared, unit, _, _ = _full_panel_preparation_invented_stage(tmp_path)
    unit["plan_attempt_sha256"] = "d" * 64
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        prepared.unit_body(unit)
    assert caught.value.code == "ROUTE_A_FULL_PANEL_UNIT_IDENTITY_CHANGED"


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
@pytest.mark.parametrize("case_id, stop_distance, executed_lots, actual_risk", [
    ("N26_ROUNDING_REQUESTED_VS_EXECUTED_STOP_RISK", 12.0, 2.0, 24.0),
    ("N28_MAX_LOT_CAP_REQUESTED_VS_EXECUTED_STOP_RISK", 10.0, 1.5, 15.0),
], ids=["rounded", "capped"])
def test_route_a_full_panel_actual_initial_risk_preserves_requested_budget(
        monkeypatch, compiled, direction, case_id, stop_distance, executed_lots, actual_risk):
    """INVENTED price fixture: genuine fixed-control kernel calls, no financial evidence."""
    from test_frozen_spread_execution import _continuous_case
    from test_backtester_cost_timing import _OneShotAtrStrategy

    assert execution.pm_core.NUMBA_AVAILABLE, "Both execution backends are required"
    backtester, fixture, _expected = _continuous_case(case_id)
    owners = fixture["entry_schedule"].owners
    base = next(owner for owner in owners if owner.spread_tier.tier == "P75")
    stress = next(owner for owner in owners if owner.spread_tier.tier == "P95")
    features = base.features.copy(deep=True)
    features["ATR_14"] = stop_distance / 2.0
    features["REGIME_LIVE"] = "TRENDING"
    strategy = _OneShotAtrStrategy(direction)
    signals = strategy.generate_signals(features, fixture["symbol"])
    monkeypatch.setattr(execution.pm_core, "NUMBA_AVAILABLE", compiled)
    ordinary = backtester.run(features, signals, fixture["symbol"], strategy,
        spec=fixture["spec"], timeframe=fixture["timeframe"], warmup_bars=fixture["warmup_bars"],
        spread_tier=base.spread_tier, entry_spread_observations=fixture["entry_spread_observations"])
    source_sha = execution._source_identity(
        execution.raw_winner.build_trade_source_contract_from_backtester_result(ordinary))
    partition = execution._PartitionSlice("optimizer_train", features, fixture["warmup_bars"],
        features.index[fixture["warmup_bars"]].isoformat(),
        features["_BAR_CLOSE_TS"].iloc[-1].isoformat(), {})
    facts, decision = {}, {}
    result, _features, _signals, _backtester = execution._run_tier_partition(
        partition=partition,
        full_prefix_surface=execution._FullPrefixStrategySurface(features, signals),
        backtester=backtester, strategy=strategy, symbol=fixture["symbol"],
        timeframe=fixture["timeframe"], regime="TRENDING", config=backtester.config,
        spec=fixture["spec"], expected_source_contract_sha256=source_sha,
        spread_tier=base.spread_tier,
        entry_spread_observations=fixture["entry_spread_observations"],
        decision_result_out=decision, _matched_base_facts_out=facts,
        _matched_stress_tier=stress.spread_tier)
    assert len(result["trades"]) == len(facts["trades"]) == 1
    trade, witness = result["trades"][0], facts["trades"][0]
    assert trade["position_size"] == executed_lots
    assert trade["risk_amount"] == 25.0
    assert trade["stop_loss"] == trade["entry_price"] - direction * stop_distance
    assert witness["recorded_stop_price"] == trade["stop_loss"]
    assert witness["executed_volume"] == executed_lots
    assert witness["actual_initial_risk_dollars"] == actual_risk
    assert witness["risk_at_recorded_stop_dollars"] == actual_risk
    assert witness["actual_initial_risk_dollars"] != trade["risk_amount"]
    assert witness["initial_stop_status"] == "ORIGINAL_ENTRY_STOP_FIXED_CONTROL"
    assert decision["trade_identity_pnl"] == execution.build_backtest_trade_identity_pnl_evidence(ordinary["trades"])
    binding = facts["initial_risk_source_binding"]
    assert binding["executor_release_sha256"] == execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
    assert binding["backtest_source_contract_sha256"] == source_sha
    assert binding["instrument_spec_sha256"] == execution._sha256(vars(fixture["spec"]))
    assert binding["caller"] == "pm_authoritative_search_execution._run_tier_partition"
    assert binding["governance_policy"] is binding["entry_schedule"] is None
    assert binding["terminal_policy"] == "LIQUIDATE"
    assert facts["stress_cost_scalar"] is None and facts["dated_fx_vector"] is None
    assert facts["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"
    assert facts["dated_fx_status"] == "NOT_REQUIRED_PRICE_RATIO_FLOOR"
    exact = witness["executed_price_swap_capture"]
    expected_base_r = direction * (exact["exit_price"] - exact["entry_price"]) / stop_distance + exact["swap_dollars_exact"] / actual_risk
    assert facts["base_actual_initial_risk_net_r"] == [execution.pm_core.canonical_backtest_winner_r(expected_base_r, 1.0)]
    assert result == ordinary
    assert facts["matched_p95_spread_cost_r"] == [execution.pm_core.canonical_backtest_winner_r(4.0, stop_distance)]
    unbound = execution._route_a_matched_base_trade_facts(
        result=result, features=features, backtester=backtester, spec=fixture["spec"],
        base_tier=base.spread_tier, stress_tier=stress.spread_tier,
        timeframe=fixture["timeframe"], partition_role="optimizer_train")
    assert "initial_risk_source_binding" not in unbound
    assert unbound["trades"][0]["actual_initial_risk_dollars"] is None


@pytest.mark.parametrize("fold_ordinal", [0, 3], ids=["first", "last"])
def test_route_a_full_panel_private_rebind_keeps_original_lockbox(monkeypatch, fold_ordinal):
    """INVENTED metadata joins; no process issuance or kernel authentication."""
    case, snapshot = _full_panel_preparation_invented_inputs(monkeypatch)
    contract = search_planner._route_a_full_panel_preparation_contract(
        owner_snapshot=snapshot, partition_cost_precommitments=case["precommitments"])
    rows = execution._route_a_full_panel_specifications(snapshot=snapshot, contract=contract, attempt_ordinal=0)
    assert len(rows) == 24
    row = next(row for row in rows if row["fold_ordinal"] == fold_ordinal
        and row["partition_role"] == "final_lockbox" and row["spread_tier"] == "P75")
    selected, fold, partition, parameters = execution._route_a_rebind_full_panel_owner_membership(
        specification=row, snapshot=snapshot, **{key: case[key] for key in (
            "attempts", "owner_attempts", "raw_ids", "frame", "dependency_warmup_bars", "precommitments")})
    assert selected is case["attempts"][0]
    assert fold == selected.candidate["folds"][fold_ordinal]
    assert partition.role == "final_lockbox" and parameters == row["parameters"]
    assert case["derive_calls"] == [(0, fold_ordinal)]
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_batch_specifications([{key: value for key, value in row.items() if key != "request_ordinal"}])
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_LOCKBOX_REFUSED"


@pytest.mark.parametrize("drift", ["footprint", "raw-id", "owner", "precommit"],
    ids=["footprint", "raw-id", "owner", "precommit"])
def test_route_a_full_panel_private_rebind_refuses_crossed_original_metadata(monkeypatch, drift):
    """INVENTED mutation of one frozen join, never financial evidence."""
    case, snapshot = _full_panel_preparation_invented_inputs(monkeypatch)
    contract = search_planner._route_a_full_panel_preparation_contract(
        owner_snapshot=snapshot, partition_cost_precommitments=case["precommitments"])
    if drift == "footprint":
        contract["requests"].pop()
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._route_a_full_panel_specifications(snapshot=snapshot, contract=contract, attempt_ordinal=0)
        assert caught.value.code == "ROUTE_A_FULL_PANEL_REQUEST_ROSTER_CHANGED"
        return
    row = execution._route_a_full_panel_specifications(snapshot=snapshot, contract=contract, attempt_ordinal=0)[4]
    if drift == "raw-id":
        case["raw_ids"] = ("INVENTED-CROSSED-RAW", case["raw_ids"][1])
    elif drift == "owner":
        case["owner_attempts"][0].plan_attempt["attempt_id"] = "INVENTED-CROSSED-HORIZON"
    else:
        geometry = snapshot["attempt_roster"][0]["folds"][0]["partitions"]["final_lockbox"]["partition_geometry_receipt_sha256"]
        case["precommitments"]["records"][geometry]["partition_frame_sha256"] = "0" * 64
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_rebind_full_panel_owner_membership(specification=row, snapshot=snapshot,
            **{key: case[key] for key in ("attempts", "owner_attempts", "raw_ids", "frame", "dependency_warmup_bars", "precommitments")})
    assert caught.value.code == "ROUTE_A_FULL_PANEL_OWNER_REBIND_INVALID"


def _full_panel_assembly_invented_entries(monkeypatch):
    """INVENTED codec/call-wiring fixture. It authenticates no kernel or Raw evidence."""
    case, snapshot = _full_panel_preparation_invented_inputs(monkeypatch)
    spec = execution.InstrumentSpec("INVENTED", 0, pip_value=1.0,
        tick_size=1.0, tick_value=1.0, point=1.0, volume_step=0.5)
    fitted = SimpleNamespace(_frame=case["frame"], instrument_spec=spec,
        source_contract_seeds={"P75": {"INVENTED_SOURCE": True}},
        frozen_tier_pair_contract={"INVENTED_PAIR": True})
    source_sha = "d" * 64
    def source_identity(seed):
        assert seed == {"INVENTED_SOURCE": True}
        return source_sha
    monkeypatch.setattr(execution, "_source_identity", source_identity)
    binding = {"executor_release_sha256": execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        "backtest_source_contract_sha256": source_sha, "instrument_spec_sha256": execution._sha256(vars(spec)),
        "caller": "pm_authoritative_search_execution._run_tier_partition", "backtester": "pm_core.Backtester.run",
        "governance_policy": None, "entry_schedule": None, "terminal_policy": "LIQUIDATE",
        "stop_provenance": "ORIGINAL_ENTRY_STOP_PRESERVED_BOTH_FIXED_CONTROL_BACKENDS"}
    entries = {}
    for fold in range(4):
        for role in ("optimizer_train", "tuning_validation", "final_lockbox"):
            trade = {"entry_bar": 1, "exit_bar": 2, "signal_bar": 0,
                "entry_time": "2019-01-01T01:00:00Z", "exit_time": "2019-01-01T02:00:00Z",
                "direction": "LONG", "entry_price": 101.0, "stop_loss": 89.0,
                "position_size": 2.0, "risk_amount": 25.0, "pnl_dollars": -4.0, "exit_reason": "closed_eod"}
            identity = execution.build_backtest_trade_identity_pnl_evidence([trade])[0]
            costs = {tier: {"INVENTED_COST": tier} for tier in ("P75", "P95")}
            axis = {"INVENTED_AXIS": role}
            matched = {"partition_role": role, "initial_risk_source_binding": copy.deepcopy(binding),
                "instrument_spec": vars(spec).copy(), "trade_power_partition_axis": axis.copy(),
                "numerical_execution_cost_attestations": copy.deepcopy(costs),
                "trades": [{"base_trade_identity": identity, "entry_price": 101.0,
                    "recorded_stop_price": 89.0, "executed_volume": 2.0,
                    "actual_initial_risk_dollars": 24.0, "risk_at_recorded_stop_dollars": 24.0,
                    "initial_stop_status": "ORIGINAL_ENTRY_STOP_FIXED_CONTROL"}]}
            for tier in ("P75", "P95"):
                lane = {key: {"INVENTED_LANE_FIELD": key} for key in execution._RAW_LANE_KEYS}
                lane["trade_identity_pnl"] = [copy.deepcopy(identity)]
                lane["numerical_execution_cost_attestation"] = costs[tier].copy()
                entries[fold, role, tier] = {"matched_base_facts": copy.deepcopy(matched) if tier == "P75" else None,
                    "result": {"trades": [trade.copy()], "trade_power_partition_axis": axis.copy(),
                        "numerical_execution_cost_attestation": costs[tier].copy()},
                    "decision": lane, "pair_check": {"status": "VERIFIED", "missing_tiers": []},
                    "signals": pd.Series([0, 1, 0])}
    calls = []
    def receipt(lanes, pair):
        assert len(lanes) == 6 and pair == fitted.frozen_tier_pair_contract
        return {"INVENTED_RECEIPT_SPY": True}
    def provenance(**kwargs):
        assert kwargs["independent_tier_execution_receipt"] == {"INVENTED_RECEIPT_SPY": True}
        assert set(kwargs["partition_signals"]) == {"optimizer_train", "tuning_validation", "final_lockbox"}
        assert len(kwargs["source_observation_references"]) == 3
        calls.append(kwargs["fold_ordinal"])
        return {"INVENTED_PROVENANCE_SPY": kwargs["fold_ordinal"]}
    monkeypatch.setattr(execution, "_independent_tier_execution_receipt", receipt)
    monkeypatch.setattr(execution, "_fold_provenance", provenance)
    monkeypatch.setattr(execution, "_partition_source_observation_reference", lambda **kwargs: {"INVENTED_REFERENCE_SPY": kwargs["partition"].role})
    def forbidden(*args, **kwargs):
        raise AssertionError("assembly must not execute another strategy/kernel")
    monkeypatch.setattr(execution, "_build_planned_attempt_evidence", forbidden)
    monkeypatch.setattr(execution, "_run_tier_partition", forbidden)
    kwargs = {"attempt": case["attempts"][0], "raw_attempt_id": case["raw_ids"][0],
        "plan_body": case["plan_body"], "fitted": fitted, "entries": entries,
        "partition_cost_precommitments": case["precommitments"], "source_observation_references": {"INVENTED": True}}
    return kwargs, calls


def test_route_a_full_panel_assembly_uses_existing_provenance_without_kernel_replay(monkeypatch):
    kwargs, calls = _full_panel_assembly_invented_entries(monkeypatch)
    body, facts = execution._route_a_assemble_full_panel_attempt(**kwargs)
    assert calls == [0, 1, 2, 3]
    assert set(body) == {"attempt_id", "folds"} and body["attempt_id"] == kwargs["raw_attempt_id"]
    assert len(body["folds"]) == 4 and len(facts) == 12
    for fold in body["folds"]:
        assert set(fold) == {"fold_ordinal", "provenance", "lanes"}
        assert len(fold["lanes"]) == 6
        for lane in fold["lanes"].values():
            assert set(lane) == set(execution._RAW_LANE_KEYS)
            assert lane["trade_identity_pnl"][0]["risk_amount"] == 25.0
    assert all(row["facts"]["trades"][0]["actual_initial_risk_dollars"] == 24.0 for row in facts)
    assert not {"matched_base_facts", "initial_risk_source_binding"} & set(body)


@pytest.mark.parametrize("drift", ["risk", "identity", "p95-cost"], ids=["risk", "identity", "p95-cost"])
def test_route_a_full_panel_assembly_refuses_crossed_matched_base_witness(monkeypatch, drift):
    kwargs, calls = _full_panel_assembly_invented_entries(monkeypatch)
    entry = kwargs["entries"][0, "optimizer_train", "P75"]
    if drift == "risk":
        entry["matched_base_facts"]["trades"][0]["actual_initial_risk_dollars"] = 25.0
    elif drift == "identity":
        entry["matched_base_facts"]["trades"][0]["base_trade_identity"]["direction"] = "SHORT"
    else:
        kwargs["entries"][0, "optimizer_train", "P95"]["result"]["numerical_execution_cost_attestation"] = {"INVENTED_CROSSED_COST": True}
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._route_a_assemble_full_panel_attempt(**kwargs)
    assert caught.value.code == "ROUTE_A_FULL_PANEL_MATCHED_BASE_WITNESS_CHANGED"
    assert calls == []



def _full_panel_matched_fill_invented_inputs(direction, scenario):
    """INVENTED integer-price fixture reusing N26 rounding/core810 branch anchors."""
    from test_frozen_spread_execution import _continuous_case
    from test_backtester_cost_timing import _OneShotAtrStrategy
    backtester, fixture, _expected = _continuous_case("N26_ROUNDING_REQUESTED_VS_EXECUTED_STOP_RISK")
    owners = fixture["entry_schedule"].owners
    base = next(owner for owner in owners if owner.spread_tier.tier == "P75")
    stress = next(owner for owner in owners if owner.spread_tier.tier == "P95")
    frame = base.features.copy(deep=True)
    frame["ATR_14"] = 6.0
    frame["REGIME_LIVE"] = "TRENDING"
    bar = 1 if scenario.startswith("samebar") else 2
    if scenario == "gap_sl":
        price = 80.0 if direction == 1 else 120.0
        frame.loc[frame.index[bar], ["Open", "High", "Low", "Close"]] = price
    elif scenario == "touch_sl":
        frame.loc[frame.index[bar], "Low" if direction == 1 else "High"] = 88.0 if direction == 1 else 112.0
    elif scenario == "touch_tp":
        frame.loc[frame.index[bar], "High" if direction == 1 else "Low"] = 114.0 if direction == 1 else 86.0
    elif scenario == "tp_gap":
        price = 130.0 if direction == 1 else 70.0
        frame.loc[frame.index[bar], ["Open", "High", "Low", "Close"]] = price
    elif scenario == "samebar_both":
        frame.loc[frame.index[bar], ["High", "Low"]] = [120.0, 80.0]
    elif scenario in ("samebar_spread_gap", "exit_spike"):
        frame.loc[frame.index[bar], "SpreadMax"] = 32.0
    elif scenario != "eod":
        raise AssertionError("Unknown invented branch fixture")
    strategy = _OneShotAtrStrategy(direction)
    signals = strategy.generate_signals(frame, fixture["symbol"])
    return backtester, fixture, frame, strategy, signals, base.spread_tier, stress.spread_tier


def _full_panel_run_matched_invented_fixture(monkeypatch, compiled, direction, scenario,
        mutate_result=None, empty_signals=False, mutate_features=None, stress_override=None):
    """Run genuine tiny kernels; optional corruption is labelled refusal-only evidence."""
    assert execution.pm_core.NUMBA_AVAILABLE, "Both execution backends are required"
    backtester, fixture, frame, strategy, signals, base, stress = _full_panel_matched_fill_invented_inputs(direction, scenario)
    if mutate_features is not None:
        mutate_features(frame)
    if stress_override is not None:
        stress = stress_override
    if empty_signals:
        signals[:] = 0
    monkeypatch.setattr(execution.pm_core, "NUMBA_AVAILABLE", compiled)
    def ordinary_run(*args, **kwargs):
        return original_run(*args, **kwargs)
    original_run = backtester.run
    ordinary = ordinary_run(frame, signals, fixture["symbol"], strategy,
        spec=fixture["spec"], timeframe=fixture["timeframe"], warmup_bars=fixture["warmup_bars"],
        spread_tier=base, entry_spread_observations=fixture["entry_spread_observations"])
    source_sha = execution._source_identity(execution.raw_winner.build_trade_source_contract_from_backtester_result(ordinary))
    if mutate_result is not None:
        def corrupted_run(*args, **kwargs):
            result = copy.deepcopy(original_run(*args, **kwargs))
            mutate_result(result)
            return result
        monkeypatch.setattr(backtester, "run", corrupted_run)
    partition = execution._PartitionSlice("final_lockbox", frame, fixture["warmup_bars"],
        frame.index[fixture["warmup_bars"]].isoformat(), frame["_BAR_CLOSE_TS"].iloc[-1].isoformat(), {})
    facts, decision = {}, {}
    result, features, _signals, _backtester = execution._run_tier_partition(
        partition=partition, full_prefix_surface=execution._FullPrefixStrategySurface(frame, signals),
        backtester=backtester, strategy=strategy, symbol=fixture["symbol"], timeframe=fixture["timeframe"],
        regime="TRENDING", config=backtester.config, spec=fixture["spec"],
        expected_source_contract_sha256=source_sha, spread_tier=base,
        entry_spread_observations=fixture["entry_spread_observations"], decision_result_out=decision,
        _matched_base_facts_out=facts, _matched_stress_tier=stress)
    return result, facts, features, decision, backtester, fixture, base, stress


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
@pytest.mark.parametrize("scenario, branch", [
    ("eod", "EOD"), ("gap_sl", "GAP_SL"), ("touch_sl", "TOUCH_SL"),
    ("touch_tp", "TOUCH_TP"), ("tp_gap", "TOUCH_TP"),
    ("samebar_both", "TOUCH_SL"), ("samebar_spread_gap", "GAP_SL"), ("exit_spike", "GAP_SL"),
], ids=["eod", "gap-sl", "touch-sl", "touch-tp", "tp-gap-level", "samebar-sl-first", "samebar-spread-gap", "exit-spike"])
def test_route_a_matched_base_fill_and_price_ratio_exact_kernel_parity(monkeypatch, compiled, direction, scenario, branch):
    result, facts, features, decision, backtester, fixture, base, stress = _full_panel_run_matched_invented_fixture(
        monkeypatch, compiled, direction, scenario)
    assert len(result["trades"]) == len(facts["trades"]) == 1
    trade, row = result["trades"][0], facts["trades"][0]
    witness = row["base_fill_witness"]
    assert witness["status"] == "VERIFIED" and witness["branch"] == branch
    assert witness["expected_entry_price"] == trade["entry_price"]
    assert witness["expected_exit_price"] == trade["exit_price"]
    assert witness["original_stop_price"] == trade["stop_loss"]
    assert witness["original_target_price"] == trade["take_profit"]
    assert witness["same_bar"] == (scenario.startswith("samebar"))
    assert witness["held_bars_scanned"] == trade["exit_bar"] - trade["entry_bar"] + 1
    assert [item["bar"] for item in witness["held_bar_inputs"]] == list(range(trade["entry_bar"], trade["exit_bar"] + 1))
    assert witness["held_bar_inputs_sha256"] == execution._sha256(witness["held_bar_inputs"])
    assert witness["exit_bar_ohlc"] == {key: float(features[key].iloc[trade["exit_bar"]]) for key in ("Open", "High", "Low", "Close")}
    assert facts["base_fill_witness_sha256"] == execution._sha256([witness])
    assert facts["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"
    assert facts["floor_reason_codes"] == [] and facts["position_event_bijection_status"] == "VERIFIED"
    assert row["base_position_event"] == result["position_events"][0]
    assert facts["completed_position_ids"] == [result["position_events"][0]["position_id"]]
    assert facts["completed_position_open_event_order"] == [trade["entry_bar"] * 4 + 1]
    assert facts["completed_position_close_event_order"] == [trade["exit_bar"] * 4 + 3]
    assert pd.Timestamp(facts["completed_position_open_utc"][0]) == pd.Timestamp(trade["entry_time"])
    assert pd.Timestamp(facts["completed_position_close_utc"][0]) == pd.Timestamp(trade["exit_time"])
    entry, intrabar, _metadata = backtester._build_half_spread_arrays(features, fixture["spec"], spread_tier=stress, timeframe=fixture["timeframe"])
    numerator = float(entry[trade["entry_bar"]]) + float(intrabar[trade["exit_bar"]])
    distance = abs(trade["entry_price"] - trade["stop_loss"])
    assert row["matched_p95_cost_inputs"]["numerator_price"] == numerator
    assert row["matched_p95_cost_inputs"]["original_initial_stop_distance_price"] == distance
    assert row["matched_p95_spread_cost_r_unrounded"] == numerator / distance
    assert row["matched_p95_spread_cost_r"] == execution.pm_core.canonical_backtest_winner_r(numerator, distance)
    assert facts["matched_p95_spread_cost_r"] == [row["matched_p95_spread_cost_r"]]
    exact = row["executed_price_swap_capture"]
    expected_base_r = direction * (exact["exit_price"] - exact["entry_price"]) / distance + exact["swap_dollars_exact"] / row["actual_initial_risk_dollars"]
    assert facts["base_actual_initial_risk_net_r"] == [execution.pm_core.canonical_backtest_winner_r(expected_base_r, 1.0)]
    assert row["actual_initial_risk_dollars"] == 24.0 and trade["risk_amount"] == 25.0
    assert facts["dated_fx_status"] == "NOT_REQUIRED_PRICE_RATIO_FLOOR"
    assert facts["cash_conversion_limitation"] == "INSTRUMENT_SPEC_ACQUISITION_SNAPSHOT_NO_DATED_FX_SERIES"
    verified = execution._verify_route_a_matched_base_facts(facts)
    assert verified["authenticates_origin"] is False
    assert all(verified[key] == facts[key] for key in verified if key != "authenticates_origin")
    assert set(decision) == set(execution._RAW_LANE_KEYS)


@pytest.mark.parametrize("corruption", ["entry-fill", "exit-fill", "position-id"], ids=["entry-fill", "exit-fill", "position-id"])
def test_route_a_matched_floor_is_wholly_unavailable_after_kernel_witness_mismatch(monkeypatch, corruption):
    """INVENTED corruption after genuine tiny kernel; it grants no owner authenticity."""
    def corrupt(result):
        if corruption == "entry-fill":
            result["trades"][0]["entry_price"] += 1.0
        elif corruption == "exit-fill":
            result["trades"][0]["exit_price"] += 1.0
        else:
            result["position_events"][0]["position_id"] = "INVENTED-CROSSED-ID"
    result, facts, *_ = _full_panel_run_matched_invented_fixture(monkeypatch, False, 1, "eod", mutate_result=corrupt)
    assert len(result["trades"]) == 1
    assert facts["floor_status"] == "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH"
    assert facts["floor_reason_codes"]
    assert facts["completed_position_ids"] is None
    assert facts["base_actual_initial_risk_net_r"] is facts["matched_p95_spread_cost_r"] is None


def test_route_a_matched_floor_empty_completed_base_is_explicitly_unavailable(monkeypatch):
    result, facts, *_ = _full_panel_run_matched_invented_fixture(monkeypatch, False, 1, "eod", empty_signals=True)
    assert result["trades"] == result["position_events"] == facts["trades"] == []
    assert facts["floor_status"] == "UNAVAILABLE_NO_COMPLETED_BASE_TRADES"
    assert facts["base_fill_witness_status"] == "NO_COMPLETED_BASE_TRADES"
    assert facts["completed_position_ids"] is facts["matched_p95_spread_cost_r"] is None


def test_route_a_matched_base_fill_replay_refuses_hidden_earlier_trigger(monkeypatch):
    result, facts, features, _decision, backtester, fixture, base, _stress = _full_panel_run_matched_invented_fixture(monkeypatch, False, 1, "eod")
    changed = features.copy(deep=True)
    changed.loc[changed.index[2], "Low"] = 80.0
    entry, intrabar, _metadata = backtester._build_half_spread_arrays(changed, fixture["spec"], spread_tier=base, timeframe=fixture["timeframe"])
    witness = execution._route_a_base_fill_witness(trade=result["trades"][0], identity=facts["trades"][0]["base_trade_identity"],
        features=changed, base_entry_half=entry, base_intrabar_half=intrabar)
    assert witness["status"] == "MISMATCH"
    assert witness["reason_code"] == "BASE_FIRST_TRIGGER_OR_EXIT_FILL_MISMATCH"



@pytest.mark.parametrize("role", ["entry_half_spread", "intrabar_half_spread"])
def test_route_a_spread_array_codec_preserves_exact_sentinels_and_signed_zero(role):
    """INVENTED binary payload; no new sentinel or numerical policy."""
    import struct
    raw = struct.pack("<QQQQ", 0x8000000000000000, 0x7FF8000000000042, 0x3FF0000000000000, 0)
    array = execution.np.frombuffer(raw, dtype="<f8")
    encoded = execution._route_a_encode_spread_array(array, role)
    assert encoded["dtype"] == "<f8" and encoded["shape"] == [4]
    decoded = execution._route_a_decode_spread_array(encoded, role)
    assert decoded.tobytes() == raw
    assert encoded["array_sha256"] == execution.Backtester._spread_array_sha256(role, array)
    wrong_role = "intrabar_half_spread" if role == "entry_half_spread" else "entry_half_spread"
    with pytest.raises(ValueError):
        execution._route_a_decode_spread_array(encoded, wrong_role)


@pytest.mark.parametrize("corruption", ["shape", "bytes", "fill-path", "cost", "risk", "claimed-verdict"],
    ids=["shape", "bytes", "fill-path", "cost", "risk", "claimed-verdict"])
def test_route_a_serialized_matched_witness_recomputes_data_and_refuses_tampering(monkeypatch, corruption):
    """INVENTED corruption after genuine tiny kernel; validator issues no origin authority."""
    import base64
    result, facts, *_ = _full_panel_run_matched_invented_fixture(monkeypatch, False, 1, "eod")
    changed = copy.deepcopy(facts)
    if corruption == "shape":
        changed["half_spread_arrays"]["P75"]["entry_half_spread"]["shape"][0] += 1
    elif corruption == "bytes":
        encoded = changed["half_spread_arrays"]["P95"]["intrabar_half_spread"]
        values = bytearray(base64.b64decode(encoded["values_base64"]))
        values[-1] ^= 1
        encoded["values_base64"] = base64.b64encode(values).decode("ascii")
    elif corruption == "fill-path":
        witness = changed["trades"][0]["base_fill_witness"]
        witness["held_bar_inputs"][1]["ohlc"]["Low"] = 80.0
        witness["held_bar_inputs_sha256"] = execution._sha256(witness["held_bar_inputs"])
        changed["base_fill_witness_sha256"] = execution._sha256([witness])
    elif corruption == "cost":
        changed["trades"][0]["matched_p95_spread_cost_r"] += 1.0
    elif corruption == "risk":
        changed["trades"][0]["actual_initial_risk_dollars"] = 25.0
    else:
        changed["trades"][0]["base_fill_witness"]["status"] = "MISMATCH"
        changed["base_fill_witness_sha256"] = execution._sha256([changed["trades"][0]["base_fill_witness"]])
    verified = execution._verify_route_a_matched_base_facts(changed)
    assert verified["authenticates_origin"] is False
    assert verified["floor_status"] == "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH"
    assert verified["floor_reason_codes"]
    assert verified["base_actual_initial_risk_net_r"] is verified["matched_p95_spread_cost_r"] is None
    assert result["trades"][0]["risk_amount"] == 25.0



@pytest.mark.parametrize("role", ["entry_half_spread", "intrabar_half_spread"])
def test_route_a_non_improvement_checks_unused_bars_in_both_spread_legs(role):
    """INVENTED vectors; an unused finite inversion still violates E5872."""
    arrays = {tier: {name: execution.np.array([1.0, 1.0, 1.0]) for name in ("entry_half_spread", "intrabar_half_spread")}
        for tier in ("P75", "P95")}
    arrays["P95"][role][0] = 0.5
    assert execution._route_a_spread_non_improvement(arrays) == {"status": "REFUSED_P95_BELOW_P75", "role": role, "bar": 0}


@pytest.mark.parametrize("inversion", [False, True], ids=["unused-sentinel", "sentinel-does-not-hide-inversion"])
def test_route_a_non_improvement_preserves_unknown_sentinels_without_new_finiteness_gate(inversion):
    """INVENTED warmup sentinel; it is preserved and disclosed, never normalized."""
    arrays = {tier: {role: execution.np.array([float("nan"), 1.0, 2.0]) for role in ("entry_half_spread", "intrabar_half_spread")}
        for tier in ("P75", "P95")}
    if inversion:
        arrays["P95"]["intrabar_half_spread"][2] = 1.0
    observed = execution._route_a_spread_non_improvement(arrays)
    if inversion:
        assert observed == {"status": "REFUSED_P95_BELOW_P75", "role": "intrabar_half_spread", "bar": 2}
    else:
        assert observed == {"status": "NO_FINITE_INVERSION_WITH_SENTINELS",
            "unknown_indices": {"entry_half_spread": [0], "intrabar_half_spread": [0]}}


def test_route_a_live_matched_capture_refuses_unused_warmup_tier_inversion(monkeypatch):
    """INVENTED frozen-tier mutation after accepted N26 fixture; no policy authority."""
    import json
    _backtester, _fixture, _frame, _strategy, _signals, _base, stress = _full_panel_matched_fill_invented_inputs(1, "eod")
    descriptor = stress.descriptor()
    descriptor["full_spread_price"], descriptor["spread_points"] = 1.0, 1
    inverted = execution.spread_profile.FrozenSpreadTier(json.dumps(descriptor, sort_keys=True, separators=(",", ":")))
    def warmup(frame):
        frame.loc[frame.index[0], ["SpreadOpen", "SpreadMax"]] = 0.0
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        _full_panel_run_matched_invented_fixture(monkeypatch, False, 1, "eod",
            mutate_features=warmup, stress_override=inverted)
    assert caught.value.code == "ROUTE_A_FULL_PANEL_P95_SPREAD_NON_IMPROVEMENT_VIOLATION"



def _exact_floor_fractional_invented_fixture(monkeypatch, compiled, direction, capped=False, legacy=False):
    """INVENTED fractional/swap fixture; genuine tiny kernels, no financial evidence."""
    assert execution.pm_core.NUMBA_AVAILABLE, "Both execution backends are required"
    original_swap_inputs = execution.Backtester._build_causal_swap_inputs
    label = "INVENTED_SOURCE_ENVELOPE_NO_AUTHORITY"

    def invented_source_envelope(self, *args, **kwargs):
        inputs = original_swap_inputs(self, *args, **kwargs)
        # Invented provenance only; real kernel, costs and exact swap arrays remain.
        swap = dict(inputs[-1])
        swap.update(fixture_provenance=label, status="AVAILABLE", provenance_status="AUTHENTICATED",
            source_provenance_authenticated=True, historical_rates_authenticated=True,
            execution_state_compounding=True, timestamp_source="_BAR_CLOSE_TS",
            projection_contract="fxpm-swap-preopen-close-projection-v1",
            projection_sha256=execution._sha256([label, "projection"]))
        for name in ("price_activation_manifest_sha256", "terms_activation_manifest_sha256",
                     "terms_snapshot_sha256", "schedule_sha256", "target_bars_sha256",
                     "target_sidecar_sha256", "historical_rates_sha256"):
            swap[name] = execution._sha256([label, name])
        return (*inputs[:-1], swap)

    monkeypatch.setattr(execution.Backtester, "_build_causal_swap_inputs", invented_source_envelope)
    backtester, fixture, frame, strategy, signals, base, stress = _full_panel_matched_fill_invented_inputs(direction, "eod")
    frame.index = pd.date_range("2026-01-05T22:00:00Z", periods=len(frame), freq="h")
    frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(hours=1)
    frame.loc[frame.index[-1], ["High", "Close"]] = 100.00371
    if legacy:
        frame = frame.drop(columns=["_BAR_CLOSE_TS"])
    fixture["entry_spread_observations"] = pd.Series(2.0, index=frame.index)
    fixture["spec"].max_lot = 1.5 if capped else 100.0
    fixture["spec"].swap_mode = 4
    fixture["spec"].swap_long, fixture["spec"].swap_short = -0.001731, 0.002913
    backtester.config.swap_model_enabled = True
    signals.index = frame.index
    monkeypatch.setattr(execution.pm_core, "NUMBA_AVAILABLE", compiled)
    ordinary = backtester.run(frame, signals, fixture["symbol"], strategy,
        spec=fixture["spec"], timeframe=fixture["timeframe"], warmup_bars=fixture["warmup_bars"],
        spread_tier=base, entry_spread_observations=fixture["entry_spread_observations"])
    source_sha = execution._source_identity(execution.raw_winner.build_trade_source_contract_from_backtester_result(ordinary))
    end = frame.index[-1] + pd.Timedelta(hours=1)
    partition = execution._PartitionSlice("final_lockbox", frame, fixture["warmup_bars"],
        frame.index[fixture["warmup_bars"]].isoformat(), end.isoformat(), {})
    facts = {}
    refusal = None
    try:
        result, features, _signals, _backtester = execution._run_tier_partition(
            partition=partition, full_prefix_surface=execution._FullPrefixStrategySurface(frame, signals),
            backtester=backtester, strategy=strategy, symbol=fixture["symbol"], timeframe=fixture["timeframe"],
            regime="TRENDING", config=backtester.config, spec=fixture["spec"], expected_source_contract_sha256=source_sha,
            spread_tier=base, entry_spread_observations=fixture["entry_spread_observations"],
            _matched_base_facts_out=facts, _matched_stress_tier=stress)
    except execution.AuthoritativeSearchExecutionError as exc:
        if legacy is not True:
            raise
        if not facts or not facts.get("trades") or not facts.get("initial_risk_source_binding"):
            raise
        refusal = exc
    if legacy:
        return ordinary, facts, frame, backtester, fixture, base, stress, end, refusal
    assert result == ordinary
    return result, facts, features, backtester, fixture, base, stress, end


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
@pytest.mark.parametrize("capped", [False, True], ids=["lot-rounded", "lot-capped"])
def test_exact_executed_floor_fractional_cash_swap_and_public_output_parity(monkeypatch, compiled, direction, capped):
    result, facts, frame, backtester, fixture, base, stress, end = _exact_floor_fractional_invented_fixture(
        monkeypatch, compiled, direction, capped)
    trade, row = result["trades"][0], facts["trades"][0]
    exact = row["executed_price_swap_capture"]
    assert len(result["trades"]) == 1 and exact["backend"] == ("NUMBA" if compiled else "PYTHON")
    assert exact["executed_volume"] == (1.5 if capped else 2.0)
    assert row["actual_initial_risk_dollars"] == (18.0 if capped else 24.0)
    assert trade["risk_amount"] == 25.0
    assert exact["swap_dollars_exact"] != round(exact["swap_dollars_exact"], 2)
    assert trade["swap_dollars"] == round(exact["swap_dollars_exact"], 2)
    assert exact["swap_source_kind"] == "CAUSAL_KERNEL" and exact["swap_cash_steps"]
    assert facts["exact_swap_accrual_contract"] == result["swap_accrual_contract"]
    distance = abs(exact["entry_price"] - exact["initial_stop_price"])
    wanted = direction * (exact["exit_price"] - exact["entry_price"]) / distance + exact["swap_dollars_exact"] / row["actual_initial_risk_dollars"]
    actual = execution.pm_core.canonical_backtest_winner_r(wanted, 1.0)
    assert facts["base_actual_initial_risk_net_r"] == [actual]
    assert actual != execution.pm_core.canonical_backtest_winner_r(trade["pnl_dollars"], row["actual_initial_risk_dollars"])
    verified = execution._verify_route_a_matched_base_facts(facts)
    assert verified["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED" and verified["authenticates_origin"] is False
    assert verified["base_actual_initial_risk_net_r"] == [actual]
    geometry = {"frame_start_utc": frame.index[0].isoformat(), "end_exclusive_utc": end.isoformat(),
        "frame_rows": len(frame), "frame_sha256": execution._frame_sha256(frame)}
    def forbidden_run(*args, **kwargs):
        raise AssertionError("cold witness source verification must not execute a kernel")
    monkeypatch.setattr(execution.Backtester, "run", forbidden_run)
    assert execution._verify_route_a_matched_fact_source(facts, geometry, source_frame=frame, spec=fixture["spec"],
        tiers={"P75": base, "P95": stress}, config=backtester.config, timeframe=fixture["timeframe"])


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
def test_exact_executed_floor_legacy_swap_preserves_public_reporting(monkeypatch, compiled):
    ordinary, facts, frame, backtester, fixture, _base, _stress, _end, refusal = _exact_floor_fractional_invented_fixture(
        monkeypatch, compiled, 1, legacy=True)
    assert isinstance(refusal, execution.AuthoritativeSearchExecutionError)
    assert refusal.code == "BACKTEST_DECISION_RELEVANT_RESULT_INCOMPLETE"
    row = facts["trades"][0]
    exact = row["executed_price_swap_capture"]
    assert exact["swap_source_kind"] == "LEGACY_MODEL" and exact["swap_cash_steps"]
    assert exact["swap_cash_steps"][0]["phase"] == "LEGACY_AT_EXIT"
    assert exact["swap_dollars_exact"] != round(exact["swap_dollars_exact"], 2)
    assert ordinary["trades"][0]["swap_dollars"] == round(exact["swap_dollars_exact"], 2)
    assert execution._verify_route_a_exact_swap_source(facts, frame, spec=fixture["spec"], pricing=backtester,
        timeframe=fixture["timeframe"])
    assert execution._verify_route_a_matched_base_facts(facts)["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"


@pytest.mark.parametrize("corruption", ["missing", "ordinal", "backend", "entry", "stop", "volume", "swap-cash", "swap-unit", "swap-step"])
def test_exact_executed_floor_serialized_capture_tampering_is_unavailable(monkeypatch, corruption):
    _result, original, *_rest = _exact_floor_fractional_invented_fixture(monkeypatch, False, 1)
    facts = copy.deepcopy(original)
    exact = facts["trades"][0]["executed_price_swap_capture"]
    if corruption == "missing":
        facts["trades"][0]["executed_price_swap_capture"] = None
    elif corruption == "backend":
        exact["backend"] = "PUBLIC_FLAG"
    elif corruption == "swap-step":
        exact["swap_cash_steps"][0]["phase"] = "PRE_OPEN"
    else:
        field = {"ordinal": "ordinal", "entry": "entry_price", "stop": "initial_stop_price", "volume": "executed_volume",
            "swap-cash": "swap_dollars_exact", "swap-unit": "swap_rollover_units"}[corruption]
        exact[field] += 1
    verified = execution._verify_route_a_matched_base_facts(facts)
    assert verified["floor_status"] == "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH"
    assert verified["authenticates_origin"] is False and verified["base_actual_initial_risk_net_r"] is None


@pytest.mark.parametrize("corruption", ["self-consistent-rate", "contract"])
def test_exact_swap_source_rejects_self_consistent_public_capture_changes(monkeypatch, corruption):
    _result, original, frame, pricing, fixture, _base, _stress, _end = _exact_floor_fractional_invented_fixture(monkeypatch, False, 1)
    facts = copy.deepcopy(original)
    if corruption == "contract":
        facts["exact_swap_accrual_contract"]["mode"] = "INVENTED_FORGED_SOURCE"
        expected = "MATCHED_FLOOR_SWAP_CONTRACT_SOURCE_CHANGED"
    else:
        row = facts["trades"][0]
        exact = row["executed_price_swap_capture"]
        for step in exact["swap_cash_steps"]:
            step["rate_per_lot_dollars"] *= 1.37
            step["cash"] = step["rate_per_lot_dollars"] * exact["executed_volume"] * step["rollover_units"]
        exact["swap_dollars_exact"] = sum(step["cash"] for step in exact["swap_cash_steps"])
        components = execution._route_a_exact_base_return_components(row, spec=fixture["spec"], ordinal=0, swap_model_enabled=True)
        row["base_return_components"] = components
        row["base_actual_initial_risk_net_r"] = components["canonical_net_r"]
        facts["base_actual_initial_risk_net_r"] = [components["canonical_net_r"]]
        assert execution._verify_route_a_matched_base_facts(facts)["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"
        expected = "MATCHED_FLOOR_EXACT_SWAP_SOURCE_CHANGED"
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._verify_route_a_exact_swap_source(facts, frame, spec=fixture["spec"], pricing=pricing,
            timeframe=fixture["timeframe"])
    assert caught.value.code == expected


@pytest.mark.parametrize("argument", ["governance", "scheduled", "mark-open", "nonempty"])
def test_exact_trade_companion_refuses_non_fixed_control_or_existing_output(argument):
    """INVENTED invalid invocation stops before signal/data access or execution."""
    pricing = execution.Backtester(execution.PipelineConfig())
    output, kwargs = {}, {}
    if argument == "governance": kwargs["governance_policy"] = {"kind": "INVENTED"}
    elif argument == "scheduled": kwargs["entry_schedule"] = object()
    elif argument == "mark-open": kwargs["terminal_policy"] = "MARK_OPEN"
    else: output["foreign"] = "INVENTED_DRAFT"
    with pytest.raises(ValueError, match="EXACT_TRADE_CAPTURE_FIXED_CONTROL_ARGUMENT_INVALID"):
        pricing.run(None, None, "INVENTED", None, _exact_trade_facts_out=output, **kwargs)



def _attempt_boundary_invented_continuation(tmp_path, monkeypatch, *, full_panel=True,
                                           damage=None, expiry=None, attempt_count=1):
    """INVENTED one-period call-routing fixture, never an authenticated owner/model witness.

    Authentication, source probes and the kernel are labelled stand-ins. The actual
    continuation, preparation boundary method, request coordinator and working-copy
    fingerprints execute; no fitting, market data or ordinary programme is run.
    """
    from dataclasses import dataclass
    import json
    import pm_historical_search_producer as producer
    import pm_production_config_binding as config_binding

    events, reads = [], {"config": 0, "stream": 0}
    state = {"expired": expiry == "admission", "working": None}
    frame = _d1_frame(2)
    fitted = _route_a_invented_fingerprint_namespace()
    fitted._frame = frame
    fitted._entry_spread_observations = pd.Series([1.0, 1.0], index=frame.index)
    fitted.frame_sha256 = execution._frame_sha256(frame)
    fitted._spread_tiers["P95"] = copy.deepcopy(fitted._spread_tiers["P75"])
    fitted.source_contract_seeds["P95"] = copy.deepcopy(fitted.source_contract_seeds["P75"])
    config = SimpleNamespace(use_spread=True, fixture_value=1)
    payload = {"fixture_provenance": "INVENTED_ONE_PERIOD_NO_AUTHORITY"}
    frozen_config = SimpleNamespace(active_config_path=tmp_path / "invented-config.json",
        config_sha256="c" * 64, effective_config_sha256="d" * 64,
        _payload_json=execution._canonical_bytes(payload),
        _snapshot_json=execution._canonical_bytes({"pipeline_config": vars(config)}), _seal=object())

    @dataclass
    class InventedLimits:
        max_json_bytes: int = 65536
        max_artifact_bytes: int = 65536

    @dataclass
    class InventedFrozen:
        partition_cost_precommitments: dict
        source_contract_seeds: dict
        frozen_tier_pair_contract: dict

    descriptor = {"relative_path": "invented-frozen.json", "sha256": "b" * 64, "size_bytes": 2}
    frozen = InventedFrozen({}, copy.deepcopy(fitted.source_contract_seeds),
                           copy.deepcopy(fitted.frozen_tier_pair_contract))
    handle = SimpleNamespace(frozen=frozen, frozen_descriptor_graph=descriptor,
        cross_binding_descriptor=descriptor, frozen_receipt_descriptor=descriptor,
        symbol="INVENTED", timeframe="D1", regime="INVENTED", artifact_namespace="invented",
        _cross_binding_bytes=b"{}", _attempt_artifacts=(), _horizon_execution_contracts=(),
        _source_count_receipts=(), _capture_nonce=None)
    context = SimpleNamespace(root=tmp_path, generation_id="INVENTED-ONE-PERIOD",
        context_sha256="a" * 64, config_sha256="c" * 64,
        native_universe_sha256="e" * 64, limits=InventedLimits())
    body = {"artifact_namespace": "invented", "cell_id": "invented-cell",
        "slot_id": "invented-slot", "attempt_id": "invented-attempt", "precommit_sha256": "7" * 64,
        "source_access_window_utc": {
        "start_utc": "2020-01-01T00:00:00+00:00", "end_exclusive_utc": "2020-01-02T00:00:00+00:00"}}
    request = SimpleNamespace(receipt_sha256="f" * 64, to_dict=lambda: copy.deepcopy(body),
        _precommit=SimpleNamespace(to_dict=lambda: {"fixture": "INVENTED_PRECOMMIT"}))
    domain = {"strategy_id": "INVENTED_STRATEGY", "strategy_family": "INVENTED_FAMILY",
        "domain_sha256": "1" * 64, "parameter_domain": {
            "schema_version": "fxpm.parameter-domain.v1", "defaults": {"period": 2},
            "axes": [{"names": ["period"], "choices": [[2]]}], "point_count": 1}}
    process = SimpleNamespace(receipt_sha256="2" * 64,
        to_dict=lambda: {"semantics": {"strategy_domains": [domain]}})
    plan = {"cell": {"symbol": "INVENTED", "timeframe": "D1", "regime": "INVENTED"},
        "anchor": {"anchor_id": "INVENTED", "selection_cutoff_utc": "2020-01-01T00:00:00Z"},
        "derived_dependency_geometry": {"warmup_bars": 0}}
    cell_plan = SimpleNamespace(receipt_sha256="3" * 64, cell_receipt={}, spool_entry={},
        horizon_plan=SimpleNamespace(to_dict=lambda: {}), attempt_artifacts=lambda: (),
        state_requirements=lambda: ())
    session = SimpleNamespace(_frozen_config=frozen_config, _entry={"descriptor": descriptor})
    capture = SimpleNamespace(context=context, request=request, source_session=session,
        sink=SimpleNamespace(artifact_roster=lambda: ()), process=process, cell_plan=cell_plan,
        pipeline_config=config, fitted_timeframe=fitted, _finished=False, _completion=None,
        _require_active=lambda current: events.append(("owner", current is handle)))
    header = {"artifact_roster": [], "frozen_plan_receipt": descriptor,
        "frozen_descriptor_graph": descriptor, "horizon_winner_cross_binding": descriptor,
        "source_session_entry": session._entry, "horizon_plan": {"descriptor": descriptor}}
    attempts = tuple(SimpleNamespace(ordinal=ordinal, plan_attempt={"status": "PLANNED",
        "attempt_id": "INVENTED-ATTEMPT" + (f"-{ordinal}" if ordinal else "")},
        artifacts=SimpleNamespace(winner_process={"strategy_id": "INVENTED_STRATEGY"}))
        for ordinal in range(attempt_count))
    raw_ids = tuple("INVENTED-RAW" + (f"-{ordinal}" if ordinal else "")
        for ordinal in range(attempt_count))
    binding = {"executor_release_sha256": execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256}
    snapshot = {"declared_unit_binding": binding,
        "attempt_roster": [{"raw_attempt_id": raw_id} for raw_id in raw_ids]}
    rows = [{"strategy_id": "INVENTED_STRATEGY", "parameters": {"period": 2},
        "fold_ordinal": fold, "partition_role": role, "spread_tier": tier,
        "request_ordinal": index, "bookkeeping": {"attempt_ordinal": 0}}
        for index, (fold, role, tier) in enumerate((fold, role, tier)
            for fold in range(4) for role in ("optimizer_train", "tuning_validation", "final_lockbox")
            for tier in ("P75", "P95"))]

    def specifications(attempt_ordinal):
        selected = copy.deepcopy(rows)
        for row in selected:
            row["bookkeeping"]["attempt_ordinal"] = attempt_ordinal
        return selected

    def read_config(path):
        reads["config"] += 1
        events.append(("full", reads["config"]))
        return copy.deepcopy(payload), None

    def stream_file(root, relative, **kwargs):
        reads["stream"] += 1
        raw = execution._canonical_bytes(header) if relative.endswith("header.json") else b"{}"
        return "b" * 64, len(raw), raw if kwargs.get("collect") else None

    def partitions(*, frame, fold, **kwargs):
        return tuple(SimpleNamespace(role=role, frame=frame,
            geometry_receipt={"fixture_fold": fold["ordinal"], "fixture_role": role})
            for role in ("optimizer_train", "tuning_validation", "final_lockbox"))

    def rebind(*, specification, frame, **kwargs):
        fold = {"fold_id": "INVENTED-" + str(specification["fold_ordinal"]),
                "ordinal": specification["fold_ordinal"]}
        partition = next(row for row in partitions(frame=frame, fold=fold)
                         if row.role == specification["partition_role"])
        return attempts[specification["bookkeeping"]["attempt_ordinal"]], fold, partition, {"period": 2}

    def probe_mutation(config, frame):
        state["working"] = (config, frame)
        if damage == "frame_probe":
            frame.iloc[0, 0] += 1.0
        elif damage == "config_probe":
            config.fixture_value += 1
        if expiry == "after_probe":
            state["expired"] = True

    class InventedProbeBacktester:
        def __init__(self, config):
            self.config = config

        def build_evidence_source_seed_result(self, features, symbol, *, spec=None,
                                             timeframe="", spread_tier=None,
                                             entry_spread_observations=None):
            events.append(("probe", reads["config"], reads["stream"]))
            probe_mutation(self.config, features)
            return {"spread_model": "INVENTED"}

    def run_partition(*, backtester, partition, decision_result_out, **kwargs):
        events.append(("run", reads["config"], reads["stream"]))
        state["working"] = (backtester.config, partition.frame)
        if damage == "frame_run":
            partition.frame.iloc[0, 0] += 1.0
        elif damage == "config_run":
            backtester.config.fixture_value += 1
        if expiry == "after_run":
            state["expired"] = True
        if kwargs.get("_matched_base_facts_out") is not None:
            kwargs["_matched_base_facts_out"]["half_spread_arrays"] = {}
        decision_result_out["fixture_provenance"] = "INVENTED_KERNEL_STAND_IN"
        return {"spread_model": "INVENTED"}, None, pd.Series([0, 0]), None

    source_identity = SimpleNamespace(source_contract_sha256="4" * 64,
                                     partition_source_contract_sha256="5" * 64)
    monkeypatch.setattr(config_binding, "_read_config", read_config)
    monkeypatch.setattr(config_binding, "_snapshot", lambda value: vars(value).copy())
    monkeypatch.setattr(execution.authority_spool, "_stream_file", stream_file)
    monkeypatch.setattr(execution, "_file_sha256", lambda path: "6" * 64)
    invented_contract = {"dependency_source_sha256": {"pm_production_search_planner": "6" * 64}}
    original_sha256 = execution._sha256
    monkeypatch.setattr(execution, "authoritative_search_execution_contract", lambda: invented_contract)
    monkeypatch.setattr(execution, "_sha256", lambda value:
        execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256 if value is invented_contract else original_sha256(value))
    monkeypatch.setattr(execution, "_declared_unit_binding", lambda *args: copy.deepcopy(binding))
    monkeypatch.setattr(execution, "_route_a_working_fitted", copy.deepcopy)
    monkeypatch.setattr(execution, "_route_a_owner_membership_snapshot", lambda **kwargs: copy.deepcopy(snapshot))
    monkeypatch.setattr(search_planner, "_route_a_full_panel_preparation_contract", lambda **kwargs: {"fixture": "INVENTED"})
    monkeypatch.setattr(execution, "_route_a_full_panel_specifications", lambda **kwargs:
                        specifications(kwargs["attempt_ordinal"]))
    monkeypatch.setattr(execution, "_route_a_rebind_full_panel_owner_membership", rebind)
    monkeypatch.setattr(execution, "_route_a_rebind_owner_membership", rebind)
    monkeypatch.setattr(execution, "_fold_partitions", partitions)
    monkeypatch.setattr(execution, "_source_identity", lambda seed: source_identity.source_contract_sha256)
    monkeypatch.setattr(producer, "_verified_declared_slot_source_session_current", lambda *args, **kwargs: None)
    monkeypatch.setattr(execution.historical_adapter, "_verify_declared_slot_capture_current", lambda capture: None)
    monkeypatch.setattr(execution.historical_adapter, "_verify_declared_slot_capture", lambda capture: None)
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: "2020-01-02T00:00:00+00:00"
                        if state["expired"] else "2020-01-01T12:00:00+00:00")
    monkeypatch.setattr(execution, "Backtester", InventedProbeBacktester)
    monkeypatch.setattr(execution.raw_winner, "build_trade_source_contract_from_backtester_result", lambda result: {})
    monkeypatch.setattr(execution.trade_power, "build_trade_sample_evidence", lambda *args, **kwargs:
                        SimpleNamespace(source_contract=source_identity))
    monkeypatch.setattr(execution.pm_core, "build_backtest_spread_mode", lambda value: "INVENTED")
    monkeypatch.setattr(execution, "_verify_partition_precommitment", lambda **kwargs: events.append(("provenance",)))
    monkeypatch.setattr(execution, "_runtime_strategy", lambda attempt: (object(), None))
    monkeypatch.setattr(execution, "_build_full_prefix_strategy_surface", lambda **kwargs: object())
    monkeypatch.setattr(execution, "_run_tier_partition", run_partition)
    monkeypatch.setattr(execution, "_route_a_pair_check", lambda **kwargs: {"status": "VERIFIED", "missing_tiers": []})
    monkeypatch.setattr(execution, "_route_a_assemble_full_panel_attempt", lambda **kwargs:
        ({"attempt_id": kwargs["raw_attempt_id"], "folds": [{"fixture": ordinal} for ordinal in range(4)]},
         [{"fixture_provenance": "INVENTED_NOT_KERNEL_EVIDENCE"} for _ in range(12)]))
    monkeypatch.setattr(execution, "_read_declared_unit_json", lambda *args: None)
    monkeypatch.setattr(search_planner, "describe_route_a_evaluation_design", lambda **kwargs: {
        "requests": [{"shared_evaluation_key": {"parameters": value["parameters"],
             "evaluation_binding": value["evaluation_binding"]}, "bookkeeping": value["bookkeeping"]}
            for value in kwargs["configurations"]]})

    stage = tmp_path / "invented-stage"
    stage.mkdir()
    info = stage.stat()
    reservation = SimpleNamespace(consume=lambda size: None, materialize=lambda size: None)
    custody = execution._StageCapacityCustody(tmp_path, (info.st_dev, info.st_ino), (0, 0), None,
        {"generation_context_sha256": context.context_sha256,
         "producer_request_sha256": request.receipt_sha256,
         "frozen_receipt_descriptor": descriptor, "original_stage_directory": stage.name})
    staged = []
    consumer = execution._FullPanelOwnerPreparation(capture=capture, handle=handle, binding=binding,
        stage_custody=custody, reservation=reservation, staged_rows=staged, source_observation_references={})
    legacy_observation = {}
    if not full_panel:
        specifications = [{key: value for key, value in row.items() if key != "request_ordinal"}
                          for row in (rows[0], rows[1], rows[0])]

        def consumer(*, evaluate_batch):
            legacy_observation["before"] = reads["config"]
            result = evaluate_batch(specifications=specifications)
            legacy_observation["after"] = reads["config"]
            return result

    def run():
        return execution._run_route_a_owned_unit_continuation(config=config, handle=handle,
            search_process=process, cell_plan=cell_plan, binding=binding, refreshed=fitted,
            attempts=attempts, request_body=body, declared_header=header, capture=capture,
            normalized_regime="INVENTED", route_a_consumer=consumer, fitted=fitted,
            raw_ids=raw_ids, plan_body=plan, timeframe="D1", fold_plan=(), symbol="INVENTED")

    return SimpleNamespace(run=run, events=events, reads=reads, staged=staged,
        original_frame=frame.copy(deep=True), fitted=fitted, config=config,
        legacy_observation=legacy_observation)


def test_route_a_full_panel_checks_whole_state_at_owned_unit_boundaries_not_nested_requests(tmp_path, monkeypatch):
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch)
    result = case.run()
    assert type(result) is execution._PreparedRouteAFullPanelStage and len(case.staged) == 1
    probes = [row for row in case.events if row[0] == "probe"]
    runs = [row for row in case.events if row[0] == "run"]
    assert len(probes) == len(runs) == 24
    # One initial admitted snapshot and one completion snapshot for the owned unit.
    assert case.reads == {"config": 2, "stream": 4}
    assert {row[1:] for row in probes + runs} == {(1, 2)}
    assert sum(row[0] == "owner" for row in case.events) > len(probes)
    assert sum(row[0] == "provenance" for row in case.events) == 48


@pytest.mark.parametrize("damage", ("frame_probe", "config_probe", "frame_run", "config_run"))
def test_route_a_full_panel_nested_request_keeps_actual_working_copy_mutation_refusal(tmp_path, monkeypatch, damage):
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch, damage=damage)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        case.run()
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_SHARED_CONTENT_CHANGED"
    assert sum(row[0] == "probe" for row in case.events) == 1
    assert sum(row[0] == "run" for row in case.events) == (1 if damage.endswith("run") else 0)
    pd.testing.assert_frame_equal(case.fitted._frame, case.original_frame)
    assert case.config.fixture_value == 1 and case.staged == []


@pytest.mark.parametrize("expiry, probes, runs", (("admission", 0, 0), ("after_probe", 1, 0), ("after_run", 1, 1)))
def test_route_a_full_panel_nested_request_keeps_original_window_expiry_points(tmp_path, monkeypatch, expiry, probes, runs):
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch, expiry=expiry)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        case.run()
    assert caught.value.code == "ROUTE_A_UNIT_BATCH_SOURCE_WINDOW_CLOSED"
    assert sum(row[0] == "probe" for row in case.events) == probes
    assert sum(row[0] == "run" for row in case.events) == runs
    assert case.staged == []


def test_route_a_legacy_nested_requests_keep_outputs_and_only_boundary_whole_state_checks(tmp_path, monkeypatch):
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch, full_panel=False)
    result = case.run()
    assert result["status"] == "EVALUATED_DIAGNOSTIC_ONLY", result
    assert len(result["requests"]) == 3 and result["requests"][2]["cache_hit"] is True
    assert case.legacy_observation == {"before": 1, "after": 1}
    assert case.reads == {"config": 2, "stream": 4}
    probes = [row for row in case.events if row[0] == "probe"]
    assert len(probes) == 2 and {row[1:] for row in probes} == {(1, 2)}
    assert case.staged == []



def _tiny_prepared_matched_source_case(monkeypatch):
    """INVENTED one-period fixed-kernel witness; no fits, model search or financial evidence."""
    result, facts, frame, backtester, fixture, base, stress, end = _exact_floor_fractional_invented_fixture(
        monkeypatch, False, 1)
    geometry = {"frame_start_utc": frame.index[0].isoformat(), "end_exclusive_utc": end.isoformat(),
        "frame_rows": len(frame), "frame_sha256": execution._frame_sha256(frame)}
    build_calls, swap_calls, frame_hash_calls = [], [], []
    original_build = execution.Backtester._build_half_spread_arrays
    original_swap = execution._verify_route_a_exact_swap_source
    original_frame_sha256 = execution._frame_sha256

    def build(self, *args, **kwargs):
        build_calls.append(kwargs["spread_tier"].tier)
        return original_build(self, *args, **kwargs)

    def swap(*args, **kwargs):
        swap_calls.append(True)
        return original_swap(*args, **kwargs)

    def frame_hash(frame):
        frame_hash_calls.append(True)
        return original_frame_sha256(frame)

    def forbidden_run(*args, **kwargs):
        raise AssertionError("cold source proof must not execute another kernel")

    monkeypatch.setattr(execution.Backtester, "_build_half_spread_arrays", build)
    monkeypatch.setattr(execution, "_verify_route_a_exact_swap_source", swap)
    monkeypatch.setattr(execution, "_frame_sha256", frame_hash)
    monkeypatch.setattr(execution.Backtester, "run", forbidden_run)
    arguments = {"source_frame": frame, "spec": fixture["spec"],
        "tiers": {"P75": base, "P95": stress}, "config": backtester.config, "timeframe": fixture["timeframe"]}
    return SimpleNamespace(facts=facts, geometry=geometry, arguments=arguments,
                           build_calls=build_calls, swap_calls=swap_calls, frame_hash_calls=frame_hash_calls, result=result)


def test_matched_source_preparation_reuses_only_identical_geometry_config_spec_and_tiers(monkeypatch):
    case = _tiny_prepared_matched_source_case(monkeypatch)
    cache = {}
    for facts in (case.facts, copy.deepcopy(case.facts)):
        assert execution._verify_route_a_matched_fact_source(facts, case.geometry,
            **case.arguments, _prepared_source_cache=cache)
    assert case.build_calls == ["P75", "P95"] and len(case.swap_calls) == 2
    assert len(case.frame_hash_calls) == 1
    # Each different bound source input must prepare its own pair, even when its
    # numerical values agree. Every witness still receives exact swap verification.
    variants = []
    for field in ("config", "spec"):
        variants.append((case.geometry, {**case.arguments, field: copy.deepcopy(case.arguments[field])}))
    variants.append((case.geometry, {**case.arguments, "tiers": {
        tier: execution.spread_profile.FrozenSpreadTier(value._json)
        for tier, value in case.arguments["tiers"].items()}}))
    variants.append(({**case.geometry, "receipt_sha256": "e" * 64}, case.arguments))
    for ordinal, (geometry, arguments) in enumerate(variants, 2):
        assert execution._verify_route_a_matched_fact_source(copy.deepcopy(case.facts), geometry,
            **arguments, _prepared_source_cache=cache)
        assert case.build_calls == ["P75", "P95"] * ordinal
        assert len(case.frame_hash_calls) == ordinal
    assert len(case.swap_calls) == 6


@pytest.mark.parametrize("corruption", ("instrument", "spread", "entry", "held", "swap-contract", "swap-cash", "initial-risk"))
def test_matched_source_cached_preparation_still_checks_each_witness(monkeypatch, corruption):
    case = _tiny_prepared_matched_source_case(monkeypatch)
    cache = {}
    assert execution._verify_route_a_matched_fact_source(case.facts, case.geometry,
        **case.arguments, _prepared_source_cache=cache)
    original = copy.deepcopy(case.facts)
    changed = copy.deepcopy(case.facts)
    if corruption == "instrument":
        changed["instrument_spec"]["symbol"] = "INVENTED_CROSSED_SPEC"
    elif corruption == "spread":
        changed["half_spread_arrays"]["P75"]["entry_half_spread"]["shape"][0] += 1
    elif corruption == "entry":
        changed["trades"][0]["base_fill_witness"]["entry_mid_open"] += 0.5
    elif corruption == "held":
        changed["trades"][0]["base_fill_witness"]["held_bar_inputs"][0]["ohlc"]["Low"] -= 0.5
    elif corruption == "swap-contract":
        changed["exact_swap_accrual_contract"]["fixture_splice"] = True
    elif corruption == "swap-cash":
        changed["trades"][0]["executed_price_swap_capture"]["swap_dollars_exact"] += 0.5
    else:
        changed["trades"][0]["actual_initial_risk_dollars"] += 0.5
    if corruption == "initial-risk":
        # Source origin and executed-risk math remain separate existing gates.
        assert execution._verify_route_a_matched_fact_source(changed, case.geometry,
            **case.arguments, _prepared_source_cache=cache)
        verified = execution._verify_route_a_matched_base_facts(changed)
        assert verified["floor_status"] == "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH"
        assert verified["authenticates_origin"] is False and verified["floor_reason_codes"]
    else:
        with pytest.raises((execution.AuthoritativeSearchExecutionError, ValueError)):
            execution._verify_route_a_matched_fact_source(changed, case.geometry,
                **case.arguments, _prepared_source_cache=cache)
    assert case.build_calls == ["P75", "P95"]
    assert case.facts == original


def test_fresh_matched_source_call_rechecks_changed_original_frame(monkeypatch):
    case = _tiny_prepared_matched_source_case(monkeypatch)
    assert execution._verify_route_a_matched_fact_source(case.facts, case.geometry,
        **case.arguments, _prepared_source_cache={})
    changed = case.arguments["source_frame"].copy(deep=True)
    changed.iloc[0, changed.columns.get_loc("Close")] += 0.5
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._verify_route_a_matched_fact_source(case.facts, case.geometry,
            **{**case.arguments, "source_frame": changed}, _prepared_source_cache={})
    assert caught.value.code == "MATCHED_FLOOR_PARTITION_SOURCE_CHANGED"


@pytest.fixture(scope="module")
def source303_roster_fixture():
    """INVENTED software-only metadata and frames; no process/window is issued."""
    with search_planner._process_verification_scope():
        body = search_planner.noise101_process_metadata()
        plan, artifacts, fitted, binding = _attempt_roster_fixture(
            boundary=True, attempt_count=303, process_body=body)
    assert len(artifacts) == 303
    return body, plan, artifacts, fitted, binding


@pytest.mark.parametrize("mode", ("bound303", "legacy300"))
def test_roster_extent_uses_authenticated_edition_count(monkeypatch, source303_roster_fixture, mode):
    body, original_plan, artifacts, fitted, binding = source303_roster_fixture
    body_hash = execution._sha256(body)
    plan_body = original_plan.to_dict()
    causal_hash = execution._sha256({"fixture": "INVENTED_CAUSAL_PRECOMMIT"})
    assert causal_hash != body_hash
    plan_body["anchor"] = {"process_precommit_receipt_sha256": causal_hash}
    process = SimpleNamespace(to_dict=lambda: copy.deepcopy(body), receipt_sha256=body_hash)
    monkeypatch.setattr(execution.historical, "historical_horizon_plan_binding", lambda _plan: binding)
    kwargs = dict(plan=_PlanBody(plan_body), attempt_artifacts=artifacts, fitted=fitted, regime="TREND")
    if mode != "legacy300":
        kwargs["search_process"] = process
    if mode == "bound303":
        _, verified = execution._verify_attempt_roster(**kwargs)
        assert len(verified) == 303
        assert sum(row.artifacts.winner_process["strategy_id"] == "NoiseBandBreakoutStrategy"
            for row in verified) == 3
        assert {row.plan_attempt["status"] for row in verified} == {"PROCESS_REJECTED"}
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._verify_attempt_roster(**kwargs)
        assert caught.value.code == "HORIZON_ATTEMPT_ROSTER_INVALID"


@pytest.mark.parametrize("timeframe,bar_minutes", (("H1", 60), ("D1", 1440)), ids=("H1", "D1"))
def test_frozen_complexity_uses_authenticated_timeframe(source303_roster_fixture, timeframe, bar_minutes):
    body, _, _, _, _ = source303_roster_fixture
    domains = search_planner._domain_by_name(body, timeframe=timeframe)
    source = domains["NoiseBandBreakoutStrategy"]
    artifact = {"winner_process": {"strategy_id": source["strategy_id"],
        "process_candidate_id": hashlib.sha1(f"INVENTED-{timeframe}".encode("ascii")).hexdigest()},
        "trial_definition": {"fixture": "INVENTED_COMPLEXITY_ONLY", "parameters": source["defaults"]},
        "refit_recipe": {"fixture": "INVENTED_COMPLEXITY_ONLY"}}
    rows = execution.historical_adapter._frozen_selection_complexity_from_process(
        body, [artifact], timeframe=timeframe)
    assert len(rows) == 1 and rows[0]["source_strategy_domain_sha256"] == source["domain_sha256"]
    assert rows[0]["parameter_domain"]["defaults"]["bar_minutes"] == bar_minutes


def test_frozen_complexity_refuses_unbound_timeframe(source303_roster_fixture):
    body, _, _, _, _ = source303_roster_fixture
    with pytest.raises(search_planner.ProductionSearchPlannerError, match="NOISE101_AUTHENTICATED_TIMEFRAME_REQUIRED"):
        execution.historical_adapter._frozen_selection_complexity_from_process(body, [])


@pytest.mark.parametrize("version", (17, 18), ids=("legacy-v17", "noise-v18"))
def test_matched_capture_routes_only_explicit_process_generation(monkeypatch, source303_roster_fixture, version):
    adapter = execution.historical_adapter
    body = (source303_roster_fixture[0] if version == 18 else {
        "schema_version": search_planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA,
        "execution_edition": search_planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION})
    sentinel = object()
    seen = []
    def open_capture(context, **kwargs):
        seen.append(kwargs)
        return sentinel
    monkeypatch.setattr(adapter, "open_declared_slot_capture", open_capture)
    assert adapter.open_matched_floor_declared_slot_capture(None, request=None,
        process=body, plan=None, source_session=None) is sentinel
    assert seen[0]["_winner_generation"] is getattr(winner, f"V{version}_GENERATION")


@pytest.mark.parametrize("scope,count", (("declared", 303), ("declared", 304), ("canary", 303)),
    ids=("declared303", "declared304-refused", "canary303-refused"))
def test_failure_diagnostic_preserves_edition_extent(source303_roster_fixture, scope, count):
    adapter = execution.historical_adapter
    if scope == "canary":
        with pytest.raises(adapter.HistoricalProductionAdapterError, match="CANARY_FAILURE_ATTEMPT_COUNT_INVALID"):
            adapter._bounded_canary_failure_diagnostic(exception_type=None, failure_details=(),
                attempt_ordinal=None, fold_ordinal=None, attempted_trials_exact=count)
        return
    published = []
    def put_json(*args):
        published.append(args)
        return {"fixture": "INVENTED_DESCRIPTOR"}
    capture = SimpleNamespace(process=source303_roster_fixture[0],
        request=SimpleNamespace(receipt_sha256="b" * 64), sink=SimpleNamespace(put_json=put_json),
        request_body={"artifact_namespace": "INVENTED-DIAGNOSTIC", "cell_id": "INVENTED-CELL",
            "slot_id": "INVENTED-SLOT", "symbol": "EURUSD", "timeframe": "D1", "regime": "TREND"})
    if count == 304:
        with pytest.raises(adapter.HistoricalProductionAdapterError, match="CANARY_FAILURE_ATTEMPT_COUNT_INVALID"):
            adapter._declared_slot_failure_entry(capture, failure_code="INVENTED_SOFTWARE_FAILURE",
                attempted_trials_exact=count)
        assert not published
    else:
        adapter._declared_slot_failure_entry(capture, failure_code="INVENTED_SOFTWARE_FAILURE",
            attempted_trials_exact=count)
        assert published[0][2]["attempted_trials_exact"] == 303
        assert published[0][2]["downstream_authority"] is False


@pytest.mark.parametrize("crossed_schema", (False, True), ids=("raw-v8", "raw-v7-refused"))
def test_worker_production_receipt_binds_winner_generation(monkeypatch, crossed_schema):
    """INVENTED receipt transport only; real schema and cross-binding checks."""
    adapter = execution.historical_adapter
    raw = adapter.raw_winner
    cross = b'{"fixture":"INVENTED_CROSS"}'
    cross_hash = hashlib.sha256(cross).hexdigest()
    def descriptor(digest="a" * 64):
        return {"schema_version": adapter.authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
            "relative_path": f"objects/sha256/{digest[:2]}/{digest}", "sha256": digest,
            "size_bytes": len(cross), "format": raw.ASCII_JSON_FORMAT}
    request_descriptor = descriptor()
    decision_descriptor = descriptor("b" * 64)
    cross_descriptor = descriptor(cross_hash)
    request = SimpleNamespace(request_sha256="c" * 64,
        precommitment=SimpleNamespace(to_dict=lambda: {"contract_version": winner.V18_GENERATION.precommitment_version}))
    decision = SimpleNamespace(receipt_sha256="d" * 64)
    context = SimpleNamespace(context_sha256="e" * 64, limits=SimpleNamespace(max_json_bytes=4096))
    body = {"schema_version": raw.V17_PRODUCTION_SCHEMA if crossed_schema else raw.V18_PRODUCTION_SCHEMA,
        "generation_context_sha256": context.context_sha256, "symbol": "EURUSD", "timeframe": "H1",
        "regime": "TREND", "request_sha256": request.request_sha256,
        "decision_receipt_sha256": decision.receipt_sha256, "provenance_cross_binding_sha256": cross_hash,
        "artifact_roster": {"winner-request": request_descriptor, "winner-decision": decision_descriptor}}
    encoded = execution._canonical_bytes(body)
    monkeypatch.setattr(adapter, "_reopen_worker_ascii_json_descriptor", lambda *a, **k: (body, encoded))
    kwargs = dict(context=context, descriptor=descriptor(), cross_descriptor=cross_descriptor,
        cross_bytes=cross, request_descriptor=request_descriptor, decision_descriptor=decision_descriptor,
        request=request, decision=decision, symbol="EURUSD", timeframe="H1", regime="TREND")
    if crossed_schema:
        with pytest.raises(adapter.HistoricalProductionAdapterError, match="WORKER_PRODUCTION_RECEIPT_SPLICE"):
            adapter._verify_worker_production_receipt(**kwargs)
    else:
        assert adapter._verify_worker_production_receipt(**kwargs) == encoded


def test_retained_matched_source_refuses_crossed_generation_before_reopen(monkeypatch, source303_roster_fixture):
    body = source303_roster_fixture[0]
    monkeypatch.setattr(execution.historical, "verify_declared_slot_producer_request", lambda *a, **k: {})
    monkeypatch.setattr(search_planner, "verify_frozen_production_search_process", lambda *a, **k: True)
    class NoSourceAccess:
        def verify_current(self, _context):
            pytest.fail("crossed generation reached source/config reopening")
    frozen = SimpleNamespace(selection_generation=winner.V17_GENERATION,
        bound_fitted_state_entry={"fixture": "INVENTED_BOUND_REFERENCE"})
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="MATCHED_FLOOR_FROZEN_SOURCE_EDITION_REQUIRED"):
        execution._reopen_route_a_matched_source_material(None, request=None, process=body,
            frozen=frozen, frozen_pipeline_config=NoSourceAccess())
