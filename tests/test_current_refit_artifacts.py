from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

import pm_current_refit_artifacts as refit_artifacts
import pm_pipeline as pipeline
import pm_authority_spool as spool
from pm_current_refit_artifacts import (
    CurrentRefitArtifactError,
    build_current_refit_search_artifacts_v2,
    canonical_artifact_bytes,
    canonical_artifact_sha256,
    spool_current_refit_search_attempt_v2,
    verify_current_refit_search_artifacts_bytes_v2,
)
from pm_strategies import StrategyRegistry
from pm_current_refit_parameters import (
    RETAINED_STRATEGY_SOURCE_V1_SHA256,
    load_retained_strategy_domain_v1,
    decode_retained_parameter_metadata,
)


STRATEGY_ID = "EMACrossoverStrategy"


def _build(**overrides):
    strategy = StrategyRegistry.get(STRATEGY_ID)
    values = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "strategy_id": STRATEGY_ID,
        "parameters": strategy.get_params(),
        "lookback_kind": "NATIVE_OBSERVATIONS",
        "lookback_value": 1_000,
        "fit_start_utc": "2021-01-01T00:00:00Z",
        "fit_end_utc": "2026-01-02T00:00:00Z",
        "latest_closed_bar_utc": "2026-01-01T23:00:00Z",
        "source_snapshot_sha256": "a" * 64,
        "source_bar_count": 1_000,
        "seed": 20260714,
    }
    if (
        "source_bar_count" in overrides
        and "lookback_value" not in overrides
        and overrides.get("lookback_kind", values["lookback_kind"])
        == "NATIVE_OBSERVATIONS"
    ):
        values["lookback_value"] = overrides["source_bar_count"]
    values.update(overrides)
    return build_current_refit_search_artifacts_v2(**values)


def _request(artifacts):
    horizon = artifacts.horizon_process
    return {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "fit_start_utc": horizon["fit_start_utc"],
        "fit_end_utc": horizon["fit_end_utc"],
        "latest_closed_bar_utc": horizon["latest_closed_bar_utc"],
    }


def _artifact_bytes(artifacts):
    return {
        "winner_process_bytes": canonical_artifact_bytes(artifacts.winner_process),
        "horizon_process_bytes": canonical_artifact_bytes(artifacts.horizon_process),
        "trial_definition_bytes": canonical_artifact_bytes(artifacts.trial_definition),
        "refit_recipe_bytes": canonical_artifact_bytes(artifacts.refit_recipe),
    }


def _verify(artifacts, **overrides):
    horizon = artifacts.horizon_process
    values = {
        **_artifact_bytes(artifacts),
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "strategy_id": artifacts.winner_process["strategy_id"],
        "lookback_kind": horizon["lookback_kind"],
        "lookback_value": horizon["lookback_value"],
        "fit_start_utc": horizon["fit_start_utc"],
        "fit_end_utc": horizon["fit_end_utc"],
        "latest_closed_bar_utc": horizon["latest_closed_bar_utc"],
        "source_snapshot_sha256": "a" * 64,
        "source_bar_count": 1_000,
        "seed": 20260714,
    }
    values.update(overrides)
    return verify_current_refit_search_artifacts_bytes_v2(**values)


def _retained_macd_metadata_chain():
    # Metadata-only reconstruction of the pinned original edition; no strategy evaluation.
    table = load_retained_strategy_domain_v1()
    record = next(row for row in table["strategies"] if row["strategy_id"] == "MACDHistogramMomentumStrategy")
    parameters = decode_retained_parameter_metadata(record["typed_metadata"]["defaults"])
    parameters["macd_fast"] = 10  # Legal v1 marginal, deliberately removed from the current domain.
    return refit_artifacts._retained_search_artifacts_v1(
        strategy_id=record["strategy_id"], trial={"parameters": parameters},
        symbol="EURUSD", timeframe="H1", regime="TREND", lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=1000, fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-02T00:00:00Z", latest_closed_bar_utc="2026-01-01T23:00:00Z",
        source_snapshot_sha256="a" * 64, source_bar_count=1000, seed=20260714)


def test_retained_v1_chain_verifies_removed_parameter_without_current_reissue():
    artifacts = _retained_macd_metadata_chain()
    assert artifacts.winner_process["strategy_code_sha256"] == RETAINED_STRATEGY_SOURCE_V1_SHA256
    assert _verify(artifacts) == artifacts
    with pytest.raises(CurrentRefitArtifactError, match="PARAMETER_VALUE_OUTSIDE_PROCESS"):
        _build(strategy_id="MACDHistogramMomentumStrategy", parameters=artifacts.trial_definition["parameters"])
    process, process_sha = pipeline._current_refit_validate_process(
        artifacts.winner_process, strategy_id="MACDHistogramMomentumStrategy")
    trial, parameters = pipeline._current_refit_validate_trial(
        artifacts.trial_definition, strategy_id="MACDHistogramMomentumStrategy", process_sha256=process_sha,
        horizon_sha256=artifacts.horizon_process_sha256,
        process_source_sha256=process["strategy_code_sha256"])
    assert parameters["macd_fast"] == 10
    recipe, _ = pipeline._current_refit_validate_recipe(
        artifacts.refit_recipe, strategy_id="MACDHistogramMomentumStrategy", process_sha256=process_sha,
        horizon_sha256=artifacts.horizon_process_sha256, trial_sha256=canonical_artifact_sha256(trial),
        source_bar_count=1000, process_source_sha256=process["strategy_code_sha256"])
    assert recipe == artifacts.refit_recipe


@pytest.mark.parametrize("mutation", ["unknown_source", "entry_rule", "old_recipe", "caller_strategy"])
def test_retained_v1_chain_rejects_source_rule_recipe_and_caller_splices(mutation):
    artifacts = _retained_macd_metadata_chain()
    changes = {}
    if mutation in ("unknown_source", "entry_rule"):
        process = copy.deepcopy(artifacts.winner_process)
        process["strategy_code_sha256" if mutation == "unknown_source" else "entry_rule_identity_sha256"] = "f" * 64
        changes["winner_process_bytes"] = canonical_artifact_bytes(process)
    elif mutation == "old_recipe":
        recipe = copy.deepcopy(artifacts.refit_recipe)
        recipe["version_pins"]["pm_strategies_sha256"] = "f" * 64
        changes["refit_recipe_bytes"] = canonical_artifact_bytes(recipe)
    else:
        changes["strategy_id"] = STRATEGY_ID
    with pytest.raises(CurrentRefitArtifactError):
        _verify(artifacts, **changes)


def test_retained_v1_refuses_changed_metadata_callable(monkeypatch):
    strategy_class = type(StrategyRegistry.get("MACDHistogramMomentumStrategy"))
    original = strategy_class.get_default_params
    def changed_defaults(self):
        return original(self)
    monkeypatch.setattr(strategy_class, "get_default_params", changed_defaults)
    with pytest.raises(CurrentRefitArtifactError, match="RETAINED_STRATEGY_METADATA_CHANGED"):
        refit_artifacts.retained_strategy_metadata_v1("MACDHistogramMomentumStrategy")


def test_retained_v1_refuses_unapproved_successor_source(monkeypatch):
    strategy_class = type(StrategyRegistry.get("MACDHistogramMomentumStrategy"))
    original = refit_artifacts._source_file_sha256
    monkeypatch.setattr(refit_artifacts, "_source_file_sha256",
        lambda value: "f" * 64 if value is strategy_class else original(value))
    with pytest.raises(CurrentRefitArtifactError, match="RETAINED_STRATEGY_SUCCESSOR_CHANGED"):
        refit_artifacts.retained_strategy_metadata_v1("MACDHistogramMomentumStrategy")


def test_v2_artifacts_are_byte_identical_to_raw_executor_contract() -> None:
    artifacts = _build()

    process, process_sha = pipeline._current_refit_validate_process(
        artifacts.winner_process,
        strategy_id=STRATEGY_ID,
    )
    horizon, horizon_sha = pipeline._current_refit_validate_horizon(
        artifacts.horizon_process,
        request=_request(artifacts),
        source_sha256="a" * 64,
        source_bar_count=1_000,
    )
    trial, parameters = pipeline._current_refit_validate_trial(
        artifacts.trial_definition,
        strategy_id=STRATEGY_ID,
        process_sha256=process_sha,
        horizon_sha256=horizon_sha,
    )
    recipe, recipe_sha = pipeline._current_refit_validate_recipe(
        artifacts.refit_recipe,
        strategy_id=STRATEGY_ID,
        process_sha256=process_sha,
        horizon_sha256=horizon_sha,
        trial_sha256=artifacts.trial_definition_sha256,
        source_bar_count=1_000,
    )

    assert process == artifacts.winner_process
    assert horizon == artifacts.horizon_process
    assert trial == artifacts.trial_definition
    assert recipe == artifacts.refit_recipe
    assert parameters == StrategyRegistry.get(STRATEGY_ID).get_params()
    assert process_sha == artifacts.winner_process_sha256
    assert horizon_sha == artifacts.horizon_process_sha256
    assert recipe_sha == artifacts.refit_recipe_sha256
    assert artifacts.trial_definition_sha256 == canonical_artifact_sha256(trial)


def test_v2_fields_and_candidate_identity_match_pipeline_exactly() -> None:
    artifacts = _build()

    assert set(artifacts.winner_process) == pipeline._CURRENT_REFIT_PROCESS_FIELDS
    assert set(artifacts.horizon_process) == pipeline._CURRENT_REFIT_HORIZON_FIELDS
    assert set(artifacts.trial_definition) == pipeline._CURRENT_REFIT_TRIAL_FIELDS
    assert set(artifacts.refit_recipe) == pipeline._CURRENT_REFIT_RECIPE_FIELDS
    expected = pipeline.RegimeOptimizer._exact_cell_candidate_id(
        "EURUSD",
        "H1",
        "TREND",
        {
            "strategy_name": STRATEGY_ID,
            "params": StrategyRegistry.get(STRATEGY_ID).get_params(),
        },
        horizon_process_sha256=artifacts.horizon_process_sha256,
    )
    assert artifacts.process_candidate_id == expected
    assert artifacts.winner_process["process_candidate_id"] == expected


def test_v2_candidate_identity_distinguishes_horizon_hypotheses() -> None:
    first = _build(source_bar_count=500)
    second = _build(source_bar_count=501)

    assert first.horizon_process_sha256 != second.horizon_process_sha256
    assert first.trial_definition["parameters"] == second.trial_definition["parameters"]
    assert first.process_candidate_id != second.process_candidate_id

    expected = pipeline.RegimeOptimizer._exact_cell_candidate_id(
        "EURUSD",
        "H1",
        "TREND",
        {
            "strategy_name": STRATEGY_ID,
            "params": StrategyRegistry.get(STRATEGY_ID).get_params(),
        },
        horizon_process_sha256=second.horizon_process_sha256,
    )
    assert second.process_candidate_id == expected


@pytest.mark.parametrize(
    ("lookback_kind", "lookback_value"),
    (("ACTIVE_SESSIONS", 400), ("CALENDAR_INTERVAL", 730)),
)
def test_v2_nonnative_horizon_keeps_policy_value_separate_from_realized_rows(
    lookback_kind, lookback_value
) -> None:
    artifacts = _build(
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        source_bar_count=975,
    )

    assert artifacts.horizon_process["lookback_kind"] == lookback_kind
    assert artifacts.horizon_process["lookback_value"] == lookback_value
    assert artifacts.horizon_process["source_bar_count"] == 975
    assert _verify(artifacts, source_bar_count=975) == artifacts
    validated, digest = pipeline._current_refit_validate_horizon(
        artifacts.horizon_process,
        request=_request(artifacts),
        source_sha256="a" * 64,
        source_bar_count=975,
    )
    assert validated == artifacts.horizon_process
    assert digest == artifacts.horizon_process_sha256


def test_v2_same_nonnative_policy_with_different_realized_rows_changes_identity() -> None:
    first = _build(
        lookback_kind="ACTIVE_SESSIONS",
        lookback_value=400,
        source_bar_count=975,
    )
    second = _build(
        lookback_kind="ACTIVE_SESSIONS",
        lookback_value=400,
        source_bar_count=976,
    )

    assert first.horizon_process_sha256 != second.horizon_process_sha256
    assert first.process_candidate_id != second.process_candidate_id


def test_v2_native_count_mismatch_and_nonnative_count_tamper_fail_closed() -> None:
    with pytest.raises(
        CurrentRefitArtifactError, match="NATIVE_LOOKBACK_SOURCE_COUNT_MISMATCH"
    ):
        _build(
            lookback_kind="NATIVE_OBSERVATIONS",
            lookback_value=1_000,
            source_bar_count=999,
        )

    artifacts = _build(
        lookback_kind="CALENDAR_INTERVAL",
        lookback_value=730,
        source_bar_count=975,
    )
    hostile = copy.deepcopy(artifacts.horizon_process)
    hostile["source_bar_count"] = 974
    with pytest.raises(CurrentRefitArtifactError, match="HORIZON_PROCESS_SEMANTICS"):
        _verify(
            artifacts,
            horizon_process_bytes=canonical_artifact_bytes(hostile),
            source_bar_count=975,
        )
    with pytest.raises(ValueError, match="CURRENT_REFIT_HORIZON_INVALID"):
        pipeline._current_refit_validate_horizon(
            hostile,
            request=_request(artifacts),
            source_sha256="a" * 64,
            source_bar_count=975,
        )


def test_v2_normalizes_utc_but_preserves_exact_hash_chain() -> None:
    artifacts = _build()

    assert artifacts.horizon_process["fit_start_utc"].endswith("+00:00")
    assert artifacts.horizon_process["fit_end_utc"].endswith("+00:00")
    assert (
        artifacts.trial_definition["process_sha256"] == artifacts.winner_process_sha256
    )
    assert (
        artifacts.trial_definition["horizon_process_sha256"]
        == artifacts.horizon_process_sha256
    )
    assert (
        artifacts.refit_recipe["winner_trial_definition_sha256"]
        == artifacts.trial_definition_sha256
    )


@pytest.mark.parametrize(
    ("artifact_name", "field", "replacement", "validator"),
    [
        (
            "winner_process",
            "strategy_code_sha256",
            "b" * 64,
            "process",
        ),
        (
            "trial_definition",
            "parameter_count",
            999,
            "trial",
        ),
        (
            "refit_recipe",
            "attempts",
            2,
            "recipe",
        ),
    ],
)
def test_executor_rejects_self_consistent_tampered_artifacts(
    artifact_name: str,
    field: str,
    replacement,
    validator: str,
) -> None:
    artifacts = _build()
    body = copy.deepcopy(getattr(artifacts, artifact_name))
    body[field] = replacement
    assert canonical_artifact_sha256(body) != getattr(
        artifacts, f"{artifact_name}_sha256"
    )

    with pytest.raises(ValueError):
        if validator == "process":
            pipeline._current_refit_validate_process(
                body,
                strategy_id=STRATEGY_ID,
            )
        elif validator == "trial":
            pipeline._current_refit_validate_trial(
                body,
                strategy_id=STRATEGY_ID,
                process_sha256=artifacts.winner_process_sha256,
                horizon_sha256=artifacts.horizon_process_sha256,
            )
        else:
            pipeline._current_refit_validate_recipe(
                body,
                strategy_id=STRATEGY_ID,
                process_sha256=artifacts.winner_process_sha256,
                horizon_sha256=artifacts.horizon_process_sha256,
                trial_sha256=artifacts.trial_definition_sha256,
                source_bar_count=1_000,
            )


def test_partial_parameters_cannot_change_candidate_identity() -> None:
    with pytest.raises(
        CurrentRefitArtifactError,
        match="PARAMETERS_NOT_FULL_CANONICAL_SET",
    ):
        _build(parameters={"fast_period": 10})


def test_parameter_value_outside_frozen_process_is_rejected() -> None:
    parameters = StrategyRegistry.get(STRATEGY_ID).get_params()
    parameters["fast_period"] = 11
    with pytest.raises(
        CurrentRefitArtifactError,
        match="PARAMETER_VALUE_OUTSIDE_PROCESS",
    ):
        _build(parameters=parameters)


def test_non_grid_parameter_must_equal_exact_fixed_default() -> None:
    parameters = StrategyRegistry.get(STRATEGY_ID).get_params()
    assert (
        "structural_sl_lookback"
        not in StrategyRegistry.get(STRATEGY_ID).get_param_grid()
    )
    parameters["structural_sl_lookback"] = 27
    with pytest.raises(
        CurrentRefitArtifactError,
        match="PARAMETER_FIXED_DEFAULT_MISMATCH",
    ):
        _build(parameters=parameters)


def test_tuple_and_list_fixed_parameters_round_trip_but_cannot_be_relabelled() -> None:
    strategy_id = "RegimeTransitionBreakoutStrategy"
    defaults = StrategyRegistry.get(strategy_id).get_default_params()
    artifacts = _build(strategy_id=strategy_id, parameters=defaults)

    assert artifacts.trial_definition["parameters"]["source_regimes"] == [
        "CHOP",
        "RANGE",
    ]
    assert _verify(artifacts) == artifacts

    changed_grid_choice = copy.deepcopy(defaults)
    changed_grid_choice["source_regimes"] = ["CHOP"]
    changed_artifacts = _build(
        strategy_id=strategy_id,
        parameters=changed_grid_choice,
    )
    assert changed_artifacts.trial_definition["parameters"]["source_regimes"] == [
        "CHOP"
    ]
    assert _verify(changed_artifacts) == changed_artifacts

    changed_fixed_default = copy.deepcopy(defaults)
    changed_fixed_default["structural_sl_lookback"] = 27
    with pytest.raises(
        CurrentRefitArtifactError,
        match="PARAMETER_FIXED_DEFAULT_MISMATCH",
    ):
        _build(strategy_id=strategy_id, parameters=changed_fixed_default)


def test_ascii_law_proves_true_false_canonicalization_parity() -> None:
    artifacts = _build()
    for body in (
        artifacts.winner_process,
        artifacts.horizon_process,
        artifacts.trial_definition,
        artifacts.refit_recipe,
    ):
        expected_false = json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        expected_true = json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        assert canonical_artifact_bytes(body) == expected_false == expected_true
        assert expected_false.isascii()


def test_non_ascii_identity_and_artifact_body_fail_closed() -> None:
    with pytest.raises(CurrentRefitArtifactError, match="SYMBOL_INVALID"):
        _build(symbol="E\u00dcRUSD")
    with pytest.raises(CurrentRefitArtifactError, match="ARTIFACT_NON_ASCII"):
        canonical_artifact_bytes({"label": "caf\u00e9"})


def test_standalone_raw_bytes_verifier_reconstructs_exact_chain() -> None:
    artifacts = _build()

    assert _verify(artifacts) == artifacts


@pytest.mark.parametrize(
    ("artifact_name", "error"),
    [
        ("winner_process", "WINNER_PROCESS_FIELDS_INVALID"),
        ("horizon_process", "HORIZON_PROCESS_FIELDS_INVALID"),
        ("trial_definition", "TRIAL_DEFINITION_FIELDS_INVALID"),
        ("refit_recipe", "REFIT_RECIPE_FIELDS_INVALID"),
    ],
)
def test_standalone_verifier_rejects_nonexact_keysets(
    artifact_name: str,
    error: str,
) -> None:
    artifacts = _build()
    body = copy.deepcopy(getattr(artifacts, artifact_name))
    body["unexpected"] = "field"

    with pytest.raises(CurrentRefitArtifactError, match=error):
        _verify(
            artifacts,
            **{f"{artifact_name}_bytes": canonical_artifact_bytes(body)},
        )


def test_standalone_verifier_rejects_noncanonical_raw_bytes() -> None:
    artifacts = _build()
    raw = b" " + canonical_artifact_bytes(artifacts.winner_process)

    with pytest.raises(
        CurrentRefitArtifactError,
        match="ARTIFACT_BYTES_NONCANONICAL",
    ):
        _verify(artifacts, winner_process_bytes=raw)


def test_standalone_verifier_rejects_escaped_non_ascii_raw_bytes() -> None:
    artifacts = _build()
    body = copy.deepcopy(artifacts.winner_process)
    body["strategy_module"] = "pm_strat\u00e9gies"
    raw = json.dumps(
        body,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")

    with pytest.raises(CurrentRefitArtifactError, match="ARTIFACT_NON_ASCII"):
        _verify(artifacts, winner_process_bytes=raw)


@pytest.mark.parametrize(
    ("artifact_name", "field", "replacement", "error"),
    [
        (
            "winner_process",
            "process_candidate_id",
            "0" * 40,
            "WINNER_PROCESS_SEMANTICS_INVALID",
        ),
        (
            "horizon_process",
            "source_snapshot_sha256",
            "b" * 64,
            "HORIZON_PROCESS_SEMANTICS_INVALID",
        ),
        (
            "trial_definition",
            "process_sha256",
            "b" * 64,
            "TRIAL_DEFINITION_SEMANTICS_INVALID",
        ),
        (
            "refit_recipe",
            "winner_trial_definition_sha256",
            "b" * 64,
            "REFIT_RECIPE_SEMANTICS_INVALID",
        ),
    ],
)
def test_standalone_verifier_rejects_identity_and_sha_relabelling(
    artifact_name: str,
    field: str,
    replacement: str,
    error: str,
) -> None:
    artifacts = _build()
    body = copy.deepcopy(getattr(artifacts, artifact_name))
    body[field] = replacement

    with pytest.raises(CurrentRefitArtifactError, match=error):
        _verify(
            artifacts,
            **{f"{artifact_name}_bytes": canonical_artifact_bytes(body)},
        )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        (
            {
                "latest_closed_bar_utc": "2020-12-31T23:00:00Z",
            },
            "FIT_INTERVAL_INVALID",
        ),
        ({"source_bar_count": 2}, "REFIT_SOURCE_TOO_SHORT"),
        ({"source_snapshot_sha256": "not-a-hash"}, "SOURCE_SNAPSHOT_SHA256_INVALID"),
        ({"seed": True}, "REFIT_SEED_INVALID"),
    ],
)
def test_invalid_source_or_recipe_inputs_fail_closed(overrides, error) -> None:
    with pytest.raises(CurrentRefitArtifactError, match=error):
        _build(**overrides)


def _spool_attempt(tmp_path: Path, *, symbol: str = "EURUSD"):
    context = spool.freeze_generation_context(
        tmp_path / "authority-spool",
        generation_id="generation-001",
        native_universe_sha256="b" * 64,
        config_sha256="c" * 64,
    )
    sink = spool.open_symbol_sink(context, symbol)
    strategy = StrategyRegistry.get(STRATEGY_ID)
    written = spool_current_refit_search_attempt_v2(
        sink,
        symbol=symbol,
        timeframe="H1",
        regime="TREND",
        strategy_id=STRATEGY_ID,
        parameters=strategy.get_params(),
        lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=1_000,
        fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-02T00:00:00Z",
        latest_closed_bar_utc="2026-01-01T23:00:00Z",
        source_snapshot_sha256="a" * 64,
        source_bar_count=1_000,
        seed=20260714,
    )
    return context, sink, written


def test_search_attempt_spools_all_executable_artifacts_and_verifies(
    tmp_path: Path,
) -> None:
    context, sink, written = _spool_attempt(tmp_path)
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(
        context.root,
        receipt,
        expected_generation_id="generation-001",
        expected_symbol="EURUSD",
        expected_native_universe_sha256="b" * 64,
        expected_config_sha256="c" * 64,
    )

    assert manifest["artifact_count"] == 4
    expected_hashes = {
        "winner_process": written.artifacts.winner_process_sha256,
        "horizon_process": written.artifacts.horizon_process_sha256,
        "trial_definition": written.artifacts.trial_definition_sha256,
        "refit_recipe": written.artifacts.refit_recipe_sha256,
    }
    assert {
        name: entry["descriptor"]["sha256"] for name, entry in written.entries.items()
    } == expected_hashes
    assert all(
        written.artifacts.process_candidate_id in entry["artifact_id"]
        for entry in manifest["artifacts"]
    )


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_search_attempt_preserves_exact_mt5_symbol(
    tmp_path: Path,
    symbol: str,
) -> None:
    context, sink, written = _spool_attempt(tmp_path, symbol=symbol)
    receipt = sink.seal()

    spool.verify_symbol_receipt(
        context.root,
        receipt,
        expected_generation_id="generation-001",
        expected_symbol=symbol,
        expected_native_universe_sha256="b" * 64,
        expected_config_sha256="c" * 64,
    )
    assert written.artifacts.horizon_process["symbol"] == symbol


@pytest.mark.parametrize("wrong_case", ("PLATINUM", "platinum"))
def test_search_attempt_rejects_wrong_case_mt5_symbol(
    tmp_path: Path,
    wrong_case: str,
) -> None:
    context = spool.freeze_generation_context(
        tmp_path / "authority-spool",
        generation_id="generation-001",
        native_universe_sha256="b" * 64,
        config_sha256="c" * 64,
    )
    sink = spool.open_symbol_sink(context, "Platinum")
    strategy = StrategyRegistry.get(STRATEGY_ID)

    with pytest.raises(
        CurrentRefitArtifactError,
        match="AUTHORITY_SINK_SYMBOL_MISMATCH",
    ):
        spool_current_refit_search_attempt_v2(
            sink,
            symbol=wrong_case,
            timeframe="H1",
            regime="TREND",
            strategy_id=STRATEGY_ID,
            parameters=strategy.get_params(),
            lookback_kind="NATIVE_OBSERVATIONS",
            lookback_value=1_000,
            fit_start_utc="2021-01-01T00:00:00Z",
            fit_end_utc="2026-01-02T00:00:00Z",
            latest_closed_bar_utc="2026-01-01T23:00:00Z",
            source_snapshot_sha256="a" * 64,
            source_bar_count=1_000,
            seed=20260714,
        )


def test_search_attempt_spool_is_byte_idempotent(tmp_path: Path) -> None:
    context, sink, written = _spool_attempt(tmp_path)
    first_receipt = sink.seal()
    reopened = spool.open_symbol_sink(context, "EURUSD")
    strategy = StrategyRegistry.get(STRATEGY_ID)
    replay = spool_current_refit_search_attempt_v2(
        reopened,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_id=STRATEGY_ID,
        parameters=strategy.get_params(),
        lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=1_000,
        fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-02T00:00:00Z",
        latest_closed_bar_utc="2026-01-01T23:00:00Z",
        source_snapshot_sha256="a" * 64,
        source_bar_count=1_000,
        seed=20260714,
    )
    assert replay == written
    assert reopened.seal() == first_receipt


def test_search_attempt_spool_rejects_symbol_splice(tmp_path: Path) -> None:
    _, sink, _ = _spool_attempt(tmp_path)
    strategy = StrategyRegistry.get(STRATEGY_ID)
    with pytest.raises(
        CurrentRefitArtifactError,
        match="AUTHORITY_SINK_SYMBOL_MISMATCH",
    ):
        spool_current_refit_search_attempt_v2(
            sink,
            symbol="GBPUSD",
            timeframe="H1",
            regime="TREND",
            strategy_id=STRATEGY_ID,
            parameters=strategy.get_params(),
            lookback_kind="NATIVE_OBSERVATIONS",
            lookback_value=1_000,
            fit_start_utc="2021-01-01T00:00:00Z",
            fit_end_utc="2026-01-02T00:00:00Z",
            latest_closed_bar_utc="2026-01-01T23:00:00Z",
            source_snapshot_sha256="a" * 64,
            source_bar_count=1_000,
            seed=20260714,
        )
