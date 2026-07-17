from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_winner_selection as winner


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
    row = execution._write_staged_attempt(
        stage=stage,
        ordinal=0,
        value=body,
        remaining_cell_bytes=1024 * 1024,
    )
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
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="ATTEMPT_EVIDENCE_BYTE_BOUND_EXCEEDED",
    ):
        execution._write_staged_attempt(
            stage=stage,
            ordinal=3,
            value={"attempt_id": "x", "folds": [{"payload": "too-large"}]},
            remaining_cell_bytes=8,
        )
    assert list(stage.iterdir()) == []
    execution._cleanup_stage(stage, [])


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


def test_winner_resource_boundaries_are_exactly_mirrored() -> None:
    assert execution.MAX_ATTEMPTS == winner.MAX_ATTEMPTS == 128
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
    frozen_bytes = b"{}"
    frozen = SimpleNamespace(
        precommitment={"latest_evidence_boundary_utc": "2020-06-01T00:00:00Z"},
        frozen_receipt_bytes=frozen_bytes,
        frozen_receipt_entry={
            "descriptor": {"sha256": execution.hashlib.sha256(frozen_bytes).hexdigest()}
        },
    )
    def freeze(*_args, **kwargs):
        assert kwargs["precommitted_evidence_roles"] == ("EVALUATION_REQUIRED",)
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
