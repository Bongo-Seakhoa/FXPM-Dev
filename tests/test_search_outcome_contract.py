import json

from pm_search_outcomes import (
    InvalidStrategyParameterError,
    SearchInfrastructureError,
    SearchOutcomeKind,
    SearchStage,
    TrialOutcome,
    aggregate_family_cell_outcomes,
    classify_search_exception,
)


CONTEXT = {
    "symbol": "EURUSD",
    "timeframe": "H1",
    "regime": "TREND",
    "strategy_family": "ExampleStrategy",
}


def _outcome(kind, code):
    return TrialOutcome(
        kind=kind,
        stage=SearchStage.TRAIN_EVIDENCE,
        reason_code=code,
        reason=code,
        parameter_hash="a" * 64,
        **CONTEXT,
    )


def test_only_explicit_parameter_exception_is_invalid_param():
    explicit = classify_search_exception(
        InvalidStrategyParameterError("fast period must be below slow period"),
        stage=SearchStage.PARAMETER_VALIDATION,
        parameter_hash="p" * 64,
        **CONTEXT,
    )
    generic_value = classify_search_exception(
        ValueError("missing feature column"),
        stage=SearchStage.SIGNAL_GENERATION,
        parameter_hash="p" * 64,
        **CONTEXT,
    )
    generic_type = classify_search_exception(
        TypeError("malformed cost input"),
        stage=SearchStage.TRAIN_BACKTEST,
        parameter_hash="p" * 64,
        **CONTEXT,
    )

    assert explicit.kind is SearchOutcomeKind.INVALID_PARAM
    assert explicit.reason_code == "EXPLICIT_INVALID_STRATEGY_PARAMETER"
    assert generic_value.kind is SearchOutcomeKind.INFRA_FAILURE
    assert generic_type.kind is SearchOutcomeKind.INFRA_FAILURE


def test_family_aggregation_gives_infrastructure_failure_absolute_precedence():
    aggregate = aggregate_family_cell_outcomes(
        [
            _outcome(SearchOutcomeKind.VALID, "VALID"),
            _outcome(SearchOutcomeKind.EXPECTED_INELIGIBLE, "UNDERPOWERED"),
            _outcome(SearchOutcomeKind.INFRA_FAILURE, "BACKTEST_CRASH"),
        ],
        **CONTEXT,
    )

    assert aggregate.kind is SearchOutcomeKind.INFRA_FAILURE
    assert not aggregate.complete
    assert not aggregate.promotable_candidate_exists
    assert aggregate.valid_trials == 1
    assert aggregate.infrastructure_failure_trials == 1
    error = SearchInfrastructureError(aggregate)
    assert "SEARCH_CELL_INCOMPLETE:EURUSD|H1|TREND|ExampleStrategy" in str(error)


def test_family_aggregation_selects_valid_only_when_every_trial_is_complete():
    aggregate = aggregate_family_cell_outcomes(
        [
            _outcome(SearchOutcomeKind.INVALID_PARAM, "BAD_COMBINATION"),
            _outcome(SearchOutcomeKind.EXPECTED_INELIGIBLE, "NO_TRADES"),
            _outcome(SearchOutcomeKind.VALID, "POWER_PASS"),
        ],
        **CONTEXT,
    )

    assert aggregate.kind is SearchOutcomeKind.VALID
    assert aggregate.complete
    assert aggregate.promotable_candidate_exists


def test_family_aggregation_distinguishes_honest_abstention_from_all_invalid():
    abstention = aggregate_family_cell_outcomes(
        [
            _outcome(SearchOutcomeKind.INVALID_PARAM, "BAD_COMBINATION"),
            _outcome(SearchOutcomeKind.EXPECTED_INELIGIBLE, "UNDERPOWERED"),
        ],
        **CONTEXT,
    )
    all_invalid = aggregate_family_cell_outcomes(
        [_outcome(SearchOutcomeKind.INVALID_PARAM, "BAD_COMBINATION")],
        **CONTEXT,
    )
    missing = aggregate_family_cell_outcomes([], **CONTEXT)

    assert abstention.kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert abstention.complete
    assert all_invalid.kind is SearchOutcomeKind.INVALID_PARAM
    assert all_invalid.complete
    assert missing.kind is SearchOutcomeKind.INFRA_FAILURE
    assert not missing.complete


def test_family_aggregation_rejects_cross_cell_trial_contamination():
    wrong_cell = TrialOutcome(
        kind=SearchOutcomeKind.VALID,
        stage=SearchStage.TRAIN_EVIDENCE,
        reason_code="VALID",
        reason="valid elsewhere",
        symbol="GBPUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family="ExampleStrategy",
        parameter_hash="a" * 64,
    )

    aggregate = aggregate_family_cell_outcomes([wrong_cell], **CONTEXT)

    assert aggregate.kind is SearchOutcomeKind.INFRA_FAILURE
    assert aggregate.reason_code == "TRIAL_CONTEXT_MISMATCH"
    assert "TRIAL_CONTEXT_MISMATCH" in aggregate.reason_codes


def test_trial_artifact_is_bounded_and_never_contains_raw_parameters_or_tracebacks():
    huge = "x" * 10_000
    outcome = TrialOutcome(
        kind=SearchOutcomeKind.INFRA_FAILURE,
        stage=SearchStage.TRAIN_BACKTEST,
        reason_code="BACKTEST_FAILED",
        reason=huge,
        exception_type="RuntimeError",
        evidence={
            "count": 3,
            "detail": huge,
            "reason_codes": [huge] * 100,
        },
        parameter_hash="f" * 64,
        **CONTEXT,
    )

    payload = outcome.to_dict()
    encoded = json.dumps(payload)
    assert len(payload["reason"]) <= 320
    assert len(payload["evidence"]["detail"]) <= 320
    assert len(payload["evidence"]["reason_codes"]) == 64
    assert "parameters" not in payload
    assert "traceback" not in encoded.lower()
    assert len(encoded) < 30_000
