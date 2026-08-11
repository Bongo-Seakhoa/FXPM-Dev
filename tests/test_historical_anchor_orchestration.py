from __future__ import annotations

from datetime import date, timedelta
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_causal_evidence_producer as causal_producer
import pm_historical_anchor_orchestration as orchestration
import pm_portfolio_replay as portfolio_replay
import pm_production_search_planner as search_planner
import pm_training_horizon as training_horizon
from tests import test_training_horizon as horizon_fixture


PROCESS_PRECOMMIT = "a" * 64
PRODUCER_RELEASE = "b" * 64


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical(value: object, *, ascii_only: bool = False) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=ascii_only,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii" if ascii_only else "utf-8")


def _binding(anchor_id: str, cutoff: str, ordinal: int, cell: tuple[str, str, str]):
    symbol, timeframe, regime = cell
    prefix = f"{anchor_id}:{symbol}:{timeframe}:{regime}"
    return {
        "schema_version": orchestration.HORIZON_BINDING_SCHEMA,
        "ordinal": ordinal,
        "cell": {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
        },
        "anchor_id": anchor_id,
        "selection_cutoff_utc": cutoff,
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
        "plan_receipt_sha256": _sha(f"{prefix}:plan"),
        "source_prefix_receipt_sha256": None,
        "candidate_registry_sha256": _sha(f"{prefix}:candidates"),
        "trial_registry_sha256": _sha(f"{prefix}:trials"),
        "attempt_roster_sha256": _sha(f"{prefix}:attempts"),
        "evidence_roster_sha256": _sha(f"{prefix}:evidence"),
        "plan_status": "HISTORICAL_SOURCE_UNAVAILABLE",
    }


def _anchor_inputs():
    cells = tuple(causal.expected_cell_roster())
    anchors = []
    bindings = {}
    cutoff = date(2015, 1, 1)
    for index in range(4):
        anchor_id = f"anchor-{index + 1:02d}"
        cutoff_text = f"{cutoff.isoformat()}T00:00:00Z"
        roster = [
            _binding(anchor_id, cutoff_text, ordinal, cell)
            for ordinal, cell in enumerate(cells)
        ]
        start = cutoff + timedelta(days=35)
        end = start + timedelta(days=364)
        anchors.append(
            {
                "anchor_id": anchor_id,
                "selection_cutoff_utc": cutoff_text,
                "forward_start_date": start.isoformat(),
                "forward_end_date": end.isoformat(),
                "purge_calendar_days": 35,
                "horizon_binding_count": book.FAMILY_SIZE,
                "horizon_binding_roster_sha256": (
                    orchestration.historical_horizon_binding_roster_sha256(roster)
                ),
            }
        )
        bindings[anchor_id] = roster
        cutoff = end + timedelta(days=36)
    return anchors, bindings


def _context(tmp_path: Path):
    return spool.freeze_generation_context(
        tmp_path / "anchor-spool",
        generation_id="historical-generation-001",
        native_universe_sha256="c" * 64,
        config_sha256="d" * 64,
    )


def _precommit(tmp_path: Path):
    context = _context(tmp_path)
    anchors, bindings = _anchor_inputs()
    precommit = orchestration.precommit_historical_anchor_run(
        context,
        run_id="historical-run-001",
        process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
        optimizer_producer_release_sha256=PRODUCER_RELEASE,
        anchors=anchors,
    )
    return context, anchors, bindings, precommit


def _canary_prerequisites(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    generation_id = "raw-winner-canary-nonauthorizing-test"
    # The issuer independently reopens the ACTIVE native contract and proves
    # its canonical sha equals the frozen context's native_universe_sha256, so
    # the fixture must freeze the context against the exact contract it fakes.
    native_contract = {
        "activation_id": "production-activation",
        "activation_manifest_sha256": "2" * 64,
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
    }
    monkeypatch.setattr(
        "pm_portfolio_replay.active_native_portfolio_contract",
        lambda _data_dir, _symbols, **_kwargs: (dict(native_contract), []),
    )
    context = spool.freeze_generation_context(
        tmp_path / "canary-spool",
        generation_id=generation_id,
        native_universe_sha256=portfolio_replay.canonical_json_sha256(native_contract),
        config_sha256="d" * 64,
    )
    process = search_planner.freeze_production_search_process(
        context, run_id=generation_id
    )
    anchors = []
    cutoff = date(2015, 1, 1)
    for index in range(4):
        start = cutoff + timedelta(days=35)
        end = start + timedelta(days=364)
        anchors.append(
            {
                "anchor_id": f"CANARY-{index + 1:02d}",
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
        process_id="raw-winner-canary-nonauthorizing-process",
        anchors=anchors,
    )
    plan = {
        "receipt_sha256": "1" * 64,
        "activation_id": "production-activation",
        "activation_manifest_sha256": "2" * 64,
        "native_run_manifest_sha256": "3" * 64,
        "native_v5_common_request_sha256": "4" * 64,
        "symbol_roster": list(book.PINNED_SYMBOL_ORDER),
        "timeframe_roster": list(book.TIMEFRAME_ORDER),
        "anchor_count": len(anchors),
        "anchor_plan": anchors,
        "anchor_plan_sha256": orchestration._canonical_sha256(anchors),
    }
    monkeypatch.setattr(
        "pm_production_anchor_plan.verify_production_anchor_plan_receipt",
        lambda _data_dir, _receipt: dict(plan),
    )
    return context, process, causal_precommit, plan


def test_bounded_canary_request_is_issuer_sealed_latest_anchor_and_one_shot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert list(book.PINNED_SYMBOL_ORDER) != sorted(book.PINNED_SYMBOL_ORDER)
    context, process, causal_precommit, plan = _canary_prerequisites(
        tmp_path, monkeypatch
    )
    request = orchestration.issue_bounded_raw_winner_canary_request(
        context,
        data_dir=tmp_path,
        anchor_plan_receipt=object(),
        search_process=process,
        causal_precommit=causal_precommit,
        symbol=book.PINNED_SYMBOL_ORDER[0],
        timeframe=book.TIMEFRAME_ORDER[0],
    )
    body = orchestration.verify_bounded_raw_winner_canary_request(context, request)

    assert body["anchor"] == plan["anchor_plan"][-1]
    assert body["production_symbols"] == list(book.PINNED_SYMBOL_ORDER)
    assert body["production_timeframes"] == list(book.TIMEFRAME_ORDER)
    assert body["regime_selection_rule"] == (
        orchestration.BOUNDED_RAW_WINNER_CANARY_SELECTION_RULE
    )
    assert "regime" not in body
    assert body["attempt_count_exact"] == (
        process.to_dict()["semantics"]["attempt_policy"][
            "attempt_count_per_observed_cell"
        ]
    )
    assert body["attempt_count_exact"] == search_planner.ATTEMPT_COUNT
    assert body["native_authority_verification"] is None
    assert all(
        body[name] is False
        for name in (
            "book_projection_authorized",
            "book_multiplicity_authorized",
            "causal_authorized",
            "current_refit_authorized",
            "capital_authorized",
            "publication_authorized",
            "active_pointer_mutation_authorized",
            "configuration_mutation_authorized",
            "order_authorized",
            "profitability_claimed",
        )
    )

    tampered = dict(body)
    tampered["timeframe"] = book.TIMEFRAME_ORDER[1]
    forged = replace(request, _body_json=_canonical(tampered))
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="CANARY_REQUEST_ISSUANCE_INVALID",
    ):
        orchestration.verify_bounded_raw_winner_canary_request(context, forged)

    substituted_sink = spool.open_symbol_sink(context, request.to_dict()["symbol"])
    copied_tag = orchestration.BoundedRawWinnerCanaryRequest(
        request._body_json,
        substituted_sink,
        request._issuance_tag,
    )
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="CANARY_REQUEST_ISSUANCE_INVALID",
    ):
        orchestration.verify_bounded_raw_winner_canary_request(context, copied_tag)

    consumed, sink = orchestration.consume_bounded_raw_winner_canary_request(
        context, request
    )
    assert consumed == body
    assert sink is request.symbol_sink()
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="CANARY_REQUEST_ALREADY_CONSUMED",
    ):
        orchestration.consume_bounded_raw_winner_canary_request(context, request)


@pytest.mark.parametrize(
    "process_body",
    [
        {},
        {"semantics": {}},
        {"semantics": {"attempt_policy": {}}},
        {
            "semantics": {
                "attempt_policy": {"attempt_count_per_observed_cell": None}
            }
        },
        {
            "semantics": {
                "attempt_policy": {"attempt_count_per_observed_cell": True}
            }
        },
        {
            "semantics": {
                "attempt_policy": {"attempt_count_per_observed_cell": 0}
            }
        },
        {
            "semantics": {
                "attempt_policy": {"attempt_count_per_observed_cell": -1}
            }
        },
        {
            "semantics": {
                "attempt_policy": {"attempt_count_per_observed_cell": 1.0}
            }
        },
    ],
)
def test_bounded_canary_process_attempt_count_rejects_invalid_contract(
    process_body: dict[str, object],
) -> None:
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="CANARY_SEARCH_PROCESS_ATTEMPT_COUNT_INVALID",
    ):
        orchestration._bounded_canary_process_attempt_count(process_body)


def test_bounded_canary_native_authority_metrics_are_exact_and_tamper_evident() -> None:
    metrics = {
        "schema_version": "fxpm.production_native_authority_verification_scope.v1",
        "scope": "9" * 64,
        "full_verification_count": 1,
        "identity_revalidation_attempt_count": 5,
        "identity_revalidation_count": 5,
        "identity_revalidation_failure_count": 0,
        "active_pointer_sha256": "8" * 64,
        "activation_manifest_sha256": "2" * 64,
        "pid": 1234,
        "thread_id": 5678,
        "poison_reason": None,
        "closed": True,
        "poisoned": False,
    }
    assert orchestration._normalized_native_authority_verification(
        metrics,
        expected_activation_manifest_sha256="2" * 64,
        allow_none=False,
    ) == metrics

    for field, value in (
        ("full_verification_count", 2),
        ("identity_revalidation_attempt_count", 4),
        ("identity_revalidation_count", 4),
        ("identity_revalidation_failure_count", 1),
        ("activation_manifest_sha256", "7" * 64),
        ("pid", 0),
        ("thread_id", 0),
        ("poison_reason", "ACTIVE_POINTER_CHANGED"),
        ("closed", False),
        ("poisoned", True),
    ):
        tampered = dict(metrics)
        tampered[field] = value
        with pytest.raises(
            orchestration.HistoricalAnchorOrchestrationError,
            match="CANARY_NATIVE_AUTHORITY",
        ):
            orchestration._normalized_native_authority_verification(
                tampered,
                expected_activation_manifest_sha256="2" * 64,
                allow_none=False,
            )


def test_bounded_canary_issuance_rejects_nonproduction_grid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, process, causal_precommit, plan = _canary_prerequisites(
        tmp_path, monkeypatch
    )
    plan["symbol_roster"] = plan["symbol_roster"][:-1]

    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="CANARY_PRODUCTION_GRID_MISMATCH",
    ):
        orchestration.issue_bounded_raw_winner_canary_request(
            context,
            data_dir=tmp_path,
            anchor_plan_receipt=object(),
            search_process=process,
            causal_precommit=causal_precommit,
            symbol=book.PINNED_SYMBOL_ORDER[0],
            timeframe=book.TIMEFRAME_ORDER[0],
        )


def test_pre_evidence_planning_request_has_no_fabricated_historical_precommit(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    anchor = _anchor_inputs()[0][0]
    geometry = {
        name: anchor[name]
        for name in (
            "anchor_id",
            "selection_cutoff_utc",
            "forward_start_date",
            "forward_end_date",
            "purge_calendar_days",
        )
    }
    sinks = {
        symbol: spool.open_symbol_sink(context, symbol)
        for symbol in book.PINNED_SYMBOL_ORDER
    }

    request = orchestration.issue_historical_pre_evidence_planning_request(
        context,
        run_id="historical-run-001",
        process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
        optimizer_producer_release_sha256=PRODUCER_RELEASE,
        anchor=geometry,
        symbol_sinks=sinks,
    )
    body = request.to_dict()

    assert body["schema_version"] == (
        orchestration.PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA
    )
    assert "precommit_receipt_sha256" not in body
    assert body["historical_anchor_precommit_pending"] is True
    assert body["planning_access_limited_to_native_prefix_and_process_design"] is True
    assert body["return_or_winner_evidence_access_authorized"] is False
    assert body["confers_causal_or_current_authority"] is False
    assert (
        request.symbol_sink(book.PINNED_SYMBOL_ORDER[0])
        is sinks[book.PINNED_SYMBOL_ORDER[0]]
    )

    incomplete_sinks = dict(sinks)
    incomplete_sinks.pop(book.PINNED_SYMBOL_ORDER[-1])
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="PRE_EVIDENCE_PLANNING_SYMBOL_SINKS_INVALID",
    ):
        orchestration.issue_historical_pre_evidence_planning_request(
            context,
            run_id="historical-run-001",
            process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
            optimizer_producer_release_sha256=PRODUCER_RELEASE,
            anchor=geometry,
            symbol_sinks=incomplete_sinks,
        )

    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="ANCHOR_FIELDS_INVALID",
    ):
        orchestration.issue_historical_pre_evidence_planning_request(
            context,
            run_id="historical-run-001",
            process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
            optimizer_producer_release_sha256=PRODUCER_RELEASE,
            anchor=anchor,
            symbol_sinks=sinks,
        )


def _terminal_plan(
    *,
    anchor_id: str,
    cutoff: str,
    forward_start_date: str,
    forward_end_date: str,
    cell: tuple[str, str, str],
):
    request = horizon_fixture._terminal_source_request("SOURCE_UNAVAILABLE")
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor_id,
        "ordinal": int(anchor_id.rsplit("-", 1)[-1]),
        "selection_cutoff_utc": cutoff,
        "forward_start_utc": f"{forward_start_date}T00:00:00Z",
        "forward_end_exclusive_utc": f"{forward_end_date}T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    request["cell"] = {
        "symbol": cell[0],
        "timeframe": cell[1],
        "regime": cell[2],
    }
    request["native_prefix"]["symbol"] = cell[0]
    request["native_prefix"]["timeframe"] = cell[1]
    request["native_prefix"]["source_terminal_receipt_sha256"] = _sha(":".join(cell))
    return training_horizon.build_historical_horizon_plan(request)


def _terminal_technical_plan(
    *,
    anchor_id: str,
    cutoff: str,
    forward_start_date: str,
    forward_end_date: str,
    cell: tuple[str, str, str],
):
    request = horizon_fixture._terminal_source_request("TECHNICAL_TERMINAL")
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor_id,
        "ordinal": int(anchor_id.rsplit("-", 1)[-1]),
        "selection_cutoff_utc": cutoff,
        "forward_start_utc": f"{forward_start_date}T00:00:00Z",
        "forward_end_exclusive_utc": f"{forward_end_date}T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    request["cell"] = {
        "symbol": cell[0],
        "timeframe": cell[1],
        "regime": cell[2],
    }
    request["native_prefix"].update(
        {
            "symbol": cell[0],
            "timeframe": cell[1],
            "source_reason": "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL",
            "source_terminal_receipt_sha256": _sha(":".join(cell)),
        }
    )
    return training_horizon.build_historical_horizon_plan(request)


def _symbol_evidence(anchor_id: str, symbol: str):
    evidence = []
    for timeframe in book.TIMEFRAME_ORDER:
        tag = f"{anchor_id}:{symbol}:{timeframe}"
        source = book.create_historical_source_group_decision(
            symbol=symbol,
            timeframe=timeframe,
            availability=book.TERMINAL_UNAVAILABLE,
            reason_code="BROKER_ARCHIVE_UNAVAILABLE",
            source_contract_sha256=_sha(f"{tag}:source-contract"),
            source_group_evidence_sha256=_sha(f"{tag}:source-evidence"),
            acquisition_attempts_exact=1,
            terminal_proof_sha256=_sha(f"{tag}:terminal-proof"),
        )
        assert source is not None
        fit = book.create_regime_fit_decision(
            source_group_decision=source,
            fit_state=book.REGIME_NOT_APPLICABLE,
            reason_code="SOURCE_TERMINAL_UNAVAILABLE",
            regime_fit_policy_sha256=_sha(f"{tag}:regime-policy"),
        )
        assert fit is not None
        for regime in book.REGIME_ORDER:
            state = book.create_historical_process_state_decision(
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                state_kind=book.TERMINAL_SOURCE_ABSENCE,
                reason_code="SOURCE_TERMINAL_UNAVAILABLE",
                source_group_decision=source,
                regime_fit_decision=fit,
                attempted_trials_exact=0,
            )
            assert state is not None
            cell = book.process_state_cell_evidence(
                process_state_decision=state,
                source_group_decision=source,
                regime_fit_decision=fit,
            )
            assert cell is not None
            evidence.append(cell)
    return evidence


def _terminal_technical_evidence(symbol: str, timeframe: str, regime: str):
    tag = f"terminal-technical:{symbol}:{timeframe}"
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.TERMINAL_TECHNICAL,
        reason_code="AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL",
        source_contract_sha256=_sha(f"{tag}:source-contract"),
        source_group_evidence_sha256=_sha(f"{tag}:source-evidence"),
        acquisition_attempts_exact=1,
        activation_id="native-activation-001",
        activation_manifest_sha256="3" * 64,
        activation_entry_sha256="4" * 64,
        terminal_proof_sha256=_sha(":".join((symbol, timeframe, regime))),
    )
    assert source is not None
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL,
        reason_code="AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL",
        regime_fit_policy_sha256=_sha(f"{tag}:regime-policy"),
    )
    assert fit is not None
    state = book.create_historical_process_state_decision(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        state_kind=book.TERMINAL_TECHNICAL,
        reason_code="AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL",
        source_group_decision=source,
        regime_fit_decision=fit,
        attempted_trials_exact=0,
    )
    assert state is not None
    evidence = book.process_state_cell_evidence(
        process_state_decision=state,
        source_group_decision=source,
        regime_fit_decision=fit,
    )
    assert evidence is not None
    return evidence


def test_horizon_binding_requires_exact_sealed_plan_and_rejects_duck_type() -> None:
    cell = tuple(causal.expected_cell_roster())[0]
    plan = _terminal_plan(
        anchor_id="anchor-01",
        cutoff="2015-01-01T00:00:00Z",
        forward_start_date="2015-02-05",
        forward_end_date="2016-02-04",
        cell=cell,
    )
    assert training_horizon._valid_plan(plan)
    binding = orchestration.historical_horizon_plan_binding(plan)
    assert binding["ordinal"] == 0
    assert binding["plan_receipt_sha256"] == plan.receipt_sha256
    assert binding["plan_status"] == "HISTORICAL_SOURCE_UNAVAILABLE"

    body = plan.to_dict()

    class DuckPlan:
        receipt_sha256 = hashlib.sha256(_canonical(body)).hexdigest()

        @staticmethod
        def to_dict():
            return json.loads(_canonical(body).decode("utf-8"))

    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_PLAN_OBJECT_INVALID",
    ):
        orchestration.historical_horizon_plan_binding(DuckPlan())

    forged = dict(binding)
    forged["candidate_registry_sha256"] = "f" * 64
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_PLAN_OBJECT_INVALID",
    ):
        orchestration.historical_horizon_plan_binding(forged)


def test_horizon_status_is_bound_to_exact_book_cell_state() -> None:
    symbol = book.PINNED_SYMBOL_ORDER[0]
    timeframe = book.TIMEFRAME_ORDER[0]
    cell = (symbol, timeframe, book.REGIME_ORDER[0])
    plan = _terminal_plan(
        anchor_id="anchor-01",
        cutoff="2015-01-01T00:00:00Z",
        forward_start_date="2015-02-05",
        forward_end_date="2016-02-04",
        cell=cell,
    )
    binding = orchestration.historical_horizon_plan_binding(plan)
    evidence = _symbol_evidence("anchor-01", symbol)[0]
    orchestration._assert_horizon_evidence_consistency(binding, evidence)

    searchable = {**binding, "plan_status": "READY_FOR_HISTORICAL_EVIDENCE"}
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_SEARCHABLE_EVIDENCE_MISMATCH",
    ):
        orchestration._assert_horizon_evidence_consistency(searchable, evidence)

    technical = {**binding, "plan_status": "HISTORICAL_TECHNICAL_INCOMPLETE"}
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_TECHNICAL_INCOMPLETE_BLOCKS_ANCHOR",
    ):
        orchestration._assert_horizon_evidence_consistency(technical, evidence)

    terminal_technical_plan = _terminal_technical_plan(
        anchor_id="anchor-01",
        cutoff="2015-01-01T00:00:00Z",
        forward_start_date="2015-02-05",
        forward_end_date="2016-02-04",
        cell=cell,
    )
    terminal_technical_binding = orchestration.historical_horizon_plan_binding(
        terminal_technical_plan
    )
    terminal_technical = _terminal_technical_evidence(*cell)
    orchestration._assert_horizon_evidence_consistency(
        terminal_technical_binding, terminal_technical
    )
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_TERMINAL_TECHNICAL_EVIDENCE_MISMATCH",
    ):
        orchestration._assert_horizon_evidence_consistency(
            terminal_technical_binding, evidence
        )


def test_forged_insufficient_registry_or_status_is_rejected() -> None:
    symbol = book.PINNED_SYMBOL_ORDER[0]
    timeframe = book.TIMEFRAME_ORDER[0]
    regime = book.REGIME_ORDER[0]
    binding = _binding(
        "anchor-01",
        "2015-01-01T00:00:00Z",
        0,
        (symbol, timeframe, regime),
    )
    binding["source_prefix_receipt_sha256"] = "1" * 64
    binding["plan_status"] = "HISTORICAL_BOUNDARY_LIMITED"
    insufficient = {
        "selection_cutoff_utc": binding["selection_cutoff_utc"],
        "source_prefix_receipt_sha256": binding["source_prefix_receipt_sha256"],
        "horizon_plan_receipt_sha256": binding["plan_receipt_sha256"],
        "candidate_registry_sha256": binding["candidate_registry_sha256"],
        "trial_registry_sha256": binding["trial_registry_sha256"],
        "horizon_attempt_roster_sha256": binding["attempt_roster_sha256"],
        "evidence_roster_sha256": binding["evidence_roster_sha256"],
        "terminal_stage": book.INSUFFICIENT_PRESEARCH_STAGE,
        "reason_code": book.INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
        "planned_attempt_count": 1,
        "process_rejected_attempt_count": 0,
    }
    state_body = {
        "state_kind": book.INSUFFICIENT_EVIDENCE,
        "attempted_trials_exact": 0,
        "insufficient_evidence_binding": insufficient,
    }
    encoded = _canonical(state_body, ascii_only=True)
    state = book.HistoricalProcessStateDecision(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    evidence = book.HistoricalBookCellEvidence(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        source_group_decision=None,
        regime_fit_decision=None,
        process_state_decision=state,
    )
    orchestration._assert_horizon_evidence_consistency(binding, evidence)

    forged_registry = {**binding, "candidate_registry_sha256": "f" * 64}
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_INSUFFICIENT_EVIDENCE_BINDING_MISMATCH",
    ):
        orchestration._assert_horizon_evidence_consistency(forged_registry, evidence)

    forged_status = {**binding, "plan_status": "READY_FOR_HISTORICAL_EVIDENCE"}
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_POSTSEARCH_INSUFFICIENT_EVIDENCE_MISMATCH",
    ):
        orchestration._assert_horizon_evidence_consistency(forged_status, evidence)


def test_generic_optimizer_payload_cannot_bypass_adapter_graph(tmp_path: Path) -> None:
    context = _context(tmp_path)
    anchor = _anchor_inputs()[0][0]
    precommit_body = {
        "run_id": "historical-run-001",
        "optimizer_producer_release_sha256": PRODUCER_RELEASE,
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    kwargs = {
        "precommit_body": precommit_body,
        "anchor": anchor,
        "symbol": book.PINNED_SYMBOL_ORDER[0],
        "producer_request_sha256": "e" * 64,
        "horizon_bindings": [],
    }
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="ADAPTER_OPTIMIZER_PAYLOAD_REQUIRED",
    ):
        orchestration._verify_adapter_manifest_graph(context, {}, None, **kwargs)
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="ADAPTER_OPTIMIZER_PAYLOAD_SCHEMA_INVALID",
    ):
        orchestration._verify_adapter_manifest_graph(
            context,
            {},
            {"schema_version": "test.anchor-optimizer-output.v1"},
            **kwargs,
        )


def test_persisted_book_is_replayed_by_exact_v11_semantic_authority() -> None:
    family = (
        evidence
        for symbol in book.PINNED_SYMBOL_ORDER
        for evidence in _symbol_evidence("anchor-01", symbol)
    )
    decision = book.evaluate_historical_book_multiplicity(family)
    assert book.verify_historical_book_multiplicity_decision(decision)
    payload = decision.to_dict()
    reopened = orchestration._reopen_verified_book_decision(payload)
    assert reopened.receipt_sha256 == decision.receipt_sha256

    forged = json.loads(_canonical(payload).decode("utf-8"))
    assert "target_fdr" in forged
    forged["target_fdr"] = "0.5"
    forged.pop("receipt_sha256")
    encoded = _canonical(forged, ascii_only=True)
    forged["receipt_sha256"] = hashlib.sha256(encoded).hexdigest()
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="BOOK_DECISION_SEMANTICS_INVALID",
    ):
        orchestration._reopen_verified_book_decision(forged)


def test_worker_request_reopens_exact_bytes_with_only_its_sealed_symbol_sink(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    sink = spool.open_symbol_sink(
        context,
        symbol,
        manifest_domain=spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
    )
    body = {
        "schema_version": orchestration.PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA,
        "authority_scope": orchestration.AUTHORITY_SCOPE,
        "run_id": "historical-worker-test",
        "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256,
        "historical_anchor_precommit_pending": True,
        "process_precommit_receipt_sha256": "a" * 64,
        "optimizer_producer_release_sha256": "b" * 64,
        "anchor": {
            "anchor_id": "A1",
            "selection_cutoff_utc": "2024-01-01T00:00:00Z",
            "forward_start_date": "2024-02-05",
            "forward_end_date": "2025-02-03",
            "purge_calendar_days": 35,
        },
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "requires_exact_sparse_state_roster": True,
        "possible_regime_slots_are_forced_observations": False,
        "planning_access_limited_to_native_prefix_and_process_design": True,
        "return_or_winner_evidence_access_authorized": False,
        "confers_causal_or_current_authority": False,
    }
    raw = _canonical(body)
    request = orchestration.reopen_anchor_producer_request_for_symbol(
        context,
        request_body_bytes=raw,
        expected_receipt_sha256=hashlib.sha256(raw).hexdigest(),
        symbol=symbol,
        sink=sink,
    )
    assert request.to_dict() == body
    assert request.symbol_sink(symbol) is sink

    forged = orchestration.AnchorProducerRequest(raw, {symbol: sink}, symbol, object())
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="PRODUCER_REQUEST_SYMBOL_SINKS_UNAVAILABLE",
    ):
        forged.symbol_sink(symbol)


def test_anchor_producer_callback_is_invoked_exactly_once(
    tmp_path: Path, monkeypatch
) -> None:
    symbol = book.PINNED_SYMBOL_ORDER[0]
    timeframe = book.TIMEFRAME_ORDER[0]
    regime = book.REGIME_ORDER[0]
    cell = (symbol, timeframe, regime)
    anchor = {
        "anchor_id": "anchor-01",
        "selection_cutoff_utc": "2015-01-01T00:00:00Z",
        "forward_start_date": "2015-02-05",
        "forward_end_date": "2016-02-04",
        "purge_calendar_days": 35,
        "horizon_binding_count": 1,
        "horizon_binding_roster_sha256": "0" * 64,
    }
    plan = _terminal_plan(
        anchor_id=anchor["anchor_id"],
        cutoff=anchor["selection_cutoff_utc"],
        forward_start_date=anchor["forward_start_date"],
        forward_end_date=anchor["forward_end_date"],
        cell=cell,
    )
    binding = orchestration.historical_horizon_plan_binding(plan)
    anchor["horizon_binding_roster_sha256"] = orchestration._canonical_sha256([binding])
    evidence = _symbol_evidence(anchor["anchor_id"], symbol)[0]
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, symbol)
    request = orchestration.AnchorProducerRequest(
        _canonical({"anchor": {"anchor_id": anchor["anchor_id"]}}),
        {symbol: sink},
    )
    precommit_body = {"process_precommit_receipt_sha256": PROCESS_PRECOMMIT}
    decision_body = {
        "valid": True,
        "optimization_run_complete": True,
        "source_group_size": 1,
        "family_size": 1,
        "counts": {"SOURCE_UNAVAILABLE": 1},
    }

    class Decision:
        receipt_sha256 = hashlib.sha256(_canonical(decision_body)).hexdigest()
        book_generation_sha256 = "9" * 64
        status = "NO_LOCAL_WINNERS"

        @staticmethod
        def to_dict():
            return decision_body

    calls = 0

    def producer(_request):
        nonlocal calls
        calls += 1
        yield orchestration.AnchorSymbolProduct(
            symbol=symbol,
            horizon_plans=[plan],
            book_evidence=[evidence],
            optimizer_payload={"schema_version": "not-inspected-in-anchor-book"},
        )

    def evaluate(stream):
        assert list(stream) == [evidence]
        return Decision()

    monkeypatch.setattr(book, "PINNED_SYMBOL_ORDER", (symbol,))
    monkeypatch.setattr(book, "TIMEFRAME_ORDER", (timeframe,))
    monkeypatch.setattr(book, "REGIME_ORDER", (regime,))
    monkeypatch.setattr(book, "SOURCE_GROUP_SIZE", 1)
    monkeypatch.setattr(book, "FAMILY_SIZE", 1)
    monkeypatch.setattr(orchestration, "_CELLS_PER_SYMBOL", 1)
    monkeypatch.setattr(book, "evaluate_historical_book_multiplicity", evaluate)
    monkeypatch.setattr(
        book, "verify_historical_book_multiplicity_decision", lambda _decision: True
    )
    orchestration._anchor_book(
        sinks={symbol: sink},
        precommit_body=precommit_body,
        anchor=anchor,
        request=request,
        producer=producer,
    )
    assert calls == 1


def test_historical_run_seals_and_reopens_the_historical_manifest_domain(
    tmp_path: Path,
    monkeypatch,
) -> None:
    context, anchors, _, precommit = _precommit(tmp_path)
    symbol = book.PINNED_SYMBOL_ORDER[0]

    def anchor_book(**kwargs):
        anchor_id = kwargs["anchor"]["anchor_id"]
        body = {
            "valid": True,
            "optimization_run_complete": True,
            "source_group_size": 1,
            "family_size": 1,
            "counts": {"SOURCE_UNAVAILABLE": 1},
        }
        return (
            SimpleNamespace(
                receipt_sha256=_sha(f"decision:{anchor_id}"),
                book_generation_sha256=_sha(f"book:{anchor_id}"),
                status="NO_LOCAL_WINNERS",
                to_dict=lambda body=body: dict(body),
            ),
            [],
        )

    observed: dict[str, object] = {}

    def verify_run(_context, _precommit, candidate):
        symbol_receipt = candidate.to_dict()["symbol_receipts"][0]
        observed["receipt"] = symbol_receipt
        observed["manifest"] = spool.verify_symbol_receipt(
            context.root,
            symbol_receipt,
            expected_generation_id=context.generation_id,
            expected_symbol=symbol,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
            expected_manifest_domain=spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
        )
        return True

    monkeypatch.setattr(orchestration, "_anchor_book", anchor_book)
    monkeypatch.setattr(
        orchestration,
        "verify_historical_anchor_run_receipt",
        verify_run,
    )
    producers = {anchor["anchor_id"]: (lambda _request: ()) for anchor in anchors}

    orchestration.execute_historical_anchor_run(
        context,
        precommit,
        producers,
    )

    assert observed["receipt"]["manifest_domain"] == (
        spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN
    )
    assert observed["manifest"]["manifest_domain"] == (
        spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN
    )

def test_exact_callback_roster_and_causal_anchor_geometry_are_mandatory(
    tmp_path: Path,
) -> None:
    context, anchors, _, precommit = _precommit(tmp_path)
    producers = {anchor["anchor_id"]: (lambda _request: ()) for anchor in anchors}
    producers.pop(anchors[-1]["anchor_id"])
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="EXACT_PER_ANCHOR_PRODUCER_CALLBACKS_REQUIRED",
    ):
        orchestration.execute_historical_anchor_run(
            context,
            precommit,
            producers,
        )

    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="ANCHOR_COUNT_OUTSIDE_CAUSAL_POLICY",
    ):
        orchestration.precommit_historical_anchor_run(
            context,
            run_id="short-run",
            process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
            optimizer_producer_release_sha256=PRODUCER_RELEASE,
            anchors=anchors[:3],
        )

    overlapped = [dict(anchor) for anchor in anchors]
    overlapped[1]["selection_cutoff_utc"] = anchors[0]["selection_cutoff_utc"]
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="PURGE_GEOMETRY|CUTOFF_ORDER|ROLLING_DISJOINT",
    ):
        orchestration.precommit_historical_anchor_run(
            context,
            run_id="overlap-run",
            process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
            optimizer_producer_release_sha256=PRODUCER_RELEASE,
            anchors=overlapped,
        )


def test_horizon_commitment_rejects_missing_or_reordered_accounting_slots() -> None:
    anchors, bindings = _anchor_inputs()
    roster = bindings[anchors[0]["anchor_id"]]
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="INCOMPLETE",
    ):
        orchestration.historical_horizon_binding_roster_sha256(roster[:-1])
    reordered = list(roster)
    reordered[0], reordered[1] = reordered[1], reordered[0]
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="ROSTER_MISMATCH",
    ):
        orchestration.historical_horizon_binding_roster_sha256(reordered)
