from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
from types import SimpleNamespace
from typing import Any

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as producer
import pm_causal_process as causal
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as historical_adapter
import pm_training_horizon as training_horizon
import pm_portfolio_replay as portfolio
import pm_production_authority_coordinator as coordinator
import pm_production_search_planner as search_planner
from tests import test_book_multiplicity as book_fixture
from tests import test_causal_process as causal_fixture
from tests import test_forward_oos_replay_producer as forward_fixture
from tests import test_historical_production_adapter as adapter_fixture

completed_raw_transport = forward_fixture.completed_raw_transport


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _tag_sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _verified_pbo_winner(
    symbol: str,
    timeframe: str,
    regime: str,
) -> book.HistoricalBookCellEvidence:
    """Reuse the genuine Winner-v14 local-winner fixture."""

    return book_fixture._verified_winner(symbol, timeframe, regime)


def _support(
    *,
    source: book.HistoricalSourceGroupDecision,
    cutoff: str,
    detector_code_sha256: str,
    parameters: dict[str, Any],
    tag: str,
) -> dict[str, Any]:
    source_body = source.to_dict()
    source_body.pop("receipt_sha256")
    counts = {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 1}
    parameters_text = json.dumps(
        parameters,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    body = {
        "schema_version": book.REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "symbol": source_body["symbol"],
        "timeframe": source_body["timeframe"],
        "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "label_order": list(book.REGIME_ORDER),
        "eligible_rows": sum(counts.values()),
        "counts": counts,
        "observed_regimes": list(book.REGIME_ORDER),
        "unobserved_regimes": [],
        "support_state_by_regime": {
            regime: "EMPIRICALLY_OBSERVED" for regime in book.REGIME_ORDER
        },
        "first_ts": "2010-01-01T00:00:00+00:00",
        "last_ts": datetime.fromisoformat(cutoff.replace("Z", "+00:00")).isoformat(),
        "vector_encoding": book.REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "vector_sha256": _tag_sha(f"{tag}:support-vector"),
        "bindings": {
            "symbol": source_body["symbol"],
            "timeframe": source_body["timeframe"],
            "activation_id": source_body["activation_id"],
            "activation_manifest_sha256": source_body["activation_manifest_sha256"],
            "source_sha256": source_body["native_source_sha256"],
            "train_window_sha256": _tag_sha(f"{tag}:train-window"),
            "params_sha256": hashlib.sha256(
                parameters_text.encode("ascii")
            ).hexdigest(),
            "detector_code_sha256": detector_code_sha256,
        },
    }
    body["receipt_sha256"] = _sha(body)
    return body


def _available_group(
    *, anchor_id: str, cutoff: str, symbol: str, timeframe: str
) -> tuple[book.HistoricalBookCellEvidence, ...]:
    base = [
        _verified_pbo_winner(symbol, timeframe, "TREND"),
        *(
            book_fixture._verified_no_winner(symbol, timeframe, regime)
            for regime in book.REGIME_ORDER[1:]
        ),
    ]
    source_contracts = {
        item.winner_request.precommitment.to_dict()["source_contract_sha256"]
        for item in base
    }
    assert len(source_contracts) == 1
    tag = f"{anchor_id}:{symbol}:{timeframe}"
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.AVAILABLE,
        reason_code="ACTIVATED_NATIVE_SOURCE_AVAILABLE",
        source_contract_sha256=next(iter(source_contracts)),
        source_group_evidence_sha256=_tag_sha(f"{tag}:source-evidence"),
        activation_id=f"activation:{tag}",
        activation_manifest_sha256=_tag_sha(f"{tag}:activation-manifest"),
        native_source_sha256=_tag_sha(f"{tag}:native-source"),
        acquisition_attempts_exact=2,
    )
    assert source is not None
    parameters = causal_fixture._detector_parameters()
    detector_code = _tag_sha(f"{tag}:detector-code")
    record = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc=datetime.fromisoformat(
            cutoff.replace("Z", "+00:00")
        ).isoformat(),
        detector_version="fxpm.market-regime-detector/v1",
        detector_code_sha256=detector_code,
        parameters=parameters,
    )
    assert record is not None
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256=_tag_sha(f"{tag}:regime-fit-policy"),
        detector_process_sha256=detector_code,
        regime_fit_evidence_sha256=_tag_sha(f"{tag}:regime-fit-evidence"),
        four_regime_mapping_sha256=_tag_sha(f"{tag}:four-regime-map"),
        regime_params_sha256=record["parameters_sha256"],
        regime_detector_record=record,
        train_regime_support=_support(
            source=source,
            cutoff=cutoff,
            detector_code_sha256=detector_code,
            parameters=parameters,
            tag=tag,
        ),
    )
    assert fit is not None
    result = []
    for item in base:
        evidence = book.winner_cell_evidence(
            item.winner_decision,
            item.winner_request,
            source,
            fit,
        )
        assert evidence is not None
        result.append(evidence)
    return tuple(result)


def _terminal_group(
    *, anchor_id: str, symbol: str, timeframe: str
) -> tuple[book.HistoricalBookCellEvidence, ...]:
    tag = f"{anchor_id}:{symbol}:{timeframe}"
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.TERMINAL_UNAVAILABLE,
        reason_code="BROKER_HISTORY_TERMINALLY_UNAVAILABLE",
        source_contract_sha256=_tag_sha(f"{tag}:source-contract"),
        source_group_evidence_sha256=_tag_sha(f"{tag}:source-evidence"),
        acquisition_attempts_exact=2,
        terminal_proof_sha256=_tag_sha(f"{tag}:terminal-proof"),
    )
    assert source is not None
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_NOT_APPLICABLE,
        reason_code="SOURCE_TERMINAL_UNAVAILABLE",
        regime_fit_policy_sha256=_tag_sha(f"{tag}:regime-fit-policy"),
    )
    assert fit is not None
    values = []
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
        evidence = book.process_state_cell_evidence(
            process_state_decision=state,
            source_group_decision=source,
            regime_fit_decision=fit,
        )
        assert evidence is not None
        values.append(evidence)
    return tuple(values)


def _family(anchor: dict[str, Any], winner_symbol: str) -> tuple[Any, ...]:
    values: list[book.HistoricalBookCellEvidence] = []
    for symbol in book.PINNED_SYMBOL_ORDER:
        for timeframe in book.TIMEFRAME_ORDER:
            if symbol == winner_symbol and timeframe == "M5":
                values.extend(
                    _available_group(
                        anchor_id=anchor["anchor_id"],
                        cutoff=anchor["selection_cutoff_utc"],
                        symbol=symbol,
                        timeframe=timeframe,
                    )
                )
            else:
                values.extend(
                    _terminal_group(
                        anchor_id=anchor["anchor_id"],
                        symbol=symbol,
                        timeframe=timeframe,
                    )
                )
    assert len(values) == book.FAMILY_SIZE
    return tuple(values)


def _binding(
    *,
    process_sha: str,
    anchor: dict[str, Any],
    ordinal: int,
    cell: tuple[str, str, str],
    winner_symbol: str,
) -> dict[str, Any]:
    symbol, timeframe, regime = cell
    tag = f"{anchor['anchor_id']}:{symbol}:{timeframe}:{regime}"
    available = symbol == winner_symbol and timeframe == "M5"
    return {
        "schema_version": historical.HORIZON_BINDING_SCHEMA,
        "ordinal": ordinal,
        "cell": {"symbol": symbol, "timeframe": timeframe, "regime": regime},
        "anchor_id": anchor["anchor_id"],
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": process_sha,
        "plan_receipt_sha256": _tag_sha(f"{tag}:plan"),
        "source_prefix_receipt_sha256": (
            _tag_sha(f"{tag}:source-prefix") if available else None
        ),
        "candidate_registry_sha256": _tag_sha(f"{tag}:candidates"),
        "trial_registry_sha256": _tag_sha(f"{tag}:trials"),
        "attempt_roster_sha256": _tag_sha(f"{tag}:attempts"),
        "evidence_roster_sha256": _tag_sha(f"{tag}:evidence"),
        "plan_status": (
            "READY_FOR_HISTORICAL_EVIDENCE"
            if available
            else "HISTORICAL_SOURCE_UNAVAILABLE"
        ),
    }


def _sealed_horizon_plan(
    *,
    process_sha: str,
    anchor: dict[str, Any],
    cell: tuple[str, str, str],
    winner_symbol: str,
):
    symbol, timeframe, regime = cell
    tag = f"{anchor['anchor_id']}:{symbol}:{timeframe}:{regime}"
    available = symbol == winner_symbol and timeframe == "M5"
    body = {
        "schema_version": training_horizon._PLAN_SCHEMA,
        "authority_scope": training_horizon._AUTHORITY_SCOPE,
        "confers_trading_or_capital_authority": False,
        "confers_final_oos_authority": False,
        "anchor": {
            "anchor_id": anchor["anchor_id"],
            "selection_cutoff_utc": anchor["selection_cutoff_utc"],
            "process_precommit_receipt_sha256": process_sha,
        },
        "cell": {"symbol": symbol, "timeframe": timeframe, "regime": regime},
        "source_prefix_receipt_sha256": (
            _tag_sha(f"{tag}:source-prefix") if available else None
        ),
        "candidate_registry_sha256": _tag_sha(f"{tag}:candidates"),
        "trial_registry_sha256": _tag_sha(f"{tag}:trials"),
        "attempt_roster_sha256": _tag_sha(f"{tag}:attempts"),
        "evidence_roster_sha256": _tag_sha(f"{tag}:evidence"),
        "plan_status": (
            "READY_FOR_HISTORICAL_EVIDENCE"
            if available
            else "HISTORICAL_SOURCE_UNAVAILABLE"
        ),
    }
    return training_horizon._HistoricalHorizonPlan(body, training_horizon._PLAN_SEAL)


def _decompose_activity(
    source_anchor: dict[str, Any],
    *,
    book_cell_id: str,
    regime: str,
    strategy_ordinal: int,
    clocks: tuple[str, str, str, str],
) -> dict[str, dict[str, Any]]:
    initial = source_anchor["activity_ledger"]["initial_positions"]
    assert initial == []
    event_days = []
    fill_days = []
    mtm_days = []
    cost_days = []
    exposure_days = []
    margin_days = []
    for day in source_anchor["activity_ledger"]["days"]:
        date_text = day["date"]
        entry_events = []
        mark_events = []
        exit_events = []
        raw_entries = []
        raw_marks = []
        mark_costs = []
        raw_exits = []
        exposures = []
        for raw in day["entries"]:
            event_id = f"{raw['position_id']}:open-event"
            raw_entries.append(
                {
                    "event_id": event_id,
                    "position_id": raw["position_id"],
                    "cell_id": book_cell_id,
                    **{
                        key: raw[key]
                        for key in (
                            "opened_at_utc",
                            "side",
                            "quantity",
                            "contract_multiplier",
                            "stop_price",
                            "broker_fill_id",
                            "reference_mid_price",
                            "reference_bid_price",
                            "reference_ask_price",
                            "base_fill_price",
                            "p75_fill_price",
                        )
                    },
                }
            )
            decided = raw["opened_at_utc"]
            entry_events.append(
                _event_record(
                    event_id,
                    raw["position_id"],
                    book_cell_id,
                    regime,
                    "OPEN",
                    decided,
                )
            )
        for raw in day["position_marks"]:
            event_id = f"{raw['position_id']}:{date_text}:mark-event"
            mark_events.append(
                {"event_id": event_id, "position_id": raw["position_id"]}
            )
            raw_marks.append(
                {
                    "event_id": event_id,
                    **{
                        key: raw[key]
                        for key in (
                            "position_id",
                            "marked_at_utc",
                            "mark_price",
                            "mark_bid_price",
                            "mark_ask_price",
                        )
                    },
                }
            )
            mark_costs.append(
                {
                    "event_id": event_id,
                    "base_financing_cash": raw["base_financing_cash"],
                    "p75_financing_cash": raw["p75_financing_cash"],
                }
            )
            opening = next(
                value
                for value in day["entries"]
                if value["position_id"] == raw["position_id"]
            )
            exposures.append(
                {
                    "position_id": raw["position_id"],
                    "quantity": opening["quantity"],
                    "contract_multiplier": opening["contract_multiplier"],
                    "stop_price": opening["stop_price"],
                    "required_margin_cash": "10",
                }
            )
        for raw in day["exits"]:
            event_id = f"{raw['position_id']}:close-event"
            raw_exits.append(
                {
                    "event_id": event_id,
                    **{
                        key: raw[key]
                        for key in (
                            "position_id",
                            "closed_at_utc",
                            "broker_fill_id",
                            "reference_mid_price",
                            "reference_bid_price",
                            "reference_ask_price",
                            "base_fill_price",
                            "p75_fill_price",
                        )
                    },
                }
            )
            decided = raw["closed_at_utc"]
            exit_events.append(
                _event_record(
                    event_id,
                    raw["position_id"],
                    book_cell_id,
                    regime,
                    "CLOSE",
                    decided,
                )
            )
        equity = day["equity_basis_cash"]
        used = str(10 * len(exposures))
        free = str(Decimal(equity) - Decimal(used))
        event_days.append(
            {
                "date": date_text,
                "entries": entry_events,
                "marks": mark_events,
                "exits": exit_events,
            }
        )
        fill_days.append(
            {"date": date_text, "entries": raw_entries, "exits": raw_exits}
        )
        mtm_days.append(
            {
                "date": date_text,
                "equity_basis_cash": equity,
                "position_marks": raw_marks,
            }
        )
        cost_days.append(
            {
                "date": date_text,
                "marks": mark_costs,
            }
        )
        exposure_days.append({"date": date_text, "positions": exposures})
        margin_days.append(
            {
                "date": date_text,
                "equity_basis_cash": equity,
                "used_margin_cash": used,
                "free_margin_cash": free,
            }
        )
    common = {
        "anchor_id": source_anchor["anchor_id"],
        "forward_start_date": source_anchor["forward_start_date"],
        "forward_end_date": source_anchor["forward_end_date"],
    }
    event = {
        "schema_version": producer.EVENT_LEDGER_SCHEMA,
        **common,
        "selection_cutoff_utc": source_anchor["selection_cutoff_utc"],
        "evidence_access_started_at_utc": clocks[0],
        "execution_started_at_utc": clocks[1],
        "execution_completed_at_utc": clocks[2],
        "reconciled_at_utc": clocks[3],
        "specialists": [
            {
                "schema_version": producer.SPECIALIST_SCHEMA,
                "cell_id": book_cell_id,
                "strategy_module": "pm_strategies",
                "strategy_class": "MovingAverageCrossoverStrategy",
                "parameters": {
                    "short_window": 5 + strategy_ordinal,
                    "long_window": 20 + strategy_ordinal,
                    "risk_per_trade": "0.0025",
                },
                "refit_market_data_end_utc": source_anchor["selection_cutoff_utc"],
            }
        ],
        "initial_positions": [],
        "days": event_days,
    }
    return {
        "EVENT_LEDGER": event,
        "FILL_LEDGER": {
            "schema_version": producer.FILL_LEDGER_SCHEMA,
            **common,
            "initial_positions": [],
            "days": fill_days,
        },
        "MTM_LEDGER": {
            "schema_version": producer.MTM_LEDGER_SCHEMA,
            **common,
            "days": mtm_days,
        },
        "COST_LEDGER": {
            "schema_version": producer.COST_LEDGER_SCHEMA,
            **common,
            "days": cost_days,
        },
        "EXPOSURE_LEDGER": {
            "schema_version": producer.EXPOSURE_LEDGER_SCHEMA,
            **common,
            "days": exposure_days,
        },
        "MARGIN_LEDGER": {
            "schema_version": producer.MARGIN_LEDGER_SCHEMA,
            **common,
            "days": margin_days,
        },
    }


def _event_record(
    event_id: str,
    position_id: str,
    cell_id: str,
    regime: str,
    action: str,
    decided_at_utc: str,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "position_id": position_id,
        "cell_id": cell_id,
        "controller": {
            "schema_version": producer.CONTROLLER_EVENT_SCHEMA,
            "event_id": event_id,
            "cell_id": cell_id,
            "regime": regime,
            "decided_at_utc": decided_at_utc,
        },
        "arbitration": {
            "schema_version": producer.ARBITRATION_EVENT_SCHEMA,
            "event_id": event_id,
            "cell_id": cell_id,
            "decision": "ADMIT" if action == "OPEN" else "EXIT",
            "decided_at_utc": decided_at_utc,
        },
        "order": {
            "schema_version": producer.ORDER_EVENT_SCHEMA,
            "event_id": event_id,
            "position_id": position_id,
            "action": action,
            "decided_at_utc": decided_at_utc,
        },
    }


@dataclass(frozen=True)
class Corpus:
    """Owned clone of retained archive inputs; no issued forward capability."""

    context: spool.GenerationSpoolContext
    scratch_root: Path
    causal_precommit_sha256: str
    historical_precommit: historical.HistoricalAnchorPrecommit
    run_receipt: historical.HistoricalAnchorRunReceipt
    anchor_plans: tuple[dict[str, Any], ...]
    historical_artifacts: Any
    replay_sets: dict[str, producer.AnchorReplayArtifactDescriptors]
    replay_bodies: dict[str, dict[str, dict[str, Any]]]
    request: coordinator.HistoricalEvidenceRequest


_RETAINED_WORKSHOP_ROOT = Path(r"C:\Users\Bongo\OneDrive\Desktop\FxPM-Dev\audit_reports\fxpm_workshop_20261007")
_RETAINED_CAUSAL_ROOT = _RETAINED_WORKSHOP_ROOT / "frozen_tier_raw_account_verification/attempt_010_pytest_tmp/causal-producer0"
_RETAINED_CAUSAL_SPOOL = _RETAINED_CAUSAL_ROOT / "spool"
_RETAINED_NATIVE_SPOOL = _RETAINED_CAUSAL_ROOT / "historical-native/native-context/spool"
_PROTECTED_RETAINED_ROOTS = (
    _RETAINED_WORKSHOP_ROOT / "frozen_tier_raw_account_verification/attempt_009_pytest_tmp",
    _RETAINED_WORKSHOP_ROOT / "frozen_tier_raw_account_verification/attempt_010_pytest_tmp",
    _RETAINED_WORKSHOP_ROOT / "frozen_spread_missing_observation_verification/attempt_001_pytest_tmp",
    _RETAINED_WORKSHOP_ROOT / "bound_midnight_selected_owner_capture",
    _RETAINED_WORKSHOP_ROOT / "selected_owner_fixture_verification",
    _RETAINED_WORKSHOP_ROOT / "selected_owner_bound_context_preflight",
    _RETAINED_WORKSHOP_ROOT / "descriptor_raw_transport_verification/runtime/current_inputs_004",
)
_OWNED_MUTATION_PROTECTED_ROOTS = (*_PROTECTED_RETAINED_ROOTS,
    _RETAINED_WORKSHOP_ROOT / "descriptor_raw_transport_verification/runtime/current_transport_001",
)
_OWNED_TEST_SPOOL_ROOTS: set[Path] = set()


def _owned_spool_root(context) -> Path:
    root = Path(context.root).resolve(strict=True)
    assert root in _OWNED_TEST_SPOOL_ROOTS, "test mutation requires its registered scratch clone"
    assert not any(root.is_relative_to(path.resolve()) for path in _OWNED_MUTATION_PROTECTED_ROOTS)
    return root


def _owned_corpus_path(corpus: Corpus, relative: str) -> Path:
    root = _owned_spool_root(corpus.context)
    target = (root / relative).resolve()
    assert target.is_relative_to(root) and target.is_relative_to(corpus.scratch_root)
    return target


def _retained_json(context, digest: str):
    root = _owned_spool_root(context)
    relative = f"objects/sha256/{digest[:2]}/{digest}.canonical-json"
    path = (root / relative).resolve(strict=True)
    assert path.is_relative_to(root)
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == digest, "retained original object changed"
    descriptor = {"schema_version": "fxpm.authority-spool-artifact-descriptor.v1",
        "format": "canonical-json", "relative_path": relative,
        "sha256": digest, "size_bytes": len(raw)}
    body = producer._read_descriptor_json(context, descriptor, code="RETAINED_ORIGINAL_OBJECT")
    return body, raw, descriptor


def test_causal_v10_raw_fill_and_cost_contracts_are_exact() -> None:
    assert producer.FILL_LEDGER_SCHEMA == "fxpm.anchor-raw-fill-ledger.v2"
    assert producer.COST_LEDGER_SCHEMA == "fxpm.anchor-raw-cost-ledger.v3"
    assert producer._RAW_OPEN_FIELDS == frozenset(
        {
            "event_id",
            "position_id",
            "cell_id",
            "opened_at_utc",
            "side",
            "quantity",
            "contract_multiplier",
            "stop_price",
            "broker_fill_id",
            "reference_mid_price",
            "reference_bid_price",
            "reference_ask_price",
            "base_fill_price",
            "p75_fill_price",
        }
    )
    assert producer._RAW_CLOSE_FIELDS == frozenset(
        {
            "event_id",
            "position_id",
            "closed_at_utc",
            "broker_fill_id",
            "reference_mid_price",
            "reference_bid_price",
            "reference_ask_price",
            "base_fill_price",
            "p75_fill_price",
        }
    )
    assert producer._DAY_COST_FIELDS == frozenset({"date", "marks"})
    assert producer._MARK_COST_FIELDS == frozenset(
        {"event_id", "base_financing_cash", "p75_financing_cash"}
    )
    assert not hasattr(producer, "_OPEN_COST_FIELDS")
    assert {
        "base_commission_cash",
        "p75_commission_cash",
        "commission_per_entry_lot",
        "full_round_trip_commission_in_every_candidate_mark",
        "stop_risk_includes_adverse_configured_stop_slippage",
    }.isdisjoint(producer._FORWARD_PRODUCER_RECEIPT_FIELDS)


@pytest.mark.parametrize(
    ("retired_schema", "current_schema", "outer_fields", "day_fields", "extra"),
    (
        (
            "fxpm.anchor-raw-fill-ledger.v1",
            producer.FILL_LEDGER_SCHEMA,
            producer._FILL_LEDGER_FIELDS,
            producer._DAY_FILL_FIELDS,
            {"initial_positions": []},
        ),
        (
            "fxpm.anchor-raw-cost-ledger.v2",
            producer.COST_LEDGER_SCHEMA,
            producer._COMMON_LEDGER_FIELDS,
            producer._DAY_COST_FIELDS,
            {},
        ),
    ),
)
def test_retired_raw_lane_schema_cannot_claim_current_p75_shape(
    retired_schema: str,
    current_schema: str,
    outer_fields: frozenset[str],
    day_fields: frozenset[str],
    extra: dict[str, Any],
) -> None:
    anchor = {
        "anchor_id": "anchor-schema-test",
        "forward_start_date": "2020-01-01",
        "forward_end_date": "2020-01-01",
    }
    retired = {
        "schema_version": retired_schema,
        **anchor,
        **extra,
        "days": [
            {
                "date": "2020-01-01",
                **({"entries": [], "exits": []} if "entries" in day_fields else {}),
                **({"marks": []} if "marks" in day_fields else {}),
            }
        ],
    }
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="RAW_LEDGER_ANCHOR_BINDING_MISMATCH",
    ):
        producer._validate_common_ledger(
            retired,
            schema=current_schema,
            anchor=anchor,
            day_fields=day_fields,
            outer_fields=outer_fields,
        )


def test_production_precommit_freezes_real_code_policy_and_geometry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    schema = causal_fixture._build_corpus().precommit_payload
    plans = schema["process_definition"]["anchor_plan"]
    trusted_now = datetime.fromisoformat("2026-07-15T01:02:03+00:00")
    monkeypatch.setattr(producer, "_now_utc", lambda: trusted_now)

    context = spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="causal-precommit-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    search_process = search_planner.freeze_production_search_process(
        context,
        run_id="causal-precommit-run",
    )
    frozen = producer.freeze_causal_process_precommit_v10(
        context=context,
        search_process=search_process,
        process_id="production-causal-process-v10",
        anchors=plans,
        evidence_access_window_seconds=3 * 24 * 60 * 60,
    )
    body = frozen.to_dict()
    causal._validate_precommit_payload(body)

    assert body["authorized_at_utc"] == "2026-07-15T01:02:03Z"
    assert body["evidence_access_not_before_utc"] == "2026-07-15T01:02:03Z"
    assert frozen.receipt_sha256 == causal.canonical_sha256(body)
    component = frozen.identity_manifest["components"]["winner_selection_code_sha256"]
    for item in component["files"]:
        raw = (Path(__file__).parents[1] / item["relative_path"]).read_bytes()
        assert item["sha256"] == hashlib.sha256(raw).hexdigest()
    policies = body["process_definition"]["policy_identities"]
    assert policies["risk_policy_sha256"] == _sha(causal._FROZEN_RISK_POLICY)
    assert policies["cost_model_policy_sha256"] == _sha(causal._FROZEN_COST_POLICY)
    assert policies["trial_registry_policy_sha256"] == search_process.receipt_sha256
    assert frozen.identity_manifest["policies"]["trial_registry_policy_sha256"][
        "policy_body"
    ]["production_search_process_receipt_sha256"] == (search_process.receipt_sha256)

    wrong_context = spool.freeze_generation_context(
        (tmp_path / "wrong-spool").resolve(),
        generation_id="other-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="SEARCH_PROCESS_REVERIFY_FAILED",
    ):
        producer.freeze_causal_process_precommit_v10(
            context=wrong_context,
            search_process=search_process,
            process_id="production-causal-process-v10",
            anchors=plans,
        )
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="SEARCH_PROCESS_REVERIFY_FAILED",
    ):
        producer.freeze_causal_process_precommit_v10(
            context=context,
            search_process=search_process.receipt_sha256,
            process_id="production-causal-process-v10",
            anchors=plans,
        )


def test_historical_cross_binding_ascii_reader_is_exact_and_format_scoped(
    tmp_path: Path,
) -> None:
    context = spool.freeze_generation_context(
        (tmp_path / "cross-binding-format").resolve(),
        generation_id="cross-binding-format",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    sink = spool.open_symbol_sink(context, "AUDUSD")
    body = {
        "schema_version": historical_adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA,
        "mapping": [{"attempt_id": "attempt-001", "ordinal": 0}],
    }
    ascii_entry = sink.put_opaque(
        "format/ascii-cross-binding",
        "historical_horizon_winner_cross_binding",
        (_canonical(body),),
        format=adapter_fixture.raw_winner.ASCII_JSON_FORMAT,
    )
    descriptor = ascii_entry["descriptor"]
    assert (
        historical._read_ascii_descriptor_json(
            context,
            descriptor,
            maximum_bytes=context.limits.max_json_bytes,
        )
        == body
    )
    with pytest.raises(
        historical.HistoricalAnchorOrchestrationError,
        match="VERIFIED_ARTIFACT_FORMAT_OR_BOUND_INVALID",
    ):
        historical._read_canonical_descriptor_json(
            context,
            descriptor,
            maximum_bytes=context.limits.max_json_bytes,
        )

    canonical_entry = sink.put_json(
        "format/canonical-control",
        "canonical_control",
        body,
    )
    with pytest.raises(
        historical.HistoricalAnchorOrchestrationError,
        match="VERIFIED_ASCII_ARTIFACT_FORMAT_OR_BOUND_INVALID",
    ):
        historical._read_ascii_descriptor_json(
            context,
            canonical_entry["descriptor"],
            maximum_bytes=context.limits.max_json_bytes,
        )

    noncanonical_entry = sink.put_opaque(
        "format/noncanonical-ascii",
        "historical_horizon_winner_cross_binding",
        (b'{"schema_version":"x", "mapping":[]}',),
        format=adapter_fixture.raw_winner.ASCII_JSON_FORMAT,
    )
    with pytest.raises(
        historical.HistoricalAnchorOrchestrationError,
        match="VERIFIED_ASCII_ARTIFACT_CANONICAL_MISMATCH",
    ):
        historical._read_ascii_descriptor_json(
            context,
            noncanonical_entry["descriptor"],
            maximum_bytes=context.limits.max_json_bytes,
        )


def test_causal_factory_builds_forward_product_only_after_historical_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = causal_fixture._build_corpus().precommit_payload
    anchor_ids = [
        plan["anchor_id"] for plan in payload["process_definition"]["anchor_plan"]
    ]
    context = spool.freeze_generation_context(
        (tmp_path / "causal-factory").resolve(),
        generation_id="causal-factory-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    execution_product = object()
    valuation_product = object()
    execution = {anchor_id: {} for anchor_id in anchor_ids}
    valuation = {anchor_id: {} for anchor_id in anchor_ids}
    execution[anchor_ids[0]]["EURUSD|M5"] = execution_product
    valuation[anchor_ids[0]]["EURUSD|M5"] = valuation_product
    config = object()
    capital_phase_binding = object()
    monkeypatch.setattr(
        producer.capital_bootstrap,
        "verify_capital_forward_phase_binding",
        lambda observed_context, observed_binding: (
            {
                "bootstrap_identity_sha256": "c" * 64,
                "bootstrap_identity": {
                    "process_precommitment_sha256": causal.canonical_sha256(payload),
                    "process_definition_sha256": payload["process_definition_sha256"],
                },
            }
            if observed_context is context and observed_binding is capital_phase_binding
            else pytest.fail("capital phase binding was substituted")
        ),
    )
    factory = producer.BoundedCausalEvidenceFactory(
        context=context,
        causal_precommit_payload=payload,
        capital_phase_binding=capital_phase_binding,
        execution_products_by_anchor=execution,
        valuation_products_by_anchor=valuation,
        config=config,
    )
    execution[anchor_ids[0]].clear()
    valuation[anchor_ids[0]].clear()

    historical_precommit = object()
    historical_run = object()
    replay_precommit = object()
    forward_product = object()
    calls: list[str] = []

    def precommit(**kwargs):
        calls.append("precommit")
        assert kwargs["historical_precommit"] is historical_precommit
        assert kwargs["historical_run_receipt"] is historical_run
        assert (
            kwargs["execution_products_by_anchor"][anchor_ids[0]]["EURUSD|M5"]
            is execution_product
        )
        assert (
            kwargs["valuation_products_by_anchor"][anchor_ids[0]]["EURUSD|M5"]
            is valuation_product
        )
        assert kwargs["capital_phase_binding"] is capital_phase_binding
        assert kwargs["config"] is config
        return replay_precommit

    def produce(**kwargs):
        calls.append("produce")
        assert kwargs["replay_precommit"] is replay_precommit
        assert (
            kwargs["native_products_by_anchor"][anchor_ids[0]]["EURUSD|M5"]
            is execution_product
        )
        return forward_product

    def verify(observed_context, observed_product):
        calls.append("verify")
        assert observed_context is context
        assert observed_product is forward_product

    monkeypatch.setattr(forward, "precommit_forward_oos_replay_products", precommit)
    monkeypatch.setattr(forward, "produce_forward_oos_replay_artifacts", produce)
    monkeypatch.setattr(forward, "verify_forward_oos_replay_product", verify)
    callback = factory.build_after_historical_run(
        historical_precommit=historical_precommit,
        historical_run_receipt=historical_run,
    )

    assert calls == ["precommit", "produce", "verify"]
    assert type(callback) is producer.BoundedCausalEvidenceProducer
    assert callback.context is context
    assert callback.forward_replay_product is forward_product
    assert callback.causal_precommit_payload == payload
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="CAUSAL_EVIDENCE_FACTORY_ALREADY_CONSUMED",
    ):
        factory.build_after_historical_run(
            historical_precommit=historical_precommit,
            historical_run_receipt=historical_run,
        )


def _resolver_factory_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    suffix: str,
) -> tuple[
    dict[str, Any],
    list[str],
    spool.GenerationSpoolContext,
    object,
    object,
]:
    payload = copy.deepcopy(causal_fixture._build_corpus().precommit_payload)
    anchor_ids = [
        plan["anchor_id"] for plan in payload["process_definition"]["anchor_plan"]
    ]
    context = spool.freeze_generation_context(
        (tmp_path / f"causal-resolver-{suffix}").resolve(),
        generation_id=f"causal-resolver-{suffix}",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    capital_phase_binding = object()
    config = object()

    def verify_capital_binding(observed_context, observed_binding):
        if (
            observed_context is not context
            or observed_binding is not capital_phase_binding
        ):
            pytest.fail("capital phase binding was substituted")
        return {
            "bootstrap_identity_sha256": "c" * 64,
            "bootstrap_identity": {
                "process_precommitment_sha256": causal.canonical_sha256(payload),
                "process_definition_sha256": payload["process_definition_sha256"],
            },
        }

    monkeypatch.setattr(
        producer.capital_bootstrap,
        "verify_capital_forward_phase_binding",
        verify_capital_binding,
    )
    return payload, anchor_ids, context, capital_phase_binding, config


@pytest.mark.parametrize(
    ("source_mode", "error_code"),
    (
        ("NEITHER", "CAUSAL_FACTORY_PRODUCT_SOURCE_AMBIGUOUS"),
        ("BOTH", "CAUSAL_FACTORY_PRODUCT_SOURCE_AMBIGUOUS"),
        ("EXECUTION_ONLY", "CAUSAL_FACTORY_PRODUCT_ROSTERS_INCOMPLETE"),
        ("VALUATION_ONLY", "CAUSAL_FACTORY_PRODUCT_ROSTERS_INCOMPLETE"),
    ),
)
def test_causal_factory_requires_one_complete_forward_product_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_mode: str,
    error_code: str,
) -> None:
    payload, anchor_ids, context, capital_phase_binding, config = (
        _resolver_factory_inputs(
            tmp_path,
            monkeypatch,
            suffix=source_mode.lower(),
        )
    )
    execution = {anchor_id: {} for anchor_id in anchor_ids}
    valuation = {anchor_id: {} for anchor_id in anchor_ids}
    kwargs: dict[str, Any] = {}
    if source_mode in {"BOTH", "EXECUTION_ONLY"}:
        kwargs["execution_products_by_anchor"] = execution
    if source_mode in {"BOTH", "VALUATION_ONLY"}:
        kwargs["valuation_products_by_anchor"] = valuation
    if source_mode == "BOTH":
        kwargs["forward_product_resolver"] = lambda _request: None

    with pytest.raises(producer.CausalEvidenceProductionError, match=error_code):
        producer.BoundedCausalEvidenceFactory(
            context=context,
            causal_precommit_payload=payload,
            capital_phase_binding=capital_phase_binding,
            config=config,
            **kwargs,
        )


def _resolver_required_rosters(
    anchor_ids: list[str],
) -> dict[str, dict[str, list[str]]]:
    return {
        anchor_id: {
            "execution_product_keys": ["AUDUSD|D1", "EURUSD|H1"],
            "valuation_m5_product_keys": ["AUDUSD|M5", "EURUSD|M5"],
        }
        for anchor_id in anchor_ids
    }


def _resolver_products(
    required: dict[str, dict[str, list[str]]],
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    execution = {
        anchor_id: {key: object() for key in rosters["execution_product_keys"]}
        for anchor_id, rosters in required.items()
    }
    valuation = {
        anchor_id: {key: object() for key in rosters["valuation_m5_product_keys"]}
        for anchor_id, rosters in required.items()
    }
    return execution, valuation


def test_causal_factory_resolves_exact_winner_roster_once_after_book_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, anchor_ids, context, capital_phase_binding, config = (
        _resolver_factory_inputs(tmp_path, monkeypatch, suffix="success")
    )
    plans = copy.deepcopy(payload["process_definition"]["anchor_plan"])
    books = {anchor_id: object() for anchor_id in anchor_ids}
    required = _resolver_required_rosters(anchor_ids)
    execution, valuation = _resolver_products(required)
    historical_precommit = SimpleNamespace(receipt_sha256="d" * 64)
    historical_run = SimpleNamespace(receipt_sha256="e" * 64)
    replay_precommit = object()
    forward_product = object()
    calls: list[str] = []
    requests: list[producer.ForwardNativeProductRequest] = []

    def reopen_books(*args):
        calls.append("reopen")
        assert args == (context, historical_precommit, historical_run)
        return object(), plans, books, object()

    def required_rosters(observed_plans, observed_books):
        calls.append("required")
        assert observed_plans is plans
        assert observed_books is books
        return required

    def resolve(request):
        calls.append("resolver")
        requests.append(request)
        return producer.ForwardNativeProductResolution(execution, valuation)

    factory = producer.BoundedCausalEvidenceFactory(
        context=context,
        causal_precommit_payload=payload,
        capital_phase_binding=capital_phase_binding,
        forward_product_resolver=resolve,
        config=config,
    )

    def precommit(**kwargs):
        calls.append("precommit")
        assert kwargs["historical_precommit"] is historical_precommit
        assert kwargs["historical_run_receipt"] is historical_run
        assert kwargs["execution_products_by_anchor"] == execution
        assert kwargs["valuation_products_by_anchor"] == valuation
        return replay_precommit

    def produce(**kwargs):
        calls.append("produce")
        assert kwargs["replay_precommit"] is replay_precommit
        assert kwargs["native_products_by_anchor"] == execution
        assert kwargs["valuation_products_by_anchor"] == valuation
        return forward_product

    def verify(observed_context, observed_product):
        calls.append("verify")
        assert observed_context is context
        assert observed_product is forward_product

    monkeypatch.setattr(forward, "_reopen_books", reopen_books)
    monkeypatch.setattr(
        forward,
        "_required_forward_product_rosters",
        required_rosters,
    )
    monkeypatch.setattr(forward, "precommit_forward_oos_replay_products", precommit)
    monkeypatch.setattr(forward, "produce_forward_oos_replay_artifacts", produce)
    monkeypatch.setattr(forward, "verify_forward_oos_replay_product", verify)

    callback = factory.build_after_historical_run(
        historical_precommit=historical_precommit,
        historical_run_receipt=historical_run,
    )
    assert type(callback) is producer.BoundedCausalEvidenceProducer
    assert callback.forward_replay_product is forward_product
    assert calls == ["reopen", "required", "resolver", "precommit", "produce", "verify"]
    assert len(requests) == 1
    request_body = requests[0].to_dict()
    expected_anchor_rows = [
        {
            "anchor_id": plan["anchor_id"],
            "forward_end_exclusive_utc": (
                date.fromisoformat(plan["forward_end_date"]) + timedelta(days=1)
            ).isoformat()
            + "T00:00:00Z",
            "execution_product_keys": required[plan["anchor_id"]][
                "execution_product_keys"
            ],
            "valuation_m5_product_keys": required[plan["anchor_id"]][
                "valuation_m5_product_keys"
            ],
        }
        for plan in plans
    ]
    assert request_body["anchors"] == expected_anchor_rows
    assert request_body["historical_precommit_receipt_sha256"] == "d" * 64
    assert request_body["historical_run_receipt_sha256"] == "e" * 64
    assert request_body["required_roster_sha256"] == _sha(expected_anchor_rows)
    assert request_body["resolver_may_choose_winners_or_product_keys"] is False
    assert (
        request_body["native_products_must_be_sealed_before_forward_frame_access"]
        is True
    )
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="CAUSAL_EVIDENCE_FACTORY_ALREADY_CONSUMED",
    ):
        factory.build_after_historical_run(
            historical_precommit=historical_precommit,
            historical_run_receipt=historical_run,
        )
    assert len(requests) == 1


@pytest.mark.parametrize(
    ("mutation", "error_code"),
    (
        ("EXTRA_ANCHOR", "CAUSAL_FACTORY_FORWARD_PRODUCT_ANCHOR_ROSTER_INVALID"),
        ("MISSING_ANCHOR", "CAUSAL_FACTORY_FORWARD_PRODUCT_ANCHOR_ROSTER_INVALID"),
        ("REORDERED_ANCHORS", "CAUSAL_FACTORY_FORWARD_PRODUCT_ANCHOR_ROSTER_INVALID"),
        ("EXTRA_PRODUCT", "CAUSAL_FACTORY_FORWARD_PRODUCT_KEY_ROSTER_INVALID"),
        ("MISSING_PRODUCT", "CAUSAL_FACTORY_FORWARD_PRODUCT_KEY_ROSTER_INVALID"),
        ("REORDERED_PRODUCTS", "CAUSAL_FACTORY_FORWARD_PRODUCT_KEY_ROSTER_INVALID"),
    ),
)
def test_causal_factory_rejects_resolver_roster_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    error_code: str,
) -> None:
    payload, anchor_ids, context, capital_phase_binding, config = (
        _resolver_factory_inputs(tmp_path, monkeypatch, suffix=mutation.lower())
    )
    plans = copy.deepcopy(payload["process_definition"]["anchor_plan"])
    books = {anchor_id: object() for anchor_id in anchor_ids}
    required = _resolver_required_rosters(anchor_ids)
    historical_precommit = SimpleNamespace(receipt_sha256="d" * 64)
    historical_run = SimpleNamespace(receipt_sha256="e" * 64)
    resolver_calls = 0

    def resolve(_request):
        nonlocal resolver_calls
        resolver_calls += 1
        execution, valuation = _resolver_products(required)
        if mutation == "EXTRA_ANCHOR":
            execution["anchor-extra"] = {}
        elif mutation == "MISSING_ANCHOR":
            valuation.pop(anchor_ids[-1])
        elif mutation == "REORDERED_ANCHORS":
            execution = dict(reversed(list(execution.items())))
        elif mutation == "EXTRA_PRODUCT":
            execution[anchor_ids[0]]["GBPUSD|M15"] = object()
        elif mutation == "MISSING_PRODUCT":
            valuation[anchor_ids[0]].pop("AUDUSD|M5")
        elif mutation == "REORDERED_PRODUCTS":
            execution[anchor_ids[0]] = dict(
                reversed(list(execution[anchor_ids[0]].items()))
            )
        else:  # pragma: no cover - the parametrized roster is closed above.
            raise AssertionError(mutation)
        return producer.ForwardNativeProductResolution(execution, valuation)

    monkeypatch.setattr(
        forward,
        "_reopen_books",
        lambda *args: (object(), plans, books, object()),
    )
    monkeypatch.setattr(
        forward,
        "_required_forward_product_rosters",
        lambda observed_plans, observed_books: required,
    )
    factory = producer.BoundedCausalEvidenceFactory(
        context=context,
        causal_precommit_payload=payload,
        capital_phase_binding=capital_phase_binding,
        forward_product_resolver=resolve,
        config=config,
    )
    with pytest.raises(producer.CausalEvidenceProductionError, match=error_code):
        factory.build_after_historical_run(
            historical_precommit=historical_precommit,
            historical_run_receipt=historical_run,
        )
    assert resolver_calls == 1
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="CAUSAL_EVIDENCE_FACTORY_ALREADY_CONSUMED",
    ):
        factory.build_after_historical_run(
            historical_precommit=historical_precommit,
            historical_run_receipt=historical_run,
        )
    assert resolver_calls == 1


@pytest.mark.parametrize("postsearch", (False, True))
def test_insufficient_evidence_projection_preserves_authenticated_abstention(
    postsearch: bool,
) -> None:
    insufficient_group = book_fixture._insufficient_group(
        postsearch=postsearch,
        sibling_winner=False,
    )
    insufficient_evidence = insufficient_group[0]
    state_decision = insufficient_evidence.process_state_decision
    horizon_plan = insufficient_evidence.insufficient_horizon_plan
    assert state_decision is not None
    assert horizon_plan is not None
    decision = book.evaluate_historical_book_multiplicity(
        book_fixture._family(overrides=insufficient_group)
    )
    assert decision.valid
    book_body = decision.to_dict()
    plan_body = horizon_plan.to_dict()
    anchor = {
        "anchor_id": plan_body["anchor"]["anchor_id"],
        "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
    }
    bindings = []
    for ordinal, raw_cell in enumerate(book_body["cells"]):
        state = raw_cell["historical_state"]
        if ordinal == 0:
            plan_status = plan_body["plan_status"]
            plan_receipt = horizon_plan.receipt_sha256
        elif state in {"WINNER", "NO_WINNER"}:
            plan_status = "READY_FOR_HISTORICAL_EVIDENCE"
            plan_receipt = _tag_sha(f"projection-plan:{ordinal}")
        elif state == book.REGIME_UNOBSERVED:
            plan_status = "HISTORICAL_REGIME_UNOBSERVED"
            plan_receipt = _tag_sha(f"projection-unobserved:{ordinal}")
        else:
            assert state == "SOURCE_UNAVAILABLE"
            plan_status = "HISTORICAL_SOURCE_UNAVAILABLE"
            plan_receipt = _tag_sha(f"projection-terminal:{ordinal}")
        bindings.append(
            {
                "cell": {
                    "symbol": raw_cell["symbol"],
                    "timeframe": raw_cell["timeframe"],
                    "regime": raw_cell["regime"],
                },
                "plan_status": plan_status,
                "plan_receipt_sha256": plan_receipt,
            }
        )

    projected, _, _, _ = producer._causal_book_projection(
        anchor=anchor,
        book_body=book_body,
        horizon_bindings=bindings,
        specialists={},
    )
    projected_cell = projected[0]
    projected_decision = projected_cell["cell_decision"]
    expected_binding = state_decision.to_dict()["insufficient_evidence_binding"]
    causal._validate_insufficient_evidence_binding(
        expected_binding,
        cutoff_text=expected_binding["selection_cutoff_utc"],
        attempt_count=expected_binding["process_rejected_attempt_count"],
        horizon_receipt_sha256=horizon_plan.receipt_sha256,
        full_attempt_ledger_sha256=expected_binding[
            "process_rejection_attempt_ledger_sha256"
        ],
    )
    assert projected_cell["status"] == causal.INSUFFICIENT_EVIDENCE
    assert (
        projected_cell["attempt_count"]
        == (expected_binding["process_rejected_attempt_count"])
    )
    assert projected_decision["schema"] == "fxpm-anchor-local-cell-decision-v6"
    assert projected_decision["insufficient_evidence_binding"] == expected_binding
    assert projected_decision["horizon_receipt_sha256"] == (horizon_plan.receipt_sha256)
    assert (
        projected_decision["full_attempt_ledger_sha256"]
        == (expected_binding["process_rejection_attempt_ledger_sha256"])
    )
    assert projected_decision["winner_request_sha256"] is None
    assert projected_decision["anchor_refit_receipt_sha256"] is None


def test_terminal_technical_projection_is_complete_non_economic_abstention() -> None:
    technical_group = {
        ordinal: book_fixture._technical_terminal_cell(ordinal)  # noqa: SLF001
        for ordinal in range(len(book.REGIME_ORDER))
    }
    decision = book.evaluate_historical_book_multiplicity(
        book_fixture._family(overrides=technical_group)  # noqa: SLF001
    )
    assert decision.valid
    book_body = decision.to_dict()
    anchor = {
        "anchor_id": "terminal-technical-anchor",
        "selection_cutoff_utc": "2026-01-01T00:00:00Z",
    }
    bindings = [
        {
            "cell": {
                "symbol": raw_cell["symbol"],
                "timeframe": raw_cell["timeframe"],
                "regime": raw_cell["regime"],
            },
            "plan_status": (
                "HISTORICAL_TERMINAL_TECHNICAL"
                if raw_cell["historical_state"] == book.TERMINAL_TECHNICAL
                else "HISTORICAL_SOURCE_UNAVAILABLE"
            ),
            "plan_receipt_sha256": _tag_sha(f"terminal-plan:{ordinal}"),
        }
        for ordinal, raw_cell in enumerate(book_body["cells"])
    ]

    projected, supports, detectors, _ = producer._causal_book_projection(
        anchor=anchor,
        book_body=book_body,
        horizon_bindings=bindings,
        specialists={},
    )
    technical = projected[: len(book.REGIME_ORDER)]
    assert {cell["status"] for cell in technical} == {causal.TERMINAL_TECHNICAL}
    assert all(cell["attempt_count"] == 0 for cell in technical)
    assert all(cell["technical_failure"] is False for cell in technical)
    assert all(cell["terminal_source_unavailable"] is False for cell in technical)
    assert all(
        cell["cell_decision"]["source_state"] == causal.TERMINAL_TECHNICAL_SOURCE_STATE
        for cell in technical
    )
    assert all(
        cell["cell_decision"]["source_terminal_reason"]
        == causal.TERMINAL_TECHNICAL_REASON
        for cell in technical
    )
    assert all(
        cell["cell_decision"][key] is None
        for cell in technical
        for key in (
            "train_regime_support_receipt_sha256",
            "horizon_receipt_sha256",
            "full_attempt_ledger_sha256",
            "winner_request_sha256",
            "strategy_code_sha256",
            "parameters_sha256",
            "anchor_refit_receipt_sha256",
        )
    )
    causal._validate_cell_book(  # noqa: SLF001
        projected,
        supports,
        detectors,
        anchor["selection_cutoff_utc"],
        anchor["anchor_id"],
        causal._EvidenceIdentityRegistry(),  # noqa: SLF001
    )


def test_anchor_evidence_binds_exact_2400_cell_book_component() -> None:
    corpus = causal_fixture._build_corpus()
    process = copy.deepcopy(corpus.precommit_payload["process_definition"])
    process_sha = causal.canonical_sha256(corpus.precommit_payload)
    anchor = process["anchor_plan"][0]
    source_anchor = corpus.evidence_payload["anchors"][0]
    winner_symbol = book.PINNED_SYMBOL_ORDER[0]
    cells = tuple(causal.expected_cell_roster())
    bindings = [
        _binding(
            process_sha=process_sha,
            anchor=anchor,
            ordinal=ordinal,
            cell=cell,
            winner_symbol=winner_symbol,
        )
        for ordinal, cell in enumerate(cells)
    ]
    book_decision = book.evaluate_historical_book_multiplicity(
        _family(anchor, winner_symbol)
    )
    assert book_decision.valid
    winner_cell_id = book.compute_cell_id(winner_symbol, "M5", "TREND")
    assert winner_cell_id is not None
    ledgers = _decompose_activity(
        source_anchor,
        book_cell_id=winner_cell_id,
        regime="TREND",
        strategy_ordinal=0,
        clocks=(
            "2026-07-13T10:02:00Z",
            "2026-07-13T10:03:00Z",
            "2026-07-13T10:20:00Z",
            "2026-07-13T10:22:00Z",
        ),
    )
    evidence = producer._build_anchor_evidence(
        anchor=anchor,
        book_body=book_decision.to_dict(),
        horizons=bindings,
        ledgers=ledgers,
        descriptor_hashes={role: _sha(body) for role, body in ledgers.items()},
        process=process,
        forward_oos_replay_lineage=corpus.evidence_payload[
            "forward_oos_replay_lineage"
        ],
    )
    outputs = evidence["component_outputs"]
    assert outputs["exact_2400_cell_book_sha256"] == evidence["book_sha256"]
    causal._validate_anchor_evidence(
        evidence,
        anchor,
        corpus.evidence_payload["forward_oos_replay_lineage"],
        causal._EvidenceIdentityRegistry(),
        process["policy_identities"]["risk_policy_sha256"],
        process["policy_identities"]["cost_model_policy_sha256"],
        set(),
    )


@pytest.fixture(scope="module")
def valid_corpus(tmp_path_factory: pytest.TempPathFactory) -> Corpus:
    """Read original QA75 bytes; no old writer, issuer or new economic evidence."""
    retained = _RETAINED_CAUSAL_SPOOL.resolve(strict=True)
    assert _RETAINED_NATIVE_SPOOL.is_dir(), "retained original native context is missing"
    scratch = tmp_path_factory.mktemp("retained-causal-raw").resolve(strict=True)
    destination = (scratch / "spool").resolve()
    assert destination.is_relative_to(scratch) and not destination.is_relative_to(retained)
    assert not retained.is_relative_to(destination)
    shutil.copytree(retained, destination)
    context = spool.reopen_generation_context(destination,
        expected_generation_id="causal-evidence-generation-v1",
        expected_native_universe_sha256="c" * 64,
        expected_config_sha256="1b29cfd39e0af9a6ade7927a288fbcbb6418e23d519379240c79dd4e59c2b573")
    assert context.context_sha256 == "6568fd19d5388995ccd14acbc61930207856050573393e72813510aa1f85d622"
    _OWNED_TEST_SPOOL_ROOTS.add(destination)
    receipt_body, _, _ = _retained_json(context,
        "086028f05ae191a919933a03d390ab5ce6689b36cb6a23296134089ef07e6405")
    precommit_body, precommit_raw, precommit_descriptor = _retained_json(context,
        "de28367e5ffd39e1323236f623d5d8b1f1f21a4b49a95b2cab9ed277eb2551fe")
    run_body, run_raw, run_descriptor = _retained_json(context,
        "6a8698cf17ccb0a0a3d628bd0fdedc0501e36dd0a02e15e19f8baa271d85258a")
    forward_precommit, _, _ = _retained_json(context,
        "85885c5d04e212e2eede692d6e94b628cbf545dbbe194218d36c34ef4281b8b6")
    assert receipt_body["forward_replay_precommit_descriptor"]["sha256"] == (
        "85885c5d04e212e2eede692d6e94b628cbf545dbbe194218d36c34ef4281b8b6")
    causal_sha = "50fffedf2aa720fb7a8cf3d331c19e15d8ebb0fc4964486f3e22a3e504b2e87b"
    assert receipt_body["causal_precommit_payload_sha256"] == causal_sha
    assert forward_precommit["causal_precommit_payload_sha256"] == causal_sha
    historical_precommit = historical.HistoricalAnchorPrecommit(precommit_raw, _canonical(precommit_descriptor))
    run_receipt = historical.HistoricalAnchorRunReceipt(run_raw, _canonical(run_descriptor))
    assert receipt_body["historical_precommit_receipt_sha256"] == historical_precommit.receipt_sha256
    assert receipt_body["historical_run_receipt_sha256"] == run_receipt.receipt_sha256
    assert run_body["precommit_receipt_sha256"] == historical_precommit.receipt_sha256
    request_body = {
        "schema_version": coordinator.COORDINATOR_SCHEMA,
        "historical_run_receipt_sha256": run_receipt.receipt_sha256,
        "anchor_book_receipt_sha256_by_id": {
            row["anchor_id"]: row["book_decision_sha256"] for row in run_body["anchors"]},
        "causal_precommit_payload_sha256": causal_sha,
        "authority_claimed": False,
    }
    request = coordinator.HistoricalEvidenceRequest(_canonical(request_body))
    artifacts = producer._reopen_historical_artifacts(context, historical_precommit,
        run_receipt, request_body, causal_sha)
    anchors = tuple(precommit_body["anchors"])
    assert [row["anchor_id"] for row in anchors] == receipt_body["ordered_anchor_ids"]
    assert len(anchors) == 4
    replay_sets, replay_bodies = {}, {}
    for anchor in anchors:
        anchor_id = anchor["anchor_id"]
        rows = receipt_body["anchor_ledger_descriptors"][anchor_id]
        assert set(rows) == {"EVENT_LEDGER", "FILL_LEDGER", "MTM_LEDGER", "COST_LEDGER", "EXPOSURE_LEDGER", "MARGIN_LEDGER"}
        descriptors = producer.AnchorReplayArtifactDescriptors(
            event_ledger=rows["EVENT_LEDGER"], fill_ledger=rows["FILL_LEDGER"],
            mtm_ledger=rows["MTM_LEDGER"], cost_ledger=rows["COST_LEDGER"],
            exposure_ledger=rows["EXPOSURE_LEDGER"], margin_ledger=rows["MARGIN_LEDGER"])
        replay_sets[anchor_id] = descriptors
        replay_bodies[anchor_id], _ = producer._load_replay_ledgers(context, descriptors, anchor)
    return Corpus(context, scratch, causal_sha, historical_precommit, run_receipt,
        anchors, artifacts, replay_sets, replay_bodies, request)



def test_retained_four_anchor_raw_reconstruction_preserves_original_activity(valid_corpus: Corpus) -> None:
    # Coverage is archived raw/anchor reconstruction, not issued Forward/Capital or global causal PASS.
    assert len(valid_corpus.anchor_plans) == 4
    for anchor in valid_corpus.anchor_plans:
        anchor_id = anchor["anchor_id"]
        ledgers, hashes = producer._load_replay_ledgers(valid_corpus.context,
            valid_corpus.replay_sets[anchor_id], anchor)
        book_body = valid_corpus.historical_artifacts.book_by_anchor[anchor_id]
        specialists = producer._specialists(ledgers["EVENT_LEDGER"], book_body,
            anchor["selection_cutoff_utc"])
        cells, supports, detectors, _ = producer._causal_book_projection(
            anchor=anchor, book_body=book_body,
            horizon_bindings=valid_corpus.historical_artifacts.horizon_by_anchor[anchor_id],
            specialists=specialists)
        assert len(cells) == causal.CELL_COUNT
        assert len(supports) == len(detectors) == len(causal.SYMBOLS) * len(causal.TIMEFRAMES)
        assert [(cell["symbol"], cell["timeframe"], cell["regime"]) for cell in cells] == list(
            causal.expected_cell_roster()
        )
        identities = causal._EvidenceIdentityRegistry()
        book_sha = causal._validate_cell_book(cells, supports, detectors,
            anchor["selection_cutoff_utc"], anchor_id, identities)
        assert book_sha == causal.canonical_sha256({
            "cells": cells, "train_regime_supports": supports,
            "regime_detector_records": detectors,
        })
        activity, gross, base_cost, p75_cost = producer._assemble_activity(
            anchor=anchor, ledgers=ledgers, descriptor_hashes=hashes,
            projected_cells=cells, book_cells=book_body["cells"])
        dates = producer._expected_dates(anchor["forward_start_date"], anchor["forward_end_date"])
        completed, _, ledger_ids, _, activity_sha = causal._validate_activity_ledger(
            {"anchor_id": anchor_id, "book_cells": cells, "activity_ledger": activity},
            dates, gross, base_cost, p75_cost, identity_registry=identities)
        assert len(gross) == len(base_cost) == len(p75_cost) == len(dates)
        assert completed
        assert activity_sha == causal.canonical_sha256(activity)
        assert ledger_ids["event_ledger_sha256"] == activity_sha
        assert all(len(value) == 64 for value in ledger_ids.values())



def test_synthetic_archived_mode6_short_rebound_passes_causal_v10_prefix_rules() -> None:
    """Invented MODE6 archive-format payload; no current engine replay or authority."""
    prototype = forward_fixture._synthetic_archived_position()
    specialist = forward_fixture._specialist()
    multiplier = prototype.contract_multiplier
    base_entry, p75_entry = Decimal("0.9999"), Decimal("0.99935")
    exit_fill = Decimal("1.0021")
    # MODE6 short financing uses the entry price: 36% annual / 360 days.
    base_finance = multiplier * base_entry * Decimal("0.36") / 360
    p75_finance = multiplier * p75_entry * Decimal("0.36") / 360
    marks = tuple(
        replace(mark, base_financing_per_lot=base_finance,
                p75_financing_per_lot=p75_finance,
                candidate_net_pnl_per_lot=float(
                    (base_entry - mark.ask_price) * multiplier - base_finance * (index + 1)))
        for index, mark in enumerate(prototype.marks)
    )
    base_net = (base_entry - exit_fill) * multiplier - base_finance * len(marks)
    p75_net = (p75_entry - exit_fill) * multiplier - p75_finance * len(marks)
    candidate = replace(
        prototype.candidate, candidate_id="synthetic-archived-mode6-short",
        strategy_name="synthetic-archived-mode6-short", direction=-1,
        margin_per_lot=99990.,
        legs=(portfolio.ExitLeg(
            prototype.candidate.legs[-1].exit_time, 1., float(base_net),
            price_pnl_per_entry_lot=float((base_entry - exit_fill) * multiplier)),),
        marks=(portfolio.MarkPoint(
            prototype.candidate.marks[0].time, marks[0].candidate_net_pnl_per_lot),),
    ).normalized()
    position = replace(
        prototype, candidate=candidate, position_id=candidate.candidate_id,
        side="SHORT", stop_price=Decimal("1.01"),
        base_entry_fill=base_entry, p75_entry_fill=p75_entry,
        base_exit_fill=exit_fill, p75_exit_fill=exit_fill,
        margin_per_lot=Decimal("99990"), marks=marks,
    )
    volume = Decimal("0.01")
    replay = forward_fixture._ArchivedAnchorRawReplay(
        anchor=forward_fixture._anchor_for(position),
        specialists=(specialist,),
        positions=(position,),
        accepted_volume_by_position_id={position.position_id: volume},
    )
    ledgers = forward_fixture._legacy_archive_ledger_bodies(replay, starting_balance=Decimal("10000"))
    cell_sha = _tag_sha("mode6-causal-cell")
    projected_cell = {
        "cell_id": specialist.cell_id,
        "status": "WINNER",
        "cell_decision_sha256": cell_sha,
        "timeframe": "D1",
    }
    raw_cell = {"cell_id": specialist.cell_id, "regime": "TREND"}
    descriptor_hashes = {
        role: str(index) * 64
        for index, role in enumerate(
            (
                "EVENT_LEDGER",
                "FILL_LEDGER",
                "MTM_LEDGER",
                "COST_LEDGER",
                "EXPOSURE_LEDGER",
                "MARGIN_LEDGER",
            ),
            1,
        )
    }

    activity, gross, base_cost, p75_cost = producer._assemble_activity(
        anchor=replay.anchor,
        ledgers=ledgers,
        descriptor_hashes=descriptor_hashes,
        projected_cells=[projected_cell],
        book_cells=[raw_cell],
    )
    marks = [mark for day in activity["days"] for mark in day["position_marks"]]
    assert forward.REPLAY_PRECOMMIT_SCHEMA == "fxpm.forward-oos-replay-precommit.v5"
    assert causal.PROCESS_AUTHORITY_VERSION == "fxpm-offline-process-validation-v10"
    assert (base_finance, p75_finance) == (Decimal("99.99"), Decimal("99.935"))
    assert position.side == "SHORT"
    assert any(
        Decimal(mark["p75_financing_cash"])
        < Decimal(mark["base_financing_cash"])
        for mark in marks
    )
    assert p75_cost[1] > base_cost[1]
    assert p75_net < base_net
    for costs, expected_net in ((base_cost, base_net), (p75_cost, p75_net)):
        # Reconcile the produced ledger cash, allowing only return quantization.
        realized = sum((gross_value + cost_value) * Decimal(day["equity_basis_cash"])
                       for gross_value, cost_value, day in zip(gross, costs, activity["days"], strict=True))
        tolerance = sum(Decimal(day["equity_basis_cash"]) for day in activity["days"]) * causal._RETURN_QUANTUM
        assert abs(realized - expected_net * volume) <= tolerance
    cumulative_base = Decimal(0)
    cumulative_p75 = Decimal(0)
    for base_value, p75_value in zip(base_cost, p75_cost, strict=True):
        cumulative_base += base_value
        cumulative_p75 += p75_value
        assert cumulative_p75 <= cumulative_base + causal._RETURN_QUANTUM

    verifier_anchor = {
        "anchor_id": replay.anchor["anchor_id"],
        "book_cells": [projected_cell],
        "activity_ledger": activity,
    }
    completed, *_ = causal._validate_activity_ledger(
        verifier_anchor,
        [day["date"] for day in activity["days"]],
        gross,
        base_cost,
        p75_cost,
    )
    assert len(completed) == 1

    tampered_activity = copy.deepcopy(activity)
    tampered_activity["days"][0]["position_marks"][0][
        "p75_financing_cash"
    ] = "0"
    with pytest.raises(
        causal.ProcessVerificationError,
        match="p75 cumulative cost prefix improves on base",
    ):
        causal._validate_activity_ledger(
            {**verifier_anchor, "activity_ledger": tampered_activity},
            [day["date"] for day in tampered_activity["days"]],
            gross,
            base_cost,
            p75_cost,
        )

    masked_position = replace(
        position,
        position_id="btp:" + "9" * 64,
        candidate=replace(position.candidate, candidate_id="oos:" + "8" * 64),
    )
    masked_replay = replace(
        replay,
        positions=(position, masked_position),
        accepted_volume_by_position_id={
            position.position_id: volume,
            masked_position.position_id: volume,
        },
    )
    masked_ledgers = forward_fixture._legacy_archive_ledger_bodies(
        masked_replay, starting_balance=Decimal("10000")
    )
    first_mtm_marks = masked_ledgers["MTM_LEDGER"]["days"][0]["position_marks"]
    event_id_by_position = {
        mark["position_id"]: mark["event_id"] for mark in first_mtm_marks
    }
    first_cost_marks = {
        mark["event_id"]: mark
        for mark in masked_ledgers["COST_LEDGER"]["days"][0]["marks"]
    }
    first_cost_marks[event_id_by_position[position.position_id]][
        "p75_financing_cash"
    ] = "0"
    first_cost_marks[event_id_by_position[masked_position.position_id]][
        "p75_financing_cash"
    ] = "10"
    opening_entry = activity["days"][0]["entries"][0]
    base_open = producer._opening_execution_cost(opening_entry, p75=False)
    p75_open = producer._opening_execution_cost(opening_entry, p75=True)
    aggregate_base_prefix = base_open * 2 + sum(
        Decimal(mark["base_financing_cash"]) for mark in first_cost_marks.values()
    )
    aggregate_p75_prefix = p75_open * 2 + sum(
        Decimal(mark["p75_financing_cash"]) for mark in first_cost_marks.values()
    )
    assert aggregate_p75_prefix > aggregate_base_prefix
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="P75_POSITION_COST_PREFIX_IMPROVEMENT",
    ):
        producer._assemble_activity(
            anchor=masked_replay.anchor,
            ledgers=masked_ledgers,
            descriptor_hashes=descriptor_hashes,
            projected_cells=[projected_cell],
            book_cells=[raw_cell],
        )


def test_bare_replay_descriptors_cannot_bypass_issued_forward_product(
    valid_corpus: Corpus,
) -> None:
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED",
    ) as caught:
        producer._forward_oos_replay_lineage(
            context=valid_corpus.context,
            product=valid_corpus.replay_sets,
            historical_precommit=valid_corpus.historical_precommit,
            historical_run_receipt=valid_corpus.run_receipt,
            causal_precommit_payload={},
        )
    assert isinstance(caught.value.__cause__, forward.ForwardOOSReplayProductionError)
    assert caught.value.__cause__.code == "FORWARD_OOS_REPLAY_PRODUCT_NOT_ISSUED"


def test_missing_raw_cost_ledger_fails_typed(valid_corpus: Corpus) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    missing = dict(original.cost_ledger)
    missing["relative_path"] = "artifacts/missing-cost-ledger.json"
    replacement = replace(original, cost_ledger=missing)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="COST_LEDGER_ARTIFACT_UNAVAILABLE",
    ):
        producer._load_replay_ledgers(valid_corpus.context, replacement,
            valid_corpus.anchor_plans[0])


def test_descriptor_bound_cost_tamper_fails_before_returns(
    valid_corpus: Corpus,
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    descriptor = dict(original.cost_ledger)
    root = _owned_spool_root(valid_corpus.context)
    source = _owned_corpus_path(valid_corpus, descriptor["relative_path"])
    target = _owned_corpus_path(valid_corpus, "tampered/cost-ledger.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = source.read_bytes()
    target.write_bytes(raw[:-1] + (b" " if raw[-1:] != b" " else b"\n"))
    descriptor["relative_path"] = target.relative_to(root).as_posix()
    replacement = replace(original, cost_ledger=descriptor)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="COST_LEDGER_ARTIFACT_INTEGRITY_MISMATCH",
    ):
        producer._load_replay_ledgers(valid_corpus.context, replacement,
            valid_corpus.anchor_plans[0])


def test_future_refit_leakage_is_rejected_from_raw_specialist(
    valid_corpus: Corpus,
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    event = copy.deepcopy(valid_corpus.replay_bodies[anchor_id]["EVENT_LEDGER"])
    event["specialists"][0]["refit_market_data_end_utc"] = "2099-01-01T00:00:00Z"
    _owned_spool_root(valid_corpus.context)
    descriptor = spool.write_canonical_json_artifact(valid_corpus.context, event)
    replacement = replace(original, event_ledger=descriptor)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="SPECIALIST_REFIT_FUTURE_LEAKAGE",
    ):
        ledgers, _ = producer._load_replay_ledgers(valid_corpus.context, replacement,
            valid_corpus.anchor_plans[0])
        producer._specialists(ledgers["EVENT_LEDGER"],
            valid_corpus.historical_artifacts.book_by_anchor[anchor_id],
            valid_corpus.anchor_plans[0]["selection_cutoff_utc"])


def test_caller_return_vector_and_hash_fields_are_not_accepted(
    valid_corpus: Corpus,
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    mtm = copy.deepcopy(valid_corpus.replay_bodies[anchor_id]["MTM_LEDGER"])
    mtm["caller_daily_returns"] = ["0.99"]
    mtm["caller_return_sha256"] = "f" * 64
    _owned_spool_root(valid_corpus.context)
    descriptor = spool.write_canonical_json_artifact(valid_corpus.context, mtm)
    replacement = replace(original, mtm_ledger=descriptor)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="RAW_LEDGER_FIELDS_INVALID",
    ):
        producer._load_replay_ledgers(valid_corpus.context, replacement,
            valid_corpus.anchor_plans[0])


def test_coordinator_request_cannot_relabel_historical_book(
    valid_corpus: Corpus,
) -> None:
    body = valid_corpus.request.to_dict()
    first = next(iter(body["anchor_book_receipt_sha256_by_id"]))
    body["anchor_book_receipt_sha256_by_id"][first] = "0" * 64
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="COORDINATOR_BOOK_ROSTER_MISMATCH",
    ):
        producer._reopen_historical_artifacts(valid_corpus.context,
            valid_corpus.historical_precommit, valid_corpus.run_receipt,
            body, valid_corpus.causal_precommit_sha256)


@pytest.fixture
def frozen_raw_transport(completed_raw_transport, pytestconfig):
    state = getattr(pytestconfig, "_fxpm_lane127_completed_transport")
    assert state["transport"] is completed_raw_transport
    runtime_guard = state["runtime_guard"]
    assert callable(runtime_guard)
    root = Path(completed_raw_transport[0].root).resolve(strict=True)
    assert not any(root.is_relative_to(path.resolve()) for path in _OWNED_MUTATION_PROTECTED_ROOTS)
    _OWNED_TEST_SPOOL_ROOTS.add(root)
    try:
        with runtime_guard(completed_raw_transport):
            yield completed_raw_transport[:4]
    finally:
        _OWNED_TEST_SPOOL_ROOTS.discard(root)


def _reseal_frozen_raw(context, body):
    import pm_forward_oos_replay_producer as forward
    _owned_spool_root(context)
    descriptor = forward._write_raw_ledger(context, body, role="FROZEN_TIER_RAW_ACCOUNTS")
    return producer.FrozenTierRawAccountsArtifact(descriptor)


def _replace_synthetic_profile_source_binding(context, body):
    _owned_spool_root(context)
    descriptor = body["frozen_profile_descriptor"]
    profile = json.loads((Path(context.root) / descriptor["relative_path"]).read_bytes())
    tier = body["accounts"]["P75"]["tier"]
    cells = [cell for cell in profile["cells"]
             if (cell["symbol"], cell["timeframe"]) == (tier["symbol"], tier["timeframe"])]
    assert len(cells) == 1, "profile mutation requires the actual unique account cell"
    cell = cells[0]
    assert all(account["tier"]["source_binding"] == cell["source_binding"]
               and (account["tier"]["symbol"], account["tier"]["timeframe"])
               == (tier["symbol"], tier["timeframe"]) for account in body["accounts"].values())
    assert cell["source_binding"]["sidecar_sha256"] != "0" * 64
    cell["source_binding"]["sidecar_sha256"] = "0" * 64
    replacement = spool.write_opaque_artifact(context, (_canonical(profile),), format="json-profile")
    body["frozen_profile_descriptor"] = replacement
    for account in body["accounts"].values():
        account["tier"]["profile_sha256"] = replacement["sha256"]
        account["tier"]["source_binding"] = copy.deepcopy(cell["source_binding"])


def _malformed_synthetic_original_roster(context, body):
    # A malformed non-authorising input carrier, not an issued release receipt.
    _owned_spool_root(context)
    original = body["original_precommit_anchor"]
    original["execution_product_roster_sha256"] = "0" * 64
    global_body = forward._read_descriptor_json(context, body["original_global_precommit_descriptor"],
        code="SYNTHETIC_ORIGINAL_PRECOMMIT")
    global_body["anchor_product_identities"] = [copy.deepcopy(original)]
    global_body["anchor_product_identities_sha256"] = forward._sha(global_body["anchor_product_identities"])
    body["original_global_precommit_descriptor"] = forward._write_raw_ledger(
        context, global_body, role="SYNTHETIC_MALFORMED_ORIGINAL_ROSTER")


_LANE127_UNAVAILABLE_CAUSAL_WITNESSES = {
    "m5_bid": "v3 omits inline m5_marks; current one-bar capture has no held M5 mark witness",
    "m5_cash_offset": "v3 omits inline m5_marks; current one-bar capture has no held M5 cash/equity witness",
    "source_fine_row": "v3 omits inline spread_source_rows; no equivalent authenticated native-content mutation is scoped",
    "m5_drop_row": "v3 omits inline valuation_rows; no equivalent authenticated native-row deletion is scoped",
    "filled_entry_bar": "original entry-bar corruption requires a genuine captured missing-fine-close entry witness",
    "filled_lots": "precise BACKTEST_POSITION_CASH_RECONCILIATION_FAILED target remains unproved for the current capture",
    "filled_financing_posting": "current one-bar capture has no SWAP transaction witness",
    "holding_m5_mark": "v3 omits inline m5_marks and current one-bar capture has no held M5 inventory witness",
    "source_observation": "v3 omits inline source_observations; no equivalent authenticated source-value mutation is scoped",
    "account_observation_axis": "v3 omits inline source_rows; no equivalent authenticated execution-axis mutation is scoped",
    "execution_source_row": "v3 omits inline source_rows; no equivalent authenticated native-price mutation is scoped",
    "missing_mask_admitted": "original integration obligation requires a genuine missing-observation admission witness",
    "actual_close_invented": "v3 omits inline source_observations; original missing-observation timestamp witness is absent",
    "expected_close_changed": "v3 omits inline source_observations; no equivalent expected-close mutation is scoped",
    "missing_points_invented": "v3 omits inline source_observations; original missing-observation points witness is absent",
}


def _lane127_causal_case(mutation, expected):
    reason = _LANE127_UNAVAILABLE_CAUSAL_WITNESSES[mutation]
    return pytest.param(mutation, expected, marks=forward_fixture._lane127_blocked(reason))


@pytest.mark.parametrize("mutation,expected", [
    ("interior_cash_offset", "FROZEN_TIER_DERIVED_STATE_CHANGED"),
    ("interior_engine_offset", "FROZEN_TIER_INTERIOR_EQUITY_RECONCILIATION_FAILED"),
    _lane127_causal_case("m5_bid", "FROZEN_TIER_M5_EXECUTABLE_MARK_CHANGED"),
    _lane127_causal_case("m5_cash_offset", "FROZEN_TIER_M5_EXECUTABLE_MARK_CHANGED"),
    _lane127_causal_case("source_fine_row", "FROZEN_TIER_FINE_SOURCE_ROWS_CHANGED"),
    ("profile_tier_width", "FROZEN_TIER_RAW_ACCOUNT_INVALID"),
    ("genesis_carry", "FROZEN_TIER_ACCOUNT_GENESIS_OR_TERMS_SPLICE"),
    ("book_binding", "FROZEN_TIER_ORIGINAL_BOOK_BINDING_INVALID"),
    ("caller_authentication", "FROZEN_TIER_ACCOUNT_GENESIS_OR_TERMS_SPLICE"),
    ("release_authority", "FROZEN_TIER_RAW_BINDING_INVALID"),
    ("window", "FROZEN_TIER_ORIGINAL_WINDOW_BINDING_CHANGED"),
    ("phase", "FROZEN_TIER_ORIGINAL_WINDOW_BINDING_CHANGED"),
    ("warmup", "FROZEN_TIER_RETAINED_WINDOW_OR_WARMUP_CHANGED"),
    ("execution_roster", "FROZEN_TIER_USED_SOURCE_PRECOMMIT_SPLICE"),
    ("fine_roster", "FROZEN_TIER_USED_SOURCE_PRECOMMIT_SPLICE"),
    ("m5_roster", "FROZEN_TIER_USED_M5_SOURCE_PRECOMMIT_SPLICE"),
    _lane127_causal_case("m5_drop_row", "FROZEN_TIER_M5_RETAINED_SOURCE_ROWS_CHANGED"),
    ("m5_cost_metadata", "FROZEN_TIER_M5_COST_SURFACE_CHANGED"),
    ("parameters", "FROZEN_TIER_ORIGINAL_OWNER_PROVENANCE_CHANGED"),
    ("source_count", "FROZEN_TIER_ORIGINAL_OWNER_PROVENANCE_CHANGED"),
    ("detector", "FROZEN_TIER_ORIGINAL_OWNER_PROVENANCE_CHANGED"),
    ("financing_contract", "FROZEN_TIER_ORIGINAL_FINANCING_MODEL_CHANGED"),
    ("execution_spread_model", "FROZEN_TIER_EXECUTION_SPREAD_MODEL_CHANGED"),
    ("entry_admission_mask", "FROZEN_TIER_ENTRY_ADMISSION_BINDING_CHANGED"),
    ("null_engine_equity", "FROZEN_TIER_ENGINE_EQUITY_MARK_UNAVAILABLE"),
    ("filled_position_event", "EVENTS_V2_RAW_REVERIFY_FAILED"),
    # Feb5 entry moved to Feb6 hits the captured fine-close gap before financing.
    _lane127_causal_case("filled_entry_bar", "FROZEN_TIER_TRADE_ADMISSION_RECONCILIATION_FAILED"),
    _lane127_causal_case("filled_lots", "BACKTEST_POSITION_CASH_RECONCILIATION_FAILED"),
    ("filled_margin", "FROZEN_TIER_DERIVED_STATE_CHANGED"),
    ("filled_stop_risk", "FROZEN_TIER_DERIVED_STATE_CHANGED"),
    _lane127_causal_case("filled_financing_posting", "FROZEN_TIER_DERIVED_STATE_CHANGED"),
    _lane127_causal_case("holding_m5_mark", "FROZEN_TIER_M5_EXECUTABLE_MARK_CHANGED"),
    ("original_config", "FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_CHANGED"),
    ("deployment_source", "FROZEN_TIER_DEPLOYMENT_SOURCE_PRECOMMIT_SPLICE"),
    ("original_source_roster", "FROZEN_TIER_ORIGINAL_SOURCE_ROSTER_CHANGED"),
    # Descriptor verification refuses this corruption before the later
    # PROFILE_ARTIFACT_CHANGED cross-read guard, without an invented IO race.
    ("profile_artifact_identity", "FROZEN_TIER_RAW_ACCOUNT_INVALID"),
    ("profile_source_binding", "FROZEN_TIER_PROFILE_SOURCE_BINDING_MISMATCH"),
    ("profile_pair", "FROZEN_TIER_PROFILE_PAIR_SPLICE"),
    ("execution_spec_receipt", "FROZEN_TIER_EXECUTION_SPEC_RECEIPT_CHANGED"),
    ("m5_execution_spec_receipt", "FROZEN_TIER_M5_EXECUTION_SPEC_RECEIPT_CHANGED"),
    ("m5_target_terms", "FROZEN_TIER_M5_TARGET_CELL_OR_TERMS_SPLICE"),
    ("book_owner", "FROZEN_TIER_ORIGINAL_BOOK_OWNER_MISMATCH"),
    _lane127_causal_case("source_observation", "FROZEN_TIER_SOURCE_OBSERVATIONS_CHANGED"),
    _lane127_causal_case("account_observation_axis", "FROZEN_TIER_ACCOUNT_OBSERVATION_AXIS_SPLICE"),
    ("trade_admission", "FROZEN_TIER_TRADE_ADMISSION_RECONCILIATION_FAILED"),
    _lane127_causal_case("execution_source_row", "FROZEN_TIER_EXECUTION_SOURCE_ROWS_CHANGED"),
    ("unknown_raw_version", "FROZEN_TIER_RAW_SCHEMA_UNSUPPORTED"),
    pytest.param("current_packet_relabelled_v1", "FROZEN_TIER_RAW_FIELDS_INVALID",
                 id="current_packet_relabelled_v1-FROZEN_TIER_SOURCE_OBSERVATIONS_CHANGED"),
    ("old_admission_in_v2", "FROZEN_TIER_ADMISSION_VERSION_MISMATCH"),
    ("old_model_in_v2", "FROZEN_TIER_EXECUTION_SPREAD_MODEL_CHANGED"),
    ("missing_executed_leg", "FROZEN_TIER_ADMISSION_COUNTS_INVALID"),
    ("missing_refusal_counter", "FROZEN_TIER_ADMISSION_COUNTS_INVALID"),
    ("all_opportunity_counts_zero", "FROZEN_TIER_ADMISSION_COUNTS_INVALID"),
    ("rebalanced_opportunity_counts", "FROZEN_TIER_ADMISSION_COUNTS_INVALID"),
    _lane127_causal_case("missing_mask_admitted", "FROZEN_TIER_ENTRY_ADMISSION_BINDING_CHANGED"),
    _lane127_causal_case("actual_close_invented", "FROZEN_TIER_SOURCE_OBSERVATIONS_CHANGED"),
    _lane127_causal_case("expected_close_changed", "FROZEN_TIER_SOURCE_OBSERVATIONS_CHANGED"),
    _lane127_causal_case("missing_points_invented", "FROZEN_TIER_SOURCE_OBSERVATIONS_CHANGED"),
])
def test_frozen_raw_reopened_account_rejects_resealed_tamper(frozen_raw_transport, mutation, expected):
    import copy
    reason = _LANE127_UNAVAILABLE_CAUSAL_WITNESSES.get(mutation)
    if reason is not None:
        raise forward_fixture._Lane127WitnessUnavailable(reason)
    context, anchor, original, _ = frozen_raw_transport
    _owned_spool_root(context)
    body = copy.deepcopy(original)
    account = body["accounts"]["P95"]
    terminal = copy.deepcopy(account["derived"]["cash_inventory"][-1])
    if mutation == "interior_cash_offset":
        account["derived"]["cash_inventory"][0]["cash"] += 25
        account["derived"]["cash_inventory"][0]["derived_equity"] += 25
        assert account["derived"]["cash_inventory"][-1] == terminal
    elif mutation == "interior_engine_offset":
        account["engine_equity"][account["warmup_bars"]] += 25
        account["derived"]["cash_inventory"][0]["cash"] += 25
        account["derived"]["cash_inventory"][0]["derived_equity"] += 25
        assert account["derived"]["cash_inventory"][-1] == terminal
    elif mutation == "m5_bid":
        account["m5_marks"][0]["bid"] += 0.0001
    elif mutation == "m5_cash_offset":
        account["m5_marks"][0]["cash"] += 25
        account["m5_marks"][0]["equity"] += 25
    elif mutation == "source_fine_row":
        body["spread_source_rows"].pop()
    elif mutation == "profile_tier_width":
        account["tier"]["spread_points"] += 1
        account["tier"]["full_spread_price"] = account["tier"]["spread_points"] * account["tier"]["point_price"]
    elif mutation == "genesis_carry":
        body["genesis"]["initial_inventory"] = [{"lots": 1}]
    elif mutation == "book_binding":
        body["original_precommit_anchor"]["full_book_sha256"] = "0" * 64
    elif mutation == "caller_authentication":
        account["engine_spread_admission"]["input_provenance_authenticated"] = True
    elif mutation == "release_authority":
        body["release_authority_status"] = "AUTHENTICATED_RELEASE"
    elif mutation == "window":
        body["capital_window"]["end_date"] = (date.fromisoformat(body["capital_window"]["end_date"]) + timedelta(days=1)).isoformat()
    elif mutation == "phase":
        body["capital_phase"] = "INNER_SELECTION"
    elif mutation == "warmup":
        account["warmup_bars"] += 1
    elif mutation == "execution_roster":
        body["execution_source"]["role"] += "-substituted"
    elif mutation == "fine_roster":
        body["spread_source"]["source_audit_sidecar_sha256"] = "0" * 64
    elif mutation == "m5_roster":
        account["valuation_source"]["native_product_identity"]["role"] += "-substituted"
    elif mutation == "m5_drop_row":
        account["valuation_rows"].pop()
    elif mutation == "m5_cost_metadata":
        account["valuation_source"]["spread_and_cost_surface"]["valuation_price_field"] = "Close"
    elif mutation == "parameters":
        parameters = body["owner_provenance"]["parameters"]
        parameters["sl_atr_mult"] = parameters.get("sl_atr_mult", 0.0) + 1.0
    elif mutation == "source_count":
        body["owner_provenance"]["source_count_receipt"]["source_bar_count"] += 1
    elif mutation == "detector":
        body["owner_provenance"]["detector_parameters_sha256"] = "0" * 64
    elif mutation == "financing_contract":
        account["financing_model"]["contract"]["reason"] = "substituted metadata"
    elif mutation == "execution_spread_model":
        account["engine_spread_model"]["entry_observations_sha256"] = "0" * 64
    elif mutation == "entry_admission_mask":
        account["engine_spread_admission"]["entry_admission_mask_sha256"] = "0" * 64
    elif mutation == "null_engine_equity":
        account["engine_equity"][0] = None
    elif mutation == "filled_position_event":
        assert account["trades"] and account["position_events"]
        before = copy.deepcopy(account["position_events"])
        account["position_events"][0]["risk_amount"] += 1
        assert sum(left != right for left, right in zip(before, account["position_events"], strict=True)) == 1
    elif mutation == "filled_entry_bar":
        assert len(account["trades"]) == 1
        account["trades"][0]["entry_bar"] += 1
    elif mutation == "filled_lots":
        assert len(account["trades"]) == 1
        account["trades"][0]["position_size"] += account["instrument_spec"]["volume_step"]
    elif mutation == "filled_margin":
        positions = account["derived"]["positions"]
        assert positions
        before = copy.deepcopy(positions)
        positions[0]["margin_amount"] /= 100
        assert sum(left != right for left, right in zip(before, positions, strict=True)) == 1
    elif mutation == "filled_stop_risk":
        positions = account["derived"]["positions"]
        assert positions
        before = copy.deepcopy(positions)
        positions[0]["gross_stop_risk_per_lot"] += 1
        assert sum(left != right for left, right in zip(before, positions, strict=True)) == 1
    elif mutation == "filled_financing_posting":
        posting = next(row for row in account["derived"]["transactions"] if row["kind"] == "SWAP")
        posting["cash"] += 1
    elif mutation == "holding_m5_mark":
        mark = next(row for row in account["m5_marks"] if row["state"] == "AVAILABLE" and row["inventory"])
        mark["inventory"][0]["lots"] += account["instrument_spec"]["volume_step"]
    elif mutation == "original_config":
        body["pipeline_config_binding"]["effective_config_sha256"] = "0" * 64
    elif mutation == "deployment_source":
        descriptor = body["deployment_source_descriptors"]["broker"]
        raw = (Path(context.root) / descriptor["relative_path"]).read_bytes()
        body["deployment_source_descriptors"]["broker"] = spool.write_opaque_artifact(
            context, (raw + b"\n",), format="json-broker")
    elif mutation == "original_source_roster":
        _malformed_synthetic_original_roster(context, body)
    elif mutation == "profile_artifact_identity":
        body["frozen_profile_descriptor"]["sha256"] = "0" * 64
    elif mutation == "profile_source_binding":
        _replace_synthetic_profile_source_binding(context, body)
    elif mutation == "profile_pair":
        account["tier"]["profile_sha256"] = "0" * 64
    elif mutation == "execution_spec_receipt":
        body["execution_spec_receipt"]["receipt_sha256"] = "0" * 64
    elif mutation == "m5_execution_spec_receipt":
        account["valuation_source"]["execution_spec_receipt"]["receipt_sha256"] = "0" * 64
    elif mutation == "m5_target_terms":
        account["valuation_source"]["execution_spec_receipt"]["instrument_spec"]["point"] *= 2
    elif mutation == "book_owner":
        body["cell_projection"]["process_sha256"] = "0" * 64
    elif mutation == "source_observation":
        next(row for row in body["source_observations"] if row["points"] is not None)["points"] += 1
    elif mutation == "account_observation_axis":
        account["source_rows"][0]["open_utc"] = account["source_rows"][1]["open_utc"]
    elif mutation == "trade_admission":
        assert account["trades"]
        before = copy.deepcopy(account["trades"])
        assert account["trades"][0]["entry_spread_admission"] == "ADMIT"
        account["trades"][0]["entry_spread_admission"] = "MISSING"
        assert sum(left != right for left, right in zip(before, account["trades"], strict=True)) == 1
    elif mutation == "execution_source_row":
        account["source_rows"][account["warmup_bars"]]["open"] += 0.00001
    elif mutation == "unknown_raw_version":
        body["schema_version"] = "fxpm.frozen-tier-raw-accounts.v99"
    elif mutation == "current_packet_relabelled_v1":
        body["schema_version"] = forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA
    elif mutation == "old_admission_in_v2":
        admission = account["engine_spread_admission"]
        admission.pop("schema_version")
        admission.pop("entry_admission_policy")
        admission["untested_missing_observation_opportunities"] = admission.pop("missing_refused_opportunities")
        admission["untested_executed_legs"] = admission.pop("missing_executed_legs")
    elif mutation == "old_model_in_v2":
        account["engine_spread_model"].pop("entry_admission_schema")
        account["engine_spread_model"]["entry_admission_policy"] = "EXACT_PRIOR_FINE_CLOSE_CAP_MISSING_UNTESTED"
    elif mutation == "missing_executed_leg":
        account["engine_spread_admission"]["missing_executed_legs"] = 1
    elif mutation == "missing_refusal_counter":
        account["engine_spread_admission"]["missing_refused_opportunities"] += 1
    elif mutation == "all_opportunity_counts_zero":
        for field in ("signal_opportunities", "known_cap_pass_opportunities", "known_cap_refused_opportunities", "missing_refused_opportunities"):
            account["engine_spread_admission"][field] = 0
    elif mutation == "rebalanced_opportunity_counts":
        admission = account["engine_spread_admission"]
        assert admission["known_cap_pass_opportunities"] > 0
        fields = ("known_cap_pass_opportunities", "known_cap_refused_opportunities", "missing_refused_opportunities")
        total = sum(admission[field] for field in fields)
        admission["known_cap_pass_opportunities"] -= 1
        admission["missing_refused_opportunities"] += 1
        assert sum(admission[field] for field in fields) == total
    elif mutation == "missing_mask_admitted":
        import numpy as np
        points = [float("nan") if row["points"] is None else row["points"] for row in body["source_observations"]]
        old_allowed = np.array([not value > account["tier"]["spread_points"] for value in points], dtype=np.uint8)
        account["engine_spread_admission"]["entry_admission_mask_sha256"] = hashlib.sha256(old_allowed.tobytes()).hexdigest()
    elif mutation == "actual_close_invented":
        body["source_observations"][0]["prior_fine_close_utc"] = body["source_observations"][0]["target_open_utc"]
    elif mutation == "expected_close_changed":
        body["source_observations"][-1]["expected_prior_fine_close_utc"] = body["source_observations"][-2]["target_open_utc"]
    elif mutation == "missing_points_invented":
        body["source_observations"][-1]["points"] = 12.0
    else:
        raise AssertionError(f"unhandled controlled mutation: {mutation}")
    assert _sha(body) != _sha(original), "controlled mutation must change the canonical packet"
    artifact = _reseal_frozen_raw(context, body)
    expected_code = {"filled_position_event": "BACKTEST_POSITION_EVENTS_V2_RAW_REVERIFY_FAILED"}.get(mutation, expected)
    with pytest.raises(producer.CausalEvidenceProductionError) as caught:
        producer._read_frozen_tier_raw_accounts(context, artifact, anchor)
    assert caught.value.code == expected_code


def _archived_frozen_v1_admission_snapshot():
    """Immutable synthetic attempt009 metadata, not output from a current run.

    Original raw packet 77ec9522a0dc84d7b321bfb21f98fb6ae303957107124f29ddcc42a11a89512b,
    frozen_tier_raw_account_verification/attempt_009, P75. This bounded snapshot
    tests archive interpretation; the retained original run proves its full graph.
    """
    import numpy as np
    import pandas as pd
    axis = pd.date_range("2018-01-01T00:00:00Z", "2020-02-07T00:00:00Z", freq="D", inclusive="left")
    values = np.full(767, np.nan)
    values[-2:] = 12.0
    observations = pd.Series(values, index=axis, name="EntrySpreadPoints")
    account = {"warmup_bars": 765,
        "engine_spread_model": {"entry_admission_policy": "EXACT_PRIOR_FINE_CLOSE_CAP_MISSING_UNTESTED"},
        "engine_spread_admission": {
            "entry_admission_mask_sha256": "a7d557d3f055585e25404d99094725149dedf302f3a75c567a44ea5a4545695a",
            "entry_observations_sha256": "c6770ca7dcc4ab1fe8557babbe2552d5c9dc1bc3e5e073ef17826f23a7251b68",
            "input_provenance_authenticated": False, "qualification_authority": False,
            "signal_opportunities": 1, "known_cap_pass_opportunities": 0,
            "known_cap_refused_opportunities": 1, "untested_executed_legs": 0,
            "untested_missing_observation_opportunities": 0}}
    return account, observations, SimpleNamespace(spread_points=9)


def test_archived_raw_v1_admission_snapshot_keeps_original_hashes_and_fields():
    account, observations, tier = _archived_frozen_v1_admission_snapshot()
    before = copy.deepcopy(account)
    producer._verify_frozen_spread_admission(schema=forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
        account=account, observations=observations, tier=tier)
    assert account == before
    assert "entry_admission_schema" not in account["engine_spread_model"]
    assert "schema_version" not in account["engine_spread_admission"]
    assert account["engine_spread_admission"]["untested_executed_legs"] == 0


def test_archived_raw_v1_still_refuses_a_missing_scored_observation():
    import numpy as np
    import pandas as pd
    account, observations, tier = _archived_frozen_v1_admission_snapshot()
    observations.iloc[-1] = np.nan
    admission = account["engine_spread_admission"]
    admission["entry_observations_sha256"] = hashlib.sha256(
        pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest()
    admission["entry_admission_mask_sha256"] = hashlib.sha256(
        np.array([not value > 9 for value in observations], dtype=np.uint8).tobytes()).hexdigest()
    with pytest.raises(producer.CausalEvidenceProductionError, match="FROZEN_TIER_PRIOR_FINE_ROW_UNAVAILABLE"):
        producer._verify_frozen_spread_admission(schema=forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
            account=account, observations=observations, tier=tier)


@pytest.mark.parametrize("schema", ["fxpm.frozen-tier-raw-accounts.v99", "fxpm.frozen-tier-raw-accounts.v2"])
def test_archived_raw_v1_snapshot_cannot_claim_unknown_or_current_admission(schema):
    account, observations, tier = _archived_frozen_v1_admission_snapshot()
    code = "FROZEN_TIER_RAW_SCHEMA_UNSUPPORTED" if schema.endswith("v99") else "FROZEN_TIER_ADMISSION_VERSION_MISMATCH"
    with pytest.raises(producer.CausalEvidenceProductionError, match=code):
        producer._verify_frozen_spread_admission(schema=schema, account=account, observations=observations, tier=tier)


def test_frozen_raw_projection_cannot_enter_legacy_daily_qualifier(frozen_raw_transport, monkeypatch):
    context, anchor, _, artifact = frozen_raw_transport
    def forbidden(*args, **kwargs):
        pytest.fail("new raw accounts entered the retired daily qualification path")
    monkeypatch.setattr(producer, "_validate_common_ledger", forbidden)
    with pytest.raises(producer.CausalEvidenceProductionError,
                       match="FROZEN_TIER_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"):
        producer._load_replay_ledgers(context, artifact, anchor)


@pytest.mark.parametrize("live_state", ["denied", "missing"])
def test_frozen_reader_reconstructs_original_config_and_specs_from_spooled_bytes(frozen_raw_transport, live_state, monkeypatch):
    context, anchor, body, artifact = frozen_raw_transport
    original = forward._read_descriptor_json(context, body["original_global_precommit_descriptor"],
        code="SYNTHETIC_ORIGINAL_PRECOMMIT")
    protected = {Path(original["deployment_binding"][field]).resolve()
        for field in ("deployment_config_path", "broker_specs_path")}
    opened = []
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        if path.resolve() in protected:
            opened.append(path.resolve())
            if live_state == "denied":
                raise PermissionError("retained original live path is protected")
            raise FileNotFoundError("retained original live path is unavailable")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    projection = producer._read_frozen_tier_raw_accounts(context, artifact, anchor)
    assert opened == []
    assert projection.accounts["P95"]["trades"] == body["accounts"]["P95"]["trades"]
    assert len(projection.accounts["P95"]["trades"]) == len(body["accounts"]["P95"]["trades"]) > 0
    assert projection.qualification_authority is False


def _mechanical_compact_source_case():
    """Invented row reconstruction only; no authenticated native or engine output."""
    import numpy as np
    import pandas as pd

    index = pd.date_range("2026-01-05T00:00:00Z", periods=2, freq="h")
    frame = pd.DataFrame({"Open": [1.0, 1.01], "High": [1.02, 1.03], "Low": [0.99, 1.0],
        "Close": [1.01, 1.02], "SpreadOpen": [np.nan, 0.0], "SpreadBar": [3.0, 4.0],
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    rich = forward_fixture._mechanical_compact_rich_account()
    return rich, frame, np.array([0.0001, 0.0002]), np.array([0.0002, 0.0003]), {
        "entry_column": "SpreadOpen", "bar_column": "SpreadBar"}


def test_mechanical_current_compact_account_roundtrip_restores_source_rows(monkeypatch):
    """Exercise writer/reader shape mechanics, without a native-positive claim."""
    rich, frame, entry_half, intrabar_half, spread_meta = _mechanical_compact_source_case()
    compact = forward._compact_tier_account(rich)
    before = copy.deepcopy(compact)
    frame_before = frame.copy(deep=True)
    def no_engine(*args, **kwargs):
        pytest.fail("mechanical compact row reconstruction reached an engine")
    monkeypatch.setattr(forward.Backtester, "run", no_engine)
    hydrated = producer._materialize_tier_source_rows(compact, frame, entry_half, intrabar_half, spread_meta)
    assert compact == before
    assert frame.equals(frame_before)
    assert hydrated == {key: value for key, value in rich.items() if key not in {"valuation_rows", "m5_marks"}}
    assert "engine_equity" not in hydrated
    assert hydrated["source_rows"][0]["recorded_entry_points"] is None
    assert hydrated["source_rows"][1]["recorded_entry_points"] == 0.0
    assert hydrated["source_rows"][1]["engine_equity"] == 9999.123456789


@pytest.mark.parametrize("mutation", ["source_rows", "valuation_rows", "m5_marks", "missing_metadata"])
def test_mechanical_current_compact_account_rejects_mixed_field_sets(mutation):
    rich, frame, entry_half, intrabar_half, spread_meta = _mechanical_compact_source_case()
    compact = forward._compact_tier_account(rich)
    if mutation == "missing_metadata":
        compact.pop("valuation_source")
    else:
        compact[mutation] = []
    with pytest.raises(producer.CausalEvidenceProductionError, match="^FROZEN_TIER_ACCOUNT_FIELDS_INVALID$"):
        producer._materialize_tier_source_rows(compact, frame, entry_half, intrabar_half, spread_meta)


@pytest.mark.parametrize("vector", [None, [10000.0], [10000.0, 10000.0, 10000.0]],
    ids=["not-list", "short", "long"])
def test_mechanical_current_compact_account_requires_every_engine_axis_value(vector):
    rich, frame, entry_half, intrabar_half, spread_meta = _mechanical_compact_source_case()
    compact = forward._compact_tier_account(rich)
    compact["engine_equity"] = vector
    with pytest.raises(producer.CausalEvidenceProductionError,
                       match="^FROZEN_TIER_ENGINE_EQUITY_AXIS_MISSING$"):
        producer._materialize_tier_source_rows(compact, frame, entry_half, intrabar_half, spread_meta)


@pytest.mark.parametrize("value", [None, True, float("nan"), float("inf"), "10000"],
    ids=["null", "bool", "nan", "infinite", "string"])
def test_mechanical_current_compact_account_refuses_unobserved_engine_values(value):
    rich, frame, entry_half, intrabar_half, spread_meta = _mechanical_compact_source_case()
    compact = forward._compact_tier_account(rich)
    compact["engine_equity"][0] = value
    with pytest.raises(producer.CausalEvidenceProductionError,
                       match="^FROZEN_TIER_ENGINE_EQUITY_MARK_UNAVAILABLE$"):
        producer._materialize_tier_source_rows(compact, frame, entry_half, intrabar_half, spread_meta)


def _mechanical_modern_admission_case():
    """Declared synthetic missing/pass/refusal states, never observed quote evidence."""
    import numpy as np
    import pandas as pd

    observations = pd.Series([np.nan, 5.0, 12.0, np.nan],
        index=pd.date_range("2026-01-05T00:00:00Z", periods=4, freq="h"), name="EntrySpreadPoints")
    opportunities = np.array([False, True, True, True], dtype=bool)
    model = {"entry_admission_schema": forward.spread_profile.ADMISSION_SCHEMA,
        "entry_admission_policy": forward.spread_profile.ADMISSION_POLICY}
    admission = {"schema_version": forward.spread_profile.ADMISSION_SCHEMA,
        "entry_admission_policy": forward.spread_profile.ADMISSION_POLICY,
        "signal_opportunities": 3, "known_cap_pass_opportunities": 1,
        "known_cap_refused_opportunities": 1, "missing_refused_opportunities": 1,
        "missing_executed_legs": 0, "input_provenance_authenticated": False,
        "qualification_authority": False,
        "entry_observations_sha256": hashlib.sha256(
            pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest(),
        "entry_admission_mask_sha256": hashlib.sha256(np.array([0, 1, 0, 0], dtype=np.uint8).tobytes()).hexdigest()}
    return {"warmup_bars": 1, "engine_spread_model": model, "engine_spread_admission": admission}, (
        observations, opportunities, SimpleNamespace(spread_points=9))


@pytest.mark.parametrize("schema", ["current", "retained-v2"])
def test_mechanical_current_and_retained_v2_keep_modern_missing_refusal_semantics(schema):
    account, (observations, opportunities, tier) = _mechanical_modern_admission_case()
    before = copy.deepcopy(account)
    version = forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA if schema == "current" else forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA
    producer._verify_frozen_spread_admission(schema=version, account=account,
        observations=observations, tier=tier, opportunities=opportunities)
    assert account == before


@pytest.mark.parametrize("schema", ["current", "retained-v2"])
@pytest.mark.parametrize("mutation,expected", [
    ("missing_mask_admitted", "FROZEN_TIER_ENTRY_ADMISSION_BINDING_CHANGED"),
    ("rebalanced_counts", "FROZEN_TIER_ADMISSION_COUNTS_INVALID"),
    ("no_opportunities", "FROZEN_TIER_ORIGINAL_SIGNAL_OPPORTUNITIES_UNAVAILABLE"),
])
def test_mechanical_modern_admission_cannot_launder_missing_states(schema, mutation, expected):
    import numpy as np

    account, (observations, opportunities, tier) = _mechanical_modern_admission_case()
    admission = account["engine_spread_admission"]
    if mutation == "missing_mask_admitted":
        admission["entry_admission_mask_sha256"] = hashlib.sha256(
            np.array([1, 1, 0, 1], dtype=np.uint8).tobytes()).hexdigest()
    elif mutation == "rebalanced_counts":
        admission["known_cap_pass_opportunities"] = 0
        admission["missing_refused_opportunities"] = 2
    else:
        opportunities = None
    version = forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA if schema == "current" else forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA
    with pytest.raises(producer.CausalEvidenceProductionError, match=f"^{expected}$"):
        producer._verify_frozen_spread_admission(schema=version, account=account,
            observations=observations, tier=tier, opportunities=opportunities)


@pytest.mark.parametrize("protected_root", _PROTECTED_RETAINED_ROOTS, ids=(
    "retained-009", "retained-010", "missing-observation-001",
    "bound-002", "native-qa90", "config-109", "prepared-004",
))
@pytest.mark.parametrize("location", ("root", "descendant"))
def test_mechanical_custody_refuses_registered_protected_roots(monkeypatch, protected_root, location):
    """Invented context proves the path guard only; retained inputs are never written."""
    root = protected_root.resolve(strict=True)
    assert root.is_dir()
    if location == "descendant":
        children = sorted(path for path in root.iterdir() if path.is_dir())
        assert children, "protected-root mechanical case requires an existing child directory"
        root = children[0].resolve(strict=True)
    monkeypatch.setitem(globals(), "_OWNED_TEST_SPOOL_ROOTS", {root})
    context = SimpleNamespace(root=root)
    with pytest.raises(AssertionError):
        _owned_spool_root(context)


def test_mechanical_custody_accepts_registered_owned_scratch(monkeypatch, tmp_path):
    """An invented context can resolve an owned target without creating it."""
    root = tmp_path.resolve(strict=True)
    monkeypatch.setitem(globals(), "_OWNED_TEST_SPOOL_ROOTS", {root})
    context = SimpleNamespace(root=root)
    corpus = SimpleNamespace(context=context, scratch_root=root)
    target = _owned_corpus_path(corpus, "mechanical-unwritten.json")
    assert _owned_spool_root(context) == root
    assert target == root / "mechanical-unwritten.json"
    assert not target.exists()


def test_mechanical_custody_refuses_unregistered_scratch(monkeypatch, tmp_path):
    """Registry ownership is required even for an otherwise usable scratch directory."""
    monkeypatch.setitem(globals(), "_OWNED_TEST_SPOOL_ROOTS", set())
    context = SimpleNamespace(root=tmp_path.resolve(strict=True))
    with pytest.raises(AssertionError, match="registered scratch clone"):
        _owned_spool_root(context)


def _mechanical_execution_model_binding_case():
    """Declared four-state format case; no native source or engine output."""
    import pandas as pd

    account, (observations, opportunities, tier) = _mechanical_modern_admission_case()
    derived = {**account["engine_spread_model"], "entry_column": "SpreadOpen", "bar_column": "Spread"}
    old_model = {**derived, "entry_observations_sha256": account["engine_spread_admission"]["entry_observations_sha256"]}
    # MISSING, ADMIT, REFUSE, MISSING: independently declared expected mask.
    current_model = {**old_model, "entry_admission_mask_sha256": hashlib.sha256(bytes((0, 1, 0, 0))).hexdigest()}
    return derived, old_model, current_model, pd.DataFrame(index=observations.index), account, observations, opportunities, tier


def test_mechanical_v3_execution_model_rebuilds_full_admission_binding(monkeypatch):
    derived, _, expected, frame, _, observations, _, tier = _mechanical_execution_model_binding_case()
    before = copy.deepcopy((derived, expected, observations))
    monkeypatch.setattr(forward.Backtester, "run", lambda *_a, **_k: pytest.fail("model verifier reached an engine"))
    actual = producer._verify_frozen_execution_spread_model(derived, expected,
        forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA, features=frame, tier=tier, observations=observations)
    assert actual == expected
    assert "entry_admission_mask_sha256" not in derived
    assert derived == before[0] and expected == before[1]
    assert observations.equals(before[2])


@pytest.mark.parametrize("mutation", ("missing_mask", "wrong_mask", "wrong_observations_hash"))
def test_mechanical_v3_execution_model_refuses_missing_or_tampered_bindings(mutation):
    derived, _, stored, frame, _, observations, _, tier = _mechanical_execution_model_binding_case()
    if mutation == "missing_mask":
        stored.pop("entry_admission_mask_sha256")
    else:
        field = "entry_admission_mask_sha256" if mutation == "wrong_mask" else "entry_observations_sha256"
        stored[field] = "0" * 64
    before = copy.deepcopy(stored)
    with pytest.raises(producer.CausalEvidenceProductionError,
                       match="^FROZEN_TIER_EXECUTION_SPREAD_MODEL_CHANGED$"):
        producer._verify_frozen_execution_spread_model(derived, stored,
            forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA, features=frame, tier=tier, observations=observations)
    assert stored == before


def test_mechanical_v3_separate_admission_mask_still_requires_independent_verification():
    derived, _, stored, frame, account, observations, opportunities, tier = _mechanical_execution_model_binding_case()
    account["engine_spread_model"] = stored
    account["engine_spread_admission"]["entry_admission_mask_sha256"] = "0" * 64
    assert producer._verify_frozen_execution_spread_model(derived, stored,
        forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA, features=frame, tier=tier, observations=observations) == stored
    with pytest.raises(producer.CausalEvidenceProductionError,
                       match="^FROZEN_TIER_ENTRY_ADMISSION_BINDING_CHANGED$"):
        producer._verify_frozen_spread_admission(schema=forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
            account=account, observations=observations, tier=tier, opportunities=opportunities)


@pytest.mark.parametrize("schema", (forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
                                   forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA))
def test_mechanical_retained_execution_models_keep_observation_only_binding(monkeypatch, schema):
    derived, old_model, _, frame, _, observations, _, tier = _mechanical_execution_model_binding_case()
    if schema == forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA:
        old_model.pop("entry_admission_schema")
        old_model["entry_admission_policy"] = "EXACT_PRIOR_FINE_CLOSE_CAP_MISSING_UNTESTED"
    before = copy.deepcopy((derived, old_model))
    monkeypatch.setattr(forward.Backtester, "_entry_spread_admission_inputs",
        lambda *_a, **_k: pytest.fail("retained/archive model reached current admission reconstruction"))
    actual = producer._verify_frozen_execution_spread_model(derived, old_model, schema,
        features=frame, tier=tier, observations=observations)
    assert actual == old_model and "entry_admission_mask_sha256" not in actual
    assert (derived, old_model) == before


def test_mechanical_retained_v2_execution_model_refuses_added_current_mask():
    derived, _, stored, frame, _, observations, _, tier = _mechanical_execution_model_binding_case()
    with pytest.raises(producer.CausalEvidenceProductionError,
                       match="^FROZEN_TIER_EXECUTION_SPREAD_MODEL_CHANGED$"):
        producer._verify_frozen_execution_spread_model(derived, stored,
            forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA, features=frame, tier=tier, observations=observations)
