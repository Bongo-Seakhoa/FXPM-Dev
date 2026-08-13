from __future__ import annotations

import copy
import csv
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import struct
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
import pm_native_prefix_production as native_prefix
import pm_training_horizon as training_horizon
import pm_portfolio_replay as portfolio
import pm_production_capital_bootstrap as capital_bootstrap
import pm_production_authority_coordinator as coordinator
import pm_production_search_planner as search_planner
from tests import test_book_multiplicity as book_fixture
from tests import test_causal_process as causal_fixture
from tests import test_historical_production_adapter as adapter_fixture
from tests import test_native_prefix_production as native_prefix_fixture
from tests import test_production_capital_bootstrap as capital_fixture
from pm_native_request_contract import PRODUCTION_ARTIFACT_COLUMNS, TIMEFRAME_SECONDS


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


def _capital_specialist(
    *, anchor_id: str, symbol: str, timeframe: str, cell_id: str
) -> forward._VerifiedSpecialist:
    return forward._VerifiedSpecialist(
        anchor_id=anchor_id,
        cell_id=cell_id,
        symbol=symbol,
        timeframe=timeframe,
        regime="TREND",
        strategy_id="causal-fixture-strategy",
        strategy_class="CausalFixtureStrategy",
        strategy_module="tests.test_causal_evidence_producer",
        parameters={"fixture_parameter": 1},
        process_candidate_id=f"{anchor_id}:causal-fixture-candidate",
        process_sha256=_tag_sha(f"{anchor_id}:process"),
        priority_score=1.0,
        source_count_receipt={"fixture": "source-count"},
        detector_parameters={"fixture": "detector"},
        detector_parameters_sha256=_tag_sha(f"{anchor_id}:detector-parameters"),
        detector_record={"record_sha256": _tag_sha(f"{anchor_id}:detector")},
    )


def _capital_candidate(
    *,
    phase_id: str,
    cell_id: str,
    symbol: str,
    timeframe: str,
    window: dict[str, str],
) -> portfolio.CandidateTrade:
    entry = datetime.fromisoformat(window["start_date"]) + timedelta(hours=1)
    mark = entry + timedelta(minutes=30)
    exit_time = entry + timedelta(hours=1)
    return portfolio.CandidateTrade(
        candidate_id=f"{phase_id}:candidate",
        cell_id=cell_id,
        symbol=symbol,
        timeframe=timeframe,
        regime="TREND",
        strategy_name="CausalFixtureStrategy",
        promotion_status="HISTORICAL_OOS_REPLAY",
        asset_class="fx",
        risk_groups=("FX",),
        entry_time=entry,
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=1000.0,
        volume_min=0.01,
        volume_max=1.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(
            portfolio.ExitLeg(
                exit_time=exit_time,
                fraction=1.0,
                net_pnl_per_entry_lot=10.0,
                price_pnl_per_entry_lot=17.0,
                commission_per_entry_lot=7.0,
                exit_reason="fixture-exit",
            ),
        ),
        marks=(portfolio.MarkPoint(mark, 4.0),),
        source_schema=forward.POSITION_SOURCE_SCHEMA,
        margin_model=forward.MARGIN_MODEL,
    ).normalized()


def _anchor_native_prefixes(
    root: Path,
    anchors: list[dict[str, Any]],
    *,
    symbol: str,
) -> dict[str, native_prefix.NativePrefixProduct]:
    """Create one activated D1 history and genuine cutoff-local products."""

    data_dir = root / "native-data"
    # Pass symbol= explicitly. _write_activation declares `symbol: str = SYMBOL`,
    # and that default is bound at function-definition time, so reassigning the
    # module global native_prefix_fixture.SYMBOL never reaches it - the fixture
    # would write EURUSD's tree and then this file reads `symbol`'s
    # provenance.json, which never existed. Same defect previously fixed in
    # test_historical_production_adapter.py.
    facts = native_prefix_fixture._write_activation(
        data_dir, adapter_fixture.OBSERVED_TIMEFRAME, symbol=symbol
    )

    start = datetime(2016, 1, 1, tzinfo=timezone.utc)
    last_cutoff = max(
        datetime.fromisoformat(anchor["selection_cutoff_utc"].replace("Z", "+00:00"))
        for anchor in anchors
    )
    cadence = TIMEFRAME_SECONDS[adapter_fixture.OBSERVED_TIMEFRAME]
    row_count = int((last_cutoff - start).total_seconds() // cadence) + 1
    rows: list[list[str]] = []
    for ordinal in range(row_count):
        opened = start + timedelta(seconds=ordinal * cadence)
        closed = opened + timedelta(seconds=cadence)
        price = 1.1 + ordinal * 0.00001
        rows.append(
            [
                opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"{price:.6f}",
                f"{price + 0.002:.6f}",
                f"{price - 0.002:.6f}",
                f"{price + 0.001:.6f}",
                "100",
                "12",
                "80",
                opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
                opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
                closed.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
    bars_path = Path(facts["bars_path"])
    with bars_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("time", *PRODUCTION_ARTIFACT_COLUMNS))
        writer.writerows(rows)
    bars_raw = bars_path.read_bytes()
    bars_sha256 = hashlib.sha256(bars_raw).hexdigest()
    request_end = start + timedelta(seconds=row_count * cadence)
    request_end_utc = request_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    native_root = data_dir / "native"
    provenance_path = (
        native_root
        / "runs"
        / "run-prefix"
        / symbol
        / adapter_fixture.OBSERVED_TIMEFRAME
        / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["artifact"].update(
        {"sha256": bars_sha256, "bytes": len(bars_raw), "rows": row_count}
    )
    provenance.update(
        {
            "sha256": bars_sha256,
            "rows": row_count,
            "start_ts": rows[0][0],
            "end_ts": rows[-1][0],
        }
    )
    provenance["request"]["source_request_interval"] = {
        "start_utc": rows[0][0],
        "end_exclusive_utc": request_end_utc,
    }
    provenance_sha256 = native_prefix_fixture._write_json(provenance_path, provenance)

    sidecar_path = (
        native_root
        / "audits"
        / native_prefix_fixture.ACTIVATION_ID
        / symbol
        / adapter_fixture.OBSERVED_TIMEFRAME
        / "sidecar.json"
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["source_provenance_sha256"] = provenance_sha256
    sidecar_sha256 = native_prefix_fixture._write_json(sidecar_path, sidecar)

    run_manifest_path = native_root / "runs" / "run-prefix" / "run_manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest["results"][0].update({"rows": row_count, "sha256": bars_sha256})
    run_manifest_sha256 = native_prefix_fixture._write_json(
        run_manifest_path, run_manifest
    )
    activation_path = (
        native_root / "activations" / f"{native_prefix_fixture.ACTIVATION_ID}.json"
    )
    activation = json.loads(activation_path.read_text(encoding="utf-8"))
    activation["run_manifest_sha256"] = run_manifest_sha256
    activation["entries"][symbol][adapter_fixture.OBSERVED_TIMEFRAME].update(
        {"bars_sha256": bars_sha256, "sidecar_sha256": sidecar_sha256}
    )
    activation_sha256 = native_prefix_fixture._write_json(activation_path, activation)
    active_path = native_root / "ACTIVE.json"
    active = json.loads(active_path.read_text(encoding="utf-8"))
    active["manifest_sha256"] = activation_sha256
    native_prefix_fixture._write_json(active_path, active)

    native_context_root = root / "native-context"
    native_context_root.mkdir(parents=True, exist_ok=True)
    native_context = native_prefix_fixture._context(native_context_root)
    products: dict[str, native_prefix.NativePrefixProduct] = {}
    for anchor in anchors:
        product = native_prefix.produce_activated_native_prefix(
            data_dir=data_dir,
            context=native_context,
            symbol=symbol,
            timeframe=adapter_fixture.OBSERVED_TIMEFRAME,
            cutoff_utc=anchor["selection_cutoff_utc"],
            source_accessed_at_utc="2024-12-31T00:00:00Z",
            production_contract_required=False,
        )
        assert product.source_state == native_prefix.AVAILABLE
        products[anchor["anchor_id"]] = product
    return products


def _verified_pbo_winner(
    symbol: str,
    timeframe: str,
    regime: str,
) -> book.HistoricalBookCellEvidence:
    """Build a genuine local winner with the minimum two-column CSCV matrix."""

    fixture = book_fixture._winner_fixture_module()
    raw = fixture._payload_request(symbol, timeframe, regime)
    entries = raw["evidence_ledger_payload"]["entries"]
    for candidate_index, daily_mean in enumerate((0.20, 0.08)):
        entry = entries[candidate_index]
        entry["evidence_status"] = "AUTHENTICATED"
        entry["status_reason"] = None
        for fold_index, fold in enumerate(entry["folds"]):
            base = [
                round(
                    daily_mean + 0.04 * math.sin((fold_index * 63 + day_index) * 1.70),
                    12,
                )
                for day_index in range(63)
            ]
            fold["daily_net_mtm_r"] = base
            fold["daily_net_mtm_r_cost_stress"] = [
                round(value - 0.02, 12) for value in base
            ]
            fixture._refresh_fold_accounting(fold)
        entry["fold_count"] = len(entry["folds"])
        entry["folds_sha256"] = fixture._sha_json(entry["folds"])
        entry["pbo_folds"] = [fixture._pbo_projection(fold) for fold in entry["folds"]]
        entry["pbo_fold_count"] = len(entry["pbo_folds"])
        entry["pbo_folds_sha256"] = fixture._sha_json(entry["pbo_folds"])
    for entry in entries[2:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = fixture._sha_json([])
    raw["evidence_ledger_payload"]["entries_sha256"] = fixture._sha_json(entries)
    request = fixture._typed_request(raw)
    decision = book_fixture.winner.evaluate_historical_cell_winner(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY", (
        decision.reason_codes
    )
    assert book_fixture.winner.verify_historical_cell_winner_decision(decision, request)
    source, fit = book_fixture._source_and_fit(
        symbol,
        timeframe,
        book.AVAILABLE,
    )
    evidence = book.winner_cell_evidence(decision, request, source, fit)
    assert evidence is not None
    return evidence


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


def _adapter_available_authority(
    *,
    anchor: dict[str, Any],
    symbol: str,
    source_contract_sha256: str,
    native_product: native_prefix.NativePrefixProduct,
) -> tuple[book.HistoricalSourceGroupDecision, book.RegimeFitDecision]:
    timeframe = adapter_fixture.OBSERVED_TIMEFRAME
    prefix = native_product.native_prefix
    tag = f"{anchor['anchor_id']}:{symbol}:{timeframe}:adapter"
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.AVAILABLE,
        reason_code="AUTHENTICATED_NATIVE_SOURCE",
        source_contract_sha256=source_contract_sha256,
        source_group_evidence_sha256=_tag_sha(f"{tag}:source-evidence"),
        activation_id=prefix["activation_id"],
        activation_manifest_sha256=prefix["activation_manifest_sha256"],
        native_source_sha256=prefix["artifact_sha256"],
        acquisition_attempts_exact=1,
    )
    assert source is not None
    parameters = causal_fixture._detector_parameters()
    detector_code = _tag_sha(f"{tag}:detector-code")
    detector = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc=datetime.fromisoformat(
            anchor["selection_cutoff_utc"].replace("Z", "+00:00")
        ).isoformat(),
        detector_version="fxpm.market-regime-detector/v1",
        detector_code_sha256=detector_code,
        parameters=parameters,
    )
    assert detector is not None
    counts = {regime: 0 for regime in book.REGIME_ORDER}
    counts["TREND"] = 100
    support = {
        "schema_version": book.REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "symbol": symbol,
        "timeframe": timeframe,
        "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "label_order": list(book.REGIME_ORDER),
        "eligible_rows": 100,
        "counts": counts,
        "observed_regimes": ["TREND"],
        "unobserved_regimes": list(book.REGIME_ORDER[1:]),
        "support_state_by_regime": {
            regime: ("EMPIRICALLY_OBSERVED" if count else "UNOBSERVED")
            for regime, count in counts.items()
        },
        "first_ts": prefix["prefix_first_open_utc"].replace("Z", "+00:00"),
        "last_ts": prefix["prefix_last_open_utc"].replace("Z", "+00:00"),
        "vector_encoding": book.REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "vector_sha256": _tag_sha(f"{tag}:regime-vector"),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": prefix["activation_id"],
            "activation_manifest_sha256": prefix["activation_manifest_sha256"],
            "source_sha256": prefix["artifact_sha256"],
            "train_window_sha256": _tag_sha(f"{tag}:train-window"),
            "params_sha256": detector["parameters_sha256"],
            "detector_code_sha256": detector_code,
        },
    }
    support["receipt_sha256"] = _sha(support)
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256=_tag_sha(f"{tag}:regime-fit-policy"),
        detector_process_sha256=detector_code,
        regime_fit_evidence_sha256=_tag_sha(f"{tag}:regime-fit-evidence"),
        four_regime_mapping_sha256=_tag_sha(f"{tag}:four-regime-map"),
        regime_params_sha256=detector["parameters_sha256"],
        regime_detector_record=detector,
        train_regime_support=support,
    )
    assert fit is not None
    return source, fit


def _adapter_raw_fold_for_attempt(
    *,
    handle: historical_adapter.FrozenHistoricalCell,
    attempt_ordinal: int,
    fold_ordinal: int,
    planned: dict[str, Any],
    process_precommit_receipt_sha256: str,
) -> dict[str, Any]:
    """Bind genuine Backtester lane bodies to one frozen adapter attempt."""

    raw = adapter_fixture._raw_fold(fold_ordinal, planned)
    lanes = raw["lanes"]
    # The historical adapter fixture's compact 12-trade sample intentionally
    # clusters every close at the start of the 63-day fold.  That is useful for
    # adapter mechanics, but the resulting zero-padded return vector has only
    # about 27 HAC-effective days and therefore cannot be a genuine Winner v13
    # candidate (the frozen floor is 126 across the four folds).  Exercise this
    # production-shaped causal fixture with a complete daily position-events-v2
    # path instead.  Values remain deterministic, are materially distinct by
    # precommitted attempt, and are passed through the real Backtester surface,
    # cost-stress, TradePower, bootstrap, PBO, and Winner verifier chain.
    final_start = date.fromisoformat(planned["oos_start_date_utc"])
    final_end = date.fromisoformat(planned["oos_end_date_utc"])
    observation_count = (final_end - final_start).days + 1
    daily_values: list[float] = []
    centre = 0.32 if attempt_ordinal == 0 else 0.18
    for day_ordinal in range(observation_count):
        digest = hashlib.sha256(
            (
                "causal-genuine-daily-position-v1:"
                f"{attempt_ordinal}:{fold_ordinal}:{day_ordinal}"
            ).encode("ascii")
        ).digest()
        unit = int.from_bytes(digest[:8], "big") / float((1 << 64) - 1)
        daily_values.append(centre + 0.12 * (unit - 0.5))
    namespace = f"fold-{fold_ordinal}:final"
    final = adapter_fixture._result(
        final_start,
        final_end,
        namespace,
        values=daily_values,
    )
    stress = adapter_fixture._result(
        final_start,
        final_end,
        namespace,
        cost=adapter_fixture.STRESS_COST,
        # A distinct cost namespace makes stress_spread_values_sha256 differ from
        # the base. Without it the receipt claimed strict adversity while
        # presenting an UNCHANGED spread vector, which is exactly what
        # STRESS_METHOD_VECTOR_ADVERSITY_INCONSISTENT exists to catch.
        cost_namespace=f"{namespace}:stress",
        values=[value - 0.02 for value in daily_values],
    )
    lanes["final_lockbox_base"] = final
    lanes["final_lockbox_stress"] = stress
    frozen = handle.frozen
    attempt = frozen.attempt_ledger["attempts"][attempt_ordinal]
    artifacts = frozen.attempt_artifacts_bundle[attempt_ordinal]
    cross = json.loads(handle._cross_binding_bytes.decode("ascii"))
    mapped = cross["mapping"][attempt_ordinal]
    mapped_fold = mapped["fold_mapping"][fold_ordinal]
    geometry = adapter_fixture._partition_geometry(fold_ordinal, lanes)
    final_axis = final["trade_power_partition_axis"]
    signal_values = [0] * (final_axis["warmup_bars"] + final_axis["scored_bars"])
    signal_sha256 = hashlib.sha256(
        b"".join(struct.pack("<i", value) for value in signal_values)
    ).hexdigest()
    signal_body = {
        "schema_version": "fxpm.authoritative-search-signal-vector.v1",
        "encoding": "little-endian-signed-int32",
        "observation_count": len(signal_values),
        "values": signal_values,
        "signal_vector_sha256": signal_sha256,
    }
    signal_receipt = {
        **signal_body,
        "receipt_sha256": adapter_fixture._sha(signal_body),
    }
    common = {
        "authority_scope": "PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY",
        "anchor_id": cross["anchor_id"],
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "attempt_ordinal": attempt_ordinal,
        "horizon_attempt_id": mapped["horizon_attempt_id"],
        "fold_ordinal": fold_ordinal,
        "fold_id": mapped_fold["horizon_fold_id"],
        "confers_final_oos_or_trading_authority": False,
    }
    stress_body = {
        # Was pinned to a hardcoded v1 string. Track the module constant so this
        # fixture cannot silently fall behind the contract again - it did, and
        # every test in this file stopped running as a result.
        "schema_version": adapter_fixture.raw_winner.FROZEN_PATH_COST_STRESS_SCHEMA,
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "strategy_name": artifacts["winner_process"]["strategy_id"],
        "stress_mode": "stress_p95",
        "warmup_bars": final_axis["warmup_bars"],
        "partition_axis_sha256": final_axis["horizon_receipt_sha256"],
        "signal_vector_sha256": signal_sha256,
        "base_cost_attestation_sha256": final["numerical_execution_cost_attestation"][
            "attestation_sha256"
        ],
        "stress_cost_attestation_sha256": stress[
            "numerical_execution_cost_attestation"
        ]["attestation_sha256"],
        "base_spread_values_sha256": final["numerical_execution_cost_attestation"][
            "spread_values_sha256"
        ],
        "stress_spread_values_sha256": stress[
            "numerical_execution_cost_attestation"
        ]["spread_values_sha256"],
        "base_position_roster_sha256": (
            adapter_fixture.Backtester._cost_attestation_json_sha256(
                {"position_events": final["position_events"]}
            )
        ),
        "stress_position_roster_sha256": (
            adapter_fixture.Backtester._cost_attestation_json_sha256(
                {"position_events": stress["position_events"]}
            )
        ),
        "position_count": len(stress["position_events"]),
        "daily_observation_count": len(
            stress["winner_daily_r_surface"]["daily_net_mtm_r"]
        ),
        "path_identity_preserved": True,
        "stress_never_improves": True,
        "strictly_worse_trade_count": len(stress["position_events"]),
        "strictly_worse_daily_observation_count": sum(
            right < left
            for left, right in zip(
                final["winner_daily_r_surface"]["daily_net_mtm_r"],
                stress["winner_daily_r_surface"]["daily_net_mtm_r"],
                strict=True,
            )
        ),
        "realized_adversity_status": (
            adapter_fixture.raw_winner.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT
        ),
    }
    stress_receipt = {
        **stress_body,
        "receipt_sha256": adapter_fixture._sha(stress_body),
    }
    final_start = date.fromisoformat(planned["oos_start_date_utc"])
    train_start = final_start - timedelta(days=300)
    source_binding = frozen.source_contract_seed["source_binding"]
    provenance = {
        "anchor_selection_receipt": adapter_fixture._provenance_receipt(
            "anchor_selection_receipt",
            common,
            process_precommit_receipt_sha256=(process_precommit_receipt_sha256),
            horizon_plan_receipt_sha256=cross["horizon_roster"]["plan_receipt_sha256"],
            selection_cutoff_utc=cross["selection_cutoff_utc"],
        ),
        "anchor_horizon_decision": adapter_fixture._provenance_receipt(
            "anchor_horizon_decision",
            common,
            candidate_id=mapped["horizon_candidate_id"],
            candidate_definition_sha256=mapped["horizon_candidate_definition_sha256"],
            horizon_process_sha256=attempt["horizon_process_sha256"],
            fold_definition_sha256=mapped_fold["horizon_fold_definition_sha256"],
            partition_geometry=geometry,
            partition_geometry_receipts=[item["receipt_sha256"] for item in geometry],
        ),
        "anchor_trial_ledger": adapter_fixture._provenance_receipt(
            "anchor_trial_ledger",
            common,
            trial_registry_sha256=cross["horizon_roster"]["trial_registry_sha256"],
            attempt_roster_sha256=cross["horizon_roster"]["attempt_roster_sha256"],
            attempted_trials_exact=frozen.attempt_ledger["attempt_count"],
        ),
        "anchor_selected_trial": adapter_fixture._provenance_receipt(
            "anchor_selected_trial",
            common,
            selection_scope="PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER",
            trial_id=mapped["horizon_trial_id"],
            trial_definition_sha256=mapped["horizon_trial_definition_sha256"],
            winner_process_sha256=attempt["process_sha256"],
            executable_trial_sha256=attempt["trial_definition_sha256"],
        ),
        "anchor_refit_receipt": adapter_fixture._provenance_receipt(
            "anchor_refit_receipt",
            common,
            refit_recipe_sha256=attempt["refit_recipe_sha256"],
            fit_start_utc=f"{train_start.isoformat()}T00:00:00Z",
            fit_end_exclusive_utc=(
                f"{(final_start - timedelta(days=1)).isoformat()}T00:00:00Z"
            ),
            post_selection_search=False,
        ),
        "attempt_replay_receipt": adapter_fixture._provenance_receipt(
            "attempt_replay_receipt",
            common,
            lane_sha256={
                name: adapter_fixture._sha(value) for name, value in lanes.items()
            },
            stress_method_receipt_sha256=stress_receipt["receipt_sha256"],
            stress_method_receipt=stress_receipt,
            signal_vector_receipt=signal_receipt,
            position_events_are_genuine_backtester_v2=True,
        ),
        "oos_source_receipt": adapter_fixture._provenance_receipt(
            "oos_source_receipt",
            common,
            native_prefix_index_sha256=cross["native_prefix_index_sha256"],
            native_prefix_content_sha256=artifacts["horizon_process"][
                "source_snapshot_sha256"
            ],
            source_contract_sha256=frozen.precommitment["source_contract_sha256"],
            native_source_sha256=source_binding["target_bars_sha256"],
            activation_manifest_sha256=source_binding[
                "price_activation_manifest_sha256"
            ],
            source_group_decision_sha256=cross["source_group_decision_sha256"],
            regime_fit_decision_sha256=cross["regime_fit_decision_sha256"],
            execution_spec_receipt=adapter_fixture._execution_spec_receipt(
                frozen, cross
            ),
            partition_frame_sha256=[item["frame_sha256"] for item in geometry],
        ),
    }
    return {
        "fold_ordinal": fold_ordinal,
        "provenance": provenance,
        "lanes": lanes,
    }


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
        entry_costs = []
        raw_marks = []
        mark_costs = []
        raw_exits = []
        exit_costs = []
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
                            "stress_fill_price",
                        )
                    },
                }
            )
            entry_costs.append(
                {
                    "event_id": event_id,
                    **{
                        key: raw[key]
                        for key in (
                            "base_commission_cash",
                            "stress_commission_cash",
                            "base_fee_cash",
                            "stress_fee_cash",
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
                    "stress_financing_cash": raw["stress_financing_cash"],
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
                            "stress_fill_price",
                        )
                    },
                }
            )
            exit_costs.append(
                {
                    "event_id": event_id,
                    **{
                        key: raw[key]
                        for key in (
                            "base_commission_cash",
                            "stress_commission_cash",
                            "base_fee_cash",
                            "stress_fee_cash",
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
                "entries": entry_costs,
                "marks": mark_costs,
                "exits": exit_costs,
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
    context: spool.GenerationSpoolContext
    precommit_payload: dict[str, Any]
    historical_precommit: historical.HistoricalAnchorPrecommit
    run_receipt: historical.HistoricalAnchorRunReceipt
    replay_sets: dict[str, producer.AnchorReplayArtifactDescriptors]
    replay_bodies: dict[str, dict[str, dict[str, Any]]]
    forward_product: forward.ForwardOOSReplayProduct
    request: coordinator.HistoricalEvidenceRequest
    callback: producer.BoundedCausalEvidenceProducer


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
    frozen = producer.freeze_causal_process_precommit_v8(
        context=context,
        search_process=search_process,
        process_id="production-causal-process-v8",
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
        producer.freeze_causal_process_precommit_v8(
            context=wrong_context,
            search_process=search_process,
            process_id="production-causal-process-v8",
            anchors=plans,
        )
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="SEARCH_PROCESS_REVERIFY_FAILED",
    ):
        producer.freeze_causal_process_precommit_v8(
            context=context,
            search_process=search_process.receipt_sha256,
            process_id="production-causal-process-v8",
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
    tmp_path = tmp_path_factory.mktemp("causal-producer")
    causal_corpus = causal_fixture._build_corpus()
    precommit_payload = copy.deepcopy(causal_corpus.precommit_payload)
    replay_component_manifest = {
        "schema": "fxpm.causal-component-source-roster.v1",
        "component_identity_key": "synchronized_replay_code_sha256",
        "files": [
            forward._source_file_identity("pm_portfolio_replay.py"),
            forward._source_file_identity("pm_causal_evidence_producer.py"),
            forward._source_file_identity("pm_forward_oos_replay_producer.py"),
        ],
    }
    precommit_payload["process_definition"]["component_identities"][
        "synchronized_replay_code_sha256"
    ] = _sha(replay_component_manifest)
    precommit_payload["process_definition_sha256"] = _sha(
        precommit_payload["process_definition"]
    )
    process_sha = causal.canonical_sha256(precommit_payload)
    capital_config = capital_fixture._config()
    context = spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="causal-evidence-generation-v1",
        native_universe_sha256="c" * 64,
        config_sha256=_sha(capital_config),
    )
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(
            capital_bootstrap,
            "_utc_now",
            lambda: datetime(2026, 7, 13, 10, 0, 30, tzinfo=timezone.utc),
        )
        capital_authority = capital_bootstrap.bootstrap_production_capital_authority(
            bootstrap_root=(tmp_path / "capital-bootstrap").resolve(),
            output_root=(tmp_path / "capital-output").resolve(),
            authorization_scope=capital_bootstrap.DEMO_PROBATION_SCOPE,
            generation_context=context,
            config_payload=capital_config,
            native_source_identity=capital_fixture._native_identity(),
            process_precommitment_payload=precommit_payload,
            inner_selection_windows_by_anchor=capital_fixture._inner_windows(),
            current_selection_window={
                "start_date": "2025-02-05",
                "end_date": "2026-01-01",
            },
            contract_lineage=capital_fixture._contract(),
            margin_terms_sha256=_sha({"margin": "CURRENT"}),
            broker_terms_sha256=_sha({"broker": "CURRENT"}),
            replay_output_loader=lambda _request: capital_fixture._dummy_output(),
        )
    capital_binding_body = capital_bootstrap.verify_capital_forward_phase_binding(
        context,
        capital_authority.forward_phase_binding,
    )
    anchor_plan = precommit_payload["process_definition"]["anchor_plan"]
    winner_symbol = book.PINNED_SYMBOL_ORDER[0]
    winner_symbols = [winner_symbol] * len(anchor_plan)
    native_products = _anchor_native_prefixes(
        tmp_path / "historical-native",
        anchor_plan,
        symbol=winner_symbol,
    )
    anchors = []
    plans_by_anchor: dict[str, dict[str, dict[str, tuple[Any, ...]]]] = {}
    observed_by_anchor: dict[str, dict[str, Any]] = {}
    original_process_precommit = adapter_fixture.PROCESS_PRECOMMIT
    adapter_fixture.PROCESS_PRECOMMIT = process_sha
    try:
        for plan in anchor_plan:
            anchor_id = plan["anchor_id"]
            cutoff = datetime.fromisoformat(
                plan["selection_cutoff_utc"].replace("Z", "+00:00")
            ).date()
            first_fold_start = cutoff - timedelta(days=4 * 63)
            fold_plan = [
                {
                    "anchor_as_of_utc": (
                        f"{(first_fold_start + timedelta(days=63 * ordinal)).isoformat()}"
                        "T00:00:00+00:00"
                    ),
                    "oos_start_date_utc": (
                        first_fold_start + timedelta(days=63 * ordinal)
                    ).isoformat(),
                    "oos_end_date_utc": (
                        first_fold_start + timedelta(days=63 * ordinal + 62)
                    ).isoformat(),
                }
                for ordinal in range(4)
            ]
            seed_result = adapter_fixture._raw_fold(0, fold_plan[0])["lanes"][
                "optimizer_train_base"
            ]
            source_seed = adapter_fixture.raw_winner.build_trade_source_contract_from_backtester_result(
                seed_result
            )
            sample = adapter_fixture.build_trade_sample_evidence(
                [],
                source_contract=source_seed,
                minimum_observations=8,
            )
            assert sample.source_contract is not None
            native_product = native_products[anchor_id]
            source, fit = _adapter_available_authority(
                anchor=plan,
                symbol=winner_symbol,
                source_contract_sha256=(sample.source_contract.source_contract_sha256),
                native_product=native_product,
            )
            attempts = (
                adapter_fixture._attempt_artifact(
                    winner_symbol,
                    native_prefix_product=native_product,
                    lookback_value=50,
                ),
                adapter_fixture._attempt_artifact(
                    winner_symbol,
                    native_prefix_product=native_product,
                    lookback_value=60,
                ),
            )
            per_symbol: dict[str, dict[str, tuple[Any, ...]]] = {}
            bindings: list[dict[str, Any]] = []
            for symbol in book.PINNED_SYMBOL_ORDER:
                per_timeframe: dict[str, tuple[Any, ...]] = {}
                for timeframe in book.TIMEFRAME_ORDER:
                    if (
                        symbol == winner_symbol
                        and timeframe == adapter_fixture.OBSERVED_TIMEFRAME
                    ):
                        values = [
                            adapter_fixture._real_horizon_plan(
                                plan,
                                symbol,
                                "TREND",
                                native_prefix_product=native_product,
                                attempt_artifact=attempts[0],
                                additional_attempt_artifacts=(attempts[1],),
                                fold_plan=fold_plan,
                            ),
                            *(
                                adapter_fixture._real_unobserved_horizon_plan(
                                    plan,
                                    symbol,
                                    timeframe,
                                    regime,
                                    native_prefix_body=native_product.native_prefix,
                                    source_decision=source,
                                    fit_decision=fit,
                                )
                                for regime in book.REGIME_ORDER[1:]
                            ),
                        ]
                    else:
                        values = adapter_fixture._plans(
                            plan,
                            symbol,
                            timeframe,
                            source_available=False,
                        )
                    per_timeframe[timeframe] = tuple(values)
                    bindings.extend(
                        historical.historical_horizon_plan_binding(value)
                        for value in values
                    )
                per_symbol[symbol] = per_timeframe
            assert len(bindings) == book.FAMILY_SIZE
            plans_by_anchor[anchor_id] = per_symbol
            observed_by_anchor[anchor_id] = {
                "native_product": native_product,
                "source": source,
                "fit": fit,
                "fold_plan": fold_plan,
                "attempts": attempts,
                "source_seed": source_seed,
            }
            anchors.append(
                {
                    **plan,
                    "horizon_binding_count": book.FAMILY_SIZE,
                    "horizon_binding_roster_sha256": (
                        historical.historical_horizon_binding_roster_sha256(bindings)
                    ),
                }
            )
    finally:
        adapter_fixture.PROCESS_PRECOMMIT = original_process_precommit
    historical_precommit = historical.precommit_historical_anchor_run(
        context,
        run_id="historical-run-001",
        process_precommit_receipt_sha256=process_sha,
        optimizer_producer_release_sha256=(
            historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        anchors=anchors,
    )
    callbacks = {}
    for plan in anchor_plan:
        anchor_id = plan["anchor_id"]
        symbol_plans = plans_by_anchor[anchor_id]
        observed = observed_by_anchor[anchor_id]

        def callback(
            request: historical.AnchorProducerRequest,
            *,
            plan: dict[str, Any] = plan,
            anchor_id: str = anchor_id,
            symbol_plans: dict[str, dict[str, tuple[Any, ...]]] = symbol_plans,
            observed: dict[str, Any] = observed,
        ):
            for symbol in book.PINNED_SYMBOL_ORDER:
                capture = historical_adapter.open_historical_symbol_capture(
                    context,
                    request,
                    symbol,
                )
                for timeframe in book.TIMEFRAME_ORDER:
                    if (
                        symbol == winner_symbol
                        and timeframe == adapter_fixture.OBSERVED_TIMEFRAME
                    ):
                        source = observed["source"]
                        fit = observed["fit"]
                    else:
                        source, fit = adapter_fixture._terminal_authority(
                            anchor_id,
                            symbol,
                            timeframe,
                        )
                    capture.bind_timeframe(
                        timeframe,
                        horizon_plans=symbol_plans[symbol][timeframe],
                        source_group_decision=source,
                        regime_fit_decision=fit,
                    )
                if symbol == winner_symbol:
                    handle = capture.freeze_observed_cell(
                        adapter_fixture.OBSERVED_TIMEFRAME,
                        "TREND",
                        native_prefix_product=observed["native_product"],
                        precommitted_at_utc="2026-07-13T09:01:00Z",
                        attempts_frozen_at_utc="2026-07-13T09:01:01Z",
                        fold_plan=observed["fold_plan"],
                        attempt_artifacts=observed["attempts"],
                        source_contract_seed=observed["source_seed"],
                        base_cost_contract=adapter_fixture.BASE_COST_POLICY,
                        stress_cost_contract=adapter_fixture.STRESS_COST_POLICY,
                        # freeze_observed_cell gained this required argument when
                        # cost-stress v2 landed; this file was not updated, so
                        # every test below errored at setup and stopped running.
                        cost_stress_adversity_plan=(
                            adapter_fixture._cost_stress_adversity_plan(
                                horizon_plan=symbol_plans[symbol][
                                    adapter_fixture.OBSERVED_TIMEFRAME
                                ][0],
                                fold_plan=observed["fold_plan"],
                                symbol=symbol,
                            )
                        ),
                        risk_unit_contract=adapter_fixture.RISK_CONTRACT,
                    )
                    capture.complete_observed_cell(
                        handle,
                        attempt_evidence=[
                            {
                                "attempt_id": handle.frozen.attempt_ledger["attempts"][
                                    attempt_ordinal
                                ]["attempt_id"],
                                "folds": [
                                    _adapter_raw_fold_for_attempt(
                                        handle=handle,
                                        attempt_ordinal=attempt_ordinal,
                                        fold_ordinal=fold_ordinal,
                                        planned=planned,
                                        process_precommit_receipt_sha256=(process_sha),
                                    )
                                    for fold_ordinal, planned in enumerate(
                                        observed["fold_plan"]
                                    )
                                ],
                            }
                            for attempt_ordinal in range(len(observed["attempts"]))
                        ],
                        evidence_access_started_at_utc=("2026-07-13T09:01:02Z"),
                        evidence_access_completed_at_utc=("2026-07-13T09:01:03Z"),
                    )
                yield capture.finish()

        callbacks[anchor_id] = callback
    run_receipt = historical.execute_historical_anchor_run(
        context,
        historical_precommit,
        callbacks,
    )
    assert historical.verify_historical_anchor_run_receipt(
        context, historical_precommit, run_receipt
    )

    clocks = (
        "2026-07-13T10:02:00Z",
        "2026-07-13T10:03:00Z",
        "2026-07-13T10:20:00Z",
        "2026-07-13T10:22:00Z",
    )
    replay_sets = {}
    replay_bodies = {}
    run_body = run_receipt.to_dict()
    for index, (plan, source_anchor, winner_symbol) in enumerate(
        zip(
            precommit_payload["process_definition"]["anchor_plan"],
            causal_corpus.evidence_payload["anchors"],
            winner_symbols,
            strict=True,
        )
    ):
        cell_id = book.compute_cell_id(
            winner_symbol,
            adapter_fixture.OBSERVED_TIMEFRAME,
            "TREND",
        )
        assert cell_id is not None
        bodies = _decompose_activity(
            source_anchor,
            book_cell_id=cell_id,
            regime="TREND",
            strategy_ordinal=index,
            clocks=clocks,
        )
        descriptors = {
            role: spool.write_canonical_json_artifact(context, body)
            for role, body in bodies.items()
        }
        replay_bodies[plan["anchor_id"]] = bodies
        replay_sets[plan["anchor_id"]] = producer.AnchorReplayArtifactDescriptors(
            event_ledger=descriptors["EVENT_LEDGER"],
            fill_ledger=descriptors["FILL_LEDGER"],
            mtm_ledger=descriptors["MTM_LEDGER"],
            cost_ledger=descriptors["COST_LEDGER"],
            exposure_ledger=descriptors["EXPOSURE_LEDGER"],
            margin_ledger=descriptors["MARGIN_LEDGER"],
        )
    anchor_ids = [
        plan["anchor_id"]
        for plan in precommit_payload["process_definition"]["anchor_plan"]
    ]
    code_identity = forward._replay_code_identity(precommit_payload)
    pipeline_identity = {
        "schema_version": "fxpm.forward-oos-pipeline-config-identity.v1",
        "deployment_config_path": "config.json",
        "full_dataclass_surface": {},
        "backtester_cost_surface": {},
    }
    deployment_binding = {
        "deployment_config_path": "config.json",
        "deployment_config_sha256": context.config_sha256,
        "deployment_config_size_bytes": 1,
        "broker_specs_path": "broker_specs.json",
        "broker_specs_relative_path": "broker_specs.json",
        "broker_specs_sha256": "e" * 64,
        "broker_specs_size_bytes": 1,
        "pipeline_config_identity": pipeline_identity,
        "pipeline_config_identity_sha256": _sha(pipeline_identity),
    }
    capital_windows = forward._binding_windows(capital_binding_body)
    required_rosters = {
        anchor_id: {
            "execution_product_keys": [
                f"{winner_symbol}|{adapter_fixture.OBSERVED_TIMEFRAME}"
            ],
            "valuation_m5_product_keys": [f"{winner_symbol}|M5"],
        }
        for anchor_id, winner_symbol in zip(anchor_ids, winner_symbols, strict=True)
    }
    product_identities = []
    for plan, winner_symbol in zip(
        precommit_payload["process_definition"]["anchor_plan"],
        winner_symbols,
        strict=True,
    ):
        anchor_id = plan["anchor_id"]
        shared_activation = {
            "activation_id": f"activation:{anchor_id}",
            "activation_manifest_sha256": _tag_sha(f"{anchor_id}:activation"),
        }
        execution_identity = {
            "symbol": winner_symbol,
            "timeframe": adapter_fixture.OBSERVED_TIMEFRAME,
            **shared_activation,
            "source_bars_sha256": _tag_sha(f"{anchor_id}:D1:bars"),
            "source_audit_sidecar_sha256": _tag_sha(f"{anchor_id}:D1:sidecar"),
            "index_sha256": _tag_sha(f"{anchor_id}:D1:index"),
        }
        valuation_identity = {
            "symbol": winner_symbol,
            "timeframe": "M5",
            **shared_activation,
            "source_bars_sha256": _tag_sha(f"{anchor_id}:M5:bars"),
            "source_audit_sidecar_sha256": _tag_sha(f"{anchor_id}:M5:sidecar"),
            "index_sha256": _tag_sha(f"{anchor_id}:M5:index"),
        }
        execution_products = [execution_identity]
        valuation_products = [valuation_identity]
        product_identities.append(
            {
                "anchor_id": anchor_id,
                "forward_end_exclusive_utc": (
                    datetime.fromisoformat(plan["forward_end_date"]) + timedelta(days=1)
                )
                .date()
                .isoformat()
                + "T00:00:00Z",
                "execution_products": execution_products,
                "valuation_m5_products": valuation_products,
                "execution_product_roster_sha256": _sha(execution_products),
                "valuation_m5_product_roster_sha256": _sha(valuation_products),
                "valuation_products_supplied_separately": True,
                "capital_phase_windows": capital_windows[anchor_id],
            }
        )
    forward_precommit_body = {
        "schema_version": forward.REPLAY_PRECOMMIT_SCHEMA,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "historical_precommit_receipt_sha256": historical_precommit.receipt_sha256,
        "historical_run_receipt_sha256": run_receipt.receipt_sha256,
        "historical_run_payload_sha256": _sha(run_body),
        "causal_precommit_payload_sha256": process_sha,
        "created_at_utc": "2026-07-13T10:01:30.000000Z",
        "created_before_first_forward_oos_frame_access": True,
        "all_native_source_access_times_not_after_precommit": True,
        "required_rosters": required_rosters,
        "anchor_product_identities": product_identities,
        "anchor_product_identities_sha256": _sha(product_identities),
        "capital_phase_windows_by_anchor": capital_windows,
        "capital_phase_windows_sha256": _sha(capital_windows),
        "capital_forward_phase_binding": capital_binding_body,
        "capital_forward_phase_binding_sha256": _sha(capital_binding_body),
        "capital_bootstrap_identity_sha256": capital_binding_body[
            "bootstrap_identity_sha256"
        ],
        "capital_precommitment_body_sha256": capital_binding_body[
            "capital_precommitment_body_sha256"
        ],
        "capital_contract_lineage_sha256": _sha(
            capital_binding_body["contract_lineage"]
        ),
        "capital_margin_terms_sha256": capital_binding_body["margin_terms_sha256"],
        "capital_broker_terms_sha256": capital_binding_body["broker_terms_sha256"],
        "deployment_binding": deployment_binding,
        "code_identity": code_identity,
        "valuation_contract": forward.VALUATION_CONTRACT,
        "product_cutoff_contract": "EXACT_FORWARD_END_EXCLUSIVE_NO_LATER_ROWS",
        "confers_causal_current_refit_capital_publication_or_trading_authority": False,
    }
    forward_precommit_descriptor = spool.write_canonical_json_artifact(
        context, forward_precommit_body
    )
    forward_precommit_raw = _canonical(forward_precommit_body)
    precommit_issuer = forward.precommit_forward_oos_replay_products.__kwdefaults__[
        "_issue_precommit"
    ]
    replay_precommit = forward.ForwardOOSReplayPrecommit(
        forward_precommit_raw,
        forward_precommit_descriptor,
        precommit_issuer(forward_precommit_raw, forward_precommit_descriptor),
    )
    run_anchors = {value["anchor_id"]: value for value in run_body["anchors"]}
    precommit_anchors = {value["anchor_id"]: value for value in product_identities}
    phase_manifests: dict[str, dict[str, Any]] = {}
    for plan, winner_symbol in zip(
        precommit_payload["process_definition"]["anchor_plan"],
        winner_symbols,
        strict=True,
    ):
        anchor_id = plan["anchor_id"]
        cell_id = book.compute_cell_id(
            winner_symbol,
            adapter_fixture.OBSERVED_TIMEFRAME,
            "TREND",
        )
        assert cell_id is not None
        specialist = _capital_specialist(
            anchor_id=anchor_id,
            symbol=winner_symbol,
            timeframe=adapter_fixture.OBSERVED_TIMEFRAME,
            cell_id=cell_id,
        )
        for phase in forward.CAPITAL_PHASES:
            phase_id = forward._capital_phase_id(anchor_id, phase)
            window = capital_windows[anchor_id][phase]
            candidate = _capital_candidate(
                phase_id=phase_id,
                cell_id=cell_id,
                symbol=winner_symbol,
                timeframe=adapter_fixture.OBSERVED_TIMEFRAME,
                window=window,
            )
            replay = forward._AnchorRawReplay(
                anchor=plan,
                specialists=(specialist,),
                positions=(SimpleNamespace(candidate=candidate),),
                accepted_volume_by_position_id={},
                production_metadata={
                    "capital_phase_id": phase_id,
                    "capital_phase": phase,
                    "capital_window": window,
                },
            )
            manifest_descriptor, _ = forward._persist_capital_phase_evidence(
                context=context,
                replay=replay,
                run_anchor=run_anchors[anchor_id],
                replay_precommit=replay_precommit,
                replay_precommit_body=forward_precommit_body,
                precommit_anchor=precommit_anchors[anchor_id],
            )
            phase_manifests[phase_id] = manifest_descriptor
    receipt_body = {
        "schema_version": forward.PRODUCER_RECEIPT_SCHEMA,
        "production_contract_version": forward.PRODUCTION_CONTRACT_VERSION,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "historical_precommit_receipt_sha256": historical_precommit.receipt_sha256,
        "historical_run_receipt_sha256": run_receipt.receipt_sha256,
        "causal_precommit_payload_sha256": process_sha,
        "forward_replay_precommit_receipt_sha256": forward_precommit_descriptor[
            "sha256"
        ],
        "forward_replay_precommit_descriptor": forward_precommit_descriptor,
        "capital_bootstrap_identity_sha256": capital_binding_body[
            "bootstrap_identity_sha256"
        ],
        "capital_precommitment_body_sha256": capital_binding_body[
            "capital_precommitment_body_sha256"
        ],
        "capital_contract_lineage_sha256": _sha(
            capital_binding_body["contract_lineage"]
        ),
        "capital_margin_terms_sha256": capital_binding_body["margin_terms_sha256"],
        "capital_broker_terms_sha256": capital_binding_body["broker_terms_sha256"],
        "evidence_access_started_at_utc": clocks[0],
        "execution_started_at_utc": clocks[1],
        "execution_completed_at_utc": clocks[2],
        "reconciled_at_utc": clocks[3],
        "deployment_binding": deployment_binding,
        "code_identity": code_identity,
        "anchor_ledger_descriptors": {
            anchor_id: {
                role: dict(value)
                for role, value in replay_sets[anchor_id].descriptors()
            }
            for anchor_id in anchor_ids
        },
        "anchor_production_metadata": {
            anchor_id: {
                "schema": "fxpm.test-forward-anchor-production-metadata.v1",
                "anchor_id": anchor_id,
            }
            for anchor_id in anchor_ids
        },
        "ordered_anchor_ids": anchor_ids,
        "capital_phase_manifest_descriptors": phase_manifests,
        "ordered_capital_phase_ids": list(phase_manifests),
        "capital_candidate_records_complete_pre_policy": True,
        "capital_phase_window_contract": (
            "SIGNED_BOOTSTRAP_INNER_AND_CAUSAL_OUTER_HALF_OPEN_V1"
        ),
        "valuation_contract": forward.VALUATION_CONTRACT,
        "valuation_event_order": "close_existing_then_mark_existing_then_admit",
        "full_round_trip_commission_in_every_candidate_mark": True,
        "stop_risk_includes_adverse_configured_stop_slippage": True,
        "candidate_marks_thinned_or_sampled": False,
        "causal_consumer_reopened_and_verified": False,
        "causal_binding_followup_required": True,
        "confers_causal_process_current_refit_capital_readiness_publication_or_trading_authority": False,
    }
    receipt_descriptor = spool.write_canonical_json_artifact(context, receipt_body)
    anchor_rows = tuple((anchor_id, replay_sets[anchor_id]) for anchor_id in anchor_ids)
    receipt_raw = _canonical(receipt_body)
    # White-box fixture only: production callers receive products solely from
    # produce_forward_oos_replay_artifacts.  Hostile forward tests prove that
    # direct construction and dataclasses.replace cannot mint a valid tag.
    issuer = forward.produce_forward_oos_replay_artifacts.__kwdefaults__[
        "_issue_product"
    ]
    forward_product = forward.ForwardOOSReplayProduct(
        anchor_rows,
        receipt_raw,
        receipt_descriptor,
        issuer(anchor_rows, receipt_raw, receipt_descriptor),
    )
    forward.verify_forward_oos_replay_product(context, forward_product)
    request_body = {
        "schema_version": coordinator.COORDINATOR_SCHEMA,
        "historical_run_receipt_sha256": run_receipt.receipt_sha256,
        "anchor_book_receipt_sha256_by_id": {
            item["anchor_id"]: item["book_decision_sha256"]
            for item in run_body["anchors"]
        },
        "causal_precommit_payload_sha256": process_sha,
        "authority_claimed": False,
    }
    request = coordinator.HistoricalEvidenceRequest(_canonical(request_body))
    callback = producer.BoundedCausalEvidenceProducer(
        context=context,
        historical_precommit=historical_precommit,
        historical_run_receipt=run_receipt,
        causal_precommit_payload=precommit_payload,
        forward_replay_product=forward_product,
    )
    return Corpus(
        context,
        precommit_payload,
        historical_precommit,
        run_receipt,
        replay_sets,
        replay_bodies,
        forward_product,
        request,
        callback,
    )


def test_valid_four_anchor_genuine_replay_produces_verified_product(
    valid_corpus: Corpus,
) -> None:
    product = valid_corpus.callback(valid_corpus.request)

    assert type(product) is coordinator.CausalEvidenceProduct
    decision = causal.evaluate_historical_process(
        valid_corpus.precommit_payload,
        dict(product.evidence_payload),
        dict(product.reconciliation_payload),
    )
    assert causal.verify_historical_process_decision(
        decision,
        valid_corpus.precommit_payload,
        dict(product.evidence_payload),
        dict(product.reconciliation_payload),
    )
    assert decision.body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    assert len(product.evidence_payload["anchors"]) == 4
    assert all(
        len(anchor["book_cells"]) == causal.CELL_COUNT
        for anchor in product.evidence_payload["anchors"]
    )


def test_bare_replay_descriptors_cannot_bypass_issued_forward_product(
    valid_corpus: Corpus,
) -> None:
    callback = replace(
        valid_corpus.callback,
        forward_replay_product=valid_corpus.replay_sets,
    )
    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED",
    ):
        callback(valid_corpus.request)


def _callback_with(
    corpus: Corpus,
    anchor_id: str,
    replacement: producer.AnchorReplayArtifactDescriptors,
) -> producer.BoundedCausalEvidenceProducer:
    rows = tuple(
        (observed_id, replacement if observed_id == anchor_id else descriptors)
        for observed_id, descriptors in corpus.forward_product._anchor_rows
    )
    forged = replace(corpus.forward_product, _anchor_rows=rows)
    return replace(corpus.callback, forward_replay_product=forged)


def test_missing_raw_cost_ledger_fails_typed(
    valid_corpus: Corpus, tmp_path: Path
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    missing = dict(original.cost_ledger)
    missing["relative_path"] = "artifacts/missing-cost-ledger.json"
    replacement = replace(original, cost_ledger=missing)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED",
    ):
        _callback_with(valid_corpus, anchor_id, replacement)(valid_corpus.request)


def test_descriptor_bound_cost_tamper_fails_before_returns(
    valid_corpus: Corpus,
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    descriptor = dict(original.cost_ledger)
    root = Path(valid_corpus.context.root)
    source = root / Path(descriptor["relative_path"])
    target = root / "tampered" / "cost-ledger.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = source.read_bytes()
    target.write_bytes(raw[:-1] + (b" " if raw[-1:] != b" " else b"\n"))
    descriptor["relative_path"] = target.relative_to(root).as_posix()
    replacement = replace(original, cost_ledger=descriptor)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED",
    ):
        _callback_with(valid_corpus, anchor_id, replacement)(valid_corpus.request)


def test_future_refit_leakage_is_rejected_from_raw_specialist(
    valid_corpus: Corpus,
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    event = copy.deepcopy(valid_corpus.replay_bodies[anchor_id]["EVENT_LEDGER"])
    event["specialists"][0]["refit_market_data_end_utc"] = "2099-01-01T00:00:00Z"
    descriptor = spool.write_canonical_json_artifact(valid_corpus.context, event)
    replacement = replace(original, event_ledger=descriptor)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED",
    ):
        _callback_with(valid_corpus, anchor_id, replacement)(valid_corpus.request)


def test_caller_return_vector_and_hash_fields_are_not_accepted(
    valid_corpus: Corpus,
) -> None:
    anchor_id = next(iter(valid_corpus.replay_sets))
    original = valid_corpus.replay_sets[anchor_id]
    mtm = copy.deepcopy(valid_corpus.replay_bodies[anchor_id]["MTM_LEDGER"])
    mtm["caller_daily_returns"] = ["0.99"]
    mtm["caller_return_sha256"] = "f" * 64
    descriptor = spool.write_canonical_json_artifact(valid_corpus.context, mtm)
    replacement = replace(original, mtm_ledger=descriptor)

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED",
    ):
        _callback_with(valid_corpus, anchor_id, replacement)(valid_corpus.request)


def test_coordinator_request_cannot_relabel_historical_book(
    valid_corpus: Corpus,
) -> None:
    body = valid_corpus.request.to_dict()
    first = next(iter(body["anchor_book_receipt_sha256_by_id"]))
    body["anchor_book_receipt_sha256_by_id"][first] = "0" * 64
    request = coordinator.HistoricalEvidenceRequest(_canonical(body))

    with pytest.raises(
        producer.CausalEvidenceProductionError,
        match="COORDINATOR_BOOK_ROSTER_MISMATCH",
    ):
        valid_corpus.callback(request)
