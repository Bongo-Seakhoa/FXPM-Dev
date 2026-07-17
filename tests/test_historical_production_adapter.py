from __future__ import annotations

import copy
import csv
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import struct

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_current_refit_artifacts as current_refit
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_native_prefix_production as native_prefix
import pm_training_horizon as training_horizon
from pm_core import Backtester, InstrumentSpec
from pm_native_request_contract import PRODUCTION_ARTIFACT_COLUMNS, TIMEFRAME_SECONDS
from pm_strategies import StrategyRegistry
from pm_trade_power import (
    build_numerical_execution_cost_attestation,
    build_trade_sample_evidence,
)
import pm_winner_raw_evidence as raw_winner
from tests import test_training_horizon as horizon_fixture
from tests import test_native_prefix_production as native_prefix_fixture


PROCESS_PRECOMMIT = "a" * 64
OBSERVED_TIMEFRAME = "D1"


def _canonical(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha(value):
    if isinstance(value, str):
        value = {"label": value}
    return hashlib.sha256(_canonical(value)).hexdigest()


def test_precommitted_evidence_roles_come_only_from_horizon_status() -> None:
    assert adapter._precommitted_evidence_roles(
        {
            "attempts": [
                {"status": "PLANNED"},
                {"status": "PROCESS_REJECTED"},
            ]
        },
        2,
    ) == ("EVALUATION_REQUIRED", "PRESEARCH_PROCESS_REJECTED")
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="PRECOMMITTED_EVIDENCE_ROLE_ROSTER_INVALID",
    ):
        adapter._precommitted_evidence_roles(
            {"attempts": [{"status": "PLANNED"}]}, 2
        )


@pytest.fixture(scope="module")
def observed_native_prefix(tmp_path_factory):
    """Build one real sealed 500-row D1 prefix shared by observed-cell tests."""

    root = tmp_path_factory.mktemp("historical-adapter-native-prefix")
    data_dir = root / "data"
    symbol = book.PINNED_SYMBOL_ORDER[0]
    original_symbol = native_prefix_fixture.SYMBOL
    native_prefix_fixture.SYMBOL = symbol
    try:
        facts = native_prefix_fixture._write_activation(data_dir, OBSERVED_TIMEFRAME)
    finally:
        native_prefix_fixture.SYMBOL = original_symbol

    start = datetime(2018, 1, 1, tzinfo=timezone.utc)
    cadence = TIMEFRAME_SECONDS[OBSERVED_TIMEFRAME]
    row_count = 500
    rows = []
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
    cutoff = start + timedelta(seconds=row_count * cadence)
    cutoff_utc = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    native_root = data_dir / "native"
    provenance_path = (
        native_root
        / "runs"
        / "run-prefix"
        / symbol
        / OBSERVED_TIMEFRAME
        / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["artifact"].update(
        {
            "sha256": bars_sha256,
            "bytes": len(bars_raw),
            "rows": row_count,
        }
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
        "end_exclusive_utc": cutoff_utc,
    }
    provenance_sha256 = native_prefix_fixture._write_json(provenance_path, provenance)

    sidecar_path = (
        native_root
        / "audits"
        / native_prefix_fixture.ACTIVATION_ID
        / symbol
        / OBSERVED_TIMEFRAME
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

    manifest_path = (
        native_root / "activations" / f"{native_prefix_fixture.ACTIVATION_ID}.json"
    )
    activation = json.loads(manifest_path.read_text(encoding="utf-8"))
    activation["run_manifest_sha256"] = run_manifest_sha256
    activation_entry = activation["entries"][symbol][OBSERVED_TIMEFRAME]
    activation_entry.update(
        {
            "bars_sha256": bars_sha256,
            "sidecar_sha256": sidecar_sha256,
        }
    )
    manifest_sha256 = native_prefix_fixture._write_json(manifest_path, activation)
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = manifest_sha256
    native_prefix_fixture._write_json(pointer_path, pointer)

    context = native_prefix_fixture._context(root)
    product = native_prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=symbol,
        timeframe=OBSERVED_TIMEFRAME,
        cutoff_utc=cutoff_utc,
        source_accessed_at_utc="2020-01-01T00:00:00Z",
        production_contract_required=False,
    )
    frame = native_prefix.open_verified_prefix_frame(product)
    assert product.source_state == native_prefix.AVAILABLE
    assert len(frame) == row_count
    assert product.native_prefix["prefix_rows"] == row_count
    return product


def _sha1(value):
    if isinstance(value, str):
        value = {"label": value}
    return hashlib.sha1(_canonical(value)).hexdigest()


def _read_descriptor_json(context, descriptor):
    path = context.root.joinpath(*descriptor["relative_path"].split("/"))
    payload = path.read_bytes()
    assert len(payload) == descriptor["size_bytes"]
    assert hashlib.sha256(payload).hexdigest() == descriptor["sha256"]
    return json.loads(payload.decode("utf-8"))


def _plan(
    anchor,
    symbol,
    timeframe,
    regime,
    *,
    source_available,
    commitments=None,
):
    label = f"{anchor['anchor_id']}:{symbol}:{timeframe}:{regime}"
    request = (
        horizon_fixture._request()
        if source_available
        else horizon_fixture._terminal_source_request("SOURCE_UNAVAILABLE")
    )
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": int(anchor["anchor_id"].rsplit("-", 1)[-1]),
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": f"{anchor['forward_end_date']}T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    request["cell"] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
    }
    prefix = request["native_prefix"]
    prefix["symbol"] = symbol
    prefix["timeframe"] = timeframe
    if source_available:
        prefix["artifact_rows"] = 1_000
        prefix["prefix_rows"] = 700
        dependency = request["dependency_policy"]["payload"]
        for name in (
            "feature_lookback_bars",
            "label_lookahead_bars",
            "max_holding_bars",
            "execution_delay_bars",
            "regime_warmup_bars",
            "cost_lookback_bars",
            "max_holding_seconds",
        ):
            dependency[name] = 0
        request["dependency_policy"]["payload_sha256"] = _sha(dependency)
    prefix["source_terminal_receipt_sha256"] = (
        None if source_available else _sha(f"{label}:source-terminal")
    )
    # Registry commitments are deliberately derived by Horizon's public builder.
    # The legacy helper argument remains only so older call sites stay compact.
    assert commitments is None or type(commitments) is dict
    return training_horizon.build_historical_horizon_plan(request)


def _plans(
    anchor,
    symbol,
    timeframe,
    *,
    source_available,
    commitments=None,
):
    return [
        _plan(
            anchor,
            symbol,
            timeframe,
            regime,
            source_available=source_available,
            commitments=commitments,
        )
        for regime in book.REGIME_ORDER
    ]


def _terminal_authority(anchor_id, symbol, timeframe):
    label = f"{anchor_id}:{symbol}:{timeframe}"
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.TERMINAL_UNAVAILABLE,
        reason_code="BROKER_ARCHIVE_UNAVAILABLE",
        source_contract_sha256=_sha(f"{label}:source-contract"),
        source_group_evidence_sha256=_sha(f"{label}:source-evidence"),
        acquisition_attempts_exact=3,
        terminal_proof_sha256=_sha(f"{label}:terminal-proof"),
    )
    assert source is not None
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_NOT_APPLICABLE,
        reason_code="SOURCE_TERMINAL_UNAVAILABLE",
        regime_fit_policy_sha256=_sha(f"{label}:regime-policy"),
    )
    assert fit is not None
    return source, fit


def _request(context, anchor, *, bind_sinks=True):
    body = {
        "schema_version": orchestration.PRODUCER_REQUEST_SCHEMA,
        "authority_scope": orchestration.AUTHORITY_SCOPE,
        "run_id": "historical-run-001",
        "generation_id": context.generation_id,
        "precommit_receipt_sha256": _sha("precommit"),
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
        "optimizer_producer_release_sha256": (
            adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        "anchor": anchor,
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "requires_exact_sparse_state_roster": True,
        "possible_regime_slots_are_forced_observations": False,
        "confers_causal_or_current_authority": False,
    }
    sinks = None
    if bind_sinks:
        sinks = {
            symbol: spool.open_symbol_sink(context, symbol)
            for symbol in book.PINNED_SYMBOL_ORDER
        }
    return orchestration.AnchorProducerRequest(_canonical(body), sinks)


def _context(tmp_path):
    return spool.freeze_generation_context(
        tmp_path / "authority",
        generation_id="historical-adapter-generation-001",
        native_universe_sha256="b" * 64,
        config_sha256="c" * 64,
    )


def _anchor(anchor_id="anchor-01", cutoff=date(2020, 1, 1)):
    return {
        "anchor_id": anchor_id,
        "selection_cutoff_utc": f"{cutoff.isoformat()}T00:00:00Z",
        "forward_start_date": (cutoff + timedelta(days=35)).isoformat(),
        "forward_end_date": (cutoff + timedelta(days=399)).isoformat(),
        "purge_calendar_days": 35,
        "horizon_binding_count": book.FAMILY_SIZE,
        "horizon_binding_roster_sha256": "d" * 64,
    }


def _bind_terminal_symbol(capture, anchor):
    for timeframe in book.TIMEFRAME_ORDER:
        source, fit = _terminal_authority(
            anchor["anchor_id"], capture.symbol, timeframe
        )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=_plans(
                anchor, capture.symbol, timeframe, source_available=False
            ),
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def _detector_params():
    values = {
        "k_confirm": 2,
        "gap_min": 0.1,
        "k_hold": 3,
        "adx_period": 14,
        "atr_period": 14,
        "bb_period": 20,
        "bb_std": 2.0,
        "efficiency_period": 10,
        "bb_squeeze_lookback": 100,
        "bb_squeeze_percentile": 20.0,
        "atr_lookback": 100,
        "structure_period": 20,
        "structure_atr_mult": 1.0,
        "adx_trend_threshold": 25.0,
    }
    for prefix in ("trend", "range", "breakout", "chop"):
        for suffix in (
            ("adx", "efficiency", "slope")
            if prefix == "trend"
            else (
                ("adx", "efficiency", "containment")
                if prefix == "range"
                else (
                    ("squeeze", "atr", "structure")
                    if prefix == "breakout"
                    else ("adx", "efficiency", "whipsaw")
                )
            )
        ):
            values[f"{prefix}_{suffix}_weight"] = 1.0 / 3.0
    return values


def _available_authority(
    symbol, timeframe, source_contract_sha, *, native_prefix_body=None
):
    native_prefix_body = native_prefix_body or {}
    activation_id = native_prefix_body.get("activation_id", "activation-001")
    activation_manifest_sha256 = native_prefix_body.get(
        "activation_manifest_sha256", "1" * 64
    )
    native_source_sha256 = native_prefix_body.get("artifact_sha256", "2" * 64)
    support_first_ts = native_prefix_body.get(
        "prefix_first_open_utc", "2018-01-01T00:00:00+00:00"
    )
    support_last_ts = native_prefix_body.get(
        "prefix_last_open_utc", "2019-12-31T00:00:00+00:00"
    )
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.AVAILABLE,
        reason_code="AUTHENTICATED_NATIVE_SOURCE",
        source_contract_sha256=source_contract_sha,
        source_group_evidence_sha256=_sha("available-source-evidence"),
        acquisition_attempts_exact=1,
        activation_id=activation_id,
        activation_manifest_sha256=activation_manifest_sha256,
        native_source_sha256=native_source_sha256,
    )
    assert source is not None
    code_sha = "3" * 64
    detector = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc="2020-01-01T00:00:00+00:00",
        detector_version="regime-detector-v1",
        detector_code_sha256=code_sha,
        parameters=_detector_params(),
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
        "unobserved_regimes": ["RANGE", "BREAKOUT", "CHOP"],
        "support_state_by_regime": {
            regime: ("EMPIRICALLY_OBSERVED" if count else "UNOBSERVED")
            for regime, count in counts.items()
        },
        "first_ts": support_first_ts,
        "last_ts": support_last_ts,
        "vector_encoding": book.REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "vector_sha256": _sha("regime-vector"),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": activation_id,
            "activation_manifest_sha256": activation_manifest_sha256,
            "source_sha256": native_source_sha256,
            "train_window_sha256": "4" * 64,
            "params_sha256": detector["parameters_sha256"],
            "detector_code_sha256": code_sha,
        },
    }
    support["receipt_sha256"] = _sha(support)
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_FITTED,
        reason_code="TRAIN_ONLY_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256="5" * 64,
        detector_process_sha256=code_sha,
        regime_fit_evidence_sha256="6" * 64,
        four_regime_mapping_sha256="7" * 64,
        regime_params_sha256=detector["parameters_sha256"],
        regime_detector_record=detector,
        train_regime_support=support,
    )
    assert fit is not None
    return source, fit


def _bind_single_observed_capture(
    capture,
    anchor,
    source_sha,
    *,
    commitments=None,
    observed_plan=None,
):
    observed_plan_body = observed_plan.to_dict() if observed_plan is not None else {}
    native_prefix_body = observed_plan_body.get("native_prefix")
    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe == OBSERVED_TIMEFRAME:
            source, fit = _available_authority(
                capture.symbol,
                timeframe,
                source_sha,
                native_prefix_body=native_prefix_body,
            )
            available = True
        else:
            source, fit = _terminal_authority(
                anchor["anchor_id"], capture.symbol, timeframe
            )
            available = False
        horizon_plans = _plans(
            anchor,
            capture.symbol,
            timeframe,
            source_available=available,
            commitments=commitments if timeframe == OBSERVED_TIMEFRAME else None,
        )
        if timeframe == OBSERVED_TIMEFRAME and observed_plan is not None:
            horizon_plans[0] = observed_plan
            for ordinal, regime in enumerate(book.REGIME_ORDER[1:], start=1):
                horizon_plans[ordinal] = _real_unobserved_horizon_plan(
                    anchor,
                    capture.symbol,
                    timeframe,
                    regime,
                    native_prefix_body=native_prefix_body,
                    source_decision=source,
                    fit_decision=fit,
                )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=horizon_plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )


TEST_INSTRUMENT_SPEC = InstrumentSpec(
    symbol=book.PINNED_SYMBOL_ORDER[0],
    pip_position=4,
    pip_value=10.0,
    spread_avg=1.0,
    commission_per_lot=7.0,
)
TEST_INSTRUMENT_SPEC_SHA256 = Backtester._cost_attestation_json_sha256(
    {
        "schema_version": "fxpm-backtester-instrument-spec-v1",
        "instrument_spec": asdict(TEST_INSTRUMENT_SPEC),
    }
)
BASE_SPREAD_POLICY = json.dumps(
    {
        "bar_column": "SpreadMax",
        "entry_column": "SpreadOpen",
        "entry_model": "open_spread",
        "fallback_to_fixed": False,
        "mode": "conservative_max",
        "source": "historical_spread_points",
        "stress_entry_fallback": False,
        "units": "price",
    },
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
)
STRESS_SPREAD_POLICY = json.dumps(
    {
        "bar_column": "SpreadMax",
        "entry_column": "SpreadP95",
        "entry_model": "p95_spread_stress",
        "fallback_to_fixed": False,
        "mode": "stress_p95",
        "source": "historical_spread_points",
        "stress_entry_fallback": False,
        "units": "price",
    },
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
)


BASE_COST = build_numerical_execution_cost_attestation(
    use_spread=True,
    use_slippage=True,
    use_commission=True,
    spread_mode=BASE_SPREAD_POLICY,
    spread_values_sha256="7" * 64,
    commission_inputs_sha256="8" * 64,
    instrument_spec_sha256=TEST_INSTRUMENT_SPEC_SHA256,
    slippage_pips=0.5,
    slippage_price=0.00005,
)
STRESS_COST = build_numerical_execution_cost_attestation(
    use_spread=True,
    use_slippage=True,
    use_commission=True,
    spread_mode=STRESS_SPREAD_POLICY,
    spread_values_sha256="6" * 64,
    commission_inputs_sha256="8" * 64,
    instrument_spec_sha256=TEST_INSTRUMENT_SPEC_SHA256,
    slippage_pips=0.5,
    slippage_price=0.00005,
)


def _frozen_cost_policy(lane, attestation):
    return raw_winner.build_frozen_numerical_execution_cost_policy(
        lane=lane,
        use_spread=attestation["use_spread"],
        use_slippage=attestation["use_slippage"],
        use_commission=attestation["use_commission"],
        spread_mode=attestation["spread_mode"],
        commission_inputs_sha256=attestation["commission_inputs_sha256"],
        instrument_spec_sha256=attestation["instrument_spec_sha256"],
        slippage_pips=attestation["slippage_pips_hex"],
        slippage_price=attestation["slippage_price_hex"],
    )


BASE_COST_POLICY = _frozen_cost_policy("BASE", BASE_COST)
STRESS_COST_POLICY = _frozen_cost_policy("STRESS", STRESS_COST)
RISK_CONTRACT = raw_winner.build_precommitted_risk_unit_contract()


def _swap(label):
    return {
        "contract_version": "causal_swap_accrual_v1",
        "status": "AVAILABLE",
        "provenance_status": "STRESS_APPROVED",
        "execution_state_compounding": True,
        "historical_rates_authenticated": False,
        "stress_policy_explicitly_approved": True,
        "stress_policy_immutable": True,
        "stress_policy_sha256": "a" * 64,
        "timestamp_source": "_BAR_CLOSE_TS",
        "price_activation_manifest_sha256": "1" * 64,
        "terms_activation_manifest_sha256": "2" * 64,
        "terms_snapshot_sha256": "3" * 64,
        "schedule_sha256": "4" * 64,
        "target_bars_sha256": "5" * 64,
        "target_sidecar_sha256": "6" * 64,
        "projection_contract": "fxpm-swap-preopen-close-projection-v1",
        "projection_sha256": _sha(label),
    }


def _events(start, namespace, values=None):
    values = values or [0.30 + 0.01 * (index % 2) for index in range(12)]
    rows = []
    for index, value in enumerate(values):
        opened = datetime.combine(
            start + timedelta(days=index), datetime.min.time(), timezone.utc
        ) + timedelta(hours=10)
        rows.append(
            {
                "trade_record_schema_version": "2026-07-14-position-events-v2",
                "position_id": f"{namespace}:{index:04d}",
                "entry_time": opened.isoformat(),
                "exit_time": (opened + timedelta(hours=1)).isoformat(),
                "entry_event_order": index * 4 + 1,
                "exit_event_order": index * 4 + 3,
                "pnl_dollars": value * 100.0,
                "risk_amount": 100.0,
            }
        )
    return rows


def _partition_cost_attestation(policy_seed, namespace):
    """Materialize the genuine partition-local vector under frozen policy facts."""

    return build_numerical_execution_cost_attestation(
        use_spread=policy_seed["use_spread"],
        use_slippage=policy_seed["use_slippage"],
        use_commission=policy_seed["use_commission"],
        spread_mode=policy_seed["spread_mode"],
        spread_values_sha256=_sha(f"{namespace}:spread-values"),
        commission_inputs_sha256=policy_seed["commission_inputs_sha256"],
        instrument_spec_sha256=policy_seed["instrument_spec_sha256"],
        slippage_pips=float.fromhex(policy_seed["slippage_pips_hex"]),
        slippage_price=float.fromhex(policy_seed["slippage_price_hex"]),
    )


def _result(start, end, namespace, *, cost=BASE_COST, values=None):
    events = _events(start, namespace, values)
    surface = raw_winner.build_flat_daily_r_surface(
        events,
        start_date_utc=start.isoformat(),
        end_date_utc=end.isoformat(),
        risk_unit_contract=RISK_CONTRACT,
    )
    axis = {
        "schema_version": "fxpm.backtest-partition-axis.v1",
        "timeframe": OBSERVED_TIMEFRAME,
        "start_utc": datetime.combine(
            start, datetime.min.time(), timezone.utc
        ).isoformat(),
        "end_utc": datetime.combine(
            end + timedelta(days=1), datetime.min.time(), timezone.utc
        ).isoformat(),
        "scored_bars": (end - start).days + 1,
        "warmup_bars": 0,
        "event_axis_sha256": _sha(f"{namespace}:axis"),
    }
    axis["horizon_receipt_sha256"] = _sha(axis)
    return {
        "position_events": events,
        "winner_daily_r_surface": surface,
        "trade_power_partition_axis": axis,
        "execution_cost_timing_contract": {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "numerical_execution_cost_attestation": _partition_cost_attestation(
            cost, namespace
        ),
        "swap_accrual_contract": _swap(f"{namespace}:projection"),
    }


def _attempt_artifact(
    symbol,
    timeframe=OBSERVED_TIMEFRAME,
    regime="TREND",
    *,
    native_prefix_product,
    lookback_kind="ACTIVE_SESSIONS",
    lookback_value=50,
    declared_source_bar_count=None,
):
    strategy_id = "EMACrossoverStrategy"
    prefix = native_prefix_product.native_prefix
    source_bar_count = (
        int(prefix["prefix_rows"])
        if declared_source_bar_count is None
        else declared_source_bar_count
    )
    artifacts = current_refit.build_current_refit_search_artifacts_v2(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_id=strategy_id,
        parameters=StrategyRegistry.get(strategy_id).get_params(),
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        fit_start_utc=prefix["prefix_start_utc"],
        fit_end_utc=prefix["prefix_end_exclusive_utc"],
        latest_closed_bar_utc=prefix["prefix_last_close_utc"],
        source_snapshot_sha256=prefix["prefix_content_sha256"],
        source_bar_count=source_bar_count,
        seed=7,
    )
    return {
        "winner_process": artifacts.winner_process,
        "horizon_process": artifacts.horizon_process,
        "trial_definition": artifacts.trial_definition,
        "refit_recipe": artifacts.refit_recipe,
    }


def _real_horizon_plan(
    anchor,
    symbol,
    regime,
    *,
    native_prefix_product,
    attempt_artifact,
    fold_plan,
    additional_attempt_artifacts=(),
    boundary_limited=False,
):
    request = copy.deepcopy(horizon_fixture._request())
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": 0,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": (f"{anchor['forward_end_date']}T00:00:00Z"),
        "evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    request["cell"] = {
        "symbol": symbol,
        "timeframe": OBSERVED_TIMEFRAME,
        "regime": regime,
    }
    request["native_prefix"] = dict(native_prefix_product.native_prefix)
    artifact_roster = [attempt_artifact, *additional_attempt_artifacts]
    candidates = []
    for artifact in artifact_roster:
        horizon_process = artifact["horizon_process"]
        fit_start_utc = horizon_process["fit_start_utc"].replace("+00:00", "Z")
        folds = []
        for ordinal, planned in enumerate(() if boundary_limited else fold_plan):
            validation_start = date.fromisoformat(planned["oos_start_date_utc"])
            validation_end = date.fromisoformat(
                planned["oos_end_date_utc"]
            ) + timedelta(days=1)
            fit_end = validation_start - timedelta(days=60)
            value = horizon_fixture._fold(
                f"ROLL-{ordinal + 1:03d}",
                "CAUSAL_ROLLING_ORIGIN_INNER",
                fit_start_utc,
                f"{fit_end.isoformat()}T00:00:00Z",
                f"{(validation_start - timedelta(days=5)).isoformat()}T00:00:00Z",
                f"{validation_start.isoformat()}T00:00:00Z",
                f"{validation_end.isoformat()}T00:00:00Z",
            )
            value["fit_rows"] = 50
            value["validation_rows"] = 30
            value.pop("fold_definition_sha256")
            value["fold_definition_sha256"] = _sha(value)
            folds.append(value)
        realized_start_utc = fit_start_utc
        if boundary_limited:
            realized_start_utc = (
                (
                    datetime.fromisoformat(fit_start_utc.replace("Z", "+00:00"))
                    - timedelta(days=1)
                )
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
        candidate_definition = {
            "candidate_id": (
                f"{horizon_process['lookback_kind']}-"
                f"{horizon_process['lookback_value']}"
            ),
            "lookback_kind": horizon_process["lookback_kind"],
            "lookback_value": horizon_process["lookback_value"],
            "realized_start_utc": realized_start_utc,
        }
        candidates.append(
            {
                **candidate_definition,
                "candidate_definition_sha256": _sha(candidate_definition),
                "folds": folds,
            }
        )
    request["candidates"] = candidates
    trials_by_identity = {}
    for artifact in artifact_roster:
        process = artifact["winner_process"]
        raw_trial = artifact["trial_definition"]
        identity = (
            process["strategy_id"],
            process["strategy_code_sha256"],
            _sha(raw_trial["parameters"]),
        )
        if identity in trials_by_identity:
            continue
        trial_definition = {
            "trial_id": f"TRIAL-{len(trials_by_identity) + 1:03d}",
            "strategy_id": identity[0],
            "strategy_release_sha256": identity[1],
            "parameter_set_sha256": identity[2],
        }
        trials_by_identity[identity] = {
            **trial_definition,
            "trial_definition_sha256": _sha(trial_definition),
        }
    request["trials"] = list(trials_by_identity.values())
    request["horizon_policy"]["cost_contract_sha256"] = _sha(BASE_COST_POLICY)
    request["horizon_policy"]["cost_stress_contract_sha256"] = _sha(STRESS_COST_POLICY)
    return training_horizon.build_historical_horizon_plan(request)


def _real_unobserved_horizon_plan(
    anchor,
    symbol,
    timeframe,
    regime,
    *,
    native_prefix_body,
    source_decision,
    fit_decision,
):
    request = copy.deepcopy(horizon_fixture._unobserved_request())
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": 0,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": f"{anchor['forward_end_date']}T00:00:00Z",
        "evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    request["cell"] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
    }
    request["native_prefix"] = copy.deepcopy(native_prefix_body)
    fit_body = fit_decision.to_dict()
    support = copy.deepcopy(fit_body["train_regime_support"])
    binding_body = {
        "schema_version": "fxpm.historical-horizon-regime-support-binding.v1",
        "source_group_decision_sha256": source_decision.receipt_sha256,
        "regime_fit_decision_sha256": fit_decision.receipt_sha256,
        "regime_detector_record_sha256": fit_body["regime_detector_record_sha256"],
        "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "target_regime": regime,
        "target_regime_count": 0,
        "train_regime_support": support,
    }
    request["regime_support_binding"] = {
        **binding_body,
        "binding_sha256": _sha(binding_body),
    }
    return training_horizon.build_historical_horizon_plan(request)


def _fold_plan():
    starts = [date(2018, 5, 1) + timedelta(days=63 * index) for index in range(4)]
    return [
        {
            "anchor_as_of_utc": f"{start.isoformat()}T00:00:00+00:00",
            "oos_start_date_utc": start.isoformat(),
            "oos_end_date_utc": (start + timedelta(days=62)).isoformat(),
        }
        for start in starts
    ]


def _provenance_receipt(role, common, **specific):
    body = {
        "schema_version": "fxpm.authoritative-search-provenance.v1",
        **common,
        "receipt_role": role,
        **specific,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _partition_geometry(fold_ordinal, lanes):
    geometry = []
    for index, (role, lane_name) in enumerate(
        (
            ("optimizer_train", "optimizer_train_base"),
            ("tuning_validation", "tuning_validation_base"),
            ("final_lockbox", "final_lockbox_base"),
        )
    ):
        axis = lanes[lane_name]["trade_power_partition_axis"]
        body = {
            "schema_version": "fxpm.winner-partition-geometry.v1",
            "role": role,
            "frame_start_utc": axis["start_utc"],
            "scored_start_utc": axis["start_utc"],
            "end_exclusive_utc": axis["end_utc"],
            "warmup_bars": axis["warmup_bars"],
            "scored_bars": axis["scored_bars"],
            "frame_rows": axis["warmup_bars"] + axis["scored_bars"],
            "frame_sha256": _sha(f"frame:{fold_ordinal}:{index}"),
        }
        geometry.append({**body, "receipt_sha256": _sha(body)})
    return geometry


def _execution_spec_receipt(frozen, cross):
    body = {
        "schema_version": "fxpm.execution-instrument-spec-receipt.v1",
        "symbol": frozen.symbol,
        "authority_timing": "FROZEN_BEFORE_FIRST_SEARCH_EVALUATION",
        "deployment_scope": "TEST_PRODUCTION_SCOPE",
        "deployment_config_sha256": _sha("deployment-config"),
        "deployment_config_size_bytes": 1024,
        "broker_specs_relative_path": "broker_specs.json",
        "broker_specs_sha256": _sha("broker-specs"),
        "broker_specs_size_bytes": 2048,
        "broker_record_key": frozen.symbol,
        "broker_record_sha256": _sha("broker-record"),
        "configured_instrument_record_sha256": _sha("configured-instrument"),
        "native_prefix_index_sha256": cross["native_prefix_index_sha256"],
        "native_provenance_sha256": _sha("native-provenance"),
        "native_instrument_snapshot_sha256": _sha("native-instrument"),
        "tick_value_source": "ACTIVATED_ACQUISITION_INSTRUMENT_SNAPSHOT",
        "commission_source": "FROZEN_DEPLOYMENT_CONFIG_INSTRUMENT_SURFACE",
        "volume_source": "FROZEN_BROKER_SPECS_RECORD",
        "instrument_spec": asdict(TEST_INSTRUMENT_SPEC),
        "instrument_spec_sha256": TEST_INSTRUMENT_SPEC_SHA256,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _raw_fold(fold_ordinal, planned, handle=None):
    final_start = date.fromisoformat(planned["oos_start_date_utc"])
    final_end = date.fromisoformat(planned["oos_end_date_utc"])
    train_start = final_start - timedelta(days=300)
    tuning_start = final_start - timedelta(days=150)
    prefix = f"fold-{fold_ordinal}"
    final = _result(final_start, final_end, f"{prefix}:final")
    stress_values = [
        event["pnl_dollars"] / event["risk_amount"] - 0.02
        for event in final["position_events"]
    ]
    stress = _result(
        final_start,
        final_end,
        f"{prefix}:final",
        cost=STRESS_COST,
        values=stress_values,
    )
    lanes = {
        "optimizer_train_base": _result(
            train_start,
            train_start + timedelta(days=62),
            f"{prefix}:train",
        ),
        "tuning_validation_base": _result(
            tuning_start,
            tuning_start + timedelta(days=62),
            f"{prefix}:tuning",
        ),
        "final_lockbox_base": final,
        "final_lockbox_stress": stress,
    }
    provenance = {}
    if handle is not None:
        frozen = handle.frozen
        attempt = frozen.attempt_ledger["attempts"][0]
        artifacts = frozen.attempt_artifacts_bundle[0]
        cross = json.loads(handle._cross_binding_bytes.decode("ascii"))
        mapped = cross["mapping"][0]
        mapped_fold = mapped["fold_mapping"][fold_ordinal]
        geometry = _partition_geometry(fold_ordinal, lanes)
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
        signal_receipt = {**signal_body, "receipt_sha256": _sha(signal_body)}
        common = {
            "authority_scope": "PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY",
            "anchor_id": cross["anchor_id"],
            "symbol": frozen.symbol,
            "timeframe": frozen.timeframe,
            "regime": frozen.regime,
            "attempt_ordinal": 0,
            "horizon_attempt_id": mapped["horizon_attempt_id"],
            "fold_ordinal": fold_ordinal,
            "fold_id": mapped_fold["horizon_fold_id"],
            "confers_final_oos_or_trading_authority": False,
        }
        stress_body = {
            "schema_version": "fxpm.backtester-frozen-path-cost-stress.v1",
            "symbol": frozen.symbol,
            "timeframe": frozen.timeframe,
            "strategy_name": artifacts["winner_process"]["strategy_id"],
            "stress_mode": "stress_p95",
            "warmup_bars": final["trade_power_partition_axis"]["warmup_bars"],
            "partition_axis_sha256": final["trade_power_partition_axis"][
                "horizon_receipt_sha256"
            ],
            "signal_vector_sha256": signal_sha256,
            "base_cost_attestation_sha256": final[
                "numerical_execution_cost_attestation"
            ]["attestation_sha256"],
            "stress_cost_attestation_sha256": stress[
                "numerical_execution_cost_attestation"
            ]["attestation_sha256"],
            "base_position_roster_sha256": Backtester._cost_attestation_json_sha256(
                {"position_events": final["position_events"]}
            ),
            "stress_position_roster_sha256": Backtester._cost_attestation_json_sha256(
                {"position_events": stress["position_events"]}
            ),
            "position_count": len(stress["position_events"]),
            "daily_observation_count": len(
                stress["winner_daily_r_surface"]["daily_net_mtm_r"]
            ),
            "path_identity_preserved": True,
            "stress_never_improves": True,
        }
        stress_receipt = {**stress_body, "receipt_sha256": _sha(stress_body)}
        source_binding = frozen.source_contract_seed["source_binding"]
        provenance = {
            "anchor_selection_receipt": _provenance_receipt(
                "anchor_selection_receipt",
                common,
                process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
                horizon_plan_receipt_sha256=cross["horizon_roster"][
                    "plan_receipt_sha256"
                ],
                selection_cutoff_utc="2020-01-01T00:00:00Z",
            ),
            "anchor_horizon_decision": _provenance_receipt(
                "anchor_horizon_decision",
                common,
                candidate_id=mapped["horizon_candidate_id"],
                candidate_definition_sha256=mapped[
                    "horizon_candidate_definition_sha256"
                ],
                horizon_process_sha256=attempt["horizon_process_sha256"],
                fold_definition_sha256=mapped_fold["horizon_fold_definition_sha256"],
                partition_geometry=geometry,
                partition_geometry_receipts=[
                    item["receipt_sha256"] for item in geometry
                ],
            ),
            "anchor_trial_ledger": _provenance_receipt(
                "anchor_trial_ledger",
                common,
                trial_registry_sha256=cross["horizon_roster"]["trial_registry_sha256"],
                attempt_roster_sha256=cross["horizon_roster"]["attempt_roster_sha256"],
                attempted_trials_exact=frozen.attempt_ledger["attempt_count"],
            ),
            "anchor_selected_trial": _provenance_receipt(
                "anchor_selected_trial",
                common,
                selection_scope="PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER",
                trial_id=mapped["horizon_trial_id"],
                trial_definition_sha256=mapped["horizon_trial_definition_sha256"],
                winner_process_sha256=attempt["process_sha256"],
                executable_trial_sha256=attempt["trial_definition_sha256"],
            ),
            "anchor_refit_receipt": _provenance_receipt(
                "anchor_refit_receipt",
                common,
                refit_recipe_sha256=attempt["refit_recipe_sha256"],
                fit_start_utc=f"{train_start.isoformat()}T00:00:00Z",
                fit_end_exclusive_utc=(
                    f"{(final_start - timedelta(days=1)).isoformat()}T00:00:00Z"
                ),
                post_selection_search=False,
            ),
            "attempt_replay_receipt": _provenance_receipt(
                "attempt_replay_receipt",
                common,
                lane_sha256={name: _sha(value) for name, value in lanes.items()},
                stress_method_receipt_sha256=stress_receipt["receipt_sha256"],
                stress_method_receipt=stress_receipt,
                signal_vector_receipt=signal_receipt,
                position_events_are_genuine_backtester_v2=True,
            ),
            "oos_source_receipt": _provenance_receipt(
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
                execution_spec_receipt=_execution_spec_receipt(frozen, cross),
                partition_frame_sha256=[item["frame_sha256"] for item in geometry],
            ),
        }
    return {
        "fold_ordinal": fold_ordinal,
        "provenance": provenance,
        "lanes": lanes,
    }


def test_symbol_capture_uses_raw_winner_and_empirical_unobserved_states(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    source_sha = sample.source_contract.source_contract_sha256
    attempt_artifacts = [
        _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    ]
    real_horizon_plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=attempt_artifacts[0],
        fold_plan=fold_plan,
    )

    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe == OBSERVED_TIMEFRAME:
            source, fit = _available_authority(
                symbol,
                timeframe,
                source_sha,
                native_prefix_body=observed_native_prefix.native_prefix,
            )
            available = True
        else:
            source, fit = _terminal_authority(anchor["anchor_id"], symbol, timeframe)
            available = False
        horizon_plans = _plans(
            anchor,
            symbol,
            timeframe,
            source_available=available,
        )
        if timeframe == OBSERVED_TIMEFRAME:
            horizon_plans[0] = real_horizon_plan
            for ordinal, regime in enumerate(book.REGIME_ORDER[1:], start=1):
                horizon_plans[ordinal] = _real_unobserved_horizon_plan(
                    anchor,
                    symbol,
                    timeframe,
                    regime,
                    native_prefix_body=observed_native_prefix.native_prefix,
                    source_decision=source,
                    fit_decision=fit,
                )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=horizon_plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )

    handle = capture.freeze_observed_cell(
        OBSERVED_TIMEFRAME,
        "TREND",
        native_prefix_product=observed_native_prefix,
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=fold_plan,
        attempt_artifacts=attempt_artifacts,
        source_contract_seed=source_seed,
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        risk_unit_contract=RISK_CONTRACT,
    )
    assert handle._horizon_execution_contracts == (
        {
            "lookback_kind": "ACTIVE_SESSIONS",
            "lookback_value": 50,
            "source_bar_count": 500,
        },
    )
    source_count_receipt = handle._source_count_receipts[0]
    receipt_body = dict(source_count_receipt)
    assert receipt_body.pop("receipt_sha256") == _sha(receipt_body)
    assert source_count_receipt["native_prefix_index_sha256"] == (
        observed_native_prefix.index_sha256
    )
    assert (
        source_count_receipt["fit_frame_sha256"]
        != (observed_native_prefix.native_prefix["prefix_content_sha256"])
    )
    production = capture.complete_observed_cell(
        handle,
        attempt_evidence=[
            {
                "attempt_id": handle.frozen.attempt_ledger["attempts"][0]["attempt_id"],
                "folds": [
                    _raw_fold(index, planned, handle)
                    for index, planned in enumerate(fold_plan)
                ],
            }
        ],
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
    )
    assert production.request.precommitment.to_dict()["regime"] == "TREND"
    product = capture.finish()
    returned_plans = list(product.horizon_plans)
    assert len(returned_plans) == 24
    assert all(training_horizon._valid_plan(plan) for plan in returned_plans)
    assert [
        (
            plan.to_dict()["cell"]["timeframe"],
            plan.to_dict()["cell"]["regime"],
        )
        for plan in returned_plans
    ] == [
        (timeframe, regime)
        for timeframe in book.TIMEFRAME_ORDER
        for regime in book.REGIME_ORDER
    ]
    payload = product.optimizer_payload
    sink = request.symbol_sink(symbol)
    manifest = spool.verify_symbol_receipt(
        context.root,
        sink.seal(),
        expected_generation_id=context.generation_id,
        expected_symbol=symbol,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    artifacts = {entry["artifact_id"]: entry for entry in manifest["artifacts"]}
    root_id = f"{anchor['anchor_id']}/adapter/raw-winner-descriptor-graph"
    index_id = f"{anchor['anchor_id']}/adapter/production-index"
    assert artifacts[root_id]["descriptor"] == payload["raw_descriptor_graph"]
    assert artifacts[index_id]["descriptor"] == payload["production_index"]
    assert (
        f"{anchor['anchor_id']}/adapter/{OBSERVED_TIMEFRAME}/TREND/frozen-receipt"
        not in artifacts
    )
    root_graph = _read_descriptor_json(context, payload["raw_descriptor_graph"])
    for root_entry in root_graph["artifacts"]:
        assert artifacts[root_entry["artifact_id"]] == root_entry
        if root_entry["artifact_type"] != "historical_raw_winner_descriptor_graph":
            continue
        phase_graph = _read_descriptor_json(context, root_entry["descriptor"])
        for leaf_entry in phase_graph["artifacts"]:
            assert artifacts[leaf_entry["artifact_id"]] == leaf_entry
    assert payload["cell_count"] == 24
    assert payload["state_counts"] == {
        "REGIME_UNOBSERVED": 3,
        "TERMINAL_SOURCE_ABSENCE": 20,
        "WINNER_DECISION": 1,
    }
    assert payload["raw_artifact_namespace"] == (
        "historical-anchor/historical-run-001/anchor-01"
    )
    assert payload["raw_descriptor_graph_root_count"] == 3
    cells = list(product.book_evidence)
    assert len(cells) == 24
    observed = [cell for cell in cells if cell.timeframe == OBSERVED_TIMEFRAME]
    assert observed[0].winner_decision is production.decision
    assert [cell.process_state_decision.state_kind for cell in observed[1:]] == [
        book.REGIME_UNOBSERVED,
        book.REGIME_UNOBSERVED,
        book.REGIME_UNOBSERVED,
    ]


def test_zero_support_cannot_be_inferred_from_a_ready_plan(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    for timeframe in book.TIMEFRAME_ORDER[:-1]:
        source, fit = _terminal_authority(anchor["anchor_id"], symbol, timeframe)
        capture.bind_timeframe(
            timeframe,
            horizon_plans=_plans(anchor, symbol, timeframe, source_available=False),
            source_group_decision=source,
            regime_fit_decision=fit,
        )
    source, fit = _available_authority(
        symbol,
        OBSERVED_TIMEFRAME,
        _sha("ready-plan-source"),
        native_prefix_body=observed_native_prefix.native_prefix,
    )
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="UNOBSERVED_REGIME_HORIZON_STATUS_INVALID",
    ):
        capture.bind_timeframe(
            OBSERVED_TIMEFRAME,
            horizon_plans=_plans(
                anchor, symbol, OBSERVED_TIMEFRAME, source_available=True
            ),
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def test_boundary_limited_observed_cell_becomes_bound_insufficient_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    boundary_plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=_fold_plan(),
        boundary_limited=True,
    )
    assert boundary_plan.to_dict()["plan_status"] == "HISTORICAL_BOUNDARY_LIMITED"
    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe == OBSERVED_TIMEFRAME:
            source, fit = _available_authority(
                symbol,
                timeframe,
                _sha("boundary-source"),
                native_prefix_body=observed_native_prefix.native_prefix,
            )
            plans = _plans(anchor, symbol, timeframe, source_available=True)
            plans[0] = boundary_plan
            for ordinal, regime in enumerate(book.REGIME_ORDER[1:], start=1):
                plans[ordinal] = _real_unobserved_horizon_plan(
                    anchor,
                    symbol,
                    timeframe,
                    regime,
                    native_prefix_body=observed_native_prefix.native_prefix,
                    source_decision=source,
                    fit_decision=fit,
                )
        else:
            source, fit = _terminal_authority(anchor["anchor_id"], symbol, timeframe)
            plans = _plans(anchor, symbol, timeframe, source_available=False)
        capture.bind_timeframe(
            timeframe,
            horizon_plans=plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )
    product = capture.finish()
    trend = next(
        cell
        for cell in product.book_evidence
        if cell.timeframe == OBSERVED_TIMEFRAME and cell.regime == "TREND"
    )
    state = trend.process_state_decision.to_dict()
    binding = state["insufficient_evidence_binding"]
    assert state["state_kind"] == book.INSUFFICIENT_EVIDENCE
    assert state["attempted_trials_exact"] == 0
    assert binding["terminal_stage"] == book.INSUFFICIENT_PRESEARCH_STAGE
    assert binding["horizon_plan_receipt_sha256"] == boundary_plan.receipt_sha256
    assert binding["process_rejected_attempt_count"] == 0
    assert product.optimizer_payload["state_counts"][book.INSUFFICIENT_EVIDENCE] == 1


def test_all_process_rejected_raw_winner_becomes_bound_insufficient_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=fold_plan,
    )
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )
    handle = capture.freeze_observed_cell(
        OBSERVED_TIMEFRAME,
        "TREND",
        native_prefix_product=observed_native_prefix,
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=fold_plan,
        attempt_artifacts=[artifact],
        source_contract_seed=source_seed,
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        risk_unit_contract=RISK_CONTRACT,
    )
    attempt = {
        "attempt_id": handle.frozen.attempt_ledger["attempts"][0]["attempt_id"],
        "folds": [
            _raw_fold(index, planned, handle) for index, planned in enumerate(fold_plan)
        ],
    }
    attempt["folds"][0]["lanes"]["optimizer_train_base"]["position_events"] = attempt[
        "folds"
    ][0]["lanes"]["optimizer_train_base"]["position_events"][:2]
    replay_receipt = attempt["folds"][0]["provenance"]["attempt_replay_receipt"]
    replay_receipt["lane_sha256"]["optimizer_train_base"] = _sha(
        attempt["folds"][0]["lanes"]["optimizer_train_base"]
    )
    replay_receipt_body = dict(replay_receipt)
    replay_receipt_body.pop("receipt_sha256")
    replay_receipt["receipt_sha256"] = _sha(replay_receipt_body)
    cross_binding = json.loads(handle._cross_binding_bytes.decode("ascii"))
    assert (
        cross_binding["schema_version"] == adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA
    )
    assert cross_binding["mapping_count"] == 1
    assert cross_binding["mapping_sha256"] == _sha(cross_binding["mapping"])
    mapping = cross_binding["mapping"][0]
    assert (
        mapping["horizon_trial_definition_sha256"] != mapping["trial_definition_sha256"]
    )
    assert (
        handle.cross_binding_descriptor["sha256"]
        == hashlib.sha256(handle._cross_binding_bytes).hexdigest()
    )
    production = capture.complete_observed_cell(
        handle,
        attempt_evidence=[attempt],
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
    )
    assert production.decision.status == "NO_PROCESS_WINNER"
    product = capture.finish()
    trend = next(
        cell
        for cell in product.book_evidence
        if cell.timeframe == OBSERVED_TIMEFRAME and cell.regime == "TREND"
    )
    state = trend.process_state_decision.to_dict()
    binding = state["insufficient_evidence_binding"]
    receipt_descriptor = production.entries["production-receipt"]["descriptor"]
    assert state["state_kind"] == book.INSUFFICIENT_EVIDENCE
    assert state["attempted_trials_exact"] == 1
    assert binding["terminal_stage"] == book.INSUFFICIENT_POSTSEARCH_STAGE
    assert binding["process_rejection_request_sha256"] == (
        production.request.request_sha256
    )
    assert binding["process_rejection_decision_sha256"] == (
        production.decision.receipt_sha256
    )
    assert binding["horizon_winner_cross_binding_schema_version"] == (
        adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA
    )
    assert (
        binding["horizon_winner_cross_binding_content_sha256"]
        == (handle.cross_binding_descriptor["sha256"])
    )
    assert (
        binding["horizon_winner_cross_binding_mapping_sha256"]
        == (cross_binding["mapping_sha256"])
    )
    assert binding["winner_raw_production_schema_version"] == (
        raw_winner.PRODUCTION_SCHEMA
    )
    assert (
        binding["winner_raw_production_receipt_sha256"] == receipt_descriptor["sha256"]
    )
    assert product.optimizer_payload["state_counts"][book.INSUFFICIENT_EVIDENCE] == 1


def test_raw_production_receipt_reopen_pins_bounded_spool_primitive(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, book.PINNED_SYMBOL_ORDER[0])
    payload = _canonical(
        {
            "schema_version": raw_winner.PRODUCTION_SCHEMA,
            "probe": "adapter-bounded-reopen",
        }
    )
    entry = sink.put_opaque(
        "adapter/reopen-probe",
        "winner_raw_evidence_production_receipt",
        (payload,),
        format=raw_winner.ASCII_JSON_FORMAT,
    )
    descriptor = entry["descriptor"]
    assert adapter._reopen_winner_raw_production_receipt(context, descriptor) == payload

    original_stream = spool._stream_file

    def drifted_stream(*args, **kwargs):
        digest, size, encoded = original_stream(*args, **kwargs)
        if kwargs.get("collect") is True:
            digest = "0" * 64
        return digest, size, encoded

    monkeypatch.setattr(spool, "_stream_file", drifted_stream)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="WINNER_RAW_PRODUCTION_RECEIPT_REOPEN_FAILED",
    ):
        adapter._reopen_winner_raw_production_receipt(context, descriptor)


def test_missing_observed_cell_cannot_be_defaulted(tmp_path):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    _bind_terminal_symbol(capture, anchor)
    first_key = (book.TIMEFRAME_ORDER[0], book.REGIME_ORDER[0])
    capture._evidence.pop(first_key)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="BOOK_EVIDENCE_ROSTER_INCOMPLETE",
    ):
        capture.finish()


def _anchor_inputs():
    anchors = []
    plans = {}
    cutoff = date(2010, 1, 1)
    cells = tuple(causal.expected_cell_roster())
    for index in range(4):
        anchor_id = f"anchor-{index + 1:02d}"
        anchor = _anchor(anchor_id, cutoff)
        roster = []
        per_symbol = {}
        for symbol in book.PINNED_SYMBOL_ORDER:
            symbol_plans = {}
            for timeframe in book.TIMEFRAME_ORDER:
                values = _plans(anchor, symbol, timeframe, source_available=False)
                symbol_plans[timeframe] = values
                roster.extend(
                    orchestration.historical_horizon_plan_binding(plan)
                    for plan in values
                )
            per_symbol[symbol] = symbol_plans
        assert len(roster) == len(cells)
        anchor["horizon_binding_roster_sha256"] = (
            orchestration.historical_horizon_binding_roster_sha256(roster)
        )
        anchors.append(anchor)
        plans[anchor_id] = per_symbol
        cutoff = date.fromisoformat(anchor["forward_end_date"]) + timedelta(days=36)
    return anchors, plans


def test_orchestration_consumes_exact_100_by_6_by_4_adapter_stream(tmp_path):
    context = _context(tmp_path)
    anchors, plans = _anchor_inputs()
    precommit = orchestration.precommit_historical_anchor_run(
        context,
        run_id="historical-run-001",
        process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
        optimizer_producer_release_sha256=(adapter.HISTORICAL_PRODUCER_RELEASE_SHA256),
        anchors=anchors,
    )
    producers = {}
    for anchor in anchors:
        anchor_id = anchor["anchor_id"]

        def producer(request, anchor=anchor, anchor_id=anchor_id):
            for symbol in book.PINNED_SYMBOL_ORDER:
                capture = adapter.open_historical_symbol_capture(
                    context, request, symbol
                )
                for timeframe in book.TIMEFRAME_ORDER:
                    source, fit = _terminal_authority(anchor_id, symbol, timeframe)
                    capture.bind_timeframe(
                        timeframe,
                        horizon_plans=plans[anchor_id][symbol][timeframe],
                        source_group_decision=source,
                        regime_fit_decision=fit,
                    )
                yield capture.finish()

        producers[anchor_id] = producer

    receipt = orchestration.execute_historical_anchor_run(context, precommit, producers)
    assert orchestration.verify_historical_anchor_run_receipt(
        context, precommit, receipt
    )
    body = receipt.to_dict()
    assert body["symbol_count"] == 100
    assert len(body["anchors"]) == 4
    assert all(
        item["accounting_slot_count"] == 100 * 6 * 4
        and item["historical_state_counts"]["SOURCE_UNAVAILABLE"] == 100 * 6 * 4
        for item in body["anchors"]
    )


def test_adapter_release_contract_rejects_unbound_request(tmp_path):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    body = request.to_dict()
    body["optimizer_producer_release_sha256"] = "f" * 64
    forged = orchestration.AnchorProducerRequest(_canonical(body))
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ANCHOR_REQUEST_CONTRACT_INVALID",
    ):
        adapter.open_historical_symbol_capture(
            context, forged, book.PINNED_SYMBOL_ORDER[0]
        )


def test_capture_uses_exact_request_sink_and_never_opens_private_sink(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    expected = request.symbol_sink(symbol)

    def forbidden(*args, **kwargs):
        raise AssertionError("adapter attempted to open a private sink")

    monkeypatch.setattr(spool, "open_symbol_sink", forbidden)
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    assert capture.sink is expected


def test_direct_request_without_orchestration_sinks_fails_typed(tmp_path):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor, bind_sinks=False)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ORCHESTRATION_SYMBOL_SINK_UNAVAILABLE",
    ):
        adapter.open_historical_symbol_capture(
            context, request, book.PINNED_SYMBOL_ORDER[0]
        )


def test_incomplete_executable_artifact_fails_before_raw_freeze(
    tmp_path, monkeypatch, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=fold_plan,
    )
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )
    artifact["refit_recipe"].pop("seed")
    called = False

    def forbidden(*args, artifact_namespace, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("raw freeze received an unverified artifact")

    monkeypatch.setattr(raw_winner, "freeze_winner_search_plan_v3", forbidden)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ATTEMPT_0000_EXECUTABLE_ARTIFACT_INVALID",
    ):
        capture.freeze_observed_cell(
            OBSERVED_TIMEFRAME,
            "TREND",
            native_prefix_product=observed_native_prefix,
            precommitted_at_utc="2020-01-01T00:00:00Z",
            attempts_frozen_at_utc="2020-01-02T00:00:00Z",
            fold_plan=fold_plan,
            attempt_artifacts=[artifact],
            source_contract_seed=source_seed,
            base_cost_contract=BASE_COST_POLICY,
            stress_cost_contract=STRESS_COST_POLICY,
            risk_unit_contract=RISK_CONTRACT,
        )
    assert not called


def test_attempt_source_count_is_recomputed_from_sealed_prefix(
    tmp_path, monkeypatch, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    caller_asserted = _attempt_artifact(
        symbol,
        native_prefix_product=observed_native_prefix,
        declared_source_bar_count=499,
    )
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=caller_asserted,
        fold_plan=fold_plan,
    )
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )
    raw_called = False

    def forbidden(*args, **kwargs):
        nonlocal raw_called
        raw_called = True
        raise AssertionError("raw freeze received a caller-asserted source count")

    monkeypatch.setattr(raw_winner, "freeze_winner_search_plan_v3", forbidden)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ATTEMPT_0000_EXECUTABLE_ARTIFACT_INVALID",
    ):
        capture.freeze_observed_cell(
            OBSERVED_TIMEFRAME,
            "TREND",
            native_prefix_product=observed_native_prefix,
            precommitted_at_utc="2020-01-01T00:00:00Z",
            attempts_frozen_at_utc="2020-01-02T00:00:00Z",
            fold_plan=fold_plan,
            attempt_artifacts=[caller_asserted],
            source_contract_seed=source_seed,
            base_cost_contract=BASE_COST_POLICY,
            stress_cost_contract=STRESS_COST_POLICY,
            risk_unit_contract=RISK_CONTRACT,
        )
    assert not raw_called


def test_duplicate_depth_same_trial_is_distinct_when_identity_is_horizon_bound(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    attempts = [
        _attempt_artifact(
            symbol,
            native_prefix_product=observed_native_prefix,
            lookback_kind="ACTIVE_SESSIONS",
            lookback_value=50,
        ),
        _attempt_artifact(
            symbol,
            native_prefix_product=observed_native_prefix,
            lookback_kind="CALENDAR_INTERVAL",
            lookback_value=120,
        ),
    ]
    assert (
        attempts[0]["winner_process"]["process_candidate_id"]
        != attempts[1]["winner_process"]["process_candidate_id"]
    )
    assert _sha(attempts[0]["horizon_process"]) != _sha(attempts[1]["horizon_process"])
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=attempts[0],
        additional_attempt_artifacts=attempts[1:],
        fold_plan=fold_plan,
    )
    assert len(plan.to_dict()["attempts"]) == 2
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )

    handle = capture.freeze_observed_cell(
        OBSERVED_TIMEFRAME,
        "TREND",
        native_prefix_product=observed_native_prefix,
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=fold_plan,
        attempt_artifacts=attempts,
        source_contract_seed=source_seed,
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        risk_unit_contract=RISK_CONTRACT,
    )
    frozen_attempts = handle.frozen.attempt_ledger["attempts"]
    assert len(frozen_attempts) == 2
    assert len({attempt["process_candidate_id"] for attempt in frozen_attempts}) == 2
    cross_binding = _read_descriptor_json(context, handle.cross_binding_descriptor)
    assert cross_binding["mapping_count"] == 2
    assert len({item["horizon_candidate_id"] for item in cross_binding["mapping"]}) == 2


def test_horizon_registry_splice_fails_before_winner_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    genuine_plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=fold_plan,
    )
    spliced_body = genuine_plan.to_dict()
    spliced_body["candidate_registry_sha256"] = "f" * 64

    class DuckPlan:
        receipt_sha256 = _sha(spliced_body)

        @staticmethod
        def to_dict():
            return spliced_body

    source, fit = _available_authority(
        symbol,
        OBSERVED_TIMEFRAME,
        _sha("registry-splice-source"),
        native_prefix_body=observed_native_prefix.native_prefix,
    )
    for timeframe in book.TIMEFRAME_ORDER[:-1]:
        terminal_source, terminal_fit = _terminal_authority(
            anchor["anchor_id"], symbol, timeframe
        )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=_plans(anchor, symbol, timeframe, source_available=False),
            source_group_decision=terminal_source,
            regime_fit_decision=terminal_fit,
        )
    plans = [DuckPlan()]
    plans.extend(
        _real_unobserved_horizon_plan(
            anchor,
            symbol,
            OBSERVED_TIMEFRAME,
            regime,
            native_prefix_body=observed_native_prefix.native_prefix,
            source_decision=source,
            fit_decision=fit,
        )
        for regime in book.REGIME_ORDER[1:]
    )
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_PLAN_OBJECT_INVALID",
    ):
        capture.bind_timeframe(
            OBSERVED_TIMEFRAME,
            horizon_plans=plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def test_cross_anchor_raw_artifacts_share_sink_without_id_collision(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    first_anchor = _anchor("anchor-01", date(2020, 1, 1))
    second_anchor = _anchor("anchor-02", date(2022, 1, 1))
    first_request = _request(context, first_anchor)
    unbound_second = _request(context, second_anchor, bind_sinks=False)
    second_request = orchestration.AnchorProducerRequest(
        unbound_second._body_json, first_request._symbol_sinks
    )
    symbol = book.PINNED_SYMBOL_ORDER[0]
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    source_sha = sample.source_contract.source_contract_sha256
    handles = []
    for anchor, request in (
        (first_anchor, first_request),
        (second_anchor, second_request),
    ):
        capture = adapter.open_historical_symbol_capture(context, request, symbol)
        attempt_artifact = _attempt_artifact(
            symbol, native_prefix_product=observed_native_prefix
        )
        _bind_single_observed_capture(
            capture,
            anchor,
            source_sha,
            observed_plan=_real_horizon_plan(
                anchor,
                symbol,
                "TREND",
                native_prefix_product=observed_native_prefix,
                attempt_artifact=attempt_artifact,
                fold_plan=fold_plan,
            ),
        )
        handles.append(
            capture.freeze_observed_cell(
                OBSERVED_TIMEFRAME,
                "TREND",
                native_prefix_product=observed_native_prefix,
                precommitted_at_utc="2020-01-01T00:00:00Z",
                attempts_frozen_at_utc="2020-01-02T00:00:00Z",
                fold_plan=fold_plan,
                attempt_artifacts=[attempt_artifact],
                source_contract_seed=source_seed,
                base_cost_contract=BASE_COST_POLICY,
                stress_cost_contract=STRESS_COST_POLICY,
                risk_unit_contract=RISK_CONTRACT,
            )
        )
    assert handles[0].artifact_namespace != handles[1].artifact_namespace
    sink = first_request.symbol_sink(symbol)
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(
        context.root,
        receipt,
        expected_generation_id=context.generation_id,
        expected_symbol=symbol,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    ids = {entry["artifact_id"] for entry in manifest["artifacts"]}
    assert (
        "historical-anchor/historical-run-001/anchor-01/"
        f"winner-search/{OBSERVED_TIMEFRAME}/TREND/frozen/precommitment"
    ) in ids
    assert (
        "historical-anchor/historical-run-001/anchor-02/"
        f"winner-search/{OBSERVED_TIMEFRAME}/TREND/frozen/precommitment"
    ) in ids
