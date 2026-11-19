"""Invented software-only Raw/Power contracts; no native source or trading qualification."""
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib

import numpy as np
import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_core
import pm_trade_power as power
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from pm_search_outcomes import SearchOutcomeKind
from test_independent_signal_r import _config, _evaluate
from test_signal_opportunities import FixedStops, _frame, _spec
from test_trade_power import _ar1, _lineage, _policy, _source_contract, _trade_records


def _capture_case(*, daily=False, hour=0):
    """Actual M1 numerics on invented bars; source labels here grant no authority."""
    frame = _frame(count=6)
    if daily:
        frame.index = pd.date_range(f"2024-01-01T{hour:02d}:00:00Z", periods=6, freq="D")
        frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(days=1)
    frame["REGIME"] = "TREND"
    frame.loc[frame.index[-1], "High"] = 1.001
    strategy, spec = FixedStops(), _spec()
    pricing = pm_core.Backtester(_config())
    values = [1, 1, 1, 1, 1, 0]
    batch = asdict(_evaluate(frame, values, strategy=strategy, spec=spec, pricing=pricing,
        timeframe="D1" if daily else "H1"))
    batch["records"] = list(batch["records"])
    axis = pm_core.build_backtest_partition_axis(frame, timeframe="D1" if daily else "H1", warmup_bars=1)
    entry, intrabar, spread_meta = pricing._build_half_spread_arrays(frame, spec, timeframe=axis["timeframe"])
    attestation = pricing._numerical_execution_cost_attestation(entry_half_spread=entry,
        intrabar_half_spread=intrabar, spread_meta=spread_meta, spec=spec)
    source_reference = {"provenance": "INVENTED_SOFTWARE_ONLY_NO_NATIVE_AUTHORITY"}
    lane = {"schema_version": raw.INDEPENDENT_R_PARTITION_SCHEMA, "trade_power_partition_axis": axis,
        "partition_axis": {"start_utc": axis["start_utc"], "end_utc": axis["end_utc"],
            "warmup_bars": 1, "observation_count": len(frame)}, "source_reference": source_reference,
        "batch": batch, "source_contract": _source_contract(), "numerical_execution_cost_attestation": attestation,
        "spread_model": spread_meta, "spread_admission": {}, "swap_accrual_contract": batch["cost_model"]["swap_contract"]}
    index = {"schema_version": "fxpm.independent-signal-source-index.v1",
        "partition_geometry": {"provenance": "INVENTED_SOFTWARE_ONLY"},
        "half_spread_arrays": {tier: {"entry_half_spread": execution._route_a_encode_spread_array(entry, "entry_half_spread"),
            "intrabar_half_spread": execution._route_a_encode_spread_array(intrabar, "intrabar_half_spread")}
            for tier in ("P75", "P95")}, "instrument_spec": asdict(spec),
        "numerical_execution_cost_attestations": {tier: attestation for tier in ("P75", "P95")},
        "spread_model_metadata": {tier: spread_meta for tier in ("P75", "P95")},
        "strategy_id": strategy.name, "parameters": {}, "source_reference": source_reference,
        "configuration_sha256": batch["provenance"]["configuration_sha256"], "regime_parameters": {},
        "regime_parameters_sha256": "0" * 64, "source_prefix_frame_sha256": execution._frame_sha256(frame),
        "surface_sha256": batch["provenance"]["source_surface_sha256"], "session_spec": None}
    lanes = {role + "_" + tier.lower(): deepcopy(lane) for role in raw._PARTITIONS for tier in ("P75", "P95")}
    fold = {"fold_ordinal": 0, "lanes": lanes, "independent_signal_r_indexes": {role: deepcopy(index) for role in raw._PARTITIONS}}
    return {"frame": frame, "strategy": strategy, "spec": spec, "pricing": pricing,
        "signals": pd.Series(values, index=frame.index), "lane": lane, "index": index, "fold": fold,
        "entry_half": entry, "intrabar_half": intrabar}


@pytest.mark.parametrize("hour", (0, 23), ids=("exact-midnight-end", "straddling-d1"))
def test_raw_independent_daily_r_uses_recorded_bar_day_and_exact_end_close(hour):
    case = _capture_case(daily=True, hour=hour)
    lane = case["lane"]
    axis = lane["trade_power_partition_axis"]
    start, end = pd.Timestamp(axis["start_utc"]).to_pydatetime(), pd.Timestamp(axis["end_utc"]).to_pydatetime()
    records, dates, daily, values, *_ = raw._independent_trade_vectors(lane, regime="TREND", start=start, end=end)
    assert len(records) == 5
    assert records[-1]["exit_observed_at_utc"] == lane["batch"]["cutoff_utc"]
    bucket = pd.Timestamp(records[0]["exit_bar_open_utc"]).date().isoformat()
    assert sum(daily) == sum(values)
    assert daily[dates.index(bucket)] == sum(values)
    assert all(value == 0 for day, value in zip(dates, daily) if day != bucket)
    assert dates[-1] == (pd.Timestamp(end) - pd.Timedelta(nanoseconds=1)).date().isoformat()
    if hour == 23:
        assert pd.Timestamp(records[0]["exit_observed_at_utc"]).date().isoformat() != bucket


@pytest.mark.parametrize("change,code", (
    ("observed-beyond-end", "INDEPENDENT_R_EXIT_OUTSIDE_WINDOW"),
    ("censored-with-r", "INDEPENDENT_R_CENSOR_CARRIES_RESULT"),
    ("legacy-method", "INDEPENDENT_R_BATCH_CONTRACT_INVALID"),
    ("account-claim", "INDEPENDENT_R_BATCH_CONTRACT_INVALID"),
), ids=("observed-beyond-end", "censored-with-r", "legacy-method", "account-claim"))
def test_raw_independent_refuses_crossed_window_censor_and_method(change, code):
    lane = _capture_case()["lane"]
    record = lane["batch"]["records"][0]
    if change == "observed-beyond-end":
        record["exit_observed_at_utc"] = (pd.Timestamp(lane["batch"]["cutoff_utc"]) + pd.Timedelta(seconds=1)).isoformat()
    elif change == "censored-with-r":
        record["status"] = "RIGHT_CENSORED"
    elif change == "legacy-method":
        lane["batch"]["method_edition"] = "fxpm.signal-opportunity-labels.v1"
    else:
        lane["batch"]["cost_model"]["account_compounding"] = True
    with pytest.raises(raw.WinnerRawEvidenceError) as caught:
        raw._independent_r_records(lane, regime="TREND")
    assert caught.value.code == code


def test_raw_independent_preserves_censor_and_original_entry_order_without_exit_sort():
    lane = _capture_case()["lane"]
    original = lane["batch"]["records"]
    original[-1] = {**original[-1], "status": "RIGHT_CENSORED", "modeled_net_r": None,
        "exit_bar": None, "exit_bar_open_utc": None, "exit_observed_at_utc": None}
    records = raw._independent_r_records(lane, regime="TREND")
    assert [row["opportunity_id"] for row in records] == [row["opportunity_id"] for row in original[:-1]]
    assert original[-1]["status"] == "RIGHT_CENSORED"
    assert all("held_bar_inputs" not in row for row in original)


def test_raw_independent_floor_uses_same_p75_exit_and_original_stop_without_volume():
    case = _capture_case()
    answer = raw._independent_floor_partition(case["fold"], "final_lockbox", regime="TREND")
    records = raw._independent_r_records(case["lane"], regime="TREND")
    expected = [pm_core.canonical_backtest_winner_r(
        float(case["entry_half"][row["entry_bar"]]) + float(case["intrabar_half"][row["exit_bar"]]),
        abs(row["entry_price"] - row["stop_loss_price"])) for row in records]
    assert answer["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"
    assert answer["matched_p95_spread_cost_r"] == expected
    assert answer["base_actual_initial_risk_net_r"] == [pm_core.canonical_backtest_winner_r(row["modeled_net_r"], 1.) for row in records]
    assert winner._v17_below_matched_stress_economic_floor(expected, expected) is False


def test_raw_independent_floor_refuses_changed_array_cost_attestation():
    case = _capture_case()
    index = case["fold"]["independent_signal_r_indexes"]["final_lockbox"]
    decoded = case["entry_half"].copy()
    decoded[1] += 0.1
    index["half_spread_arrays"]["P95"]["entry_half_spread"] = execution._route_a_encode_spread_array(decoded, "entry_half_spread")
    with pytest.raises(raw.WinnerRawEvidenceError, match="INDEPENDENT_R_INDEX_COST_CHANGED"):
        raw._independent_floor_partition(case["fold"], "final_lockbox", regime="TREND")


def _independent_events(legacy):
    return [{"trade_record_schema_version": power.INDEPENDENT_TRADE_RECORD_SCHEMA,
        "opportunity_id": "iso:" + hashlib.sha256(row["position_id"].encode()).hexdigest(),
        "entry_open_utc": row["entry_time"], "exit_observed_at_utc": row["exit_time"],
        "entry_event_order": row["entry_event_order"], "exit_event_order": row["exit_event_order"],
        "modeled_net_r": row["pnl_dollars"] / row["risk_amount"]} for row in legacy]


@pytest.mark.parametrize("crossing", ("old-events", "old-source", "wrong-method", "missing-source-binding"))
def test_independent_power_source_and_event_schemas_refuse_crossing(crossing):
    legacy = _trade_records(_ar1(20, 0., seed=101) + .3)
    rows = _independent_events(legacy)
    source = power.build_independent_trade_power_source_contract(_source_contract())
    if crossing == "old-events":
        rows = legacy
    elif crossing == "old-source":
        source = _source_contract()
    elif crossing == "wrong-method":
        source["method_edition"] = "invented-wrong-method"
    else:
        source["source_contract"]["source_binding"].pop("schedule_sha256")
    answer = power.build_trade_sample_evidence(rows, source_contract=source)
    assert answer.outcome_kind == SearchOutcomeKind.INFRA_FAILURE
    assert not answer.passed


def test_independent_power_freezes_same_math_and_validation_cannot_recalibrate(monkeypatch):
    values = _ar1(900, 0., seed=101) + .3
    legacy = _trade_records(values)
    multiplicity, train, tuning, _ = _lineage(25)
    old = power.calibrate_train_trade_power(legacy, policy=_policy(), multiplicity_receipt=multiplicity,
        partition_contract=train, source_contract=_source_contract())
    new = power.calibrate_train_trade_power(_independent_events(legacy), policy=_policy(), multiplicity_receipt=multiplicity,
        partition_contract=train, source_contract=power.build_independent_trade_power_source_contract(_source_contract()))
    assert old.passed and new.passed
    for field in ("required_effective_train_trades", "required_effective_tuning_trades", "required_effective_final_trades",
            "sigma_r_robust", "attempted_trials_exact"):
        assert getattr(old.frozen_requirement, field) == getattr(new.frozen_requirement, field)
    assert new.frozen_requirement.trade_record_schema_version == power.INDEPENDENT_TRADE_RECORD_SCHEMA
    assert new.sample_evidence.net_r_contract == "independent_modeled_net_r_per_initial_stop_risk_v1"
    def forbidden(*args, **kwargs):
        raise AssertionError("validation must not recalibrate the train requirement")
    monkeypatch.setattr(power, "derive_trade_power_requirement", forbidden)
    tested = power.evaluate_trade_power_partition(_independent_events(_trade_records(_ar1(900, 0., seed=102) + .3, base="2031-01-01")),
        frozen_requirement=new.frozen_requirement, partition="tuning_validation", partition_contract=tuning,
        prior_partition_contract=train, source_contract=power.build_independent_trade_power_source_contract(_source_contract(projection_sha256="d" * 64)))
    assert tested.passed, (tested.status, tested.reason_codes, tested.reason)
    assert tested.frozen_requirement == new.frozen_requirement


def test_independent_power_observation_at_exact_end_is_not_an_end_open_entry():
    rows = _trade_records(_ar1(900, 0., seed=101) + .3)
    rows[-1]["entry_time"] = pd.Timestamp("2029-12-31T00:00:00Z")
    rows[-1]["exit_time"] = pd.Timestamp("2030-01-01T00:00:00Z")
    multiplicity, train, _, _ = _lineage(25)
    new = power.calibrate_train_trade_power(_independent_events(rows), policy=_policy(), multiplicity_receipt=multiplicity,
        partition_contract=train, source_contract=power.build_independent_trade_power_source_contract(_source_contract()))
    old = power.calibrate_train_trade_power(rows, policy=_policy(), multiplicity_receipt=multiplicity,
        partition_contract=train, source_contract=_source_contract())
    assert new.outcome_kind != SearchOutcomeKind.INFRA_FAILURE
    assert old.outcome_kind == SearchOutcomeKind.INFRA_FAILURE


def test_independent_raw_versions_are_explicit_and_legacy_readers_stay_mapped():
    assert raw.bound_frozen_plan_schema_for_generation(winner.V19_GENERATION) == "fxpm.winner-search-plan-frozen.v11"
    assert raw.raw_production_schema_for_generation(winner.V19_GENERATION) == "fxpm.winner-raw-evidence-production.v9"
    assert raw.bound_frozen_plan_restorer_for_generation(winner.V19_GENERATION) is raw.restore_frozen_winner_search_plan_v11
    assert raw.bound_frozen_plan_restorer_for_generation(winner.V17_GENERATION) is raw.restore_frozen_winner_search_plan_v9
    assert raw.bound_frozen_plan_restorer_for_generation(winner.V18_GENERATION) is raw.restore_frozen_winner_search_plan_v10


def _authenticated_index_binding_case():
    """Invented bound-receipt values; parameter codec and registry are actual."""
    from types import SimpleNamespace
    from pm_strategies import StrategyRegistry, resolve_strategy_parameters
    from pm_current_refit_parameters import canonicalize_parameter_mapping

    strategy_id = "EMACrossoverStrategy"
    strategy = StrategyRegistry.get(strategy_id)
    defaults, _, _ = resolve_strategy_parameters(strategy, "H1", defaults_only=True)
    artifacts = {"winner_process": {"strategy_id": strategy_id},
        "trial_definition": {"parameters": canonicalize_parameter_mapping(defaults)}}
    metadata = {"frame_sha256": "f" * 64, "receipts": {"regime_parameters": {"INVENTED_FIT_PARAMETER": 7},
        "regime_fit_decision": {"regime_params_sha256": "a" * 64}}}
    expected = raw._independent_expected_index_binding(SimpleNamespace(timeframe="H1"), artifacts, metadata)
    fold = {"independent_signal_r_indexes": {role: deepcopy(expected) for role in raw._PARTITIONS}}
    return fold, expected


def test_independent_raw_index_binds_actual_parameter_codec_and_bound_fit_receipt():
    fold, expected = _authenticated_index_binding_case()
    assert raw._validate_independent_index_bindings(fold, expected) is None
    assert expected["strategy_id"] == "EMACrossoverStrategy"
    assert expected["regime_parameters"] == {"INVENTED_FIT_PARAMETER": 7}


@pytest.mark.parametrize("field", ("strategy_id", "parameters", "regime_parameters", "regime_parameters_sha256", "source_prefix_frame_sha256"))
def test_independent_raw_index_refuses_coherent_crossed_strategy_or_fit(field):
    fold, expected = _authenticated_index_binding_case()
    index = fold["independent_signal_r_indexes"]["final_lockbox"]
    if field == "parameters":
        index[field] = {**index[field], "INVENTED_UNPRECOMMITTED_AXIS": 2}
    elif field == "regime_parameters":
        index[field] = {"INVENTED_FIT_PARAMETER": 8}
    else:
        index[field] = "b" * 64
    with pytest.raises(raw.WinnerRawEvidenceError) as caught:
        raw._validate_independent_index_bindings(fold, expected)
    assert caught.value.code == "INDEPENDENT_R_PRECOMMITTED_INDEX_CHANGED"


def test_independent_index_arrays_publish_once_and_cold_expand_exactly(monkeypatch, tmp_path):
    """Invented capture: real two-attempt stage bytes, codec math and cold expansion."""
    import json
    from types import SimpleNamespace
    case = _capture_case()
    indexes = case["fold"]["independent_signal_r_indexes"]
    original = deepcopy(indexes)
    payloads, entries, calls = {}, {}, []
    actual_sha = raw._sha_json
    codec_hashes = []
    def sha(value):
        if isinstance(value, dict) and "values_base64" in value:
            codec_hashes.append(id(value))
        return actual_sha(value)
    def put(sink, artifact_id, artifact_type, value):
        digest = actual_sha(value)
        entry = {"artifact_id": artifact_id, "artifact_type": artifact_type, "descriptor": {"sha256": digest}}
        payloads[artifact_id] = deepcopy(value)
        entries[artifact_id] = entry
        calls.append(artifact_id)
        return entry
    monkeypatch.setattr(raw, "_put_ascii_json", put)
    monkeypatch.setattr(raw, "_sha_json", sha)
    frozen = SimpleNamespace(artifact_namespace="declared-slot/INVENTED_SOFTWARE", timeframe="H1", regime="TREND")
    cache = {}
    packed, _ = raw._pack_independent_index_arrays(sink=object(), frozen=frozen, indexes=indexes, array_entries=cache)
    first_hashes = len(codec_hashes)
    again, _ = raw._pack_independent_index_arrays(sink=object(), frozen=frozen, indexes=indexes, array_entries=cache)
    repacked, _ = raw._pack_independent_index_arrays(sink=object(), frozen=frozen, indexes=packed, array_entries=cache)
    assert len(codec_hashes) == first_hashes
    unique = {actual_sha(codec) for index in indexes.values()
        for sides in index["half_spread_arrays"].values() for codec in sides.values()}
    assert len(calls) == len(unique)
    assert packed == again == repacked and indexes == original
    consumed, materialized = [], []
    reservation = SimpleNamespace(consume=consumed.append, materialize=materialized.append)
    staged_folds = []
    for ordinal, prepared in enumerate((packed, again)):
        fold = {**case["fold"], "independent_signal_r_indexes": prepared}
        body = {"attempt_id": f"INVENTED_ATTEMPT_{ordinal}", "folds": [fold]}
        staged = execution._write_staged_attempt(stage=tmp_path, ordinal=ordinal, value=body, reservation=reservation)
        encoded = staged.path.read_bytes()
        assert b'"values_base64"' not in encoded
        assert staged.sha256 == hashlib.sha256(encoded).hexdigest() == actual_sha(body)
        assert staged.size_bytes == len(encoded)
        decoded = json.loads(encoded)
        assert decoded == body
        staged_folds.append(decoded["folds"][0])
    assert consumed == materialized and len(materialized) == 2
    assert len(calls) == len(unique)
    cold_cache, reads = {}, []
    def read(key):
        reads.append(key)
        return deepcopy(payloads[key])
    for staged_fold in staged_folds:
        expanded = raw._expand_independent_index_arrays(staged_fold["independent_signal_r_indexes"],
            read=read, entries=entries, array_entries=cold_cache)
        assert expanded == original
        for role in raw._PARTITIONS:
            expected = raw._independent_floor_partition(case["fold"], role, regime="TREND")
            assert raw._independent_floor_partition(staged_fold, role, regime="TREND",
                _independent_array_entries=cold_cache) == expected
    assert len(reads) == len(unique)
    crossed = deepcopy(packed)
    crossed["final_lockbox"]["half_spread_arrays"]["P95"]["intrabar_half_spread"]["descriptor"]["sha256"] = "f" * 64
    with pytest.raises(raw.WinnerRawEvidenceError) as caught:
        raw._expand_independent_index_arrays(crossed, read=read, entries=entries, array_entries=cold_cache)
    assert caught.value.code == "INDEPENDENT_R_ARRAY_REFERENCE_CHANGED"
