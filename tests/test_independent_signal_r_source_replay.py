"""INVENTED six-bar source/profile metadata; actual M1 math, no market authority."""
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
import json
import hashlib

import numpy as np
import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_core
import pm_production_config_binding as config_binding
import pm_signal_opportunities as opportunities
from pm_spread_profile import FrozenSpreadTier, TIER_SCHEMA
from test_independent_signal_r_production_raw import _capture_case
from test_signal_opportunities import FixedStops


class InventedRegisteredFixedStops(FixedStops):
    """A registry stand-in using genuine fixed-stop and signal interfaces."""
    name = "INVENTED_SOURCE_REPLAY_FIXED_STOPS"
    __module__ = execution.StrategyRegistry.__module__

    def __init__(self, values):
        super().__init__()
        self.values = tuple(values)

    def generate_signals(self, features, symbol):
        return pd.Series(self.values, index=features.index, dtype=np.int32)


def _source_case(monkeypatch, *, earlier=False, ambiguous=False, boundary_gap=False):
    case = _capture_case()
    frame, spec = case["frame"], case["spec"]
    if earlier or ambiguous:
        frame.loc[frame.index[2], "High"] = 1.001
    if ambiguous:
        frame.loc[frame.index[2], "Low"] = 0.999
    if boundary_gap:
        frame.loc[frame.index[3], ["Open", "High", "Low", "Close"]] = [1.002, 1.0021, 1.0019, 1.002]
    monkeypatch.setitem(execution.StrategyRegistry._strategies,
        InventedRegisteredFixedStops.name, InventedRegisteredFixedStops)
    case["tiers"] = {tier: FrozenSpreadTier(json.dumps({
        "schema_version": TIER_SCHEMA, "profile_sha256": execution._sha256("INVENTED_PROFILE"),
        "symbol": spec.symbol, "timeframe": "H1", "tier": tier, "source_timeframe": "M1",
        "point_price": spec.point, "spread_points": points, "full_spread_price": points * spec.point,
        "qualification_authority": False}, sort_keys=True))
        for tier, points in (("P75", 2), ("P95", 4))}
    case["observations"] = pd.Series(1.0, index=frame.index)
    case["config"] = case["pricing"].config
    return case


def _facts(case, *, first=0, end_bar=6):
    frame, spec, config = case["frame"], case["spec"], case["config"]
    end = frame["_BAR_CLOSE_TS"].iloc[-1] if end_bar == len(frame) else frame.index[end_bar]
    base = frame.loc[(frame.index >= frame.index[first]) & (frame.index < end)]
    geometry = {"provenance": "INVENTED_SOFTWARE_ONLY", "frame_start_utc": frame.index[first].isoformat(),
        "end_exclusive_utc": pd.Timestamp(end).isoformat(), "frame_rows": len(base),
        "frame_sha256": execution._frame_sha256(base), "warmup_bars": 1}
    geometry["receipt_sha256"] = execution._sha256(geometry)
    parameters = {"values": [int(value) for value in case["signals"]]}
    strategy = execution.StrategyRegistry.get(InventedRegisteredFixedStops.name, **parameters)
    fitted = SimpleNamespace(regime_parameters={}, regime_fit_decision=SimpleNamespace(
        to_dict=lambda: {"regime_params_sha256": execution._sha256("INVENTED_REGIME_PARAMETERS")}))
    surface = execution._build_full_prefix_strategy_surface(frame=frame, strategy=strategy,
        fitted=fitted, symbol=spec.symbol, timeframe="H1")
    surface_sha = execution._sha256({"frame_sha256": execution._frame_sha256(surface.features),
        "raw_signals_sha256": hashlib.sha256(surface.raw_signals.to_numpy(dtype="<i4").tobytes()).hexdigest()})
    config_sha = execution._independent_r_configuration_sha256(config)
    index = {"schema_version": "fxpm.independent-signal-source-index.v1",
        "partition_geometry": geometry, "instrument_spec": asdict(spec),
        "strategy_id": strategy.name, "parameters": parameters, "configuration_sha256": config_sha,
        "regime_parameters": {}, "regime_parameters_sha256": execution._sha256("INVENTED_REGIME_PARAMETERS"),
        "source_prefix_frame_sha256": execution._frame_sha256(frame), "surface_sha256": surface_sha,
        "source_reference": {"provenance": "INVENTED_NO_NATIVE_OR_PROFILE_AUTHORITY"}, "session_spec": None,
        "half_spread_arrays": {}, "numerical_execution_cost_attestations": {}, "spread_model_metadata": {}}
    episodes = opportunities.independent_entry_episodes(frame, symbol=spec.symbol,
        timeframe="H1", cutoff_utc=frame["_BAR_CLOSE_TS"].iloc[-1])
    batches = {}
    for tier in ("P75", "P95"):
        pricing, prepared = pm_core.Backtester(config), {}
        probe = pricing.build_evidence_source_seed_result(base, spec.symbol, spec=spec, timeframe="H1",
            spread_tier=case["tiers"][tier], entry_spread_observations=case["observations"].loc[base.index],
            _prepared_inputs_out=prepared)
        index["half_spread_arrays"][tier] = {name: execution._route_a_encode_spread_array(prepared[name], name)
            for name in ("entry_half_spread", "intrabar_half_spread")}
        index["numerical_execution_cost_attestations"][tier] = probe["numerical_execution_cost_attestation"]
        index["spread_model_metadata"][tier] = probe["spread_model"]
        provenance = opportunities.OpportunityProvenance(strategy.name, config_sha, surface_sha, tier,
            execution._sha256(probe["numerical_execution_cost_attestation"]), execution._sha256(probe["spread_admission"]))
        batches[tier] = asdict(opportunities.evaluate_independent_signal_r(
            surface.features.loc[base.index].copy(deep=True), surface.raw_signals.loc[base.index].copy(deep=True),
            pricing=pricing, episode_ids=episodes.loc[base.index], strategy=strategy, spec=spec,
            symbol=spec.symbol, timeframe="H1", cutoff_utc=end,
            signal_start_utc=base["_BAR_CLOSE_TS"].iloc[0],
            entry_half_spreads=prepared["entry_half_spread"], exit_half_spreads=prepared["intrabar_half_spread"],
            entry_admission=prepared["admission_states"], provenance=provenance))
        batches[tier]["records"] = list(batches[tier]["records"])
    return {"index": index, "batches": batches}, geometry


def _verify(case, facts, geometry, *, cache=None, target_regime=None):
    return execution._verify_independent_r_fact_source(facts, geometry, source_frame=case["frame"],
        spec=case["spec"], tiers=case["tiers"], config=case["config"], timeframe="H1",
        _prepared_source_cache=cache, entry_observations=case["observations"], target_regime=target_regime)


def test_actual_cold_source_replay_accepts_real_m1_batches(monkeypatch):
    case = _source_case(monkeypatch)
    facts, geometry = _facts(case)
    assert _verify(case, facts, geometry) is True
    assert all(batch["records"][0]["status"] == "CLOSED" for batch in facts["batches"].values())


@pytest.mark.parametrize("change", ["missed_earlier_trigger", "wrong_ambiguous_branch", "window_crossing"])
def test_actual_source_replay_refuses_wrong_trigger_branch_and_window(monkeypatch, change):
    case = _source_case(monkeypatch, earlier=True, ambiguous=change == "wrong_ambiguous_branch")
    facts, geometry = _facts(case)
    row = facts["batches"]["P75"]["records"][0]
    assert row["exit_bar"] == 2
    if change == "missed_earlier_trigger":
        row.update(exit_bar=5, exit_bar_open_utc=case["frame"].index[5].isoformat(),
            exit_observed_at_utc=case["frame"]["_BAR_CLOSE_TS"].iloc[5].isoformat())
    elif change == "wrong_ambiguous_branch":
        assert row["exit_reason"] == "SL"
        row.update(exit_reason="TP", exit_fill="TARGET_LEVEL", exit_price=row["take_profit_price"])
    else:
        row["exit_observed_at_utc"] = (pd.Timestamp(geometry["end_exclusive_utc"]) + pd.Timedelta(hours=1)).isoformat()
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="INDEPENDENT_R_OUTCOME_SOURCE_CHANGED"):
        _verify(case, facts, geometry)


def test_actual_source_replay_refuses_tampered_cost_array(monkeypatch):
    case = _source_case(monkeypatch)
    facts, geometry = _facts(case)
    original = facts["index"]["half_spread_arrays"]["P75"]["entry_half_spread"]
    values = execution._route_a_decode_spread_array(original, "entry_half_spread").copy()
    values[0] += case["spec"].point
    facts["index"]["half_spread_arrays"]["P75"]["entry_half_spread"] = execution._route_a_encode_spread_array(values, "entry_half_spread")
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="INDEPENDENT_R_COST_SOURCE_CHANGED"):
        _verify(case, facts, geometry)


def test_exact_end_open_is_excluded_and_later_prefix_trigger_cannot_resolve_window(monkeypatch):
    case = _source_case(monkeypatch, boundary_gap=True)
    bounded, geometry = _facts(case, end_bar=3)
    complete, _ = _facts(case)
    assert complete["batches"]["P75"]["records"][0]["exit_bar"] == 3
    for batch in bounded["batches"].values():
        entered = [row for row in batch["records"] if row["entry_open_utc"] is not None]
        assert entered
        assert all(pd.Timestamp(row["entry_open_utc"]) < pd.Timestamp(geometry["end_exclusive_utc"]) for row in entered)
        assert all(row["status"] == "RIGHT_CENSORED" and row["modeled_net_r"] is None for row in entered)
    assert _verify(case, bounded, geometry) is True


def test_cold_replay_reuses_one_strategy_surface_and_each_unique_window_tier_cost(monkeypatch):
    case = _source_case(monkeypatch)
    first, first_geometry = _facts(case)
    second, second_geometry = _facts(case, first=2)
    counts = {"surface": 0, "cost": 0}
    real_surface = execution._build_full_prefix_strategy_surface
    real_cost = execution.Backtester.build_evidence_source_seed_result

    def counted_surface(*args, **kwargs):
        counts["surface"] += 1
        return real_surface(*args, **kwargs)

    def counted_cost(self, *args, **kwargs):
        counts["cost"] += 1
        return real_cost(self, *args, **kwargs)

    monkeypatch.setattr(execution, "_build_full_prefix_strategy_surface", counted_surface)
    monkeypatch.setattr(execution.Backtester, "build_evidence_source_seed_result", counted_cost)
    cache = {}
    held_surface = None
    held_frame = None
    for facts, geometry in ((first, first_geometry), (second, second_geometry),
            (deepcopy(first), first_geometry), (deepcopy(second), second_geometry)):
        assert _verify(case, facts, geometry, cache=cache) is True
        entries = {key: value for key, value in cache.items()
            if isinstance(key, tuple) and key[0] == "independent-surface"}
        assert len(entries) == 1
        current = next(iter(entries.values()))[2]
        if held_surface is None:
            held_surface, held_frame = current, current.features.copy(deep=True)
        assert current is held_surface
        pd.testing.assert_frame_equal(current.features, held_frame)
    assert counts == {"surface": 1, "cost": 4}
    old_key = next(iter(entries))
    changed = dict(case, signals=pd.Series([1, 0, 1, 1, 1, 0], index=case["frame"].index))
    changed_facts, changed_geometry = _facts(changed)
    before = dict(counts)
    assert _verify(changed, changed_facts, changed_geometry, cache=cache) is True
    assert old_key not in cache
    assert len([key for key in cache if isinstance(key, tuple) and key[0] == "independent-surface"]) == 1
    assert counts == {"surface": before["surface"] + 1, "cost": before["cost"]}


def _producer_case(monkeypatch, *, mixed=False):
    """Invented authority seams around actual production capture and M1 math."""
    import pm_historical_search_producer as producer
    case = _source_case(monkeypatch)
    if mixed:
        case["frame"]["REGIME"] = ["TREND", "RANGE", "TREND", "RANGE", "TREND", "RANGE"]
        case["frame"]["REGIME_LIVE"] = case["frame"]["REGIME"].shift(1).fillna("RANGE")
    _, base_geometry = _facts(case)
    partitions = []
    for role in ("optimizer_train", "tuning_validation", "final_lockbox"):
        geometry = {**base_geometry, "role": role}
        geometry["receipt_sha256"] = execution._sha256({key:value for key,value in geometry.items() if key != "receipt_sha256"})
        partitions.append(execution._PartitionSlice(role=role, frame=case["frame"], warmup_bars=1,
            scored_start_utc=case["frame"].index[1].isoformat(),
            end_exclusive_utc=geometry["end_exclusive_utc"], geometry_receipt=geometry))
    parameters = {"values": case["signals"].tolist()}
    monkeypatch.setattr(execution, "_runtime_strategy", lambda *a, **kw: (
        execution.StrategyRegistry.get(InventedRegisteredFixedStops.name, **parameters), parameters))
    monkeypatch.setattr(execution, "_causal_folds", lambda *a: [{"fold_id": "INVENTED_AUTHORITY_SEAM"}])
    monkeypatch.setattr(execution, "_fold_partitions", lambda **kw: partitions)
    monkeypatch.setattr(execution, "_source_observation_receipt", lambda *a: {
        "provenance": "INVENTED_NO_NATIVE_OR_PROFILE_AUTHORITY"})
    monkeypatch.setattr(execution, "_verify_partition_precommitment", lambda **kw: None)
    monkeypatch.setattr(execution.raw_winner, "build_trade_source_contract_from_backtester_result",
        lambda *a: case["lane"]["source_contract"])
    monkeypatch.setattr(execution, "_fold_provenance", lambda **kw: {"INVENTED_AUTHORITY_SEAM": True})
    fitted = SimpleNamespace(_frame=case["frame"], frame_sha256=execution._frame_sha256(case["frame"]),
        _numeric_preparation=producer._UnitNumericPreparation(), _numeric_key=("INVENTED_SOURCE", "INVENTED_FIT"),
        instrument_spec=case["spec"], _spread_tiers=case["tiers"],
        _entry_spread_observations=case["observations"], frozen_tier_pair_contract={},
        regime_parameters={}, regime_fit_decision=SimpleNamespace(to_dict=lambda: {
            "regime_params_sha256": execution._sha256("INVENTED_REGIME_PARAMETERS")}))
    plan = {"cell": {"symbol":case["spec"].symbol,"timeframe":"H1","regime":"TREND"},
        "anchor": {"selection_cutoff_utc":case["frame"]["_BAR_CLOSE_TS"].iloc[-1].isoformat()},
        "derived_dependency_geometry":{"warmup_bars":1}}
    def capture(prepared):
        return execution._build_independent_r_attempt_evidence(attempt=SimpleNamespace(), raw_attempt_id="INVENTED_ATTEMPT",
            plan_body=plan, plan_receipt_sha256="0"*64, fitted=fitted, config=case["config"],
            partition_cost_precommitments={}, source_observation_references=None, prepared_inputs=prepared)
    return case, capture


def test_production_and_cold_replay_apply_entry_live_cell_regime_after_shared_surface(monkeypatch):
    case, capture = _producer_case(monkeypatch, mixed=True)
    body = capture({})
    fold = body["folds"][0]
    frame = case["frame"]
    expected_clock = [(1, 2), (3, 4)]
    for lane in fold["lanes"].values():
        records = lane["batch"]["records"]
        assert [(row["signal_bar"], row["entry_bar"]) for row in records] == expected_clock
        for row, (signal, entry) in zip(records, expected_clock, strict=True):
            assert frame["REGIME_LIVE"].iat[signal] == "TREND"
            assert frame["REGIME"].iat[signal] == "RANGE"
            assert row["signal_regime"] == row["entry_regime"] == "TREND"
            entry_open = frame.index[entry].isoformat().replace("+00:00", "Z")
            assert row["entry_open_utc"] == entry_open
            assert row["episode_id"] == f"{case['spec'].symbol}/H1/TREND/{entry_open}"
    facts = {"index":fold["independent_signal_r_indexes"]["final_lockbox"],
        "batches":{tier:fold["lanes"]["final_lockbox_"+tier.lower()]["batch"] for tier in ("P75","P95")}}
    geometry = facts["index"]["partition_geometry"]
    assert _verify(case, facts, geometry, target_regime="TREND") is True
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="INDEPENDENT_R_OUTCOME_SOURCE_CHANGED"):
        _verify(case, facts, geometry, target_regime="RANGE")


def test_production_reuses_episode_axis_and_array_encodings_at_their_existing_owner(monkeypatch):
    case, capture = _producer_case(monkeypatch)
    counts = {"episodes":0,"axis":0,"encode":0}
    def counted(name, function):
        def call(*a, **kw):
            counts[name] += 1
            return function(*a, **kw)
        return call
    monkeypatch.setattr(opportunities, "independent_entry_episodes", counted("episodes", opportunities.independent_entry_episodes))
    monkeypatch.setattr(pm_core, "build_backtest_partition_axis", counted("axis", pm_core.build_backtest_partition_axis))
    monkeypatch.setattr(execution, "_route_a_encode_spread_array", counted("encode", execution._route_a_encode_spread_array))
    prepared = {}
    first, second = capture(prepared), capture(prepared)
    assert counts == {"episodes":1,"axis":3,"encode":12}
    for role in ("optimizer_train", "tuning_validation", "final_lockbox"):
        left = first["folds"][0]["independent_signal_r_indexes"][role]["half_spread_arrays"]
        right = second["folds"][0]["independent_signal_r_indexes"][role]["half_spread_arrays"]
        assert left == right
        for tier in ("P75", "P95"):
            for side in ("entry_half_spread", "intrabar_half_spread"):
                assert left[tier][side] is right[tier][side]


def test_v19_session_admission_counters_pass_strict_raw_reconstruction(monkeypatch):
    import pm_winner_raw_evidence as raw
    from pm_spread_profile import spread_admission

    class SessionStops(FixedStops):
        def session_spec(self, symbol):
            return (0, 120)

    case = _source_case(monkeypatch)
    frame, spec = case["frame"], case["spec"]
    strategy = SessionStops()
    signals = case["signals"]
    observations = pd.Series([1., 1., 3., 5., np.nan, 1.], index=frame.index)
    observations_sha = hashlib.sha256(pd.util.hash_pandas_object(
        observations, index=True).values.tobytes()).hexdigest()
    axis = pm_core.build_backtest_partition_axis(frame, timeframe="H1", warmup_bars=1)
    cutoff = pd.Timestamp(axis["end_utc"])
    episodes = opportunities.independent_entry_episodes(
        frame, symbol=spec.symbol, timeframe="H1", cutoff_utc=cutoff)
    entry_bars = np.flatnonzero(signals.to_numpy()[:-1]) + 1
    expected = {"P75": (5, 2, 2, 1, 0), "P95": (5, 3, 1, 1, 0)}
    counter_keys = ("signal_opportunities", "known_cap_pass_opportunities",
        "known_cap_refused_opportunities", "missing_refused_opportunities", "missing_executed_legs")
    reconstructed, lanes = [], {}
    for tier, contract in case["tiers"].items():
        pricing, prepared = pm_core.Backtester(case["config"]), {}
        probe = pricing.build_evidence_source_seed_result(frame, spec.symbol, spec=spec, timeframe="H1",
            spread_tier=contract, entry_spread_observations=observations, _prepared_inputs_out=prepared)
        seed = probe["spread_admission"]
        before = dict(seed)
        provenance = opportunities.OpportunityProvenance(strategy.name, execution._sha256("INVENTED_CONFIG"),
            execution._sha256("INVENTED_SURFACE"), tier, execution._sha256(probe["numerical_execution_cost_attestation"]),
            execution._sha256(seed))
        batch = opportunities.evaluate_independent_signal_r(
            frame, signals, pricing=pricing, episode_ids=episodes, strategy=strategy, spec=spec,
            symbol=spec.symbol, timeframe="H1", cutoff_utc=cutoff, signal_start_utc=frame["_BAR_CLOSE_TS"].iloc[0],
            entry_half_spreads=prepared["entry_half_spread"], exit_half_spreads=prepared["intrabar_half_spread"],
            entry_admission=prepared["admission_states"], provenance=provenance)
        admission = execution._independent_r_spread_admission(seed, entry_bars=entry_bars,
            admission_states=prepared["admission_states"], records=batch.records)
        assert seed == before and all(key not in seed for key in counter_keys)
        assert tuple(admission[key] for key in counter_keys) == expected[tier]
        assert any(row.exit_reason == "SESSION" for row in batch.records)
        assert any(row.eligibility_reason == "STRATEGY_SESSION_REFUSED" for row in batch.records)
        states = np.array([spread_admission(contract, point) for point in observations])
        mask = states == "ADMIT"
        mask_sha = hashlib.sha256(mask.astype(np.uint8).tobytes()).hexdigest()
        reconstructed.append((tier, states, mask, mask_sha))
        lanes["optimizer_train_" + tier.lower()] = {
            "schema_version": raw.INDEPENDENT_R_PARTITION_SCHEMA,
            "trade_power_partition_axis": axis, "batch": asdict(batch),
            "spread_model": probe["spread_model"], "spread_admission": admission,
        }

    transport = {"schema_version": raw.SOURCE_OBSERVATIONS_SCHEMA, "symbol": spec.symbol,
        "timeframe": "H1", "source_timeframe": "M1", "observations": [], "entry_observations_sha256": observations_sha,
        "source_product": {"data_dir": "INVENTED_UNIT_TRANSPORT", "index_descriptor": {},
            "index_sha256": "0" * 64, "symbol": spec.symbol, "timeframe": "M1",
            "cutoff_utc": cutoff.isoformat(), "activation_manifest_sha256": "1" * 64,
            "production_contract_required": False}}
    transport["receipt_sha256"] = raw._sha_json(transport)

    # The source state is independently reconstructed above. Exercise the strict
    # partition/receipt checks with the real session evaluator output.
    monkeypatch.setattr(raw, "_prepare_source_observation_receipt",
        lambda *_args, **_kwargs: (frame.index, observations_sha, tuple(reconstructed)))
    inputs = {"sink": None, "frozen": SimpleNamespace(symbol=spec.symbol, timeframe="H1"),
        "expected_sha256": transport["receipt_sha256"], "partition": "optimizer_train",
        "signals": {"values": signals.tolist()}, "lanes": lanes}
    assert raw._validated_source_observation_receipt(transport, **inputs) == transport
    lanes["optimizer_train_p75"]["spread_admission"]["known_cap_pass_opportunities"] += 1
    with pytest.raises(raw.WinnerRawEvidenceError, match="TIER_ADMISSION_RECONSTRUCTION_FAILED"):
        raw._validated_source_observation_receipt(transport, **inputs)
