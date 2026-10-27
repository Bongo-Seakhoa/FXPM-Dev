"""Synthetic execution conformance; no market profitability evidence."""

import hashlib
import json
import numpy as np
import pandas as pd
import pytest

import pm_core
from pm_core import Backtester, InstrumentSpec, PipelineConfig
from pm_spread_profile import (
    ADMISSION_POLICY, ADMISSION_SCHEMA, RETIRED_COST_POLICY, SpreadProfileError, align_prior_completed_spreads,
    load_spread_profile, spread_admission,
)


SOURCE_ID = "0be6d0831806c1a62ab723193a87a362772a14d8bf4ccbe2d3a8774962eefe09"


@pytest.fixture(scope="module")
def profile():
    return load_spread_profile()


def tier(profile, level="P75"):
    return profile.resolve("EURUSD", "H1", level, point=0.00001,
                           source_identity_sha256=SOURCE_ID)


def test_profile_resolves_existing_values_without_calculating(profile):
    normal, stress = tier(profile), tier(profile, "P95")
    assert (normal.spread_points, stress.spread_points) == (9, 16)
    assert normal.source_timeframe == "M5"
    assert normal.descriptor()["qualification_authority"] is False
    assert profile.from_descriptor(normal.descriptor(), symbol="EURUSD", timeframe="H1",
                                   point=0.00001, source_identity_sha256=SOURCE_ID) == normal


@pytest.mark.parametrize("change", [
    {"symbol": "GBPUSD"}, {"timeframe": "D1"}, {"point_price": 0.0001},
    {"tier": "P95"}, {"profile_sha256": "0" * 64},
    {"source_identity_sha256": "0" * 64}, {"qualification_authority": True},
    {"spread_points": 8},
])
def test_descriptor_cannot_move_or_upgrade(profile, change):
    descriptor = tier(profile).descriptor()
    descriptor.update(change)
    with pytest.raises(SpreadProfileError):
        profile.from_descriptor(descriptor, symbol="EURUSD", timeframe="H1", point=0.00001,
                                source_identity_sha256=SOURCE_ID)


def test_legacy_multiplier_descriptor_rejected(profile):
    with pytest.raises(SpreadProfileError, match="MISSING_OR_RETIRED"):
        profile.from_descriptor(pm_core.build_backtest_validated_cost_ladder_tolerance(),
                                symbol="EURUSD", timeframe="H1", point=0.00001,
                                source_identity_sha256=SOURCE_ID)


@pytest.mark.parametrize("point,identity", [(0.0001, SOURCE_ID), (0.00001, "0" * 64)])
def test_wrong_point_or_broker_rejected(profile, point, identity):
    with pytest.raises(SpreadProfileError, match="POINT_OR_BROKER"):
        profile.resolve("EURUSD", "H1", "P75", point=point, source_identity_sha256=identity)


def test_unavailable_or_zero_required_profile_cannot_trade(profile):
    zero = next(c for c in profile._cells.values()
                if c.get("status") == "CALCULATED" and c["tiers"]["P75"]["spread_points"] == 0)
    for level in ("P75", "P95"):
        with pytest.raises(SpreadProfileError, match="UNUSABLE"):
            profile.resolve(zero["symbol"], zero["timeframe"], level,
                            point=zero["point_price"],
                            source_identity_sha256=zero["source_identity"]["identity_sha256"])
    with pytest.raises(SpreadProfileError, match="UNAVAILABLE"):
        profile.resolve("TONUSD", "H1", "P75", point=0.01, source_identity_sha256=SOURCE_ID)


def test_equal_positive_tiers_remain_usable(profile):
    cell = next(c for c in profile._cells.values()
                if c.get("status") == "CALCULATED" and c["tiers"]["P75"]["spread_points"] > 0
                and c["tiers"]["P75"]["spread_points"] == c["tiers"]["P95"]["spread_points"])
    resolved = [profile.resolve(cell["symbol"], cell["timeframe"], level,
                                point=cell["point_price"],
                                source_identity_sha256=cell["source_identity"]["identity_sha256"])
                for level in ("P75", "P95")]
    assert resolved[0].full_spread_price == resolved[1].full_spread_price
    assert resolved[0].tier != resolved[1].tier


def test_old_rolling_multiplier_setting_is_not_silently_accepted():
    with pytest.raises(ValueError, match="execution_spread_spike_mult"):
        pm_core.reject_retired_scoring_config_keys({"execution_spread_spike_mult": 2})


@pytest.mark.parametrize("observation,state", [(0, "ADMIT"), (9, "ADMIT"), (9.1, "REFUSE"),
                                                (16, "REFUSE"), (None, "MISSING"),
                                                (np.nan, "MISSING"), (-1, "INVALID"),
                                                (np.inf, "INVALID"), (True, "INVALID")])
def test_shared_cap_boundary(profile, observation, state):
    assert spread_admission(tier(profile), observation) == state


def test_exact_prior_close_no_inbar_or_gap_filling():
    opens = pd.date_range("2020-01-01 00:00", periods=3, freq="5min", tz="UTC")
    source = pd.DataFrame({"Spread": [0, 9, 99]}, index=opens)
    targets = pd.DatetimeIndex([opens[0], opens[1], opens[2], opens[2] + pd.Timedelta(minutes=3)])
    result = align_prior_completed_spreads(targets, source, "M5")
    assert result.iloc[1:3].tolist() == [0, 9]
    assert np.isnan(result.iloc[0]) and np.isnan(result.iloc[3])


def test_observation_axes_reject_duplicates():
    index = pd.DatetimeIndex(["2020-01-01", "2020-01-01"])
    with pytest.raises(SpreadProfileError, match="AXIS_INVALID"):
        align_prior_completed_spreads(index, pd.DataFrame({"Spread": [1, 2]}, index=index), "M5")


def test_matched_closes_distinguish_weekend_gap_from_missing_points():
    opens = pd.DatetimeIndex([
        "2020-01-03T15:55:00Z", "2020-01-06T08:55:00Z", "2020-01-06T09:00:00Z",
    ])
    source = pd.DataFrame({"Spread": [0, np.nan, 99],
                           "_BAR_CLOSE_TS": opens + pd.Timedelta(minutes=5)}, index=opens)
    targets = pd.DatetimeIndex([
        "2020-01-03T15:55:00Z", "2020-01-03T16:00:00Z",
        "2020-01-06T09:00:00Z", "2020-01-06T09:10:00Z",
    ])
    points, matched = align_prior_completed_spreads(
        targets, source, "M5", include_matched_closes=True)
    pd.testing.assert_series_equal(points, align_prior_completed_spreads(targets, source, "M5"))
    assert points.iloc[1] == 0
    assert points.iloc[[0, 2, 3]].isna().all()
    assert matched.iloc[[0, 3]].isna().all()
    assert matched.iloc[1] == targets[1]
    assert matched.iloc[2] == targets[2]  # A real row matched despite its NaN points.
    assert source["Spread"].isna().tolist() == [False, True, False]


def frame():
    index = pd.date_range("2020-01-01", periods=4, freq="h", tz="UTC")
    return pd.DataFrame({"Open": 1.0, "High": 1.0001, "Low": 0.9999, "Close": 1.0,
                         "ATR_14": 0.001, "Spread": [5, 5, 1000, 5],
                         "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)


def spec():
    return InstrumentSpec("EURUSD", 4, pip_value=10, spread_avg=50, point=0.00001,
                          tick_size=0.00001, tick_value=1, min_lot=0.01, max_lot=100,
                          volume_step=0.01, asset_class="fx")


class SyntheticStrategy:
    name = "SyntheticCostConformance"
    params = {}

    def calculate_stops(self, features, direction, symbol, **kwargs):
        return 100, 100


def engine(*, research_mode=False):
    return Backtester(PipelineConfig(use_spread=True, backtest_spread_mode="conservative_max",
                                     swap_model_enabled=False, initial_capital=10000,
                                     research_mode=research_mode))


def test_spread_arrays_use_tier_and_keep_recorded_spikes(profile):
    entry, exit_, meta = engine()._build_half_spread_arrays(
        frame(), spec(), spread_tier=tier(profile), timeframe="H1")
    np.testing.assert_allclose(entry, np.array([9, 9, 1000, 9]) * 0.00001 / 2)
    np.testing.assert_array_equal(entry, exit_)
    assert meta["broker_fixed_spread_price"] == 0
    assert meta["fallback_to_fixed"] is False


def test_missing_recorded_spread_uses_only_tier(profile):
    instrument = spec()
    instrument.spread_avg = 999
    f = frame().drop(columns="Spread")
    entry, exit_, meta = engine()._build_half_spread_arrays(
        f, instrument, spread_tier=tier(profile), timeframe="H1")
    np.testing.assert_allclose(entry, tier(profile).full_spread_price / 2)
    np.testing.assert_array_equal(entry, exit_)
    assert meta["recorded_spread_missing_rows"] == {"Spread": len(f)}


def test_runtime_admission_policy_is_attested_without_changing_tier(profile):
    frozen = tier(profile)
    original_descriptor = frozen._json
    entry, intrabar, meta = engine()._build_half_spread_arrays(
        frame(), spec(), spread_tier=frozen, timeframe="H1")
    policy = json.loads(pm_core.build_backtest_spread_mode(meta))
    assert policy["entry_admission_policy"] == ADMISSION_POLICY
    assert policy["entry_admission_schema"] == ADMISSION_SCHEMA
    assert meta["frozen_spread_tier"] == frozen.descriptor()
    # Historical metadata interpretation is read-only, never an engine option.
    archived_meta = {**meta, "entry_admission_policy": "EXACT_PRIOR_FINE_CLOSE_CAP_MISSING_UNTESTED"}
    archived_meta.pop("entry_admission_schema")
    archived_policy = json.loads(pm_core.build_backtest_spread_mode(archived_meta))
    assert "entry_admission_schema" not in archived_policy
    assert policy != archived_policy
    current = engine()._numerical_execution_cost_attestation(
        entry_half_spread=entry, intrabar_half_spread=intrabar, spread_meta=meta, spec=spec())
    archived = engine()._numerical_execution_cost_attestation(
        entry_half_spread=entry, intrabar_half_spread=intrabar, spread_meta=archived_meta, spec=spec())
    assert current != archived
    assert frozen._json == original_descriptor


def test_nontrading_tier_pair_precommit_matches_two_actual_accounts(profile, monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    f = frame()
    observations = pd.Series([0, 12, 0, 0], index=f.index)
    runner, instrument = engine(), spec()
    probes = {
        name: runner.build_evidence_source_seed_result(
            f, "EURUSD", spec=instrument, timeframe="H1", spread_tier=tier(profile, name),
            entry_spread_observations=observations,
        ) for name in ("P75", "P95")
    }
    committed = pm_core.build_backtest_frozen_tier_pair_contract(
        probes, spec=instrument, use_spread=True,
    )
    assert committed["qualification_authority"] is False
    assert [item["tier"] for item in committed["tiers"]] == ["P75", "P95"]
    assert committed["admission_policy"] == ADMISSION_POLICY
    assert committed["admission_schema"] == ADMISSION_SCHEMA
    for probe in probes.values():
        assert probe["source_seed_has_strategy_or_position_evidence"] is False
        assert probe["position_events"] == []
        assert "trades" not in probe and "equity_curve" not in probe
    # P75 refuses the only entry; P95 admits it. The contract predates both outcomes.
    accounts = runner.run_frozen_spread_tiers(
        f, pd.Series([1, 0, 0, 0], index=f.index), "EURUSD", SyntheticStrategy(),
        spec=instrument, timeframe="H1", p75=tier(profile), p95=tier(profile, "P95"),
        entry_spread_observations=observations,
    )["accounts"]
    assert accounts["P75"]["trades"] == []
    assert len(accounts["P95"]["trades"]) == 1
    assert pm_core.build_backtest_frozen_tier_pair_contract(
        accounts, spec=instrument, use_spread=True,
    ) == committed
    for name in accounts:
        for key in ("entry_observations_sha256", "entry_admission_mask_sha256"):
            assert probes[name]["spread_admission"][key] == accounts[name]["spread_admission"][key]
    assert (probes["P75"]["spread_admission"]["entry_admission_mask_sha256"]
            != probes["P95"]["spread_admission"]["entry_admission_mask_sha256"])


def test_pair_policy_is_slice_independent_but_probe_attestations_are_not(profile):
    f, runner, instrument = frame(), engine(), spec()
    def probe_pair(part):
        return {name: runner.build_evidence_source_seed_result(
            part, "EURUSD", spec=instrument, timeframe="H1", spread_tier=tier(profile, name),
            entry_spread_observations=pd.Series(0.0, index=part.index),
        ) for name in ("P75", "P95")}
    full, sliced = probe_pair(f), probe_pair(f.iloc[1:])
    assert pm_core.build_backtest_frozen_tier_pair_contract(full, spec=instrument, use_spread=True) == (
        pm_core.build_backtest_frozen_tier_pair_contract(sliced, spec=instrument, use_spread=True))
    assert full["P75"]["numerical_execution_cost_attestation"] != sliced["P75"]["numerical_execution_cost_attestation"]
    assert full["P75"]["spread_admission"] != sliced["P75"]["spread_admission"]


@pytest.mark.parametrize("mutation", ["missing_tier", "wrong_tier", "wrong_policy", "authority", "extra_array"])
def test_frozen_pair_validator_rejects_mixed_or_expanded_contracts(profile, mutation):
    runner, instrument, f = engine(), spec(), frame()
    probes = {name: runner.build_evidence_source_seed_result(
        f, "EURUSD", spec=instrument, timeframe="H1", spread_tier=tier(profile, name),
    ) for name in ("P75", "P95")}
    contract = pm_core.build_backtest_frozen_tier_pair_contract(probes, spec=instrument, use_spread=True)
    assert pm_core.validate_backtest_frozen_tier_pair_contract(contract) == contract
    if mutation == "missing_tier":
        contract["tiers"].pop()
    elif mutation == "wrong_tier":
        contract["tiers"][1]["frozen_spread_tier"] = contract["tiers"][0]["frozen_spread_tier"]
    elif mutation == "wrong_policy":
        contract["tiers"][1]["numerical_execution_cost_policy"]["use_spread"] = False
    elif mutation == "authority":
        contract["qualification_authority"] = True
    else:
        contract["entry_admission_mask_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="FROZEN_TIER_PAIR"):
        pm_core.validate_backtest_frozen_tier_pair_contract(contract)


def test_tier_pair_rejects_forged_attestation_and_missing_bound_probe(profile):
    runner, instrument, f = engine(), spec(), frame()
    with pytest.raises(ValueError, match="FROZEN_SPREAD_TIER_REQUIRED"):
        runner.build_evidence_source_seed_result(f, "EURUSD", spec=instrument, timeframe="H1")
    probes = {name: runner.build_evidence_source_seed_result(
        f, "EURUSD", spec=instrument, timeframe="H1", spread_tier=tier(profile, name),
    ) for name in ("P75", "P95")}
    probes["P95"]["numerical_execution_cost_attestation"]["attestation_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="ATTESTATION_INVALID"):
        pm_core.build_backtest_frozen_tier_pair_contract(probes, spec=instrument, use_spread=True)


@pytest.mark.parametrize("path", ["builder", "research_arrays", "run", "empty_run", "tiers", "seed"])
def test_wrong_timeframe_tier_rejected_on_every_entry_path(profile, path):
    f, t = frame(), tier(profile)
    with pytest.raises(ValueError, match="BINDING_MISMATCH"):
        if path == "builder":
            engine()._build_half_spread_arrays(f, spec(), spread_tier=t, timeframe="D1")
        elif path == "research_arrays":
            engine()._research_execution_spread_arrays(f, spec(), None, spread_tier=t, timeframe="D1")
        elif path == "tiers":
            engine().run_frozen_spread_tiers(f, pd.Series(0, index=f.index), "EURUSD",
                                             SyntheticStrategy(), spec=spec(), timeframe="D1", p75=t)
        elif path == "seed":
            engine().build_evidence_source_seed_result(
                f, "EURUSD", spec=spec(), timeframe="D1", spread_tier=t)
        else:
            if path == "empty_run":
                f = f.iloc[:0]
            engine().run(f, pd.Series(0, index=f.index), "EURUSD", SyntheticStrategy(),
                          spec=spec(), timeframe="D1", spread_tier=t)


def test_evidence_costs_cannot_fall_back_to_spec_floor():
    with pytest.raises(ValueError, match="FROZEN_SPREAD_TIER_REQUIRED"):
        engine()._build_half_spread_arrays(frame(), spec())


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("research_mode", [False, True])
def test_cap_refusal_and_missing_coverage_reach_both_engines(profile, monkeypatch, compiled, research_mode):
    if compiled and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    f = frame()
    signals = pd.Series([1, 0, 0, 0], index=f.index)
    kwargs = dict(spec=spec(), timeframe="H1", spread_tier=tier(profile))
    refused = engine(research_mode=research_mode).run(f, signals, "EURUSD", SyntheticStrategy(),
                           entry_spread_observations=pd.Series([0, 10, 0, 0], index=f.index), **kwargs)
    assert refused["trades"] == []
    assert refused["spread_admission"]["known_cap_refused_opportunities"] == 1
    missing = engine(research_mode=research_mode).run(f, signals, "EURUSD", SyntheticStrategy(), **kwargs)
    assert missing["trades"] == []
    admission = missing["spread_admission"]
    assert admission["schema_version"] == ADMISSION_SCHEMA
    assert admission["entry_admission_policy"] == ADMISSION_POLICY
    assert admission["signal_opportunities"] == 1
    assert admission["known_cap_pass_opportunities"] == admission["known_cap_refused_opportunities"] == 0
    assert admission["missing_refused_opportunities"] == 1
    assert admission["missing_executed_legs"] == 0
    assert admission["entry_admission_mask_sha256"] == hashlib.sha256(bytes(len(f))).hexdigest()
    assert admission["qualification_authority"] is False
    assert missing["equity_curve"] == [10000.0] * len(f)


@pytest.mark.parametrize("compiled", [False, True])
def test_missing_observations_without_opportunity_keep_full_equity_axis(profile, monkeypatch, compiled):
    if compiled and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    f = frame()
    observations = pd.Series(np.nan, index=f.index)
    original = observations.copy()
    result = engine().run(
        f, pd.Series(0, index=f.index), "EURUSD", SyntheticStrategy(), spec=spec(), timeframe="H1",
        spread_tier=tier(profile), entry_spread_observations=observations, warmup_bars=2)
    assert result["trades"] == []
    assert result["equity_curve"] == [10000.0] * len(f)
    assert result["trade_power_partition_axis"]["scored_bars"] == 2
    assert result["equity_growth"]["raw_mark_count"] == 2
    assert result["spread_admission"]["signal_opportunities"] == 0
    assert result["spread_admission"]["missing_refused_opportunities"] == 0
    assert result["spread_admission"]["missing_executed_legs"] == 0
    pd.testing.assert_series_equal(observations, original)


@pytest.mark.parametrize("compiled", [False, True])
def test_missing_observations_while_holding_preserve_exits_and_marks(profile, monkeypatch, compiled):
    if compiled and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    f = frame()
    f.loc[f.index[2], "Low"] = 0.98
    signals = pd.Series([1, 0, 0, 0], index=f.index)
    kwargs = dict(spec=spec(), timeframe="H1", spread_tier=tier(profile))
    control = engine().run(f, signals, "EURUSD", SyntheticStrategy(),
                           entry_spread_observations=pd.Series(5.0, index=f.index), **kwargs)
    observations = pd.Series([np.nan, 5, np.nan, np.nan], index=f.index)
    missing = engine().run(f, signals, "EURUSD", SyntheticStrategy(),
                           entry_spread_observations=observations, **kwargs)
    assert missing["trades"] == control["trades"]
    assert missing["trades"][0]["exit_bar"] == 2
    assert missing["trades"][0]["exit_reason"] == "closed_sl"
    assert missing["equity_curve"] == control["equity_curve"]
    assert missing["position_events"] == control["position_events"]
    assert missing["spread_admission"]["known_cap_pass_opportunities"] == 1
    assert missing["spread_admission"]["missing_refused_opportunities"] == 0
    assert missing["spread_admission"]["missing_executed_legs"] == 0
    assert observations.isna().tolist() == [True, False, True, True]


def test_independent_tiers_can_have_different_positions(profile, monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    f = frame()
    observations = pd.Series([0, 12, 0, 0], index=f.index)
    result = engine().run_frozen_spread_tiers(
        f, pd.Series([1, 0, 0, 0], index=f.index), "EURUSD", SyntheticStrategy(),
        spec=spec(), timeframe="H1", p75=tier(profile), p95=tier(profile, "P95"),
        entry_spread_observations=observations,
    )
    assert result["accounts"]["P75"]["trades"] == []
    assert len(result["accounts"]["P95"]["trades"]) == 1
    assert result["qualification_authority"] is False


@pytest.mark.parametrize("compiled", [False, True])
def test_protective_exit_is_not_refused_by_later_spread(profile, monkeypatch, compiled):
    if compiled and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    f = frame()
    f.loc[f.index[2], "Low"] = 0.98
    result = engine().run(f, pd.Series([1, 0, 0, 0], index=f.index), "EURUSD", SyntheticStrategy(),
                          spec=spec(), timeframe="H1", spread_tier=tier(profile),
                          entry_spread_observations=pd.Series([0, 5, 1000, 0], index=f.index))
    assert result["trades"][0]["exit_bar"] == 2
    assert result["trades"][0]["exit_reason"] == "closed_sl"


def test_retired_multiplier_execution_is_explicit():
    with pytest.raises(ValueError, match=RETIRED_COST_POLICY):
        engine()._validate_research_cost_level("UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75")
    with pytest.raises(ValueError, match=RETIRED_COST_POLICY):
        engine().replay_frozen_path_cost_ladder(None, None, None, None, {})


@pytest.mark.parametrize("later_observation", [1000, np.nan])
def test_refused_new_entry_preserves_governance_exit(profile, monkeypatch, later_observation):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    f = frame()
    result = engine().run(f, pd.Series([1, -1, 0, 0], index=f.index),
                          "EURUSD", SyntheticStrategy(), spec=spec(), timeframe="H1",
                          spread_tier=tier(profile),
                          entry_spread_observations=pd.Series([0, 5, later_observation, 0], index=f.index),
                          governance_policy={"name": "time_decay_exit", "max_bars_held": 1,
                                             "time_decay_min_favorable_r": 100})
    assert len(result["trades"]) == 1
    assert result["trades"][0]["exit_bar"] == 2
    assert result["trades"][0]["exit_reason"] == "governance_exit"


@pytest.mark.parametrize("module_name,name,arguments", [
    ("pm_authoritative_search_execution", "execute_bounded_raw_winner_canary",
     ["context", "request", "process", "causal_precommit_payload", "frozen_pipeline_config"]),
    ("pm_forward_oos_replay_producer", "precommit_forward_oos_replay_products",
     ["context", "historical_precommit", "historical_run_receipt", "causal_precommit_payload",
      "execution_products_by_anchor", "valuation_products_by_anchor", "capital_phase_binding", "config"]),
    ("pm_forward_oos_replay_producer", "produce_forward_oos_replay_artifacts",
     ["context", "historical_precommit", "historical_run_receipt", "causal_precommit_payload",
      "replay_precommit", "native_products_by_anchor", "valuation_products_by_anchor", "config"]),
])
def test_retired_producers_stop_before_source_access(module_name, name, arguments):
    import importlib
    module = importlib.import_module(module_name)
    error = (module.AuthoritativeSearchExecutionError if module_name.endswith("execution")
             else module.ForwardOOSReplayProductionError)
    with pytest.raises(error) as caught:
        getattr(module, name)(**dict.fromkeys(arguments))
    assert caught.value.code == "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"


def test_readiness_block_is_evidence_scoped_and_not_a_management_stop():
    import pm_live_readiness as funded
    import pm_demo_readiness as demo
    for validator in (funded.validate_live_readiness, demo.validate_demo_probation_readiness):
        result = validator(PipelineConfig(), symbols=["EURUSD"],
                           config_path="__synthetic_missing_config__/config.json",
                           winner_ledger_path="__synthetic_missing_config__/winners.json")
        findings = [x for x in result.errors if x.code == "interval_cell_qualification_policy_unadopted"]
        assert len(findings) == 1
        assert findings[0].scope == "evidence"


@pytest.mark.parametrize("extra", [None, "other_evidence", "runtime", "duplicate"])
def test_demo_management_exception_is_exactly_one_new_entry_error(extra):
    import pm_demo_readiness as demo
    from pm_live_readiness import ReadinessFinding
    policy = ReadinessFinding("error", "interval_cell_qualification_policy_unadopted",
                              "entry policy unavailable", "evidence")
    errors = [policy]
    if extra is not None:
        errors.append(policy if extra == "duplicate" else ReadinessFinding(
            "error", extra, "synthetic rejection", "runtime" if extra == "runtime" else "evidence"))
    report = demo.DemoProbationReadinessReport(False, errors, _execution_binding=object())
    assert demo._demo_management_binding_checks_pass(report) is (extra is None)
    assert report.ok is False
    assert report.management_only is False  # No verified capsule/projection exists.
    assert report.management_projection == {}


# Fixed synthetic arithmetic was recorded before implementation, not derived from results.
def _continuous_case(case_id, *, compiled=False):
    from copy import deepcopy
    from pathlib import Path
    import types
    protocol = json.loads((Path(__file__).parents[1] / "audit_reports/fxpm_workshop_20261007/continuous_cell_account_verification/new_mode_cases.json").read_text())
    case = next(row for row in protocol["numeric_cases"] if row["id"] == case_id)
    def merge(base, override):
        result = deepcopy(base)
        for key, value in override.items():
            result[key] = merge(result[key], value) if isinstance(value, dict) and isinstance(result.get(key), dict) else deepcopy(value)
        return result
    data = merge(protocol["defaults"], case["overrides"])
    index = pd.DatetimeIndex(data["utc_opens"])
    n = len(index)
    def vector(value):
        return [float("nan") if item == "MISSING" else item for item in value] if isinstance(value, list) else [value] * n
    features = pd.DataFrame({name.title(): vector(data["ohlc"][name]) for name in ("open", "high", "low", "close")}, index=index)
    features["SpreadOpen"] = vector(data["recorded_spread_open"])
    features["SpreadMax"] = vector(data["recorded_spread_max"])
    features["_BAR_CLOSE_TS"] = pd.DatetimeIndex(data["utc_closes"])
    owners = []
    class Stops:
        name = "DeclaredContinuousStops"
        params = {}
        def __init__(self, stop, target):
            self.stop, self.target = stop, target
        def calculate_stops(self, features, direction, symbol, **kwargs):
            return self.stop, self.target
    for owner_id, owner in data["owners"].items():
        level = owner["tier"]
        full = data["tiers"][level]["full_spread_price"]
        frozen = pm_core.FrozenSpreadTier(json.dumps({
            "schema_version": "fxpm.frozen-spread-tier.v1", "symbol": "QA132", "timeframe": "H1", "source_timeframe": "M5",
            "tier": level, "point_price": data["point"], "spread_points": int(full / data["point"]), "full_spread_price": full,
            "profile_sha256": "1" * 64, "source_identity_sha256": "2" * 64,
            "source_binding": {"classification": "DECLARED_SYNTHETIC_NOT_NATIVE"},
            "cost_policy": "MAX_FROZEN_TIER_AND_RECORDED_SPREAD_BOTH_SIDES", "qualification_authority": False}, sort_keys=True, separators=(",", ":")))
        owners.append(pm_core.ContinuousEntryOwner(owner_id, Stops(owner["stop_distance"], owner["target_distance"]),
            features.copy(), pd.Series(owner["signals"], index=index, dtype=np.int32), frozen))
    slots = tuple(pm_core.ContinuousEntrySlot(row["slot_id"], pd.Timestamp(row["start"]), pd.Timestamp(row["end"]), None if row["owner"] == "NO_TRADE" else row["owner"]) for row in data["slots"])
    cfg = PipelineConfig(research_mode=True, initial_capital=data["capital_usd"], discovery_risk_per_trade_pct=data["risk_pct"],
        use_spread=True, backtest_spread_mode="conservative_max", swap_model_enabled=data["swap_enabled"])
    bt = Backtester(cfg)
    instrument = InstrumentSpec("QA132", 0, pip_value=data["pip_value"], tick_size=data["tick_size"], tick_value=data["tick_value"],
        point=data["point"], spread_avg=2, min_lot=data["min_lot"], max_lot=data["max_lot"], volume_step=data["volume_step"], asset_class="fx")
    if "synthetic_causal_swap" in data:
        carry = data["synthetic_causal_swap"]
        def projection(self, features, spec, entry_prices, **kwargs):
            return (np.asarray(carry["pre_open_units"], dtype=np.int32), np.asarray(carry["at_close_units"], dtype=np.int32),
                np.asarray(carry["entry_long_rate_per_lot"], dtype=np.float64), np.asarray(carry["entry_short_rate_per_lot"], dtype=np.float64),
                True, {"status": "CAUSAL_SYNTHETIC", "execution_state_compounding": True})
        bt._build_causal_swap_inputs = types.MethodType(projection, bt)
    kwargs = dict(features=features, signals=None, symbol="QA132", strategy=None, spec=instrument, timeframe="H1", warmup_bars=data["warmup_bars"],
        governance_policy=data["governance_policy"], entry_spread_observations=pd.Series(vector(data["recorded_spread_open"]), index=index, dtype=float),
        entry_schedule=pm_core.ContinuousEntrySchedule(tuple(owners), slots), terminal_policy="MARK_OPEN")
    return bt, kwargs, deepcopy(case["expected"])


def _assert_continuous_expected(result, expected):
    # The frozen protocol permits only this predeclared binary64 bound for decimal arithmetic.
    def same(actual, wanted):
        if isinstance(wanted, float):
            import math
            assert abs(float(actual) - wanted) <= max(1e-12, 32 * math.ulp(max(abs(wanted), 1.0)))
        else:
            assert actual == wanted
    for name in ("cash", "equity_curve", "max_drawdown_pct", "net_account_profit", "first_depletion_bar"):
        if name in expected:
            if name == "equity_curve":
                assert len(result[name]) == len(expected[name])
                for actual, wanted in zip(result[name], expected[name]): same(actual, wanted)
            else: same(result[name], expected[name])
    if "marked_equity" in expected: same(result["final_equity"], expected["marked_equity"])
    if "closed_trade_count" in expected: same(result["total_trades"], expected["closed_trade_count"])
    if "max_drawdown_pct_fraction" in expected:
        fraction = expected["max_drawdown_pct_fraction"]
        assert result["max_drawdown_pct"] == (fraction["peak"] - fraction["trough"]) / fraction["peak"] * 100.0
    terminal = result["terminal"]
    position = terminal["open_position"]
    if "terminal_open" in expected: assert position is expected["terminal_open"]
    if "terminal_unrealized_gross" in expected: same(terminal["unrealized_gross"], expected["terminal_unrealized_gross"])
    wanted_terminal = expected.get("terminal", {})
    for name in ("asof_bar", "bid", "ask", "unrealized_gross"):
        if name in wanted_terminal: same(terminal[name], wanted_terminal[name])
    if "asof_close" in wanted_terminal: assert pd.Timestamp(terminal["asof_close"]) == pd.Timestamp(wanted_terminal["asof_close"])
    names = {"owner": "owner_id", "slot": "slot_id", "entry": "entry_price", "sl": "stop_loss", "tp": "take_profit",
             "posted_swap": "posted_swap_cash", "swap_units": "posted_swap_units"}
    for name, value in wanted_terminal.get("open", {}).items():
        actual = position["tier"]["tier"] if name == "tier" else position[names.get(name, name)]
        if name == "entry_utc_open": assert pd.Timestamp(actual) == pd.Timestamp(value)
        elif name == "lots": assert actual == value
        else: same(actual, value)
    for actual, wanted in zip(result["trades"], expected.get("closed_trades", [])):
        names = {"owner": "owner_id", "slot": "slot_id", "entry": "entry_price", "exit": "exit_price", "lots": "position_size",
                 "gross": "gross_pnl_dollars", "reason": "exit_reason", "posted_swap": "swap_dollars", "swap_units": "swap_rollover_units"}
        for name, value in wanted.items():
            actual_value = actual["frozen_spread_tier"]["tier"] if name == "tier" else (1 if actual["direction"] == "LONG" else -1) if name == "direction" else actual[names.get(name, name)]
            # Frozen cases name the Enum member; execution records use its existing value.
            wanted_value = pm_core.TradeStatus[value].value if name == "reason" else value
            same(actual_value, wanted_value)
    assert not any(t["exit_reason"] == pm_core.TradeStatus.CLOSED_EOD.value for t in result["trades"])
    for name in ("equity_growth", "winner_daily_r_surface", "position_events", "numerical_execution_cost_attestation"):
        assert name not in result
    if "held_excursion_price" in expected:
        same(position["mae_pips"], expected["held_excursion_price"]["mae"])
        same(position["mfe_pips"], expected["held_excursion_price"]["mfe"])
    counters = {"cost_refused_entries": "known_cap_refused_opportunities", "missing_entry_refusals": "missing_refused_opportunities", "no_trade_scored_bar_count": "no_trade_scored_bar_count"}
    for name, key in counters.items():
        if name in expected: same(result["spread_admission"][key], expected[name])
    if "closed_gross" in expected: same(sum(t["gross_pnl_dollars"] for t in result["trades"]), expected["closed_gross"])
    if "total_posted_swap" in expected:
        same(sum(t["swap_dollars"] for t in result["trades"]) + (position["posted_swap_cash"] if position else 0), expected["total_posted_swap"])
    for level, wanted in expected.get("tier_entry_counts", {}).items():
        for key, value in wanted.items(): assert result["tier_entry_counts"][level][key] == value
    assert result["schema_version"] == "fxpm.continuous-account-result.v1"
    assert result["terminal_policy"] == "MARK_OPEN"
    assert terminal["schema_version"] == "fxpm.continuous-account-terminal.v1"
    assert result["qualification_authority"] is False
    # R04: every observed terminal account identity, including all carry cases.
    assert result["final_equity"] == terminal["cash"] + terminal["unrealized_gross"]
    assert result["final_equity"] == result["equity_curve"][-1]
    assert result["total_trades"] == len(result["trades"])
    assert "END_OF_DATA_LIQUIDATED" not in terminal.values()


def _run_continuous_case(case_id, compiled=False, monkeypatch=None):
    bt, kwargs, expected = _continuous_case(case_id, compiled=compiled)
    if monkeypatch is not None:
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
        result = bt.run(**kwargs)
    else:
        old_flag = pm_core.NUMBA_AVAILABLE
        try:
            pm_core.NUMBA_AVAILABLE = compiled
            result = bt.run(**kwargs)
        finally:
            pm_core.NUMBA_AVAILABLE = old_flag
    _assert_continuous_expected(result, expected)
    return result


_CONTINUOUS_NUMERIC_IDS = (
    "N01_P75_LONG_OPEN_ONLY", "N02_P75_SHORT_OPEN_ONLY", "N03_P95_LONG_OPEN_ONLY", "N04_P95_SHORT_OPEN_ONLY",
    "N05_ENTRY_BAR_OWNER_COHERENT_LONG", "N06_ENTRY_BAR_OWNER_COHERENT_SHORT", "N07_LONG_STRADDLE_P75_TO_P95",
    "N08_SHORT_STRADDLE_P95_TO_P75", "N09_NO_TRADE_SUPPRESSES_ENTRY", "N10_NO_TRADE_MANAGES_HELD_LONG_STOP",
    "N11_NO_TRADE_MANAGES_HELD_SHORT_STOP", "N12_HELD_P75_WITH_RECORDED_WIDER_BAR", "N13_HELD_P95_NOT_DOWNGRADED_TO_P75",
    "N14_GAP_EXIT_OLD_P75_NEW_P95_SAME_BAR", "N15_GAP_EXIT_OLD_P95_NEW_P75_SAME_BAR", "N19_CLOSED_PROFIT_PLUS_OPEN_LOSS",
    "N20_FLAT_TERMINAL_WITHOUT_EOD_LEG", "N21_CROSS_SLOT_DRAWDOWN_RETAINS_PRIOR_PEAK", "N22_GAP_DEPLETION_STAYS_NEGATIVE",
    "N23_ADMISSION_USES_INCOMING_TIER", "N24_SPLICE_INVARIANCE_SAME_OWNER", "N25_NAIVE_UTC_LABEL_WITHOUT_SHIFT",
    "N26_ROUNDING_REQUESTED_VS_EXECUTED_STOP_RISK", "N27_SUB_MINIMUM_RISK_SIZE_SKIPS", "N28_MAX_LOT_CAP_REQUESTED_VS_EXECUTED_STOP_RISK",
    "N29_MISSING_ENTRY_OBSERVATION_REFUSES_ONLY_ENTRY")


@pytest.mark.parametrize("case_id", _CONTINUOUS_NUMERIC_IDS)
def test_continuous_account_frozen_numeric_and_backend_parity(case_id, monkeypatch):
    results = []
    for compiled in (False, True):
        calls = []
        original = pm_core._backtest_loop_numba if compiled else Backtester._run_python_loop
        def observed(*args, **kwargs):
            calls.append(1)
            return original(*args, **kwargs)
        with monkeypatch.context() as m:
            m.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
            if compiled: m.setattr(pm_core, "_backtest_loop_numba", observed)
            else: m.setattr(Backtester, "_run_python_loop", observed)
            bt, kwargs, expected = _continuous_case(case_id, compiled=compiled)
            result = bt.run(**kwargs)
            _assert_continuous_expected(result, expected)
            assert calls == [1]
            results.append(result)
    def exact(value):
        if isinstance(value, (float, np.floating)): return float(value).hex()
        if isinstance(value, dict): return {key: exact(item) for key, item in value.items()}
        if isinstance(value, list): return [exact(item) for item in value]
        return value
    assert exact(results[0]) == exact(results[1])


@pytest.mark.parametrize("mutation,code", [
    ("utc", "UTC_REQUIRED"), ("owner_axis", "AXIS_MISMATCH"), ("price", "AXIS_MISMATCH"),
    ("gap", "SLOT_GAP"), ("overlap", "SLOT_OVERLAP"), ("unknown", "OWNER_UNKNOWN"),
    ("length", "INPUT_INVALID"), ("tier", "TIER_BINDING_INVALID"),
    ("profile_identity", "TIER_BINDING_INVALID"), ("source_identity", "TIER_BINDING_INVALID"),
    ("governance", "GOVERNANCE_UNSUPPORTED"), ("swap", "CAUSAL_SWAP_REQUIRED"),
    ("long_stop", "STOP_SIDE_INVALID"), ("short_stop", "STOP_SIDE_INVALID")])
@pytest.mark.parametrize("compiled", [False, True])
def test_continuous_account_frozen_refusals(mutation, code, compiled, monkeypatch):
    from dataclasses import replace
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    schedule = kwargs["entry_schedule"]
    a, b = schedule.owners
    slots = schedule.slots
    if mutation == "utc":
        bad = kwargs["features"].copy()
        bad.index = pd.date_range("2026-01-05T09:00:00+02:00", periods=5, freq="h")
        kwargs["features"] = bad
    if mutation == "owner_axis":
        frame = b.features.copy(); frame.index = frame.index + pd.Timedelta(minutes=1)
        b = replace(b, features=frame)
    if mutation == "price":
        frame = b.features.copy(); frame.loc[frame.index[2], "Close"] = 101.
        b = replace(b, features=frame)
    if mutation in {"gap", "overlap"}:
        slots = (pm_core.ContinuousEntrySlot("S1", pd.Timestamp("2026-01-05T10:00:00Z"), pd.Timestamp("2026-01-05T11:00:00Z" if mutation == "gap" else "2026-01-05T12:00:00Z"), "A"),
                 pm_core.ContinuousEntrySlot("S2", pd.Timestamp("2026-01-05T12:00:00Z" if mutation == "gap" else "2026-01-05T11:00:00Z"), pd.Timestamp("2026-01-05T14:00:00Z"), "B"))
    if mutation == "unknown": slots = (replace(slots[0], owner_id="C"),)
    if mutation == "length": b = replace(b, signals=b.signals.iloc[:4])
    if mutation == "tier": a = replace(a, spread_tier=None)
    if mutation in {"profile_identity", "source_identity"}:
        descriptor = b.spread_tier.descriptor()
        descriptor["profile_sha256" if mutation == "profile_identity" else "source_identity_sha256"] = "3" * 64
        b = replace(b, spread_tier=pm_core.FrozenSpreadTier(json.dumps(descriptor, sort_keys=True, separators=(",", ":"))))
    if mutation == "governance": kwargs["governance_policy"] = {}
    if mutation == "swap":
        bt.config.swap_model_enabled = True
        frame = kwargs["features"].drop(columns="_BAR_CLOSE_TS"); kwargs["features"] = frame
    if mutation in {"long_stop", "short_stop"}:
        a.strategy.stop = -1.
        if mutation == "short_stop": a = replace(a, signals=pd.Series([-1, 0, 0, 0, 0], index=a.features.index))
    kwargs["entry_schedule"] = replace(schedule, owners=(a, b), slots=slots)
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    def forbidden(*args, **kw): raise AssertionError("kernel entered before typed refusal")
    monkeypatch.setattr(pm_core, "_backtest_loop_numba", forbidden)
    monkeypatch.setattr(Backtester, "_run_python_loop", forbidden)
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as exc:
        bt.run(**kwargs)
    assert exc.value.code == "CONTINUOUS_ACCOUNT_" + code
    if mutation in {"long_stop", "short_stop"}: assert "owner A" in str(exc.value)


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("bad", ["missing", "float_shape", "float_dtype", "int_shape", "int_dtype", "readonly"])
def test_continuous_direct_terminal_buffers_refuse_before_execution(compiled, bad):
    bt, kw, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    n = len(kw["features"])
    prices = np.full(n, 100., dtype=np.float64)
    entries, stops, targets = (np.full(n, np.nan) for _ in range(3))
    entries[1], stops[1], targets[1] = 101., 91., 111.
    floats = np.zeros(pm_core.CONTINUOUS_TERMINAL_FLOAT_COUNT, dtype=np.float64)
    ints = np.zeros(pm_core.CONTINUOUS_TERMINAL_INT_COUNT, dtype=np.int64)
    if bad == "missing": floats = None
    if bad == "float_shape": floats = np.zeros(14, dtype=np.float64)
    if bad == "float_dtype": floats = np.zeros(15, dtype=np.float32)
    if bad == "int_shape": ints = np.zeros(6, dtype=np.int64)
    if bad == "int_dtype": ints = np.zeros(7, dtype=np.int32)
    if bad == "readonly": floats.flags.writeable = False
    extra = dict(entry_owner_codes=np.zeros(n, dtype=np.int32), owner_half_spreads=np.ones((1, n)), mark_open=True,
                 terminal_float_out=floats, terminal_int_out=ints)
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as exc:
        if compiled:
            pm_core._backtest_loop_numba(prices, prices, prices, prices, np.array([1, 0, 0, 0, 0]), stops, targets, entries,
                10000., .25, np.ones(n), True, 1., 1., 1., 1., .5, 100., .5, 1, **extra)
        else:
            bt._run_python_loop(kw["features"], prices, prices, prices, prices, np.array([1, 0, 0, 0, 0]), stops, targets, entries,
                np.ones(n), kw["spec"], 1, **extra)
    assert exc.value.code == "CONTINUOUS_ACCOUNT_INPUT_INVALID"


@pytest.mark.parametrize("compiled", [False, True])
def test_continuous_R01_single_owner_common_path_reduction(compiled, monkeypatch):
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    scheduled = bt.run(**kwargs)
    legacy = dict(kwargs)
    owner = kwargs["entry_schedule"].owners[0]
    legacy.update(signals=owner.signals, strategy=owner.strategy, spread_tier=owner.spread_tier)
    legacy.pop("entry_schedule"); legacy.pop("terminal_policy")
    closed = bt.run(**legacy)
    assert scheduled["equity_curve"] == closed["equity_curve"]
    assert scheduled["trades"] == []
    assert len(closed["trades"]) == 1 and closed["trades"][0]["exit_reason"] == pm_core.TradeStatus.CLOSED_EOD.value


def test_continuous_R02_fixed_case_matrix_has_no_omitted_carry_cases():
    from pathlib import Path
    fixed = json.loads((Path(__file__).parents[1] / "audit_reports/fxpm_workshop_20261007/continuous_cell_account_verification/new_mode_cases.json").read_text())
    assert {case["id"] for case in fixed["numeric_cases"]} == set(_CONTINUOUS_NUMERIC_IDS) | {
        "N16_PREOPEN_SWAP_THEN_OLD_GAP_EXIT_THEN_NEW_SIZE", "N17_CAUSAL_SWAP_OWNER_CHANGE_AND_WEEKEND",
        "N18_NEW_BOUNDARY_ENTRY_NOT_CHARGED_PREOPEN_HISTORY"}


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("case_id", ["N09_NO_TRADE_SUPPRESSES_ENTRY", "N23_ADMISSION_USES_INCOMING_TIER"])
def test_continuous_R03_entry_mask_is_coherent_at_dispatch(case_id, compiled, monkeypatch):
    original = pm_core._backtest_loop_numba if compiled else Backtester._run_python_loop
    observed = []
    def checked(*args, **kwargs):
        offset = 0 if compiled else 2  # Python also receives self and the feature frame.
        sl, tp, entry = args[5 + offset:8 + offset]
        assert np.isnan(sl[1]) and np.isnan(tp[1]) and np.isnan(entry[1])
        observed.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    if compiled: monkeypatch.setattr(pm_core, "_backtest_loop_numba", checked)
    else: monkeypatch.setattr(Backtester, "_run_python_loop", checked)
    bt, kwargs, expected = _continuous_case(case_id)
    result = bt.run(**kwargs)
    _assert_continuous_expected(result, expected)
    assert observed == [1]


@pytest.mark.parametrize("compiled", [False, True])
def test_continuous_R05_R06_owner_indicators_are_coherent_and_need_not_match(compiled, monkeypatch):
    from dataclasses import replace
    bt, kwargs, expected = _continuous_case("N05_ENTRY_BAR_OWNER_COHERENT_LONG")
    schedule = kwargs["entry_schedule"]
    owners, calls = [], []
    for number, owner in enumerate(schedule.owners):
        frame = owner.features.copy(); frame["OWNER_INDICATOR"] = number + 100
        owners.append(replace(owner, features=frame))
    strategy = owners[1].strategy
    def checked(features, direction, symbol, **kw):
        assert features["OWNER_INDICATOR"].iat[kw["bar_index"]] == 101
        assert direction == 1 and kw["bar_index"] == 1
        calls.append(1)
        return 10., 10.
    strategy.calculate_stops = checked
    kwargs["entry_schedule"] = replace(schedule, owners=tuple(owners))
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    result = bt.run(**kwargs)
    _assert_continuous_expected(result, expected)
    assert calls == [1]


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("count", [0, 1])
def test_continuous_empty_one_bar_has_only_actual_terminal_provenance(count, compiled, monkeypatch):
    from dataclasses import replace
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    kwargs["features"] = kwargs["features"].iloc[:count]
    kwargs["entry_spread_observations"] = kwargs["entry_spread_observations"].iloc[:count]
    schedule = kwargs["entry_schedule"]
    kwargs["entry_schedule"] = replace(schedule, owners=tuple(replace(owner, features=owner.features.iloc[:count], signals=owner.signals.iloc[:count]) for owner in schedule.owners))
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    result = bt.run(**kwargs)
    assert result["cash"] == 10000 and result["trades"] == []
    terminal = result["terminal"]
    assert terminal["open_position"] is None and terminal["bid"] is None and terminal["ask"] is None
    assert terminal["asof_bar"] == (0 if count else None)
    assert terminal["asof_close"] == (pd.Timestamp("2026-01-05T10:00:00Z") if count else None)
    assert terminal["valuation_status"] == ("FLAT_CASH_ONLY" if count else "UNAVAILABLE_NO_OBSERVATION")
    assert result["final_equity"] == (10000. if count else None)


_CONTINUOUS_BAD_POLICIES = [(compiled, mode, costs) for compiled in (False, True)
    for mode, costs in (("fixed_spec", True), ("historical_bar", True), ("historical_max", True), ("conservative_max", False))]


@pytest.mark.parametrize("compiled,mode,costs", _CONTINUOUS_BAD_POLICIES,
    ids=[("numba" if row[0] else "python") + "-" + (row[1] if row[2] else "costs-disabled") for row in _CONTINUOUS_BAD_POLICIES])
def test_continuous_rework_spread_policy_refuses_before_kernel(compiled, mode, costs, monkeypatch):
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    bt.config.backtest_spread_mode, bt.config.use_spread = mode, costs
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    def forbidden(*args, **kw): raise AssertionError("spread policy bypass entered kernel")
    monkeypatch.setattr(pm_core, "_backtest_loop_numba", forbidden)
    monkeypatch.setattr(Backtester, "_run_python_loop", forbidden)
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as exc:
        bt.run(**kwargs)
    assert exc.value.code == "CONTINUOUS_ACCOUNT_SPREAD_POLICY_INVALID"


_CONTINUOUS_BAD_CLOCKS = [(compiled, surface, kind) for compiled in (False, True)
    for surface, kinds in (("open", ("london", "gmt", "numeric", "string", "date", "bool", "none", "nat")),
                           ("close", ("london", "gmt", "numeric", "string")),
                           ("slot", ("london", "gmt", "numeric", "string", "date", "bool", "none", "nat")))
    for kind in kinds]


@pytest.mark.parametrize("compiled,surface,kind", _CONTINUOUS_BAD_CLOCKS,
    ids=[("numba" if row[0] else "python") + "-" + row[1] + "-" + row[2] for row in _CONTINUOUS_BAD_CLOCKS])
def test_continuous_rework_clock_contract_refuses_before_kernel(compiled, surface, kind, monkeypatch):
    from dataclasses import replace
    from datetime import date
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    schedule = kwargs["entry_schedule"]
    first = "2026-01-05T09:00:00" if surface == "open" else "2026-01-05T10:00:00"
    if kind in {"london", "gmt"}:
        values = pd.date_range(first, periods=5, freq="h", tz="Europe/London" if kind == "london" else "Etc/GMT")
    elif kind == "numeric": values = pd.Index(range(5))
    elif kind == "string": values = pd.Index([value.isoformat() for value in kwargs["features"].index], dtype=object)
    elif kind == "date": values = pd.Index([date(2026, 1, 5)] * 5, dtype=object)
    elif kind == "bool": values = pd.Index([True] * 5, dtype=object)
    elif kind == "none": values = pd.Index([None] * 5, dtype=object)
    else: values = pd.DatetimeIndex([pd.NaT] * 5)
    if surface == "slot":
        kwargs["entry_schedule"] = replace(schedule, slots=(replace(schedule.slots[0], start_utc=values[0]),))
    else:
        frame = kwargs["features"].copy()
        if surface == "open": frame.index = values
        else: frame["_BAR_CLOSE_TS"] = values
        kwargs["features"] = frame
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    def forbidden(*args, **kw): raise AssertionError("invalid clock entered kernel")
    monkeypatch.setattr(pm_core, "_backtest_loop_numba", forbidden)
    monkeypatch.setattr(Backtester, "_run_python_loop", forbidden)
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as exc:
        bt.run(**kwargs)
    assert exc.value.code == ("CONTINUOUS_ACCOUNT_UTC_REQUIRED" if kind in {"london", "gmt"} else "CONTINUOUS_ACCOUNT_INPUT_INVALID")


def test_continuous_rework_utc_vector_paths_preserve_clock_without_scalar_loop(monkeypatch):
    aware = pd.date_range("2026-01-01", periods=100000, freq="min", tz="UTC")
    naive = pd.date_range("2026-01-01", periods=100000, freq="min")
    aware_series, naive_series = pd.Series(aware), pd.Series(naive)
    native_array = naive.to_numpy()
    def forbidden(*args, **kwargs): raise AssertionError("vector path used scalar timestamp/conversion")
    monkeypatch.setattr(pd, "Timestamp", forbidden)
    monkeypatch.setattr(pd.DatetimeIndex, "tz_convert", forbidden)
    assert Backtester._continuous_utc_axis(aware) is aware
    for source in (aware_series, naive, naive_series, native_array):
        result = Backtester._continuous_utc_axis(source)
        assert str(result.tz) == "UTC"
        assert np.array_equal(result.asi8, aware.asi8)


@pytest.mark.parametrize("compiled,naive", [(False, False), (False, True), (True, False), (True, True)],
    ids=["python-utc", "python-naive", "numba-utc", "numba-naive"])
def test_continuous_rework_utc_and_naive_account_preserve_supplied_clock(compiled, naive, monkeypatch):
    bt, kwargs, expected = _continuous_case("N25_NAIVE_UTC_LABEL_WITHOUT_SHIFT" if naive else "N01_P75_LONG_OPEN_ONLY")
    original_index = kwargs["features"].index.copy()
    original_closes = pd.DatetimeIndex(kwargs["features"]["_BAR_CLOSE_TS"])
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    result = bt.run(**kwargs)
    _assert_continuous_expected(result, expected)
    assert kwargs["features"].index.equals(original_index)
    assert pd.DatetimeIndex(kwargs["features"]["_BAR_CLOSE_TS"]).equals(original_closes)
    assert result["terminal"]["asof_open"].value == original_index[-1].value
    assert result["terminal"]["asof_close"].value == original_closes[-1].value
    assert str(result["terminal"]["asof_open"].tzinfo) == "UTC"


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
def test_continuous_rework_no_trade_code_cannot_carry_finite_entry(compiled):
    bt, kw, _ = _continuous_case("N09_NO_TRADE_SUPPRESSES_ENTRY")
    n = len(kw["features"])
    prices = np.full(n, 100., dtype=np.float64)
    entries, stops, targets = (np.full(n, np.nan) for _ in range(3))
    entries[1], stops[1], targets[1] = 101., 91., 111.
    extra = dict(entry_owner_codes=np.full(n, -1, dtype=np.int32), owner_half_spreads=np.ones((1, n)), mark_open=True,
        terminal_float_out=np.zeros(pm_core.CONTINUOUS_TERMINAL_FLOAT_COUNT, dtype=np.float64),
        terminal_int_out=np.zeros(pm_core.CONTINUOUS_TERMINAL_INT_COUNT, dtype=np.int64))
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as exc:
        if compiled:
            pm_core._backtest_loop_numba(prices, prices, prices, prices, np.array([1, 0, 0, 0, 0]), stops, targets, entries,
                10000., .25, np.ones(n), True, 1., 1., 1., 1., .5, 100., .5, 1, **extra)
        else:
            bt._run_python_loop(kw["features"], prices, prices, prices, prices, np.array([1, 0, 0, 0, 0]), stops, targets, entries,
                np.ones(n), kw["spec"], 1, **extra)
    assert exc.value.code == "CONTINUOUS_ACCOUNT_INPUT_INVALID"
    assert "NO_TRADE" in str(exc.value)


@pytest.mark.parametrize("compiled,bad", [(False, "spread"), (False, "observation"), (True, "spread"), (True, "observation")],
    ids=["python-spread", "python-observation", "numba-spread", "numba-observation"])
def test_continuous_rework_data_error_is_not_tier_binding_error(compiled, bad, monkeypatch):
    from dataclasses import replace
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    if bad == "spread":
        frame = kwargs["features"].copy(); frame.iloc[1, frame.columns.get_loc("SpreadMax")] = -1.
        kwargs["features"] = frame
        schedule = kwargs["entry_schedule"]
        kwargs["entry_schedule"] = replace(schedule, owners=tuple(replace(owner, features=frame.copy()) for owner in schedule.owners))
    else: kwargs["entry_spread_observations"].iat[1] = -1.
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    def forbidden(*args, **kw): raise AssertionError("data error entered kernel")
    monkeypatch.setattr(pm_core, "_backtest_loop_numba", forbidden)
    monkeypatch.setattr(Backtester, "_run_python_loop", forbidden)
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as exc:
        bt.run(**kwargs)
    assert exc.value.code == "CONTINUOUS_ACCOUNT_INPUT_INVALID"


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
def test_continuous_rework_stop_failure_identifies_owner(compiled, monkeypatch):
    bt, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
    def broken(*args, **kw): raise ValueError("declared synthetic stop failure")
    kwargs["entry_schedule"].owners[0].strategy.calculate_stops = broken
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    def forbidden(*args, **kw): raise AssertionError("stop failure entered kernel")
    monkeypatch.setattr(pm_core, "_backtest_loop_numba", forbidden)
    monkeypatch.setattr(Backtester, "_run_python_loop", forbidden)
    with pytest.raises(RuntimeError, match="owner A/"):
        bt.run(**kwargs)
