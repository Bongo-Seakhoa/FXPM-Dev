"""INVENTED five-bar software fixtures; real kernels, costs and reconstruction.

The session hook is a test strategy. No production clock, strategy quality,
market data, process issuance or trading authority is established here.
"""
import copy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_forward_oos_replay_producer as forward


def _session_inputs(direction=1, boundary="outside", recorded_offset=False):
    from test_authoritative_search_execution import _full_panel_matched_fill_invented_inputs

    backtester, fixture, frame, strategy, signals, base, stress = (
        _full_panel_matched_fill_invented_inputs(direction, "eod"))
    # EURUSD is only the public fixture roster key; all N26 numerical inputs stay exact.
    fixture = {**fixture, "symbol": "EURUSD", "spec": replace(fixture["spec"], symbol="EURUSD")}
    rebound = []
    for tier in (base, stress):
        descriptor = tier.descriptor()
        descriptor["symbol"] = fixture["symbol"]
        rebound.append(execution.spread_profile.FrozenSpreadTier(
            json.dumps(descriptor, sort_keys=True, separators=(",", ":"))))
    base, stress = rebound
    if boundary == "key_change":
        frame.index = pd.date_range("2026-01-05T22:00:00Z", periods=len(frame), freq="h")
        frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(hours=1)
        fixture["entry_spread_observations"] = fixture["entry_spread_observations"].set_axis(frame.index)
        signals = signals.set_axis(frame.index)
        session_spec = (0, 1440)
    else:
        if recorded_offset:
            frame.index = frame.index.tz_convert("Etc/GMT-2")
            frame["_BAR_CLOSE_TS"] = frame["_BAR_CLOSE_TS"].dt.tz_convert("Etc/GMT-2")
            fixture["entry_spread_observations"] = fixture["entry_spread_observations"].set_axis(frame.index)
            signals = signals.set_axis(frame.index)
        first = frame.index[0]
        session_spec = (first.hour * 60 + first.minute, 120)
    strategy.session_spec = lambda _symbol: session_spec
    # Capture these extremes before public activation; never graft another source's hashes.
    frame.loc[frame.index[2], ["High", "Low", "Close"]] = [500.0, 1.0, 100.0]
    frame.loc[frame.index[2], "SpreadOpen"] = 2.0
    frame.loc[frame.index[2], "SpreadMax"] = 32.0
    return backtester, fixture, frame, strategy, signals, base, stress


def _capture_session_source(root, frame, symbol):
    import pm_broker_terms as terms
    from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER
    from tests.native_v5_support import write_receipt_backed_v5_authority
    from tests.test_broker_terms import _export_row, _write_export

    root.mkdir()
    native = root / "native"
    target = frame.reset_index(drop=True).copy(deep=True)
    target.insert(0, "time", [stamp.isoformat() for stamp in frame.index])
    target["_BAR_CLOSE_TS"] = [stamp.isoformat() for stamp in frame["_BAR_CLOSE_TS"]]
    default_days = pd.date_range("2025-01-06T00:00:00Z", periods=16, freq="D")
    needed_days = pd.date_range(frame.index[0].tz_convert("UTC").normalize(),
        pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1]).tz_convert("UTC").normalize(), freq="D")
    days = default_days.union(needed_days)
    d1 = pd.DataFrame({"time": [stamp.isoformat() for stamp in days],
        "_BAR_CLOSE_TS": [(stamp + pd.Timedelta(days=1)).isoformat() for stamp in days],
        "Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.0})
    authority = write_receipt_backed_v5_authority(native,
        run_id="invented-noise101-software-run", activation_id="invented-noise101-software-price",
        target_frame_overrides={(symbol, "H1"): target, (symbol, "D1"): d1})
    now = datetime.now(timezone.utc)
    identity = {"broker_server": "INVENTED-SOFTWARE-ONLY",
        "terminal_company": "INVENTED-FIXTURE-NO-MARKET-OR-TRADING-AUTHORITY", "account_currency": "USD"}
    roster = {name: name for name in PRODUCTION_SYMBOL_ORDER}
    nonce = hashlib.sha256(target.to_csv(index=False).encode()).hexdigest()[:48]
    terms.write_capture_request(root / "invented-terms-request.csv", roster, nonce=nonce,
        requested_at_utc=now - timedelta(minutes=3), expected_identity=identity)
    anchors = "|".join(f"{int(stamp.timestamp())}:{(stamp.weekday() + 1) % 7}"
        for stamp in default_days[:2])
    rows = []
    for name in PRODUCTION_SYMBOL_ORDER:
        row = _export_row(name, nonce, now - timedelta(minutes=2), anchors=anchors)
        row.update(identity)
        rows.append(row)
    export = root / "invented-terms-response.csv"
    _write_export(export, rows)
    snapshot = terms.parse_terms_export(export, expected_nonce=nonce, expected_roster=roster,
        expected_identity=identity, requested_at_utc=now - timedelta(minutes=3), now_utc=now)
    assert snapshot["source_export_sha256"] == terms.sha256_file(export)
    stressed = terms.build_explicit_stress_snapshot(snapshot)
    terms.activate_broker_terms(native, stressed, activation_id="invented-noise101-software-terms", now_utc=now)
    resolved = terms.resolve_active_swap_contract(native, symbol, "H1")
    binding = {"symbol": symbol, "timeframe": "H1",
        "activation_id": resolved["price_activation_id"],
        "activation_manifest_sha256": resolved["price_activation_manifest_sha256"],
        "bars_sha256": resolved["target_bars_sha256"], "sidecar_sha256": resolved["target_sidecar_sha256"]}
    target_path = native / "runs" / authority["run_id"] / symbol / "H1" / "bars.csv"
    assert terms.sha256_file(target_path) == binding["bars_sha256"]
    reopened = pd.read_csv(target_path)
    assert reopened["time"].tolist() == target["time"].tolist()
    assert reopened["_BAR_CLOSE_TS"].tolist() == target["_BAR_CLOSE_TS"].tolist()
    pd.testing.assert_frame_equal(reopened[["Open", "High", "Low", "Close", "SpreadOpen", "SpreadMax"]],
        target[["Open", "High", "Low", "Close", "SpreadOpen", "SpreadMax"]], check_dtype=False)
    source_hashes = {key: resolved[key] for key in ("price_activation_manifest_sha256",
        "terms_activation_manifest_sha256", "terms_snapshot_sha256", "schedule_sha256",
        "target_bars_sha256", "target_sidecar_sha256")}
    provenance = {"fixture_provenance": "INVENTED_SOFTWARE_ONLY_NO_MARKET_FINANCIAL_OR_TRADING_AUTHORITY",
        "qualification_authority": False, "price_receipt_sha256": terms.sha256_file(authority["receipt_path"]),
        "terms_export_sha256": terms.sha256_file(export), "actual_source_hashes": source_hashes,
        "target_rows": len(frame), "recorded_labels": target["time"].tolist(),
        "public_calls": ["write_receipt_backed_v5_authority", "write_capture_request", "parse_terms_export",
            "build_explicit_stress_snapshot", "activate_broker_terms", "resolve_active_swap_contract"]}
    (root / "INVENTED_SOFTWARE_CAPTURE.json").write_text(json.dumps(provenance, indent=2) + "\n")
    return {"data_dir": native.parent, "frame": frame.copy(deep=True),
        "binding": binding, "source_hashes": source_hashes}


@pytest.fixture(scope="module")
def _session_source_captures(tmp_path_factory):
    root = tmp_path_factory.mktemp("invented-noise101-public-source-captures")
    captures = {}
    for boundary, recorded_offset in (("outside", False), ("key_change", False), ("outside", True)):
        _bt, fixture, frame, _strategy, _signals, _base, _stress = _session_inputs(
            boundary=boundary, recorded_offset=recorded_offset)
        captures[(boundary, recorded_offset)] = _capture_session_source(
            root / f"{boundary}-offset-{recorded_offset}", frame, fixture["symbol"])
    return captures


def _run_session_case(monkeypatch, source_captures, *, compiled=False, direction=1,
        boundary="outside", recorded_offset=False):
    assert execution.pm_core.NUMBA_AVAILABLE, "Both real execution backends are required"
    backtester, fixture, frame, strategy, signals, base, stress = _session_inputs(
        direction, boundary, recorded_offset)
    capture = source_captures[(boundary, recorded_offset)]
    pd.testing.assert_frame_equal(frame, capture["frame"])
    frame.attrs[execution.pm_core.NATIVE_SOURCE_BINDING_ATTR] = dict(capture["binding"])
    backtester.config.data_dir = str(capture["data_dir"])
    backtester.config.swap_model_enabled = True
    monkeypatch.setattr(execution.pm_core, "NUMBA_AVAILABLE", compiled)
    common = dict(spec=fixture["spec"], timeframe=fixture["timeframe"],
        warmup_bars=fixture["warmup_bars"], spread_tier=base,
        entry_spread_observations=fixture["entry_spread_observations"])
    ordinary = backtester.run(frame, signals, fixture["symbol"], strategy, **common)
    contract = execution.raw_winner.build_trade_source_contract_from_backtester_result(ordinary)
    assert all(contract["source_binding"][key] == value for key, value in capture["source_hashes"].items())
    source_sha = execution._source_identity(contract)
    partition = execution._PartitionSlice("final_lockbox", frame, fixture["warmup_bars"],
        frame.index[fixture["warmup_bars"]].isoformat(), frame["_BAR_CLOSE_TS"].iloc[-1].isoformat(), {})
    facts, decision = {}, {}
    result, features, _signals, _bt = execution._run_tier_partition(
        partition=partition, full_prefix_surface=execution._FullPrefixStrategySurface(frame, signals),
        backtester=backtester, strategy=strategy, symbol=fixture["symbol"], timeframe=fixture["timeframe"],
        regime="TRENDING", config=backtester.config, spec=fixture["spec"],
        expected_source_contract_sha256=source_sha, spread_tier=base,
        entry_spread_observations=fixture["entry_spread_observations"], decision_result_out=decision,
        _matched_base_facts_out=facts, _matched_stress_tier=stress)
    assert facts["exact_capture_count_matches"] is True
    assert facts["exact_capture_backend"] == ("NUMBA" if compiled else "PYTHON")
    return result, facts, features, backtester, fixture, strategy, base, stress


def _tier_account(case):
    result, _facts, frame, bt, fixture, strategy, tier, _stress = case
    spec = fixture["spec"]
    entry, intrabar, spread_meta = bt._build_half_spread_arrays(
        frame, spec, spread_tier=tier, timeframe=fixture["timeframe"])
    prices = np.full(len(frame), np.nan)
    for trade in result["trades"]:
        prices[trade["entry_bar"]] = trade["entry_price"]
    pre, close, long_rates, short_rates, enabled, contract = bt._build_causal_swap_inputs(
        frame, spec, prices, timeframe=fixture["timeframe"])
    raw = forward._raw_json
    return {"symbol": fixture["symbol"], "timeframe": fixture["timeframe"], "cell_id": "INVENTED-SESSION-WITNESS",
        "tier": tier.descriptor(), "initial_cash": float(bt.config.initial_capital),
        "warmup_bars": fixture["warmup_bars"], "instrument_spec": asdict(spec),
        "position_namespace": "|".join((fixture["symbol"], fixture["timeframe"], strategy.name)),
        "trades": raw(result["trades"]), "position_events": raw(result["position_events"]),
        "source_rows": forward._tier_source_rows(frame, entry, intrabar, spread_meta, result["equity_curve"],
            session_clock=True),
        "engine_equity_growth": raw(result["equity_growth"]),
        "engine_spread_admission": raw(result["spread_admission"]),
        "engine_spread_model": raw(result["spread_model"]),
        "execution_cost_timing_contract": raw(result["execution_cost_timing_contract"]),
        "numerical_execution_cost_attestation": raw(result["numerical_execution_cost_attestation"]),
        "financing_model": {"pre_open_units": raw(pre), "close_units": raw(close),
            "long_per_lot": raw(long_rates), "short_per_lot": raw(short_rates),
            "enabled": bool(enabled), "contract": raw(contract)},
        "session_execution": raw(result["session_execution"])}


@pytest.mark.parametrize("compiled", (False, True), ids=("python", "numba"))
@pytest.mark.parametrize("direction", (1, -1), ids=("long", "short"))
@pytest.mark.parametrize("boundary", ("outside", "key_change"))
def test_session_kernel_fill_matched_cost_and_forward_account_share_actual_array(monkeypatch, compiled, direction, boundary, _session_source_captures):
    case = _run_session_case(monkeypatch, _session_source_captures, compiled=compiled, direction=direction, boundary=boundary)
    result, facts, frame, bt, fixture, _strategy, base, stress = case
    assert len(result["trades"]) == 1
    trade, row = result["trades"][0], facts["trades"][0]
    assert trade["entry_bar"] == 1 and trade["exit_bar"] == 2
    assert trade["exit_reason"] == "closed_session"
    entry, intrabar, _ = bt._build_half_spread_arrays(frame, fixture["spec"],
        spread_tier=base, timeframe=fixture["timeframe"])
    assert entry[2] != intrabar[2], "The fixture must distinguish the two actual cost arrays"
    assert result["session_execution"]["exit_half_spreads"] == [float(intrabar[2])]
    assert result["session_execution"]["exit_open_prices"] == [float(frame["Open"].iloc[2])]
    assert trade["exit_price"] == float(frame["Open"].iloc[2]) - direction * float(intrabar[2])
    assert row["base_fill_witness"]["status"] == "VERIFIED"
    assert row["base_fill_witness"]["branch"] == "SESSION_OPEN"
    assert row["executed_price_swap_capture"]["exit_price"] == trade["exit_price"]
    assert row["executed_price_swap_capture"]["exit_reason"] == "closed_session"
    assert all(not (step["phase"] == "CLOSE" and step["bar"] == 2)
        for step in row["executed_price_swap_capture"]["swap_cash_steps"])
    p95_entry, p95_intrabar, _ = bt._build_half_spread_arrays(frame, fixture["spec"],
        spread_tier=stress, timeframe=fixture["timeframe"])
    numerator = float(p95_entry[1]) + float(p95_intrabar[2])
    assert row["matched_p95_cost_inputs"]["numerator_price"] == numerator
    assert row["matched_p95_spread_cost_r"] == execution.pm_core.canonical_backtest_winner_r(
        numerator, abs(trade["entry_price"] - trade["stop_loss"]))
    assert execution._verify_route_a_matched_base_facts(facts)["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED"
    derived = forward._reconcile_tier_account(_tier_account(case))
    assert derived["positions"][0]["exit_phase"] == "BAR_OPEN"
    assert derived["positions"][0]["exit_fill"] == trade["exit_price"]
    axis = result["session_execution"]["specs"][0]
    assert forward._declared_cell_exit_at(frame, 2, direction=direction, half_spread=float(intrabar[2]),
        stop=trade["stop_loss"], target=trade["take_profit"], session_axis=axis, entry_bar=1) == (
            trade["exit_price"], "closed_session", True)


@pytest.mark.parametrize("damage", ("cost_array", "clock", "earlier_trigger"))
def test_session_witness_refuses_crossed_cost_clock_and_earlier_order(monkeypatch, damage, _session_source_captures):
    case = _run_session_case(monkeypatch, _session_source_captures)
    _result, facts, _frame, _bt, _fixture, _strategy, _base, _stress = case
    changed = copy.deepcopy(facts)
    if damage == "cost_array":
        changed["session_execution"]["exit_half_spreads"][0] += 1.0
    elif damage == "clock":
        changed["session_execution"]["specs"][0]["session_keys"][1] += 1
    else:
        witness = changed["trades"][0]["base_fill_witness"]
        witness["held_bar_inputs"][0]["ohlc"]["Low"] = 0.0
        witness["held_bar_inputs_sha256"] = execution._sha256(witness["held_bar_inputs"])
        changed["base_fill_witness_sha256"] = execution._sha256([witness])
    assert execution._verify_route_a_matched_base_facts(changed)["floor_status"] != "AVAILABLE_MATCHED_BASE_VERIFIED"


@pytest.mark.parametrize("damage", ("cost_array", "clock"))
def test_forward_session_account_refuses_crossed_actual_capture(monkeypatch, damage, _session_source_captures):
    account = _tier_account(_run_session_case(monkeypatch, _session_source_captures))
    if damage == "cost_array":
        account["session_execution"]["exit_half_spreads"][0] += 1.0
        expected = "FROZEN_TIER_SESSION_OPEN_ARRAY_CHANGED"
    else:
        account["session_execution"]["specs"][0]["in_session"][1] = False
        expected = "SESSION_EXECUTION_WITNESS_INVALID"
    with pytest.raises(forward.ForwardOOSReplayProductionError) as refused:
        forward._reconcile_tier_account(account)
    assert refused.value.code == expected


def test_forward_session_clock_keeps_recorded_labels_beside_utc_reporting(monkeypatch, _session_source_captures):
    account = _tier_account(_run_session_case(monkeypatch, _session_source_captures, recorded_offset=True))
    first = account["source_rows"][0]
    assert first["session_recorded_open"] != first["open_utc"]
    assert pd.Timestamp(first["session_recorded_open"]) == pd.Timestamp(first["open_utc"])
    assert forward._reconcile_tier_account(account)["positions"][0]["exit_phase"] == "BAR_OPEN"
