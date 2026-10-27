"""Cheap declared-account software witnesses; no real-market or qualification claim.

Prices, OWNER authentication, native identities and broker terms are invented.
The actual metadata gates, features, strategy signals, kernel and cold reader run.
Sparse newly invented :01 bars exercise the retained roster structurally only.
"""

from contextlib import contextmanager

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_authority_spool as spool
import pm_causal_evidence_producer as causal
import pm_core
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_native_prefix_production as native
from pm_current_refit_parameters import canonicalize_parameter_mapping
from pm_regime import RegimeParams, RegimeType, regime_params_sha256
from prototype_support import _declared_environment
from prototype_support import _sealed_identity_only_product, _synthetic_profile
from prototype_support import _execution_config
import test_production_config_binding as config_fixture


assert Path(orchestration.__file__).resolve() == PRODUCT_ROOT / "pm_historical_anchor_orchestration.py"
assert Path(causal.__file__).resolve() == PRODUCT_ROOT / "pm_causal_evidence_producer.py"
CLASSIFICATION = "SYNTHETIC_NONCONSTANT_SOFTWARE_WITNESS_NOT_RAW_OWNER_OR_PROFIT_EVIDENCE"


@contextmanager
def _no_broker_calls_context():
    try:
        import MetaTrader5 as mt5
    except ImportError:
        yield
        return
    with pytest.MonkeyPatch.context() as patch:
        guards = []
        for name in ("initialize", "login", "order_send", "positions_get", "account_info", "copy_rates_range"):
            guard = Mock(side_effect=AssertionError(f"Real broker call forbidden: {name}"))
            patch.setattr(mt5, name, guard, raising=False)
            guards.append(guard)
        yield
        for guard in guards:
            guard.assert_not_called()


@pytest.fixture(scope="module", autouse=True)
def no_broker_calls():
    with _no_broker_calls_context():
        yield


@pytest.fixture(autouse=True)
def _no_broker_calls_per_test(block_real_mt5_terminal):
    with _no_broker_calls_context():
        yield


def _native_shaped_frame(index, closes, *, timeframe_minutes):
    values = np.asarray(closes, dtype=np.float64)
    frame = pd.DataFrame({
        "Open": values, "High": values + 0.00001, "Low": values - 0.00001,
        "Close": values, "Volume": np.ones(len(index), dtype=np.float64),
        "Spread": np.full(len(index), 3.0), "RealVolume": np.ones(len(index), dtype=np.float64),
        "_SOURCE_FIRST_TS": index, "_SOURCE_LAST_TS": index + pd.Timedelta(minutes=timeframe_minutes) - pd.Timedelta(seconds=1),
        "_BAR_CLOSE_TS": index + pd.Timedelta(minutes=timeframe_minutes),
    }, index=index)
    frame.index.name = "time"
    return frame


@pytest.fixture(scope="module")
def nonconstant_complete_account(tmp_path_factory, no_broker_calls):
    with pytest.MonkeyPatch.context() as patch:
        if VERIFY_APPLIED_SOURCE:
            mixed = orchestration._declared_source_identity()
        else:
            # Staged-layout accommodation only; applied mode uses the real locator.
            mixed = {name: hashlib.sha256(((STAGE if name == "pm_historical_anchor_orchestration.py" else REPO) / name).read_bytes()).hexdigest()
                     for name in orchestration._DECLARED_SOURCE_FILES}
            patch.setattr(orchestration, "_declared_source_identity", lambda: dict(mixed))
        root = tmp_path_factory.mktemp("nonconstant-declared-software")
        _execution_config(root, symbol="AUDUSD")
        original_payload = config_fixture._payload

        def payload(path):
            body = original_payload(path)
            body["broker_specs_path"] = "broker_specs.json"
            body["pipeline"]["swap_model_enabled"] = False
            return body

        with patch.context() as initial:
            initial.setattr(config_fixture, "_payload", payload)
            env = _declared_environment(root, patch, issue=False)
        config = env.frozen.materialize(env.context)
        slots, cell = env.roster["slots"], env.arguments["cell_roster"][0]
        start, end = pd.Timestamp(slots[0]["activation_utc"]), pd.Timestamp(slots[-1]["end_exclusive_utc"])
        first_cutoff = pd.Timestamp(slots[0]["native_cutoff_utc"])
        prefix = pd.date_range(end=first_cutoff - pd.Timedelta(hours=1), periods=220, freq="h")
        chunks = [pd.date_range(slot["activation_utc"], periods=64, freq="h") for slot in slots]
        index = prefix
        for chunk in chunks:
            index = index.append(chunk)
        index = index.append(pd.DatetimeIndex([end - pd.Timedelta(hours=1)]))
        prices = 1.0 + 0.003 * np.sin(np.arange(len(index)) * 2 * np.pi / 64)
        frame = _native_shaped_frame(index, prices, timeframe_minutes=60)
        # Wide boundary wicks close incumbents under the real adverse-first rule.
        # Wide original ATR stops allow positions to survive the ordinary waves.
        for slot in slots[1:]:
            at = pd.Timestamp(slot["activation_utc"])
            frame.loc[at, "High"] = frame.loc[at, "Close"] + 0.05
            frame.loc[at, "Low"] = frame.loc[at, "Close"] - 0.05
        fine_index = (index - pd.Timedelta(minutes=5)).append(pd.DatetimeIndex([end - pd.Timedelta(minutes=5)]))
        fine_frame = _native_shaped_frame(fine_index, np.ones(len(fine_index)), timeframe_minutes=5)
        cutoff = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        products = {tf: _sealed_identity_only_product(symbol=cell["symbol"], timeframe=tf, cutoff_utc=cutoff) for tf in ("H1", "M5")}
        identities = {tf: forward._native_product_identity(product, expected_symbol=cell["symbol"], expected_timeframe=tf,
            expected_cutoff_utc=cutoff, role="DECLARED_CELL_EXECUTION" if tf == "H1" else "DECLARED_CELL_SPREAD")
            for tf, product in products.items()}
        terms = Path(config.data_dir) / "native/terms"
        tiers = _synthetic_profile(terms, symbol=cell["symbol"], identity=identities["M5"])
        profile_path = terms / "spread_profile.json"
        (terms / "synthetic_frozen_profile.json").rename(profile_path)
        env.arguments["spread_profile_identity"] = {"profile_sha256": tiers["P75"].descriptor()["profile_sha256"]}
        env.precommit = orchestration.precommit_declared_slot_owner_run_v2(env.context, **env.arguments)
        detector = RegimeParams(
            k_confirm=1, gap_min=0.0, k_hold=1, adx_trend_threshold=1.0,
            trend_adx_weight=1.0, trend_efficiency_weight=0.0, trend_slope_weight=0.0,
            range_adx_weight=1.0, range_efficiency_weight=0.0, range_containment_weight=0.0,
            breakout_squeeze_weight=1.0, breakout_atr_weight=0.0, breakout_structure_weight=0.0,
            chop_adx_weight=1.0, chop_efficiency_weight=0.0, chop_whipsaw_weight=0.0,
        ).to_dict()
        process = {"strategy_id": "EMACrossoverStrategy", "strategy_class": "EMACrossoverStrategy", "strategy_module": "pm_strategies"}
        params = forward.StrategyRegistry.get(process["strategy_id"]).get_default_params()
        params.update(fast_period=5, slow_period=10, sl_atr_mult=60.0, tp_atr_mult=60.0)
        encoded = canonicalize_parameter_mapping(params)
        known, owners, commit_entries = {}, {}, []

        def finite_completion_auth(context, *, precommit, process, completion, artifact_roster, frozen_pipeline_config):
            assert context is env.context and process is env.process and frozen_pipeline_config is env.frozen
            row = known.get(completion.get("request_sha256"))
            assert row is not None and forward._canonical_bytes(completion) == row["bytes"]
            assert list(artifact_roster) == row["artifacts"] and completion["classification"] == CLASSIFICATION
            sink = spool.open_symbol_sink(context, cell["symbol"])
            request = orchestration.DeclaredSlotProducerRequest(
                orchestration._declared_json(completion["request_body"]), precommit, sink, orchestration._DECLARED_SLOT_SEAL)
            orchestration._register_declared_request(request, context, live=False)
            try:
                body = orchestration.verify_declared_slot_producer_request(context, request, precommit=precommit, process=process)
                assert body["slot_id"] == completion["slot_id"] and request.receipt_sha256 == completion["request_sha256"]
                for artifact in artifact_roster:
                    descriptor = artifact["descriptor"]
                    digest, size, _ = spool._stream_file(context.root, descriptor["relative_path"],
                        maximum_bytes=descriptor["size_bytes"], expected_size=descriptor["size_bytes"], collect=False)
                    assert (digest, size) == (descriptor["sha256"], descriptor["size_bytes"])
            finally:
                orchestration._release_declared_slot_producer_request(request, context)
            return deepcopy(completion)

        patch.setattr(adapter, "verify_declared_slot_completion", finite_completion_auth)
        search_guard = Mock(side_effect=AssertionError("No candidate search belongs in this software witness"))
        patch.setattr(producer, "execute_declared_slot_owner_search", search_guard)
        previous = None
        for slot in slots:
            sink = spool.open_symbol_sink(env.context, cell["symbol"])
            request = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit, process=env.process,
                cell_id=cell["cell_id"], slot_id=slot["slot_id"], attempt_id="synthetic-nonconstant", sink=sink)
            try:
                boundary = pd.Timestamp(slot["native_cutoff_utc"])
                fit = frame.loc[frame["_BAR_CLOSE_TS"] <= boundary]
                count = {**cell, "fit_start_utc": fit.index[0].isoformat(), "fit_end_exclusive_utc": boundary.isoformat(),
                         "source_bar_count": len(fit), "fit_frame_sha256": forward._frame_sha256(fit),
                         "latest_closed_bar_utc": pd.Timestamp(fit["_BAR_CLOSE_TS"].iloc[-1]).isoformat()}
                count["receipt_sha256"] = forward._sha(count)
                selected = {"strategy_id": process["strategy_id"], "process_candidate_id": f"SYNTHETIC-PROCESS-{slot['ordinal']:02d}",
                            "process_sha256": forward._sha(process), "encoded_parameters": encoded}
                level = "P75" if slot["ordinal"] % 2 else "P95"
                owner = {"classification": CLASSIFICATION,
                    "owner_id": orchestration.declared_slot_owner_id(cell_id=cell["cell_id"], slot_id=slot["slot_id"],
                        winner_decision_sha256="f" * 64, process_receipt_sha256=env.process.receipt_sha256),
                    "cell_id": cell["cell_id"], "slot_id": slot["slot_id"], "selected_tier": level,
                    "selected_tier_contract": {"frozen_spread_tier": tiers[level].descriptor()},
                    "selected_artifacts": {"winner_process": process}, "selected_attempt": selected,
                    "regime_detector_record": {"parameters_canonical_json": json.dumps(detector, sort_keys=True, separators=(",", ":")),
                        "parameters_sha256": regime_params_sha256(detector)}}
                owners[owner["owner_id"]] = owner
                namespace = request.to_dict()["artifact_namespace"]
                frozen = sink.put_opaque(namespace + "/invented-frozen", "synthetic_frozen_schema_only",
                    (orchestration._declared_json({"schema_version": adapter.raw_winner.V16_FROZEN_PLAN_SCHEMA,
                        "classification": CLASSIFICATION}),), format=adapter.raw_winner.ASCII_JSON_FORMAT)
                cross = sink.put_json(namespace + "/invented-cross", "synthetic_owner_mapping_not_raw_cross_proof",
                    {"classification": CLASSIFICATION, "mapping": [{"winner_process_candidate_id": selected["process_candidate_id"],
                        "process_sha256": selected["process_sha256"], "source_count_receipt": count}]})
                completion = producer._declared_base_completion(request, status="OWNER", reason_code=None, source_entry=None)
                completion.update(classification=CLASSIFICATION, owner=owner, frozen_plan_receipt=frozen["descriptor"],
                                  horizon_winner_cross_binding=cross["descriptor"])
                artifacts = [frozen, cross]
                known[request.receipt_sha256] = {"bytes": forward._canonical_bytes(completion), "artifacts": deepcopy(artifacts)}
                _, descriptor = producer._commit_declared_slot_completion(env.context, precommit=env.precommit,
                    process=env.process, cell_id=cell["cell_id"], slot=slot, completion=completion,
                    artifact_roster=artifacts, previous_commit_sha256=previous, frozen_pipeline_config=env.frozen)
                previous = descriptor["sha256"]
                commit_entries.append(producer._declared_commit_artifact(slot, descriptor))
            finally:
                orchestration._release_declared_slot_producer_request(request, env.context)
        sink = spool.open_symbol_sink(env.context, cell["symbol"])
        for artifact in commit_entries:
            sink.register_artifact(artifact["artifact_id"], artifact["artifact_type"], artifact["descriptor"])
        sink.seal()
        spec = pm_core.InstrumentSpec(cell["symbol"], 5, point=0.00001, tick_size=0.00001,
            tick_value=1.25, pip_value=12.5, min_lot=0.01, max_lot=500.0, volume_step=0.01, asset_class="fx")

        def verify_native(**kwargs):
            product = products[kwargs["expected_timeframe"]]
            assert kwargs["context"] is env.context and kwargs["expected_symbol"] == cell["symbol"]
            assert kwargs["expected_index_sha256"] == product.index_sha256
            assert kwargs["index_descriptor"] == product.index_descriptor and kwargs["expected_cutoff_utc"] == cutoff
            assert kwargs["pinned_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
            assert kwargs["expected_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
            assert kwargs["production_contract_required"] is False and kwargs["require_current_activation"] is False
            assert Path(kwargs["data_dir"]) == Path(config.data_dir)
            return product

        def open_native(product):
            assert any(product is candidate for candidate in products.values())
            return (frame if product is products["H1"] else fine_frame).copy(deep=True)

        def freeze_spec(*, product, config):
            assert any(product is candidate for candidate in products.values())
            assert config._deployment_config_path == str(env.config_path.resolve())
            deployment = forward._deployment_config(config)
            receipt = {"classification": CLASSIFICATION, "deployment_config_sha256": deployment.config_sha256,
                "deployment_config_size_bytes": deployment.config_size_bytes, "broker_specs_relative_path": deployment.broker_relative_path,
                "broker_specs_sha256": deployment.broker_sha256, "broker_specs_size_bytes": deployment.broker_size_bytes,
                "native_prefix_index_sha256": product.index_sha256, "grants_search_authority": False, "grants_trading_authority": False}
            receipt["receipt_sha256"] = forward._sha(receipt)
            return spec, receipt

        patch.setattr(native, "verify_activated_native_prefix", verify_native)
        patch.setattr(native, "open_verified_prefix_frame", open_native)
        patch.setattr(forward.search_execution, "_freeze_bound_execution_spec", freeze_spec)
        patch.setattr(pm_core, "NUMBA_AVAILABLE", False)
        runs, preparations = [], []
        real_run, real_prepare = forward._run_declared_cell_account, forward._declared_cell_native_frames

        def once(**kwargs):
            assert not runs, "This software witness permits one synthetic account run"
            runs.append(kwargs)
            return real_run(**kwargs)

        def prepare(**kwargs):
            result = real_prepare(**kwargs)
            preparations.append(result)
            return result

        patch.setattr(forward, "_run_declared_cell_account", once)
        patch.setattr(forward, "_declared_cell_native_frames", prepare)
        artifact = forward.produce_declared_cell_account_artifact(env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, execution_product=products["H1"],
            spread_source_product=products["M5"], frozen_profile_path=profile_path)
        projection = causal.read_declared_cell_account_artifact(env.context, artifact,
            process=env.process, frozen_pipeline_config=env.frozen)
        yield SimpleNamespace(env=env, projection=projection, artifact=artifact, frame=frame,
                              owners=owners, sample=preparations[0][0], runs=runs, mixed=mixed,
                              feature_frames=tuple(owner.features for owner in preparations[0][1]))
        assert len(runs) == 1
        search_guard.assert_not_called()


def test_nonconstant_complete_writer_reader_trades_across_owners_and_refits(nonconstant_complete_account):
    witness = nonconstant_complete_account
    body = witness.projection.verified_body
    assert witness.projection.progress_status == "COMPLETE" and witness.projection.admissible
    assert len(body["owner_records"]) == len(body["slot_schedule"]) == 13
    assert all(slot["status"] == "OWNER" for slot in body["slot_schedule"])
    result = body["account"]["engine_result"]
    trades = result["trades"]
    assert result["total_trades"] == len(trades) >= 3
    assert len({trade["owner_id"] for trade in trades}) >= 3
    crossings = []
    boundaries = [pd.Timestamp(slot["start_utc"]) for slot in body["slot_schedule"]][1:]
    for trade in trades:
        owner = witness.owners[trade["owner_id"]]
        assert trade["slot_id"] == owner["slot_id"]
        assert trade["frozen_spread_tier"] == owner["selected_tier_contract"]["frozen_spread_tier"]
        entry, exit_time = (witness.frame.index[int(trade[key])] for key in ("entry_bar", "exit_bar"))
        crossings.extend(boundary for boundary in boundaries if entry < boundary <= exit_time)
    assert crossings, "No actual filled incumbent crossed a refit boundary"
    assert len(result["equity_curve"]) == len(witness.frame)
    assert len(witness.runs) == 1 and np.isfinite(result["final_equity"])
    assert result["qualification_authority"] is False
    assert all(body[name] is False for name in ("qualification_authority", "capital_authority", "live_authority", "order_authority"))
    with pytest.raises(causal.CausalEvidenceProductionError, match="DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"):
        witness.projection.require_interval_qualification()
    print(json.dumps({"classification": CLASSIFICATION, "trades": len(trades),
        "owners_with_fills": len({trade["owner_id"] for trade in trades}), "crossed_refits": len(crossings),
        "source_identity_mode": "REAL_APPLIED_FIVE_SIBLING_LOCATOR" if VERIFY_APPLIED_SOURCE else "TEST_ONLY_MEASURED_MIXED_OVERLAY_ROSTER", "qualification_authority": False}))


def test_thirteen_owner_feature_bound_at_recorded_native_row_count(nonconstant_complete_account):
    metadata_path = REPO / "tests/fixtures/declared_feature_footprint/h1_rows.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["price_or_outcome_fields_parsed"] is False
    count = metadata["sources"]["H1"]["source_rows"]
    assert count == 34950
    sample = nonconstant_complete_account.sample
    assert len(nonconstant_complete_account.feature_frames) == 13
    for owner_frame in nonconstant_complete_account.feature_frames:
        assert owner_frame.columns.equals(sample.columns) and owner_frame.dtypes.equals(sample.dtypes)
    floats, datetimes, categories, booleans, unknown = [], [], [], [], []
    for name, dtype in sample.dtypes.items():
        if dtype == np.dtype("float64"):
            floats.append(name)
        elif isinstance(dtype, pd.DatetimeTZDtype) and dtype.unit == "ns" and str(dtype.tz) == "UTC":
            datetimes.append(name)
        elif isinstance(dtype, pd.CategoricalDtype) and dtype.categories.tolist() == list(RegimeType.ALL):
            categories.append(name)
        elif dtype == np.dtype("bool"):
            booleans.append(name)
        else:
            unknown.append((name, str(dtype)))
    assert not unknown, f"UNRESOLVED_FEATURE_DTYPE_BOUND:{unknown}"
    assert (len(floats), len(datetimes), len(categories), len(booleans), len(sample.columns)) == (71, 3, 3, 1, 78)
    fixed = 0
    per_row = sum(sample[name].dtype.itemsize for name in floats + datetimes + booleans)
    for name in categories:
        series = sample[name]
        assert series.cat.codes.dtype == np.dtype("int8")
        series.cat.categories.get_indexer(list(RegimeType.ALL))
        fixed += series.cat.categories.memory_usage(deep=True)
        per_row += series.cat.codes.dtype.itemsize
    assert per_row == 596
    # Only the index is allocated at native length. No feature values are fabricated.
    full_index = pd.date_range("2020-01-01", periods=count, freq="h", tz="UTC", name="time")
    lookup = full_index._engine.get_indexer(full_index.asi8[::-1])
    assert np.array_equal(lookup, np.arange(count - 1, -1, -1))
    assert full_index._engine.sizeof() > 0
    per_owner = count * per_row + fixed + full_index.memory_usage(deep=True)
    total = 13 * per_owner
    cap = forward._MAX_ACTIVE_SYMBOL_NATIVE_BYTES
    assert cap == 512 * 1024 * 1024
    assert total <= cap, f"THIRTEEN_OWNER_FEATURE_BOUND_EXCEEDED:{total}>{cap}"
    print(json.dumps({"classification": "SYNTHETIC_SCHEMA_CAPACITY_PREFLIGHT_NOT_RSS_OR_MARKET_EVIDENCE",
        "source_rows": count, "owners": 13, "feature_columns": len(sample.columns),
        "bytes_per_row": per_row, "category_fixed_bytes": int(fixed),
        "index_bytes_with_lookup": int(full_index.memory_usage(deep=True)),
        "summed_feature_bound_bytes": int(total), "cap_bytes": cap, "qualification_authority": False}))
