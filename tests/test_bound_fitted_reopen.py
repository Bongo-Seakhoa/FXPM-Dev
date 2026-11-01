"""Prospective bound-fit codec/refusal tests. Synthetic derived state; no old proof is re-keyed."""
import ast
import copy
from types import SimpleNamespace
from dataclasses import replace
import hashlib
import inspect
import json
import struct
import numpy as np
import pandas as pd
import pytest
import pm_current_refit_artifacts as artifacts
import pm_authoritative_search_execution as execution


def _codec_case():
    frame = pd.DataFrame(index=pd.date_range("2000-01-01", periods=4, tz="UTC", name="synthetic_utc"))
    bits = np.array([0, 0x8000000000000000, 0x7ff8000000000042, 0x3ff0000000000000], dtype="<u8")
    for name in artifacts.BOUND_REGIME_COLUMNS:
        if name in {"REGIME_RAW", "REGIME", "REGIME_LIVE"}:
            frame[name] = pd.Categorical.from_codes([0, 1, -1, 3], dtype=execution.pm_regime.REGIME_DTYPE)
        elif name == "REGIME_WARMUP":
            frame[name] = np.array([True, False, True, False], dtype=bool)
        else:
            frame[name] = bits.view("<f8").copy()
    return frame, {"binding": {"artifact_namespace": "declared-slot/SYNTHETIC/ORIGINAL",
        "generation_context_sha256": "a" * 64, "producer_request_sha256": "b" * 64},
        "frame_sha256": "c" * 64, "synthetic_storage_only": True}


def test_bound_fitted_codec_roundtrip_preserves_nan_signedzero_categories_dtypes_and_utc():
    frame, metadata = _codec_case()
    raw = artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)
    state, actual = artifacts.verify_bound_fitted_state_bytes(raw, expected_sha256=hashlib.sha256(raw).hexdigest())
    assert state["metadata"] == metadata
    assert actual.index.equals(frame.index) and actual.index.name == frame.index.name
    assert actual.index.asi8.tobytes() == frame.index.asi8.tobytes()
    assert list(actual) == list(frame)
    for name in frame:
        assert actual[name].dtype == frame[name].dtype
        if isinstance(frame[name].dtype, pd.CategoricalDtype):
            assert list(actual[name].cat.categories) == list(frame[name].cat.categories)
            assert actual[name].cat.ordered is frame[name].cat.ordered
            assert actual[name].cat.codes.to_numpy().tobytes() == frame[name].cat.codes.to_numpy().tobytes()
        else:
            assert actual[name].to_numpy().tobytes() == frame[name].to_numpy().tobytes()


@pytest.mark.parametrize("kind", ["nan_bit", "ulp", "index_bit"])
def test_bound_fitted_codec_original_external_digest_refuses_bit_changes(kind):
    frame, metadata = _codec_case()
    raw = artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)
    expected = hashlib.sha256(raw).hexdigest()
    prefix = len(artifacts._BOUND_FIT_MAGIC);length = struct.unpack("<Q", raw[prefix:prefix+8])[0]
    offset = prefix + 8 + length
    # First block is UTC int64; each subsequent float column preserves exact IEEE bytes.
    offset += 0 if kind == "index_bit" else 4 * 8 + (2 * 8 if kind == "nan_bit" else 3 * 8)
    changed = bytearray(raw);changed[offset] ^= 1
    with pytest.raises(artifacts.CurrentRefitArtifactError, match="BOUND_FIT_BYTES_INVALID"):
        artifacts.verify_bound_fitted_state_bytes(bytes(changed), expected_sha256=expected)


def _change_header(raw, mutate):
    magic = artifacts._BOUND_FIT_MAGIC;length = struct.unpack("<Q", raw[len(magic):len(magic)+8])[0]
    start = len(magic) + 8;body = json.loads(raw[start:start+length]);data = raw[start+length:]
    mutate(body)
    body.pop("receipt_sha256")
    body["receipt_sha256"] = artifacts.canonical_artifact_sha256(body)
    header = artifacts.canonical_artifact_bytes(body)
    return magic + struct.pack("<Q", len(header)) + header + data


@pytest.mark.parametrize("kind,code", [("reorder", "BOUND_FIT_COLUMN_LAYOUT_INVALID"),
    ("dtype", "BOUND_FIT_DTYPE_INVALID"), ("version", "BOUND_FIT_HEADER_INVALID"),
    ("timezone", "BOUND_FIT_HEADER_INVALID")])
def test_bound_fitted_codec_refuses_unsupported_structure_even_with_consistent_own_hash(kind, code):
    frame, metadata = _codec_case();raw = artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)
    def mutate(body):
        if kind == "reorder":
            body["columns"][1]["name"], body["columns"][2]["name"] = body["columns"][2]["name"], body["columns"][1]["name"]
        elif kind == "dtype":
            body["columns"][1]["dtype"] = "<f4"
        elif kind == "version":
            body["schema_version"] = "UNSUPPORTED-SYNTHETIC-VERSION"
        else:
            body["index_timezone"] = "NON-UTC"
    changed = _change_header(raw, mutate)
    with pytest.raises(artifacts.CurrentRefitArtifactError, match=code):
        artifacts.verify_bound_fitted_state_bytes(changed, expected_sha256=hashlib.sha256(changed).hexdigest())


def test_bound_fitted_codec_does_not_accept_object_arrays_or_timezone_conversion():
    frame, metadata = _codec_case();frame["TREND_SCORE"] = frame["TREND_SCORE"].astype(object)
    with pytest.raises(artifacts.CurrentRefitArtifactError, match="BOUND_FIT_DTYPE_INVALID"):
        artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)
    frame, metadata = _codec_case();frame.index = frame.index.tz_localize(None)
    with pytest.raises(artifacts.CurrentRefitArtifactError, match="BOUND_FIT_FRAME_INVALID"):
        artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)


def test_bound_fitted_private_owners_cannot_be_called_as_trusted_flags():
    for cls in (execution._FreshFitOwner, execution._BoundFitOwner):
        with pytest.raises(execution.AuthoritativeSearchExecutionError):
            cls()


def test_bound_fitted_cold_verifier_has_no_fit_detector_or_seed_probe_call():
    tree = ast.parse(inspect.getsource(execution.reopen_bound_fitted_timeframe))
    calls = [node.func for node in ast.walk(tree) if isinstance(node, ast.Call)]
    forbidden = {"fit_native_timeframe_for_search", "_reverify_fitted_timeframe", "compute_regime_scores",
        "build_evidence_source_seed_result"}
    assert not any((isinstance(fn, ast.Name) and fn.id in forbidden)
        or (isinstance(fn, ast.Attribute) and fn.attr in forbidden) for fn in calls)


def test_bound_fitted_fresh_selector_does_not_publish_or_mint_after_reverify_failure(monkeypatch):
    from types import SimpleNamespace
    fault = RuntimeError("SYNTHETIC-REVERIFY-FAILURE")
    calls = []
    def fail(*args):
        calls.append("existing_reverify")
        raise fault
    monkeypatch.setattr(execution, "_reverify_fitted_timeframe", fail)
    with pytest.raises(RuntimeError) as caught:
        execution._declared_fitted_for_execution(SimpleNamespace(context=object()), object(), object())
    assert caught.value is fault and calls == ["existing_reverify"]


def test_bound_fitted_legacy_reverify_refuses_reopened_owner_before_native_work(monkeypatch):
    fitted = execution.FittedNativeSearchTimeframe(product=None, source_group_decision=None, regime_fit_decision=None,
        source_contract_seed={}, cost_ladder_contract={}, regime_parameters={}, train_regime_support={},
        instrument_spec=None, execution_spec_receipt={}, frame_sha256="0" * 64, _frame=pd.DataFrame(), _bound_fit_owner=object())
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", lambda **kw: pytest.fail("Reopened state reached model fit"))
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="REOPENED_FITTED_STATE_REQUIRES_DECLARED_VERIFIER"):
        execution._reverify_fitted_timeframe(fitted, object())


def _assert_genuine_declared_bound_fit_roundtrip(*, context, request, process, frozen_pipeline_config,
        header, original_fitted, monkeypatch):
    """Native proof body for ONE future fresh declared fixture; never a mocked positive verifier.

    Fixture acquisition/initial fit/actual existing reverify/snapshot/new-version
    freeze must have completed through the real public declared path first.
    This helper does not fabricate a request, owner or snapshot for old records.
    """
    def forbidden(*args, **kwargs):
        pytest.fail("Cold native reopen silently ran fit/regime scores/seed probe")
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", forbidden)
    monkeypatch.setattr(execution, "_reverify_fitted_timeframe", forbidden)
    monkeypatch.setattr(execution.pm_regime.MarketRegimeDetector, "compute_regime_scores", forbidden)
    monkeypatch.setattr(execution.Backtester, "build_evidence_source_seed_result", forbidden)
    actual, current_receipt = execution.reopen_bound_fitted_timeframe(context, request=request, process=process,
        frozen_pipeline_config=frozen_pipeline_config, header=header)
    pairs = [
        (original_fitted.source_group_decision.to_dict(), actual.source_group_decision.to_dict()),
        (original_fitted.regime_fit_decision.to_dict(), actual.regime_fit_decision.to_dict()),
        (dict(original_fitted.source_contract_seed), dict(actual.source_contract_seed)),
        (dict(original_fitted.cost_ladder_contract), dict(actual.cost_ladder_contract)),
        (original_fitted.source_contract_seeds, actual.source_contract_seeds),
        (original_fitted.frozen_tier_pair_contract, actual.frozen_tier_pair_contract),
        (dict(original_fitted.regime_parameters), dict(actual.regime_parameters)),
        (dict(original_fitted.train_regime_support), dict(actual.train_regime_support)),
        (dict(original_fitted.execution_spec_receipt), dict(actual.execution_spec_receipt)),
    ]
    assert all(execution._canonical_bytes(a) == execution._canonical_bytes(b) for a, b in pairs)
    assert original_fitted.frame_sha256 == actual.frame_sha256
    pd.testing.assert_frame_equal(original_fitted._frame, actual._frame, check_exact=True, check_dtype=True)
    for name in original_fitted._frame:
        left, right = original_fitted._frame[name], actual._frame[name]
        if isinstance(left.dtype, pd.CategoricalDtype):
            assert left.cat.codes.to_numpy().tobytes() == right.cat.codes.to_numpy().tobytes()
            assert list(left.cat.categories) == list(right.cat.categories) and left.cat.ordered == right.cat.ordered
        elif left.dtype.kind in "mM":
            assert left.dtype == right.dtype
            assert left.array.asi8.tobytes() == right.array.asi8.tobytes()
        elif left.dtype.kind in "biuf":
            assert left.to_numpy().tobytes() == right.to_numpy().tobytes()
    assert original_fitted._frame.index.asi8.tobytes() == actual._frame.index.asi8.tobytes()
    assert original_fitted._entry_spread_observations.equals(actual._entry_spread_observations)
    assert original_fitted._entry_spread_observations.to_numpy().tobytes() == actual._entry_spread_observations.to_numpy().tobytes()
    assert original_fitted.product.index_sha256 == actual.product.index_sha256
    assert original_fitted._spread_source_product.index_sha256 == actual._spread_source_product.index_sha256
    assert execution.bound_fitted_state_reference(actual, context=context, request=request) == header["fitted_state_entry"]
    assert current_receipt["origin"] == "REOPENED_BOUND_FIT"
    assert current_receipt["validation_started_at_utc"] <= current_receipt["validation_completed_at_utc"]
    return actual, current_receipt


# Contract regression additions.  Invented metadata/storage cases are not a
# complete native fit, Winner verdict, or issued DECLARED300 completion.
def _bound_contract_case(tmp_path, monkeypatch, *, edition=15, runtime_change=None):
    from datetime import date, timedelta
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_production_adapter as adapter
    import pm_training_horizon as horizon
    import pm_production_search_planner as planner
    import pm_winner_raw_evidence as raw
    from tests import test_current_refit_artifacts as refit_fixture
    from tests import test_historical_production_adapter as adapter_fixture
    from tests import test_training_horizon as horizon_fixture
    from tests import test_trade_power as power_fixture
    from tests import test_winner_selection as winner_fixture
    from tests.test_declared_slot_owner_surface import (
        _declared_environment, _structural_live_capture,
    )

    # Reuse the existing labelled structural source-session/plan boundary.
    # Its verifier stubs do NOT prove native ownership.  Actual capture tags,
    # Raw freeze/restore, spool descriptors and registry are never replaced.
    binding_builder = orchestration.historical_horizon_plan_binding
    released_plan = adapter._released_horizon_plan_body
    env = _declared_environment(tmp_path, monkeypatch)
    old_capture, fitted, config, _ = _structural_live_capture(env, monkeypatch)
    request = old_capture.request
    request_body = request.to_dict()
    pair = winner_fixture._current_pair_contract("AUDUSD", "H1")
    seeds = {}
    for level in pair["tiers"]:
        tier = level["tier"]
        policy = level["numerical_execution_cost_policy"]
        seed = power_fixture._source_contract(cost_overrides={key: policy[key] for key in (
            "use_spread", "spread_mode", "instrument_spec_sha256")})
        seeds[tier] = seed
    seeds, pair, pair_hashes = raw._current_pair_inputs(seeds, pair)
    source, fit = adapter_fixture._available_authority(
        "AUDUSD", "H1", pair_hashes["source_contract_sha256"],
    )
    fitted._source_group_decision = source
    fitted._regime_fit_decision = fit

    # A genuine generic Horizon object and actual cell-plan class, explicitly
    # unissued at the production-plan boundary retained by the structural helper.
    horizon_request = horizon_fixture._current_request()
    horizon_request["cell"]["symbol"] = "AUDUSD"
    horizon_request["native_prefix"]["symbol"] = "AUDUSD"
    horizon_plan = horizon.build_historical_horizon_plan(horizon_request)
    assert horizon_plan is not None
    cell_receipt = {"schema_version": "SYNTHETIC_CELL_PLAN_STRUCTURE_ONLY",
                    "symbol": "AUDUSD", "timeframe": "H1", "regime": "TREND",
                    "horizon_plan_receipt_sha256": horizon_plan.receipt_sha256}
    cell_receipt["receipt_sha256"] = execution._sha256(cell_receipt)
    cell_plan = planner.ProductionCellSearchPlan(
        mode="DECLARED_SLOT_UNISSUED", symbol="AUDUSD", timeframe="H1", regime="TREND",
        causal_precommit_receipt_sha256=env.precommit.receipt_sha256,
        search_process_receipt_sha256=env.process.receipt_sha256,
        horizon_plan=horizon_plan, _attempt_artifacts_json=(),
        _state_requirements_json=(), _cell_receipt_json=execution._canonical_bytes(cell_receipt),
        _spool_entry_json=b"{}", boundary_limited=False, _seal=object(),
        _declared_slot_request=request,
    )
    assert cell_plan.receipt_sha256 != horizon_plan.receipt_sha256
    monkeypatch.setattr(orchestration, "historical_horizon_plan_binding", binding_builder)
    monkeypatch.setattr(adapter, "_released_horizon_plan_body", released_plan)
    opener = (adapter.open_declared_slot_capture if edition == 15
              else adapter.open_transport_declared_slot_capture)
    capture = opener(env.context, request=request, process=env.process,
                     plan=cell_plan, source_session=old_capture.source_session)
    assert adapter._verify_declared_slot_capture(capture) is True

    start = date(2021, 1, 1)
    fold_plan = [
        {"anchor_as_of_utc": (start + timedelta(days=ordinal * 90)).isoformat() + "T00:00:00Z",
         "oos_start_date_utc": (start + timedelta(days=ordinal * 90)).isoformat(),
         "oos_end_date_utc": (start + timedelta(days=ordinal * 90 + 89)).isoformat()}
        for ordinal in range(4)
    ]
    normalized_folds, _ = raw._normalize_fold_plan(
        fold_plan, symbol="AUDUSD", timeframe="H1", regime="TREND",
    )
    built = refit_fixture._build(
        symbol="AUDUSD", timeframe="H1",
        fit_start_utc="2018-01-01T00:00:00Z",
        fit_end_utc="2020-01-01T00:00:00Z",
        latest_closed_bar_utc="2019-12-31T23:00:00Z",
    )
    attempts = [{key: getattr(built, key) for key in (
        "winner_process", "horizon_process", "trial_definition", "refit_recipe",
    )}]
    contracts = [{"lookback_kind": "NATIVE_OBSERVATIONS",
                  "lookback_value": 1000, "source_bar_count": 1000}]
    rows = []
    for ordinal in range(4):
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
            identity = ["SYNTHETIC_METADATA_ONLY", ordinal, partition]
            rows.append({
                "attempt_ordinal": 0, "fold_ordinal": ordinal, "partition": partition,
                "partition_geometry_receipt_sha256": execution._sha256(["geometry", identity]),
                "partition_frame_sha256": execution._sha256(["frame", identity]),
                "source_observation_receipt_sha256": execution._sha256(["source", identity]),
                "tiers": {tier: {
                    "numerical_execution_cost_attestation": copy.deepcopy(
                        seeds[tier]["numerical_execution_cost_attestation"]),
                    "entry_observations_sha256": execution._sha256(["observations", identity, tier]),
                    "entry_admission_mask_sha256": execution._sha256(["admission", identity, tier]),
                } for tier in ("P75", "P95")},
            })
    partition_costs = raw.build_partition_cost_precommitments(rows, pair=pair)
    frame, _ = _codec_case()
    parameters = execution._default_regime_parameters("H1")
    runtime = execution._bound_fit_runtime(parameters)
    if runtime_change is not None:
        runtime[runtime_change] = ("0" * 64 if runtime_change == "pm_core_sha256"
                                   else "SYNTHETIC_DIFFERENT_PANDAS_VERSION")
    metadata = {
        "binding": {
            "generation_context_sha256": env.context.context_sha256,
            "producer_request_sha256": request.receipt_sha256,
            "precommit_sha256": request_body["precommit_sha256"],
            "process_sha256": env.process.receipt_sha256,
            "artifact_namespace": capture.raw_artifact_namespace,
            "effective_config_sha256": env.frozen.effective_config_sha256,
            "horizon_plan_receipt_sha256": horizon_plan.receipt_sha256,
            "cell_plan_receipt_sha256": cell_plan.receipt_sha256,
        },
        "snapshot_issued_at_utc": "2026-10-22T11:30:00Z",
        "runtime": runtime, "frame_sha256": execution._frame_sha256(frame),
        "receipts": {
            "source_group_decision": source.to_dict(), "regime_fit_decision": fit.to_dict(),
            "source_contract_seed": {}, "cost_ladder_contract": {},
            "source_contract_seeds": seeds, "frozen_tier_pair_contract": pair,
            "regime_parameters": parameters, "train_regime_support": fit.to_dict()["train_regime_support"],
            "instrument_spec": {}, "execution_spec_receipt": {},
        },
        "fold_plan": normalized_folds,
        "source_session_entry": {}, "source_session_receipt_sha256": "e" * 64,
        "target_carrier": {}, "fine_carrier": {}, "base_columns": [],
        "entry_observations_sha256": "d" * 64,
        "spread_profile_sha256": request_body["spread_profile_identity"]["profile_sha256"],
    }
    reference = artifacts.spool_bound_fitted_state(capture.sink, frame=frame, metadata=metadata)
    arguments = {
        "native_prefix_product": object(),
        "precommitted_at_utc": "2026-10-22T11:31:00Z",
        "attempts_frozen_at_utc": "2026-10-22T11:32:00Z",
        "fold_plan": fold_plan, "attempt_artifacts": attempts,
        "source_contract_seeds": seeds, "frozen_tier_pair_contract": pair,
        "partition_cost_precommitments": partition_costs,
        "risk_unit_contract": raw.build_precommitted_risk_unit_contract(),
        "bound_fitted_state_entry": reference,
    }
    cross = {
        "schema_version": "SYNTHETIC_REGISTRY_BOUNDARY_ONLY",
        "producer_request_sha256": request.receipt_sha256,
        "process_precommit_receipt_sha256": request_body["precommit_sha256"],
        "selection_cutoff_utc": request_body["anchor"]["selection_cutoff_utc"],
        "anchor_id": request_body["slot_id"],
        "horizon_roster": {"plan_receipt_sha256": horizon_plan.receipt_sha256},
        "source_group_decision_sha256": source.receipt_sha256,
        "regime_fit_decision_sha256": fit.receipt_sha256,
    }
    # These two upstream metadata seams isolate the registry defect. They do
    # not authenticate a native count or cross-binding and cannot yield a
    # genuine complete Raw or cold-fitted success.
    monkeypatch.setattr(adapter, "_strict_attempt_artifacts",
                        lambda *a, **k: (attempts, contracts, []))
    monkeypatch.setattr(adapter, "_precommitted_evidence_roles",
                        lambda *a, **k: ["EVALUATION_REQUIRED"])
    monkeypatch.setattr(adapter, "_cross_binding_body", lambda **k: copy.deepcopy(cross))
    return SimpleNamespace(
        env=env, capture=capture, fitted=fitted, config=config, cell_plan=cell_plan,
        arguments=arguments, cross=cross, frame=frame, metadata=metadata,
        reference=reference, attempts=attempts, fold_plan=fold_plan,
    )


@pytest.mark.parametrize("edition", [15, 16], ids=["v6", "v7"])
def test_declared_bound_freeze_reopen_registry_uses_actual_raw_and_capture(tmp_path, monkeypatch, edition):
    import pm_historical_production_adapter as adapter
    import pm_winner_raw_evidence as raw
    case = _bound_contract_case(tmp_path, monkeypatch, edition=edition)
    handle = case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    frozen = handle.frozen
    expected = (raw.BOUND_FIT_V15_FROZEN_PLAN_SCHEMA if edition == 15
                else raw.BOUND_FIT_V16_FROZEN_PLAN_SCHEMA)
    assert json.loads(frozen.frozen_receipt_bytes)["schema_version"] == expected
    entries = case.capture._frozen_raw_entries(frozen)
    bound_entries = [entry for entry in entries if entry["artifact_type"] == "winner_bound_fitted_state"]
    assert len(bound_entries) == 1
    assert bound_entries[0]["descriptor"] == json.loads(
        frozen.frozen_receipt_bytes)["artifact_roster"]["bound_fitted_state"]
    bodies = [execution._bound_fit_entry_bytes(case.env.context, entry) for entry in entries]
    restore = (raw.restore_frozen_winner_search_plan_v6 if edition == 15
               else raw.restore_frozen_winner_search_plan_v7)
    reopened = restore(
        context_identity={
            "generation_id": case.env.context.generation_id,
            "generation_context_sha256": case.env.context.context_sha256,
            "native_universe_sha256": case.env.context.native_universe_sha256,
            "config_sha256": case.env.context.config_sha256,
        },
        symbol="AUDUSD", timeframe="H1", regime="TREND",
        artifact_namespace=case.capture.raw_artifact_namespace,
        manifest_entries=entries, body_bytes=bodies,
    )
    assert reopened.frozen_receipt_bytes == frozen.frozen_receipt_bytes
    assert reopened.bound_fitted_state_entry == case.reference
    assert reopened.attempt_ledger == frozen.attempt_ledger
    assert reopened.precommitment == frozen.precommitment
    assert reopened.source_contract_seeds == frozen.source_contract_seeds
    assert reopened.partition_cost_precommitments == frozen.partition_cost_precommitments
    # Genuine capture tag and version-generation match remain necessary.
    case.capture._winner_generation = (
        execution.winner.V16_GENERATION if edition == 15
        else execution.winner.CURRENT_GENERATION)
    with pytest.raises(adapter.HistoricalProductionAdapterError,
                       match="DECLARED_SLOT_CAPTURE_BINDING_MISMATCH"):
        case.capture._frozen_raw_entries(frozen)


@pytest.mark.parametrize("entry", [None, {}, []], ids=["absent", "empty", "wrong_type"])
def test_actual_declared_freeze_requires_bound_entry_before_raw_writes(tmp_path, monkeypatch, entry):
    import pm_historical_production_adapter as adapter
    case = _bound_contract_case(tmp_path, monkeypatch)
    before = copy.deepcopy(case.capture.sink.artifact_roster())
    case.arguments["bound_fitted_state_entry"] = entry
    with pytest.raises(adapter.HistoricalProductionAdapterError,
                       match="BOUND_FIT_DECLARED_ENTRY_REQUIRED"):
        case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    assert case.capture.sink.artifact_roster() == before
    assert case.capture._active is None


def test_actual_bound_registry_refuses_non_declared_capture(tmp_path, monkeypatch):
    import pm_historical_production_adapter as adapter
    case = _bound_contract_case(tmp_path, monkeypatch)
    handle = case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    foreign = SimpleNamespace(raw_artifact_namespace=case.capture.raw_artifact_namespace)
    with pytest.raises(adapter.HistoricalProductionAdapterError,
                       match="BOUND_FIT_CANARY_OR_LEGACY_CAPTURE_FORBIDDEN"):
        adapter.HistoricalSymbolSearchCapture._frozen_raw_entries(foreign, handle.frozen)


def test_actual_declared_header_binds_real_cell_plan_structure_and_distinct_horizon(tmp_path, monkeypatch):
    case = _bound_contract_case(tmp_path, monkeypatch)
    handle = case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    case.capture.source_session._entry = case.capture.sink.put_json(
        case.capture.raw_artifact_namespace + "/structural-session-test-input",
        "synthetic_source_session_test_input", {"authority": False},
    )
    case.fitted.product = SimpleNamespace(index={"activation": {"manifest_sha256": "a" * 64}})
    case.fitted.frame_sha256 = execution._frame_sha256(case.frame)
    case.fitted.execution_spec_receipt = {"scope": "SYNTHETIC_HEADER_STRUCTURE_ONLY"}
    attempts = [SimpleNamespace(plan_attempt=case.attempts[0])]
    header = execution._make_declared_unit_header(
        case.capture, case.fitted, case.cell_plan, attempts, case.fold_plan,
        handle, [], "2026-10-22T11:33:00Z",
    )
    horizon_sha = case.cell_plan.horizon_plan.receipt_sha256
    cell_sha = case.cell_plan.receipt_sha256
    assert horizon_sha != cell_sha
    assert header["binding"]["horizon_plan_receipt_sha256"] == horizon_sha
    assert header["binding"]["cell_plan_receipt_sha256"] == cell_sha
    assert header["horizon_plan"]["receipt_sha256"] == horizon_sha
    assert header["plan_transport"]["cell_receipt"] == case.cell_plan.cell_receipt
    assert header["plan_transport"]["attempt_artifacts"] == list(case.cell_plan.attempt_artifacts())
    assert header["plan_transport"]["state_requirements"] == list(case.cell_plan.state_requirements())
    assert header["fitted_state_entry"] == case.reference
    assert header["qualification_authority"] is False
    assert header["live_authority"] is False
    stored = execution._bound_fit_entry_bytes(
        case.env.context, {"descriptor": header["horizon_plan"]["descriptor"]},
    )
    assert json.loads(stored) == case.cell_plan.horizon_plan.to_dict()


@pytest.mark.parametrize("field", ["pm_core_sha256", "pandas"])
def test_actual_cold_loader_refuses_resealed_code_or_pandas_drift_before_native_open(
        tmp_path, monkeypatch, field):
    # The external frozen/cross-chain join is supplied as a labelled test seam;
    # the stored state, codec, request/config and runtime refusal are ACTUAL.
    # This negative cannot return a successful fitted owner or native verdict.
    case = _bound_contract_case(tmp_path, monkeypatch, runtime_change=field)
    handle = case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    header = {
        "schema_version": "fxpm.declared-attempt-unit-header.v2",
        "request_body": case.capture.request.to_dict(),
        "binding": {
            "horizon_plan_receipt_sha256": case.cell_plan.horizon_plan.receipt_sha256,
            "cell_plan_receipt_sha256": case.cell_plan.receipt_sha256,
        },
    }
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: "2026-10-22T12:00:00Z")
    monkeypatch.setattr(execution, "_bound_fit_frozen_chain",
                        lambda *a, **k: (handle.frozen, copy.deepcopy(case.cross)))
    monkeypatch.setattr(execution, "_bound_fit_open_carrier",
                        lambda *a, **k: pytest.fail("native open reached before runtime refusal"))
    with pytest.raises(execution.AuthoritativeSearchExecutionError,
                       match="BOUND_FIT_CODE_RUNTIME_OR_RECEIPT_CHANGED"):
        execution.reopen_bound_fitted_timeframe(
            case.env.context, request=case.capture.request, process=case.env.process,
            frozen_pipeline_config=case.env.frozen, header=header,
        )


def test_bound_runtime_refuses_changed_dependency_after_import(monkeypatch):
    original = execution.authoritative_search_execution_contract
    def changed_contract():
        body = copy.deepcopy(original())
        body["dependency_source_sha256"]["pm_core"] = "0" * 64
        return body
    monkeypatch.setattr(execution, "authoritative_search_execution_contract", changed_contract)
    with pytest.raises(execution.AuthoritativeSearchExecutionError,
                       match="BOUND_FIT_SOURCE_CHANGED_AFTER_IMPORT"):
        execution._bound_fit_runtime(execution._default_regime_parameters("H1"))


def _resealed_state_data(raw, mutate):
    start = len(artifacts._BOUND_FIT_MAGIC)
    size = struct.unpack("<Q", raw[start:start + 8])[0]
    header = json.loads(raw[start + 8:start + 8 + size])
    data = bytearray(raw[start + 8 + size:])
    mutate(header, data)
    header["data_sha256"] = hashlib.sha256(data).hexdigest()
    header.pop("receipt_sha256")
    header["receipt_sha256"] = artifacts.canonical_artifact_sha256(header)
    encoded = artifacts.canonical_artifact_bytes(header)
    return artifacts._BOUND_FIT_MAGIC + struct.pack("<Q", len(encoded)) + encoded + bytes(data)


@pytest.mark.parametrize("kind,expected", [
    ("boolean", "BOUND_FIT_DTYPE_INVALID"),
    ("category", "BOUND_FIT_CATEGORY_INVALID"),
    ("duplicate_index", "BOUND_FIT_INDEX_INVALID"),
])
def test_resealed_inner_bit_patterns_still_obey_typed_codec(kind, expected):
    frame, metadata = _codec_case()
    raw = artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)
    def mutate(header, data):
        if kind == "duplicate_index":
            data[8:16] = data[0:8]
        else:
            name = "REGIME_WARMUP" if kind == "boolean" else "REGIME"
            column = next(item for item in header["columns"] if item["name"] == name)
            data[column["offset"]] = 2 if kind == "boolean" else 127
    changed = _resealed_state_data(raw, mutate)
    with pytest.raises(artifacts.CurrentRefitArtifactError, match=expected):
        artifacts.verify_bound_fitted_state_bytes(
            changed, expected_sha256=hashlib.sha256(changed).hexdigest(),
        )


@pytest.mark.parametrize("column", ["first_float64", "__utc_index__"])
def test_resealed_payload_digest_does_not_replace_inner_data_integrity(column):
    frame, metadata = _codec_case()
    raw = artifacts.build_bound_fitted_state_bytes(frame=frame, metadata=metadata)
    start = len(artifacts._BOUND_FIT_MAGIC)
    size = struct.unpack("<Q", raw[start:start + 8])[0]
    header = json.loads(raw[start + 8:start + 8 + size])
    item = (next(item for item in header["columns"] if item["dtype"] == "<f8")
            if column != "__utc_index__" else header["columns"][0])
    changed = bytearray(raw)
    changed[start + 8 + size + item["offset"]] ^= 1
    changed = bytes(changed)
    with pytest.raises(artifacts.CurrentRefitArtifactError, match="BOUND_FIT_RECEIPT_INVALID"):
        artifacts.verify_bound_fitted_state_bytes(
            changed, expected_sha256=hashlib.sha256(changed).hexdigest(),
        )


@pytest.mark.parametrize("carrier", ["header", "validation", "actual_plan"])
@pytest.mark.parametrize("axis", ["horizon", "cell"])
def test_cold_plan_join_keeps_cell_and_horizon_identities_separate(carrier, axis):
    import pm_historical_search_producer as producer
    horizon_sha, cell_sha = "a" * 64, "c" * 64
    header = {"binding": {
        "horizon_plan_receipt_sha256": horizon_sha,
        "cell_plan_receipt_sha256": cell_sha,
    }}
    validation = {
        "original_horizon_plan_receipt_sha256": horizon_sha,
        "original_cell_plan_receipt_sha256": cell_sha,
    }
    plan = SimpleNamespace(receipt_sha256=cell_sha,
                           horizon_plan=SimpleNamespace(receipt_sha256=horizon_sha))
    assert producer._require_reopened_declared_plan(header, validation, plan) is None
    wrong = cell_sha if axis == "horizon" else horizon_sha
    if carrier == "header":
        header["binding"][axis + "_plan_receipt_sha256"] = wrong
    elif carrier == "validation":
        validation["original_" + axis + "_plan_receipt_sha256"] = wrong
    elif axis == "horizon":
        plan.horizon_plan.receipt_sha256 = wrong
    else:
        plan.receipt_sha256 = wrong
    with pytest.raises(producer.HistoricalSearchProducerError,
                       match="DECLARED_BOUND_FIT_ORIGINAL_PLAN_CHANGED"):
        producer._require_reopened_declared_plan(header, validation, plan)


@pytest.mark.parametrize("required", [False, 1, "true"])
def test_bound_native_carrier_refuses_non_strict_production_authority_before_native(required, monkeypatch):
    carrier = {
        "index_descriptor": {}, "index_sha256": "a" * 64,
        "symbol": "AUDUSD", "timeframe": "H1", "cutoff_utc": "2020-01-01T00:00:00Z",
        "activation_id": "SYNTHETIC", "activation_manifest_sha256": "b" * 64,
        "production_contract_required": required,
    }
    monkeypatch.setattr(execution.native_prefix, "verify_activated_native_prefix",
                        lambda **kw: pytest.fail("invalid authority reached native verification"))
    with pytest.raises(execution.AuthoritativeSearchExecutionError,
                       match="BOUND_FIT_NATIVE_CARRIER_INVALID"):
        execution._bound_fit_open_carrier(object(), SimpleNamespace(data_dir="SYNTHETIC"), carrier)


def test_bound_reference_rejects_other_producer_request_without_issuing_owner(tmp_path, monkeypatch):
    case = _bound_contract_case(tmp_path, monkeypatch)
    reference = copy.deepcopy(case.reference)
    reference["producer_request_sha256"] = "0" * 64
    with pytest.raises(artifacts.CurrentRefitArtifactError,
                       match="BOUND_FIT_REFERENCE_BINDING_INVALID"):
        artifacts.read_bound_fitted_state_reference(
            case.env.context, reference,
            artifact_namespace=case.capture.raw_artifact_namespace,
        )


def _execute_original_declared_header_seam(scope):
    """Execute only the actual call/publication/resume statements, not an executor clone.

    Outer native/planner/authority setup is excluded. The supplied fixture is
    structural and unissued; actual helpers, spool publisher and failure branch
    remain in use. Plain source aliases are evaluated rather than name-pinned.
    """
    function = execution.execute_historical_observed_cell
    tree = ast.parse(inspect.getsource(function))
    targets = {execution._declared_unit_binding, execution._make_declared_unit_header}
    filename = inspect.getsourcefile(function)
    namespace = dict(scope)
    global_scope = vars(execution)

    def target_call(node):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            return None
        value = namespace.get(node.func.id, global_scope.get(node.func.id))
        return next((target for target in targets if value is target), None)

    # Resolve harmless object/function Name aliases that precede the real seam.
    # No strategy/native/model calls from surrounding executor setup are run.
    ordered = sorted(ast.walk(tree), key=lambda node: (
        getattr(node, "lineno", 10**9), getattr(node, "col_offset", 0)))
    for node in ordered:
        if target_call(node) is not None:
            break
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Name)
                and (node.value.id in namespace or node.value.id in global_scope)):
            exec(compile(ast.Module(body=[copy.deepcopy(node)], type_ignores=[]),
                         filename, "exec"), global_scope, namespace)
    owners = [
        node for node in ast.walk(tree) if isinstance(node, ast.If)
        and targets <= {target_call(child) for child in ast.walk(node)}
    ]
    assert owners, "actual declared binding/header seam is absent"
    owner = min(owners, key=lambda node: node.end_lineno - node.lineno)
    last = next(
        ordinal for ordinal, statement in enumerate(owner.body)
        if isinstance(statement, ast.If)
        and execution._make_declared_unit_header in {
            target_call(child) for child in ast.walk(statement)}
    )
    # The real header conditional includes the actual publish and resumed-binding
    # refusal. Later per-attempt/native work is intentionally outside this probe.
    code = ast.Module(body=copy.deepcopy(owner.body[:last + 1]), type_ignores=[])
    exec(compile(ast.fix_missing_locations(code), filename, "exec"),
         global_scope, namespace)
    return namespace


def test_actual_executor_header_call_seam_publishes_and_refuses_changed_resume_bindings(
        tmp_path, monkeypatch):
    import pm_training_horizon as horizon
    from tests import test_training_horizon as horizon_fixture
    case = _bound_contract_case(tmp_path, monkeypatch)
    handle = case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    case.capture.source_session._entry = case.capture.sink.put_json(
        case.capture.raw_artifact_namespace + "/actual-call-seam-session-test-input",
        "synthetic_source_session_test_input", {"authority": False},
    )
    case.fitted.product = SimpleNamespace(index={"activation": {"manifest_sha256": "a" * 64}})
    case.fitted.frame_sha256 = execution._frame_sha256(case.frame)
    case.fitted.execution_spec_receipt = {"scope": "SYNTHETIC_HEADER_CALL_SEAM_ONLY"}
    inputs = {
        "capture": case.capture, "fitted": case.fitted, "refreshed": case.fitted,
        "cell_plan": case.cell_plan, "horizon_plan": case.cell_plan.horizon_plan,
        "attempts": [SimpleNamespace(plan_attempt=case.attempts[0])],
        "fold_plan": case.fold_plan, "handle": handle,
        "source_observation_references": [],
        "evidence_started": "2026-10-22T11:33:00Z", "declared_header": None,
    }
    fresh = _execute_original_declared_header_seam(inputs)
    header = fresh["declared_header"]
    assert header["binding"] == fresh["binding"]
    assert header["binding"]["cell_plan_receipt_sha256"] == case.cell_plan.receipt_sha256
    assert header["binding"]["horizon_plan_receipt_sha256"] == case.cell_plan.horizon_plan.receipt_sha256
    assert case.cell_plan.receipt_sha256 != case.cell_plan.horizon_plan.receipt_sha256
    path = case.env.context.root / execution._declared_unit_relative(
        case.capture.request.to_dict(), "header.json")
    persisted = path.read_bytes()
    assert json.loads(persisted) == header
    assert header["qualification_authority"] is False and header["live_authority"] is False

    resumed_inputs = {**inputs, "declared_header": copy.deepcopy(header)}
    resumed = _execute_original_declared_header_seam(resumed_inputs)
    assert resumed["binding"] == fresh["binding"]
    assert resumed["declared_header"] == header
    assert path.read_bytes() == persisted

    different_cell = case.cell_plan.cell_receipt
    different_cell.pop("receipt_sha256")
    different_cell["synthetic_resume_identity"] = "OTHER_UNISSUED_CELL"
    different_cell["receipt_sha256"] = execution._sha256(different_cell)
    changed_cell_plan = replace(
        case.cell_plan, _cell_receipt_json=execution._canonical_bytes(different_cell))
    request = horizon_fixture._current_request()
    request["cell"]["symbol"] = "AUDUSD"
    request["native_prefix"]["symbol"] = "AUDUSD"
    request["horizon_policy"]["policy_id"] += "-resume-other"
    different_horizon = horizon.build_historical_horizon_plan(request)
    assert different_horizon.receipt_sha256 != case.cell_plan.horizon_plan.receipt_sha256
    changed_horizon_plan = replace(case.cell_plan, horizon_plan=different_horizon)
    for changed_plan in (changed_cell_plan, changed_horizon_plan):
        with pytest.raises(execution.AuthoritativeSearchExecutionError,
                           match="DECLARED_UNIT_HEADER_BINDING_MISMATCH"):
            _execute_original_declared_header_seam({
                **resumed_inputs, "cell_plan": changed_plan,
                "horizon_plan": changed_plan.horizon_plan,
            })
        assert path.read_bytes() == persisted


@pytest.mark.parametrize("axis", ["horizon", "cell"])
def test_actual_cold_loader_refuses_header_vs_persisted_snapshot_identity_before_native(
        tmp_path, monkeypatch, axis):
    # The same explicitly labelled external frozen/cross-chain test seam as the
    # runtime negatives; stored codec/request/config and new H/C guard are real.
    case = _bound_contract_case(tmp_path, monkeypatch)
    handle = case.capture.freeze_observed_cell("H1", "TREND", **case.arguments)
    header = {
        "schema_version": "fxpm.declared-attempt-unit-header.v2",
        "request_body": case.capture.request.to_dict(),
        "binding": {
            "horizon_plan_receipt_sha256": case.cell_plan.horizon_plan.receipt_sha256,
            "cell_plan_receipt_sha256": case.cell_plan.receipt_sha256,
        },
    }
    header["binding"][axis + "_plan_receipt_sha256"] = "0" * 64
    monkeypatch.setattr(execution, "_now_utc_seconds", lambda: "2026-10-22T12:00:00Z")
    monkeypatch.setattr(execution, "_bound_fit_frozen_chain",
                        lambda *a, **k: (handle.frozen, copy.deepcopy(case.cross)))
    monkeypatch.setattr(execution, "_bound_fit_open_carrier",
                        lambda *a, **k: pytest.fail("native open reached before original H/C refusal"))
    with pytest.raises(execution.AuthoritativeSearchExecutionError,
                       match="BOUND_FIT_ORIGINAL_CHAIN_CHANGED"):
        execution.reopen_bound_fitted_timeframe(
            case.env.context, request=case.capture.request, process=case.env.process,
            frozen_pipeline_config=case.env.frozen, header=header,
        )
