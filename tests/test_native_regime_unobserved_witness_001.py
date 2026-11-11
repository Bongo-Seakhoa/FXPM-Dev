"""Invented native-witness metadata and owned spool I/O. No native or model proof."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_core as core
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_portfolio_replay as portfolio
import pm_spread_profile as spread
import pm_winner_raw_evidence as raw

from prototype_support import _declared_environment
from test_book_multiplicity import _detector_record, _train_support
from test_declared_slot_owner_surface import _request, _structural_native_snapshot
from test_trade_power import _source_contract


_SUFFIX = "native-regime-unobserved-tier-witness"
_TYPE = "declared_native_regime_unobserved_tier_witness"
_SCHEMA = "fxpm.declared-native-regime-unobserved-tier-witness.v1"
_PROFILE = "e" * 64
_BARS = "8" * 64
_SOURCE_ID = "b" * 64
_INVALID = "DECLARED_NATIVE_UNOBSERVED_WITNESS_INVALID"


def _invented_costs(*, profile=_PROFILE, target_bars=_BARS, source_id=_SOURCE_ID, schedule_sha="6" * 64):
    pair = {
        "schema_version": core.BACKTEST_FROZEN_TIER_PAIR_CONTRACT_SCHEMA_VERSION,
        "admission_schema": core.ADMISSION_SCHEMA, "admission_policy": core.ADMISSION_POLICY,
        "tiers": [], "qualification_authority": False,
    }
    seeds = {}
    for tier, points in (("P75", 9), ("P95", 16)):
        point = 0.00001
        descriptor = {
            "schema_version": spread.TIER_SCHEMA, "symbol": "AUDUSD", "timeframe": "H1",
            "tier": tier, "source_timeframe": "M5", "point_price": point,
            "spread_points": points, "full_spread_price": points * point,
            "profile_sha256": profile, "source_identity_sha256": source_id,
            "source_binding": {"target_bars_sha256": target_bars},
            "qualification_authority": False,
        }
        mode = core.build_backtest_spread_mode({
            "mode": "conservative_max", "frozen_spread_tier": descriptor,
            "entry_admission_schema": core.ADMISSION_SCHEMA, "entry_admission_policy": core.ADMISSION_POLICY,
        })
        seed = _source_contract(binding_overrides={"schedule_sha256": schedule_sha},
                                cost_overrides={"spread_mode": mode})
        seeds[tier] = seed
        attestation = seed["numerical_execution_cost_attestation"]
        policy = core.build_numerical_execution_cost_policy(
            use_spread=True, spread_mode=mode, instrument_spec_sha256=attestation["instrument_spec_sha256"],
        )
        pair["tiers"].append({"tier": tier, "frozen_spread_tier": descriptor,
                              "numerical_execution_cost_policy": policy})
    return seeds, pair


def _source_sha(seeds, pair):
    return raw._current_pair_inputs(seeds, pair)[2]["source_contract_sha256"]


def _fit_decisions(product, request, source_contract_sha256):
    activation = product.index["activation"]
    source = book.create_historical_source_group_decision(
        symbol=request["symbol"], timeframe=request["timeframe"], availability=book.AVAILABLE,
        reason_code="AUTHENTICATED_NATIVE_PREFIX_AVAILABLE", source_contract_sha256=source_contract_sha256,
        source_group_evidence_sha256=product.index_sha256, acquisition_attempts_exact=1,
        activation_id=activation["activation_id"], activation_manifest_sha256=activation["manifest_sha256"],
        activation_entry_sha256=activation["entry_sha256"], native_source_sha256=_BARS,
    )
    assert source is not None
    detector = _detector_record(
        source, "a" * 64, cutoff=request["anchor"]["selection_cutoff_utc"].replace("Z", "+00:00"),
    )
    support = _train_support(
        request["symbol"], request["timeframe"],
        (0, 0, 0, 0),
        activation_id=activation["activation_id"], activation_manifest_sha256=activation["manifest_sha256"],
        native_source_sha256=_BARS, params_sha256=detector["parameters_sha256"], detector_code_sha256="a" * 64,
    )
    fit = book.create_regime_fit_decision(
        source_group_decision=source, fit_state=book.REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_CAUSAL_REGIME_FIT_COMPLETE", regime_fit_policy_sha256="1" * 64,
        detector_process_sha256="a" * 64, regime_fit_evidence_sha256="2" * 64,
        four_regime_mapping_sha256="3" * 64, regime_params_sha256=detector["parameters_sha256"],
        regime_detector_record=detector, train_regime_support=support,
    )
    assert fit is not None
    return source, fit, support


def _install_invented_unobserved_source(request, snapshot, monkeypatch, *, mismatch=False):
    """Install the invented source and fit boundaries for one real issued request."""
    body = request.to_dict()
    namespace = body["artifact_namespace"]
    native = producer.native_prefix
    seen = SimpleNamespace(product=None, source=None, fit=None)
    seeds, pair = _invented_costs()

    def provide_prefix(**kwargs):
        index = {
            "source_state": native.AVAILABLE, "symbol": kwargs["symbol"], "timeframe": kwargs["timeframe"],
            "cutoff_utc": kwargs["cutoff_utc"], "source_accessed_at_utc": kwargs["source_accessed_at_utc"],
            "activation": {"activation_id": snapshot["activation_id"],
                           "manifest_sha256": snapshot["activation_manifest_sha256"], "entry_sha256": "d" * 64},
            "source": {"bars_sha256": _BARS, "source_identity_sha256": _SOURCE_ID},
        }
        sink = request.symbol_sink(body["symbol"])
        descriptor = sink.put_json(namespace + "/invented-index", "invented_native_index", index)["descriptor"]
        seen.product = native.NativePrefixProduct(
            source_state=native.AVAILABLE, index=index, index_descriptor=descriptor, index_sha256=descriptor["sha256"],
            native_prefix={"activation_id": index["activation"]["activation_id"],
                           "activation_manifest_sha256": index["activation"]["manifest_sha256"],
                           "activation_entry_sha256": index["activation"]["entry_sha256"], "artifact_sha256": _BARS},
            instrument_snapshot=None, prefix_path=None, prefix_descriptor=None,
            artifact_descriptors=(descriptor,), _seal=native._PRODUCT_SEAL,
        )
        return seen.product

    def verify_prefix(**kwargs):
        assert kwargs["expected_index_sha256"] == seen.product.index_sha256
        assert kwargs["pinned_activation_manifest_sha256"] == snapshot["activation_manifest_sha256"]
        return seen.product

    def fitted(*, product, config, acquisition_attempts_exact):
        assert product is seen.product and acquisition_attempts_exact == 1
        seen.source, seen.fit, support = _fit_decisions(product, body, _source_sha(seeds, pair))
        descriptors = {level["tier"]: deepcopy(level["frozen_spread_tier"]) for level in pair["tiers"]}
        if mismatch:
            descriptors["P75"]["source_binding"]["target_bars_sha256"] = "0" * 64
        return SimpleNamespace(
            product=product, source_group_decision=seen.source, regime_fit_decision=seen.fit,
            train_regime_support=support, source_contract_seeds=seeds, frozen_tier_pair_contract=pair,
            _spread_tiers={tier: spread.FrozenSpreadTier(adapter._canonical_bytes(desc).decode("ascii"))
                           for tier, desc in descriptors.items()},
        )

    def forbidden(*args, **kwargs):
        pytest.fail("unobserved native fixture entered planning, model or actual source I/O")

    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", lambda *a, **k: (deepcopy(snapshot), []))
    monkeypatch.setattr(spread, "load_spread_profile", lambda path: SimpleNamespace(sha256=_PROFILE))
    monkeypatch.setattr(native, "produce_activated_native_prefix", provide_prefix)
    monkeypatch.setattr(native, "reverify_activated_native_prefix_product", lambda product, *a, **k: product)
    monkeypatch.setattr(native, "verify_activated_native_prefix", verify_prefix)
    monkeypatch.setattr(producer.search_execution, "fit_native_timeframe_for_search", fitted)
    monkeypatch.setattr(producer.search_planner, "build_declared_slot_observed_cell_plan", forbidden)
    monkeypatch.setattr(producer.search_execution, "execute_historical_observed_cell", forbidden)
    return seen, seeds, pair


@pytest.mark.parametrize("scenario", ["exact", "descriptor-mismatch", "missing-witness", "repointed-witness"])
def test_native_unobserved_producer_and_public_verifier(tmp_path, monkeypatch, scenario):
    mismatch = scenario == "descriptor-mismatch"
    snapshot = _structural_native_snapshot()
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=snapshot)
    request = _request(env)
    namespace = request.to_dict()["artifact_namespace"]
    seen, seeds, pair = _install_invented_unobserved_source(request, snapshot, monkeypatch, mismatch=mismatch)

    completion = producer.execute_declared_slot_owner_search(
        env.context, request=request, process=env.process, frozen_pipeline_config=env.frozen,
    )
    roster = [entry for entry in env.sink.artifact_roster() if entry["artifact_id"].startswith(namespace + "/")]
    witnesses = [entry for entry in roster if entry["artifact_type"] == _TYPE]
    if mismatch:
        assert completion["status"] == "STOP"
        assert witnesses == []
        source = adapter._reopen_ascii_json_entry(
            env.context, completion["source_session_entry"], expected_type="declared_slot_source_session", code=_INVALID,
        )
        assert source["failure_code"] == "DECLARED_NATIVE_UNOBSERVED_TIER_WITNESS_INVALID"
        return

    assert (completion["status"], completion["reason_code"]) == ("NO_TRADE", "REGIME_UNOBSERVED")
    assert all(completion[name] is None for name in (
        "owner", "horizon_plan", "plan_transport", "frozen_plan_receipt", "frozen_descriptor_graph",
        "horizon_winner_cross_binding", "winner_request_entry", "winner_decision_entry", "raw_production_receipt",
    ))
    (entry,) = witnesses
    assert entry["artifact_id"] == namespace + "/" + _SUFFIX
    expected = {
        "schema_version": _SCHEMA, "request_sha256": request.receipt_sha256,
        "source_session_sha256": completion["source_session_entry"]["descriptor"]["sha256"],
        "native_prefix_index_sha256": seen.product.index_sha256,
        "source_group_decision_sha256": seen.source.receipt_sha256, "regime_fit_decision_sha256": seen.fit.receipt_sha256,
        "profile_sha256": _PROFILE, "source_contract_seeds": seeds, "frozen_tier_pair_contract": pair,
    }
    descriptor = entry["descriptor"]
    assert descriptor["format"] == spool.CANONICAL_JSON_FORMAT
    assert (env.context.root / descriptor["relative_path"]).read_bytes() == spool._canonical_json_bytes(
        expected, env.context.limits.max_json_bytes,
    )
    if scenario == "missing-witness":
        roster = [item for item in roster if item["artifact_id"] != entry["artifact_id"]]
        reason = "DECLARED_NATIVE_UNOBSERVED_WITNESS_REQUIRED"
    elif scenario == "repointed-witness":
        alternate = completion["source_session_entry"]["descriptor"]
        assert alternate != descriptor and alternate["format"] == spool.CANONICAL_JSON_FORMAT
        roster = [
            {**item, "descriptor": alternate} if item["artifact_id"] == entry["artifact_id"] else item
            for item in roster
        ]
        reason = _INVALID
    else:
        assert adapter.verify_declared_slot_completion(
            env.context, precommit=env.precommit, process=env.process, completion=completion,
            artifact_roster=roster, frozen_pipeline_config=env.frozen,
        ) == completion
        return

    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        adapter.verify_declared_slot_completion(
            env.context, precommit=env.precommit, process=env.process, completion=completion,
            artifact_roster=roster, frozen_pipeline_config=env.frozen,
        )
    assert str(caught.value) == reason


def _completion_shape(namespace):
    return {
        "request_body": {"schema_version": orchestration.DECLARED_SLOT_REQUEST_SCHEMA,
                         "artifact_namespace": namespace, "symbol": "AUDUSD", "timeframe": "H1"},
        "status": "NO_TRADE", "reason_code": "REGIME_UNOBSERVED", "plan_transport": None,
        "request_sha256": "1" * 64, "source_session_entry": {"descriptor": {"sha256": "2" * 64}},
    }


@pytest.mark.parametrize("mutation, code", [
    ("missing", "DECLARED_NATIVE_UNOBSERVED_WITNESS_REQUIRED"),
    ("wrong-type", _INVALID), ("duplicate", _INVALID), ("wrong-slot", _INVALID),
    ("missing-status", "DECLARED_NATIVE_UNOBSERVED_WITNESS_FORBIDDEN"),
    ("stop", "DECLARED_NATIVE_UNOBSERVED_WITNESS_FORBIDDEN"),
    ("planned", "DECLARED_NATIVE_UNOBSERVED_WITNESS_FORBIDDEN"),
    ("m5", "DECLARED_NATIVE_UNOBSERVED_WITNESS_FORBIDDEN"),
])
def test_native_witness_roster_refuses_missing_wrong_or_disallowed_entries(mutation, code):
    completion = _completion_shape("invented/slot-01")
    body = completion["request_body"]
    key = body["artifact_namespace"] + "/" + _SUFFIX
    entry = {"artifact_id": key, "artifact_type": _TYPE, "descriptor": {"format": spool.CANONICAL_JSON_FORMAT}}
    entries = {key: entry}
    assert adapter._declared_native_unobserved_witness_entry(body, completion, entries) is entry
    if mutation == "missing":
        entries.clear()
    elif mutation == "wrong-type":
        entry["artifact_type"] = "invented_other_type"
    elif mutation in ("duplicate", "wrong-slot"):
        other = "invented/slot-02/" + _SUFFIX
        if mutation == "wrong-slot":
            entries.clear()
        entries[other] = {**entry, "artifact_id": other}
    elif mutation == "missing-status":
        completion["status"] = "MISSING"
    elif mutation == "stop":
        completion["status"] = "STOP"
    elif mutation == "planned":
        completion["plan_transport"] = {"invented": True}
    else:
        body["schema_version"] = orchestration.DECLARED_M5_SLOT_REQUEST_SCHEMA
    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        adapter._declared_native_unobserved_witness_entry(body, completion, entries)
    assert str(caught.value) == code


@pytest.mark.parametrize("mutation", ["exact", "seed", "pair", "policy", "profile", "target-bars",
                                      "source-identity", "cross-slot", "extra-key"])
def test_native_witness_reopens_canonical_bytes_and_checks_provenance(tmp_path, mutation):
    # The actual caller verifies these metadata carriers before the shared reader.
    # This fixture owns only invented witness bytes and tests that reader directly.
    context = spool.freeze_generation_context(
        tmp_path / "witness-spool", generation_id="invented-witness",
        native_universe_sha256="a" * 64, config_sha256="b" * 64,
    )
    sink = spool.open_symbol_sink(context, "AUDUSD")
    namespace = "invented/slot-01"
    completion = _completion_shape(namespace)
    session = {
        "request_sha256": completion["request_sha256"], "native_prefix_index_sha256": "3" * 64,
        "native_prefix_index_descriptor": {"sha256": "3" * 64}, "source_group_decision_sha256": "4" * 64,
        "regime_fit_decision_sha256": "5" * 64, "spread_profile_identity": {"profile_sha256": _PROFILE},
    }
    index = {"source": {"bars_sha256": _BARS, "source_identity_sha256": _SOURCE_ID}}
    seeds, pair = _invented_costs()
    source_hash = _source_sha(seeds, pair)
    if mutation == "seed":
        seeds, _ = _invented_costs(schedule_sha="0" * 64)
        assert _source_sha(seeds, pair) != source_hash
    elif mutation == "pair":
        pair["tiers"].reverse()
    elif mutation == "policy":
        pair["tiers"][0]["numerical_execution_cost_policy"]["instrument_spec_sha256"] = "0" * 64
    elif mutation in ("profile", "target-bars", "source-identity"):
        # Rebuild a valid pair/seed commitment so only the final source-specific join fails.
        changed = {"profile": "0" * 64} if mutation == "profile" else (
            {"target_bars": "0" * 64} if mutation == "target-bars" else {"source_id": "0" * 64})
        seeds, pair = _invented_costs(**changed)
        source_hash = _source_sha(seeds, pair)

    source = SimpleNamespace(receipt_sha256="4" * 64, to_dict=lambda: {
        "source_group_evidence_sha256": "3" * 64, "native_source_sha256": _BARS,
        "source_contract_sha256": source_hash,
    })
    fit = SimpleNamespace(receipt_sha256="5" * 64)
    witness = {
        "schema_version": _SCHEMA, "request_sha256": "0" * 64 if mutation == "cross-slot" else "1" * 64,
        "source_session_sha256": "2" * 64, "native_prefix_index_sha256": "3" * 64,
        "source_group_decision_sha256": source.receipt_sha256, "regime_fit_decision_sha256": fit.receipt_sha256,
        "profile_sha256": _PROFILE, "source_contract_seeds": seeds, "frozen_tier_pair_contract": pair,
    }
    if mutation == "extra-key":
        witness["invented_extra"] = None
    entry = sink.put_json(namespace + "/" + _SUFFIX, _TYPE, witness)
    arguments = {"entry": entry, "completion": completion, "session": session,
                 "index": index, "source": source, "fit": fit, "profile_sha256": _PROFILE}
    if mutation == "exact":
        assert adapter._reopen_declared_native_unobserved_p75(context, **arguments) == pair["tiers"][0]["frozen_spread_tier"]
    else:
        with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
            adapter._reopen_declared_native_unobserved_p75(context, **arguments)
        assert str(caught.value) == _INVALID
