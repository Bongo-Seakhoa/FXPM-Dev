"""INVENTED metadata: real issuers and custody, no native/model/financial evidence."""
import copy
import hashlib
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_historical_anchor_orchestration as orchestration
import pm_production_anchor_plan as anchors
import pm_spread_profile as spread
from prototype_support import _roster_arguments
from test_development_cell_prefix import prefix_metadata_factory, _prefix_m5_pins


@pytest.fixture
def ordinary_roster():
    arguments = _roster_arguments()
    arguments["launch_activation_utc"] = "2023-06-24T00:00:00Z"
    return anchors.derive_declared_owner_slot_roster_v2(**arguments)


def _issue(env, kind, **selection):
    if kind == "native":
        return orchestration.precommit_declared_slot_owner_run_v2(env.context, **env.kwargs, **selection)
    return orchestration.precommit_declared_m5_slot_owner_run(env.context, **env.kwargs,
        m5_source_pins=_prefix_m5_pins(env), **selection)


@pytest.mark.parametrize("mode,count", [("one_period", 1), ("handover", 2), ("final_study_13", 13)])
def test_ordinary_projection_selects_actual_prefix_and_detaches(ordinary_roster, mode, count):
    original = copy.deepcopy(ordinary_roster)
    selected = orchestration.declared_ordinary_period_selection(ordinary_roster, mode=mode,
        final_study_13=mode == "final_study_13", symbols=("EURUSD",))
    expected = {"mode": mode,
        "selected_slot_ids": [slot["slot_id"] for slot in ordinary_roster["slots"][:count]]}
    assert selected == expected
    assert set(selected) == {"mode", "selected_slot_ids"}
    selected["selected_slot_ids"][0] = "INVENTED_CHANGED_PROJECTION"
    assert ordinary_roster == original
    assert orchestration.declared_ordinary_period_selection(ordinary_roster, mode=mode,
        final_study_13=mode == "final_study_13", symbols=("EURUSD",)) == expected


@pytest.mark.parametrize("final,mode,count", [(False, "one_period", 1), (True, "final_study_13", 13)])
def test_shared_helper_alone_resolves_default_and_existing_final_flag(ordinary_roster, final, mode, count):
    selected = orchestration.declared_ordinary_period_selection(ordinary_roster,
        final_study_13=final, symbols=("EURUSD",))
    assert selected == {"mode": mode,
        "selected_slot_ids": [slot["slot_id"] for slot in ordinary_roster["slots"][:count]]}


@pytest.mark.parametrize("damage", ["null", "extra", "wrong-prefix", "tuple", "duplicate", "bad-mode"])
def test_ordinary_decoder_refuses_malformed_selection(ordinary_roster, damage):
    selected = orchestration.declared_ordinary_period_selection(ordinary_roster, mode="handover", symbols=("EURUSD",))
    body = {"schema_version": orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2,
        "slot_roster": ordinary_roster, "period_selection": selected, "cells": [{"symbol": "EURUSD"}]}
    if damage == "null":
        body["period_selection"] = None
    elif damage == "extra":
        selected["qualification_authority"] = False
    elif damage == "wrong-prefix":
        selected["selected_slot_ids"] = [slot["slot_id"] for slot in ordinary_roster["slots"][1:3]]
    elif damage == "tuple":
        selected["selected_slot_ids"] = tuple(selected["selected_slot_ids"])
    elif damage == "duplicate":
        selected["selected_slot_ids"][1] = selected["selected_slot_ids"][0]
    else:
        selected["mode"] = "INVENTED_UNKNOWN_MODE"
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError,
            match="^DECLARED_ORDINARY_PERIOD_SELECTION_INVALID$"):
        orchestration.declared_slot_period_selection(body)


@pytest.mark.parametrize("schema", [
    orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA,
    orchestration.DECLARED_NATIVE_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA,
    orchestration.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA,
])
def test_ordinary_selection_cannot_relabel_legacy_or_development(ordinary_roster, schema):
    body = {"schema_version": schema, "slot_roster": ordinary_roster}
    assert orchestration.declared_slot_period_selection(body) is None
    body["period_selection"] = orchestration.declared_ordinary_period_selection(ordinary_roster, symbols=("EURUSD",))
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError,
            match="^DECLARED_ORDINARY_PERIOD_SELECTION_INVALID$"):
        orchestration.declared_slot_period_selection(body)


@pytest.mark.parametrize("kind", ["native", "m5"])
@pytest.mark.parametrize("mode,count", [("handover", 2), ("final_study_13", 13)])
def test_ordinary_issued_selection_binds_requests_without_development_scope(
        prefix_metadata_factory, kind, mode, count):
    env = prefix_metadata_factory(full_panel=False)
    selected = orchestration.declared_ordinary_period_selection(env.roster, mode=mode,
        final_study_13=mode == "final_study_13", symbols=(env.cell["symbol"],))
    precommit = _issue(env, kind, period_selection=selected, final_study_13=mode == "final_study_13")
    body = orchestration.verify_declared_slot_owner_run_precommit(env.context, precommit, process=env.process)
    assert body["period_selection"] == selected
    assert body["slot_roster"] == env.roster and len(body["slot_roster"]["slots"]) == 13
    assert body["slot_roster_sha256"] == hashlib.sha256(orchestration._declared_json(env.roster)).hexdigest()
    assert "period_selection_sha256" not in body and "development_scope" not in body
    assert orchestration.declared_slot_development_scope(body) is None
    assert list(orchestration.declared_slot_selected_slots(body)) == env.roster["slots"][:count]
    assert orchestration._validate_declared_precommit_body(env.context, body) is None
    sink = spool.open_symbol_sink(env.context, env.cell["symbol"])
    slot = env.roster["slots"][count - 1]
    request = orchestration.declared_slot_producer_request(env.context, precommit=precommit,
        process=env.process, cell_id=env.cell["cell_id"], slot_id=slot["slot_id"],
        attempt_id="invented-ordinary-selection", sink=sink)
    try:
        issued = orchestration.verify_declared_slot_producer_request(env.context, request,
            precommit=precommit, process=env.process)
        assert issued["precommit_sha256"] == precommit.receipt_sha256
        assert issued["anchor"]["ordinal"] == count and issued["source_access_window_utc"] == env.window
        assert not {"period_selection", "period_selection_sha256", "development_scope"} & set(issued)
    finally:
        orchestration._release_declared_slot_producer_request(request, env.context)
    if count < 13:
        with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_SLOT_SLOT_INVALID$"):
            orchestration.declared_slot_producer_request(env.context, precommit=precommit,
                process=env.process, cell_id=env.cell["cell_id"], slot_id=env.roster["slots"][count]["slot_id"],
                attempt_id="invented-unselected-ordinary", sink=sink)


def _install_documentary_profile(env):
    # The unavailable group is invented metadata, not observations or empirical calibration.
    body = {"schema_version": spread.PROFILE_SCHEMA,
        "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "production_authority": False,
        "fixture_provenance": "INVENTED_METADATA_ONLY_NO_OBSERVATIONS_OR_CALIBRATION",
        "activation": {"id": "invented-ordinary-period-cold", "manifest_sha256": "1" * 64},
        "protected_cutoff_utc": "2024-07-14T08:05:00Z",
        "cells": [{"symbol": env.cell["symbol"], "timeframe": "H1",
            "status": "NO_VALID_SPREAD_OBSERVATIONS", "tiers": {}}]}
    path = Path(env.frozen.materialize(env.context).data_dir) / "native/terms/spread_profile.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orchestration._declared_json(body))
    profile = spread.load_spread_profile(path)
    env.profile_sha = profile.sha256
    env.kwargs["spread_profile_identity"] = {"profile_sha256": profile.sha256}


@pytest.mark.parametrize("kind", ["native", "m5"])
def test_ordinary_default_is_bound_one_period_through_public_cold_reopen(prefix_metadata_factory, kind):
    env = prefix_metadata_factory(full_panel=False)
    _install_documentary_profile(env)
    value = _issue(env, kind)
    body = value.to_dict()
    expected = orchestration.declared_ordinary_period_selection(env.roster, symbols=(env.cell["symbol"],))
    assert body["period_selection"] == expected
    saved_path = env.context.root / f"declared-slots/{value.receipt_sha256}/precommit.json"
    saved = saved_path.read_bytes()
    reopened = orchestration.reopen_declared_slot_owner_run_precommit(env.context,
        receipt_sha256=value.receipt_sha256, process=env.process, frozen_pipeline_config=env.frozen)
    assert reopened is not value and reopened.receipt_sha256 == value.receipt_sha256
    assert reopened._body_json == saved
    assert orchestration.declared_slot_period_selection(reopened.to_dict()) == expected
    assert list(orchestration.declared_slot_selected_slots(reopened.to_dict())) == env.roster["slots"][:1]
    assert not {"development_scope", "period_selection_sha256"} & set(body)
    assert all(body[flag] is False for flag in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))
    crossed = copy.deepcopy(body)
    crossed["period_selection"]["selected_slot_ids"] = [env.roster["slots"][1]["slot_id"]]
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError,
            match="^DECLARED_ORDINARY_PERIOD_SELECTION_INVALID$"):
        orchestration._validate_declared_precommit_body(env.context, crossed)


@pytest.mark.parametrize("intent", ["missing-final-flag", "conflicting-mode", "nonboolean-flag"])
def test_explicit_final_study_gate_and_conflicting_intents_remain_strict(ordinary_roster, monkeypatch, intent):
    calls = []
    monkeypatch.setattr(orchestration, "_precommit_declared_slot_owner_run",
        lambda *args, **kwargs: calls.append(kwargs))
    selected = orchestration.declared_ordinary_period_selection(ordinary_roster,
        mode="handover" if intent == "conflicting-mode" else "final_study_13",
        final_study_13=intent != "conflicting-mode", symbols=("EURUSD",))
    final = True if intent == "conflicting-mode" else 1 if intent == "nonboolean-flag" else False
    code = ("DECLARED_ORDINARY_PERIOD_SELECTION_INVALID" if intent == "conflicting-mode"
        else "DECLARED_FULL_ROSTER_FINAL_STUDY_REQUEST_REQUIRED")
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^" + code + "$"):
        orchestration.precommit_declared_slot_owner_run_v2(object(), run_id="invented-routing",
            process=object(), slot_roster=ordinary_roster, cell_roster=[], frozen_pipeline_config=object(),
            spread_profile_identity={}, source_access_window_utc={}, launch_activation_utc="2023-03-05T00:01:00Z",
            period_selection=selected, final_study_13=final)
    assert calls == []


@pytest.mark.parametrize("kind", ["native", "m5"])
def test_heldout_symbol_requires_explicit_full_study_and_cannot_borrow_adopted_selection(prefix_metadata_factory, kind):
    env = prefix_metadata_factory(full_panel=False, symbol="AUDUSD")
    borrowed = orchestration.declared_ordinary_period_selection(env.roster,
        mode="handover", symbols=("EURUSD",))
    for request in ({}, {"period_selection": borrowed}):
        with pytest.raises(orchestration.HistoricalAnchorOrchestrationError,
                match="^DECLARED_FULL_ROSTER_FINAL_STUDY_REQUEST_REQUIRED$"):
            _issue(env, kind, **request)
    assert not list((env.context.root / "declared-slots").glob("*/precommit.json"))
    legacy = _issue(env, kind, final_study_13=True).to_dict()
    assert "period_selection" not in legacy and len(orchestration.declared_slot_selected_slots(legacy)) == 13
    selected = orchestration.declared_ordinary_period_selection(env.roster,
        final_study_13=True, symbols=(env.cell["symbol"],))
    explicit = _issue(env, kind, final_study_13=True, period_selection=selected).to_dict()
    assert explicit["period_selection"] == selected
    assert len(orchestration.declared_slot_selected_slots(explicit)) == 13
    assert "development_scope" not in explicit
