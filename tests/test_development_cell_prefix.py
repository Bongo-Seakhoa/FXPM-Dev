"""INVENTED metadata fixtures: real issuers, no native/model/financial evidence.

The QA image must provide the coherent product source closure used by the real
issuers. No verifier, source clock or issuer result is substituted here. The
profile and M5 view/cost pins below are documentary invented identities only.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_cap_calibration as cap
import pm_historical_anchor_orchestration as orchestration
import pm_production_anchor_plan as anchors
import pm_production_config_binding as config_binding
import pm_production_search_planner as planner


_PREFIX_NOTICE = "INVENTED_DEVELOPMENT_METADATA_NO_NATIVE_MODEL_OR_FINANCIAL_PROOF"


def _prefix_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


@pytest.fixture
def prefix_metadata_factory(tmp_path):
    sequence = []

    def create(*, full_panel=True, symbol=None):
        sequence.append(None)
        root = (tmp_path / ("invented-prefix-" + str(len(sequence)))).resolve()
        root.mkdir()
        symbol = symbol or orchestration.DECLARED_DEVELOPMENT_SYMBOLS[0]
        run_id = "invented-development-prefix-" + str(len(sequence))
        payload = {"_meta": {"scope": "UNISSUED_TECHNICAL_BENCHMARK",
            "fixture_classification": _PREFIX_NOTICE},
            "pipeline": {"data_dir": str(root / "data"), "output_dir": str(root / "outputs"),
                "log_dir": str(root / "logs"), "manual_kill_switch_path": str(root / "unused-stop.json"),
                "regime_params_file": str(root / "absent-regime-params.json"), "research_mode": True,
                "initial_capital": 10000.0, "discovery_risk_per_trade_pct": 1.0,
                "risk_per_trade_pct": 0.25, "backtest_spread_mode": "conservative_max",
                "oos_validation_mode": "fixed_holdout"},
            "symbols": [symbol], "position": {}, "storage": {}}
        config_path = root / "invented-config.json"
        config_path.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        owned, effective = config_binding.load_effective_pipeline_config(config_path)
        context = spool.freeze_generation_context(root / "spool", generation_id=run_id,
            native_universe_sha256=_prefix_sha({"classification": _PREFIX_NOTICE, "role": "unverified-universe"}),
            config_sha256=cap.canonical_json_sha256(owned))
        frozen = config_binding.freeze_production_pipeline_config(context, active_config_path=config_path,
            config_payload=owned, supplied_config=effective)
        freeze = (planner.freeze_route_a_full_panel_production_search_process if full_panel
                  else planner.freeze_production_search_process)
        process = freeze(context, run_id=run_id)
        launch = "2023-06-24T00:00:00Z"
        roster = anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=launch,
            frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
            config_identity_refs={"config_sha256": context.config_sha256,
                "effective_config_sha256": frozen.effective_config_sha256},
            extend_frozen_clock_backward=True,
            protected_end_exclusive_utc="2024-07-14T08:05:00Z")
        cell = {"cell_id": book.compute_cell_id(symbol, "H1", "TREND"),
            "symbol": symbol, "timeframe": "H1", "regime": "TREND"}
        profile_sha = _prefix_sha({"classification": _PREFIX_NOTICE, "role": "unverified-profile"})
        now = datetime.now(timezone.utc).replace(microsecond=0)
        window = {"start_utc": (now-timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_exclusive_utc": (now+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")}
        kwargs = dict(run_id=run_id, process=process, slot_roster=roster, cell_roster=[cell],
            frozen_pipeline_config=frozen, spread_profile_identity={"profile_sha256": profile_sha},
            source_access_window_utc=window, launch_activation_utc=launch)
        return SimpleNamespace(root=root, context=context, frozen=frozen, process=process,
            roster=roster, cell=cell, window=window, profile_sha=profile_sha, kwargs=kwargs)

    return create


def _prefix_native_issue(env, ids):
    return orchestration.precommit_declared_development_native_slot_owner_run(
        env.context, **env.kwargs, development_slot_ids=ids)


def _prefix_m5_pins(env):
    """Valid documentary shape; not a verified derived view or cost receipt."""
    identity = {"receipt_sha256": _prefix_sha({"classification": _PREFIX_NOTICE, "role": "cost-receipt"}),
        "generation_id": "invented-unverified-cost", "native_universe_sha256": env.context.native_universe_sha256,
        "plan_sha256": _prefix_sha({"classification": _PREFIX_NOTICE, "role": "cost-plan"}),
        "implementation_sha256": _prefix_sha({"classification": _PREFIX_NOTICE, "role": "cost-code"}),
        "profile_sha256": env.profile_sha}
    return {"schema_version": orchestration.DECLARED_M5_SOURCE_PINS_SCHEMA,
        "source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
        "recipe_sha256": "85e321bc794c59cf26e725bbb7772b2590a7b0256e2f6e6086ee3bf52189c1f4",
        "constructor_identity": orchestration._declared_m5_constructor_identity(),
        "view_plan_sha256": _prefix_sha({"classification": _PREFIX_NOTICE, "role": "unissued-view"}),
        "gap_findings_sha256": _prefix_sha({"classification": _PREFIX_NOTICE, "role": "unexamined-gaps"}),
        "cost_edition_identity": identity}


@pytest.mark.parametrize("k", [1, 2, 3])
def test_native_development_issuer_keeps_full13_and_issues_only_selected_prefix(prefix_metadata_factory, k):
    env = prefix_metadata_factory()
    slots = env.roster["slots"]
    selected = [slot["slot_id"] for slot in slots[:k]]
    precommit = _prefix_native_issue(env, selected)
    body = orchestration.verify_declared_slot_owner_run_precommit(env.context, precommit, process=env.process)
    assert body["schema_version"] == orchestration.DECLARED_NATIVE_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA
    assert orchestration.declared_slot_transport_edition(body) == orchestration.DECLARED_WINNER_TRANSPORT_V16
    assert orchestration.declared_slot_source_kind(body) == "broker_native"
    assert body["slot_roster"] == env.roster and len(body["slot_roster"]["slots"]) == 13
    assert list(orchestration.declared_slot_selected_slots(body)) == slots[:k]
    assert body["development_scope"]["selected_slot_ids"] == selected
    assert body["development_scope"]["design_contaminated"] is True
    assert body["development_scope"]["pristine_validation"] is False
    assert not {"source_kind", "m5_source_pins", "m5_view_contract_sha256"} & set(body)
    sink = spool.open_symbol_sink(env.context, env.cell["symbol"])
    for slot in slots[:k]:
        request = orchestration.declared_slot_producer_request(env.context, precommit=precommit,
            process=env.process, cell_id=env.cell["cell_id"], slot_id=slot["slot_id"],
            attempt_id="invented-selected-" + str(slot["ordinal"]), sink=sink)
        try:
            value = orchestration.verify_declared_slot_producer_request(env.context, request,
                precommit=precommit, process=env.process)
            assert value["schema_version"] == orchestration.DECLARED_NATIVE_DEVELOPMENT_SLOT_REQUEST_SCHEMA
            assert value["development_scope"] == body["development_scope"]
            assert value["anchor"]["ordinal"] == slot["ordinal"]
            assert value["anchor"]["selection_cutoff_utc"] == slot["native_cutoff_utc"]
            assert value["source_access_window_utc"] == env.window
            assert not {"source_kind", "m5_source_pins", "m5_view_contract_sha256"} & set(value)
        finally:
            orchestration._release_declared_slot_producer_request(request, env.context)
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_SLOT_SLOT_INVALID$"):
        orchestration.declared_slot_producer_request(env.context, precommit=precommit, process=env.process,
            cell_id=env.cell["cell_id"], slot_id=slots[k]["slot_id"], attempt_id="invented-unselected", sink=sink)


@pytest.mark.parametrize("bad", ["empty", "second", "reversed", "duplicate", "four"])
def test_native_development_refuses_nonprefix_selection(prefix_metadata_factory, bad):
    env = prefix_metadata_factory()
    ids = [slot["slot_id"] for slot in env.roster["slots"]]
    values = {"empty": [], "second": ids[1:2], "reversed": ids[1::-1],
        "duplicate": [ids[0], ids[0]], "four": ids[:4]}
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_DEVELOPMENT_PREFIX_INVALID$"):
        _prefix_native_issue(env, values[bad])


@pytest.mark.parametrize("source_kind", ["native", "m5"])
def test_development_issuers_default_to_the_first_period(prefix_metadata_factory, source_kind):
    env = prefix_metadata_factory(full_panel=source_kind == "native")
    if source_kind == "native":
        value = orchestration.precommit_declared_development_native_slot_owner_run(env.context, **env.kwargs)
    else:
        value = orchestration.precommit_declared_development_m5_slot_owner_run(env.context, **env.kwargs,
            m5_source_pins=_prefix_m5_pins(env))
    parent = value.to_dict()
    first = [env.roster["slots"][0]["slot_id"]]
    assert parent["development_scope"]["selected_slot_ids"] == first
    assert [slot["slot_id"] for slot in orchestration.declared_slot_selected_slots(parent)] == first
    assert len(parent["slot_roster"]["slots"]) == 13


@pytest.mark.parametrize("kind", ["native_v2", "m5_full"])
def test_ordinary_issuers_default_to_one_period_and_require_explicit_final_study(prefix_metadata_factory, kind):
    env = prefix_metadata_factory(full_panel=False)
    issuer = (orchestration.precommit_declared_slot_owner_run_v2 if kind == "native_v2"
        else orchestration.precommit_declared_m5_slot_owner_run)
    extra = {} if kind == "native_v2" else {"m5_source_pins": _prefix_m5_pins(env)}
    for request in ({}, {"final_study_13": False}):
        selected = issuer(env.context, **env.kwargs, **extra, **request).to_dict()
        assert selected["period_selection"] == orchestration.declared_ordinary_period_selection(env.roster,
            symbols=(env.cell["symbol"],))
        assert len(orchestration.declared_slot_selected_slots(selected)) == 1
        assert "development_scope" not in selected
    before = list((env.context.root / "declared-slots").glob("*/precommit.json"))
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError,
            match="^DECLARED_FULL_ROSTER_FINAL_STUDY_REQUEST_REQUIRED$"):
        issuer(env.context, **env.kwargs, **extra, final_study_13=1)
    assert list((env.context.root / "declared-slots").glob("*/precommit.json")) == before
    parent = issuer(env.context, **env.kwargs, **extra, final_study_13=True).to_dict()
    assert not {"development_scope", "final_study_13", "period_selection"} & set(parent)
    assert len(orchestration.declared_slot_selected_slots(parent)) == 13


def test_scheduler_refuses_a_fresh_full_roster_issue_without_the_final_study_marker(prefix_metadata_factory):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    development = _prefix_native_scheduler_contract(env, 1)
    row = {key: value for key, value in development["cells"][0].items()
        if key not in ("development", "native_finalization")}
    row["root"] = str(env.root.parent / "invented-plain-fresh-root")
    contract = {key: development[key] for key in ("native_universe_sha256", "config_sha256", "profile_sha256",
        "active_config_path")}
    contract.update(schema_version=scheduler.DECLARED_SCHEDULER_SCHEMA, cells=[row])
    checked, _ = scheduler._declared_contract(contract)
    result = scheduler._declared_run_one_cell(checked, checked["cells"][0])
    assert result["status"] == "FAILED" and result["reason_code"] == "DECLARED_FULL_ROSTER_FINAL_STUDY_REQUEST_REQUIRED"
    assert not list((Path(row["root"]) / "declared-slots").glob("*/precommit.json"))


def test_native_development_requires_actual_full_panel_process(prefix_metadata_factory):
    env = prefix_metadata_factory(full_panel=False)
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_DEVELOPMENT_NATIVE_PROCESS_INVALID$"):
        _prefix_native_issue(env, [env.roster["slots"][0]["slot_id"]])


@pytest.mark.parametrize("kind", ["native_v1", "native_v2", "m5_full", "m5_development"])
def test_legacy_native_and_m5_issuers_preserve_old_schema_and_roster(prefix_metadata_factory, kind):
    env = prefix_metadata_factory(full_panel=False)
    if kind == "native_v1":
        value = orchestration.precommit_declared_slot_owner_run(env.context, **env.kwargs)
        expected = orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA
    elif kind == "native_v2":
        value = orchestration.precommit_declared_slot_owner_run_v2(env.context, **env.kwargs, final_study_13=True)
        expected = orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2
    elif kind == "m5_full":
        value = orchestration.precommit_declared_m5_slot_owner_run(env.context, **env.kwargs,
            m5_source_pins=_prefix_m5_pins(env), final_study_13=True)
        expected = orchestration.DECLARED_M5_SLOT_PRECOMMIT_SCHEMA
    else:
        value = orchestration.precommit_declared_development_m5_slot_owner_run(env.context, **env.kwargs,
            m5_source_pins=_prefix_m5_pins(env),
            development_slot_ids=[slot["slot_id"] for slot in env.roster["slots"][:2]])
        expected = orchestration.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA
    body = orchestration.verify_declared_slot_owner_run_precommit(env.context, value, process=env.process)
    assert body["schema_version"] == expected and body["slot_roster"] == env.roster
    assert len(body["slot_roster"]["slots"]) == 13
    assert len(orchestration.declared_slot_selected_slots(body)) == (2 if kind == "m5_development" else 13)
    assert orchestration.declared_slot_source_kind(body) == ("m5_derived" if kind.startswith("m5") else "broker_native")


def test_native_development_refuses_unadopted_symbol(prefix_metadata_factory):
    symbol = next(value for value in book.PINNED_SYMBOL_ORDER
        if value not in orchestration.DECLARED_DEVELOPMENT_SYMBOLS)
    env = prefix_metadata_factory(symbol=symbol)
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_DEVELOPMENT_SYMBOL_NOT_ADOPTED$"):
        _prefix_native_issue(env, [env.roster["slots"][0]["slot_id"]])


@pytest.mark.parametrize("field,value", [("source_kind", "m5_derived"),
    ("m5_source_pins", {"classification": _PREFIX_NOTICE}), ("m5_view_contract_sha256", "f" * 64)])
def test_native_development_decoder_refuses_mixed_m5_metadata(prefix_metadata_factory, field, value):
    env = prefix_metadata_factory()
    body = _prefix_native_issue(env, [env.roster["slots"][0]["slot_id"]]).to_dict()
    body[field] = value
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_SLOT_PRECOMMIT_INVALID$"):
        orchestration._validate_declared_precommit_body(env.context, body)


def test_native_development_does_not_replace_full_roster_with_prefix(prefix_metadata_factory):
    env = prefix_metadata_factory()
    shortened = json.loads(json.dumps(env.roster))
    shortened["slots"] = shortened["slots"][:1]
    shortened["slot_count"] = 1
    kwargs = {**env.kwargs, "slot_roster": shortened}
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError):
        orchestration.precommit_declared_development_native_slot_owner_run(env.context, **kwargs,
            development_slot_ids=[shortened["slots"][0]["slot_id"]])




def _prefix_native_scheduler_contract(env, k):
    import pm_historical_scheduler as scheduler
    base = env.root.parent / (env.root.name + "-retained")
    row = {"root": str(env.root), "run_id": env.kwargs["run_id"], "cell": env.cell,
        "launch_activation_utc": env.kwargs["launch_activation_utc"],
        "frozen_refit_clock": {"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        "protected_end_exclusive_utc": "2024-07-14T08:05:00Z", "extend_frozen_clock_backward": True,
        "source_access_window_policy": {"lead_seconds": 0, "validity_seconds": 172800},
        "development": {"source_kind": "broker_native", "selected_slot_ids": [s["slot_id"] for s in env.roster["slots"][:k]]},
        "native_finalization": {"end_grid_root": str(base / "grid"), "source_fence_path": str(base / "fence.json"),
            "retirement": {"receipt_path": str(base / "retirement.json"), "max_paths": 100000,
                "max_nodes": 200000, "max_bytes": 8 * 1024**3}}}
    scope = {key: value for key, value in scheduler._DECLARED_M5_SCOPE.items() if key != "source_kind"}
    scope["adopted_symbols"] = list(orchestration.DECLARED_DEVELOPMENT_SYMBOLS)
    return {"schema_version": scheduler.DECLARED_DEVELOPMENT_CELL_SCHEDULER_SCHEMA,
        "native_universe_sha256": env.context.native_universe_sha256, "config_sha256": env.context.config_sha256,
        "profile_sha256": env.profile_sha, "active_config_path": str(env.frozen.active_config_path),
        "execution_edition": planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION, "scope": scope, "cells": [row]}


@pytest.mark.parametrize("k", [1, 2, 3])
def test_native_prefix_account_measurement_and_missing_commits_are_honest(prefix_metadata_factory, k):
    import pm_forward_oos_replay_producer as forward
    env = prefix_metadata_factory()
    ids = [s["slot_id"] for s in env.roster["slots"][:k]]
    precommit = _prefix_native_issue(env, ids)
    measurement = forward._declared_native_development_cell_measurement(precommit.to_dict())
    assert measurement["refit_cycles"] == k and measurement["calendar_days"] == 28*k
    assert measurement["terminal_policy"] == "MARK_OPEN"
    inputs = forward._reopen_declared_native_development_cell_inputs(env.context, precommit=precommit,
        process=env.process, frozen_pipeline_config=env.frozen)
    assert inputs["progress_status"] == "DEVELOPMENT_INCOMPLETE"
    assert inputs["commits"] == [] and inputs["owner_records"] == []
    assert [row["slot_id"] for row in inputs["slot_schedule"]] == ids
    assert all(row["status"] == "MISSING" for row in inputs["slot_schedule"])
    body = forward._declared_native_development_cell_body(env.context, precommit=precommit,
        process=env.process, frozen_pipeline_config=env.frozen, inputs=inputs)
    assert body["schema_version"] == forward.DECLARED_NATIVE_DEVELOPMENT_CELL_RAW_ACCOUNT_SCHEMA
    assert body["account"] is None and body["saved_cell_run_receipt_sha256"] is None
    assert body["progress_status"] == "DEVELOPMENT_INCOMPLETE" and body["source_kind"] == "broker_native"


def test_new_development_scheduler_decodes_multiple_native_cells(prefix_metadata_factory):
    import pm_historical_scheduler as scheduler
    first = prefix_metadata_factory(symbol="EURUSD")
    second = prefix_metadata_factory(symbol="USDJPY")
    contract = _prefix_native_scheduler_contract(first, 1)
    contract["cells"].extend(_prefix_native_scheduler_contract(second, 3)["cells"])
    checked, groups = scheduler._declared_development_contract(contract)
    assert list(groups) == ["EURUSD", "USDJPY"] and len(checked["cells"]) == 2
    assert [len(row["development"]["selected_slot_ids"]) for row in checked["cells"]] == [1, 3]
    assert checked["execution_edition"] == planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION


def _prefix_multicell_m5_contract(tmp_path):
    """INVENTED routing/admission metadata; no actual cost edition or study authority."""
    from copy import deepcopy
    from test_m5_development_scheduler import M5DevelopmentSchedulerTests
    import pm_historical_scheduler as scheduler
    fixture = M5DevelopmentSchedulerTests(methodName="runTest")
    fixture.root = tmp_path / "invented-m5-metadata"
    fixture.root.mkdir()
    contract = fixture.declaration()
    contract["schema_version"] = scheduler.DECLARED_DEVELOPMENT_CELL_SCHEDULER_SCHEMA
    contract["scope"] = {key: value for key, value in contract["scope"].items() if key != "source_kind"}
    contract["scope"]["adopted_symbols"] = list(orchestration.DECLARED_DEVELOPMENT_SYMBOLS)
    contract["execution_edition"] = planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION
    template = contract["cells"][0]
    for ordinal, symbol in enumerate(("XAGUSD", "EURUSD")):
        row = deepcopy(template)
        row["root"] = str((fixture.root / (symbol + "-spool")).resolve())
        row["run_id"] += "-" + symbol
        row["cell"]["symbol"] = symbol
        row["cell"]["cell_id"] = book.compute_cell_id(symbol, "H1", "TREND")
        dev = row["m5_development"]
        row["development"] = {"source_kind": "m5_derived", "selected_slot_ids": dev["development_slot_ids"]}
        for axis in dev["view_axes"]: axis["symbol"] = symbol
        import pm_m5_job_view as views
        dev["m5_source_pins"]["view_plan_sha256"] = views.declared_view_plan_sha256(tuple(dev["view_axes"]))
        dev["cost_group_metadata"]["symbol"] = symbol
        for name in ("end_grid_root", "source_fence_path"):
            dev[name] = str((fixture.root / (symbol + "-" + name)).resolve())
        dev["retirement"]["receipt_path"] = str((fixture.root / (symbol + "-retirement.json")).resolve())
        original = json.loads(Path(dev["gap_provenance"]["admission_record"]["path"]).read_text())
        original.update(cell={"symbol": symbol, "timeframe": "H1", "regime": "TREND"},
            decision_event="INVENTED_SOFTWARE_METADATA_NOT_A_CANONICAL_DECISION",
            decision_event_canonical_sha256=_prefix_sha({"classification": _PREFIX_NOTICE, "symbol": symbol}),
            fixture_provenance=_PREFIX_NOTICE)
        encoded = json.dumps(original, sort_keys=True, separators=(",", ":")).encode("ascii")
        path = fixture.root / (symbol + "-invented-admission.json")
        path.write_bytes(encoded)
        dev["gap_provenance"]["admission_record"] = {"path": str(path.resolve()), "bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest()}
        if ordinal == 0: contract["cells"] = [row]
        else: contract["cells"].append(row)
    return contract


def test_new_m5_roster_preserves_per_cell_cost_metadata_and_rejects_transplant(tmp_path):
    from copy import deepcopy
    import pm_historical_scheduler as scheduler
    contract = _prefix_multicell_m5_contract(tmp_path)
    checked, groups = scheduler._declared_development_contract(contract)
    assert list(groups) == ["XAGUSD", "EURUSD"] and len(checked["cells"]) == 2
    wrong = deepcopy(contract)
    wrong["cells"][1]["m5_development"]["cost_group_metadata"]["symbol"] = "XAGUSD"
    with pytest.raises(scheduler.HistoricalSchedulerError) as caught:
        scheduler._declared_development_contract(wrong)
    assert caught.value.code == "DECLARED_M5_RECORDED_CELL_METADATA_INVALID"


def _prefix_native_grid_fixture(tmp_path, k):
    """INVENTED owned-payload/Grid fixture; real retirement IO, no native results."""
    from test_completed_generation_retirement import CompletedGenerationRetirementTests, _write
    import pm_end_grid as grid
    legacy = CompletedGenerationRetirementTests(methodName="runTest")
    legacy.parent = tmp_path
    context, entries, args = legacy.fixture()
    retained = args["source_fence_path"].parent
    grid_root = retained / "native-prefix-grid"
    grid_root.mkdir()
    start = datetime(2023, 6, 24, tzinfo=timezone.utc)
    accepted = [f"owner-slot-{ordinal + 1:02d}-{start + timedelta(days=28*ordinal):%Y%m%dT%H%M%SZ}" for ordinal in range(13)]
    selected, rows = [], []
    scope = orchestration._declared_development_scope_body({"slots": [{"slot_id": value} for value in accepted]}, accepted[:k])
    account_body = {"schema_version": "fxpm.declared-development-native-cell-raw-account.v1",
        "generation_id": context.generation_id, "generation_context_sha256": context.context_sha256,
        "declared_precommit_sha256": "1"*64, "search_process_receipt_sha256": "2"*64,
        "cell": {"cell_id": book.compute_cell_id("XAGUSD", "H1", "TREND"), "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"},
        "source_kind": "broker_native", "result_kind": "DEVELOPMENT_PARTIAL", "design_contaminated": True,
        "pristine_validation": False, "progress_status": "DEVELOPMENT_COMPLETE",
        "development_scope": scope, "development_scope_sha256": _prefix_sha(scope),
        "saved_cell_run_receipt_sha256": "3"*64,
        "slot_summaries": [{"ordinal": i+1, "slot_id": slot, "status": "NO_TRADE",
            "commit_sha256": _prefix_sha({"fixture": _PREFIX_NOTICE, "slot": slot})} for i, slot in enumerate(accepted[:k])],
        "account": {"engine_result": {"fixture_provenance": _PREFIX_NOTICE, "fixture_note": "árvíztűrő", "equity_curve": [10000.0, 10000.0],
            "trades": [], "cash": 10000.0, "final_equity": 10000.0}},
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    sink = spool.open_symbol_sink(context, "INVENTED_NATIVE_PREFIX_ACCOUNT")
    account_entry = sink.put_json("invented-account", "software_fixture", account_body)
    sink.seal()
    import pm_historical_scheduler as scheduler
    retained_account = scheduler._declared_retain_account_result(context, account_entry["descriptor"],
        end_grid_root=grid_root, account_body=account_body)
    for ordinal in range(k):
        first, end = start + timedelta(days=28*ordinal), start + timedelta(days=28*(ordinal+1))
        cutoff, slot = first-timedelta(hours=1), accepted[ordinal]
        view = {"schema_version": "fxpm.broker-native-end-grid-view.v1", "symbol": "XAGUSD", "timeframe": "H1",
            "cutoff_utc": cutoff.isoformat(), "activation_id": "INVENTED_SOFTWARE_ACTIVATION",
            "activation_manifest_sha256": "4"*64, "activation_entry_sha256": "5"*64,
            "native_bars_sha256": "6"*64, "continuity_policy_sha256": "7"*64, "continuity_geometry_sha256": "8"*64}
        result = {"precommit_sha256": "1"*64, "cell_id": book.compute_cell_id("XAGUSD", "H1", "TREND"),
            "slot_id": slot, "ordinal": ordinal+1, "cycle_start_utc": first.isoformat(), "cycle_end_utc": end.isoformat(),
            "completion_status": "NO_TRADE", "reason_code": "SOFTWARE_NO_TRADE_ONLY", "request_sha256": "9"*64,
            "slot_commit_sha256": _prefix_sha({"fixture": _PREFIX_NOTICE, "slot": slot}), "source_session_sha256": "a"*64,
            "native_prefix_index_sha256": "b"*64, "source_accessed_at_utc": cutoff.isoformat(),
            "saved_cell_run_receipt_sha256": "3"*64}
        contract_sha = grid.store_view_contract(grid_root, {"schema_version": "fxpm.broker-native-end-grid-reconstruction.v1",
            "view": view, "result": result, "cost_tier_evidence": {"semantics": "BOUND_BASELINE_DESCRIPTOR_NO_SELECTED_WINNER",
                "tier": "P75", "bound_baseline_descriptor": {"fixture_provenance": _PREFIX_NOTICE}},
            "retained_account_result": retained_account})
        rows.append(grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND", cycle_start_utc=first.isoformat(),
            cycle_end_utc=end.isoformat(), information_cutoff_utc=cutoff.isoformat(), outcome="NO_TRADE", reason_code="SOFTWARE_NO_TRADE_ONLY",
            source_kind="broker_native", source_contract_sha256=None, source_identity={key: view[key] for key in
                ("activation_id", "activation_manifest_sha256", "activation_entry_sha256", "native_bars_sha256")},
            no_trade_run_receipt_sha256="3"*64, view_key=_prefix_sha(view), view_contract_sha256=contract_sha,
            cost_tier="P75", process_edition_sha256="2"*64, code_sha256={"software-fixture": "f"*64}, metrics={}, produced_at_utc=end.isoformat()))
        selected.append({"slot_id": slot, "outcome": "NO_TRADE", "cycle_start_utc": first.isoformat(),
            "cycle_end_utc": end.isoformat(), "information_cutoff_utc": cutoff.isoformat()})
    written = grid.EndGridWriter(grid_root).append_many_same_symbol(rows)
    scope = orchestration._declared_development_scope_body({"slots": [{"slot_id": value} for value in accepted]}, accepted[:k])
    fence = {"schema_version": spool.NATIVE_DEVELOPMENT_COMPLETION_FENCE_SCHEMA,
        "workspace_owner_sha256": args["workspace_owner_sha256"], "binding": {"generation_context_sha256": context.context_sha256,
            "run_id": context.generation_id, "cell_id": book.compute_cell_id("XAGUSD", "H1", "TREND"), "cell_contract_sha256": "c"*64,
            "search_process_receipt_sha256": "2"*64, "precommit_sha256": "1"*64},
        "completion": {"cell_run_receipt_sha256": "3"*64, "account_artifact_sha256": account_entry["descriptor"]["sha256"]},
        "retained_account_result": retained_account,
        "selected_slots": selected, "end_grid_rows": {slot["slot_id"]: {"path": item["path"], "row_sha256": item["row_sha256"]}
            for slot, item in zip(selected, written, strict=True)}, "development_scope": scope,
        "development_scope_sha256": _prefix_sha(scope), "accepted_slot_ids": accepted,
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    path = retained / "native-development-prefix-fence.json"
    args.update(source_fence_path=path, source_fence_sha256=_write(path, fence))
    return context, entries, args, grid_root, fence


@pytest.mark.parametrize("k", [1, 2, 3])
def test_native_prefix_retires_only_after_real_all_period_grid_readback(tmp_path, k):
    import pm_end_grid as grid
    context, entries, args, grid_root, fence = _prefix_native_grid_fixture(tmp_path, k)
    record = spool.retire_completed_generation(context.root, **args)
    assert record["status"] == "ARCHIVED_RECORD_ONLY"
    reference = record["retained_account_result"]
    assert reference == fence["retained_account_result"]
    retained_body = json.loads(Path(reference["path"]).read_bytes())
    assert retained_body["account"]["engine_result"]["equity_curve"] == [10000.0, 10000.0]
    assert retained_body["account"]["engine_result"]["fixture_note"] == "árvíztűrő"
    assert "árvíztűrő".encode("utf-8") in Path(reference["path"]).read_bytes()
    assert not Path(reference["path"]).is_relative_to(context.root)
    assert hashlib.sha256(Path(reference["path"]).read_bytes()).hexdigest() == reference["sha256"]
    assert reference["size_bytes"] == Path(reference["path"]).stat().st_size
    reopened = spool.read_generation_retirement(context.root,
        expected_completed_fence_schema=spool.NATIVE_DEVELOPMENT_COMPLETION_FENCE_SCHEMA)
    assert reopened["development_scope_sha256"] == fence["development_scope_sha256"]
    assert set(reopened["end_grid_rows"]) == set(fence["accepted_slot_ids"][:k])
    actual = grid.read_end_grid(grid_root, "XAGUSD")
    assert len(actual) == k and {r.row_sha256 for r in actual} == {v["row_sha256"] for v in record["end_grid_rows"].values()}


@pytest.mark.parametrize("corruption", ["missing-period", "legacy13-schema", "nonprefix"])
def test_native_prefix_retirement_refuses_wrong_or_missing_period_before_release(tmp_path, corruption):
    from test_completed_generation_retirement import _write
    context, entries, args, _grid_root, fence = _prefix_native_grid_fixture(tmp_path, 2)
    if corruption == "missing-period": del fence["end_grid_rows"][fence["accepted_slot_ids"][1]]
    elif corruption == "nonprefix": fence["accepted_slot_ids"][0], fence["accepted_slot_ids"][1] = fence["accepted_slot_ids"][1], fence["accepted_slot_ids"][0]
    else:
        fence["schema_version"] = spool.NATIVE_COMPLETION_FENCE_SCHEMA
        for key in ("development_scope", "development_scope_sha256", "accepted_slot_ids"): del fence[key]
    args["source_fence_sha256"] = _write(args["source_fence_path"], fence)
    with pytest.raises(spool.AuthoritySpoolError): spool.retire_completed_generation(context.root, **args)
    assert (context.root / spool.CONTEXT_RELATIVE_PATH).exists()


@pytest.mark.parametrize("corruption", ["missing", "changed"])
def test_retained_native_account_missing_or_changed_refuses_payload_release(tmp_path, corruption):
    """A final account cannot disappear while its working results are deleted."""
    context, entries, args, _grid_root, fence = _prefix_native_grid_fixture(tmp_path, 1)
    path = Path(fence["retained_account_result"]["path"])
    if corruption == "missing":
        path.unlink()
    else:
        path.write_bytes(b"{}")
    with pytest.raises(spool.AuthoritySpoolError):
        spool.retire_completed_generation(context.root, **args)
    assert (context.root / spool.CONTEXT_RELATIVE_PATH).exists()
    assert any((context.root / entry["descriptor"]["relative_path"]).exists() for entry in entries)


@pytest.mark.parametrize("native_reason,stop_reason,scheduler_reason,expected", [
    ("ACCOUNT_REFUSED", "PERIOD_TWO_STOP", "SCHEDULER_FAILED", "ACCOUNT_REFUSED"),
    (None, "PERIOD_TWO_STOP", "SCHEDULER_FAILED", "PERIOD_TWO_STOP"),
    (None, None, "SCHEDULER_FAILED", "SCHEDULER_FAILED"),
])
def test_commit_periods_survive_account_finalization_refusal(prefix_metadata_factory,
        native_reason, stop_reason, scheduler_reason, expected):
    """Distinct invented reasons prove finalization/STOP/scheduler precedence."""
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    contract, _ = scheduler._declared_development_contract(_prefix_native_scheduler_contract(env, 3))
    row = contract["cells"][0]
    states = ["COMMITTED", "STOPPED", "NOT_RUN"] if stop_reason else ["COMMITTED"] * 3
    actual = scheduler._declared_cell_row(row, "FAILED", scheduler_reason)
    actual["native_finalization_reason_code"] = native_reason
    actual["period_facts"] = [{"ordinal": ordinal, "slot_id": slot, "status": state,
        "reason_code": stop_reason if state == "STOPPED" else None,
        "commit_sha256": None if state == "NOT_RUN" else _prefix_sha({"fixture": _PREFIX_NOTICE, "ordinal": ordinal}),
        "completion_status": None if state == "NOT_RUN" else "STOP" if state == "STOPPED" else "NO_TRADE",
        "end_grid_reference": None}
        for ordinal, (slot, state) in enumerate(zip(row["development"]["selected_slot_ids"], states, strict=True), 1)]
    result = scheduler.build_declared_development_completion(contract, {"cells": [actual]}).to_dict()["cells"][0]
    assert result["status"] == "NOT_FINALIZED" and result["reason_code"] == expected
    assert [period["status"] for period in result["periods"]] == states
    assert result["completed_periods"] == states.count("COMMITTED")
    assert result["retained_account_result"] is None


@pytest.mark.parametrize("reported", ["NOT_RUN", "FAILED", "UNRESOLVED_CRASH", "ARCHIVED", "COMPLETED"])
def test_dispatch_completed_without_commit_evidence_cannot_finalize(prefix_metadata_factory, reported):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    contract, _ = scheduler._declared_development_contract(_prefix_native_scheduler_contract(env, 1))
    actual = scheduler._declared_cell_row(contract["cells"][0], reported, "INVENTED_DISPATCH_LABEL")
    result = scheduler.build_declared_development_completion(contract, {"cells": [actual]}).to_dict()["cells"][0]
    assert result["status"] == "NOT_FINALIZED" and result["period_evidence_status"] == "NOT_OBSERVED"
    assert [item["status"] for item in result["periods"]] == ["NOT_OBSERVED"]
    assert result["reported_status"] == reported


@pytest.mark.parametrize("kind", ["disk", "memory"])
def test_actual_capacity_does_not_abort_or_record_terminal_stop(kind):
    """A transient capacity failure cannot become a permanent trading refusal."""
    import errno
    import pm_historical_scheduler as scheduler
    error = OSError(errno.ENOSPC, "invented disk full") if kind == "disk" else MemoryError("invented allocation failure")
    reason = "DISK_LOW" if kind == "disk" else "MEMORY_ERROR"
    # Capacity exits before inspecting a source owner or creating abort/STOP records.
    result = scheduler._declared_cell_row({"descriptor_index": 0, "root": "unused", "run_id": "unused",
        "cell": {"cell_id": "f"*64, "symbol": "EURUSD"}}, "NOT_RUN", reason)
    scheduler._declared_m5_record_terminal_refusal(result, context=None, workspace_sha=None, precommit=None,
        process=None, frozen=None, config=None, contract=None, row=None, cell_contract_sha256=None,
        resumed=False, source_attempted=True, owner_created=True, owner=None, phase="ACCOUNT", run_body=None)
    assert "terminal_refusal_record_status" not in result
    assert scheduler._declared_cell_deferral({}, {}, result) == reason
    assert spool.resource_capacity_reason(error) == reason
    assert scheduler._declared_m5_capture_source_abort(owner=object(), context=None, precommit=None,
        process=None, cell_id="f"*64, source_error=error, resumed=False, source_attempted=True,
        owner_created=True, controller_started=False, dispatch_lease_held=True, phase="SOURCE_ACQUISITION") is None


def test_missing_source_commit_is_counted_without_financial_completion(prefix_metadata_factory):
    """A durable MISSING commit is neither unrun nor a completed account period."""
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    contract, _ = scheduler._declared_development_contract(_prefix_native_scheduler_contract(env, 1))
    row = contract["cells"][0]
    actual = scheduler._declared_cell_row(row, "FAILED", "SOURCE_UNAVAILABLE")
    actual["period_facts"] = [{"ordinal": 1, "slot_id": row["development"]["selected_slot_ids"][0],
        "status": "COMMITTED", "completion_status": "MISSING", "reason_code": "SOURCE_UNAVAILABLE",
        "commit_sha256": _prefix_sha({"fixture": _PREFIX_NOTICE, "commit": "MISSING"}), "end_grid_reference": None}]
    body = scheduler.build_declared_development_completion(contract, {"cells": [actual]}).to_dict()["cells"][0]
    assert body["status"] == "NOT_FINALIZED" and body["completed_periods"] == 0
    assert body["committed_periods"] == 1
    assert body["periods"][0]["completion_status"] == "MISSING"
    assert body["periods"][0]["reason_code"] == "SOURCE_UNAVAILABLE"


def test_ordinary_native_archive_keeps_account_without_development_fact_contract(tmp_path):
    """A retained ordinary13 result must not acquire a DEVELOPMENT-only roster requirement."""
    import pm_historical_scheduler as scheduler
    expected = {"descriptor_index": 0, "root": str(tmp_path / "working"), "run_id": "invented-native13",
        "cell": {"cell_id": "f"*64, "symbol": "EURUSD"}, "native_finalization": {}}
    body = {"schema_version": "fxpm.declared-cell-raw-account.v4", "fixture_provenance": _PREFIX_NOTICE,
        "account": {"engine_result": {"trades": [], "equity_curve": [10000.0]}}}
    saved = scheduler._declared_m5_external_record(tmp_path / "ordinary-account.json", body, maximum_bytes=4096)
    reference = {key: saved[key] for key in ("path", "sha256", "size_bytes")}
    record = {"generation_context_sha256": "1"*64, "retirement_receipt_path": str(tmp_path / "receipt.json"),
        "retirement_receipt_sha256": "2"*64, "source_fence": {"path": str(tmp_path / "fence.json"), "sha256": "3"*64},
        "end_grid_rows": {}, "precommit_sha256": "4"*64, "retained_account_result": reference}
    actual = scheduler._declared_cell_row(expected, "COMPLETED", pid=123)
    actual["source_kind"] = "broker_native"
    scheduler._declared_m5_archived_row(actual, record)
    assert "period_facts" not in actual
    checked = scheduler._declared_validate_result({"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA,
        "symbol": "EURUSD", "cells": [actual], "closes_admission": False}, [expected], "EURUSD", worker_pid=123)
    assert checked["cells"][0]["retained_account_result"] == reference
    assert json.loads(Path(reference["path"]).read_bytes()) == body


def test_original_commit_and_grid_only_stop_survive_refusal_retirement(tmp_path, monkeypatch):
    """Invented metadata, real commit/Grid/retirement IO; no source or issuer claim."""
    from test_terminal_refusal_retirement import TerminalRefusalRetirementTests, _write
    import pm_historical_scheduler as scheduler
    import pm_historical_search_producer as producer
    harness = TerminalRefusalRetirementTests(methodName="runTest")
    harness.parent = tmp_path
    row_change = {"cycle_start_utc": "2023-07-22T00:00:00+00:00", "cycle_end_utc": "2023-08-19T00:00:00+00:00",
        "information_cutoff_utc": "2023-07-21T00:00:00+00:00"}
    def second_period(fence):
        old = fence["refusal"]["slot_id"]
        fence["refusal"].update(slot_id="software-slot-02", **row_change)
        fence["end_grid_rows"]["software-slot-02"] = fence["end_grid_rows"].pop(old)
    context, payload, args, fence = harness.fixture(fence_change=second_period, row_change=row_change)
    precommit = SimpleNamespace(receipt_sha256=fence["binding"]["precommit_sha256"],
        to_dict=lambda: {"fixture_provenance": _PREFIX_NOTICE})
    cell_id = fence["binding"]["cell_id"]
    commit = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
        "generation_context_sha256": context.context_sha256, "precommit_sha256": precommit.receipt_sha256,
        "cell_id": cell_id, "slot_id": "software-slot-01", "ordinal": 1, "previous_commit_sha256": None,
        "completion": {"status": "NO_TRADE", "reason_code": "INVENTED_NO_TRADE"}, "artifact_roster": []}
    descriptor = spool.write_canonical_json_artifact(context, commit)
    relative = producer._declared_commit_path(precommit, cell_id, 1)
    pointer = context.root / relative
    pointer.parent.mkdir(parents=True)
    _write(pointer, {"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA, "commit_descriptor": descriptor})
    actual_commit = producer._read_declared_commit(context, relative)
    assert actual_commit[0] == commit
    reference = fence["end_grid_rows"]["software-slot-02"]
    slots = [{"ordinal": 1, "slot_id": "software-slot-01", "activation_utc": "2023-06-24T00:00:00+00:00",
        "end_exclusive_utc": "2023-07-22T00:00:00+00:00", "native_cutoff_utc": "2023-06-23T00:00:00+00:00"},
        {"ordinal": 2, "slot_id": "software-slot-02", "activation_utc": row_change["cycle_start_utc"],
         "end_exclusive_utc": row_change["cycle_end_utc"], "native_cutoff_utc": row_change["information_cutoff_utc"]}]
    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda _: slots)
    source_row = {"cell": {"cell_id": cell_id}, "development": {"selected_slot_ids": [x["slot_id"] for x in slots]}}
    captured = {"status": "FAILED", "reason_code": "INVENTED_REENTRY"}
    scheduler._declared_capture_period_facts(captured, context, precommit, source_row)
    assert [item["status"] for item in captured["period_facts"]] == ["COMMITTED", "NOT_RUN"]
    assert captured["period_facts"][0]["commit_sha256"] == actual_commit[1]
    fence["period_facts"] = captured["period_facts"]
    fence["period_facts"][1] = {"ordinal": 2, "slot_id": "software-slot-02", "status": "STOPPED",
        "completion_status": "STOP", "reason_code": fence["refusal"]["reason_code"],
        "commit_sha256": None, "end_grid_reference": reference}
    args["source_fence_sha256"] = _write(args["source_fence_path"], fence)
    reused = {"status": "FAILED", "reason_code": "INVENTED_REENTRY"}
    scheduler._declared_capture_period_facts(reused, context, precommit, source_row)
    assert scheduler._declared_m5_reuse_recorded_rows(reused,
        end_grid_root=Path(reference["path"]).parent, source_fence_path=args["source_fence_path"],
        cell={name: fence["refusal"][name] for name in ("symbol", "timeframe", "regime")} | {"cell_id": cell_id},
        slots=slots, process_sha256=fence["binding"]["search_process_receipt_sha256"],
        generation_context_sha256=context.context_sha256, precommit_sha256=precommit.receipt_sha256)
    assert reused["period_facts"] == fence["period_facts"]
    args["owned_direct_files"] = (*args["owned_direct_files"], relative)
    retired = spool.retire_completed_generation(context.root, **args)
    assert not pointer.exists() and not (context.root / descriptor["relative_path"]).exists()
    reopened = spool.read_generation_retirement(context.root)
    assert reopened["period_facts"] == fence["period_facts"] == retired["period_facts"]
    expected = {"descriptor_index": 0, "root": str(context.root), "run_id": context.generation_id,
        "cell": {"cell_id": cell_id, "symbol": "XAGUSD"}}
    row = scheduler._declared_cell_row(expected, "NOT_RUN")
    scheduler._declared_m5_refusal_archived_row(row, reopened)
    assert [item["status"] for item in row["period_facts"]] == ["COMMITTED", "STOPPED"]
    assert row["period_facts"][0]["commit_sha256"] == actual_commit[1]
    assert row["period_facts"][1]["commit_sha256"] is None
    assert row["period_facts"][1]["end_grid_reference"] == reference


def _retained_account_atomic_case(tmp_path):
    context, _entries, _args, grid_root, fence = _prefix_native_grid_fixture(tmp_path, 1)
    body = json.loads(Path(fence["retained_account_result"]["path"]).read_bytes())
    descriptor = spool.write_canonical_json_artifact(context, body)
    destination = grid_root.parent / "atomic-account-result"
    destination.mkdir()
    return context, body, descriptor, destination


@pytest.mark.parametrize("failure", ["disk", "memory"])
def test_interrupted_retained_account_copy_cleans_only_its_temporary_and_can_complete(tmp_path, monkeypatch, failure):
    """A partial software write cannot poison the one permanent account filename."""
    import errno
    import os
    import pm_historical_scheduler as scheduler
    context, body, descriptor, destination = _retained_account_atomic_case(tmp_path)
    original = context.root / descriptor["relative_path"]
    original_bytes = original.read_bytes()
    target = destination / ("declared-account-" + descriptor["sha256"] + ".json")
    def interrupted(fd, data):
        assert os.write(fd, data[:7]) == 7  # Real partial owned-temp IO, then an injected capacity failure.
        if failure == "disk":
            raise OSError(errno.ENOSPC, "INVENTED_SOFTWARE_DISK_FULL")
        raise MemoryError("INVENTED_SOFTWARE_ALLOCATION_FAILURE")
    with monkeypatch.context() as controlled:
        controlled.setattr(spool, "_write_all", interrupted)
        with pytest.raises((spool.AuthoritySpoolError, MemoryError)) as caught:
            scheduler._declared_retain_account_result(context, descriptor, end_grid_root=destination, account_body=body)
        assert spool.resource_capacity_reason(caught.value) == ("DISK_LOW" if failure == "disk" else "MEMORY_ERROR")
    assert not target.exists() and not list((destination / "tmp").glob("*.part"))
    assert original.read_bytes() == original_bytes
    # This second local software call asserts recovery; it grants no production retry.
    reference = scheduler._declared_retain_account_result(context, descriptor, end_grid_root=destination, account_body=body)
    assert Path(reference["path"]).read_bytes() == original_bytes
    assert reference["sha256"] == descriptor["sha256"] and reference["size_bytes"] == len(original_bytes)


def test_existing_changed_retained_account_is_preserved_and_refused(tmp_path):
    """Publication must preserve an unknown final rather than overwrite it."""
    import pm_historical_scheduler as scheduler
    context, body, descriptor, destination = _retained_account_atomic_case(tmp_path)
    target = destination / ("declared-account-" + descriptor["sha256"] + ".json")
    changed = b'{"INVENTED_CHANGED_FINAL":true}'
    target.write_bytes(changed)
    original = (context.root / descriptor["relative_path"]).read_bytes()
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_CONTROL_CONFLICT"):
        scheduler._declared_retain_account_result(context, descriptor, end_grid_root=destination, account_body=body)
    assert target.read_bytes() == changed
    assert (context.root / descriptor["relative_path"]).read_bytes() == original


@pytest.mark.parametrize("saved_stop", [None, "FAILED", "TERMINAL_STOP"])
def test_m5_worker_retained_account_conflict_never_becomes_durable_stop(tmp_path, monkeypatch, saved_stop):
    """Post-account CONTROL-FLOW fixture: upstream stand-ins prove no issuer/source/account origin.

    The full worker, owned context, original JSON, conflicting copy, actual atomic
    retention, exception handlers, finally block and terminal recorder are real.
    """
    from dataclasses import asdict
    import pm_historical_scheduler as scheduler
    import pm_forward_oos_replay_producer as forward
    import pm_historical_search_producer as producer
    import pm_portfolio_replay as portfolio
    import pm_production_config_binding as binding
    context, body, descriptor, destination = _retained_account_atomic_case(tmp_path)
    workspace, _ = spool.read_owned_generation_workspace(context)
    target = destination / ("declared-account-" + descriptor["sha256"] + ".json")
    target.write_bytes(b'{"INVENTED_CHANGED_FINAL":true}')
    original_bytes = (context.root / descriptor["relative_path"]).read_bytes()
    slot = {"slot_id": body["slot_summaries"][0]["slot_id"], "ordinal": 1,
        "activation_utc": "2023-06-24T00:00:00+00:00", "end_exclusive_utc": "2023-07-22T00:00:00+00:00",
        "native_cutoff_utc": "2023-06-23T23:00:00+00:00"}
    precommit = SimpleNamespace(receipt_sha256="1"*64, to_dict=lambda: {"fixture_provenance": _PREFIX_NOTICE})
    process = SimpleNamespace(receipt_sha256="2"*64)
    declared = context.root / "declared-slots" / precommit.receipt_sha256 / "precommit.json"
    declared.parent.mkdir(parents=True, exist_ok=True)
    declared.write_text("{}", encoding="ascii")
    closed = []
    owner = SimpleNamespace(view_plan_sha256="3"*64, close=lambda: closed.append(True))
    edition = SimpleNamespace(reopen_route=lambda: {"fixture_provenance": _PREFIX_NOTICE})
    config = SimpleNamespace(data_dir=str(tmp_path / "unused-market-data"))
    frozen = SimpleNamespace(materialize=lambda _: config)
    dev = {"development_slot_ids": [slot["slot_id"]], "slot_view_ids": {slot["slot_id"]: "slot-view"},
        "calibration_view_id": "calibration", "account_view_id": "account",
        "view_axes": [{"view_id": "account", "cutoff_utc": slot["end_exclusive_utc"]}],
        "m5_source_pins": {"view_plan_sha256": owner.view_plan_sha256},
        "end_grid_root": str(destination), "source_fence_path": str(destination / "refusal.json"),
        "retirement": {"receipt_path": str(destination / "retirement.json"), "max_paths": 100,
            "max_nodes": 300, "max_bytes": 4*1024**2}}
    row = {"descriptor_index": 0, "root": str(context.root), "run_id": context.generation_id,
        "cell": body["cell"], "m5_development": dev,
        "development": {"source_kind": "m5_derived", "selected_slot_ids": [slot["slot_id"]]}}
    contract = {"schema_version": scheduler.DECLARED_DEVELOPMENT_CELL_SCHEDULER_SCHEMA, "cells": [row],
        "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
        "profile_sha256": "4"*64, "active_config_path": str(tmp_path / "unused-config.json"),
        "spool_limits": asdict(context.limits), "cost_edition_route": edition.reopen_route()}
    monkeypatch.setattr(scheduler, "_declared_development_contract", lambda value: (value, {}))
    monkeypatch.setattr(scheduler, "_declared_m5_dependencies", lambda: None)
    monkeypatch.setattr(scheduler, "_declared_m5_cell_contract_sha256", lambda *_: workspace["cell_contract_sha256"])
    monkeypatch.setattr(scheduler, "_declared_reopen_process", lambda *_: process)
    monkeypatch.setattr(scheduler, "_declared_m5_check_roster", lambda *_, **__: [slot])
    monkeypatch.setattr(binding, "load_effective_pipeline_config", lambda _: ({}, config))
    monkeypatch.setattr(binding, "freeze_production_pipeline_config", lambda *_, **__: frozen)
    monkeypatch.setattr(orchestration, "reopen_declared_slot_owner_run_precommit", lambda *_, **__: precommit)
    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda _: [slot])
    monkeypatch.setattr(orchestration, "_declared_slot_effective_access", lambda *args: (
        {"start_utc": "2000-01-01T00:00:00+00:00", "end_exclusive_utc": "2100-01-01T00:00:00+00:00"}, None))
    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", lambda *_, **__: ({}, []))
    monkeypatch.setattr(orchestration, "_declared_slot_native_snapshot_status", lambda *_: "MATCH")
    monkeypatch.setattr(forward, "declared_m5_development_cell_account_request", lambda *_, **__: {"cutoff_utc": slot["end_exclusive_utc"]})
    source_header = context.root / producer._declared_m5_cell_source_path(precommit, row["cell"]["cell_id"])
    source_header.parent.mkdir(parents=True, exist_ok=True)
    source_header.write_text('{"INVENTED_SOURCE_HEADER_STAND_IN":true}', encoding="ascii")
    monkeypatch.setattr(producer, "reopen_declared_m5_cell_source_context", lambda *_, **__: {
        "owner": owner, "cost_edition": edition, "slot_view_ids": dev["slot_view_ids"], "calibration_view_id": "calibration"})
    run_body = {"terminal_stop": False, "slot_commits": [{"slot_id": slot["slot_id"], "status": "NO_TRADE"}]}
    monkeypatch.setattr(producer, "execute_declared_cell_owner_slots", lambda *_, **__: SimpleNamespace(to_dict=lambda: run_body))
    account_inputs = {"fixture_provenance": _PREFIX_NOTICE, "body": body}
    monkeypatch.setattr(forward, "_prepare_declared_m5_development_cell_account_artifact",
        lambda *_, **__: (None, account_inputs, body))
    def produce_account(*args, **kwargs):
        assert kwargs["inputs"] is account_inputs and kwargs["body"] is body
        return SimpleNamespace(descriptor=descriptor)
    monkeypatch.setattr(forward, "_produce_declared_m5_development_cell_account_from_inputs", produce_account)
    reached = []
    def final_account(*args, **kwargs):
        reached.append(True)
        # Fail-only finalization seam: the genuine retain helper must observe the conflict.
        return scheduler._declared_retain_account_result(context, kwargs["artifact"].descriptor,
            end_grid_root=destination, account_body=body)
    monkeypatch.setattr(scheduler, "_declared_m5_finalize", final_account)
    if saved_stop:
        import pm_end_grid as grid
        _workspace, owned_sha = spool.read_owned_generation_workspace(context)
        refusal_result = {"status": saved_stop, "reason_code": "INVENTED_PERSISTED_STOP"}
        code = {name: "6"*64 for name in ("pm_historical_scheduler.py", "pm_end_grid.py", "pm_authority_spool.py")}
        fence = scheduler._declared_m5_refusal_body(binding={
            "generation_context_sha256": context.context_sha256, "run_id": row["run_id"],
            "cell_id": row["cell"]["cell_id"], "cell_contract_sha256": workspace["cell_contract_sha256"],
            "search_process_receipt_sha256": process.receipt_sha256, "precommit_sha256": precommit.receipt_sha256},
            workspace_owner_sha256=owned_sha, result=refusal_result, slot=slot, cell=row["cell"],
            code_sha256=code, recorded_at_utc="2026-11-04T00:00:00+00:00", resumed=True,
            source_attempted=False, owner_created=False, owner_present=False)
        stopped = grid.build_end_grid_row(symbol=row["cell"]["symbol"], timeframe=row["cell"]["timeframe"],
            regime=row["cell"]["regime"], cycle_start_utc=slot["activation_utc"],
            cycle_end_utc=slot["end_exclusive_utc"], information_cutoff_utc=slot["native_cutoff_utc"],
            outcome="STOP", reason_code=refusal_result["reason_code"], source_kind="m5_derived", cost_tier="NONE",
            source_contract_sha256=None, view_key=None, source_identity={},
            process_edition_sha256=process.receipt_sha256, code_sha256=code,
            produced_at_utc="2026-11-04T00:00:00+00:00")
        written = grid.EndGridWriter(destination).append(stopped)
        reference = {key: written[key] for key in ("path", "row_sha256")}
        fence["end_grid_rows"] = {slot["slot_id"]: reference}
        fence["period_facts"] = [{"ordinal": 1, "slot_id": slot["slot_id"], "status": "STOPPED",
            "completion_status": "STOP", "reason_code": refusal_result["reason_code"],
            "commit_sha256": None, "end_grid_reference": reference}]
        scheduler._declared_m5_external_record(Path(dev["source_fence_path"]), fence, maximum_bytes=4*1024**2)
        def forbidden(*args, **kwargs):
            pytest.fail("saved terminal STOP reached source/account/controller work")
        monkeypatch.setattr(scheduler, "_declared_m5_new_owner", forbidden)
        monkeypatch.setattr(producer, "reopen_declared_m5_cell_source_context", forbidden)
        monkeypatch.setattr(producer, "execute_declared_cell_owner_slots", forbidden)
        monkeypatch.setattr(forward, "declared_m5_development_cell_account_request", forbidden)
        monkeypatch.setattr(forward, "_prepare_declared_m5_development_cell_account_artifact", forbidden)
        monkeypatch.setattr(forward, "_produce_declared_m5_development_cell_account_from_inputs", forbidden)
        from copy import deepcopy
        import os
        import pm_memory_governor as governor
        following = deepcopy(row)
        following.update(descriptor_index=1, root=str(tmp_path/"unconsumed-following-cell"))
        following["cell"] = {**row["cell"], "regime": "RANGE",
            "cell_id": book.compute_cell_id(row["cell"]["symbol"], "H1", "RANGE")}
        contract["cells"] = [row, following]
        monkeypatch.setattr(scheduler, "_declared_development_contract",
            lambda value: (value, {row["cell"]["symbol"]: value["cells"]}))
        actual_worker = scheduler._declared_m5_run_one_cell
        visited = []
        def run_cell(declaration, cell):
            visited.append(cell["descriptor_index"])
            if cell["descriptor_index"] == 0:
                return actual_worker(declaration, cell)
            return scheduler._declared_cell_row(cell, "NOT_RUN", "INVENTED_FOLLOWING_CELL", pid=os.getpid())
        monkeypatch.setattr(scheduler, "_declared_m5_run_one_cell", run_cell)
        def inline(self, worker, tasks, **kwargs):
            for task in tasks:
                yield governor.TaskOutcome(task.key, "COMPLETED", worker(*task.args), worker_pid=os.getpid())
        monkeypatch.setattr(governor.MemoryGovernor, "iter_completed", inline)
        if saved_stop == "TERMINAL_STOP":
            report = scheduler.run_declared_symbol_scheduler(scheduler.declared_symbol_worker,
                contract=contract, requested_workers=1, telemetry_path=tmp_path/"recorded-stop-telemetry.jsonl")
            result = report["cells"][0]
            assert report["errors"] == [] and visited == [0, 1]
            assert result["status"] == "TERMINAL_STOP" and result["reason_code"] == "RECORDED_TERMINAL_STOP"
            assert report["cells"][1]["reason_code"] == "INVENTED_FOLLOWING_CELL"
        else:
            with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
                scheduler.run_declared_symbol_scheduler(scheduler.declared_symbol_worker,
                    contract=contract, requested_workers=1, telemetry_path=tmp_path/"failed-stop-telemetry.jsonl")
            report = caught.value.report
            result = report["cells"][0]
            assert result["status"] == "FAILED" and result["reason_code"] == "INVENTED_PERSISTED_STOP"
            assert visited == [0] and any(value.startswith("CELL_FAILURE:") for value in report["errors"])
        assert result["recorded_before_dispatch"] is True
        assert result["terminal_refusal"]["reason_code"] == "INVENTED_PERSISTED_STOP"
        assert result["period_facts"] == fence["period_facts"]
        assert reached == [] and closed == []
        assert [item.row_sha256 for item in grid.read_end_grid(destination, row["cell"]["symbol"])] == [stopped.row_sha256]
        return
    result = scheduler._declared_m5_run_one_cell(contract, row)
    assert reached == [True] and closed == [True]
    assert result["reason_code"] == "RETIREMENT_CONTROL_CONFLICT"
    assert result["run_receipt"] is not None and result["account_descriptor"] == descriptor
    assert "terminal_refusal_end_grid" not in result and "terminal_refusal_record" not in result
    assert not (destination / "XAGUSD.jsonl").exists() and not Path(dev["source_fence_path"]).exists()
    assert not Path(dev["retirement"]["receipt_path"]).exists() and not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    assert (context.root / descriptor["relative_path"]).read_bytes() == original_bytes
    completion = scheduler.build_declared_development_completion(contract, {"cells": [result]}).to_dict()["cells"][0]
    assert completion["status"] == "NOT_FINALIZED" and completion["reason_code"] == "RETIREMENT_CONTROL_CONFLICT"


@pytest.mark.parametrize("marker", ["RETIREMENT_RELATIVE_PATH", "RETIREMENT_INTENT_RELATIVE_PATH"])
def test_period_capture_does_not_read_unlinked_commits_after_retirement(tmp_path, monkeypatch, marker):
    import pm_historical_scheduler as scheduler
    import pm_historical_search_producer as producer
    root = tmp_path.resolve()
    (root / getattr(spool, marker)).write_text("{}", encoding="ascii")
    old_facts = [{"ordinal": 1, "slot_id": "slot", "status": "COMMITTED", "completion_status": "NO_TRADE",
        "reason_code": None, "commit_sha256": "1"*64, "end_grid_reference": None}]
    result = {"status": "NOT_RUN", "reason_code": "RETIREMENT_INCOMPLETE", "period_facts": old_facts}
    monkeypatch.setattr(producer, "_read_declared_commit", lambda *a, **k: pytest.fail("commit was already released"))
    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda _: [{"ordinal": 1, "slot_id": "slot"}])
    scheduler._declared_capture_period_facts(result, SimpleNamespace(root=root), SimpleNamespace(to_dict=lambda: {}),
        {"development": {}, "cell": {"cell_id": "2"*64}})
    assert result["period_facts"] == old_facts and "period_fact_error" not in result


def test_one_cells_period_error_keeps_other_cells_completion_report(prefix_metadata_factory):
    import pm_historical_scheduler as scheduler
    first, second = prefix_metadata_factory(symbol="EURUSD"), prefix_metadata_factory(symbol="USDJPY")
    contract = _prefix_native_scheduler_contract(first, 1)
    contract["cells"].extend(_prefix_native_scheduler_contract(second, 1)["cells"])
    checked, _ = scheduler._declared_development_contract(contract)
    rows = [scheduler._declared_cell_row(cell, "NOT_RUN", "INVENTED_DEFERRED") for cell in checked["cells"]]
    rows[0]["period_fact_error"] = "INVENTED_CORRUPT_COMMIT"
    rows[1]["period_facts"] = [{"ordinal": 1, "slot_id": checked["cells"][1]["development"]["selected_slot_ids"][0],
        "status": "NOT_RUN", "reason_code": None, "commit_sha256": None, "completion_status": None, "end_grid_reference": None}]
    body = scheduler.build_declared_development_completion(checked, {"cells": rows}).to_dict()
    assert len(body["cells"]) == 2 and body["status"] == "INCOMPLETE"
    assert body["cells"][0]["period_fact_error"] == "INVENTED_CORRUPT_COMMIT"
    assert body["cells"][0]["periods"][0]["status"] == "NOT_OBSERVED"
    assert body["cells"][1]["periods"][0]["status"] == "NOT_RUN"


@pytest.mark.parametrize("open_window", [True, False])
def test_clean_missing_m5_header_reacquires_only_inside_existing_window(prefix_metadata_factory, open_window):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    precommit = _prefix_native_issue(env, [env.roster["slots"][0]["slot_id"]])
    before = (env.context.root / ("declared-slots/" + precommit.receipt_sha256 + "/precommit.json")).read_bytes()
    assert scheduler._declared_m5_source_resume(env.context, precommit, env.cell["cell_id"], open_window) is (False if open_window else None)
    assert (env.context.root / ("declared-slots/" + precommit.receipt_sha256 + "/precommit.json")).read_bytes() == before


@pytest.mark.parametrize("name", ["attempts", "commits", "run.lock"])
def test_missing_m5_header_with_recorded_progress_is_not_clean_reacquisition(prefix_metadata_factory, name):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    precommit = _prefix_native_issue(env, [env.roster["slots"][0]["slot_id"]])
    path = env.context.root / "declared-slots" / precommit.receipt_sha256 / env.cell["cell_id"] / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("INVENTED_OBSERVED_PROGRESS", encoding="ascii")
    with pytest.raises(scheduler.HistoricalSchedulerError, match="SOURCE_HEADER_MISSING_WITH_PROGRESS"):
        scheduler._declared_m5_source_resume(env.context, precommit, env.cell["cell_id"], True)
    assert path.read_text() == "INVENTED_OBSERVED_PROGRESS"


def test_first_precommit_preparation_reopens_same_sealed_process(prefix_metadata_factory, monkeypatch):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    monkeypatch.setattr(planner, "freeze_route_a_full_panel_production_search_process",
        lambda *a, **k: pytest.fail("must reuse the already sealed process"))
    process = scheduler._declared_initial_process(env.context, {"run_id": env.context.generation_id}, development=True)
    assert process.receipt_sha256 == env.process.receipt_sha256
    assert process.spool_receipt == env.process.spool_receipt


def test_unknown_precommit_namespace_is_not_first_issuance(prefix_metadata_factory):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    unexpected = env.context.root / "declared-slots" / "unknown-source-header.json"
    unexpected.parent.mkdir()
    unexpected.write_text("INVENTED_UNKNOWN_HISTORY", encoding="ascii")
    with pytest.raises(scheduler.HistoricalSchedulerError, match="PRECOMMIT_MISSING_WITH_PROGRESS"):
        scheduler._declared_initial_process(env.context, {"run_id": env.context.generation_id}, development=True)
    assert unexpected.read_text() == "INVENTED_UNKNOWN_HISTORY"


def test_first_precommit_allows_only_recursively_empty_declared_directories(prefix_metadata_factory):
    import pm_historical_scheduler as scheduler
    env = prefix_metadata_factory()
    residue = env.context.root/"declared-slots"/("a"*64)/"empty-child"
    residue.mkdir(parents=True)
    process = scheduler._declared_initial_process(env.context, {"run_id": env.context.generation_id}, development=True)
    assert process.receipt_sha256 == env.process.receipt_sha256 and residue.is_dir()
    foreign = residue/"foreign.json"
    foreign.write_text("INVENTED_FOREIGN_PAYLOAD", encoding="ascii")
    with pytest.raises(scheduler.HistoricalSchedulerError, match="PRECOMMIT_MISSING_WITH_PROGRESS"):
        scheduler._declared_initial_process(env.context, {"run_id": env.context.generation_id}, development=True)
    assert foreign.read_text() == "INVENTED_FOREIGN_PAYLOAD"


@pytest.mark.parametrize("invalid", [False, True])
def test_refusal_period_validation_precedes_any_grid_stop(tmp_path, monkeypatch, invalid):
    from test_terminal_refusal_retirement import TerminalRefusalRetirementTests
    import pm_historical_scheduler as scheduler
    import pm_end_grid as grid
    harness = TerminalRefusalRetirementTests(methodName="runTest")
    harness.parent = tmp_path
    context, _payload, args, fence = harness.fixture()
    workspace, workspace_sha = spool.read_owned_generation_workspace(context)
    refusal = fence["refusal"]
    slot = {"ordinal": 1, "slot_id": refusal["slot_id"], "activation_utc": refusal["cycle_start_utc"],
        "end_exclusive_utc": refusal["cycle_end_utc"], "native_cutoff_utc": refusal["information_cutoff_utc"]}
    cell = {name: refusal[name] for name in ("symbol", "timeframe", "regime")} | {"cell_id": fence["binding"]["cell_id"]}
    row = {"descriptor_index": 0, "root": str(context.root), "run_id": context.generation_id, "cell": cell,
        "m5_development": {"end_grid_root": str(Path(next(iter(fence["end_grid_rows"].values()))["path"]).parent),
            "source_fence_path": str(tmp_path/"new-refusal.json")}}
    result = scheduler._declared_cell_row(row, "FAILED", "INVENTED_REFUSAL")
    if invalid:
        result["period_facts"] = [{"INVENTED_INVALID_FACT": True}]
    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda _: [slot])
    monkeypatch.setattr(scheduler, "_declared_m5_reuse_recorded_rows", lambda *a, **k: False)
    monkeypatch.setattr(grid.EndGridWriter, "append", lambda *a, **k: pytest.fail("STOP published before facts validation"))
    with pytest.raises(scheduler.HistoricalSchedulerError, match="PERIOD_EVIDENCE"):
        scheduler._declared_m5_record_terminal_refusal(result, context=context, workspace_sha=workspace_sha,
            precommit=SimpleNamespace(receipt_sha256=fence["binding"]["precommit_sha256"], to_dict=lambda: {}),
            process=SimpleNamespace(receipt_sha256=fence["binding"]["search_process_receipt_sha256"]),
            frozen=object(), config=object(), contract={}, row=row,
            cell_contract_sha256=workspace["cell_contract_sha256"], resumed=False, source_attempted=True,
            owner_created=True, owner=object(), phase="SOURCE_HEADER", run_body=None)
    assert not Path(row["m5_development"]["source_fence_path"]).exists()


@pytest.mark.parametrize("changed", [False, True])
def test_legacy_grid_contract_reentry_keeps_exact_old_bytes(tmp_path, changed):
    import pm_historical_scheduler as scheduler
    import pm_end_grid as grid
    legacy = {"classification": _PREFIX_NOTICE, "result": {"account_sha": "1"*64}}
    digest = grid.store_view_contract(tmp_path, legacy)
    prior = SimpleNamespace(view_contract_sha256=digest, metrics={})
    current = {**legacy, "retained_account_result": {"path": str((tmp_path/"account.json").resolve()),
        "sha256": "1"*64, "size_bytes": 20}}
    if changed:
        current["result"] = {"account_sha": "2"*64}
        with pytest.raises(scheduler.HistoricalSchedulerError, match="GRID_CONFLICT"):
            scheduler._declared_retained_grid_contract(tmp_path, current, [prior])
    else:
        assert scheduler._declared_retained_grid_contract(tmp_path, current, [prior]) == legacy
    assert grid.load_view_contract(tmp_path, digest) == legacy


def test_held_bootstrap_lock_is_busy_without_m5_source_or_refusal(tmp_path, monkeypatch):
    import pm_historical_scheduler as scheduler
    contract, _ = scheduler._declared_development_contract(_prefix_multicell_m5_contract(tmp_path))
    monkeypatch.setattr(scheduler, "_declared_m5_dependencies", lambda: None)
    def busy(*args, **kwargs):
        raise spool.AuthoritySpoolError("GENERATION_BOOTSTRAP_BUSY")
    monkeypatch.setattr(spool, "freeze_generation_context", busy)
    row = scheduler._declared_m5_run_one_cell(contract, contract["cells"][0])
    assert row["status"] == "NOT_RUN" and row["reason_code"] == "BUSY"
    assert "terminal_refusal_record" not in row and "source_abort" not in row
    assert row["run_receipt"] is None and row["account_descriptor"] is None


def _m5_full_worker_metadata_case(tmp_path, prefix_metadata_factory):
    """Genuine issuers over INVENTED metadata; no source, cost or financial proof."""
    from dataclasses import asdict
    import pm_historical_scheduler as scheduler
    import pm_m5_job_view as views
    env = prefix_metadata_factory(symbol="XAGUSD")
    contract = _prefix_multicell_m5_contract(tmp_path)
    contract["cells"] = [contract["cells"][0]]
    contract.update(native_universe_sha256=env.context.native_universe_sha256,
        config_sha256=env.context.config_sha256, active_config_path=str(env.frozen.active_config_path))
    row = contract["cells"][0]
    row.update(cell=dict(env.cell), launch_activation_utc=env.kwargs["launch_activation_utc"],
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        protected_end_exclusive_utc="2024-07-14T08:05:00Z", extend_frozen_clock_backward=True)
    slot = env.roster["slots"][0]
    selected = [slot["slot_id"]]
    row["development"]["selected_slot_ids"] = selected
    dev = row["m5_development"]
    dev.update(development_slot_ids=selected, slot_view_ids={slot["slot_id"]: "slot-view"},
        required_result_keys=selected + [dev["terminal_result_key"]])
    for axis in dev["view_axes"]:
        cutoff = slot["end_exclusive_utc"] if axis["view_id"] == dev["account_view_id"] else slot["native_cutoff_utc"]
        last = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
        axis.update(cutoff_utc=cutoff, interval={"start_utc": (last-timedelta(hours=1)).isoformat(),
            "end_exclusive_utc": last.isoformat()})
    calibration = next(axis for axis in dev["view_axes"] if axis["view_id"] == dev["calibration_view_id"])
    dev["cost_group_metadata"]["available_interval"] = dict(calibration["interval"])
    dev["m5_source_pins"]["view_plan_sha256"] = views.declared_view_plan_sha256(tuple(dev["view_axes"]))
    Path(dev["end_grid_root"]).mkdir()
    contract, _ = scheduler._declared_development_contract(contract)
    row = contract["cells"][0]
    digest = scheduler._declared_m5_cell_contract_sha256(contract, row)
    context = spool.freeze_generation_context(Path(row["root"]), generation_id=row["run_id"],
        native_universe_sha256=contract["native_universe_sha256"], config_sha256=contract["config_sha256"],
        limits=spool.SpoolLimits(**contract["spool_limits"]),
        owned_workspace={"cell_id": row["cell"]["cell_id"], "cell_contract_sha256": digest})
    payload, supplied = config_binding.load_effective_pipeline_config(Path(contract["active_config_path"]))
    frozen = config_binding.freeze_production_pipeline_config(context,
        active_config_path=Path(contract["active_config_path"]), config_payload=payload, supplied_config=supplied)
    process = planner.freeze_route_a_full_panel_production_search_process(context, run_id=row["run_id"])
    assert asdict(context.limits) == contract["spool_limits"]
    assert not list((context.root/"declared-slots").glob("*/precommit.json"))
    return SimpleNamespace(contract=contract, row=row, context=context, frozen=frozen, process=process)


@pytest.mark.parametrize("negative", ["expired", "dirty-progress"])
def test_full_m5_worker_issues_first_precommit_and_reacquires_only_its_original_window(
        tmp_path, prefix_metadata_factory, monkeypatch, negative):
    """Real worker/config/process/precommit; only source boundaries are controlled."""
    import errno
    import pm_historical_scheduler as scheduler
    import pm_historical_search_producer as producer
    import pm_m5_derived_costs as costs
    import pm_portfolio_replay as portfolio
    case = _m5_full_worker_metadata_case(tmp_path, prefix_metadata_factory)
    called = []
    # These are labelled source-boundary stand-ins, not Native/COST_READY evidence.
    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", lambda *a, **k: ({}, []))
    monkeypatch.setattr(orchestration, "_declared_slot_native_snapshot_status", lambda *a: "MATCH")
    def capacity(route):
        assert route == case.contract["cost_edition_route"]
        called.append(route)
        if len(called) == 1:
            raise OSError(errno.ENOSPC, "INVENTED_SOURCE_BOUNDARY_DISK_FULL")
        raise MemoryError("INVENTED_SOURCE_BOUNDARY_ALLOCATION_FAILURE")
    monkeypatch.setattr(costs, "open_recorded_derived_costs_route", capacity)
    monkeypatch.setattr(planner, "freeze_route_a_full_panel_production_search_process",
        lambda *a, **k: pytest.fail("must reopen the existing sealed process"))
    original_context = (case.context.root/spool.CONTEXT_RELATIVE_PATH).read_bytes()
    first = scheduler._declared_m5_run_one_cell(case.contract, case.row)
    assert (first["status"], first["reason_code"]) == ("NOT_RUN", "DISK_LOW")
    path, = (case.context.root/"declared-slots").glob("*/precommit.json")
    original = path.read_bytes()
    body = json.loads(original)
    assert body["generation_context_sha256"] == case.context.context_sha256
    assert body["search_process_receipt_sha256"] == case.process.receipt_sha256
    assert body["source_kind"] == "m5_derived"
    precommit = orchestration.reopen_declared_slot_owner_run_precommit(case.context,
        receipt_sha256=path.parent.name, process=case.process, frozen_pipeline_config=case.frozen)
    header = case.context.root/producer._declared_m5_cell_source_path(precommit, case.row["cell"]["cell_id"])
    assert not header.exists()
    second = scheduler._declared_m5_run_one_cell(case.contract, case.row)
    assert (second["status"], second["reason_code"]) == ("NOT_RUN", "MEMORY_ERROR")
    assert len(called) == 2 and path.read_bytes() == original and not header.exists()
    assert (case.context.root/spool.CONTEXT_RELATIVE_PATH).read_bytes() == original_context
    if negative == "expired":
        end = datetime.fromisoformat(body["source_access_window_utc"]["end_exclusive_utc"].replace("Z", "+00:00"))
        monkeypatch.setattr(scheduler, "_declared_utc_now", lambda: end)
        expected = ("NOT_RUN", "WINDOW_EXPIRED")
    else:
        foreign = path.parent/case.row["cell"]["cell_id"]/"attempts"/"unknown-progress.json"
        foreign.parent.mkdir(parents=True)
        foreign.write_text("INVENTED_FOREIGN_PROGRESS_PRESERVE", encoding="ascii")
        expected = ("FAILED", "DECLARED_M5_SOURCE_HEADER_MISSING_WITH_PROGRESS")
    third = scheduler._declared_m5_run_one_cell(case.contract, case.row)
    assert (third["status"], third["reason_code"]) == expected
    assert len(called) == 2 and path.read_bytes() == original and not header.exists()
    assert "terminal_refusal_end_grid" not in third
    if negative == "dirty-progress":
        assert foreign.read_text() == "INVENTED_FOREIGN_PROGRESS_PRESERVE"


@pytest.mark.parametrize("source_kind", ["native", "m5"])
def test_actual_bootstrap_holder_precedes_visible_owner_empty_root_refusal(
        tmp_path, prefix_metadata_factory, monkeypatch, source_kind):
    """Real kernel contention and worker admission, no context/source/model doubles."""
    from copy import deepcopy
    from dataclasses import asdict
    import pm_historical_scheduler as scheduler
    if source_kind == "native":
        env = prefix_metadata_factory(symbol="XAGUSD")
        contract = _prefix_native_scheduler_contract(env, 1)
        root = (tmp_path/"native-contended-root").resolve()
        contract["cells"][0]["root"] = str(root)
        policy = contract["cells"][0]["native_finalization"]
        Path(policy["end_grid_root"]).mkdir(parents=True)
        contract, _ = scheduler._declared_development_contract(contract)
        worker = scheduler._declared_run_one_cell
        foreign_code = "DECLARED_NATIVE_FINALIZATION_FRESH_PATH_REQUIRED"
    else:
        case = _m5_full_worker_metadata_case(tmp_path, prefix_metadata_factory)
        contract = deepcopy(case.contract)
        root = (tmp_path/"m5-contended-root").resolve()
        contract["cells"][0]["root"] = str(root)
        contract, _ = scheduler._declared_development_contract(contract)
        worker = scheduler._declared_m5_run_one_cell
        foreign_code = "DECLARED_M5_FRESH_ROOT_NOT_EMPTY"
    row = contract["cells"][0]
    limits = (scheduler._declared_native_spool_limits(row) if source_kind == "native"
        else spool.SpoolLimits(**contract["spool_limits"]))
    cell_contract_sha = (scheduler._declared_native_cell_contract_sha256(contract, row, limits)
        if source_kind == "native" else scheduler._declared_m5_cell_contract_sha256(contract, row))
    context_payload = {"schema_version": spool.GENERATION_CONTEXT_SCHEMA, "generation_id": row["run_id"],
        "native_universe_sha256": contract["native_universe_sha256"],
        "config_sha256": contract["config_sha256"], "limits": asdict(limits)}
    context_sha = hashlib.sha256(spool._canonical_json_bytes(context_payload, spool.MAX_CONTEXT_BYTES)).hexdigest()
    root.mkdir()
    info = root.stat(follow_symlinks=False)
    root_identity = [info.st_dev, info.st_ino]
    owner_payload = {"schema_version": spool.WORKSPACE_OWNER_SCHEMA, "generation_id": row["run_id"],
        "generation_context_sha256": context_sha, "creation_provenance": "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION",
        "cell_id": row["cell"]["cell_id"], "cell_contract_sha256": cell_contract_sha,
        "root": str(root), "root_identity": root_identity}
    owner_bytes = spool._canonical_json_bytes(owner_payload, spool.MAX_CONTEXT_BYTES)
    with spool._generation_bootstrap_lock(root):
        (root/"tmp").mkdir()
        owner = root/spool.WORKSPACE_OWNER_RELATIVE_PATH
        spool._retirement_publish_v2(root, spool.WORKSPACE_OWNER_RELATIVE_PATH,
            owner_payload, spool.MAX_CONTEXT_BYTES, root_owner={"root": str(root), "root_identity": root_identity})
        assert owner.read_bytes() == owner_bytes
        observed = worker(contract, row)
        assert (observed["status"], observed["reason_code"]) == ("NOT_RUN", "BUSY")
        assert not (root/spool.CONTEXT_RELATIVE_PATH).exists()
        assert owner.read_bytes() == owner_bytes
    # Once unlocked, the same unknown residue is foreign, never inferred owned.
    refused = worker(contract, row)
    assert (refused["status"], refused["reason_code"]) == ("FAILED", foreign_code)
    assert owner.read_bytes() == owner_bytes
    assert (root/"tmp").is_dir() and not (root/spool.CONTEXT_RELATIVE_PATH).exists()


def test_full_native_refinalization_preserves_legacy_grid_view_and_fence_bytes(tmp_path, monkeypatch):
    """Existing labelled source-proof seams; real finalizer, copy, Grid and retirement."""
    from test_native_regime_unobserved_finalization_001 import _invented_completed_cell
    import pm_historical_scheduler as scheduler
    import pm_end_grid as grid
    (context, row, contract, config, precommit, process, artifact, run, owner_sha, contract_sha) = (
        _invented_completed_cell(tmp_path, monkeypatch, None))
    parent, slots, proofs, summaries, saved_run, _no_plan = scheduler._declared_native_completed_records(
        context, row=row, precommit=precommit, process=process, artifact=artifact, run_body={},
        profile_sha=contract["profile_sha256"])
    grid_root = Path(row["native_finalization"]["end_grid_root"])
    original_account = (context.root/artifact.descriptor["relative_path"]).read_bytes()
    planned, selected = [], []
    for slot, summary in zip(slots, summaries, strict=True):
        view, evidence, session_sha, index_sha, accessed, selection = proofs[slot["slot_id"]]
        start, end, cutoff = (scheduler._declared_m5_grid_utc(slot[name]) for name in (
            "activation_utc", "end_exclusive_utc", "native_cutoff_utc"))
        legacy_view = {"schema_version": "fxpm.broker-native-end-grid-reconstruction.v1", "view": view,
            "result": {"precommit_sha256": precommit.receipt_sha256, "cell_id": row["cell"]["cell_id"],
                "slot_id": slot["slot_id"], "ordinal": slot["ordinal"], "cycle_start_utc": start,
                "cycle_end_utc": end, "completion_status": summary["status"], "reason_code": selection["reason_code"],
                "request_sha256": summary["request_sha256"], "slot_commit_sha256": summary["commit_sha256"],
                "source_session_sha256": session_sha, "native_prefix_index_sha256": index_sha,
                "source_accessed_at_utc": accessed, "saved_cell_run_receipt_sha256": run["sha256"]},
            "cost_tier_evidence": evidence}
        digest = grid.store_view_contract(grid_root, legacy_view)
        record = grid.build_end_grid_row(symbol=row["cell"]["symbol"], timeframe=row["cell"]["timeframe"],
            regime=row["cell"]["regime"], cycle_start_utc=start, cycle_end_utc=end,
            information_cutoff_utc=cutoff, source_kind="broker_native", view_key=_prefix_sha(view),
            view_contract_sha256=digest, source_identity={name: view[name] for name in (
                "activation_id", "activation_manifest_sha256", "activation_entry_sha256", "native_bars_sha256")},
            no_trade_run_receipt_sha256=run["sha256"] if selection["outcome"] == "NO_TRADE" else None,
            process_edition_sha256=process.receipt_sha256, code_sha256={"INVENTED_LEGACY_CODE_DATA": "9"*64},
            metrics={}, produced_at_utc=end, **selection)
        planned.append(record)
        selected.append({"slot_id": slot["slot_id"], "cycle_start_utc": start, "cycle_end_utc": end,
            "information_cutoff_utc": cutoff, "outcome": selection["outcome"]})
    receipts = grid.EndGridWriter(grid_root).append_many_same_symbol(planned)
    legacy_fence = {"schema_version": spool.NATIVE_COMPLETION_FENCE_SCHEMA, "workspace_owner_sha256": owner_sha,
        "binding": {"generation_context_sha256": context.context_sha256, "run_id": row["run_id"],
            "cell_id": row["cell"]["cell_id"], "cell_contract_sha256": contract_sha,
            "search_process_receipt_sha256": process.receipt_sha256, "precommit_sha256": precommit.receipt_sha256},
        "completion": {"cell_run_receipt_sha256": run["sha256"], "account_artifact_sha256": artifact.descriptor["sha256"]},
        "selected_slots": selected, "end_grid_rows": {slot["slot_id"]: {key: receipt[key] for key in ("path", "row_sha256")}
            for slot, receipt in zip(slots, receipts, strict=True)},
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    fence_path = Path(row["native_finalization"]["source_fence_path"])
    saved = scheduler._declared_m5_external_record(fence_path, legacy_fence, maximum_bytes=4*1024**2)
    symbol_path = Path(receipts[0]["path"])
    original_grid, original_fence = symbol_path.read_bytes(), fence_path.read_bytes()
    original_views = {file.name: file.read_bytes() for file in (grid_root/grid.CONTRACTS_DIR).iterdir()}
    result = scheduler._declared_native_finalize_account(
        {"status": "REOPENED", "run_receipt": run, "account_descriptor": artifact.descriptor},
        context=context, contract=contract, row=row, config=config, precommit=precommit, process=process,
        artifact=artifact, run_body={}, workspace_sha=owner_sha, cell_contract_sha256=contract_sha)
    assert result["status"] == "ARCHIVED", result
    assert symbol_path.read_bytes() == original_grid and fence_path.read_bytes() == original_fence
    assert {file.name: file.read_bytes() for file in (grid_root/grid.CONTRACTS_DIR).iterdir()} == original_views
    assert "retained_account_result" not in json.loads(original_fence)
    kept = result["retained_account_result"]
    assert Path(kept["path"]).read_bytes() == original_account
    assert kept["sha256"] == artifact.descriptor["sha256"] and kept["size_bytes"] == len(original_account)
    reopened = spool.read_generation_retirement(context.root)
    assert reopened["retained_account_result"] == kept
    assert reopened["source_fence"]["sha256"] == saved["sha256"]
