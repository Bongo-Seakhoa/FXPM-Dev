"""INVENTED config metadata; real compiler/roster and launcher, no market or model run."""
import copy
from datetime import datetime
import hashlib
import json

import pytest

import pm_historical_anchor_orchestration as orchestration
import pm_historical_scheduler as scheduler
import pm_main as main
import pm_production_anchor_plan as anchors
import pm_production_config_binding as binding
import pm_winner_selection as selection


def _template(tmp_path, monkeypatch, *, symbol="EURUSD"):
    refs = {"config_sha256": "a" * 64, "effective_config_sha256": "b" * 64}
    calls = []

    def resolve(path):
        calls.append(path)
        return {}, object(), dict(refs)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Compilation must not issue a process or source window")

    monkeypatch.setattr(binding, "resolve_effective_pipeline_config_identity", resolve)
    monkeypatch.setattr(scheduler, "_declared_initial_process", forbidden)
    monkeypatch.setattr(scheduler, "_declared_utc_now", forbidden)
    launch = "2026-03-28T00:00:00Z"
    row = {"root": str(tmp_path / "unused-workspace"), "run_id": "INVENTED-FRESH",
        "cell": {"cell_id": selection.compute_cell_id(symbol, "H1", "TREND"),
            "symbol": symbol, "timeframe": "H1", "regime": "TREND"},
        "launch_activation_utc": launch,
        "frozen_refit_clock": {"anchor_activation_utc": anchors._DECLARED_REFIT_CLOCK_ANCHOR_UTC,
            "interval_calendar_days": anchors.DECLARED_OWNER_SLOT_CALENDAR_DAYS},
        "protected_end_exclusive_utc": "2040-01-01T00:00:00Z",
        "extend_frozen_clock_backward": datetime.fromisoformat(launch.replace("Z", "+00:00"))
            < datetime.fromisoformat(anchors._DECLARED_REFIT_CLOCK_ANCHOR_UTC.replace("Z", "+00:00")),
        "source_access_window_policy": {"lead_seconds": 0, "validity_seconds": 900}}
    contract = {"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA,
        "native_universe_sha256": "c" * 64, "config_sha256": refs["config_sha256"],
        "profile_sha256": "d" * 64, "active_config_path": str(tmp_path / "invented-config.json"),
        "cells": [row]}
    roster = anchors.derive_declared_owner_slot_roster_v2(
        launch_activation_utc=row["launch_activation_utc"], frozen_refit_clock=row["frozen_refit_clock"],
        config_identity_refs=refs, extend_frozen_clock_backward=row["extend_frozen_clock_backward"],
        protected_end_exclusive_utc=row["protected_end_exclusive_utc"])
    return contract, roster, calls


@pytest.mark.parametrize("mode,count", ((None, 1), ("handover", 2), ("final_study_13", 13)))
def test_fresh_compiler_uses_shared_accepted_roster_selection(tmp_path, monkeypatch, mode, count):
    contract, roster, calls = _template(tmp_path, monkeypatch)
    if mode is not None:
        contract["cells"][0]["period_selection"] = {"mode": mode}
    if mode == "final_study_13":
        contract["cells"][0]["final_study_13"] = True
    before = copy.deepcopy(contract)
    compiled = scheduler.compile_fresh_declared_contract(contract)
    row = compiled["cells"][0]
    assert contract == before and len(calls) == 1
    assert row["period_selection"]["selected_slot_ids"] == [slot["slot_id"] for slot in roster["slots"][:count]]
    assert row["slot_roster_sha256"] == hashlib.sha256(orchestration._declared_json(roster)).hexdigest()
    assert row["source_access_window_policy"] == before["cells"][0]["source_access_window_policy"]
    assert compiled["schema_version"] == scheduler.DECLARED_SCHEDULER_SCHEMA
    assert "development" not in row and not (tmp_path / "unused-workspace").exists()


@pytest.mark.parametrize("mode", (None, "handover"))
def test_fresh_prefix_refuses_heldout_symbols(tmp_path, monkeypatch, mode):
    contract, _, _ = _template(tmp_path, monkeypatch, symbol="AUDUSD")
    if mode is not None:
        contract["cells"][0]["period_selection"] = {"mode": mode}
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as refused:
        scheduler.compile_fresh_declared_contract(contract)
    assert str(refused.value) == "DECLARED_FULL_ROSTER_FINAL_STUDY_REQUEST_REQUIRED"


def test_explicit_final_study_keeps_heldout_roster(tmp_path, monkeypatch):
    contract, roster, _ = _template(tmp_path, monkeypatch, symbol="AUDUSD")
    contract["cells"][0]["final_study_13"] = True
    compiled = scheduler.compile_fresh_declared_contract(contract)
    assert compiled["cells"][0]["period_selection"]["selected_slot_ids"] == [slot["slot_id"] for slot in roster["slots"]]


def test_fresh_compiler_refuses_saved_workspace(tmp_path, monkeypatch):
    contract, _, _ = _template(tmp_path, monkeypatch)
    root = tmp_path / "unused-workspace"
    root.mkdir()
    saved = root / "generation-context.json"
    saved.write_text("INVENTED saved-workspace marker", encoding="utf-8")
    with pytest.raises(scheduler.HistoricalSchedulerError) as refused:
        scheduler.compile_fresh_declared_contract(contract)
    assert refused.value.code == "DECLARED_FRESH_CONTRACT_SAVED_WORKSPACE_REFUSED"
    assert saved.read_text(encoding="utf-8") == "INVENTED saved-workspace marker"


@pytest.mark.parametrize("damage", ("ids", "digest"))
def test_fresh_compiler_does_not_replace_crossed_bindings(tmp_path, monkeypatch, damage):
    contract, _, _ = _template(tmp_path, monkeypatch)
    if damage == "ids":
        contract["cells"][0]["period_selection"] = {"mode": "one_period", "selected_slot_ids": ["INVENTED-CROSSED-ID"]}
    else:
        contract["cells"][0]["slot_roster_sha256"] = "0" * 64
    before = copy.deepcopy(contract)
    with pytest.raises(scheduler.HistoricalSchedulerError) as refused:
        scheduler.compile_fresh_declared_contract(contract)
    assert refused.value.code == "DECLARED_ORDINARY_ROSTER_BINDING_CHANGED"
    assert contract == before


def test_fresh_m5_compilation_keeps_neutral_source_metadata(tmp_path, monkeypatch):
    contract, roster, _ = _template(tmp_path, monkeypatch)
    contract["cells"][0]["m5_source"] = {"fixture_provenance": "INVENTED METADATA VALIDATOR STAND-IN"}
    observed = []
    monkeypatch.setattr(scheduler, "_declared_ordinary_m5_metadata", lambda owned, row: observed.append(copy.deepcopy(row)))
    compiled = scheduler.compile_fresh_declared_contract(contract)
    row = compiled["cells"][0]
    assert row["m5_source"]["selected_slot_ids"] == [roster["slots"][0]["slot_id"]]
    assert len(observed) == 1 and "m5_development" not in row
    assert compiled["schema_version"] == scheduler.DECLARED_SCHEDULER_SCHEMA


def _neutral_m5_metadata_fixture(tmp_path):
    # The inherited fixture labels its routing/admission records as invented.
    from test_development_cell_prefix import _prefix_multicell_m5_contract

    contract = _prefix_multicell_m5_contract(tmp_path)
    contract["schema_version"] = scheduler.DECLARED_SCHEDULER_SCHEMA
    contract.pop("scope")
    contract.pop("execution_edition")
    for row in contract["cells"]:
        source = row.pop("m5_development")
        ids = source.pop("development_slot_ids")
        source["selected_slot_ids"] = ids
        row.pop("development")
        row["m5_source"] = source
        row["period_selection"] = {"mode": "one_period", "selected_slot_ids": list(ids)}
        row["slot_roster_sha256"] = "e" * 64  # structural parser only, not issued authority
    return contract


def test_neutral_m5_contract_preserves_per_cell_admission_without_development_relabel(tmp_path):
    contract = _neutral_m5_metadata_fixture(tmp_path)
    before = copy.deepcopy(contract)
    checked, groups = scheduler._declared_contract(contract)
    assert contract == before and list(groups) == ["XAGUSD", "EURUSD"]
    assert checked["schema_version"] == scheduler.DECLARED_SCHEDULER_SCHEMA
    assert all("development" not in row and "m5_development" not in row for row in checked["cells"])
    assert checked["cells"][1]["m5_source"]["cost_group_metadata"]["symbol"] == "EURUSD"
    assert checked["cells"][0]["m5_source"]["gap_provenance"] == before["cells"][0]["m5_source"]["gap_provenance"]


def test_neutral_m5_contract_refuses_transplanted_cost_metadata(tmp_path):
    contract = _neutral_m5_metadata_fixture(tmp_path)
    contract["cells"][1]["m5_source"]["cost_group_metadata"]["symbol"] = "XAGUSD"
    with pytest.raises(scheduler.HistoricalSchedulerError) as refused:
        scheduler._declared_contract(contract)
    assert refused.value.code == "DECLARED_M5_RECORDED_CELL_METADATA_INVALID"


def test_launcher_freezes_once_and_resumes_without_recompiling(tmp_path, monkeypatch):
    contract, _, _ = _template(tmp_path, monkeypatch)
    source, report = tmp_path / "original.json", tmp_path / "report.json"
    source.write_text(json.dumps(contract, indent=3) + "\n", encoding="utf-8")
    original = source.read_bytes()
    dispatched = []
    monkeypatch.setattr(scheduler, "run_declared_symbol_scheduler", lambda **kwargs:
        dispatched.append(copy.deepcopy(kwargs["contract"])) or {"fixture_provenance": "INVENTED DISPATCH; NO WORKER"})
    assert main._run_declared_rnd_contract(source, report, fresh_contract=True) == 0
    first = json.loads(report.read_text(encoding="utf-8"))
    frozen_path = first["effective_contract"]["path"]
    assert source.read_bytes() == original
    assert first["effective_contract"]["source_contract_sha256"] == hashlib.sha256(main._declared_rnd_contract_bytes(contract)).hexdigest()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Resume must consume its frozen effective contract")

    monkeypatch.setattr(scheduler, "compile_fresh_declared_contract", forbidden)
    monkeypatch.setattr(binding, "resolve_effective_pipeline_config_identity", forbidden)
    assert main._run_declared_rnd_contract(source, report, fresh_contract=True) == 0
    second = json.loads(report.read_text(encoding="utf-8"))
    assert second["effective_contract"]["path"] == frozen_path and dispatched[0] == dispatched[1]
    assert second["contract_sha256"] == first["contract_sha256"]


@pytest.mark.parametrize("damage", ("source", "effective"))
def test_frozen_effective_contract_refuses_changed_owner(tmp_path, monkeypatch, damage):
    contract, _, _ = _template(tmp_path, monkeypatch)
    report = tmp_path / "report.json"
    effective, digest, reference = main._declared_rnd_effective_contract(contract, report, None, fresh_contract=True)
    prior = {"contract_sha256": digest, "effective_contract": reference}
    if damage == "source":
        contract["cells"][0]["run_id"] = "INVENTED-OTHER-RUN"
    else:
        effective["cells"][0]["run_id"] = "INVENTED-OTHER-RUN"
        from pathlib import Path
        Path(reference["path"]).write_text(json.dumps(effective), encoding="utf-8")
    with pytest.raises(ValueError):
        main._declared_rnd_effective_contract(contract, report, prior, fresh_contract=True)


def test_legacy_saved_contract_keeps_original_hash_and_absent_selection(tmp_path, monkeypatch):
    contract, _, _ = _template(tmp_path, monkeypatch)
    expected = hashlib.sha256(main._declared_rnd_contract_bytes(contract)).hexdigest()
    value, digest, reference = main._declared_rnd_effective_contract(contract, tmp_path / "report.json", None, fresh_contract=False)
    assert value is contract and digest == expected and reference is None
    assert "period_selection" not in value["cells"][0]
    assert not (tmp_path / "report.effective-contract.json").exists()


def test_fresh_compilation_does_not_reinterpret_legacy_report(tmp_path, monkeypatch):
    contract, _, _ = _template(tmp_path, monkeypatch)
    prior = {"contract_sha256": hashlib.sha256(main._declared_rnd_contract_bytes(contract)).hexdigest()}
    with pytest.raises(ValueError, match="LEGACY_REPORT_CANNOT_COMPILE_FRESH"):
        main._declared_rnd_effective_contract(contract, tmp_path / "report.json", prior, fresh_contract=True)


def test_config_identity_resolver_matches_the_real_frozen_snapshot(tmp_path):
    from test_production_config_binding import _payload, _write, _context

    payload = _payload(tmp_path)
    path = tmp_path / "config.json"
    _write(path, payload)
    owned, config, refs = binding.resolve_effective_pipeline_config_identity(path)
    context = _context(tmp_path, payload)
    frozen = binding.freeze_production_pipeline_config(context, active_config_path=path,
        config_payload=owned, supplied_config=config)
    assert refs == {"config_sha256": frozen.config_sha256,
        "effective_config_sha256": frozen.effective_config_sha256}


def test_legacy_optional_roster_digest_does_not_create_a_period_selection(tmp_path, monkeypatch):
    contract, _, _ = _template(tmp_path, monkeypatch)
    contract["cells"][0]["slot_roster_sha256"] = "e" * 64
    before = copy.deepcopy(contract)
    checked, _ = scheduler._declared_contract(contract)
    assert contract == before and "period_selection" not in checked["cells"][0]
    assert checked["cells"][0]["slot_roster_sha256"] == "e" * 64


@pytest.mark.parametrize("fresh_contract", (False, True))
def test_existing_null_report_is_refused_without_writes_or_dispatch(tmp_path, monkeypatch, capsys, fresh_contract):
    contract, _, _ = _template(tmp_path, monkeypatch)
    source, report = tmp_path / "original.json", tmp_path / "report.json"
    source.write_text(json.dumps(contract) + "\n", encoding="utf-8")
    report.write_bytes(b"null\n")
    original_source, original_report = source.read_bytes(), report.read_bytes()
    calls = []

    def forbidden(*_args, **_kwargs):
        calls.append("effective-contract-or-dispatch")
        raise AssertionError("An unowned null report must be refused before compilation or dispatch")

    monkeypatch.setattr(main, "_declared_rnd_effective_contract", forbidden)
    monkeypatch.setattr(scheduler, "run_declared_symbol_scheduler", forbidden)
    assert main._run_declared_rnd_contract(source, report, fresh_contract=fresh_contract) == 2
    assert "DECLARED_RND_REPORT_DIFFERENT_OWNER" in capsys.readouterr().out
    assert calls == []
    assert source.read_bytes() == original_source and report.read_bytes() == original_report
    assert not report.with_suffix(".telemetry.jsonl").exists()
    assert not report.with_name(report.stem + ".effective-contract.json").exists()
    assert not (tmp_path / "unused-workspace").exists()


@pytest.mark.parametrize("parallel_workers", (None, 2))
def test_explicit_null_period_selection_refuses_before_parallel_worker_validation(tmp_path, monkeypatch, parallel_workers):
    contract, _, _ = _template(tmp_path, monkeypatch)
    row = contract["cells"][0]
    row["period_selection"] = None
    if parallel_workers is not None:
        row["slot_parallel_workers"] = parallel_workers
    original = copy.deepcopy(contract)
    with pytest.raises(scheduler.HistoricalSchedulerError) as refused:
        scheduler._declared_contract(contract)
    assert refused.value.code == "DECLARED_ORDINARY_PERIOD_SELECTION_INVALID"
    assert contract == original
    assert not (tmp_path / "unused-workspace").exists()
