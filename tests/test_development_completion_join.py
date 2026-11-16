"""Invented software I/O and frozen arithmetic; no native issuer or trading authority.

The retirement protocol is real. The constructed account/source metadata is
explicitly synthetic and cannot establish genuine producer or market acceptance.
"""

from copy import deepcopy
from datetime import timedelta
import hashlib
import json
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_end_grid as grid
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as orchestration
import pm_historical_scheduler as scheduler
import pm_m5_job_view as views
import pm_production_anchor_plan as anchors


PROVENANCE = "INVENTED_SOFTWARE_IO_ONLY_NO_NATIVE_ISSUER_NO_QUALIFICATION"


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
        separators=(",", ":"), allow_nan=False).encode("ascii")).hexdigest()


def write_json(path, value):
    raw = json.dumps(value, sort_keys=True, ensure_ascii=True,
        separators=(",", ":"), allow_nan=False).encode("ascii")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def eligible_contract(root, count=1):
    """K1 uses actual admission; K>1 is an unadmitted pure-join fixture only."""
    from tests.test_m5_development_scheduler import M5DevelopmentSchedulerTests
    harness = M5DevelopmentSchedulerTests("test_declaration_is_detached_and_does_not_create_context")
    harness.root = root
    contract = harness.declaration()
    cell = contract["cells"][0]
    cell["cell"]["cell_id"] = book.compute_cell_id("XAGUSD", "H1", "TREND")
    cell["launch_activation_utc"] = "2023-06-24T00:00:00Z"
    cell["frozen_refit_clock"] = {"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28}
    cell["extend_frozen_clock_backward"] = True
    roster = anchors.derive_declared_owner_slot_roster_v2(
        launch_activation_utc=cell["launch_activation_utc"], frozen_refit_clock=cell["frozen_refit_clock"],
        config_identity_refs={"config_sha256": contract["config_sha256"], "effective_config_sha256": "e" * 64},
        extend_frozen_clock_backward=True, protected_end_exclusive_utc=cell["protected_end_exclusive_utc"])
    slots = roster["slots"][:count]
    dev = cell["m5_development"]
    old_axes = deepcopy(dev["view_axes"])
    axes = []
    for ordinal, slot in enumerate(slots):
        axis = deepcopy(old_axes[0])
        axis.update(view_id="selection-" + str(ordinal), cutoff_utc=slot["native_cutoff_utc"])
        end = scheduler._declared_m5_utc(slot["native_cutoff_utc"])
        axis["interval"] = {"start_utc": (end-timedelta(hours=2)).isoformat(), "end_exclusive_utc": end.isoformat()}
        axes.append(axis)
    calibration, account = old_axes[1:]
    account["cutoff_utc"] = slots[-1]["end_exclusive_utc"]
    account["interval"] = {"start_utc": slots[0]["activation_utc"], "end_exclusive_utc": slots[-1]["end_exclusive_utc"]}
    dev["view_axes"] = axes + [calibration, account]
    dev["development_slot_ids"] = [slot["slot_id"] for slot in slots]
    dev["slot_view_ids"] = {slot["slot_id"]: axes[index]["view_id"] for index, slot in enumerate(slots)}
    dev["required_result_keys"] = dev["development_slot_ids"] + [dev["terminal_result_key"]]
    dev["m5_source_pins"]["view_plan_sha256"] = views.declared_view_plan_sha256(tuple(dev["view_axes"]))
    if count == 1:
        checked, _ = scheduler._declared_m5_contract(contract)
        return checked, roster
    cell["descriptor_index"] = 0
    return contract, roster


def recorded_case(root, *, outcomes=("NO_TRADE",), retire=True, account_payload=None):
    contract, roster = eligible_contract(root, len(outcomes))
    cell = contract["cells"][0]
    dev = cell["m5_development"]
    slots = roster["slots"][:len(outcomes)]
    context = spool.freeze_generation_context(Path(cell["root"]), generation_id=cell["run_id"],
        native_universe_sha256=contract["native_universe_sha256"], config_sha256=contract["config_sha256"],
        limits=spool.SpoolLimits(**contract["spool_limits"]), owned_workspace={"cell_id": cell["cell"]["cell_id"],
            "cell_contract_sha256": scheduler._declared_m5_cell_contract_sha256(contract, cell)})
    sink = spool.open_symbol_sink(context, "SOFTWARE_COMPLETION_FIXTURE")
    payload = {"fixture_provenance": PROVENANCE, "account": account_payload}
    account_entry = sink.put_json("invented-account", "software_fixture", payload)
    sink.seal()
    account_sha = account_entry["descriptor"]["sha256"]
    run_sha, process_sha, precommit_sha = sha("invented-run"), sha("invented-process"), sha("invented-precommit")
    binding = {"m5_view_contract_sha256": sha("invented-view-contract"),
        "generation_context_sha256": context.context_sha256, "config_sha256": contract["config_sha256"],
        "effective_config_sha256": "e" * 64, "search_process_receipt_sha256": process_sha,
        "precommit_sha256": precommit_sha, "run_id": cell["run_id"], "cell_id": cell["cell"]["cell_id"],
        "view_budgets_sha256": sha(contract["view_budgets"]), "view_plan_sha256": dev["m5_source_pins"]["view_plan_sha256"],
        "required_result_keys": dev["required_result_keys"], "terminal_result_key": dev["terminal_result_key"]}
    results, references, rows = [], {}, []
    terminal_consumer = sha("invented-terminal-consumer")
    grid_root = Path(dev["end_grid_root"])
    grid_root.mkdir(parents=True, exist_ok=True)
    for slot, outcome in zip(slots, outcomes):
        slot_id = slot["slot_id"]
        commit_sha, consumer = sha("invented-commit-" + slot_id), sha("invented-consumer-" + slot_id)
        body = {"schema_version": "fxpm.m5-end-grid-reconstruction.v1", "fixture_provenance": PROVENANCE,
            "development_scope": orchestration._declared_development_scope_body(roster, dev["development_slot_ids"]),
            "job_binding": binding, "m5_source_pins": dev["m5_source_pins"],
            "actual_slot_consumer": {"slot_id": slot_id, "request_sha256": sha("invented-request-" + slot_id)},
            "target_view_window": {"view_key": sha("invented-view-" + slot_id),
                "m5_view_contract_sha256": binding["m5_view_contract_sha256"]},
            "selected_slots": len(slots), "accepted_roster_slots": len(roster["slots"]),
            "actual_result_identity": {"slot_commit_sha256": commit_sha, "cell_run_receipt_sha256": run_sha,
                "account_artifact_sha256": account_sha}}
        if slot_id == slots[-1]["slot_id"]:
            body["terminal_account_reference"] = {"semantics": "FINAL_SELECTED_SLOT_ROW_WITH_SEPARATE_ACCOUNT_BINDING",
                "account_artifact_sha256": account_sha, "cell_run_receipt_sha256": run_sha,
                "actual_terminal_consumer_sha256": [terminal_consumer]}
        result = outcome in {"WINNER", "NO_TRADE"}
        view_sha = grid.store_view_contract(grid_root, body) if result else None
        row = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc=scheduler._declared_m5_grid_utc(slot["activation_utc"]),
            cycle_end_utc=scheduler._declared_m5_grid_utc(slot["end_exclusive_utc"]),
            information_cutoff_utc=scheduler._declared_m5_grid_utc(slot["native_cutoff_utc"]),
            outcome=outcome, reason_code="INVENTED_" + outcome,
            source_kind="m5_derived", source_contract_sha256=sha("invented-trade-contract") if outcome == "WINNER" else None,
            no_trade_run_receipt_sha256=run_sha if outcome == "NO_TRADE" else None,
            view_key=sha("invented-view-" + slot_id) if result else None, view_contract_sha256=view_sha,
            source_identity={"activation_id": "invented", "m5_source_sha256": sha("invented-bars"),
                "constructor_implementation_sha256": sha("invented-constructor"), "gap_findings_sha256": sha("invented-gaps")},
            cost_tier="P75" if result else "NONE", process_edition_sha256=process_sha,
            code_sha256={"software-fixture": sha(PROVENANCE)}, metrics={},
            strategy_family="INVENTED_SOFTWARE_FAMILY" if outcome == "WINNER" else None,
            configuration_sha256=sha("invented-configuration") if outcome == "WINNER" else None,
            job_binding_sha256=sha(binding), consumer_binding_sha256=consumer,
            produced_at_utc=scheduler._declared_m5_grid_utc(slot["end_exclusive_utc"]))
        written = grid.EndGridWriter(grid_root).append(row)
        references[slot_id] = {"path": written["path"], "row_sha256": row.row_sha256}
        rows.append(row)
        results.append([slot_id, {"result_sha256": commit_sha, "job_binding_sha256": sha(binding),
            "consumer_binding_sha256": [consumer], "result_descriptor_sha256": row.row_sha256,
            "existing_producer_receipt_sha256": None}])
    results.append([dev["terminal_result_key"], {"result_sha256": account_sha, "job_binding_sha256": sha(binding),
        "consumer_binding_sha256": [terminal_consumer], "result_descriptor_sha256": rows[-1].row_sha256,
        "existing_producer_receipt_sha256": None}])
    fence_path = Path(dev["source_fence_path"])
    fence = {"schema_version": "fxpm.declared-m5-development-source-fence.v1", "fixture_provenance": PROVENANCE,
        "workspace_owner_sha256": spool.read_owned_generation_workspace(context)[1], "end_grid_rows": references,
        "source_summary": {"binding": binding, "results": results, "owned_values_released": True,
            "release_eligible": True, "full_native_context_eligibility": False, "financial_consumer_ready": False},
        **scheduler._DECLARED_M5_SCOPE}
    fence_sha = write_json(fence_path, fence)
    retirement = dict(expected_context_sha256=context.context_sha256,
        workspace_owner_sha256=fence["workspace_owner_sha256"], source_fence_path=fence_path,
        source_fence_sha256=fence_sha, retirement_receipt_path=Path(dev["retirement"]["receipt_path"]),
        protected_paths=(grid_root, fence_path), owned_direct_files=(), unit_lock_paths=(),
        max_paths=100, max_nodes=300, max_bytes=4 * 1024**2)
    actual = scheduler._declared_cell_row(cell, "NOT_RUN", "INVENTED_NOT_FINISHED")
    actual.update(source_kind="m5_derived", result_kind="DEVELOPMENT_PARTIAL",
        design_contaminated=True, pristine_validation=False, terminal_refusal_end_grid=deepcopy(references))
    if retire:
        archived = spool.retire_completed_generation(context.root, **retirement)
        scheduler._declared_m5_archived_row(actual, archived)
        if len(slots) == 1:
            scheduler._declared_confirm_receipt(contract, actual)
    dispatch = {"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA, "status": "COMPLETED", "cells": [actual],
        "errors": [], "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    return {"contract": contract, "dispatch": dispatch, "rows": rows, "context": context,
        "archive": archived if retire else None, "roster": roster,
        "retirement": retirement, "fence": fence, "fence_path": fence_path, "account_sha": account_sha}


def projected_periods(env):
    cell = env["contract"]["cells"][0]
    expected = [{"cell_id": cell["cell"]["cell_id"], "slot_id": slot["slot_id"],
        "information_cutoff_utc": scheduler._declared_m5_grid_utc(slot["native_cutoff_utc"])}
        for slot in env["roster"]["slots"][:len(env["rows"])]]
    actual_rows = scheduler._declared_development_grid_rows(cell,
        env["dispatch"]["cells"][0]["terminal_refusal_end_grid"])
    return expected, actual_rows


def test_all_periods_survive_latest_cycle_summary_and_no_trade_is_bound(tmp_path):
    """Three-period identity join only; the pinned public admission is still K1."""
    env = recorded_case(tmp_path, outcomes=("NO_TRADE",) * 3)
    expected, actual_rows = projected_periods(env)
    identities = scheduler._declared_development_completed_bindings(env["contract"],
        env["contract"]["cells"][0], expected, env["archive"], env["fence"], actual_rows)
    periods = scheduler._declared_development_project_periods(expected, env["dispatch"]["cells"][0], actual_rows)
    assert grid.grid_summary(env["rows"])["cells_recorded"] == 1
    assert [period["row_sha256"] for period in periods] == [row.row_sha256 for row in env["rows"]]
    assert [period["outcome"] for period in periods] == ["NO_TRADE"] * 3
    assert identities == (env["account_sha"], sha("invented-run"))


def test_current_k1_public_completion_is_repeatable_and_authority_stays_false(tmp_path):
    env = recorded_case(tmp_path)
    first = scheduler.build_declared_m5_development_completion(env["contract"], env["dispatch"])
    again = scheduler.build_declared_m5_development_completion(env["contract"], env["dispatch"])
    body = first.to_dict()
    assert first.receipt_sha256 == again.receipt_sha256
    assert body["mechanism_complete"] is True and len(body["periods"]) == 1
    assert body["account_artifact_sha256"] == env["account_sha"]
    assert body["design_contaminated"] is True and body["pristine_validation"] is False
    assert all(body[key] is False for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"))


def test_unadmitted_k3_does_not_bypass_current_pinned_k1_scope(tmp_path):
    contract, _ = eligible_contract(tmp_path, 3)
    with pytest.raises(scheduler.HistoricalSchedulerError, match="DECLARED_M5_RESEARCH_ADMISSION_CHANGED"):
        scheduler._declared_development_expected(contract)


@pytest.mark.parametrize("case_id", ["N10_NO_TRADE_MANAGES_HELD_LONG_STOP", "N14_GAP_EXIT_OLD_P75_NEW_P95_SAME_BAR"])
def test_retained_join_does_not_rerun_frozen_carry_account(tmp_path, monkeypatch, case_id):
    from tests.test_declared_cell_account_transport import _synthetic_continuous_account_case
    original = _synthetic_continuous_account_case(case_id)
    env = recorded_case(tmp_path, outcomes=("WINNER", "NO_TRADE", "WINNER"), account_payload=original.account)
    def forbidden(*args, **kwargs):
        pytest.fail("completion or resume reran the account")
    monkeypatch.setattr(forward, "_run_declared_cell_account", forbidden)
    monkeypatch.setattr(forward, "produce_declared_m5_development_cell_account_artifact", forbidden)
    expected, rows = projected_periods(env)
    args = (env["contract"], env["contract"]["cells"][0], expected, env["archive"], env["fence"], rows)
    first = scheduler._declared_development_completed_bindings(*args)
    resumed = scheduler._declared_development_completed_bindings(*args)
    assert first == resumed == (env["account_sha"], sha("invented-run"))
    assert [row.outcome for row in rows.values()] == ["WINNER", "NO_TRADE", "WINNER"]


@pytest.mark.parametrize("status", ["FAILED", "NOT_RUN", "UNRESOLVED_CRASH"])
def test_dispatch_failure_never_becomes_completed_no_trade(tmp_path, status):
    contract, _ = eligible_contract(tmp_path)
    actual = scheduler._declared_cell_row(contract["cells"][0], status, "INVENTED_TECHNICAL_FAILURE")
    dispatch = {"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA, "status": "COMPLETED", "cells": [actual],
        "errors": [], "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    body = scheduler.build_declared_m5_development_completion(contract, dispatch).to_dict()
    assert [item["coverage_status"] for item in body["periods"]] == [status]
    assert all(item["outcome"] is None for item in body["periods"])
    assert body["mechanism_complete"] is False and body["period_coverage_complete"] is False


def test_unavailable_stop_and_unfinished_periods_remain_distinct(tmp_path):
    env = recorded_case(tmp_path, outcomes=("UNAVAILABLE", "STOP", "NO_TRADE"), retire=False)
    actual = env["dispatch"]["cells"][0]
    del actual["terminal_refusal_end_grid"][env["contract"]["cells"][0]["m5_development"]["development_slot_ids"][-1]]
    expected, rows = projected_periods(env)
    periods = scheduler._declared_development_project_periods(expected, actual, rows)
    assert [item["coverage_status"] for item in periods] == ["UNAVAILABLE", "STOP", "NOT_RUN"]
    assert periods[-1]["outcome"] is None and len(rows) < len(expected)


def test_missing_row_blocks_retirement_before_payload_release(tmp_path):
    env = recorded_case(tmp_path, retire=False)
    path = Path(next(iter(env["fence"]["end_grid_rows"].values()))["path"])
    path.unlink()
    before = list((env["context"].root / "objects").rglob("*"))
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_GRID_READBACK_REQUIRED"):
        spool.retire_completed_generation(env["context"].root, **env["retirement"])
    assert before and all(path.exists() for path in before)
    body = scheduler.build_declared_m5_development_completion(env["contract"],
        {**env["dispatch"], "cells": [{**env["dispatch"]["cells"][0], "terminal_refusal_end_grid": {}}]}).to_dict()
    assert body["mechanism_complete"] is False and body["archive_record"] is None


def test_replaced_retained_view_contract_fails_closed_on_resume(tmp_path):
    env = recorded_case(tmp_path)
    row = env["rows"][0]
    contract_path = grid._contract_path(Path(env["contract"]["cells"][0]["m5_development"]["end_grid_root"]), row.view_contract_sha256)
    assert contract_path.exists()
    contract_path.write_bytes(b'{"fixture_provenance":"INVENTED_TAMPER"}')
    with pytest.raises((spool.AuthoritySpoolError, grid.EndGridError)):
        scheduler.build_declared_m5_development_completion(env["contract"], env["dispatch"])


def test_protected_cutoff_is_not_relaxed_for_completion(tmp_path):
    contract, _ = eligible_contract(tmp_path)
    dev = contract["cells"][0]["m5_development"]
    dev["view_axes"][0]["cutoff_utc"] = "2024-07-14T08:06:00Z"
    # The real view decoder rejects the future cutoff before any hash or join.
    with pytest.raises(views.M5JobViewError, match="M5_VIEW_PLAN_INVALID"):
        scheduler._declared_development_expected(contract)


def test_roster_requires_each_frozen_dispatch_once_and_returns_isolated_copy(tmp_path):
    env = recorded_case(tmp_path)
    with pytest.raises(scheduler.HistoricalSchedulerError, match="DEVELOPMENT_COMPLETION_ROSTER_SHAPE"):
        scheduler.reconcile_declared_m5_development_roster([env["contract"]], [])
    with pytest.raises(scheduler.HistoricalSchedulerError, match="DEVELOPMENT_COMPLETION_DUPLICATE_PERIOD"):
        scheduler.reconcile_declared_m5_development_roster([env["contract"]] * 2, [env["dispatch"]] * 2)
    report = scheduler.reconcile_declared_m5_development_roster([env["contract"]], [env["dispatch"]])
    copy = report.to_dict()
    copy["cells"].clear()
    assert len(report.to_dict()["cells"]) == 1 and report.to_dict()["mechanism_complete"] is True


def test_existing_launcher_report_writer_retains_typed_completion(tmp_path):
    """The real writer serializes the new child field; no CLI or worker runs."""
    import pm_main
    env = recorded_case(tmp_path)
    completion = scheduler.build_declared_m5_development_completion(env["contract"], env["dispatch"]).to_dict()
    destination = tmp_path / "software-dispatch-report.json"
    parent = {"schema_version": pm_main._DECLARED_RND_REPORT_SCHEMA, "status": "COMPLETED",
        "fixture_provenance": PROVENANCE, "dispatch_report": {**env["dispatch"], "development_completion": completion}}
    pm_main._declared_rnd_write_report(destination, parent)
    restored = json.loads(destination.read_bytes())
    assert restored == parent
    assert restored["dispatch_report"]["development_completion"]["status"] == "MECHANISM_COMPLETE"
    assert restored["dispatch_report"]["development_completion"]["qualification_authority"] is False


def test_native_dispatch_is_not_silently_shortened(tmp_path):
    contract, _ = eligible_contract(tmp_path)
    contract["schema_version"] = scheduler.DECLARED_SCHEDULER_SCHEMA
    with pytest.raises(scheduler.HistoricalSchedulerError, match="DECLARED_M5_CONTRACT_INVALID"):
        scheduler._declared_development_expected(contract)
