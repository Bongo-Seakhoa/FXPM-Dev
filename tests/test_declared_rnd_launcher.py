"""Actual declared launcher/dispatcher witnesses with invented technical payloads.

The child adapter reuses the approved scheduler fixture's finite upstream
doubles. It runs the real governor, symbol worker, context store, lease, result
validation and receipt confirmation. No market, fit or native history is built.
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import time

import pytest

import pm_authority_spool as spool
import pm_historical_scheduler as scheduler
import pm_live_readiness as readiness
import pm_main as main
import test_declared_symbol_scheduler as declared_fixture


REPORT_SCHEMA = "fxpm.declared-rnd-launcher-report.v1"
GRANTS = ("qualification_authority", "capital_authority", "live_authority", "order_authority")
_LAUNCHER_BARRIER_TIMEOUT_SECONDS = 120


def _launcher_inert_worker(contract, symbol):
    """Spawn-safe timing envelope; all domain work stays in the existing fixture."""
    board = Path(contract["synthetic_board"])
    board.mkdir(parents=True, exist_ok=True)
    start = {"pid": os.getpid(), "symbol": symbol, "at_ns": time.monotonic_ns()}
    (board / f"{symbol}.launcher-start.json").write_text(json.dumps(start), encoding="utf-8")
    # A batch barrier would deadlock: AUDUSD releases only after a freed slot
    # has admitted GBPUSD while AUDUSD is still alive.
    if contract.get("launcher_hold_first_until_refill") and symbol == "AUDUSD":
        deadline = time.monotonic() + _LAUNCHER_BARRIER_TIMEOUT_SECONDS
        while not (board / "GBPUSD.launcher-start.json").exists() and time.monotonic() < deadline:
            time.sleep(.02)
        assert (board / "GBPUSD.launcher-start.json").is_file(), "Rolling refill did not admit the third symbol"
    try:
        return declared_fixture._synthetic_declared_worker(contract, symbol)
    finally:
        finish = {"pid": os.getpid(), "symbol": symbol, "at_ns": time.monotonic_ns()}
        (board / f"{symbol}.launcher-end.json").write_text(json.dumps(finish), encoding="utf-8")


def _launcher_report_owner_worker(contract, symbol):
    """Hold only an inert worker while its actual launcher owns the report."""
    hold = Path(contract["launcher_report_hold_board"])
    hold.mkdir(parents=True, exist_ok=True)
    (hold / "owner-worker-started.json").write_text(
        json.dumps({"pid": os.getpid(), "symbol": symbol}), encoding="utf-8")
    deadline = time.monotonic() + _LAUNCHER_BARRIER_TIMEOUT_SECONDS
    while not (hold / "release").exists() and time.monotonic() < deadline:
        time.sleep(.02)
    assert (hold / "release").is_file(), "Bounded report-owner barrier was not released"
    return _launcher_inert_worker(contract, symbol)


def _launcher_owner_process(source, report, result):
    code = main._run_declared_rnd_contract(
        Path(source), Path(report), requested_workers=1, _worker=_launcher_report_owner_worker)
    Path(result).write_text(json.dumps({"exit_code": code, "pid": os.getpid()}), encoding="utf-8")
    if code:
        raise SystemExit(code)


def _inputs(tmp_path, symbols=("AUDUSD",)):
    contract = declared_fixture._contract(tmp_path, symbols)
    config = Path(contract["active_config_path"])
    config.write_text('{"classification":"INERT_LAUNCHER_TEST_INPUT"}\n', encoding="utf-8")
    source = tmp_path / "contract.json"
    report = tmp_path / "launch-report.json"
    _write_contract(source, contract)
    return contract, source, report


def _slot_parallel_contract(tmp_path, symbols=("EURUSD",)):
    """Invented launch metadata for route selection, without input or spool bodies."""
    contract = declared_fixture._contract(tmp_path, symbols)
    row = contract["cells"][0]
    row["native_finalization"] = {
        "end_grid_root": str(tmp_path / "grid"),
        "source_fence_path": str(tmp_path / "fence" / "source.json"),
        "retirement": {
            "receipt_path": str(tmp_path / "retirement" / "receipt.json"),
            "max_paths": 181815,
            "max_nodes": 2000000,
            "max_bytes": 8 * 1024**3,
        },
    }
    row["source_access_window_policy"] = {"lead_seconds": 0, "validity_seconds": 172800}
    row["slot_parallel_workers"] = 4
    return contract


def _write_contract(path, body):
    path.write_text(json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False), encoding="utf-8")


def _report(path, source, status):
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["schema_version"] == REPORT_SCHEMA
    assert body["contract_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert body["status"] == status
    assert all(body[name] is False for name in GRANTS)
    return body


def _cli_exit(argv):
    try:
        return main.main(argv)
    except SystemExit as exc:
        return exc.code


def test_real_launcher_overlaps_children_refills_and_preserves_serial_cell_accounts(tmp_path, monkeypatch):
    contract, source, report = _inputs(tmp_path, ("AUDUSD", "AUDUSD", "EURUSD", "GBPUSD"))
    contract["launcher_hold_first_until_refill"] = True
    _write_contract(source, contract)
    real_fsync, real_replace = os.fsync, os.replace
    synced_files, publications = set(), []

    def observe_fsync(fd):
        info = os.fstat(fd)
        result = real_fsync(fd)
        synced_files.add((info.st_dev, info.st_ino))
        return result

    def observe_replace(src, dst):
        if Path(dst).resolve() == report.resolve():
            temporary = Path(src)
            assert temporary.resolve() != report.resolve()
            assert temporary.parent.resolve() == report.parent.resolve()
            info = temporary.stat()
            assert (info.st_dev, info.st_ino) in synced_files, "Report temp was not fsynced before replacement"
            publications.append(json.loads(temporary.read_text(encoding="utf-8"))["status"])
        return real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", observe_fsync)
    monkeypatch.setattr(os, "replace", observe_replace)
    assert main._run_declared_rnd_contract(source, report, requested_workers=2, _worker=_launcher_inert_worker) == 0
    body = _report(report, source, "COMPLETED")
    dispatch = body["dispatch_report"]
    assert publications == ["RUNNING", "COMPLETED"]
    assert dispatch["worker_limit"] == 2
    assert [row["descriptor_index"] for row in dispatch["cells"]] == list(range(4))
    assert [row["status"] for row in dispatch["cells"]] == ["COMPLETED"] * 4
    assert all(row["run_receipt_confirmed"] is True for row in dispatch["cells"])
    assert all(row[name] is False for row in dispatch["cells"] for name in GRANTS)
    assert len({row["root"] for row in dispatch["cells"]}) == 4
    assert len({row["run_id"] for row in dispatch["cells"]}) == 4
    assert dispatch["cells"][0]["worker_pid"] == dispatch["cells"][1]["worker_pid"]
    assert len({row["worker_pid"] for row in dispatch["cells"]}) == 3
    for row in dispatch["cells"]:
        stored = Path(row["root"]) / row["run_receipt"]["relative_path"]
        receipt = json.loads(stored.read_text(encoding="ascii"))
        assert receipt["completed_slots"] == 13
        assert receipt["local_trial_roster_sizes"] == [300] * 13
        assert receipt["child_pid"] == row["worker_pid"]
        assert hashlib.sha256(stored.read_bytes()).hexdigest() == row["run_receipt"]["sha256"]
    board = Path(contract["synthetic_board"])
    starts = {s: json.loads((board / f"{s}.launcher-start.json").read_text()) for s in ("AUDUSD", "EURUSD", "GBPUSD")}
    ends = {s: json.loads((board / f"{s}.launcher-end.json").read_text()) for s in starts}
    assert starts["AUDUSD"]["at_ns"] < ends["EURUSD"]["at_ns"]
    assert starts["EURUSD"]["at_ns"] < ends["AUDUSD"]["at_ns"]
    assert ends["EURUSD"]["at_ns"] <= starts["GBPUSD"]["at_ns"] < ends["AUDUSD"]["at_ns"]
    assert report.with_suffix(".telemetry.jsonl").is_file()


def test_real_child_failure_persists_drained_report_without_discarding_healthy_symbol(tmp_path):
    contract, source, report = _inputs(tmp_path, ("AUDUSD", "AUDUSD", "EURUSD"))
    contract["synthetic_behaviors"] = {"synthetic-cell-0": {"action": "raise", "delay": .2}}
    _write_contract(source, contract)
    assert main._run_declared_rnd_contract(source, report, requested_workers=2, _worker=_launcher_inert_worker) == 1
    body = _report(report, source, "FAILED")
    assert body["error_code"] == "DECLARED_SYMBOL_DISPATCH_FAILED"
    rows = body["dispatch_report"]["cells"]
    assert [row["status"] for row in rows] == ["FAILED", "NOT_RUN", "COMPLETED"]
    assert rows[1]["reason_code"] == "PRIOR_CELL_FAILURE"
    assert rows[2]["run_receipt_confirmed"] is True
    board = Path(contract["synthetic_board"])
    assert {p.name.split(".")[0] for p in board.glob("*.launcher-end.json")} == {"AUDUSD", "EURUSD"}


def test_real_child_disk_low_is_preserved_as_nonterminal_not_run(tmp_path):
    contract, source, report = _inputs(tmp_path, ("AUDUSD", "AUDUSD"))
    contract["synthetic_child_free_bytes"] = 0
    _write_contract(source, contract)
    assert main._run_declared_rnd_contract(source, report, requested_workers=1, _worker=_launcher_inert_worker) == 0
    body = _report(report, source, "COMPLETED")
    assert body["dispatch_report"]["admission_deferral_reason"] == "DISK_LOW"
    assert [(row["status"], row["reason_code"]) for row in body["dispatch_report"]["cells"]] == [
        ("NOT_RUN", "DISK_LOW"), ("NOT_RUN", "DISK_LOW")]
    assert all(not Path(row["root"]).exists() for row in contract["cells"])


def test_real_held_cell_lease_is_persisted_as_busy_without_issuance(tmp_path):
    contract, source, report = _inputs(tmp_path)
    row = contract["cells"][0]
    context = spool.freeze_generation_context(
        Path(row["root"]), generation_id=row["run_id"],
        native_universe_sha256=contract["native_universe_sha256"], config_sha256=contract["config_sha256"],
        limits=spool.SpoolLimits(min_free_bytes=scheduler.DECLARED_CELL_MIN_FREE_BYTES))
    with scheduler._declared_dispatch_lease(context) as acquired:
        assert acquired is True
        assert main._run_declared_rnd_contract(source, report, requested_workers=1, _worker=_launcher_inert_worker) == 0
    body = _report(report, source, "COMPLETED")
    outcome = body["dispatch_report"]["cells"][0]
    assert (outcome["status"], outcome["reason_code"]) == ("NOT_RUN", "BUSY")
    assert outcome["precommit_sha256"] is None
    assert outcome["run_receipt"] is None
    assert list(Path(row["root"]).glob("declared-slots/*/precommit.json")) == []


def test_same_contract_report_resume_reopens_saved_account_without_changing_run_receipt(tmp_path):
    contract, source, report = _inputs(tmp_path)
    assert main._run_declared_rnd_contract(source, report, requested_workers=1, _worker=_launcher_inert_worker) == 0
    first = _report(report, source, "COMPLETED")["dispatch_report"]["cells"][0]
    run = Path(first["root"]) / first["run_receipt"]["relative_path"]
    original = run.read_bytes()
    assert main._run_declared_rnd_contract(source, report, requested_workers=1, _worker=_launcher_inert_worker) == 0
    second = _report(report, source, "COMPLETED")["dispatch_report"]["cells"][0]
    assert second["status"] == "REOPENED"
    assert second["recorded_before_dispatch"] is True
    assert second["run_receipt_confirmed"] is True
    assert second["account_descriptor"] == first["account_descriptor"]
    assert run.read_bytes() == original


def test_real_concurrent_different_contract_refuses_held_report_then_same_owner_resumes(tmp_path):
    first_root, second_root = tmp_path / "first", tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first_contract, first_source, _ = _inputs(first_root)
    second_contract, second_source, _ = _inputs(second_root)
    hold = tmp_path / "owner-barrier"
    first_contract["launcher_report_hold_board"] = str(hold)
    _write_contract(first_source, first_contract)
    report = tmp_path / "shared-report.json"
    result_path = tmp_path / "first-launcher-exit.json"
    process = multiprocessing.get_context("spawn").Process(
        target=_launcher_owner_process, args=(str(first_source), str(report), str(result_path)))
    process.start()
    try:
        deadline = time.monotonic() + _LAUNCHER_BARRIER_TIMEOUT_SECONDS
        while not (hold / "owner-worker-started.json").exists() and time.monotonic() < deadline:
            assert process.is_alive(), "First launcher ended before holding its real report lease"
            time.sleep(.02)
        assert (hold / "owner-worker-started.json").is_file()
        _report(report, first_source, "RUNNING")
        running_bytes = report.read_bytes()
        telemetry = report.with_suffix(".telemetry.jsonl")
        telemetry_before = telemetry.read_bytes() if telemetry.exists() else None
        assert main._run_declared_rnd_contract(
            second_source, report, requested_workers=1, _worker=_launcher_inert_worker) == 2
        assert process.is_alive()
        assert report.read_bytes() == running_bytes
        assert not Path(second_contract["synthetic_board"]).exists()
        assert all(not Path(row["root"]).exists() for row in second_contract["cells"])
        # Active first-owner telemetry may append on its own. The refused
        # caller must not replace/truncate any already-observed prefix.
        if telemetry_before is not None:
            assert telemetry.read_bytes().startswith(telemetry_before)
    finally:
        hold.mkdir(parents=True, exist_ok=True)
        (hold / "release").write_text("release labelled inert owner", encoding="utf-8")
        process.join(timeout=90)
    assert not process.is_alive(), "Released bounded launcher did not drain"
    assert process.exitcode == 0
    assert json.loads(result_path.read_text())["exit_code"] == 0
    complete = _report(report, first_source, "COMPLETED")
    assert complete["dispatch_report"]["cells"][0]["run_receipt_confirmed"] is True
    lock = report.with_name(report.name + ".lock")
    assert lock.is_file() and lock.stat().st_size == 1
    assert main._run_declared_rnd_contract(
        first_source, report, requested_workers=1, _worker=_launcher_report_owner_worker) == 0
    resumed = _report(report, first_source, "COMPLETED")
    assert resumed["dispatch_report"]["cells"][0]["status"] == "REOPENED"
    assert lock.stat().st_size == 1


@pytest.mark.parametrize("prior", [
    {"operator_file": "preserve exactly"},
    {"schema_version": REPORT_SCHEMA, "contract_sha256": "f" * 64, "status": "COMPLETED"},
])
def test_unrelated_existing_report_refuses_without_clobber_or_dispatch(tmp_path, prior):
    contract, source, report = _inputs(tmp_path)
    report.write_text(json.dumps(prior), encoding="utf-8")
    original = report.read_bytes()
    assert main._run_declared_rnd_contract(source, report, _worker=_launcher_inert_worker) == 2
    assert report.read_bytes() == original
    assert not Path(contract["synthetic_board"]).exists()
    assert all(not Path(row["root"]).exists() for row in contract["cells"])


def test_unowned_existing_telemetry_refuses_without_clobber_or_report(tmp_path):
    contract, source, report = _inputs(tmp_path)
    telemetry = report.with_suffix(".telemetry.jsonl")
    telemetry.write_text('{"operator_file":"preserve"}\n', encoding="utf-8")
    original = telemetry.read_bytes()
    assert main._run_declared_rnd_contract(source, report, _worker=_launcher_inert_worker) == 2
    assert telemetry.read_bytes() == original
    assert not report.exists()
    assert not Path(contract["synthetic_board"]).exists()


@pytest.mark.parametrize("collision", ["contract", "config", "telemetry-contract", "telemetry-config"])
def test_output_input_collisions_refuse_without_changing_inputs(tmp_path, collision):
    contract, source, report = _inputs(tmp_path)
    config = Path(contract["active_config_path"])
    if collision == "contract":
        report = source
    elif collision == "config":
        report = config
    elif collision == "telemetry-contract":
        source = report.with_suffix(".telemetry.jsonl")
        _write_contract(source, contract)
    else:
        config = report.with_suffix(".telemetry.jsonl")
        config.write_text("preserve inert config", encoding="utf-8")
        contract["active_config_path"] = str(config)
        _write_contract(source, contract)
    originals = {path: path.read_bytes() for path in {source, config}}
    assert main._run_declared_rnd_contract(source, report, _worker=_launcher_inert_worker) == 2
    assert all(path.read_bytes() == body for path, body in originals.items())
    assert not Path(contract["synthetic_board"]).exists()


@pytest.mark.parametrize("bad_json", ["duplicate", "NaN", "Infinity", "-Infinity"])
def test_strict_contract_json_refuses_before_dispatch_or_report_write(tmp_path, monkeypatch, bad_json):
    contract, source, report = _inputs(tmp_path)
    valid = source.read_text(encoding="utf-8")
    if bad_json == "duplicate":
        raw = '{"schema_version":' + json.dumps(contract["schema_version"]) + "," + valid[1:]
    else:
        raw = '{"launcher_nonfinite_probe":' + bad_json + "," + valid[1:]
    source.write_text(raw, encoding="utf-8")

    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid JSON reached dispatch")

    monkeypatch.setattr(scheduler, "run_declared_symbol_scheduler", forbidden)
    assert main._run_declared_rnd_contract(source, report, _worker=_launcher_inert_worker) == 2
    assert not report.exists()
    assert not Path(contract["synthetic_board"]).exists()


def test_cli_routes_declared_rnd_before_config_or_app_and_preserves_no_policy(tmp_path, monkeypatch):
    _, source, report = _inputs(tmp_path)
    calls = []

    def forbidden(*args, **kwargs):
        raise AssertionError("Explicit declared route reached ordinary config/application startup")

    def route(contract_path, report_path, *, requested_workers=0, _worker=None):
        calls.append((Path(contract_path), Path(report_path), requested_workers, _worker))
        return 0

    original_policy = readiness.interval_cell_qualification_readiness_finding()
    monkeypatch.setattr(main, "load_config_json", forbidden)
    monkeypatch.setattr(main, "FXPortfolioManagerApp", forbidden)
    monkeypatch.setattr(main, "_run_declared_rnd_contract", route)
    assert _cli_exit(["declared-rnd", "--contract", str(source), "--report", str(report), "--workers", "2"]) == 0
    assert calls == [(source, report, 2, None)]
    assert readiness.interval_cell_qualification_readiness_finding() == original_policy
    assert original_policy[0] == "interval_cell_qualification_policy_unadopted"


@pytest.mark.parametrize("slot_workers", [None, 2, 4], ids=["legacy-symbol", "two-slots", "four-slots"])
def test_declared_launcher_selects_only_the_opted_in_parent_slot_route(tmp_path, monkeypatch, slot_workers):
    contract = _slot_parallel_contract(tmp_path)
    row = contract["cells"][0]
    if slot_workers is None:
        del row["slot_parallel_workers"]
    else:
        row["slot_parallel_workers"] = slot_workers
    source, report = tmp_path / "contract.json", tmp_path / "launch-report.json"
    _write_contract(source, contract)
    calls = []

    def parent_cell(owned, selected, *, slot_dispatch=None, slot_worker_limit=1):
        assert slot_workers is not None, "A legacy cell bypassed the symbol governor"
        assert owned["cells"] == [selected]
        assert selected["cell"] == row["cell"]
        assert selected["slot_parallel_workers"] == slot_worker_limit == slot_workers
        assert callable(slot_dispatch)
        calls.append("parent-slot-route")
        return scheduler._declared_cell_row(selected, "NOT_RUN", "BUSY")

    def symbol_stream(self, worker, tasks, **kwargs):
        assert slot_workers is None, "The opt-in cell was dispatched as a daemon symbol task"
        assert worker is scheduler.declared_symbol_worker
        task, = list(tasks)
        assert task.key == row["cell"]["symbol"]
        assert "slot_parallel_workers" not in task.args[0]["cells"][0]
        calls.append("legacy-symbol-route")
        yield scheduler.memory_governor.TaskOutcome(task_key=task.key, status="NOT_RUN")

    def forbidden(*args, **kwargs):
        pytest.fail("The metadata-only routing probe created a spool context")

    monkeypatch.setattr(scheduler, "_declared_run_one_cell", parent_cell)
    monkeypatch.setattr(scheduler.memory_governor.MemoryGovernor, "iter_completed", symbol_stream)
    monkeypatch.setattr(scheduler, "_declared_disk_free_bytes", lambda path: 40 * 1024**3)
    monkeypatch.setattr(spool, "freeze_generation_context", forbidden)
    workers = slot_workers or 1
    assert _cli_exit(["declared-rnd", "--contract", str(source), "--report", str(report), "--workers", str(workers)]) == 0
    dispatch = _report(report, source, "COMPLETED")["dispatch_report"]
    assert calls == ["parent-slot-route" if slot_workers else "legacy-symbol-route"]
    assert dispatch["requested_workers"] == dispatch["worker_limit"] == workers
    result, = dispatch["cells"]
    assert result["status"] == "NOT_RUN"
    assert all(result[name] is False for name in GRANTS)
    if slot_workers:
        assert result["reason_code"] == "BUSY"
    assert not Path(row["root"]).exists()


@pytest.mark.parametrize(
    "defect",
    ["null-row", "bool-row", "float-row", "string-row", "wrong-row-count", "non-native", "multiple-cells",
     "opt-in-second-cell", "m5-schema", "mixed-source", "auto-workers", "serial-workers",
     "too-few-workers", "too-many-workers", "float-workers", "custom-worker"],
)
def test_declared_slot_parallel_refuses_invalid_route_before_context_or_dispatch(tmp_path, monkeypatch, defect):
    symbols = ("EURUSD", "GBPUSD") if defect in {"multiple-cells", "opt-in-second-cell"} else ("EURUSD",)
    contract = _slot_parallel_contract(tmp_path, symbols)
    row = contract["cells"][0]
    requested_workers = 4
    options = {}
    row_values = {"null-row": None, "bool-row": True, "float-row": 4.0, "string-row": "4", "wrong-row-count": 3}
    worker_values = {"auto-workers": 0, "serial-workers": 1, "too-few-workers": 3,
                     "too-many-workers": 5, "float-workers": 4.0}
    if defect in row_values:
        row["slot_parallel_workers"] = row_values[defect]
    elif defect == "non-native":
        del row["native_finalization"]
    elif defect == "opt-in-second-cell":
        second = contract["cells"][1]
        second["native_finalization"] = row.pop("native_finalization")
        second["slot_parallel_workers"] = row.pop("slot_parallel_workers")
    elif defect == "m5-schema":
        contract["schema_version"] = scheduler.DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA
    elif defect == "mixed-source":
        row["m5_development"] = {}
    elif defect in worker_values:
        requested_workers = worker_values[defect]
    elif defect == "custom-worker":
        options["worker"] = _launcher_inert_worker

    def forbidden(*args, **kwargs):
        pytest.fail("An invalid slot-parallel contract reached work")

    monkeypatch.setattr(scheduler, "_declared_run_one_cell", forbidden)
    monkeypatch.setattr(scheduler.memory_governor.MemoryGovernor, "iter_completed", forbidden)
    monkeypatch.setattr(spool, "freeze_generation_context", forbidden)
    telemetry = tmp_path / "telemetry.jsonl"
    with pytest.raises(scheduler.HistoricalSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(
            contract=contract, requested_workers=requested_workers, telemetry_path=telemetry, **options)
    assert caught.value.code == "DECLARED_SLOT_PARALLEL_CONTRACT_INVALID"
    assert not telemetry.exists()
    assert all(not Path(cell["root"]).exists() for cell in contract["cells"])


@pytest.mark.parametrize("telemetry_failure", [False, True])
def test_declared_slot_parallel_preserves_parent_admission_failures(tmp_path, monkeypatch, telemetry_failure):
    contract = _slot_parallel_contract(tmp_path)
    telemetry = tmp_path / "telemetry.jsonl"
    if telemetry_failure:
        telemetry.mkdir()

    def parent_cell(owned, row, **kwargs):
        assert not telemetry_failure, "Telemetry failure reached cell setup"
        return scheduler._declared_cell_row(row, "NOT_RUN", "RETIREMENT_INCOMPLETE")

    monkeypatch.setattr(scheduler, "_declared_run_one_cell", parent_cell)
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(
            contract=contract, requested_workers=4, telemetry_path=telemetry,
        )
    report = caught.value.report
    row, = report["cells"]
    assert report["status"] == "FAILED"
    assert row["status"] == "NOT_RUN"
    assert row["reason_code"] == ("ADMISSION_CLOSED" if telemetry_failure else "RETIREMENT_INCOMPLETE")
    assert not Path(row["root"]).exists()


@pytest.mark.parametrize("case", ["expired", "child-capacity", "live-claim"])
def test_declared_slot_parallel_real_scheduler_guards(tmp_path, monkeypatch, case):
    from datetime import datetime, timedelta, timezone

    import pm_book_multiplicity as book
    import pm_authority_spool_quota as quota
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_portfolio_replay as portfolio
    import pm_spread_profile as spread
    from prototype_support import _DeclaredClock
    from test_production_config_binding import _payload

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FXPM_STAGE_TIMERS_DIR", raising=False)
    contract = _slot_parallel_contract(tmp_path)
    cell = contract["cells"][0]
    cell["cell"]["cell_id"] = book.compute_cell_id("EURUSD", "H1", "TREND")
    cell["frozen_refit_clock"]["anchor_activation_utc"] = "2026-03-29T00:01:00Z"
    for name in ("grid", "fence", "retirement"):
        (tmp_path / name).mkdir()
    payload = _payload(tmp_path)
    payload["_meta"]["scope"] = "UNISSUED_TECHNICAL_BENCHMARK"
    payload["pipeline"].update(research_mode=True, initial_capital=10000.0,
                               discovery_risk_per_trade_pct=1.0, risk_per_trade_pct=0.25)
    _write_contract(Path(contract["active_config_path"]), payload)
    contract["config_sha256"] = portfolio.canonical_json_sha256(payload)
    profile = tmp_path / "data" / "native" / "terms" / "spread_profile.json"
    profile.parent.mkdir(parents=True)
    _write_contract(profile, {
        "schema_version": spread.PROFILE_SCHEMA,
        "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "production_authority": False,
        "classification": "INVENTED_EMPTY_PROFILE_FOR_EXPIRED_SLOT_SOFTWARE_TEST",
        "cells": [],
    })
    contract["profile_sha256"] = hashlib.sha256(profile.read_bytes()).hexdigest()
    snapshot = {
        "pointer_schema": "INVENTED", "manifest_schema": "INVENTED",
        "symbols": list(book.PINNED_SYMBOL_ORDER), "timeframes": list(book.TIMEFRAME_ORDER),
        "activation_id": "INVENTED_NO_SOURCE_ACCESS", "activation_manifest_sha256": "1" * 64,
        "activation_entry_count": 0, "eligible_symbols": [], "excluded_symbols": [],
        "universe_eligibility_explicit": True, "activation_roster_sha256": "2" * 64,
        "bars_sha256_by_source": {}, "sidecar_sha256_by_source": {},
    }
    contract["native_universe_sha256"] = portfolio.canonical_json_sha256(snapshot)
    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", lambda *args, **kwargs: (snapshot, []))
    issued = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=3)
    monkeypatch.setattr(_DeclaredClock, "value", issued)
    monkeypatch.setattr(orchestration, "datetime", _DeclaredClock)
    monkeypatch.setattr(scheduler, "_declared_utc_now", lambda: issued)
    def forbid_fixed_disk_floor(root):
        pytest.fail("The parallel native route ran a fixed disk precheck")

    monkeypatch.setattr(scheduler, "_declared_disk_free_bytes", forbid_fixed_disk_floor)
    governors = []

    class ObservedGovernor(scheduler.memory_governor.MemoryGovernor):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            governors.append(self)

        def iter_completed(self, worker, tasks, **kwargs):
            if case != "child-capacity":
                yield from super().iter_completed(worker, tasks, **kwargs)
                return
            assert worker is producer.declared_slot_worker
            for task in tasks:
                if kwargs["admission_gate"]():
                    assert kwargs["task_ready"](task)
                    yield scheduler.memory_governor.TaskOutcome(task.key, "COMPLETED", worker(*task.args))
                else:
                    yield scheduler.memory_governor.TaskOutcome(task.key, "NOT_RUN")

    if case == "child-capacity":
        def fail_capacity(*args, **kwargs):
            cause = quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED")
            raise spool.AuthoritySpoolError(str(cause)) from cause

        monkeypatch.setattr(producer, "execute_declared_slot_owner_search", fail_capacity)
    elif case == "live-claim":
        def refuse_live_claim(*args, **kwargs):
            producer._fail("DECLARED_PARALLEL_SLOT_CLAIM_ACTIVE")

        monkeypatch.setattr(producer, "execute_declared_cell_owner_slots", refuse_live_claim)

    monkeypatch.setattr(scheduler.memory_governor, "MemoryGovernor", ObservedGovernor)
    kwargs = {"contract": contract, "requested_workers": 4, "telemetry_path": tmp_path / "telemetry.jsonl"}
    if case in {"expired", "child-capacity"}:
        with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
            scheduler.run_declared_symbol_scheduler(**kwargs)
        report = caught.value.report
    else:
        report = scheduler.run_declared_symbol_scheduler(**kwargs)
    result, = report["cells"]
    expected = {
        "expired": ("TERMINAL_STOP", "NEW_TERMINAL_STOP"),
        "child-capacity": ("FAILED", "DECLARED_PARALLEL_SLOT_RESOURCE_CAPACITY"),
        "live-claim": ("NOT_RUN", "BUSY"),
    }
    assert (result["status"], result["reason_code"]) == expected[case], result
    assert result["account_descriptor"] is None
    governor, = governors
    assert governor.requested_workers == report["worker_limit"] == 4
    if case == "expired":
        assert 1 <= governor.stats.peak_inflight <= 4
        assert result["run_receipt_confirmed"] is True
    else:
        assert governor.stats.peak_inflight == 0 and result["run_receipt"] is None
    assert not (tmp_path / "fence" / "source.json").exists()
    assert not (tmp_path / "data" / "native" / "ACTIVE").exists()


def test_declared_slot_parallel_retirement_includes_the_complete_inventory(tmp_path, monkeypatch):
    import pm_historical_search_producer as producer
    import test_native_regime_unobserved_finalization_001 as retirement_fixture

    (context, row, contract, config, precommit, process, artifact, run, owner_sha, contract_sha) = (
        retirement_fixture._invented_completed_cell(tmp_path, monkeypatch, None))
    row["native_finalization"]["retirement"].update(max_paths=200, max_nodes=1024)
    base = producer._parallel_slot_root(precommit.receipt_sha256, row["cell"]["cell_id"])
    paths = tuple(producer._parallel_slot_path(base, kind, ordinal)
                  for ordinal in range(1, 14) for kind in ("claims", "results"))
    for path in paths:
        spool._publish_exact_json(context, path, {"classification": "INVENTED_PARALLEL_INVENTORY_QA"},
                                  context.limits.max_json_bytes)
    result = scheduler._declared_native_finalize_account(
        {"status": "REOPENED", "run_receipt": run, "account_descriptor": artifact.descriptor},
        context=context, contract=contract, row=row, config=config, precommit=precommit, process=process,
        artifact=artifact, run_body={"invented": True}, workspace_sha=owner_sha, cell_contract_sha256=contract_sha,
        parallel_files=paths)
    assert result["status"] == "ARCHIVED", result
    assert len(result["retained_end_grid_rows"]) == 13
    assert len(set(paths)) == 26 and all(not (context.root / path).exists() for path in paths)


@pytest.mark.parametrize("extra", ["--optimize", "--trade", "--paper", "--mt5-demo", "--worker"])
def test_declared_cli_rejects_ordinary_modes_and_private_worker_argument(tmp_path, monkeypatch, extra):
    _, source, report = _inputs(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("Conflicting CLI arguments reached execution")

    monkeypatch.setattr(main, "_run_declared_rnd_contract", forbidden)
    monkeypatch.setattr(main, "load_config_json", forbidden)
    monkeypatch.setattr(main, "FXPortfolioManagerApp", forbidden)
    assert _cli_exit(["declared-rnd", "--contract", str(source), "--report", str(report), extra]) == 2
    assert not report.exists()


def test_initial_report_io_failure_refuses_before_any_dispatch(tmp_path, monkeypatch):
    contract, source, report = _inputs(tmp_path)
    real_replace = os.replace

    def refuse_report(src, dst):
        if Path(dst).resolve() == report.resolve():
            raise OSError("Labelled report publication failure")
        return real_replace(src, dst)

    def forbidden(*args, **kwargs):
        raise AssertionError("A run started without its durable RUNNING report")

    monkeypatch.setattr(os, "replace", refuse_report)
    monkeypatch.setattr(scheduler, "run_declared_symbol_scheduler", forbidden)
    assert main._run_declared_rnd_contract(source, report, _worker=_launcher_inert_worker) == 3
    assert not report.exists()
    assert not Path(contract["synthetic_board"]).exists()


def test_interrupt_persists_non_authorising_interrupted_report_and_real_default_worker_selection(tmp_path, monkeypatch):
    _, source, report = _inputs(tmp_path)
    calls = []

    def operator_interrupt(*args, **kwargs):
        # This isolated operational interruption is not the child-failure
        # witness above, which uses real spawned failure and result validation.
        calls.append(kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(scheduler, "run_declared_symbol_scheduler", operator_interrupt)
    assert main._run_declared_rnd_contract(source, report) == 130
    body = _report(report, source, "INTERRUPTED")
    assert body["error_code"] == "KEYBOARD_INTERRUPT"
    assert body["dispatch_report"] is None
    assert len(calls) == 1
    assert "worker" not in calls[0]  # Existing scheduler's actual default owns this call.
    assert scheduler.run_declared_symbol_scheduler is operator_interrupt
