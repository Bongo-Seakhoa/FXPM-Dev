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


def _launcher_inert_worker(contract, symbol):
    """Spawn-safe timing envelope; all domain work stays in the existing fixture."""
    board = Path(contract["synthetic_board"])
    board.mkdir(parents=True, exist_ok=True)
    start = {"pid": os.getpid(), "symbol": symbol, "at_ns": time.monotonic_ns()}
    (board / f"{symbol}.launcher-start.json").write_text(json.dumps(start), encoding="utf-8")
    # A batch barrier would deadlock: AUDUSD releases only after a freed slot
    # has admitted GBPUSD while AUDUSD is still alive.
    if contract.get("launcher_hold_first_until_refill") and symbol == "AUDUSD":
        deadline = time.monotonic() + 30
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
    deadline = time.monotonic() + 30
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
        deadline = time.monotonic() + 30
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
