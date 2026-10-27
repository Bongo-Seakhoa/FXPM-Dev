"""Labelled synthetic dispatch tests; no broker, market study or qualification.

The spawned worker uses real context storage and the actual declared worker.
Upstream issuance/search/account APIs are finite test doubles, not native proof.
"""
from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pm_historical_scheduler as scheduler
import pm_memory_governor as memory


def _contract(tmp_path, symbols=("AUDUSD", "AUDUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCAD")):
    cells=[]
    for index,symbol in enumerate(symbols):
        cells.append({"root":str(tmp_path/f"cell-{index}"),"run_id":f"synthetic-cell-{index}",
            "cell":{"cell_id":hashlib.sha256(f"{symbol}:{index}".encode()).hexdigest(),
                "symbol":symbol,"timeframe":"H1","regime":"TREND"},
            "launch_activation_utc":"2023-06-24T00:00:00Z",
            "frozen_refit_clock":{"anchor_activation_utc":"2024-06-22T00:00:00Z","interval_calendar_days":28},
            "extend_frozen_clock_backward":True,"protected_end_exclusive_utc":"2024-07-14T08:05:00Z",
            "source_access_window_policy":{"lead_seconds":1,"validity_seconds":3600}})
    return {"schema_version":scheduler.DECLARED_SCHEDULER_SCHEMA,
        "active_config_path":str(tmp_path/"synthetic-config.json"),"native_universe_sha256":"a"*64,
        "config_sha256":"b"*64,"profile_sha256":"c"*64,"cells":cells,
        "synthetic_behaviors":{},"synthetic_board":str(tmp_path/"board")}


def _encoded(body):
    return json.dumps(body,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode("ascii")


class _Receipt:
    def __init__(self,body):
        self.body=body
        self.receipt_sha256=hashlib.sha256(_encoded(body)).hexdigest()
    def to_dict(self):
        return json.loads(_encoded(self.body))


def _precommit_path(context):
    return next((context.root/"declared-slots").glob("*/precommit.json"))


def _synthetic_declared_worker(contract,symbol):
    """All upstream doubles live only here, inside the real spawned child."""
    import pm_authority_spool as spool
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_production_config_binding as binding
    import pm_production_search_planner as planner
    import pm_production_anchor_plan as anchors
    import pm_portfolio_replay as portfolio
    import pm_spread_profile as spread
    import pm_native_prefix_production as native
    import pm_forward_oos_replay_producer as forward
    import pm_causal_evidence_producer as causal
    from pm_core import FeatureComputer
    contexts={}
    milestones=set()
    actual_disk_probe=scheduler._declared_disk_free_bytes
    behavior=contract.get("synthetic_behaviors",{})
    board=Path(contract["synthetic_board"])
    config=SimpleNamespace(data_dir=str(board/"invented-native"))
    frozen=SimpleNamespace(effective_config_sha256=("e" if contract.get("synthetic_effective_config_changed") else "d")*64,materialize=lambda context:config)
    def note(stage):
        if contract.get("synthetic_dispatch_barrier"):
            board.mkdir(parents=True,exist_ok=True)
            (board/f"{os.getpid()}.{stage}").write_text(stage)
    def hold_dispatch(phase):
        if contract.get("synthetic_lease_hold_phase")==phase:
            note("held-"+phase)
            until=time.monotonic()+30
            while not (board/"lease-release").exists() and time.monotonic()<until:
                time.sleep(.02)
            assert (board/"lease-release").exists()
    def derive(**kwargs):
        return {"schema_version":anchors.DECLARED_OWNER_SLOT_ROSTER_V2_SCHEMA,
            "slots":[{"ordinal":i} for i in range(1,14)],
            "synthetic_nonfinancial_bindings":json.loads(_encoded(kwargs))}
    def freeze_process(context,**kwargs):
        if contract.get("synthetic_forbid_process_freeze"):
            raise AssertionError("Saved output reuse tried to publish the process")
        process=_Receipt({"schema_version":"SYNTHETIC_NONFINANCIAL_PROCESS",
            "run_id":kwargs["run_id"],"qualification_authority":False})
        sink=spool.open_symbol_sink(context,planner.PROCESS_SPOOL_SYMBOL)
        sink.put_json(planner.PROCESS_ARTIFACT_ID,planner.PROCESS_ARTIFACT_TYPE,process.to_dict())
        process.spool_receipt=sink.seal()
        return process
    def reopen_process(context,*,spool_receipt,expected_receipt_sha256):
        manifest=spool.verify_symbol_receipt(context.root,spool_receipt,
            expected_generation_id=context.generation_id,expected_symbol=planner.PROCESS_SPOOL_SYMBOL,
            expected_native_universe_sha256=context.native_universe_sha256,expected_config_sha256=context.config_sha256)
        descriptor=manifest["artifacts"][0]["descriptor"]
        process=_Receipt(json.loads((context.root/descriptor["relative_path"]).read_text()))
        assert process.receipt_sha256==expected_receipt_sha256
        process.spool_receipt=spool_receipt
        return process
    def disk_probe(root):
        if contract.get("synthetic_forbid_disk_probe"):
            raise AssertionError("Verified saved output should not probe new-work disk")
        if contract.get("synthetic_child_disk_probe_error"):
            raise OSError("labelled target-volume measurement failure")
        if contract.get("synthetic_disk_low_stage")=="execute" and "native-checked" in milestones:
            return 0
        if contract.get("synthetic_disk_low_stage")=="account" and "slots-finalised" in milestones:
            return 0
        if "synthetic_child_free_bytes" in contract:
            return contract["synthetic_child_free_bytes"]
        return actual_disk_probe(root)
    def native_snapshot(*args):
        milestones.add("native-checked")
        return "MISMATCH" if contract.get("synthetic_native_drift") else "MATCH"
    def capacity_failure():
        import pm_authority_spool_quota as quota
        failure=quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED")
        raise spool.AuthoritySpoolError(str(failure)) from failure
    def issue(context,**kwargs):
        note("issue")
        body={"schema_version":orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2,"run_id":kwargs["run_id"],"search_process_receipt_sha256":kwargs["process"].receipt_sha256,"cells":kwargs["cell_roster"],
            "spread_profile_identity":kwargs["spread_profile_identity"],
            "source_access_window_utc":kwargs["source_access_window_utc"],
            "slot_roster":kwargs["slot_roster"],"synthetic_child_pid":os.getpid(),
            "qualification_authority":False}
        if contract.get("synthetic_issued_edition")=="v1-precommit":
            body["schema_version"]=orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA
        if contract.get("synthetic_issued_edition")=="v1-roster":
            body["slot_roster"]["schema_version"]=anchors.DECLARED_OWNER_SLOT_ROSTER_SCHEMA
        receipt=_Receipt(body)
        spool._publish_exact_json(context,f"declared-slots/{receipt.receipt_sha256}/precommit.json",body,context.limits.max_json_bytes)
        contexts[receipt.receipt_sha256]=context
        return receipt
    def reopen(context,**kwargs):
        note("reopen")
        body=json.loads((context.root/f"declared-slots/{kwargs['receipt_sha256']}/precommit.json").read_text())
        receipt=_Receipt(body)
        contexts[receipt.receipt_sha256]=context
        return receipt
    def run_path(precommit,cell_id):
        return f"declared-slots/{precommit.receipt_sha256}/{cell_id}/run-receipt.json"
    def progress(context,**kwargs):
        note("progress")
        if contract.get("synthetic_forbid_progress"):
            raise AssertionError("Busy cell progress was inspected")
        precommit=kwargs["precommit"]
        path=context.root/run_path(precommit,kwargs["cell_id"])
        body=json.loads(path.read_text()) if path.exists() else None
        return SimpleNamespace(run_receipt=body,terminal_stop=bool(body and body["terminal_stop"]),
            first_uncommitted_slot=None if body else {"ordinal":1})
    def execute(*,context,precommit,process,frozen_pipeline_config,cell_id):
        assert contexts[precommit.receipt_sha256] is context
        note("search")
        if contract.get("synthetic_capacity_stage")=="execute":
            capacity_failure()
        hold_dispatch("search")
        mode=behavior.get(precommit.to_dict()["run_id"],{})
        if contract.get("synthetic_forbid_replay"):
            raise AssertionError("Completed cell was searched again")
        board.mkdir(parents=True,exist_ok=True)
        (board/f"{symbol}.started").write_text(str(os.getpid()))
        if contract.get("synthetic_barrier"):
            until=time.monotonic()+10
            while len(list(board.glob("*.started")))<3 and time.monotonic()<until:
                time.sleep(.02)
            assert len(list(board.glob("*.started")))>=3
        time.sleep(mode.get("delay",0))
        if mode.get("action")=="raise":
            raise RuntimeError("labelled synthetic exception")
        if mode.get("action")=="exit":
            os._exit(19)
        body={"schema_version":"SYNTHETIC_NONFINANCIAL_RUN","terminal_stop":mode.get("action")=="stop",
            "completed_slots":13,"slots":list(range(1,14)),"local_trial_roster_sizes":[300]*13,
            "child_pid":os.getpid(),"qualification_authority":False}
        spool._publish_exact_json(context,run_path(precommit,cell_id),body,context.limits.max_json_bytes)
        milestones.add("slots-finalised")
        return _Receipt(body)
    def discover(context,**kwargs):
        note("discover")
        path=context.root/"synthetic-account-descriptor.json"
        return causal.DeclaredCellRawAccountArtifact(json.loads(path.read_text())) if path.exists() else None
    def write_account(context,**kwargs):
        note("account")
        if contract.get("synthetic_capacity_stage")=="account":
            capacity_failure()
        hold_dispatch("account")
        if contract.get("synthetic_account_unavailable"):
            raise RuntimeError("labelled synthetic account publication failure")
        if contract.get("synthetic_forbid_replay"):
            raise AssertionError("Completed account was simulated again")
        descriptor=spool.write_canonical_json_artifact(context,{"classification":"SYNTHETIC_METADATA_ONLY",
            "cell_id":kwargs["precommit"].to_dict()["cells"][0]["cell_id"],"qualification_authority":False})
        (context.root/"synthetic-account-descriptor.json").write_bytes(_encoded(descriptor))
        return causal.DeclaredCellRawAccountArtifact(descriptor)
    def request(context,**kwargs):
        note("native-request")
        if contract.get("synthetic_forbid_native_request"):
            raise AssertionError("Known lapse reached new native production")
        return {"schema_version":"SYNTHETIC_NATIVE_REQUEST","symbol":kwargs["precommit"].to_dict()["cells"][0]["symbol"],
            "execution_timeframe":"H1","cutoff_utc":"2024-06-22T00:00:00Z",
            "spread_source_timeframe":"M5" if kwargs.get("execution_product") is not None else None,
            "frozen_profile_path":str(board/"profile.json"),"profile_sha256":"c"*64,"production_contract_required":True}
    targets=[(binding,"load_effective_pipeline_config",lambda path:({},config)),
        (binding,"freeze_production_pipeline_config",lambda *args,**kwargs:frozen),
        (planner,"freeze_production_search_process",freeze_process),
        (planner,"reopen_production_search_process",reopen_process),
        (scheduler,"_declared_disk_free_bytes",disk_probe),
        (anchors,"derive_declared_owner_slot_roster_v2",derive),
        (spread,"load_spread_profile",lambda path:SimpleNamespace(sha256="c"*64)),
        (orchestration,"precommit_declared_slot_owner_run_v2",issue),
        (orchestration,"reopen_declared_slot_owner_run_precommit",reopen),
        (orchestration,"_declared_slot_effective_access",lambda context,precommit,parent,slot:({"start_utc":"2000-01-01T00:00:00Z","end_exclusive_utc":"2001-01-01T00:00:00Z"} if contract.get("synthetic_expired_window") else parent["source_access_window_utc"],None)),
        (orchestration,"_declared_slot_native_snapshot_status",native_snapshot),
        (portfolio,"active_native_portfolio_contract",lambda *args,**kwargs:({},[])),
        (producer,"reopen_declared_cell_owner_progress",progress),
        (producer,"execute_declared_cell_owner_slots",execute),
        (forward,"discover_declared_cell_account_artifact",discover),
        (forward,"declared_cell_account_native_request",request),
        (forward,"produce_declared_cell_account_artifact",write_account),
        (native,"produce_activated_native_prefix",lambda **kwargs:SimpleNamespace()),
        (native,"reverify_activated_native_prefix_product",lambda product,*args,**kwargs:product),
        (FeatureComputer,"clear_cache",lambda:None)]
    with ExitStack() as stack:
        for owner,name,replacement in targets:
            stack.enter_context(patch.object(owner,name,replacement))
        if contract.get("synthetic_dispatch_barrier"):
            note("arrived")
            until=time.monotonic()+30
            while len(list(board.glob("*.arrived")))<2 and time.monotonic()<until:
                time.sleep(.02)
            assert len(list(board.glob("*.arrived")))==2
        return scheduler.declared_symbol_worker(contract,symbol)


def test_real_entry_drains_three_children_retains_healthy_same_symbol_cells_and_not_run(tmp_path):
    contract=_contract(tmp_path)
    contract["synthetic_barrier"]=True
    contract["synthetic_behaviors"]={"synthetic-cell-0":{"delay":.6},
        "synthetic-cell-2":{"action":"raise","delay":.15},
        "synthetic-cell-3":{"action":"exit","delay":.25}}
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
            requested_workers=3,telemetry_path=tmp_path/"telemetry.jsonl")
    report=caught.value.report
    assert [r["descriptor_index"] for r in report["cells"]]==list(range(6))
    assert [r["status"] for r in report["cells"]]==["COMPLETED","COMPLETED","FAILED","UNRESOLVED_CRASH","NOT_RUN","NOT_RUN"]
    assert all(r["run_receipt_confirmed"] for r in report["cells"][:2])
    assert report["cells"][0]["worker_pid"]==report["cells"][1]["worker_pid"]
    assert len({int(p.read_text()) for p in (tmp_path/"board").glob("*.started")})==3
    assert not Path(contract["cells"][4]["root"]).exists()
    assert not Path(contract["cells"][5]["root"]).exists()
    assert all(r["worker_pid"] is None for r in report["cells"][4:])
    assert (tmp_path/"telemetry.jsonl").stat().st_size<=1024**2
    for row in report["cells"]:
        assert row["qualification_authority"] is row["live_authority"] is row["order_authority"] is False


def test_completed_cells_reopen_without_executor_or_account_writer(tmp_path):
    contract=_contract(tmp_path,("AUDUSD","AUDUSD"))
    first=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"telemetry.jsonl")
    assert [r["status"] for r in first["cells"]]==["COMPLETED","COMPLETED"]
    contract["synthetic_forbid_replay"]=True
    second=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"reopen.jsonl")
    assert [r["status"] for r in second["cells"]]==["REOPENED","REOPENED"]
    assert [r["account_descriptor"] for r in second["cells"]]==[r["account_descriptor"] for r in first["cells"]]
    assert all(r["run_receipt_confirmed"] for r in second["cells"])


def test_new_stop_closes_gate_but_recorded_stop_does_not(tmp_path):
    contract=_contract(tmp_path,("AUDUSD","EURUSD"))
    contract["synthetic_behaviors"]={"synthetic-cell-0":{"action":"stop"}}
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
            requested_workers=1,telemetry_path=tmp_path/"first.jsonl")
    assert [r["status"] for r in caught.value.report["cells"]]==["TERMINAL_STOP","NOT_RUN"]
    resumed=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"second.jsonl")
    assert [r["status"] for r in resumed["cells"]]==["TERMINAL_STOP","COMPLETED"]
    assert resumed["cells"][0]["recorded_before_dispatch"] is True


def test_native_drift_is_not_run_without_consuming_slots(tmp_path):
    contract=_contract(tmp_path,("AUDUSD",));contract["synthetic_native_drift"]=True
    observed=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"drift.jsonl")
    assert observed["cells"][0]["status"]=="NOT_RUN"
    assert observed["cells"][0]["reason_code"]=="NATIVE_DRIFT"
    assert not list(Path(contract["cells"][0]["root"]).rglob("run.lock"))
    assert not (tmp_path/"board").exists()


def test_explicit_window_and_disjoint_cell_namespaces_are_required_before_dispatch(tmp_path):
    contract=_contract(tmp_path,("AUDUSD",))
    del contract["cells"][0]["source_access_window_policy"]
    with pytest.raises(scheduler.HistoricalSchedulerError,match="WINDOW_POLICY_REQUIRED"):
        scheduler.run_declared_symbol_scheduler(contract=contract,requested_workers=1,telemetry_path=tmp_path/"x.jsonl")
    assert not Path(contract["cells"][0]["root"]).exists()
    contract=_contract(tmp_path,("AUDUSD","AUDUSD"));contract["cells"][1]["root"]=contract["cells"][0]["root"]
    with pytest.raises(scheduler.HistoricalSchedulerError,match="NAMESPACE_COLLISION"):
        scheduler.run_declared_symbol_scheduler(contract=contract,requested_workers=1,telemetry_path=tmp_path/"x.jsonl")


def test_parent_result_validation_drains_before_raising(tmp_path,monkeypatch):
    contract=_contract(tmp_path,("AUDUSD","EURUSD"));events=[]
    class Governor:
        resource_stats={}
        def __init__(self,**kwargs): pass
        def iter_completed(self,worker,tasks,**kwargs):
            queued=list(tasks)
            yield memory.TaskOutcome(task_key=queued[0].key,status="COMPLETED",value={})
            events.append("continued-after-invalid-result")
            assert kwargs["admission_gate"]() is False
            yield memory.TaskOutcome(task_key=queued[1].key,status="NOT_RUN")
            events.append("all-drained")
    monkeypatch.setattr(memory,"MemoryGovernor",Governor)
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(contract=contract,telemetry_path=tmp_path/"x.jsonl")
    assert events==["continued-after-invalid-result","all-drained"]
    assert [r["status"] for r in caught.value.report["cells"]]==["UNRESOLVED_CRASH","NOT_RUN"]


def test_declared_default_cpu_recommendation_and_explicit_positive_request(tmp_path,monkeypatch):
    monkeypatch.setattr(memory,"effective_cpu_count",lambda:6)
    assert memory.recommended_worker_count(0,100)==5
    assert memory.recommended_worker_count(6,100)==6
    assert memory.recommended_worker_count(6,2)==2



def test_expired_existing_precommit_is_not_run_without_new_slots_or_run_lock(tmp_path):
    contract=_contract(tmp_path,("AUDUSD",));contract["synthetic_native_drift"]=True
    scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"seed.jsonl")
    root=Path(contract["cells"][0]["root"]);precommit=next(root.glob("declared-slots/*/precommit.json"));before=precommit.read_bytes()
    contract.update(synthetic_native_drift=False,synthetic_expired_window=True,synthetic_forbid_replay=True)
    observed=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"expired.jsonl")
    assert observed["cells"][0]["status"]=="NOT_RUN"
    assert observed["cells"][0]["reason_code"]=="WINDOW_EXPIRED"
    assert precommit.read_bytes()==before
    assert not list(root.rglob("run.lock"))
    assert not (tmp_path/"board").exists()


def test_existing_os_locked_cell_is_busy_and_does_not_read_progress(tmp_path):
    import pm_native_storage as storage
    contract=_contract(tmp_path,("AUDUSD",));contract["synthetic_native_drift"]=True
    scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"seed.jsonl")
    root=Path(contract["cells"][0]["root"]);p1=next(root.glob("declared-slots/*/precommit.json"))
    lock=p1.parent/contract["cells"][0]["cell"]["cell_id"]/"run.lock";lock.parent.mkdir(parents=True);lock.write_bytes(b"0")
    contract["synthetic_forbid_progress"]=True
    with lock.open("r+b",buffering=0) as handle:
        assert storage._try_kernel_lock(handle) is True
        try:
            observed=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
                requested_workers=1,telemetry_path=tmp_path/"busy.jsonl")
            dispatch_lock=root/"declared-scheduler.lock"
            assert dispatch_lock.stat().st_size==1
            with dispatch_lock.open("r+b",buffering=0) as dispatch_handle:
                assert storage._try_kernel_lock(dispatch_handle) is True
                storage._release_kernel_lock(dispatch_handle)
        finally:
            storage._release_kernel_lock(handle)
    assert observed["status"]=="COMPLETED"
    assert observed["cells"][0]["status"]=="NOT_RUN"
    assert observed["cells"][0]["reason_code"]=="BUSY"
    assert not list(root.rglob("run-receipt.json"))


def test_malformed_completed_domain_row_closes_admission_before_refill(tmp_path,monkeypatch):
    contract=_contract(tmp_path,("AUDUSD","EURUSD"));events=[]
    class Governor:
        resource_stats={}
        def __init__(self,**kwargs): pass
        def iter_completed(self,worker,tasks,**kwargs):
            queued=list(tasks);cell=queued[0].args[0]["cells"][0]
            row=scheduler._declared_cell_row(cell,"COMPLETED",pid=12345)
            # Invalid successful row: no bound account artifact or run receipt.
            value={"schema_version":scheduler.DECLARED_SCHEDULER_SCHEMA,"symbol":"AUDUSD","cells":[row],"closes_admission":False}
            yield memory.TaskOutcome(task_key=queued[0].key,status="COMPLETED",value=value,worker_pid=12345)
            assert kwargs["admission_gate"]() is False
            events.append("invalid-domain-row-closed-gate")
            yield memory.TaskOutcome(task_key=queued[1].key,status="NOT_RUN")
            events.append("drained")
    monkeypatch.setattr(memory,"MemoryGovernor",Governor)
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(contract=contract,telemetry_path=tmp_path/"invalid.jsonl")
    assert events==["invalid-domain-row-closed-gate","drained"]
    assert [r["status"] for r in caught.value.report["cells"]]==["UNRESOLVED_CRASH","NOT_RUN"]



@pytest.mark.parametrize("reason",["WINDOW_EXPIRED","NATIVE_DRIFT"])
def test_finalized_slots_without_account_precheck_lapse_before_new_native_production(tmp_path,reason):
    contract=_contract(tmp_path,("AUDUSD",));contract["synthetic_account_unavailable"]=True
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
            requested_workers=1,telemetry_path=tmp_path/"incomplete-account.jsonl")
    assert caught.value.report["cells"][0]["status"]=="FAILED"
    root=Path(contract["cells"][0]["root"]);run=next(root.rglob("run-receipt.json"));before=run.read_bytes()
    assert not (root/"synthetic-account-descriptor.json").exists()
    contract.update(synthetic_account_unavailable=False,synthetic_forbid_native_request=True,
        synthetic_forbid_replay=True,synthetic_expired_window=reason=="WINDOW_EXPIRED",
        synthetic_native_drift=reason=="NATIVE_DRIFT")
    observed=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"continue-account.jsonl")
    assert observed["cells"][0]["status"]=="NOT_RUN"
    assert observed["cells"][0]["reason_code"]==reason
    assert observed["cells"][0]["run_receipt_confirmed"] is True
    assert run.read_bytes()==before
    assert not list(root.rglob("run.lock"))



@pytest.mark.parametrize("changed",["launch","clock","backward","protected-end","effective-config"])
def test_reopened_precommit_must_match_requested_roster_before_progress_or_consumption(tmp_path,changed):
    contract=_contract(tmp_path,("AUDUSD",));contract["synthetic_native_drift"]=True
    scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"seed-binding.jsonl")
    root=Path(contract["cells"][0]["root"]);precommit=next(root.glob("declared-slots/*/precommit.json"));before=precommit.read_bytes()
    row=contract["cells"][0]
    if changed=="launch": row["launch_activation_utc"]="2023-07-22T00:00:00Z"
    elif changed=="clock": row["frozen_refit_clock"]["anchor_activation_utc"]="2024-07-20T00:00:00Z"
    elif changed=="backward": row["extend_frozen_clock_backward"]=False
    elif changed=="protected-end": row["protected_end_exclusive_utc"]="2024-07-15T08:05:00Z"
    else: contract["synthetic_effective_config_changed"]=True
    contract.update(synthetic_forbid_progress=True,synthetic_forbid_native_request=True,synthetic_forbid_replay=True)
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
            requested_workers=1,telemetry_path=tmp_path/"mismatch.jsonl")
    assert caught.value.report["cells"][0]["status"]=="FAILED"
    assert caught.value.report["cells"][0]["reason_code"]=="DECLARED_SLOT_ROSTER_MISMATCH"
    assert precommit.read_bytes()==before
    assert not list(root.rglob("run.lock"))
    assert not list(root.rglob("run-receipt.json"))
    assert not (tmp_path/"board").exists()


@pytest.mark.parametrize("edition",["v1-precommit","v1-roster"])
def test_v2_required_on_issuance_and_reopen_before_progress(tmp_path,edition):
    contract=_contract(tmp_path,("AUDUSD",));contract.update(synthetic_issued_edition=edition,
        synthetic_forbid_progress=True,synthetic_forbid_native_request=True,synthetic_forbid_replay=True)
    before=None
    for attempt in range(2):
        with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
            scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
                requested_workers=1,telemetry_path=tmp_path/f"v2-{attempt}.jsonl")
        assert caught.value.report["cells"][0]["status"]=="FAILED"
        assert caught.value.report["cells"][0]["reason_code"]=="DECLARED_SCHEDULER_PRECOMMIT_V2_REQUIRED"
        root=Path(contract["cells"][0]["root"]);precommit=next(root.glob("declared-slots/*/precommit.json"))
        if before is None: before=precommit.read_bytes()
        else: assert precommit.read_bytes()==before
    assert not list(root.rglob("run.lock"))
    assert not list(root.rglob("run-receipt.json"))
    assert not (tmp_path/"board").exists()


@pytest.mark.parametrize("phase",["search","account"])
def test_real_concurrent_scheduler_dispatch_lease_excludes_duplicate_work_through_account(tmp_path,phase):
    from concurrent.futures import FIRST_COMPLETED,ThreadPoolExecutor,wait
    import pm_authority_spool as spool
    contract=_contract(tmp_path,("AUDUSD",));row=contract["cells"][0]
    # Only real context metadata is seeded; the scheduler lease does not yet exist.
    spool.freeze_generation_context(Path(row["root"]),generation_id=row["run_id"],
        native_universe_sha256=contract["native_universe_sha256"],config_sha256=contract["config_sha256"],
        limits=spool.SpoolLimits(min_free_bytes=scheduler.DECLARED_CELL_MIN_FREE_BYTES))
    contract.update(synthetic_dispatch_barrier=True,synthetic_lease_hold_phase=phase)
    board=Path(contract["synthetic_board"])
    def dispatch(index):
        return scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
            requested_workers=1,telemetry_path=tmp_path/f"concurrent-{index}.jsonl")
    with ThreadPoolExecutor(max_workers=2) as callers:
        futures=[callers.submit(dispatch,index) for index in range(2)]
        try:
            done,pending=wait(futures,timeout=30,return_when=FIRST_COMPLETED)
            assert len(done)==1
            busy=next(iter(done)).result()
            assert busy["cells"][0]["status"]=="NOT_RUN"
            assert busy["cells"][0]["reason_code"]=="BUSY"
            busy_pid=busy["cells"][0]["worker_pid"]
            assert {p.suffix for p in board.glob(f"{busy_pid}.*")}=={".arrived"}
            assert busy["cells"][0]["precommit_sha256"] is None
            until=time.monotonic()+30
            while not list(board.glob("*.held-"+phase)) and time.monotonic()<until:
                time.sleep(.02)
            assert len(list(board.glob("*.held-"+phase)))==1
            assert len(list(Path(row["root"]).glob("declared-slots/*/precommit.json")))==1
        finally:
            board.mkdir(parents=True,exist_ok=True)
            (board/"lease-release").write_text("release synthetic metadata-only worker")
        complete=next(iter(pending)).result(timeout=30)
    assert complete["cells"][0]["status"]=="COMPLETED"
    assert complete["cells"][0]["run_receipt_confirmed"] is True
    assert len(list(Path(row["root"]).glob("declared-slots/*/precommit.json")))==1
    assert len(list(board.glob("*.search")))==1
    assert len(list(board.glob("*.account")))==1
    assert len(list(board.glob("*.held-"+phase)))==1
    assert (Path(row["root"])/"declared-scheduler.lock").stat().st_size==1


def test_dispatch_lease_released_after_exception_in_same_live_process(tmp_path):
    import pm_native_storage as storage
    contract,_=scheduler._declared_contract(_contract(tmp_path,("AUDUSD",)))
    contract["synthetic_behaviors"]={"synthetic-cell-0":{"action":"raise"}}
    failed=_synthetic_declared_worker(contract,"AUDUSD")
    assert failed["cells"][0]["status"]=="FAILED"
    lock=Path(contract["cells"][0]["root"])/"declared-scheduler.lock"
    assert lock.stat().st_size==1
    with lock.open("r+b",buffering=0) as handle:
        assert storage._try_kernel_lock(handle) is True
        storage._release_kernel_lock(handle)
    contract["synthetic_behaviors"]={}
    complete=_synthetic_declared_worker(contract,"AUDUSD")
    assert complete["cells"][0]["status"]=="COMPLETED"
    assert lock.stat().st_size==1



def test_parent_disk_low_defers_without_launching_or_creating_cells(tmp_path,monkeypatch):
    contract=_contract(tmp_path,("AUDUSD","EURUSD"));probes=[]
    def low(root): probes.append(Path(root));return 0
    def forbidden(*args,**kwargs): raise AssertionError("Parent DISK_LOW launched a worker")
    monkeypatch.setattr(scheduler,"_declared_disk_free_bytes",low)
    monkeypatch.setattr(memory,"_launch_task_process",forbidden)
    result=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=2,telemetry_path=tmp_path/"low-parent.jsonl")
    assert result["status"]=="COMPLETED"
    assert result["admission_deferral_reason"]=="DISK_LOW"
    assert [(r["status"],r["reason_code"],r["worker_pid"]) for r in result["cells"]]==[("NOT_RUN","DISK_LOW",None)]*2
    assert probes and all(path in [Path(r["root"]) for r in contract["cells"]] for path in probes)
    assert all(not Path(row["root"]).exists() for row in contract["cells"])


def test_parent_disk_deferral_drains_actual_active_job_without_refill(tmp_path,monkeypatch):
    contract=_contract(tmp_path,("AUDUSD","EURUSD","GBPUSD"));started=[];launch=memory._launch_task_process
    def actual_launch(*args,**kwargs):
        pair=launch(*args,**kwargs);started.append(pair[0].pid);return pair
    monkeypatch.setattr(memory,"_launch_task_process",actual_launch)
    monkeypatch.setattr(scheduler,"_declared_disk_free_bytes",lambda root:0 if started else 2**60)
    result=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=2,telemetry_path=tmp_path/"active-drain.jsonl")
    assert len(started)==1
    assert [row["status"] for row in result["cells"]]==["COMPLETED","NOT_RUN","NOT_RUN"]
    assert result["cells"][0]["run_receipt_confirmed"] is True
    assert all(row["reason_code"]=="DISK_LOW" and row["worker_pid"] is None for row in result["cells"][1:])
    assert all(not Path(row["root"]).exists() for row in contract["cells"][1:])


@pytest.mark.parametrize("stage",["new-context","execute","account"])
def test_child_disk_check_preserves_unconsumed_or_finalised_state(tmp_path,stage):
    contract=_contract(tmp_path,("AUDUSD","AUDUSD"))
    if stage=="new-context": contract["synthetic_child_free_bytes"]=0
    else: contract["synthetic_disk_low_stage"]=stage
    result=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"child-low.jsonl")
    assert [(r["status"],r["reason_code"]) for r in result["cells"]]==[("NOT_RUN","DISK_LOW")]*2
    assert not Path(contract["cells"][1]["root"]).exists()
    root=Path(contract["cells"][0]["root"])
    if stage=="new-context": assert not root.exists()
    elif stage=="execute": assert not list(root.rglob("run-receipt.json"))
    else:
        assert len(list(root.rglob("run-receipt.json")))==1
        assert result["cells"][0]["run_receipt_confirmed"] is True
        assert not (root/"synthetic-account-descriptor.json").exists()


@pytest.mark.parametrize("stage",["execute","account"])
def test_propagated_capacity_is_disk_deferral_with_same_reason_for_later_cells(tmp_path,stage):
    contract=_contract(tmp_path,("AUDUSD","AUDUSD"));contract["synthetic_capacity_stage"]=stage
    result=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        requested_workers=1,telemetry_path=tmp_path/"propagated.jsonl")
    assert result["status"]=="COMPLETED"
    assert result["admission_deferral_reason"]=="DISK_LOW"
    assert [(r["status"],r["reason_code"]) for r in result["cells"]]==[("NOT_RUN","DISK_LOW")]*2
    assert not Path(contract["cells"][1]["root"]).exists()
    assert not (Path(contract["cells"][0]["root"])/"synthetic-account-descriptor.json").exists()


@pytest.mark.parametrize("terminal_stop",[False,True])
def test_verified_saved_output_reuse_is_read_only_before_disk_block(tmp_path,terminal_stop):
    contract,_=scheduler._declared_contract(_contract(tmp_path,("AUDUSD",)))
    if terminal_stop: contract["synthetic_behaviors"]={"synthetic-cell-0":{"action":"stop"}}
    first=_synthetic_declared_worker(contract,"AUDUSD")
    root=Path(contract["cells"][0]["root"])
    before={str(p.relative_to(root)):p.read_bytes() for p in root.rglob("*") if p.is_file()}
    contract.update(synthetic_forbid_disk_probe=True,synthetic_forbid_process_freeze=True,
        synthetic_forbid_replay=True,synthetic_forbid_native_request=True)
    second=_synthetic_declared_worker(contract,"AUDUSD")
    assert second["cells"][0]["status"]==("TERMINAL_STOP" if terminal_stop else "REOPENED")
    assert second["cells"][0]["recorded_before_dispatch"] is True
    assert {str(p.relative_to(root)):p.read_bytes() for p in root.rglob("*") if p.is_file()}==before
    assert first["cells"][0]["run_receipt"]==second["cells"][0]["run_receipt"]


def test_disk_threshold_never_waives_verified_context_reserve_and_headroom(tmp_path):
    import pm_authority_spool as spool
    contract=_contract(tmp_path,("AUDUSD",));row=contract["cells"][0]
    reserve=2*1024**3
    spool.freeze_generation_context(Path(row["root"]),generation_id=row["run_id"],
        native_universe_sha256=contract["native_universe_sha256"],config_sha256=contract["config_sha256"],
        limits=spool.SpoolLimits(min_free_bytes=reserve))
    contract["synthetic_child_free_bytes"]=reserve-1
    result=scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
        disk_min_free_bytes=0,disk_headroom_bytes=1,requested_workers=1,telemetry_path=tmp_path/"reserve.jsonl")
    assert result["cells"][0]["reason_code"]=="DISK_LOW"
    assert not list(Path(row["root"]).glob("declared-slots/*/precommit.json"))
    assert scheduler._declared_disk_requirement({"scheduler_disk_min_free_bytes":1,
        "scheduler_disk_headroom_bytes":5},reserve)==reserve+5


def test_parent_disk_probe_error_remains_technical(tmp_path,monkeypatch):
    contract=_contract(tmp_path,("AUDUSD",))
    def probe(root): raise OSError("labelled unavailable target volume")
    monkeypatch.setattr(scheduler,"_declared_disk_free_bytes",probe)
    with pytest.raises(scheduler.DeclaredSymbolSchedulerError) as caught:
        scheduler.run_declared_symbol_scheduler(_synthetic_declared_worker,contract=contract,
            requested_workers=1,telemetry_path=tmp_path/"probe-error.jsonl")
    assert caught.value.report["status"]=="FAILED"
    assert caught.value.report["cells"][0]["reason_code"]=="DISK_PROBE_ERROR"
    assert not Path(contract["cells"][0]["root"]).exists()


def test_probe_uses_nearest_existing_target_ancestor_without_creating_root(tmp_path,monkeypatch):
    import shutil
    target=tmp_path/"uncreated"/"cell";calls=[]
    monkeypatch.setattr(shutil,"disk_usage",lambda path:(calls.append(Path(path)) or SimpleNamespace(free=12345)))
    assert scheduler._declared_disk_free_bytes(target)==12345
    assert calls==[tmp_path]
    assert not target.exists()
