"""Shared readiness-source checks; invented invalid inputs grant no authority.

The historical comparison imports a pinned archived, nonproduction test asset
in a separate ordinary Python process. Production imports never search that
fixture directory; it is used only for this original/current compatibility check.
"""

import builtins
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_live_readiness as readiness


assert Path(readiness.__file__).resolve() == PRODUCT_ROOT / "pm_live_readiness.py"
BASELINE = REPO / "tests/fixtures/readiness_accessor_baseline/pm_live_readiness.py"
BASELINE_SHA256 = "1e394473073557e97c49785f01638aad6577cf42830fbe264b9047ae7a934399"


def test_interval_policy_accessor_uses_no_config_ledger_filesystem_or_funded_validation(monkeypatch):
    calls = []

    def forbidden(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError("The interval-policy accessor must perform no IO or funded validation")

    with monkeypatch.context() as isolated:
        isolated.setattr(builtins, "open", forbidden)
        for name in ("open", "stat", "read_bytes", "read_text"):
            isolated.setattr(Path, name, forbidden)
        isolated.setattr(readiness, "validate_live_readiness", forbidden)
        isolated.setattr(readiness.json, "load", forbidden)
        isolated.setattr(readiness.json, "loads", forbidden)
        isolated.setattr(readiness.hashlib, "sha256", forbidden)
        isolated.setattr(readiness.shutil, "disk_usage", forbidden)
        finding = readiness.interval_cell_qualification_readiness_finding()
    assert calls == []
    assert type(finding) is tuple and finding == (
        "interval_cell_qualification_policy_unadopted",
        "new entries require the adopted interval cell qualification policy; "
        "the previous multiplier cost evidence is retired",
    )


_FULL_FUNDED_REPORT = r'''
import dataclasses, json, pathlib, sys
from types import SimpleNamespace
origin, repo, work = (pathlib.Path(value).resolve() for value in sys.argv[1:])
sys.path[:0] = [str(origin), str(repo)]
import pm_live_readiness as source
assert pathlib.Path(source.__file__).resolve() == origin / "pm_live_readiness.py"
config = SimpleNamespace(data_dir=work / "empty-data", log_dir=work / "logs",
    broker_specs_path=str(work / "absent-broker-specs.json"),
    regime_params_file=str(work / "absent-regime-params.json"),
    storage_pause_entries_below_free_gb=0, hard_block_reopen_mode="operator",
    live_min_account_equity=2000)
report = source.validate_live_readiness(config, symbols=["EURUSD"],
    config_path=str(work / "synthetic-config.json"),
    winner_ledger_path=str(work / "absent-winner-ledger.json"),
    mt5_config=SimpleNamespace(expected_login=700, expected_server="SYNTHETIC-PIN", expected_currency="USD"),
    account_info=SimpleNamespace(login=701, server="SYNTHETIC-OBSERVED", currency="EUR", equity=100),
    position_config=SimpleNamespace(risk_per_trade_pct=0.5, max_risk_pct=2))
rows = lambda values: [dataclasses.asdict(value) for value in values]
print(json.dumps({"source": str(pathlib.Path(source.__file__).resolve()), "report": {
    "ok": report.ok, "findings": rows(report.findings), "errors": rows(report.errors),
    "warnings": rows(report.warnings), "evidence_errors": rows(report.evidence_errors),
    "runtime_errors": rows(report.runtime_errors)}}, sort_keys=True))
'''


def test_funded_finding_order_and_scope_partitions_match_the_retained_original(tmp_path):
    """Whole before/after reports share the same invented, explicitly invalid inputs."""
    assert BASELINE.is_file(), "The required retained compatibility baseline is missing"
    assert hashlib.sha256(BASELINE.read_bytes()).hexdigest() == BASELINE_SHA256
    (tmp_path / "empty-data").mkdir()
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "institutional_supervisor_state.json").write_text(
        json.dumps({"active_block": True}), encoding="utf-8")
    (tmp_path / "synthetic-config.json").write_text(json.dumps({
        "classification": "SYNTHETIC_INVALID_READINESS_COMPATIBILITY_INPUT",
        "pipeline": {"broker_specs_path": str(tmp_path / "absent-broker-specs.json")},
    }), encoding="utf-8")
    packets = []
    for origin in (BASELINE.parent, PRODUCT_ROOT):
        result = subprocess.run([sys.executable, "-B", "-c", _FULL_FUNDED_REPORT,
            str(origin), str(REPO), str(tmp_path)], cwd=tmp_path, capture_output=True,
            text=True, encoding="utf-8", timeout=60, check=False)
        assert result.returncode == 0, result.stderr
        packet = json.loads(result.stdout)
        assert Path(packet["source"]).resolve() == origin / "pm_live_readiness.py"
        packets.append(packet["report"])
    before, after = packets
    assert after == before  # Exact full list, messages, order and every partition.
    assert after["ok"] is False and after["warnings"]
    assert after["evidence_errors"] and after["runtime_errors"]
    errors = after["errors"]
    interval = [row for row in errors if row["code"] == "interval_cell_qualification_policy_unadopted"]
    assert len(interval) == 1 and interval[0]["scope"] == "evidence"
    assert interval[0] in after["evidence_errors"] and interval[0] not in after["runtime_errors"]
    assert not (tmp_path / "absent-winner-ledger.json").exists()
