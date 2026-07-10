import json
from datetime import datetime, timedelta

from tools.live_silent_watchdog import check_live_health


def test_live_silent_watchdog_flags_stale_sweep(tmp_path):
    now = datetime(2026, 6, 7, 12, 0, 0)
    storage = tmp_path / "storage_state.json"
    actionable = tmp_path / "last_actionable_log.json"
    trade = tmp_path / "last_trade_log.json"
    storage.write_text(
        json.dumps({"last_sweep": {"at": (now - timedelta(minutes=10)).isoformat()}}),
        encoding="utf-8",
    )
    actionable.write_text(json.dumps({"EURUSD": {"action_time": now.isoformat()}}), encoding="utf-8")
    trade.write_text(json.dumps({"EURUSD": {"action_time": now.isoformat()}}), encoding="utf-8")

    report = check_live_health(
        storage_state=storage,
        actionable_log=actionable,
        trade_log=trade,
        max_sweep_age_seconds=180,
        now=now,
    )

    assert report["status"] == "CRITICAL"
    assert any(item["code"] == "last_sweep_stale" for item in report["findings"])


def test_live_silent_watchdog_ok_when_artifacts_fresh(tmp_path):
    now = datetime(2026, 6, 7, 12, 0, 0)
    storage = tmp_path / "storage_state.json"
    actionable = tmp_path / "last_actionable_log.json"
    trade = tmp_path / "last_trade_log.json"
    storage.write_text(
        json.dumps({"last_sweep": {"at": (now - timedelta(seconds=30)).isoformat()}}),
        encoding="utf-8",
    )
    actionable.write_text(json.dumps({"EURUSD": {"action_time": now.isoformat()}}), encoding="utf-8")
    trade.write_text(json.dumps({"EURUSD": {"action_time": now.isoformat()}}), encoding="utf-8")

    report = check_live_health(
        storage_state=storage,
        actionable_log=actionable,
        trade_log=trade,
        max_sweep_age_seconds=180,
        now=now,
    )

    assert report["status"] == "OK"
    assert report["findings"] == []
