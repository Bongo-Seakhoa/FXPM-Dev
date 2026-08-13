from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "fxpm_pm_dashboard", ROOT / "tools" / "pm_dashboard.py"
)
assert SPEC is not None and SPEC.loader is not None
dashboard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dashboard)


def _message(
    message_id: str,
    sender: str,
    target: str,
    kind: str,
    *,
    refs: list[str] | None = None,
    ts: str = "2026-08-13T10:00:00Z",
) -> dict:
    return {
        "id": message_id,
        "from": sender,
        "to": target,
        "type": kind,
        "refs": refs or [],
        "ts": ts,
        "body": message_id,
    }


def test_open_requests_require_a_directionally_valid_peer_response() -> None:
    bus = [
        _message("CX-900", "codex", "claude", "review_request"),
        _message("CX-901", "codex", "claude", "status", refs=["CX-900"]),
        _message("CC-900", "claude", "codex", "review", refs=["CX-900"]),
        _message("CPM-900", "claude-pm", "codex-pm", "decision"),
        _message("XPM-900", "codex-pm", "claude-pm", "question"),
    ]

    assert [row["id"] for row in dashboard.open_peer_requests(bus)] == ["XPM-900"]
    assert dashboard.message_resolutions(bus)["CX-900"]["status"] == "answered"


def test_plain_decisions_and_non_pm_requests_are_not_operator_queue_items() -> None:
    bus = [
        _message("CPM-012", "claude-pm", "codex-pm", "decision"),
        _message("CX-022", "codex", "claude", "review_request"),
    ]

    assert dashboard.open_peer_requests(bus) == []


def test_open_peer_request_count_is_not_truncated() -> None:
    bus = [
        _message(
            f"XPM-{900 + index}",
            "codex-pm",
            "claude-pm",
            "question",
            ts=f"2026-08-13T10:{index:02d}:00Z",
        )
        for index in range(12)
    ]

    assert len(dashboard.open_peer_requests(bus)) == 12


def test_source_health_rejects_valid_json_with_wrong_schema(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "agent_comms").mkdir()
    (tmp_path / "docs").mkdir()
    (tmp_path / "agent_comms" / "STATE.md").write_text("state", encoding="utf-8")
    (tmp_path / "docs" / "RECONCILIATION_REGISTER.md").write_text(
        "register", encoding="utf-8"
    )
    (tmp_path / "agent_comms" / "LANES.json").write_text(
        '{"lanes":"not-a-list"}', encoding="utf-8"
    )
    (tmp_path / "agent_comms" / "BUS.jsonl").write_text(
        '["valid-json","wrong-shape"]\n', encoding="utf-8"
    )
    monkeypatch.setattr(dashboard, "ROOT", tmp_path)

    states = {row["name"]: row["state"] for row in dashboard.source_health()}

    assert states["agent_comms/LANES.json"] == "invalid"
    assert states["agent_comms/BUS.jsonl"] == "invalid"
    assert dashboard.service_snapshot()["status"] == "degraded"


def test_current_bus_does_not_resurrect_known_closed_or_plain_decision_items() -> None:
    open_ids = {row["id"] for row in dashboard.open_peer_requests(dashboard.load_bus())}

    assert not {"CPM-012", "CX-022", "CX-032", "CX-033"} & open_ids


def test_render_removes_withdrawn_runtime_arithmetic() -> None:
    page = dashboard.render(live=False)

    assert "576,000,000" not in page
    assert "200 Optuna trials" not in page
    assert "Every 100 ms" not in page
    assert "11.52M Backtester.run calls" in page
    assert "complete multi-grid generation total and duration are not yet established" in page
    assert "2.62 s" in page
    assert "61.6%" in page
    assert "http-equiv='refresh'" not in page
    assert "Decisions waiting on you" not in page
    assert "they need your judgement" not in page


def test_service_snapshot_is_json_safe_and_has_pm_continuity() -> None:
    snapshot = dashboard.service_snapshot()
    encoded = dashboard._canonical_json_bytes(snapshot)
    reopened = json.loads(encoded)

    assert reopened["schema_version"] == "fxpm.project-dashboard-status.v1"
    assert reopened["tradeable"] is False
    assert reopened["status"] == "ok"
    assert {row["actor"] for row in reopened["pm_continuity"]} == {
        "claude-pm",
        "codex-pm",
    }
