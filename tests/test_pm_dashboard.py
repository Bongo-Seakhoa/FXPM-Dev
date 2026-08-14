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


def test_frozen_pm_edge_contract_is_symmetric_and_legacy_bounded() -> None:
    bus = [
        _message("XPM-900", "codex-pm", "claude-pm", "review_request"),
        _message("CPM-900", "claude-pm", "codex-pm", "question"),
        _message("XPM-901", "codex-pm", "claude-pm", "veto"),
        _message("CPM-901", "claude-pm", "codex-pm", "review"),
    ]

    assert [row["id"] for row in dashboard.open_peer_requests(bus)] == [
        "XPM-900",
        "CPM-900",
        "XPM-901",
    ]
    assert "CPM-901" not in dashboard.message_resolutions(bus)

    bus.extend(
        [
            _message(
                "CPM-902",
                "claude-pm",
                "codex-pm",
                "answer",
                refs=["XPM-900", "XPM-901"],
                ts="2026-08-13T10:01:00Z",
            ),
            _message(
                "XPM-902",
                "codex-pm",
                "claude-pm",
                "decision",
                refs=["CPM-900"],
                ts="2026-08-13T10:01:00Z",
            ),
        ]
    )
    assert dashboard.open_peer_requests(bus) == []

    legacy = [
        _message(message_id, "codex-pm", "claude-pm", "review")
        for message_id in sorted(dashboard.LEGACY_BLOCKING_REVIEW_IDS)
    ]
    legacy_resolutions = dashboard.message_resolutions(legacy)
    assert set(legacy_resolutions) == set(dashboard.LEGACY_BLOCKING_REVIEW_IDS)
    assert {row["status"] for row in legacy_resolutions.values()} == {"open"}

    legacy.extend(
        _message(
            f"CPM-{900 + index}",
            "claude-pm",
            "codex-pm",
            "answer",
            refs=[message_id],
            ts="2026-08-13T10:01:00Z",
        )
        for index, message_id in enumerate(
            sorted(dashboard.LEGACY_BLOCKING_REVIEW_IDS)
        )
    )
    assert {
        message_id: row["status"]
        for message_id, row in dashboard.message_resolutions(legacy).items()
    } == {
        message_id: "answered"
        for message_id in dashboard.LEGACY_BLOCKING_REVIEW_IDS
    }


def test_same_pm_followup_replaces_only_the_earlier_open_request() -> None:
    first = _message("XPM-900", "codex-pm", "claude-pm", "review_request")
    second = _message(
        "XPM-901",
        "codex-pm",
        "claude-pm",
        "review_request",
        refs=["XPM-900"],
        ts="2026-08-13T10:01:00Z",
    )
    bus = [first, second]

    resolutions = dashboard.message_resolutions(bus)
    assert resolutions["XPM-900"]["status"] == "superseded"
    assert resolutions["XPM-901"]["status"] == "open"
    assert [row["id"] for row in dashboard.open_peer_requests(bus)] == ["XPM-901"]

    peer_reply = _message(
        "CPM-900",
        "claude-pm",
        "codex-pm",
        "answer",
        refs=["XPM-901"],
        ts="2026-08-13T10:02:00Z",
    )
    bus.append(peer_reply)

    assert dashboard.message_resolutions(bus)["XPM-901"]["status"] == "answered"
    assert dashboard.open_peer_requests(bus) == []


def test_same_sender_reference_to_another_target_does_not_supersede() -> None:
    bus = [
        _message("XPM-900", "codex-pm", "claude-pm", "review_request"),
        _message("XPM-901", "codex-pm", "all", "status", refs=["XPM-900"]),
    ]

    assert dashboard.message_resolutions(bus)["XPM-900"]["status"] == "open"


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

    assert not {
        "CPM-012",
        "CX-022",
        "CX-032",
        "CX-033",
        "XPM-019",
        "XPM-023",
        "XPM-026",
        "XPM-032",
        "XPM-043",
        "XPM-056",
    } & open_ids


def test_render_removes_withdrawn_runtime_arithmetic() -> None:
    page = dashboard.render(live=False)

    assert "576,000,000" not in page
    assert "200 Optuna trials" not in page
    assert "Every 100 ms" not in page
    assert "11.52M Backtester.run calls" in page
    assert "complete multi-grid generation total and duration are not yet established" in page
    assert "2.62 s" in page
    assert "61.6%" in page
    assert "17 → 2" in page
    assert "81.884s → 5.717s" in page
    assert "36m 58s" in page
    assert "2,217.712s of 2,222.971s total" in page
    assert "zero strategy, attempt or backtest calls" in page
    assert "Current decision frontier:" in page
    assert "XPM-060 asks Claude-PM" in page
    assert "Retained · c5c42cc" in page
    assert "Removed · c50758c" in page
    assert "Retained · 2288435" in page
    assert "Retained · 7376a06" in page
    assert "4.188571s → 3.266726s (-22.01%)" in page
    assert dashboard.E26_FINGERPRINT in page
    assert "79 passed / 1 skipped" in page
    assert "2 identity / 1 structural / 1 selected-evidence call became 4 / 1 / 1" in page
    assert "two new identity reads and no extra structural or evidence replay" in page
    assert "Warm 3.2885s" in page
    assert "19/19 focused and 153 passed / 1 skipped wider" in page
    assert "Peer verdict pending · XPM-060" in page
    assert "full suite has not run" in page
    assert "Peer verdict pending · XPM-058" in page
    assert "E27b is a Round-3-final design" in page
    assert "local tag + verified/restored bundle" in page
    assert "CX-106 · 611953d · 34,119,074 B" in page
    assert "restore-tested 13-ref bundle + thin overlay + sanitized dirty state" in page
    assert "CX-111 · MANIFEST.md · C800CA08…7603" in page
    assert "annotated tag peels to 611953d" in page
    assert "ignored credential files were excluded" in page
    assert "No E27b implementation or Gate-2 rerun is authorised" in page
    assert "academic source gained only the preservation tag" in page
    assert "Not authorised · XPM-043" not in page
    assert "Round-1 PM design" not in page
    assert "Current implementation (L-0033)" not in page
    assert "Current next action (XPM-037)" not in page
    assert "Test suite can go green while blind" not in page
    assert "current end-to-end rebaseline pending" not in page
    assert "validate E23 at exactly two native fits" not in page
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
