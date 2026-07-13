from __future__ import annotations

import csv
import hashlib
import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import pm_broker_terms as terms
from tools.capture_activate_broker_terms import main as capture_main


UTC = timezone.utc


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _export_row(
    symbol: str,
    nonce: str,
    captured: datetime,
    *,
    broker_symbol: str | None = None,
    anchors: str = "1775433600:1|1775520000:2",
) -> dict[str, str]:
    values = {
        "schema_version": terms.TERMS_EXPORT_SCHEMA,
        "nonce": nonce,
        "captured_at_utc": captured.isoformat().replace("+00:00", "Z"),
        "canonical_symbol": symbol,
        "broker_symbol": broker_symbol or symbol,
        "broker_server": "Broker-Demo",
        "terminal_company": "Broker Ltd",
        "account_currency": "USD",
        "terminal_build": "5001",
        "swap_mode": "1",
        "swap_long": "-1.25",
        "swap_short": "0.50",
        "swap_rollover3days": "3",
        "swap_sunday": "0",
        "swap_monday": "1",
        "swap_tuesday": "1",
        "swap_wednesday": "3",
        "swap_thursday": "1",
        "swap_friday": "1",
        "swap_saturday": "0",
        "d1_anchors": anchors,
    }
    return values


def _write_export(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=terms.EXPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _parse_snapshot(tmp_path: Path, symbols=("EURUSD",), now=None):
    now = now or datetime(2026, 4, 13, 12, tzinfo=UTC)
    nonce = "a" * 48
    export = tmp_path / "response.csv"
    roster = {symbol: symbol for symbol in symbols}
    rows = [_export_row(symbol, nonce, now - timedelta(minutes=2)) for symbol in symbols]
    _write_export(export, rows)
    snapshot = terms.parse_terms_export(
        export,
        expected_nonce=nonce,
        expected_roster=roster,
        expected_identity={
            "broker_server": "Broker-Demo",
            "terminal_company": "Broker Ltd",
            "account_currency": "USD",
        },
        requested_at_utc=now - timedelta(minutes=3),
        now_utc=now,
    )
    return snapshot


def test_direct_export_requires_nonce_exact_roster_identity_and_freshness(tmp_path):
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, ("EURUSD", "USDJPY"), now)

    assert snapshot["provenance_status"] == terms.AUTHENTICATED
    assert snapshot["roster"] == ["EURUSD", "USDJPY"]
    assert snapshot["symbols"]["EURUSD"]["weekday_factors"]["wednesday"] == 3
    assert len(snapshot["content_sha256"]) == 64

    export = tmp_path / "bad.csv"
    _write_export(export, [_export_row("EURUSD", "b" * 48, now - timedelta(minutes=2))])
    with pytest.raises(terms.BrokerTermsError, match="nonce mismatch"):
        terms.parse_terms_export(
            export,
            expected_nonce="a" * 48,
            expected_roster={"EURUSD": "EURUSD"},
            now_utc=now,
        )
    with pytest.raises(terms.BrokerTermsError, match="roster"):
        terms.parse_terms_export(
            export,
            expected_nonce="b" * 48,
            expected_roster={"EURUSD": "EURUSD", "GBPUSD": "GBPUSD"},
            now_utc=now,
        )

    stale = tmp_path / "stale.csv"
    _write_export(stale, [_export_row("EURUSD", "c" * 48, now - timedelta(hours=2))])
    with pytest.raises(terms.BrokerTermsError, match="stale"):
        terms.parse_terms_export(
            stale,
            expected_nonce="c" * 48,
            expected_roster={"EURUSD": "EURUSD"},
            now_utc=now,
            max_age_seconds=3600,
        )


def test_incomplete_or_contradictory_weekday_factors_never_authenticate(tmp_path):
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    nonce = "d" * 48
    row = _export_row("EURUSD", nonce, now)
    row["swap_wednesday"] = ""
    export = tmp_path / "missing.csv"
    _write_export(export, [row])
    with pytest.raises(terms.BrokerTermsError, match="not numeric"):
        terms.parse_terms_export(
            export,
            expected_nonce=nonce,
            expected_roster={"EURUSD": "EURUSD"},
            now_utc=now,
        )

    row = _export_row("EURUSD", nonce, now)
    row["swap_wednesday"] = "1"
    export = tmp_path / "contradiction.csv"
    _write_export(export, [row])
    with pytest.raises(terms.BrokerTermsError, match="contradict"):
        terms.parse_terms_export(
            export,
            expected_nonce=nonce,
            expected_roster={"EURUSD": "EURUSD"},
            now_utc=now,
        )

    assert terms.classify_terms_provenance("PYTHON_MT5", complete_weekday_factors=False) == terms.INCOMPLETE
    assert terms.classify_terms_provenance("BROKER_SPECS_JSON") == terms.LEGACY_UNTRUSTED


def test_direct_factors_accept_observed_no_triple_day_sentinel(tmp_path):
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    nonce = "7" * 48
    row = _export_row("AU200", nonce, now)
    row["swap_rollover3days"] = "7"
    row["swap_wednesday"] = "1"
    export = tmp_path / "no_triple.csv"
    _write_export(export, [row])

    snapshot = terms.parse_terms_export(
        export,
        expected_nonce=nonce,
        expected_roster={"AU200": "AU200"},
        now_utc=now,
    )

    record = snapshot["symbols"]["AU200"]
    assert record["swap_rollover3days"] == 7
    assert 3 not in record["weekday_factors"].values()


def _write_price_fixture(tmp_path: Path):
    native = tmp_path / "native"
    bars_root = native / "runs" / "r1" / "EURUSD"
    bars_root.mkdir(parents=True)
    d1 = pd.DataFrame(
        {
            "time": ["2026-04-06T00:00:00Z", "2026-04-07T00:00:00Z"],
            "Open": [1.0, 1.0],
            "High": [1.0, 1.0],
            "Low": [1.0, 1.0],
            "Close": [1.0, 1.0],
            "_BAR_CLOSE_TS": ["2026-04-07T00:00:00Z", "2026-04-08T00:00:00Z"],
        }
    )
    d1_path = bars_root / "D1.csv"
    d1.to_csv(d1_path, index=False)
    m5_path = bars_root / "M5.csv"
    pd.DataFrame(
        {
            "time": ["2026-04-06T23:55:00Z", "2026-04-08T02:00:00Z"],
            "Open": [1.0, 1.0],
            "High": [1.0, 1.0],
            "Low": [1.0, 1.0],
            "Close": [1.0, 1.0],
            "_BAR_CLOSE_TS": ["2026-04-07T00:00:00Z", "2026-04-08T02:05:00Z"],
        }
    ).to_csv(m5_path, index=False)
    d1_sidecar = bars_root / "D1.meta.json"
    m5_sidecar = bars_root / "M5.meta.json"
    d1_sidecar.write_text("{}", encoding="utf-8")
    m5_sidecar.write_text("{}", encoding="utf-8")
    manifest = {
        "schema_version": "fxpm-native-activation-v1",
        "activation_id": "price-a1",
        "status": "active",
        "entries": {
            "EURUSD": {
                "D1": {
                    "status": "active",
                    "bars_relative_path": d1_path.relative_to(native).as_posix(),
                    "bars_sha256": _sha(d1_path),
                    "sidecar_relative_path": d1_sidecar.relative_to(native).as_posix(),
                    "sidecar_sha256": _sha(d1_sidecar),
                },
                "M5": {
                    "status": "active",
                    "bars_relative_path": m5_path.relative_to(native).as_posix(),
                    "bars_sha256": _sha(m5_path),
                    "sidecar_relative_path": m5_sidecar.relative_to(native).as_posix(),
                    "sidecar_sha256": _sha(m5_sidecar),
                },
            }
        },
    }
    activation = native / "activations" / "price-a1.json"
    activation.parent.mkdir(parents=True)
    activation.write_text(json.dumps(manifest), encoding="utf-8")
    pointer = {
        "schema_version": "fxpm-native-active-pointer-v1",
        "activation_id": "price-a1",
        "manifest_relative_path": activation.relative_to(native).as_posix(),
        "manifest_sha256": _sha(activation),
    }
    (native / "ACTIVE.json").write_text(json.dumps(pointer), encoding="utf-8")
    return native, d1_path, m5_path


def test_activation_binds_existing_price_hashes_and_projects_close_and_gap(tmp_path):
    native, _d1_path, m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    # Bind the direct MQL5 weekday anchor to the active Monday D1 bar.
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    activated = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="terms-a1",
        now_utc=now,
    )
    assert activated["active_symbols"] == ["EURUSD"]
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert resolved["source_provenance_authenticated"] is True
    assert resolved["price_activation_id"] == "price-a1"
    assert resolved["target_bars_sha256"] == _sha(m5_path)

    target = pd.read_csv(m5_path, parse_dates=["time", "_BAR_CLOSE_TS"]).set_index("time")
    projection = terms.project_session_events(resolved["events"], target)
    assert projection.at_close_units.tolist() == [1, 0]
    assert projection.pre_open_units.tolist() == [0, 1]
    assert projection.at_close_units.dtype == np.int8
    assert len(projection.digest) == 64
    schedule_path = native / activated["symbols"]["EURUSD"][
        "schedule_relative_path"
    ]
    persisted_schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
    assert persisted_schedule["event_axis_contract"] == (
        "event_utc_factor_pairs_v1"
    )
    assert all(len(record) == 2 for record in persisted_schedule["events"])

    # Slicing a partition may leave thousands of older immutable schedule
    # events.  They are pre-entry history, not a giant first-bar cashflow.
    sliced = target.tail(1)
    sliced_projection = terms.project_session_events(
        [["2000-01-01T00:00:00+00:00", 3], *resolved["events"]],
        sliced,
    )
    assert sliced_projection.pre_open_units.tolist() == [0]

    original_m5 = m5_path.read_bytes()
    m5_path.write_bytes(original_m5 + b"\n")
    with pytest.raises(terms.BrokerTermsError, match="target artifact hash"):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    m5_path.write_bytes(original_m5)

    # Changing the current price pointer invalidates the overlay immediately.
    pointer = json.loads((native / "ACTIVE.json").read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = "0" * 64
    (native / "ACTIVE.json").write_text(json.dumps(pointer), encoding="utf-8")
    with pytest.raises(terms.BrokerTermsError, match="price manifest hash"):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


def test_explicit_stress_is_adverse_and_never_implicit(tmp_path):
    snapshot = _parse_snapshot(tmp_path)
    stressed = terms.build_explicit_stress_snapshot(snapshot)
    record = stressed["symbols"]["EURUSD"]
    assert stressed["provenance_status"] == terms.STRESS_APPROVED
    assert record["swap_long"] == record["swap_short"] == -1.25
    assert set(record["weekday_factors"].values()) == {3}
    assert snapshot["provenance_status"] == terms.AUTHENTICATED


def test_missing_active_d1_schedule_fails_closed_without_price_fetch(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["entries"]["EURUSD"]["D1"]["status"] = "excluded_no_trade"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    with pytest.raises(terms.BrokerTermsError, match="no symbol"):
        terms.activate_broker_terms(native, snapshot, now_utc=now)
    assert "copy_" + "rates" not in inspect.getsource(terms.activate_broker_terms)


def test_capture_command_returns_honest_wait_state_and_has_no_history_api(tmp_path, capsys):
    exchange = tmp_path / "exchange"
    roster = tmp_path / "roster.json"
    roster.write_text(json.dumps({"EURUSD": "EURUSD"}), encoding="utf-8")
    code = capture_main([
        "--native-root", str(tmp_path / "native"),
        "--exchange-dir", str(exchange),
        "--roster-json", str(roster),
    ])
    output = json.loads(capsys.readouterr().out)
    assert code == 3
    assert output["status"] == "WAITING_FOR_MQL5_EXPORT"
    assert (exchange / "fxpm_terms_request.csv").exists()
    stale = _export_row(
        "EURUSD",
        "f" * 48,
        datetime.now(UTC) - timedelta(minutes=1),
    )
    _write_export(exchange / "fxpm_terms_response.csv", [stale])
    code = capture_main([
        "--native-root", str(tmp_path / "native"),
        "--exchange-dir", str(exchange),
        "--roster-json", str(roster),
    ])
    output = json.loads(capsys.readouterr().out)
    assert code == 3
    assert output["status"] == "WAITING_FOR_MQL5_EXPORT"
    assert output["observed_stale_nonce"] == "f" * 48
    source = inspect.getsource(__import__("tools.capture_activate_broker_terms", fromlist=["main"]))
    assert "copy_" + "rates" not in source


def test_retention_preserves_active_and_referenced_and_enforces_hard_bound(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(native, snapshot, activation_id="terms-a1", now_utc=now)
    snapshots = native / "terms" / "snapshots"
    junk = snapshots / "junk.json"
    junk.write_text('{"junk": true}', encoding="utf-8")
    referenced = native / "terms" / "archive" / "referenced.json"
    referenced.parent.mkdir(parents=True)
    referenced.write_text('{"keep": true}', encoding="utf-8")
    (native / "terms" / "REFERENCES.json").write_text(
        json.dumps({"protected_sha256": [_sha(referenced)]}),
        encoding="utf-8",
    )
    result = terms.prune_terms_store(native, max_snapshots=1, max_total_bytes=4 * 1024 * 1024)
    assert not junk.exists()
    assert referenced.exists()
    assert (native / "terms" / "REFERENCES.json").exists()
    assert result["protected_bytes"] > 0
    assert (native / "terms" / "ACTIVE.json").exists()
    with pytest.raises(terms.TermsStorageBoundError):
        terms.prune_terms_store(native, max_snapshots=1, max_total_bytes=1)
