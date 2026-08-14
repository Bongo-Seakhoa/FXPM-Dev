from __future__ import annotations

import csv
import hashlib
import inspect
import io
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import pm_broker_terms as terms
import tools.capture_activate_broker_terms as capture_cli
from tools.capture_activate_broker_terms import main as capture_main


UTC = timezone.utc
_REAL_RECEIPT_BACKED_SCOPE = terms._receipt_backed_price_authority_scope


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


def _full_receipt_terms_snapshot(now: datetime) -> dict:
    from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER

    symbols = {
        native_symbol.upper(): {
            "canonical_symbol": native_symbol.upper(),
            "broker_symbol": native_symbol.upper(),
            "swap_mode": 1,
            "swap_long": -1.25,
            "swap_short": 0.5,
            "swap_rollover3days": 3,
            "weekday_factors": {
                "sunday": 0,
                "monday": 1,
                "tuesday": 1,
                "wednesday": 3,
                "thursday": 1,
                "friday": 1,
                "saturday": 0,
            },
            "d1_anchors": [
                {
                    "open_epoch": int(pd.Timestamp("2025-01-06T00:00:00Z").timestamp()),
                    "server_weekday": 1,
                }
            ],
            "provenance_status": terms.AUTHENTICATED,
        }
        for native_symbol in PRODUCTION_SYMBOL_ORDER
    }
    captured = now.astimezone(UTC).isoformat().replace("+00:00", "Z")
    snapshot = {
        "schema_version": terms.TERMS_SNAPSHOT_SCHEMA,
        "provenance_status": terms.AUTHENTICATED,
        "extractor_contract": "mql5-symbolinfo-direct-v1",
        "nonce": "r" * 48,
        "captured_at_utc": captured,
        "capture_started_at_utc": captured,
        "broker_identity": {
            "broker_server": "Broker-Demo",
            "terminal_company": "Broker Ltd",
            "account_currency": "USD",
            "terminal_build": 5001,
        },
        "roster": sorted(symbols),
        "symbols": symbols,
        "source_export_sha256": "d" * 64,
    }
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    return snapshot


def test_terms_export_hashes_and_parses_the_same_bounded_payload(
    tmp_path,
    monkeypatch,
):
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    nonce = "d" * 48
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=terms.EXPORT_COLUMNS)
    writer.writeheader()
    writer.writerow(_export_row("EURUSD", nonce, now - timedelta(minutes=2)))
    payload = buffer.getvalue().encode("utf-8")
    missing_path = tmp_path / "already-replaced-export.csv"
    bounded_reads = []
    parsed_payloads = []
    real_dict_reader = csv.DictReader

    def bounded_read(path, max_bytes):
        bounded_reads.append((Path(path), max_bytes))
        return payload

    def dict_reader_from_memory(source, *args, **kwargs):
        assert isinstance(source, io.StringIO)
        parsed_payloads.append(source.getvalue().encode("utf-8"))
        return real_dict_reader(source, *args, **kwargs)

    monkeypatch.setattr(terms, "_read_bounded", bounded_read)
    monkeypatch.setattr(terms.csv, "DictReader", dict_reader_from_memory)
    snapshot = terms.parse_terms_export(
        missing_path,
        expected_nonce=nonce,
        expected_roster={"EURUSD": "EURUSD"},
        expected_identity={
            "broker_server": "Broker-Demo",
            "terminal_company": "Broker Ltd",
            "account_currency": "USD",
        },
        requested_at_utc=now - timedelta(minutes=3),
        now_utc=now,
        max_bytes=len(payload),
    )

    assert bounded_reads == [(missing_path, len(payload))]
    assert parsed_payloads == [payload]
    assert snapshot["source_export_sha256"] == hashlib.sha256(
        parsed_payloads[0]
    ).hexdigest()
    assert snapshot["symbols"]["EURUSD"]["swap_long"] == -1.25


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


def _write_price_fixture(tmp_path: Path, *, price_symbol: str = "EURUSD"):
    native = tmp_path / "native"
    bars_root = native / "runs" / "r1" / price_symbol
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
            price_symbol: {
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


@pytest.fixture(autouse=True)
def _compact_receipt_backed_price_scope(monkeypatch):
    """Adapt compact unit authorities at the one production scope seam."""

    @contextmanager
    def scope(native_root):
        price = terms.load_current_price_activation(native_root)

        def revalidate():
            current = terms.load_current_price_activation(native_root)
            if not terms._same_price_identity(price, current):
                raise terms.BrokerTermsError(
                    "receipt-backed native price authority changed"
                )

        yield price, revalidate

    monkeypatch.setattr(terms, "_receipt_backed_price_authority_scope", scope)


def _promote_price_fixture(
    native: Path,
    *,
    price_symbol: str = "EURUSD",
    technical_timeframe: str = "H1",
) -> None:
    """Convert the compact legacy fixture into the promoted v4 cell contract."""

    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cells = manifest["entries"][price_symbol]
    cells["D1"]["status"] = "evidence_candidate"
    cells["M5"]["status"] = "evidence_candidate"
    for timeframe in ("D1", "M5"):
        cells[timeframe].update(
            {
                "symbol": price_symbol,
                "timeframe": timeframe,
                "universe_eligibility_status": "evidence_candidate",
            }
        )
    terminal = json.loads(json.dumps(cells["M5"]))
    terminal["status"] = "technical_error"
    terminal["timeframe"] = technical_timeframe
    terminal["universe_eligibility_status"] = "technical_error"
    cells[technical_timeframe] = terminal
    manifest["production_authoritative"] = True
    manifest["native_is_signal_authority"] = True
    manifest["silent_fallback_allowed"] = False
    manifest["counts"] = {"pending": 0, "terminal": 3, "total": 3}
    manifest["universe_eligibility"] = {
        "schema_version": "fxpm-native-universe-authenticity-v4",
        "trading_verdicts_emitted": False,
        "performance_inputs_used": False,
        "required_timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
        "cell_statuses": {
            price_symbol: {
                "D1": "evidence_candidate",
                "M5": "evidence_candidate",
                technical_timeframe: "technical_error",
            }
        },
        "evidence_candidate_timeframes_by_symbol": {
            price_symbol: ["D1", "M5"],
        },
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer["production_authoritative"] = True
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")


def _move_compact_price_to_receipt_identity(native: Path) -> None:
    """Move the compact test price to a distinct receipt-bearing identity."""

    pointer_path = native / "ACTIVE.json"
    predecessor_pointer_sha256 = _sha(pointer_path)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    predecessor_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(predecessor_path.read_text(encoding="utf-8"))
    manifest["activation_id"] = "price-a2"
    manifest["previous_active_pointer_sha256"] = predecessor_pointer_sha256
    manifest["semantic_receipt_relative_path"] = "semantic_receipts/price-a2.json"
    manifest["semantic_receipt_sha256"] = "e" * 64
    successor_path = native / "activations" / "price-a2.json"
    successor_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer.update(
        {
            "activation_id": "price-a2",
            "manifest_relative_path": successor_path.relative_to(native).as_posix(),
            "manifest_sha256": _sha(successor_path),
        }
    )
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")


def _promote_intraday_fallback_fixture(
    native: Path,
    *,
    price_symbol: str = "EURUSD",
    source_timeframe: str = "H4",
    source_status: str = "success",
):
    """Add one promoted intraday source and make native D1 terminal."""

    timeframe = source_timeframe.upper()
    seconds = {
        "H4": 4 * 3600,
        "H1": 3600,
        "M30": 30 * 60,
        "M15": 15 * 60,
        "M5": 5 * 60,
    }[timeframe]
    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cells = manifest["entries"][price_symbol]
    bars_root = native / "runs" / "r1" / price_symbol

    opens = []
    # Ordinary session, early close, a completely absent day, another
    # ordinary session, and one trailing partial session.
    first_open_offset = seconds if timeframe == "M5" else 0
    for day, stop_seconds in (
        ("2026-04-06", 24 * 3600),
        ("2026-04-07", 20 * 3600),
        ("2026-04-09", 24 * 3600),
        ("2026-04-10", 2 * seconds),
    ):
        start = pd.Timestamp(f"{day}T00:00:00Z")
        opens.extend(
            start + pd.Timedelta(seconds=offset)
            for offset in range(first_open_offset, stop_seconds, seconds)
        )
    closes = [opened + pd.Timedelta(seconds=seconds) for opened in opens]
    source_path = bars_root / f"{timeframe}.csv"
    pd.DataFrame(
        {
            "time": [value.isoformat() for value in opens],
            "Open": [1.0] * len(opens),
            "High": [1.0] * len(opens),
            "Low": [1.0] * len(opens),
            "Close": [1.0] * len(opens),
            "_BAR_CLOSE_TS": [value.isoformat() for value in closes],
        }
    ).to_csv(source_path, index=False)
    source_sidecar = bars_root / f"{timeframe}.meta.json"
    source_sidecar.write_text("{}", encoding="utf-8")

    cells["D1"].update(
        {
            "status": "technical_error",
            "universe_eligibility_status": "technical_error",
        }
    )
    cells[timeframe] = {
        "status": "evidence_candidate",
        "symbol": price_symbol,
        "timeframe": timeframe,
        "universe_eligibility_status": "evidence_candidate",
        "source_status": source_status,
        "bars_relative_path": source_path.relative_to(native).as_posix(),
        "bars_sha256": _sha(source_path),
        "sidecar_relative_path": source_sidecar.relative_to(native).as_posix(),
        "sidecar_sha256": _sha(source_sidecar),
    }
    cells["M5"].update(
        {
            "status": "evidence_candidate",
            "symbol": price_symbol,
            "timeframe": "M5",
            "universe_eligibility_status": "evidence_candidate",
        }
    )
    statuses = {
        name: str(cell["status"])
        for name, cell in cells.items()
    }
    candidates = sorted(
        name for name, status in statuses.items()
        if status == "evidence_candidate"
    )
    manifest["counts"] = {
        "pending": 0,
        "terminal": len(cells),
        "total": len(cells),
    }
    manifest["universe_eligibility"]["cell_statuses"][price_symbol] = statuses
    manifest["universe_eligibility"][
        "evidence_candidate_timeframes_by_symbol"
    ][price_symbol] = candidates
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    return source_path, source_sidecar


def test_d1_derivation_hashes_and_parses_the_same_bounded_payload(
    tmp_path,
    monkeypatch,
):
    payload = (
        b"time,_BAR_CLOSE_TS\n"
        b"2026-04-06T00:00:00+00:00,2026-04-07T00:00:00+00:00\n"
        b"2026-04-07T00:00:00+00:00,2026-04-08T00:00:00+00:00\n"
    )
    missing_path = tmp_path / "already-replaced-d1.csv"
    bounded_reads = []
    parsed_payloads = []
    real_read_csv = pd.read_csv

    def bounded_read(path, max_bytes):
        bounded_reads.append((Path(path), max_bytes))
        return payload

    def read_csv_from_memory(source, *args, **kwargs):
        assert isinstance(source, io.BytesIO)
        parsed_payloads.append(source.getvalue())
        return real_read_csv(source, *args, **kwargs)

    monkeypatch.setattr(terms, "_read_bounded", bounded_read)
    monkeypatch.setattr(terms.pd, "read_csv", read_csv_from_memory)
    events, digest = terms._derive_d1_session_events_hashed(
        missing_path,
        symbol_terms={
            "d1_anchors": [{"open_epoch": 1775433600, "server_weekday": 1}],
            "weekday_factors": {
                "sunday": 0,
                "monday": 1,
                "tuesday": 1,
                "wednesday": 3,
                "thursday": 1,
                "friday": 1,
                "saturday": 0,
            },
        },
        max_bytes=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
    )

    assert bounded_reads == [(missing_path, len(payload))]
    assert parsed_payloads == [payload]
    assert digest == hashlib.sha256(parsed_payloads[0]).hexdigest()
    assert events == [
        {
            "d1_bar_index": 0,
            "d1_open_utc": "2026-04-06T00:00:00+00:00",
            "event_utc": "2026-04-07T00:00:00+00:00",
            "from_weekday": 1,
            "from_weekday_name": "monday",
            "factor": 1,
        },
        {
            "d1_bar_index": 1,
            "d1_open_utc": "2026-04-07T00:00:00+00:00",
            "event_utc": "2026-04-08T00:00:00+00:00",
            "from_weekday": 2,
            "from_weekday_name": "tuesday",
            "factor": 1,
        },
    ]


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
    # Runtime trusts the receipt-backed price manifest and the immutable terms
    # schedule; it does not repeat raw price-leaf verification.
    assert (
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")["target_bars_sha256"]
        == hashlib.sha256(original_m5).hexdigest()
    )
    m5_path.write_bytes(original_m5)

    # Changing the current price pointer invalidates the overlay immediately.
    pointer = json.loads((native / "ACTIVE.json").read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = "0" * 64
    (native / "ACTIVE.json").write_text(json.dumps(pointer), encoding="utf-8")
    with pytest.raises(terms.BrokerTermsError, match="price manifest hash"):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


def test_activation_enters_one_price_authority_scope(tmp_path, monkeypatch):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    inherited_scope = terms._receipt_backed_price_authority_scope
    observed = {"entries": 0, "revalidations": 0}

    @contextmanager
    def counted_scope(root):
        observed["entries"] += 1
        with inherited_scope(root) as (price, revalidate):

            def counted_revalidate():
                observed["revalidations"] += 1
                revalidate()

            yield price, counted_revalidate

    monkeypatch.setattr(
        terms,
        "_receipt_backed_price_authority_scope",
        counted_scope,
    )
    terms.activate_broker_terms(native, snapshot, now_utc=now)

    assert observed == {"entries": 1, "revalidations": 1}


def test_schedule_axis_mutation_immediately_before_cas_is_rejected(
    tmp_path,
    monkeypatch,
):
    native, d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    real_atomic_replace = terms._atomic_replace_json

    def mutate_axis_before_validation(path, value, *, before_replace=None):
        assert before_replace is not None
        if Path(path) == native / "terms" / "ACTIVE.json":
            d1_path.write_bytes(d1_path.read_bytes() + b"\n")
        return real_atomic_replace(
            path,
            value,
            before_replace=before_replace,
        )

    monkeypatch.setattr(terms, "_atomic_replace_json", mutate_axis_before_validation)
    with pytest.raises(
        terms.BrokerTermsError,
        match="staged price schedule axis hash mismatch",
    ):
        terms.activate_broker_terms(native, snapshot, now_utc=now)

    assert not (native / "terms" / "ACTIVE.json").exists()


def test_promoted_v4_binds_candidates_and_rejects_terminal_technical_cells(tmp_path):
    native, _d1_path, m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    activated = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="promoted-terms-a1",
        now_utc=now,
    )

    bindings = activated["symbols"]["EURUSD"]["target_bindings"]
    assert set(bindings) == {"D1", "M5"}
    assert bindings["M5"]["bars_sha256"] == _sha(m5_path)
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert resolved["target_bars_sha256"] == _sha(m5_path)
    with pytest.raises(terms.BrokerTermsError):
        terms.resolve_active_swap_contract(native, "EURUSD", "H1")


@pytest.mark.parametrize(
    ("source_timeframe", "source_status"),
    [
        ("H4", "success"),
        ("H4", "partial"),
        ("H1", "success"),
        ("M30", "success"),
        ("M15", "success"),
        ("M5", "success"),
    ],
)
def test_promoted_intraday_fallback_builds_one_daily_schedule_from_good_proxy(
    tmp_path,
    source_timeframe,
    source_status,
):
    native, _d1_path, m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    source_path, _source_sidecar = _promote_intraday_fallback_fixture(
        native,
        source_timeframe=source_timeframe,
        source_status=source_status,
    )
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)

    activated = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id=f"intraday-{source_timeframe.lower()}-{source_status}",
        now_utc=now,
    )

    record = activated["symbols"]["EURUSD"]
    assert record["schedule_source_timeframe"] == source_timeframe
    assert record["schedule_source_binding"]["bars_sha256"] == _sha(source_path)
    assert set(record["target_bindings"]) == {source_timeframe, "M5"}
    schedule = json.loads(
        (native / record["schedule_relative_path"]).read_text(encoding="utf-8")
    )
    assert schedule["schema_version"] == terms.SESSION_SCHEDULE_SCHEMA
    assert schedule["schedule_source_timeframe"] == source_timeframe
    assert schedule["schedule_derivation_metadata"]["phase_seconds"] == 0
    assert schedule["events"] == [
        ["2026-04-07T00:00:00+00:00", 1],
        ["2026-04-08T00:00:00+00:00", 1],
        ["2026-04-10T00:00:00+00:00", 1],
    ]

    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert resolved["target_bars_sha256"] == _sha(m5_path)
    assert resolved["schedule_source_timeframe"] == source_timeframe
    assert resolved["schedule_source_bars_sha256"] == _sha(source_path)
    assert resolved["d1_bars_sha256"] is None
    with pytest.raises(terms.BrokerTermsError):
        terms.resolve_active_swap_contract(native, "EURUSD", "D1")


def test_actual_schedule_axes_are_parsed_once_and_persisted_as_a_sorted_set(
    tmp_path,
    monkeypatch,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    source_path, _source_sidecar = _promote_intraday_fallback_fixture(
        native,
        source_timeframe="H4",
    )
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    real_reader = terms._read_interval_axis_hashed
    reads = []

    def observed_reader(path, **kwargs):
        reads.append(Path(path).resolve())
        return real_reader(path, **kwargs)

    monkeypatch.setattr(terms, "_read_interval_axis_hashed", observed_reader)
    activated = terms.activate_broker_terms(native, snapshot, now_utc=now)
    bindings = activated["schedule_axis_bindings"]

    assert bindings == sorted(
        bindings,
        key=lambda value: (value["bars_relative_path"], value["bars_sha256"]),
    )
    assert len(bindings) == 2
    assert set(reads) == {
        source_path.resolve(),
        (native / "runs/r1/EURUSD/M5.csv").resolve(),
    }
    assert all(reads.count(path) == 1 for path in set(reads))


def test_real_receipt_authority_observes_current_104_axis_surface(
    tmp_path,
    monkeypatch,
):
    import pm_native_activation as native_activation
    from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER
    from tests.native_v5_support import write_receipt_backed_v5_authority

    native = tmp_path / "native"
    write_receipt_backed_v5_authority(
        native,
        current_schedule_surface=True,
    )
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _full_receipt_terms_snapshot(now)
    reads = []
    scopes = []
    real_reader = terms._read_interval_axis_hashed
    real_scope_factory = (
        native_activation.production_native_authority_verification_scope
    )

    def observed_scope_factory():
        scope = real_scope_factory()
        scopes.append(scope)
        return scope

    def observed_reader(path, **kwargs):
        reads.append(Path(path).resolve())
        return real_reader(path, **kwargs)

    monkeypatch.setattr(
        terms,
        "_receipt_backed_price_authority_scope",
        _REAL_RECEIPT_BACKED_SCOPE,
    )
    monkeypatch.setattr(
        native_activation,
        "production_native_authority_verification_scope",
        observed_scope_factory,
    )
    monkeypatch.setattr(terms, "_read_interval_axis_hashed", observed_reader)
    activated = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="receipt-backed-terms",
        now_utc=now,
    )

    bindings = activated["schedule_axis_bindings"]
    assert len(bindings) == 104
    assert len(reads) == len(set(reads)) == 104
    assert len(activated["active_symbols"]) == 100
    assert len(scopes) == 1
    assert scopes[0].stats()["full_verification_count"] == 1
    assert scopes[0].stats()["identity_revalidation_count"] == 1
    assert (
        activated["symbols"][PRODUCTION_SYMBOL_ORDER[-1].upper()][
            "schedule_source_timeframe"
        ]
        == "H4"
    )

    native_root = native.resolve()
    real_path_open = Path.open

    def is_raw_price_leaf(path):
        candidate = Path(path).resolve()
        return (
            candidate == native_root or native_root in candidate.parents
        ) and (
            candidate.suffix.lower() == ".csv"
            or candidate.name.endswith(".meta.json")
        )

    def no_raw_path_open(path, *args, **kwargs):
        if is_raw_price_leaf(path):
            pytest.fail(f"runtime opened raw price leaf: {path}")
        return real_path_open(path, *args, **kwargs)

    def no_selected_resolver(*_args, **_kwargs):
        pytest.fail("runtime called a selected native resolver")

    monkeypatch.setattr(Path, "open", no_raw_path_open)
    monkeypatch.setattr(
        native_activation,
        "resolve_active_native_universe",
        no_selected_resolver,
    )
    monkeypatch.setattr(
        native_activation,
        "_resolve_active_native_universe_uncached",
        no_selected_resolver,
    )
    monkeypatch.setattr(
        native_activation,
        "resolve_active_native_artifact",
        no_selected_resolver,
    )
    resolved = terms.resolve_active_swap_contract(
        native,
        PRODUCTION_SYMBOL_ORDER[-1],
        "M5",
    )

    assert resolved["schedule_source_timeframe"] == "H4"
    assert len(scopes) == 2
    assert scopes[1].stats()["full_verification_count"] == 1
    assert scopes[1].stats()["identity_revalidation_count"] == 1


def test_promoted_d1_remains_preferred_and_keeps_the_v2_record_shape(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    _promote_intraday_fallback_fixture(native, source_timeframe="H4")
    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    d1 = manifest["entries"]["EURUSD"]["D1"]
    d1["status"] = "evidence_candidate"
    d1["universe_eligibility_status"] = "evidence_candidate"
    universe = manifest["universe_eligibility"]
    universe["cell_statuses"]["EURUSD"]["D1"] = "evidence_candidate"
    universe["evidence_candidate_timeframes_by_symbol"]["EURUSD"] = sorted(
        set(universe["evidence_candidate_timeframes_by_symbol"]["EURUSD"])
        | {"D1"}
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)

    activated = terms.activate_broker_terms(native, snapshot, now_utc=now)
    record = activated["symbols"]["EURUSD"]
    assert "schedule_source_timeframe" not in record
    assert "schedule_source_binding" not in record
    assert "d1_binding" in record
    schedule = json.loads(
        (native / record["schedule_relative_path"]).read_text(encoding="utf-8")
    )
    assert schedule["schema_version"] == terms.D1_SESSION_SCHEDULE_SCHEMA
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "H4")
    assert resolved["schedule_source_timeframe"] == "D1"
    assert resolved["d1_bars_sha256"] == record["d1_bars_sha256"]


def test_receipt_backed_generic_schedule_cannot_override_admissible_d1(
    tmp_path,
    monkeypatch,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    _promote_intraday_fallback_fixture(native, source_timeframe="H4")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    terms.activate_broker_terms(native, snapshot, now_utc=now)

    receipt = {
        "schema_version": terms.SCHEDULE_SOURCE_SELECTION_SCHEMA,
        "policy": terms.SCHEDULE_SOURCE_POLICY,
        "rejected_candidates": [],
        "selected_source_timeframe": "H4",
        "selected_source_completed_session_count": 3,
        "selected_source_completed_sessions_sha256": "1" * 64,
        "target_completed_session_union_count": 1,
        "target_completed_session_union_sha256": "2" * 64,
    }
    terms_pointer_path = native / "terms" / "ACTIVE.json"
    terms_pointer = json.loads(
        terms_pointer_path.read_text(encoding="utf-8")
    )
    terms_manifest_path = native / terms_pointer["manifest_relative_path"]
    terms_manifest = json.loads(
        terms_manifest_path.read_text(encoding="utf-8")
    )
    terms_record = terms_manifest["symbols"]["EURUSD"]
    schedule_path = native / terms_record["schedule_relative_path"]
    schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
    schedule["schedule_source_selection_receipt"] = receipt
    schedule_path.write_text(json.dumps(schedule), encoding="utf-8")
    terms_record["schedule_source_selection_receipt"] = receipt
    terms_record["schedule_sha256"] = _sha(schedule_path)
    terms_manifest_path.write_text(
        json.dumps(terms_manifest),
        encoding="utf-8",
    )
    terms_pointer["manifest_sha256"] = _sha(terms_manifest_path)
    terms_pointer_path.write_text(
        json.dumps(terms_pointer),
        encoding="utf-8",
    )

    price = terms.load_current_price_activation(native)
    forged_price = dict(price)
    forged_manifest = json.loads(json.dumps(price["manifest"]))
    d1 = forged_manifest["entries"]["EURUSD"]["D1"]
    d1["status"] = "evidence_candidate"
    d1["universe_eligibility_status"] = "evidence_candidate"
    universe = forged_manifest["universe_eligibility"]
    universe["cell_statuses"]["EURUSD"]["D1"] = "evidence_candidate"
    universe["evidence_candidate_timeframes_by_symbol"]["EURUSD"] = sorted(
        set(universe["evidence_candidate_timeframes_by_symbol"]["EURUSD"])
        | {"D1"}
    )
    forged_price["manifest"] = forged_manifest
    monkeypatch.setattr(
        terms,
        "load_current_price_activation",
        lambda _native_root: forged_price,
    )

    with pytest.raises(
        terms.BrokerTermsError,
        match="generic schedule cannot override native D1",
    ):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


@pytest.mark.parametrize("artifact", ["bars", "sidecar"])
def test_intraday_runtime_does_not_reopen_raw_schedule_source(
    tmp_path,
    artifact,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    source_path, source_sidecar = _promote_intraday_fallback_fixture(native)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    terms.activate_broker_terms(native, snapshot, now_utc=now)

    damaged = source_path if artifact == "bars" else source_sidecar
    damaged.write_bytes(damaged.read_bytes() + b"\n")
    assert (
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")[
            "schedule_source_timeframe"
        ]
        == "H4"
    )


@pytest.mark.parametrize("intraday", [False, True])
def test_runtime_performs_zero_raw_price_leaf_reads(
    tmp_path,
    monkeypatch,
    intraday,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    if intraday:
        _promote_price_fixture(native)
        _promote_intraday_fallback_fixture(native)
    else:
        snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
            {"open_epoch": 1775433600, "server_weekday": 1}
        ]
        snapshot.pop("content_sha256")
        snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(native, snapshot, now_utc=now)
    real_read = terms._read_bounded
    real_sha = terms.sha256_file

    def no_raw_read(path, max_bytes):
        if Path(path).suffix.lower() in {".csv", ".meta"} or str(path).endswith(
            ".meta.json"
        ):
            pytest.fail(f"runtime reopened raw price leaf: {path}")
        return real_read(path, max_bytes)

    def no_raw_sha(path):
        if Path(path).suffix.lower() == ".csv" or str(path).endswith(".meta.json"):
            pytest.fail(f"runtime rehashed raw price leaf: {path}")
        return real_sha(path)

    monkeypatch.setattr(terms, "_read_bounded", no_raw_read)
    monkeypatch.setattr(terms, "sha256_file", no_raw_sha)
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")

    assert resolved["schedule_source_timeframe"] == ("H4" if intraday else "D1")


def test_runtime_receipt_identity_drift_fails_closed(tmp_path, monkeypatch):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(native, snapshot, now_utc=now)
    inherited_scope = terms._receipt_backed_price_authority_scope

    @contextmanager
    def receipt_drift_scope(root):
        with inherited_scope(root) as (price, revalidate):

            def receipt_drift():
                revalidate()
                raise terms.BrokerTermsError(
                    "receipt-backed native price authority changed"
                )

            yield price, receipt_drift

    monkeypatch.setattr(
        terms,
        "_receipt_backed_price_authority_scope",
        receipt_drift_scope,
    )
    with pytest.raises(terms.BrokerTermsError, match="receipt-backed"):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


@pytest.mark.parametrize("tamper", ["source_timeframe", "target_binding"])
def test_intraday_record_splices_remain_fail_closed(tmp_path, tamper):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    _promote_intraday_fallback_fixture(native)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    terms.activate_broker_terms(native, snapshot, now_utc=now)

    pointer_path = native / "terms" / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    record = manifest["symbols"]["EURUSD"]
    if tamper == "source_timeframe":
        record["schedule_source_timeframe"] = "H1"
    else:
        record["target_bindings"].pop("M5")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")

    with pytest.raises(terms.BrokerTermsError):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


def test_intraday_schedule_event_axis_contract_splice_remains_fail_closed(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    _promote_intraday_fallback_fixture(native)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    terms.activate_broker_terms(native, snapshot, now_utc=now)

    pointer_path = native / "terms" / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    record = manifest["symbols"]["EURUSD"]
    schedule_path = native / record["schedule_relative_path"]
    schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
    schedule["event_axis_contract"] = "unbound-event-axis"
    schedule_path.write_text(json.dumps(schedule), encoding="utf-8")
    record["schedule_sha256"] = _sha(schedule_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")

    with pytest.raises(
        terms.BrokerTermsError, match="schedule binding/schema mismatch"
    ):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


def test_intraday_fallback_rejects_unbound_anchor_phase(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    _promote_intraday_fallback_fixture(native)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1},
        {"open_epoch": 1775448000, "server_weekday": 1},
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    with pytest.raises(terms.BrokerTermsError, match="session phase"):
        terms.activate_broker_terms(native, snapshot, now_utc=now)


def test_intraday_fallback_rejects_target_outside_source_coverage(tmp_path):
    native, _d1_path, m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    _promote_intraday_fallback_fixture(native)
    target = pd.read_csv(m5_path)
    target.loc[0, "time"] = "2026-04-05T23:55:00Z"
    target.loc[0, "_BAR_CLOSE_TS"] = "2026-04-06T00:00:00Z"
    target.to_csv(m5_path, index=False)
    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["entries"]["EURUSD"]["M5"]["bars_sha256"] = _sha(m5_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)

    with pytest.raises(
        terms.BrokerTermsError, match="covers all completed target sessions"
    ):
        terms.activate_broker_terms(native, snapshot, now_utc=now)


def test_intraday_fallback_uses_finer_source_for_missing_interior_session(
    tmp_path,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    _promote_price_fixture(native)
    h4_path, _h4_sidecar = _promote_intraday_fallback_fixture(
        native,
        source_timeframe="H4",
    )
    _h1_path, _h1_sidecar = _promote_intraday_fallback_fixture(
        native,
        source_timeframe="H1",
    )
    h4 = pd.read_csv(h4_path)
    h4 = h4[
        ~h4["time"].astype(str).str.startswith("2026-04-07")
    ]
    h4.to_csv(h4_path, index=False)
    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["entries"]["EURUSD"]["H4"]["bars_sha256"] = _sha(h4_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    activated = terms.activate_broker_terms(native, snapshot, now_utc=now)

    record = activated["symbols"]["EURUSD"]
    assert record["schedule_source_timeframe"] == "H1"
    receipt = record["schedule_source_selection_receipt"]
    assert receipt["selected_source_timeframe"] == "H1"
    assert [
        rejection["timeframe"]
        for rejection in receipt["rejected_candidates"]
    ] == ["H4"]
    assert (
        receipt["rejected_candidates"][0]["reason"]
        == "INCOMPLETE_TARGET_SESSION_COVERAGE"
    )
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert resolved["schedule_source_timeframe"] == "H1"

    terms_pointer_path = native / "terms" / "ACTIVE.json"
    terms_pointer = json.loads(
        terms_pointer_path.read_text(encoding="utf-8")
    )
    terms_manifest_path = native / terms_pointer["manifest_relative_path"]
    terms_manifest = json.loads(
        terms_manifest_path.read_text(encoding="utf-8")
    )
    terms_manifest["symbols"]["EURUSD"][
        "schedule_source_selection_receipt"
    ][
        "rejected_candidates"
    ][0]["binding"]["bars_sha256"] = "0" * 64
    terms_manifest_path.write_text(
        json.dumps(terms_manifest),
        encoding="utf-8",
    )
    terms_pointer["manifest_sha256"] = _sha(terms_manifest_path)
    terms_pointer_path.write_text(
        json.dumps(terms_pointer),
        encoding="utf-8",
    )
    with pytest.raises(
        terms.BrokerTermsError,
        match="rejected schedule-source binding mismatch",
    ):
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")


def test_promoted_v4_casefold_maps_palladium_to_exact_native_identity(tmp_path):
    native, _d1_path, m5_path = _write_price_fixture(
        tmp_path,
        price_symbol="Palladium",
    )
    _promote_price_fixture(native, price_symbol="Palladium")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, symbols=("PALLADIUM",), now=now)
    snapshot["symbols"]["PALLADIUM"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    activated = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="palladium-terms-a1",
        now_utc=now,
    )

    assert activated["active_symbols"] == ["PALLADIUM"]
    upper = terms.resolve_active_swap_contract(native, "PALLADIUM", "M5")
    mixed = terms.resolve_active_swap_contract(native, "Palladium", "M5")
    assert upper["target_bars_sha256"] == mixed["target_bars_sha256"] == _sha(m5_path)


def test_promoted_v4_rejects_casefold_colliding_native_symbols(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(
        tmp_path,
        price_symbol="Palladium",
    )
    _promote_price_fixture(native, price_symbol="Palladium")
    pointer_path = native / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["entries"]["PALLADIUM"] = json.loads(
        json.dumps(manifest["entries"]["Palladium"])
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = _sha(manifest_path)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, symbols=("PALLADIUM",), now=now)

    with pytest.raises(terms.BrokerTermsError):
        terms.activate_broker_terms(native, snapshot, now_utc=now)


def test_non_d1_resolution_does_not_reopen_d1_schedule_source_bars(tmp_path):
    native, d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(native, snapshot, now_utc=now)
    original = d1_path.read_bytes()
    d1_path.write_bytes(original + b"\n")

    assert (
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")[
            "schedule_source_timeframe"
        ]
        == "D1"
    )


def test_non_d1_resolution_does_not_reopen_d1_schedule_source_sidecar(tmp_path):
    native, d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(native, snapshot, now_utc=now)
    d1_sidecar = d1_path.with_name("D1.meta.json")
    d1_sidecar.write_text('{"tampered": true}', encoding="utf-8")

    assert (
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")[
            "schedule_source_timeframe"
        ]
        == "D1"
    )


def test_explicit_stress_is_adverse_and_never_implicit(tmp_path):
    snapshot = _parse_snapshot(tmp_path)
    stressed = terms.build_explicit_stress_snapshot(snapshot)
    record = stressed["symbols"]["EURUSD"]
    assert stressed["provenance_status"] == terms.STRESS_APPROVED
    assert record["swap_long"] == record["swap_short"] == -1.25
    assert set(record["weekday_factors"].values()) == {3}
    assert snapshot["provenance_status"] == terms.AUTHENTICATED


@pytest.mark.parametrize("source_status", [terms.INCOMPLETE, terms.LEGACY_UNTRUSTED])
def test_explicit_stress_rejects_unauthenticated_source_snapshots(
    tmp_path,
    source_status,
):
    snapshot = _parse_snapshot(tmp_path)
    snapshot["provenance_status"] = source_status
    for record in snapshot["symbols"].values():
        record["provenance_status"] = source_status
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    with pytest.raises(terms.BrokerTermsError):
        terms.build_explicit_stress_snapshot(snapshot)


def test_explicit_stress_rejects_forged_direct_export_provenance(tmp_path):
    snapshot = _parse_snapshot(tmp_path)
    snapshot["source_export_sha256"] = "not-a-sha256"
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    with pytest.raises(terms.BrokerTermsError):
        terms.build_explicit_stress_snapshot(snapshot)


def test_activation_rejects_a_non_exact_stress_transform(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    stressed = terms.build_explicit_stress_snapshot(snapshot)
    assert stressed["symbols"]["EURUSD"]["swap_long"] == -1.25
    stressed["symbols"]["EURUSD"]["swap_long"] = -0.5
    stressed.pop("content_sha256")
    stressed["content_sha256"] = terms.canonical_sha256(stressed)

    with pytest.raises(terms.BrokerTermsError, match="stress"):
        terms.activate_broker_terms(native, stressed, now_utc=now)


def test_price_race_before_publish_preserves_exact_predecessor_terms_pointer(
    tmp_path,
    monkeypatch,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="predecessor-terms",
        now_utc=now,
    )
    terms_pointer_path = native / "terms" / "ACTIVE.json"
    predecessor = terms_pointer_path.read_bytes()
    real_loader = terms.load_current_price_activation
    calls = 0

    def raced_price_loader(root):
        nonlocal calls
        loaded = real_loader(root)
        calls += 1
        if calls == 2:
            loaded = dict(loaded)
            loaded["pointer_sha256"] = "f" * 64
        return loaded

    monkeypatch.setattr(terms, "load_current_price_activation", raced_price_loader)
    with pytest.raises(terms.BrokerTermsError):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="raced-terms",
            now_utc=now,
        )

    assert calls >= 2
    assert terms_pointer_path.read_bytes() == predecessor


@pytest.mark.parametrize(
    ("staged_artifact", "expected_error"),
    [
        ("schedule", "staged D1 session schedule"),
        ("snapshot", "staged terms snapshot"),
        ("manifest", "staged terms activation manifest"),
    ],
)
def test_final_validation_rejects_damaged_staged_artifacts_without_publication(
    tmp_path,
    monkeypatch,
    staged_artifact,
    expected_error,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    predecessor_snapshot = _parse_snapshot(tmp_path, now=now)
    predecessor_snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    predecessor_snapshot.pop("content_sha256")
    predecessor_snapshot["content_sha256"] = terms.canonical_sha256(
        predecessor_snapshot
    )
    terms.activate_broker_terms(
        native,
        predecessor_snapshot,
        activation_id="terms-predecessor-artifact-race",
        now_utc=now,
    )
    terms_pointer_path = native / "terms" / "ACTIVE.json"
    predecessor_pointer_bytes = terms_pointer_path.read_bytes()
    existing_schedules = {
        path.resolve()
        for path in (native / "terms" / "schedules").rglob("EURUSD.json")
    }
    existing_snapshots = {
        path.resolve()
        for path in (native / "terms" / "snapshots").glob("*.json")
    }

    successor_snapshot = json.loads(json.dumps(predecessor_snapshot))
    successor_snapshot["symbols"]["EURUSD"]["swap_long"] = -1.5
    successor_snapshot.pop("content_sha256")
    successor_snapshot["content_sha256"] = terms.canonical_sha256(
        successor_snapshot
    )
    real_loader = terms.load_current_price_activation
    loader_calls = 0

    def damage_before_final_validation(root):
        nonlocal loader_calls
        loaded = real_loader(root)
        loader_calls += 1
        if loader_calls == 2:
            if staged_artifact == "schedule":
                successor_schedules = {
                    path.resolve()
                    for path in (native / "terms" / "schedules").rglob(
                        "EURUSD.json"
                    )
                } - existing_schedules
                assert len(successor_schedules) == 1
                successor_schedules.pop().unlink()
            elif staged_artifact == "snapshot":
                successor_snapshots = {
                    path.resolve()
                    for path in (native / "terms" / "snapshots").glob("*.json")
                } - existing_snapshots
                assert len(successor_snapshots) == 1
                successor_snapshots.pop().unlink()
            else:
                manifest_path = (
                    native
                    / "terms"
                    / "activations"
                    / "terms-successor-artifact-race.json"
                )
                manifest_path.write_text('{"tampered":true}', encoding="utf-8")
        return loaded

    monkeypatch.setattr(
        terms,
        "load_current_price_activation",
        damage_before_final_validation,
    )
    with pytest.raises(terms.BrokerTermsError, match=expected_error):
        terms.activate_broker_terms(
            native,
            successor_snapshot,
            activation_id="terms-successor-artifact-race",
            now_utc=now,
        )

    assert loader_calls == 2
    assert terms_pointer_path.read_bytes() == predecessor_pointer_bytes
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert resolved["terms_activation_id"] == "terms-predecessor-artifact-race"


def test_public_prune_holds_the_terms_pointer_lock(monkeypatch, tmp_path):
    native = tmp_path / "native"
    state = {"held": False}
    events = []

    @contextmanager
    def observed_lock(root, *, timeout_seconds=30.0):
        assert Path(root) == native.resolve()
        assert timeout_seconds == 30.0
        assert state["held"] is False
        state["held"] = True
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")
            state["held"] = False

    def prune_locked(
        root,
        *,
        max_snapshots,
        max_total_bytes,
        referenced_sha256,
    ):
        assert state["held"] is True
        events.append("prune")
        assert Path(root) == native.resolve()
        assert max_snapshots == 3
        assert max_total_bytes == 4096
        assert tuple(referenced_sha256) == ("a" * 64,)
        return {"deleted": [], "remaining_bytes": 0, "protected_bytes": 0}

    monkeypatch.setattr(terms, "_terms_pointer_lock", observed_lock)
    monkeypatch.setattr(terms, "_prune_terms_store_locked", prune_locked)
    result = terms.prune_terms_store(
        native,
        max_snapshots=3,
        max_total_bytes=4096,
        referenced_sha256=("a" * 64,),
    )

    assert result == {
        "deleted": [],
        "remaining_bytes": 0,
        "protected_bytes": 0,
    }
    assert events == ["enter", "prune", "exit"]
    assert state["held"] is False


def test_price_loader_rejects_torn_active_pointer_collection(tmp_path, monkeypatch):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    pointer_path = (native / "ACTIVE.json").resolve()
    pointer_bytes = pointer_path.read_bytes()
    real_read_bounded = terms._read_bounded
    pointer_reads = 0

    def raced_read(path, max_bytes):
        nonlocal pointer_reads
        payload = real_read_bounded(path, max_bytes)
        if Path(path).resolve() == pointer_path:
            pointer_reads += 1
            if pointer_reads == 2:
                return payload + b"\n"
        return payload

    monkeypatch.setattr(terms, "_read_bounded", raced_read)
    with pytest.raises(
        terms.BrokerTermsError,
        match="native price pointer changed during activation read",
    ):
        terms.load_current_price_activation(native)

    assert pointer_reads == 2
    assert pointer_path.read_bytes() == pointer_bytes


@pytest.mark.parametrize("update_pointer", [True, False])
def test_snapshot_must_remain_fresh_through_commit_or_staged_return(
    tmp_path,
    monkeypatch,
    update_pointer,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot["capture_started_at_utc"] = (
        now - timedelta(milliseconds=100)
    ).isoformat()
    snapshot["captured_at_utc"] = (now + timedelta(milliseconds=500)).isoformat()
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="freshness-predecessor",
        now_utc=now,
    )
    terms_pointer_path = native / "terms" / "ACTIVE.json"
    predecessor_pointer_bytes = terms_pointer_path.read_bytes()
    real_derive = terms._derive_d1_session_events_hashed

    class DerivationClock:
        value = 100.0

        def monotonic(self):
            return self.value

        def sleep(self, seconds):
            self.value += float(seconds)

    clock = DerivationClock()

    def delayed_derivation(*args, **kwargs):
        result = real_derive(*args, **kwargs)
        clock.value += 1.2
        return result

    monkeypatch.setattr(terms, "time", clock)
    monkeypatch.setattr(
        terms,
        "_derive_d1_session_events_hashed",
        delayed_derivation,
    )
    with pytest.raises(terms.BrokerTermsError, match="snapshot is not fresh"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id=f"expired-at-return-{update_pointer}",
            now_utc=now,
            update_pointer=update_pointer,
            max_snapshot_age_seconds=1,
        )

    assert terms_pointer_path.read_bytes() == predecessor_pointer_bytes
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert resolved["terms_activation_id"] == "freshness-predecessor"


def test_snapshot_expiry_in_pre_replace_callback_preserves_predecessor(
    tmp_path,
    monkeypatch,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot["capture_started_at_utc"] = (
        now - timedelta(milliseconds=100)
    ).isoformat()
    snapshot["captured_at_utc"] = (now + timedelta(milliseconds=500)).isoformat()
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="pre-replace-freshness-predecessor",
        now_utc=now,
    )
    terms_pointer_path = native / "terms" / "ACTIVE.json"
    predecessor_pointer_bytes = terms_pointer_path.read_bytes()

    class ReplaceClock:
        value = 200.0

        def monotonic(self):
            return self.value

        def sleep(self, seconds):
            self.value += float(seconds)

    clock = ReplaceClock()
    real_atomic_replace = terms._atomic_replace_json
    callback_events = []

    def delay_after_temp_is_durable(path, value, *, before_replace=None):
        assert Path(path) == terms_pointer_path
        assert before_replace is not None

        def expire_then_validate():
            callback_events.append("before_replace")
            clock.value += 1.2
            before_replace()

        return real_atomic_replace(
            path,
            value,
            before_replace=expire_then_validate,
        )

    monkeypatch.setattr(terms, "time", clock)
    monkeypatch.setattr(
        terms,
        "_atomic_replace_json",
        delay_after_temp_is_durable,
    )
    with pytest.raises(terms.BrokerTermsError, match="snapshot is not fresh"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="expired-inside-pre-replace-callback",
            now_utc=now,
            max_snapshot_age_seconds=1,
        )

    assert callback_events == ["before_replace"]
    assert terms_pointer_path.read_bytes() == predecessor_pointer_bytes
    assert list(terms_pointer_path.parent.glob(".ACTIVE.json.*.tmp")) == []
    resolved = terms.resolve_active_swap_contract(native, "EURUSD", "M5")
    assert (
        resolved["terms_activation_id"]
        == "pre-replace-freshness-predecessor"
    )


def test_update_pointer_false_retains_complete_staged_activation(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    staged = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="staged-terms",
        now_utc=now,
        update_pointer=False,
    )

    assert staged["pointer_updated"] is False
    assert Path(staged["manifest_path"]).is_file()
    assert (native / staged["terms_snapshot_relative_path"]).is_file()
    schedule = native / staged["symbols"]["EURUSD"]["schedule_relative_path"]
    assert schedule.is_file()
    assert not (native / "terms" / "ACTIVE.json").exists()


def test_exact_staged_activation_can_be_revalidated_and_promoted(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)

    staged = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="exact-staged-terms",
        now_utc=now,
        update_pointer=False,
    )
    staged_manifest_bytes = Path(staged["manifest_path"]).read_bytes()
    promoted = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="exact-staged-terms",
        now_utc=now + timedelta(minutes=5),
        update_pointer=True,
        staged_manifest_created_at_utc=now,
    )

    assert promoted["pointer_updated"] is True
    assert promoted["manifest_sha256"] == staged["manifest_sha256"]
    assert Path(promoted["manifest_path"]).read_bytes() == staged_manifest_bytes
    pointer = json.loads(
        (native / "terms" / "ACTIVE.json").read_text(encoding="utf-8")
    )
    assert pointer["activated_at_utc"].startswith("2026-04-13T12:05:")
    assert terms.resolve_active_swap_contract(native, "EURUSD", "M5")[
        "terms_activation_id"
    ] == "exact-staged-terms"


def test_exact_active_overlay_pin_allows_stale_schedule_repair_only(tmp_path):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    predecessor = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="repair-predecessor",
        now_utc=now,
    )
    pointer_path = native / "terms" / "ACTIVE.json"
    predecessor_pointer_sha = _sha(pointer_path)
    later = now + timedelta(days=30)

    with pytest.raises(terms.BrokerTermsError, match="snapshot is not fresh"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="unbound-stale-repair",
            now_utc=later,
        )
    with pytest.raises(terms.BrokerTermsError, match="pointer pin mismatch"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="wrong-pin-repair",
            now_utc=later,
            repair_source_terms_pointer_sha256="0" * 64,
        )

    repaired = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="exact-pinned-repair",
        now_utc=later,
        repair_source_terms_pointer_sha256=predecessor_pointer_sha,
    )
    assert repaired["symbols"]["EURUSD"] == predecessor["symbols"]["EURUSD"]
    assert (
        repaired["repair_source_terms_activation_id"]
        == predecessor["activation_id"]
    )
    assert (
        repaired["repair_source_terms_manifest_sha256"]
        == predecessor["manifest_sha256"]
    )
    assert (
        repaired["repair_source_terms_pointer_sha256"]
        == predecessor_pointer_sha
    )
    assert terms.resolve_active_swap_contract(native, "EURUSD", "M5")[
        "terms_activation_id"
    ] == "exact-pinned-repair"


def test_price_receipt_migration_requires_exact_pin_and_rebinds_once(
    tmp_path,
    monkeypatch,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    predecessor = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="pre-receipt-terms",
        now_utc=now,
    )
    predecessor_pointer_path = native / "terms" / "ACTIVE.json"
    predecessor_pointer_sha = _sha(predecessor_pointer_path)
    pin = terms.build_price_receipt_migration_pin(
        native,
        prepared_at_utc=now,
    )
    assert pin["source_terms_active_pointer_sha256"] == predecessor_pointer_sha
    assert pin["source_terms_manifest_sha256"] == predecessor["manifest_sha256"]

    _move_compact_price_to_receipt_identity(native)

    @contextmanager
    def receipt_scope(root):
        price = terms.load_current_price_activation(root)
        price["semantic_receipt_sha256"] = "e" * 64

        def revalidate():
            current = terms.load_current_price_activation(root)
            if not terms._same_price_identity(price, current):
                raise terms.BrokerTermsError(
                    "receipt-backed native price authority changed"
                )

        yield price, revalidate

    monkeypatch.setattr(
        terms,
        "_receipt_backed_price_authority_scope",
        receipt_scope,
    )
    with pytest.raises(
        terms.BrokerTermsError,
        match="different price activation",
    ):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="ordinary-repair-cannot-cross-price",
            now_utc=now + timedelta(days=30),
            repair_source_terms_pointer_sha256=predecessor_pointer_sha,
        )
    wrong_pin = dict(pin)
    wrong_pin["source_terms_active_pointer_sha256"] = "0" * 64
    with pytest.raises(terms.BrokerTermsError, match="source pin mismatch"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="wrong-migration-pin",
            now_utc=now + timedelta(days=30),
            price_receipt_migration_pin=wrong_pin,
        )
    assert _sha(predecessor_pointer_path) == predecessor_pointer_sha

    price_pointer = json.loads((native / "ACTIVE.json").read_text(encoding="utf-8"))
    price_manifest_path = native / price_pointer["manifest_relative_path"]
    price_manifest = json.loads(price_manifest_path.read_text(encoding="utf-8"))
    price_manifest["previous_active_pointer_sha256"] = "f" * 64
    price_manifest_path.write_text(json.dumps(price_manifest), encoding="utf-8")
    price_pointer["manifest_sha256"] = _sha(price_manifest_path)
    (native / "ACTIVE.json").write_text(json.dumps(price_pointer), encoding="utf-8")
    with pytest.raises(terms.BrokerTermsError, match="direct receipt-backed"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="unrelated-receipt-successor",
            now_utc=now + timedelta(days=30),
            price_receipt_migration_pin=pin,
        )
    price_manifest["previous_active_pointer_sha256"] = pin[
        "predecessor_price_active_pointer_sha256"
    ]
    price_manifest_path.write_text(json.dumps(price_manifest), encoding="utf-8")
    price_pointer["manifest_sha256"] = _sha(price_manifest_path)
    (native / "ACTIVE.json").write_text(json.dumps(price_pointer), encoding="utf-8")

    rebound = terms.activate_broker_terms(
        native,
        snapshot,
        activation_id="receipt-rebound-terms",
        now_utc=now + timedelta(days=30),
        price_receipt_migration_pin=pin,
    )
    migration = rebound["price_receipt_migration"]
    assert rebound["price_activation_id"] == "price-a2"
    assert migration["source_terms_activation_id"] == "pre-receipt-terms"
    assert migration["source_terms_pointer_sha256"] == predecessor_pointer_sha
    assert (
        terms.resolve_active_swap_contract(native, "EURUSD", "M5")[
            "terms_activation_id"
        ]
        == "receipt-rebound-terms"
    )

    with pytest.raises(terms.BrokerTermsError, match="source pin mismatch"):
        terms.activate_broker_terms(
            native,
            snapshot,
            activation_id="migration-pin-reuse",
            now_utc=now + timedelta(days=30),
            price_receipt_migration_pin=pin,
        )


def test_exact_promoted_manifest_terms_schedulability_classification():
    repository = Path(__file__).resolve().parents[1]
    manifest_path = (
        repository
        / "data"
        / "native"
        / "activations"
        / "promoted"
        / "20260719T101758Z-e050a26f.json"
    )
    if not manifest_path.is_file():
        pytest.skip("exact promoted production manifest is not present")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    candidates = {
        (symbol, timeframe)
        for symbol, cells in manifest["entries"].items()
        for timeframe, cell in cells.items()
        if cell["status"] == "evidence_candidate"
    }
    source_by_symbol = {
        symbol: next(
            timeframe
            for timeframe in terms._SCHEDULE_SOURCE_PREFERENCE
            if (symbol, timeframe) in candidates
        )
        for symbol in manifest["entries"]
    }
    schedulable_candidates = {
        cell for cell in candidates if cell[0] in source_by_symbol
    }
    unschedulable = candidates - schedulable_candidates

    assert len(source_by_symbol) == 100
    assert len(candidates) == 418
    assert len(schedulable_candidates) == 418
    assert unschedulable == set()
    assert {
        symbol: source
        for symbol, source in source_by_symbol.items()
        if source != "D1"
    } == {
        "Platinum": "H4",
        "USDMXN": "H4",
        "USDSGD": "H4",
        "USDTHB": "H4",
    }


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


def test_capture_cli_prepares_exact_pin_and_consumes_only_after_success(
    tmp_path,
    monkeypatch,
    capsys,
):
    native, _d1_path, _m5_path = _write_price_fixture(tmp_path)
    now = datetime(2026, 4, 13, 12, tzinfo=UTC)
    snapshot = _parse_snapshot(tmp_path, now=now)
    snapshot["symbols"]["EURUSD"]["d1_anchors"] = [
        {"open_epoch": 1775433600, "server_weekday": 1}
    ]
    snapshot.pop("content_sha256")
    snapshot["content_sha256"] = terms.canonical_sha256(snapshot)
    terms.activate_broker_terms(native, snapshot, now_utc=now)
    pin_path = tmp_path / "terms-receipt-migration.pin.json"

    code = capture_main(
        [
            "--native-root",
            str(native),
            "--prepare-price-receipt-migration-pin",
            str(pin_path),
        ]
    )
    prepared = json.loads(capsys.readouterr().out)
    assert code == 0
    assert prepared["status"] == "MIGRATION_PIN_PREPARED"
    assert prepared["sha256"] == _sha(pin_path)

    code = capture_main(
        [
            "--native-root",
            str(native),
            "--complete-price-receipt-migration-pin",
            str(pin_path),
            "--price-receipt-migration-pin-sha256",
            "0" * 64,
        ]
    )
    invalid = json.loads(capsys.readouterr().out)
    assert code == 2
    assert invalid["status"] == "INVALID"
    assert pin_path.is_file()

    calls = []

    def complete(root, pinned_snapshot, **kwargs):
        calls.append((Path(root), pinned_snapshot, dict(kwargs)))
        return {
            "activation_id": "receipt-rebound",
            "manifest_sha256": "f" * 64,
            "pointer_updated": True,
            "active_symbols": ["EURUSD"],
        }

    monkeypatch.setattr(capture_cli, "activate_broker_terms", complete)
    code = capture_main(
        [
            "--native-root",
            str(native),
            "--complete-price-receipt-migration-pin",
            str(pin_path),
            "--price-receipt-migration-pin-sha256",
            prepared["sha256"],
        ]
    )
    completed = json.loads(capsys.readouterr().out)

    assert code == 0
    assert completed["status"] == "COMPLETE"
    assert completed["pin_consumption"]["status"] == "CONSUMED"
    assert not pin_path.exists()
    assert len(calls) == 1
    assert calls[0][0] == native
    assert calls[0][2]["price_receipt_migration_pin"]["schema_version"] == (
        terms.TERMS_PRICE_RECEIPT_MIGRATION_PIN_SCHEMA
    )


def _prepare_mock_capture_publication(tmp_path, monkeypatch):
    exchange = tmp_path / "exchange"
    exchange.mkdir()
    native = tmp_path / "native"
    roster_path = tmp_path / "roster.json"
    roster = {"EURUSD": "EURUSD"}
    roster_path.write_text(json.dumps(roster), encoding="utf-8")
    nonce = "c" * 48
    state_path = exchange / capture_cli.STATE_NAME
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "fxpm-terms-capture-state-v1",
                "phase": "WAITING_FOR_MQL5_EXPORT",
                "nonce": nonce,
                "requested_at_utc": "2026-04-13T11:57:00+00:00",
                "roster_sha256": capture_cli.canonical_sha256(roster),
            }
        ),
        encoding="utf-8",
    )
    (exchange / capture_cli.RESPONSE_NAME).write_bytes(b"mocked-response")
    activation_calls = []
    state_updates = []

    monkeypatch.setattr(capture_cli, "_response_nonce", lambda _path: nonce)
    monkeypatch.setattr(
        capture_cli,
        "parse_terms_export",
        lambda *_args, **_kwargs: {"snapshot": "mocked"},
    )

    def activate(native_root, snapshot, **kwargs):
        activation_calls.append((Path(native_root), snapshot, dict(kwargs)))
        return {
            "activation_id": "terms-cli-test",
            "manifest_sha256": "b" * 64,
            "pointer_updated": kwargs["update_pointer"],
            "active_symbols": ["EURUSD"],
        }

    monkeypatch.setattr(capture_cli, "activate_broker_terms", activate)
    monkeypatch.setattr(
        capture_cli,
        "_atomic_state",
        lambda path, value: state_updates.append((Path(path), dict(value))),
    )
    argv = [
        "--native-root",
        str(native),
        "--exchange-dir",
        str(exchange),
        "--roster-json",
        str(roster_path),
    ]
    return argv, native, state_path, activation_calls, state_updates


@pytest.mark.parametrize(
    ("extra_args", "expected_pointer_updated", "expected_reason"),
    [
        (
            (),
            True,
            "predecessor retained pending explicit post-canary pruning",
        ),
        (
            ("--no-pointer-update",),
            False,
            "staged activation artifacts retained",
        ),
    ],
)
def test_capture_publication_skips_pruning_by_default_and_when_staged(
    tmp_path,
    monkeypatch,
    capsys,
    extra_args,
    expected_pointer_updated,
    expected_reason,
):
    argv, native, state_path, activation_calls, state_updates = (
        _prepare_mock_capture_publication(tmp_path, monkeypatch)
    )

    def unexpected_prune(*_args, **_kwargs):
        pytest.fail("pruning must remain opt-in after publication")

    monkeypatch.setattr(capture_cli, "prune_terms_store", unexpected_prune)
    code = capture_main([*argv, *extra_args])
    output = json.loads(capsys.readouterr().out)

    assert code == 0
    assert output["status"] == "COMPLETE"
    assert output["pointer_updated"] is expected_pointer_updated
    assert output["retention"] == {
        "status": "SKIPPED",
        "reason": expected_reason,
    }
    assert activation_calls == [
        (
            native,
            {"snapshot": "mocked"},
            {
                "activation_id": None,
                "update_pointer": expected_pointer_updated,
                "max_snapshot_age_seconds": 3600,
            },
        )
    ]
    assert len(state_updates) == 1
    assert state_updates[0][0] == state_path
    assert state_updates[0][1]["phase"] == "COMPLETE"


def test_explicit_post_publication_prune_failure_is_a_retention_warning(
    tmp_path,
    monkeypatch,
    capsys,
):
    argv, native, state_path, activation_calls, state_updates = (
        _prepare_mock_capture_publication(tmp_path, monkeypatch)
    )
    prune_calls = []

    def failing_prune(native_root, *, max_snapshots, max_total_bytes):
        prune_calls.append((Path(native_root), max_snapshots, max_total_bytes))
        raise OSError("simulated prune failure")

    monkeypatch.setattr(capture_cli, "prune_terms_store", failing_prune)
    code = capture_main(
        [
            *argv,
            "--prune-after-publication",
            "--max-snapshots",
            "7",
            "--max-terms-bytes",
            "4096",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 0
    assert output["status"] == "COMPLETE"
    assert output["pointer_updated"] is True
    assert output["retention"] == {
        "status": "WARNING",
        "reason": (
            "post-publication retention failed; the published pointer "
            "remains authoritative"
        ),
        "error_type": "OSError",
        "error": "simulated prune failure",
    }
    assert prune_calls == [(native, 7, 4096)]
    assert activation_calls == [
        (
            native,
            {"snapshot": "mocked"},
            {
                "activation_id": None,
                "update_pointer": True,
                "max_snapshot_age_seconds": 3600,
            },
        )
    ]
    assert len(state_updates) == 1
    assert state_updates[0][0] == state_path
    assert state_updates[0][1]["phase"] == "COMPLETE"


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
