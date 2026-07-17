from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pm_native_source_chain import (
    ArtifactPin,
    BaselineRefreshRequired,
    LEGACY_PREFIX_LIMITATION,
    NativeCellIdentity,
    NativeSourceChainDescriptor,
    NativeSourceChainResolver,
    ORIGIN_ROLE,
    OVERLAY_ROLE,
    SourceChainGap,
    SourceChainLimits,
    SourceIntegrityError,
    SourceRevisionConflict,
    SourceSegment,
    SourceUnavailable,
)


IDENTITY = NativeCellIdentity(
    broker="Test Broker Ltd.",
    server="TestBroker-Real",
    account_id="123456",
    canonical_symbol="ALCOA",
    broker_symbol="ALCOA",
    timeframe="H4",
)


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(64 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _pin(root: Path, relative: str) -> ArtifactPin:
    path = root / Path(relative)
    return ArtifactPin(relative, _sha(path), path.stat().st_size)


def _json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _bar(
    open_time: str,
    close_time: str,
    *,
    price: str = "10.0",
    close: str | None = None,
    volume: str = "100",
) -> dict[str, str]:
    open_ts = pd.Timestamp(open_time)
    close_ts = pd.Timestamp(close_time)
    return {
        "time": open_ts.isoformat(),
        "Open": price,
        "High": str(max(float(price), float(close or price)) + 1.0),
        "Low": str(min(float(price), float(close or price)) - 1.0),
        "Close": close or price,
        "Volume": volume,
        "Spread": "2",
        "RealVolume": "200",
        "_SOURCE_FIRST_TS": open_ts.isoformat(),
        "_SOURCE_LAST_TS": (close_ts - pd.Timedelta(nanoseconds=1)).isoformat(),
        "_BAR_CLOSE_TS": close_ts.isoformat(),
    }


def _write_bars(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False, lineterminator="\n")


def _source_environment(identity: NativeCellIdentity) -> dict[str, Any]:
    return {
        "metadata_status": "available",
        "account": {
            "company": identity.broker,
            "server": identity.server,
            "login": int(identity.account_id),
        },
    }


def _source_identity(identity: NativeCellIdentity) -> dict[str, str]:
    return {
        "broker": identity.broker,
        "server": identity.server,
        "account_id": identity.account_id,
        "source_kind": "broker_native",
    }


def _actual_coverage(rows: list[dict[str, str]]) -> dict[str, Any]:
    if not rows:
        return {
            "actual_first_open_utc": None,
            "actual_last_open_utc": None,
            "actual_last_close_utc": None,
        }
    return {
        "actual_first_open_utc": pd.Timestamp(rows[0]["time"]).isoformat(),
        "actual_last_open_utc": pd.Timestamp(rows[-1]["time"]).isoformat(),
        "actual_last_close_utc": pd.Timestamp(rows[-1]["_BAR_CLOSE_TS"]).isoformat(),
    }


def _bundle(
    root: Path,
    origin_rows: list[dict[str, str]],
    overlay_rows: list[dict[str, str]] | None,
    *,
    identity: NativeCellIdentity = IDENTITY,
    overlay_status: str = "success",
    overlay_failure_status: str | None = None,
    overlay_baseline_override: dict[str, Any] | None = None,
    other_manifest_result: dict[str, Any] | None = None,
    limits: SourceChainLimits | None = None,
) -> NativeSourceChainDescriptor:
    origin_id = "legacy-origin"
    overlay_id = "current-overlay"
    origin_leaf = f"{identity.origin_symbol}/{identity.timeframe}"
    overlay_leaf = f"{identity.canonical_symbol}/{identity.timeframe}"
    origin_prefix = f"runs/{origin_id}"
    overlay_prefix = f"overlays/runs/{overlay_id}"
    origin_bars_rel = f"{origin_prefix}/{origin_leaf}/bars.csv"
    origin_prov_rel = f"{origin_prefix}/{origin_leaf}/provenance.json"
    origin_manifest_rel = f"{origin_prefix}/run_manifest.json"
    _write_bars(root / origin_bars_rel, origin_rows)
    origin_bars = _pin(root, origin_bars_rel)
    origin_provenance = {
        "schema_version": "fxpm-native-bars-v1",
        "run_id": origin_id,
        "immutable_record": True,
        "symbol": identity.origin_symbol,
        "broker_symbol": identity.broker_symbol,
        "timeframe": identity.timeframe,
        "source_kind": "broker_native",
        "status": "success",
        "failure_status": None,
        "rows": len(origin_rows),
        "sha256": origin_bars.sha256,
        "artifact": {
            "relative_path": "bars.csv",
            "sha256": origin_bars.sha256,
            "bytes": origin_bars.bytes,
            "rows": len(origin_rows),
        },
        "coverage": _actual_coverage(origin_rows),
        # Deliberately not equal to the final H4/D1 bar close.  The resolver
        # must never use this request boundary as its ownership seam.
        "request": {"native_fetch_end_exclusive_utc": "2026-01-02T09:40:01Z"},
        "source_environment": _source_environment(identity),
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
    }
    _json(root / origin_prov_rel, origin_provenance)
    origin_prov = _pin(root, origin_prov_rel)
    origin_result = {
        "symbol": identity.origin_symbol,
        "timeframe": identity.timeframe,
        "relative_leaf": origin_leaf,
        "status": "success",
        "failure_status": None,
        "rows": len(origin_rows),
        "sha256": origin_bars.sha256,
    }
    origin_results = [origin_result]
    if other_manifest_result is not None:
        origin_results.append(other_manifest_result)
    origin_manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "run_id": origin_id,
        "complete": True,
        "immutable_record": True,
        "source_environment": _source_environment(identity),
        "results": origin_results,
    }
    _json(root / origin_manifest_rel, origin_manifest)
    origin_manifest_pin = _pin(root, origin_manifest_rel)
    origin_segment = SourceSegment(
        role=ORIGIN_ROLE,
        run_id=origin_id,
        manifest=origin_manifest_pin,
        provenance=origin_prov,
        bars=origin_bars,
        rows=len(origin_rows),
        status="success",
        failure_status=None,
        limitation=LEGACY_PREFIX_LIMITATION,
    )

    segments = [origin_segment]
    if overlay_rows is not None:
        overlay_bars_rel = f"{overlay_prefix}/{overlay_leaf}/bars.csv"
        overlay_prov_rel = f"{overlay_prefix}/{overlay_leaf}/provenance.json"
        overlay_manifest_rel = f"{overlay_prefix}/run_manifest.json"
        _write_bars(root / overlay_bars_rel, overlay_rows)
        overlay_bars = _pin(root, overlay_bars_rel)
        baseline = {
            "run_id": origin_id,
            "manifest_relative_path": origin_manifest_pin.relative_path,
            "manifest_sha256": origin_manifest_pin.sha256,
            "relative_leaf": origin_leaf,
            "provenance_sha256": origin_prov.sha256,
            "bars_sha256": origin_bars.sha256,
            "rows": len(origin_rows),
            "bytes": origin_bars.bytes,
            "actual_last_open_utc": origin_provenance["coverage"][
                "actual_last_open_utc"
            ],
            "actual_last_close_utc": origin_provenance["coverage"][
                "actual_last_close_utc"
            ],
        }
        if overlay_baseline_override:
            baseline.update(overlay_baseline_override)
        overlay_provenance = {
            "schema_version": "fxpm.native_overlay_leaf.v1",
            "run_id": overlay_id,
            "immutable_record": True,
            "canonical_symbol": identity.canonical_symbol,
            "broker_symbol": identity.broker_symbol,
            "timeframe": identity.timeframe,
            "source_identity": _source_identity(identity),
            "baseline": baseline,
            "request": {
                "start_utc": overlay_rows[0]["time"],
                "end_exclusive_utc": pd.Timestamp(
                    overlay_rows[-1]["_BAR_CLOSE_TS"]
                ).isoformat(),
            },
            "status": overlay_status,
            "failure_status": overlay_failure_status,
            "transport_status": overlay_failure_status or "complete",
            "artifact": {
                "relative_path": "bars.csv",
                "sha256": overlay_bars.sha256,
                "bytes": overlay_bars.bytes,
                "rows": len(overlay_rows),
            },
            "coverage": _actual_coverage(overlay_rows),
        }
        _json(root / overlay_prov_rel, overlay_provenance)
        overlay_prov = _pin(root, overlay_prov_rel)
        overlay_result = {
            "canonical_symbol": identity.canonical_symbol,
            "broker_symbol": identity.broker_symbol,
            "timeframe": identity.timeframe,
            "relative_leaf": overlay_leaf,
            "status": overlay_status,
            "failure_status": overlay_failure_status,
            "rows": len(overlay_rows),
            "bytes": overlay_bars.bytes,
            "sha256": overlay_bars.sha256,
            "bars_sha256": overlay_bars.sha256,
            "provenance_sha256": overlay_prov.sha256,
        }
        manifest_baseline = {
            key: baseline[key]
            for key in ("run_id", "manifest_relative_path", "manifest_sha256")
        }
        overlay_manifest = {
            "schema_version": "fxpm.native_overlay_run_manifest.v1",
            "run_id": overlay_id,
            "complete": True,
            "immutable_record": True,
            "source_identity": _source_identity(identity),
            "baseline": manifest_baseline,
            "results": [overlay_result],
        }
        _json(root / overlay_manifest_rel, overlay_manifest)
        overlay_manifest_pin = _pin(root, overlay_manifest_rel)
        segments.append(
            SourceSegment(
                role=OVERLAY_ROLE,
                run_id=overlay_id,
                manifest=overlay_manifest_pin,
                provenance=overlay_prov,
                bars=overlay_bars,
                rows=len(overlay_rows),
                status=overlay_status,
                failure_status=overlay_failure_status,
                limitation=None,
            )
        )
    return NativeSourceChainDescriptor(
        identity=identity,
        segments=tuple(segments),
        limits=limits or SourceChainLimits(chunk_rows=2),
    )


@pytest.mark.parametrize(
    ("timeframe", "origin_rows", "overlay_rows", "expected_seam_close"),
    [
        (
            "H4",
            [
                _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
                _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z", price="11"),
            ],
            [
                _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z", price="11.000"),
                _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z", price="12"),
            ],
            "2026-01-02T08:00:00+00:00",
        ),
        (
            "D1",
            [
                _bar("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
                _bar("2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z", price="11"),
            ],
            [
                _bar("2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z", price="11.0"),
                _bar("2026-01-03T00:00:00Z", "2026-01-04T00:00:00Z", price="12"),
            ],
            "2026-01-03T00:00:00+00:00",
        ),
    ],
)
def test_h4_d1_seam_uses_actual_closed_bar_not_old_request_end(
    tmp_path: Path,
    timeframe: str,
    origin_rows: list[dict[str, str]],
    overlay_rows: list[dict[str, str]],
    expected_seam_close: str,
) -> None:
    identity = replace(IDENTITY, timeframe=timeframe)
    descriptor = _bundle(tmp_path, origin_rows, overlay_rows, identity=identity)

    resolved = NativeSourceChainResolver(tmp_path, descriptor).resolve(
        origin_rows[0]["time"], overlay_rows[-1]["_BAR_CLOSE_TS"]
    )

    assert len(resolved.frame) == 3
    assert resolved.frame.index.is_unique
    assert resolved.receipt.origin_rows_owned == 1
    assert resolved.receipt.overlay_rows_owned == 2
    assert resolved.receipt.overlap_rows_compared == 1
    assert resolved.receipt.overlay_first_owned_close_utc == expected_seam_close
    assert LEGACY_PREFIX_LIMITATION in resolved.receipt.limitations


def test_overlap_revision_conflict_compares_all_execution_values(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z", close="10"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z", close="10.25"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)

    with pytest.raises(SourceRevisionConflict) as raised:
        NativeSourceChainResolver(tmp_path, descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )
    assert raised.value.code == "SOURCE_REVISION_CONFLICT"


def test_no_overlap_is_a_typed_source_gap_not_broker_absence(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T12:00:00Z", "2026-01-02T16:00:00Z"),
        _bar("2026-01-02T16:00:00Z", "2026-01-02T20:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)

    with pytest.raises(SourceChainGap) as raised:
        NativeSourceChainResolver(tmp_path, descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T20:00:00Z"
        )
    assert raised.value.code == "SOURCE_CHAIN_GAP"
    assert "broker" not in str(raised.value).lower()


def test_missing_timestamp_inside_overlap_is_not_silently_precedenced(
    tmp_path: Path,
) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T12:00:00Z", "2026-01-02T16:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)

    with pytest.raises(SourceChainGap, match="omits a timestamp"):
        NativeSourceChainResolver(tmp_path, descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T16:00:00Z"
        )


def test_hash_and_baseline_identity_splices_are_rejected(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)
    overlay_bars = tmp_path / descriptor.segments[1].bars.relative_path
    overlay_bars.write_text(
        overlay_bars.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8"
    )
    with pytest.raises(SourceIntegrityError, match="byte count|SHA-256"):
        NativeSourceChainResolver(tmp_path, descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )

    second_root = tmp_path / "baseline-splice"
    descriptor = _bundle(
        second_root,
        origin,
        overlay,
        overlay_baseline_override={"manifest_sha256": "0" * 64},
    )
    with pytest.raises(SourceIntegrityError, match="baseline binding is spliced"):
        NativeSourceChainResolver(second_root, descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )


def test_case_sensitive_source_identity_is_enforced(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)
    wrong = replace(
        descriptor.identity,
        canonical_symbol="Alcoa",
        broker_symbol="Alcoa",
        origin_symbol="Alcoa",
    )
    wrong_descriptor = NativeSourceChainDescriptor(
        identity=wrong, segments=descriptor.segments, limits=descriptor.limits
    )
    with pytest.raises(SourceIntegrityError, match="path|exactly one"):
        NativeSourceChainResolver(tmp_path, wrong_descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )


def test_segment_overlay_and_output_caps_require_baseline_refresh(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
        _bar("2026-01-02T12:00:00Z", "2026-01-02T16:00:00Z"),
    ]
    descriptor = _bundle(
        tmp_path,
        origin,
        overlay,
        limits=SourceChainLimits(max_overlay_rows=2),
    )
    with pytest.raises(BaselineRefreshRequired, match="row cap") as raised:
        NativeSourceChainResolver(tmp_path, descriptor)
    assert raised.value.code == "BASELINE_REFRESH_REQUIRED"

    ordinary = _bundle(tmp_path / "segments", origin, overlay)
    third = replace(ordinary.segments[1], run_id="third-overlay")
    with pytest.raises(BaselineRefreshRequired, match="at most one"):
        NativeSourceChainDescriptor(
            identity=ordinary.identity,
            segments=ordinary.segments + (third,),
            limits=ordinary.limits,
        )

    output_limited = replace(
        ordinary,
        limits=replace(ordinary.limits, max_output_rows=2),
    )
    with pytest.raises(BaselineRefreshRequired, match="output row cap"):
        NativeSourceChainResolver(tmp_path / "segments", output_limited).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T16:00:00Z"
        )


def test_chunked_selected_cell_only_and_receipt_is_deterministic(tmp_path: Path) -> None:
    origin = [
        _bar(
            (pd.Timestamp("2026-01-01T00:00:00Z") + pd.Timedelta(hours=4 * i)).isoformat(),
            (pd.Timestamp("2026-01-01T04:00:00Z") + pd.Timedelta(hours=4 * i)).isoformat(),
            price=str(10 + i),
        )
        for i in range(5)
    ]
    overlay = [origin[-2], origin[-1]] + [
        _bar("2026-01-01T20:00:00Z", "2026-01-02T00:00:00Z", price="15")
    ]
    other = {
        "symbol": "OTHER",
        "timeframe": "M5",
        "relative_leaf": "OTHER/M5",
        "status": "success",
        "failure_status": None,
        "rows": 999999999,
        "sha256": "not-even-a-digest",
    }
    descriptor = _bundle(
        tmp_path,
        origin,
        overlay,
        other_manifest_result=other,
        limits=SourceChainLimits(chunk_rows=2),
    )
    unselected = tmp_path / "runs/legacy-origin/OTHER/M5/bars.csv"
    unselected.parent.mkdir(parents=True)
    unselected.write_text("this file must never be parsed", encoding="utf-8")
    resolver = NativeSourceChainResolver(tmp_path, descriptor)
    first = resolver.resolve("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    second = resolver.resolve("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")

    assert len(first.frame) == 6
    assert first.receipt.maximum_chunk_rows_observed <= 2
    assert first.receipt.canonical_bytes() == second.receipt.canonical_bytes()
    assert first.receipt.sha256 == second.receipt.sha256
    assert descriptor.canonical_bytes() == NativeSourceChainDescriptor.from_bytes(
        descriptor.canonical_bytes()
    ).canonical_bytes()


def test_overlay_technical_failure_remains_typed_and_never_becomes_no_trade(
    tmp_path: Path,
) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(
        tmp_path,
        origin,
        overlay,
        overlay_status="partial",
        overlay_failure_status="terminal_prefix_unresolved",
    )
    with pytest.raises(SourceUnavailable) as raised:
        NativeSourceChainResolver(tmp_path, descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )
    assert raised.value.code == "SOURCE_UNAVAILABLE"
    assert raised.value.technical_status == "terminal_prefix_unresolved"
    assert "NO_TRADE" not in str(raised.value)


def test_provenance_only_overlay_failure_is_typed_before_any_bar_scan(
    tmp_path: Path,
) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)
    old_overlay = descriptor.segments[1]
    assert old_overlay.bars is not None
    (tmp_path / old_overlay.bars.relative_path).unlink()
    provenance_path = tmp_path / old_overlay.provenance.relative_path
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance.update(
        {
            "status": "failed",
            "failure_status": "terminal_prefix_unsynchronized",
            "transport_status": "terminal_prefix_unsynchronized",
            "coverage": None,
            "artifact": None,
        }
    )
    _json(provenance_path, provenance)
    provenance_pin = _pin(tmp_path, old_overlay.provenance.relative_path)
    manifest_path = tmp_path / old_overlay.manifest.relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["results"][0].update(
        {
            "status": "failed",
            "failure_status": "terminal_prefix_unsynchronized",
            "provenance_sha256": provenance_pin.sha256,
            "bars_sha256": None,
            "rows": 0,
            "bytes": 0,
        }
    )
    _json(manifest_path, manifest)
    failed_segment = replace(
        old_overlay,
        manifest=_pin(tmp_path, old_overlay.manifest.relative_path),
        provenance=provenance_pin,
        bars=None,
        rows=0,
        status="failed",
        failure_status="terminal_prefix_unsynchronized",
    )
    failed_descriptor = replace(
        descriptor, segments=(descriptor.segments[0], failed_segment)
    )

    with pytest.raises(SourceUnavailable) as raised:
        NativeSourceChainResolver(tmp_path, failed_descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )
    assert raised.value.code == "SOURCE_UNAVAILABLE"
    assert raised.value.technical_status == "terminal_prefix_unsynchronized"


def test_csv_reader_closes_deterministically_when_header_validation_fails(
    tmp_path: Path,
) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)
    old = descriptor.segments[1]
    assert old.bars is not None
    bars_path = tmp_path / old.bars.relative_path
    raw = bars_path.read_bytes()
    assert raw.startswith(b"time,")
    bars_path.write_bytes(raw.removeprefix(b"time"))
    bars_pin = _pin(tmp_path, old.bars.relative_path)
    provenance_path = tmp_path / old.provenance.relative_path
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["artifact"].update(
        {"sha256": bars_pin.sha256, "bytes": bars_pin.bytes}
    )
    _json(provenance_path, provenance)
    provenance_pin = _pin(tmp_path, old.provenance.relative_path)
    manifest_path = tmp_path / old.manifest.relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["results"][0].update(
        {
            "sha256": bars_pin.sha256,
            "bars_sha256": bars_pin.sha256,
            "bytes": bars_pin.bytes,
            "provenance_sha256": provenance_pin.sha256,
        }
    )
    _json(manifest_path, manifest)
    malformed = replace(
        old,
        manifest=_pin(tmp_path, old.manifest.relative_path),
        provenance=provenance_pin,
        bars=bars_pin,
    )
    malformed_descriptor = replace(
        descriptor, segments=(descriptor.segments[0], malformed)
    )

    with pytest.raises(SourceIntegrityError, match="missing or duplicate"):
        NativeSourceChainResolver(tmp_path, malformed_descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )
    # This unlink fails with WinError 32 when pandas' TextFileReader is left
    # open on the exceptional generator path.
    bars_path.unlink()
    assert not bars_path.exists()


def test_overlay_provenance_immutability_and_transport_interval_are_enforced(
    tmp_path: Path,
) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
        _bar("2026-01-02T08:00:00Z", "2026-01-02T12:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay)
    old = descriptor.segments[1]
    provenance_path = tmp_path / old.provenance.relative_path
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["immutable_record"] = False
    provenance["request"]["start_utc"] = "2026-01-02T04:00:01Z"
    _json(provenance_path, provenance)
    provenance_pin = _pin(tmp_path, old.provenance.relative_path)
    manifest_path = tmp_path / old.manifest.relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["results"][0]["provenance_sha256"] = provenance_pin.sha256
    _json(manifest_path, manifest)
    tampered = replace(
        old,
        manifest=_pin(tmp_path, old.manifest.relative_path),
        provenance=provenance_pin,
    )
    tampered_descriptor = replace(
        descriptor, segments=(descriptor.segments[0], tampered)
    )
    with pytest.raises(SourceIntegrityError, match="not immutable"):
        NativeSourceChainResolver(tmp_path, tampered_descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )

    provenance["immutable_record"] = True
    _json(provenance_path, provenance)
    provenance_pin = _pin(tmp_path, old.provenance.relative_path)
    manifest["results"][0]["provenance_sha256"] = provenance_pin.sha256
    _json(manifest_path, manifest)
    interval_tampered = replace(
        old,
        manifest=_pin(tmp_path, old.manifest.relative_path),
        provenance=provenance_pin,
    )
    interval_descriptor = replace(
        descriptor, segments=(descriptor.segments[0], interval_tampered)
    )
    with pytest.raises(SourceIntegrityError, match="transport interval"):
        NativeSourceChainResolver(tmp_path, interval_descriptor).resolve(
            "2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"
        )


def test_origin_only_is_available_with_explicit_legacy_limitation(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, None)
    result = NativeSourceChainResolver(tmp_path, descriptor).resolve(
        "2026-01-02T00:00:00Z", "2026-01-02T08:00:00Z"
    )
    assert len(result.frame) == 2
    assert result.receipt.overlay_status is None
    assert result.receipt.limitations == (LEGACY_PREFIX_LIMITATION,)


@pytest.mark.parametrize(
    ("canonical_symbol", "legacy_symbol"),
    [("Platinum", "PLATINUM"), ("Palladium", "PALLADIUM")],
)
def test_only_authenticated_case_sensitive_legacy_symbol_aliases_are_allowed(
    tmp_path: Path, canonical_symbol: str, legacy_symbol: str
) -> None:
    identity = NativeCellIdentity(
        broker=IDENTITY.broker,
        server=IDENTITY.server,
        account_id=IDENTITY.account_id,
        canonical_symbol=canonical_symbol,
        broker_symbol=canonical_symbol,
        timeframe="M5",
        origin_symbol=legacy_symbol,
    )
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T00:05:00Z"),
        _bar("2026-01-02T00:05:00Z", "2026-01-02T00:10:00Z"),
    ]
    overlay = [
        _bar("2026-01-02T00:05:00Z", "2026-01-02T00:10:00Z"),
        _bar("2026-01-02T00:10:00Z", "2026-01-02T00:15:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, overlay, identity=identity)
    result = NativeSourceChainResolver(tmp_path, descriptor).resolve(
        "2026-01-02T00:00:00Z", "2026-01-02T00:15:00Z"
    )
    assert len(result.frame) == 3
    assert descriptor.identity.origin_symbol == legacy_symbol
    assert descriptor.segments[0].provenance.relative_path.endswith(
        f"/{legacy_symbol}/M5/provenance.json"
    )
    assert descriptor.segments[1].provenance.relative_path.endswith(
        f"/{canonical_symbol}/M5/provenance.json"
    )

    with pytest.raises(SourceIntegrityError, match="authenticated legacy alias"):
        NativeCellIdentity(
            broker=IDENTITY.broker,
            server=IDENTITY.server,
            account_id=IDENTITY.account_id,
            canonical_symbol=canonical_symbol,
            broker_symbol=canonical_symbol,
            timeframe="M5",
            origin_symbol=legacy_symbol.lower(),
        )


def test_descriptor_rejects_economic_status_and_unsafe_paths(tmp_path: Path) -> None:
    origin = [
        _bar("2026-01-02T00:00:00Z", "2026-01-02T04:00:00Z"),
        _bar("2026-01-02T04:00:00Z", "2026-01-02T08:00:00Z"),
    ]
    descriptor = _bundle(tmp_path, origin, None)
    with pytest.raises(SourceIntegrityError, match="NO_TRADE"):
        replace(descriptor.segments[0], status="NO_TRADE")
    with pytest.raises(SourceIntegrityError, match="path"):
        ArtifactPin("../escape.csv", "0" * 64, 1)
