from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

import pm_native_activation as native
import pm_native_parity as parity
from tests.native_v5_support import SYMBOL, activate_v5_run, write_v5_run


def _write_bars(path: Path, *, final_close: float = 1.25) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "Date,Open,High,Low,Close,Volume\n"
        "2026-01-02T00:00:00Z,1.0,1.3,0.9,1.2,10\n"
        f"2026-01-03T00:00:00Z,1.2,1.4,1.1,{final_close},11\n",
        encoding="utf-8",
    )


def _write_bars_with_close_axis(path: Path, *, invalid_middle_close: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    middle_close = "not-a-timestamp" if invalid_middle_close else "2026-01-04T00:00:00Z"
    path.write_text(
        "Date,Open,High,Low,Close,Volume,_BAR_CLOSE_TS\n"
        "2026-01-02T00:00:00Z,1.0,1.3,0.9,1.2,10,2026-01-03T00:00:00Z\n"
        f"2026-01-03T00:00:00Z,1.2,1.4,1.1,1.25,11,{middle_close}\n"
        "2026-01-04T00:00:00Z,1.25,1.5,1.2,1.4,12,2026-01-05T00:00:00Z\n",
        encoding="utf-8",
    )


def _normalized_pair() -> tuple[pd.DataFrame, pd.DataFrame]:
    fine_index = pd.date_range("2026-01-02T00:00:00Z", periods=12, freq="5min")
    coarse_index = pd.date_range("2026-01-02T00:00:00Z", periods=4, freq="15min")
    fine = pd.DataFrame(
        {
            "Open": [1.00 + index * 0.01 for index in range(12)],
            "High": [1.03 + index * 0.01 for index in range(12)],
            "Low": [0.99 + index * 0.01 for index in range(12)],
            "Close": [1.02 + index * 0.01 for index in range(12)],
            "Volume": [10 + index for index in range(12)],
        },
        index=fine_index,
    )
    coarse = pd.DataFrame(
        {
            "Open": [1.00, 1.03, 1.06, 1.09],
            "High": [1.05, 1.08, 1.11, 1.14],
            "Low": [0.99, 1.02, 1.05, 1.08],
            "Close": [1.04, 1.07, 1.10, 1.13],
            "Volume": [33, 42, 51, 60],
        },
        index=coarse_index,
    )
    return parity.normalize_ohlc_frame(fine), parity.normalize_ohlc_frame(coarse)


def test_v5_hashed_frame_uses_one_stream_and_matches_legacy_frame(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars_path = tmp_path / "native" / "runs" / "run-a" / "EURUSD" / "D1" / "bars.csv"
    _write_bars(bars_path)
    expected_sha256 = hashlib.sha256(bars_path.read_bytes()).hexdigest()
    legacy_frame = native._v5_read_frame(bars_path)
    assert legacy_frame is not None

    original_open = Path.open
    binary_open_count = 0

    def counted_open(self: Path, *args, **kwargs):
        nonlocal binary_open_count
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == bars_path and mode == "rb":
            binary_open_count += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    monkeypatch.setattr(
        native,
        "sha256_file",
        lambda _path: (_ for _ in ()).throw(AssertionError("separate hash pass")),
    )

    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
    )

    assert binary_open_count == 1
    assert observed_sha256 == expected_sha256
    assert frame is not None
    pd.testing.assert_frame_equal(frame, legacy_frame)


def test_v5_pinned_frame_preserves_pin_errors_and_rejects_before_parse(
    tmp_path: Path,
    monkeypatch,
) -> None:
    native_root = tmp_path / "native"
    relative = "runs/run-a/EURUSD/D1/bars.csv"
    bars_path = native_root / relative
    _write_bars(bars_path)
    actual_sha256 = native.sha256_file(bars_path)
    original_read_csv = native.pd.read_csv
    parse_calls = 0

    def counted_read_csv(*args, **kwargs):
        nonlocal parse_calls
        parse_calls += 1
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(native.pd, "read_csv", counted_read_csv)

    for expected_sha256 in (actual_sha256, "0" * 64, "not-a-sha"):
        legacy_path, legacy_errors = native._verify_pinned_file(
            native_root,
            relative,
            expected_sha256,
            error_prefix="PIN",
        )
        parse_calls_before = parse_calls
        path, frame, observed_sha256, errors = native._v5_read_pinned_frame(
            native_root,
            relative,
            expected_sha256,
            error_prefix="PIN",
        )

        assert path == legacy_path
        assert errors == legacy_errors
        if expected_sha256 == actual_sha256:
            assert parse_calls == parse_calls_before + 1
            legacy_frame = native.normalize_ohlc_frame(original_read_csv(legacy_path))
            assert frame is not None
            pd.testing.assert_frame_equal(frame, legacy_frame)
            assert observed_sha256 == actual_sha256
        elif expected_sha256 == "not-a-sha":
            assert parse_calls == parse_calls_before
            assert frame is None
            assert observed_sha256 is None
        else:
            assert parse_calls == parse_calls_before
            assert frame is None
            assert observed_sha256 == actual_sha256

    parse_calls_before = parse_calls
    path, frame, observed_sha256, errors = native._v5_read_pinned_frame(
        native_root,
        "../outside.csv",
        actual_sha256,
        error_prefix="PIN",
    )
    assert path is None
    assert frame is None
    assert observed_sha256 is None
    assert errors == ["PIN_PATH_UNSAFE"]
    assert parse_calls == parse_calls_before


def test_v5_hashed_frame_rejects_tampered_bytes_before_returning_frame(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars_path = tmp_path / "bars.csv"
    _write_bars(bars_path)
    pinned_sha256 = native.sha256_file(bars_path)
    _write_bars(bars_path, final_close=9.75)
    tampered_sha256 = native.sha256_file(bars_path)
    assert tampered_sha256 != pinned_sha256
    monkeypatch.setattr(
        native.pd,
        "read_csv",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("tampered bytes reached pandas")
        ),
    )

    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        pinned_sha256,
        parse_on_hash_mismatch=False,
    )

    assert frame is None
    assert observed_sha256 == tampered_sha256


def test_v5_hashed_frame_releases_byte_buffer_before_normalization(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars_path = tmp_path / "bars.csv"
    _write_bars(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    buffers: list[BytesIO] = []
    original_normalize = native.normalize_ohlc_frame

    class TrackingBytesIO(BytesIO):
        def __init__(self) -> None:
            super().__init__()
            buffers.append(self)

    def normalize_after_release(frame: pd.DataFrame) -> pd.DataFrame:
        assert buffers and buffers[-1].closed
        return original_normalize(frame)

    monkeypatch.setattr(native, "BytesIO", TrackingBytesIO)
    monkeypatch.setattr(native, "normalize_ohlc_frame", normalize_after_release)

    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
    )

    assert frame is not None
    assert observed_sha256 == expected_sha256
    assert len(buffers) == 1
    assert buffers[0].closed


def test_v5_hashed_frame_hashes_complete_stream_after_parse_failure(
    tmp_path: Path,
) -> None:
    bars_path = tmp_path / "invalid.csv"
    bars_path.write_bytes(b"not,ohlc\n1,2\ntrailing-bytes-are-still-hashed\n")
    expected_sha256 = hashlib.sha256(bars_path.read_bytes()).hexdigest()

    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
    )

    assert frame is None
    assert observed_sha256 == expected_sha256


def test_v5_hashed_frame_enforces_explicit_byte_bound(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars_path = tmp_path / "bars.csv"
    _write_bars(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    monkeypatch.setattr(native, "_MAX_V5_BARS_BYTES", bars_path.stat().st_size - 1)

    with pytest.raises(ValueError, match="NATIVE_V5_BARS_SIZE_LIMIT_EXCEEDED"):
        native._v5_read_hashed_frame(
            bars_path,
            expected_sha256,
            parse_on_hash_mismatch=False,
        )


def test_v5_frame_cache_rehashes_every_hit_and_returns_deep_copy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars_path = tmp_path / "native" / "runs" / "run-a" / "EURUSD" / "D1" / "bars.csv"
    _write_bars(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    cache = native._V5NormalizedFrameCache("EURUSD")
    original_open = Path.open
    original_read_csv = native.pd.read_csv
    binary_opens = 0
    parse_calls = 0

    def counted_open(self: Path, *args, **kwargs):
        nonlocal binary_opens
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == bars_path and mode == "rb":
            binary_opens += 1
        return original_open(self, *args, **kwargs)

    def counted_read_csv(*args, **kwargs):
        nonlocal parse_calls
        parse_calls += 1
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    monkeypatch.setattr(native.pd, "read_csv", counted_read_csv)

    first, first_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert first is not None
    expected_frame = first.copy(deep=True)
    first.iloc[-1, first.columns.get_loc("Close")] = 99.0

    second, second_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )

    assert first_sha256 == second_sha256 == expected_sha256
    assert second is not None
    pd.testing.assert_frame_equal(second, expected_frame)
    assert binary_opens == 2
    assert parse_calls == 1
    assert cache.stats()["frame_count"] == 1
    assert 0 < cache.stats()["memory_bytes"] <= cache.stats()["max_bytes"]
    cache.clear()
    assert cache.stats()["frame_count"] == 0
    assert cache.stats()["memory_bytes"] == 0


def test_v5_frame_cache_detaches_returned_data_index_columns_and_extent(
    tmp_path: Path,
) -> None:
    bars_path = tmp_path / "native" / "runs" / "run-a" / "EURUSD" / "D1" / "bars.csv"
    _write_bars_with_close_axis(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    cache = native._V5NormalizedFrameCache("EURUSD")

    first, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert first is not None
    assert observed_sha256 == expected_sha256
    expected = native._v5_detached_frame_copy(first)
    expected_extent = (
        pd.Timestamp("2026-01-02T00:00:00Z"),
        pd.Timestamp("2026-01-05T00:00:00Z"),
    )
    assert cache.extent_for_verified_read(bars_path, observed_sha256) == (
        True,
        expected_extent,
    )

    first.iloc[-1, first.columns.get_loc("Close")] = 99.0
    first.index.values[0] = pd.Timestamp("2030-01-01T00:00:00Z").to_datetime64()
    first.columns.values[0] = "CORRUPTED"

    second, second_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )

    assert second_sha256 == expected_sha256
    assert second is not None
    pd.testing.assert_frame_equal(second, expected)
    assert cache.extent_for_verified_read(bars_path, second_sha256) == (
        True,
        expected_extent,
    )


def test_v5_cached_invalid_extent_is_reused_without_rescanning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars_path = tmp_path / "native" / "runs" / "run-a" / "EURUSD" / "D1" / "bars.csv"
    _write_bars_with_close_axis(bars_path, invalid_middle_close=True)
    expected_sha256 = native.sha256_file(bars_path)
    cache = native._V5NormalizedFrameCache("EURUSD")
    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is not None
    assert observed_sha256 == expected_sha256
    assert cache.extent_for_verified_read(bars_path, observed_sha256) == (True, None)

    monkeypatch.setattr(
        native,
        "_v5_frame_extent",
        lambda _frame: (_ for _ in ()).throw(AssertionError("extent rescanned")),
    )
    assert (
        native._v5_extent_after_verified_read(
            frame_cache=cache,
            path=bars_path,
            observed_sha256=observed_sha256,
            frame=frame,
        )
        is None
    )


def test_v5_cached_extents_match_public_scope_and_reuse_both_profiles(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = [
        tmp_path / "native" / "runs" / "run-a" / "EURUSD" / timeframe / "bars.csv"
        for timeframe in ("M5", "M15")
    ]
    for path in paths:
        _write_bars_with_close_axis(path)
    cache = native._V5NormalizedFrameCache("EURUSD")
    frames = []
    observed = []
    original_extent = native._v5_frame_extent
    extent_calls = 0

    def counted_extent(frame: pd.DataFrame):
        nonlocal extent_calls
        extent_calls += 1
        return original_extent(frame)

    monkeypatch.setattr(native, "_v5_frame_extent", counted_extent)
    for path in paths:
        frame, digest = native._v5_read_hashed_frame(
            path,
            native.sha256_file(path),
            parse_on_hash_mismatch=False,
            frame_cache=cache,
        )
        assert frame is not None and digest is not None
        frames.append(frame)
        observed.append(digest)
    assert extent_calls == 2

    extents = [
        native._v5_extent_after_verified_read(
            frame_cache=cache,
            path=path,
            observed_sha256=digest,
            frame=frame,
        )
        for path, digest, frame in zip(paths, observed, frames)
    ]
    assert extent_calls == 2

    states = {
        window: {
            "integrity_state": native.INTEGRITY_PASS,
            "evidence_state": native.EVIDENCE_PASS,
            "reason_codes": [],
        }
        for window in native.REQUIRED_AUDIT_WINDOWS
    }
    windows = {
        window: (
            pd.Timestamp("2026-01-02T00:00:00Z"),
            pd.Timestamp("2026-01-05T00:00:00Z"),
        )
        for window in native.REQUIRED_AUDIT_WINDOWS
    }
    expected = native.scope_native_pair_window_states(
        states,
        target_frame=frames[0],
        reference_frame=frames[1],
        partition_windows=windows,
        boundary_tolerance_seconds=0.0,
    )
    assert extent_calls == 4

    for _compatibility_profile in ("current", "frozen"):
        observed_states = native._scope_native_pair_window_states_from_extents(
            states,
            target_extent=extents[0],
            reference_extent=extents[1],
            partition_windows=windows,
            boundary_tolerance_seconds=0.0,
        )
        assert observed_states == expected
    assert extent_calls == 4


def test_v5_frame_cache_evicts_and_fails_closed_on_hash_identity_and_size_change(
    tmp_path: Path,
) -> None:
    bars_path = tmp_path / "native" / "runs" / "run-a" / "EURUSD" / "D1" / "bars.csv"
    _write_bars(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    cache = native._V5NormalizedFrameCache("EURUSD")

    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is not None
    assert observed_sha256 == expected_sha256
    original_stat = bars_path.stat()

    # Preserve the cached stat identity while changing same-length content.
    # Only the mandatory full-byte hash can detect this coherent stat replay.
    _write_bars(bars_path, final_close=9.75)
    assert bars_path.stat().st_size == original_stat.st_size
    os.utime(
        bars_path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is None
    assert observed_sha256 != expected_sha256
    assert cache.stats()["frame_count"] == 0

    # Refill with exact bytes, then change only file identity metadata.
    _write_bars(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    frame, _ = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is not None
    cached_stat = bars_path.stat()
    os.utime(
        bars_path,
        ns=(cached_stat.st_atime_ns, cached_stat.st_mtime_ns + 1_000_000),
    )
    with pytest.raises(
        ValueError,
        match="NATIVE_V5_FRAME_CACHE_FILE_IDENTITY_MISMATCH",
    ):
        native._v5_read_hashed_frame(
            bars_path,
            expected_sha256,
            parse_on_hash_mismatch=False,
            frame_cache=cache,
        )
    assert cache.stats()["frame_count"] == 0

    # A size change also invalidates the entry and cannot return cached data.
    _write_bars(bars_path)
    expected_sha256 = native.sha256_file(bars_path)
    frame, _ = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is not None
    bars_path.write_bytes(bars_path.read_bytes() + b"x")
    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is None
    assert observed_sha256 != expected_sha256
    assert cache.stats()["frame_count"] == 0


def test_v5_frame_cache_hashes_multichunk_tail_before_reuse(
    tmp_path: Path,
) -> None:
    bars_path = tmp_path / "native" / "runs" / "run-a" / "EURUSD" / "M5" / "bars.csv"
    bars_path.parent.mkdir(parents=True)
    index = pd.date_range("2025-01-01T00:00:00Z", periods=30_000, freq="5min")
    large_frame = pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.2,
            "Low": 0.9,
            "Close": 1.1,
            "Volume": 1,
        },
        index=index,
    )
    large_frame.to_csv(bars_path, index_label="Date")
    assert bars_path.stat().st_size > 1024 * 1024
    expected_sha256 = native.sha256_file(bars_path)
    cache = native._V5NormalizedFrameCache("EURUSD")
    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is not None
    assert observed_sha256 == expected_sha256
    cached_stat = bars_path.stat()

    # Alter the last data byte while restoring size and stat identity. A reader
    # that hashes only the first 1 MiB would incorrectly return the cache hit.
    with bars_path.open("r+b") as handle:
        handle.seek(-4, os.SEEK_END)
        tail = handle.read()
        offset = tail.rfind(b"1")
        assert offset >= 0
        handle.seek(-len(tail) + offset, os.SEEK_END)
        handle.write(b"2")
    assert bars_path.stat().st_size == cached_stat.st_size
    os.utime(
        bars_path,
        ns=(cached_stat.st_atime_ns, cached_stat.st_mtime_ns),
    )
    frame, observed_sha256 = native._v5_read_hashed_frame(
        bars_path,
        expected_sha256,
        parse_on_hash_mismatch=False,
        frame_cache=cache,
    )
    assert frame is None
    assert observed_sha256 != expected_sha256
    assert cache.stats()["frame_count"] == 0


def test_v5_frame_cache_enforces_frame_byte_and_symbol_bounds(
    tmp_path: Path,
) -> None:
    paths = []
    for timeframe in ("M5", "M15", "M30"):
        bars_path = (
            tmp_path / "native" / "runs" / "run-a" / "EURUSD" / timeframe / "bars.csv"
        )
        _write_bars(bars_path)
        paths.append(bars_path)

    measuring_cache = native._V5NormalizedFrameCache("EURUSD")
    frame, _ = native._v5_read_hashed_frame(
        paths[0],
        native.sha256_file(paths[0]),
        parse_on_hash_mismatch=False,
        frame_cache=measuring_cache,
    )
    assert frame is not None
    one_frame_bytes = measuring_cache.stats()["memory_bytes"]
    measuring_cache.clear()

    cache = native._V5NormalizedFrameCache(
        "EURUSD",
        max_frames=2,
        max_bytes=one_frame_bytes * 2,
    )
    for bars_path in paths:
        frame, observed_sha256 = native._v5_read_hashed_frame(
            bars_path,
            native.sha256_file(bars_path),
            parse_on_hash_mismatch=False,
            frame_cache=cache,
        )
        assert frame is not None
        assert observed_sha256 == native.sha256_file(bars_path)
    assert cache.stats() == {
        "frame_count": 2,
        "memory_bytes": one_frame_bytes * 2,
        "max_frames": 2,
        "max_bytes": one_frame_bytes * 2,
    }

    byte_limited_cache = native._V5NormalizedFrameCache(
        "EURUSD",
        max_frames=6,
        max_bytes=one_frame_bytes,
    )
    for bars_path in paths[:2]:
        frame, _ = native._v5_read_hashed_frame(
            bars_path,
            native.sha256_file(bars_path),
            parse_on_hash_mismatch=False,
            frame_cache=byte_limited_cache,
        )
        assert frame is not None
    assert byte_limited_cache.stats()["frame_count"] == 1
    assert byte_limited_cache.stats()["memory_bytes"] == one_frame_bytes
    byte_limited_cache.clear()

    oversize_cache = native._V5NormalizedFrameCache(
        "EURUSD",
        max_frames=6,
        max_bytes=one_frame_bytes - 1,
    )
    frame, _ = native._v5_read_hashed_frame(
        paths[0],
        native.sha256_file(paths[0]),
        parse_on_hash_mismatch=False,
        frame_cache=oversize_cache,
    )
    assert frame is not None
    assert oversize_cache.stats()["frame_count"] == 0
    assert oversize_cache.stats()["memory_bytes"] == 0

    other_symbol_path = (
        tmp_path / "native" / "runs" / "run-a" / "GBPUSD" / "D1" / "bars.csv"
    )
    _write_bars(other_symbol_path)
    with pytest.raises(
        ValueError,
        match="NATIVE_V5_FRAME_CACHE_SYMBOL_SCOPE_MISMATCH",
    ):
        native._v5_read_hashed_frame(
            other_symbol_path,
            native.sha256_file(other_symbol_path),
            parse_on_hash_mismatch=False,
            frame_cache=cache,
        )
    cache.clear()

    with pytest.raises(ValueError, match="NATIVE_V5_FRAME_CACHE_FRAME_BOUND_INVALID"):
        native._V5NormalizedFrameCache("EURUSD", max_frames=7)
    with pytest.raises(ValueError, match="NATIVE_V5_FRAME_CACHE_BYTE_BOUND_INVALID"):
        native._V5NormalizedFrameCache(
            "EURUSD",
            max_bytes=512 * 1024 * 1024 + 1,
        )


def test_v5_full_600_cell_read_topology_is_cache_identical(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_root = tmp_path / "native" / "runs" / "run-grid"
    bars_by_cell: dict[tuple[str, str], tuple[Path, str]] = {}
    for symbol_index, symbol in enumerate(native.NATIVE_PRODUCTION_SYMBOLS):
        for timeframe_index, timeframe in enumerate(
            native.NATIVE_PRODUCTION_TIMEFRAMES
        ):
            bars_path = run_root / symbol / timeframe / "bars.csv"
            _write_bars(
                bars_path,
                final_close=1.25 + (symbol_index * 6 + timeframe_index) / 10_000,
            )
            bars_by_cell[(symbol, timeframe)] = (
                bars_path,
                native.sha256_file(bars_path),
            )
    assert len(bars_by_cell) == 600
    bars_paths = {path for path, _sha256 in bars_by_cell.values()}

    original_open = Path.open
    original_read_csv = native.pd.read_csv
    current_mode = ""
    binary_opens: Counter[str] = Counter()
    parse_calls: Counter[str] = Counter()

    def counted_open(self: Path, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if mode == "rb" and self in bars_paths:
            binary_opens[current_mode] += 1
        return original_open(self, *args, **kwargs)

    def counted_read_csv(*args, **kwargs):
        parse_calls[current_mode] += 1
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    monkeypatch.setattr(native.pd, "read_csv", counted_read_csv)

    def fingerprint(frame: pd.DataFrame) -> str:
        payload = {
            "columns": list(frame.columns),
            "index": [value.isoformat() for value in frame.index],
            "rows": frame.astype(object).where(pd.notna(frame), None).values.tolist(),
        }
        return hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def replay(mode: str, *, cache_enabled: bool) -> list[dict[str, object]]:
        nonlocal current_mode
        current_mode = mode
        verdicts: list[dict[str, object]] = []
        for symbol in native.NATIVE_PRODUCTION_SYMBOLS:
            cache = native._V5NormalizedFrameCache(symbol) if cache_enabled else None
            try:
                for timeframe in native.NATIVE_PRODUCTION_TIMEFRAMES:
                    read_commitments = []

                    def read(candidate_timeframe: str) -> tuple[str, str]:
                        path, expected_sha256 = bars_by_cell[
                            (symbol, candidate_timeframe)
                        ]
                        frame, observed_sha256 = native._v5_read_hashed_frame(
                            path,
                            expected_sha256,
                            parse_on_hash_mismatch=False,
                            frame_cache=cache,
                        )
                        assert frame is not None
                        assert observed_sha256 == expected_sha256
                        return candidate_timeframe, fingerprint(frame)

                    read_commitments.append(("target", *read(timeframe)))
                    if timeframe != "D1":
                        read_commitments.append(("daily", *read("D1")))
                    for reference in native.expected_reference_timeframes(timeframe):
                        read_commitments.append(("reference", *read(reference)))
                    verdicts.append(
                        {
                            "cell_id": f"{symbol}|{timeframe}",
                            "read_commitments": read_commitments,
                        }
                    )
            finally:
                if cache is not None:
                    cache.clear()
                    assert cache.stats()["frame_count"] == 0
                    assert cache.stats()["memory_bytes"] == 0
        return verdicts

    uncached = replay("uncached", cache_enabled=False)
    cached = replay("cached", cache_enabled=True)

    assert len(uncached) == len(cached) == 600
    assert cached == uncached
    assert binary_opens == {"uncached": 2300, "cached": 2300}
    assert parse_calls == {"uncached": 2300, "cached": 600}


@pytest.mark.skipif(
    os.environ.get("FXPM_RUN_PRODUCTION_NATIVE_CACHE_CANON") != "1",
    reason="explicit read-only production 600-cell cache canon",
)
def test_v5_production_600_cell_cache_on_off_canonical_verdict_equality(
    monkeypatch,
) -> None:
    data_dir = Path(
        os.environ.get("FXPM_PRODUCTION_NATIVE_CACHE_CANON_DATA_DIR", "data")
    ).resolve()
    native_root = data_dir / "native"
    active_path = native_root / "ACTIVE.json"
    active_before = active_path.read_bytes()
    active_pointer = json.loads(active_before.decode("utf-8"))
    manifest_path = native_root / active_pointer["manifest_relative_path"]
    manifest_before = manifest_path.read_bytes()
    activation_manifest = json.loads(manifest_before.decode("utf-8"))
    run_manifest_path = native_root / activation_manifest["run_manifest_relative_path"]
    run_manifest_before = run_manifest_path.read_bytes()
    run_manifest = json.loads(run_manifest_before.decode("utf-8"))
    successful_run_cells = sum(
        native.classify_native_availability_cell(
            item.get("status"),
            item.get("failure_status"),
            item.get("rows"),
            item.get("resume_disposition"),
        )[0]
        == "success"
        for item in run_manifest["results"]
    )
    original_read = native._v5_read_hashed_frame
    original_read_csv = native.pd.read_csv
    current_mode = ""
    full_hash_accesses: Counter[str] = Counter()
    parse_calls: Counter[str] = Counter()

    def counted_read(
        path,
        expected_sha256,
        *,
        parse_on_hash_mismatch,
        frame_cache=None,
    ):
        full_hash_accesses[current_mode] += 1
        return original_read(
            path,
            expected_sha256,
            parse_on_hash_mismatch=parse_on_hash_mismatch,
            frame_cache=frame_cache,
        )

    def counted_read_csv(*args, **kwargs):
        parse_calls[current_mode] += 1
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(native, "_v5_read_hashed_frame", counted_read)
    monkeypatch.setattr(native.pd, "read_csv", counted_read_csv)

    current_mode = "cached"
    cached = native._resolve_active_native_universe_uncached(
        data_dir,
        native.NATIVE_PRODUCTION_SYMBOLS,
        verify_exclusion_evidence=True,
        production_contract_required=True,
        _v5_frame_cache_enabled=True,
    )
    current_mode = "uncached"
    uncached = native._resolve_active_native_universe_uncached(
        data_dir,
        native.NATIVE_PRODUCTION_SYMBOLS,
        verify_exclusion_evidence=True,
        production_contract_required=True,
        _v5_frame_cache_enabled=False,
    )

    assert cached.valid
    assert uncached.valid
    assert cached == uncached
    states = Counter(
        state for grid in cached.cell_statuses.values() for state in grid.values()
    )
    assert states == {
        native.NATIVE_EVIDENCE_CANDIDATE: 418,
        native.NATIVE_TECHNICAL_ERROR: 182,
    }
    assert sum(states.values()) == 600
    assert full_hash_accesses["cached"] == full_hash_accesses["uncached"]
    assert full_hash_accesses["cached"] > 0
    assert parse_calls["cached"] == successful_run_cells
    assert parse_calls["uncached"] == full_hash_accesses["uncached"]

    artifact_caches: list[native._V5NormalizedFrameCache] = []

    def fail_single_artifact_after_cache_fill(native_root, **kwargs):
        frame_cache = kwargs.get("v5_frame_cache")
        assert isinstance(frame_cache, native._V5NormalizedFrameCache)
        entry = kwargs["entry"]
        bars_path = Path(native_root) / entry["bars_relative_path"]
        frame, observed_sha256 = original_read(
            bars_path,
            entry["bars_sha256"],
            parse_on_hash_mismatch=False,
            frame_cache=frame_cache,
        )
        assert frame is not None
        assert observed_sha256 == entry["bars_sha256"]
        assert frame_cache.stats()["frame_count"] == 1
        artifact_caches.append(frame_cache)
        raise RuntimeError("INJECTED_V5_ARTIFACT_REPLAY_FAILURE")

    monkeypatch.setattr(
        native,
        "_verify_activation_cell_evidence",
        fail_single_artifact_after_cache_fill,
    )
    with pytest.raises(
        RuntimeError,
        match="INJECTED_V5_ARTIFACT_REPLAY_FAILURE",
    ):
        native.resolve_active_native_artifact(
            data_dir,
            "EURUSD",
            "H1",
            production_contract_required=True,
        )
    assert len(artifact_caches) == 1
    assert artifact_caches[0].stats()["frame_count"] == 0
    assert artifact_caches[0].stats()["memory_bytes"] == 0
    assert active_path.read_bytes() == active_before
    assert manifest_path.read_bytes() == manifest_before
    assert run_manifest_path.read_bytes() == run_manifest_before


def test_normalized_cross_resolution_path_is_report_identical_and_does_not_renormalize(
    monkeypatch,
) -> None:
    fine, coarse = _normalized_pair()
    fine_before = fine.copy(deep=True)
    coarse_before = coarse.copy(deep=True)
    arguments = {
        "symbol": "EURUSD",
        "fine_timeframe": "M5",
        "coarse_timeframe": "M15",
        "activation_target_timeframe": "M15",
        "tick_tolerance": 0.00001,
        "thresholds": parity.ParityThresholds(min_compared_native_bars=1),
    }
    profiles = (
        parity.CROSS_RESOLUTION_COMPATIBILITY_CURRENT,
        parity.CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307,
    )
    expected = {
        profile: parity.audit_native_cross_resolution(
            fine,
            coarse,
            compatibility_profile=profile,
            **arguments,
        )
        for profile in profiles
    }

    monkeypatch.setattr(
        parity,
        "normalize_ohlc_frame",
        lambda _frame: (_ for _ in ()).throw(AssertionError("redundant normalization")),
    )
    observed = {
        profile: parity.audit_normalized_native_cross_resolution(
            fine,
            coarse,
            compatibility_profile=profile,
            **arguments,
        )
        for profile in profiles
    }

    assert observed == expected
    pd.testing.assert_frame_equal(fine, fine_before)
    pd.testing.assert_frame_equal(coarse, coarse_before)


def test_normalized_cross_resolution_path_falls_back_for_unsealed_frames() -> None:
    fine, coarse = _normalized_pair()
    raw_fine = fine.reset_index(names="Date")
    raw_coarse = coarse.reset_index(names="Date")
    arguments = {
        "symbol": "EURUSD",
        "fine_timeframe": "M5",
        "coarse_timeframe": "M15",
        "activation_target_timeframe": "M15",
        "tick_tolerance": 0.00001,
        "thresholds": parity.ParityThresholds(min_compared_native_bars=1),
    }

    assert parity.audit_normalized_native_cross_resolution(
        raw_fine,
        raw_coarse,
        **arguments,
    ) == parity.audit_native_cross_resolution(raw_fine, raw_coarse, **arguments)


@pytest.mark.parametrize(
    ("column", "values"),
    (
        ("volume", [10 + index for index in range(12)]),
        ("Volume", [str(10 + index) for index in range(12)]),
        ("real_volume", [20 + index for index in range(12)]),
        ("RealVolume", [str(20 + index) for index in range(12)]),
    ),
)
def test_normalized_cross_resolution_path_falls_back_for_volume_normalization(
    column: str,
    values: list[object],
) -> None:
    fine, coarse = _normalized_pair()
    fine = fine.drop(
        columns=[name for name in ("Volume", "RealVolume") if name in fine]
    )
    fine[column] = values
    arguments = {
        "symbol": "EURUSD",
        "fine_timeframe": "M5",
        "coarse_timeframe": "M15",
        "activation_target_timeframe": "M15",
        "tick_tolerance": 0.00001,
        "thresholds": parity.ParityThresholds(min_compared_native_bars=1),
    }

    assert parity.audit_normalized_native_cross_resolution(
        fine,
        coarse,
        **arguments,
    ) == parity.audit_native_cross_resolution(fine, coarse, **arguments)


def test_normalized_cross_resolution_path_falls_back_for_ohlc_case_alias() -> None:
    fine, coarse = _normalized_pair()
    fine["open"] = fine["Open"]
    arguments = {
        "symbol": "EURUSD",
        "fine_timeframe": "M5",
        "coarse_timeframe": "M15",
        "activation_target_timeframe": "M15",
        "tick_tolerance": 0.00001,
        "thresholds": parity.ParityThresholds(min_compared_native_bars=1),
    }

    assert parity.audit_normalized_native_cross_resolution(
        fine,
        coarse,
        **arguments,
    ) == parity.audit_native_cross_resolution(fine, coarse, **arguments)


def test_v5_production_replay_uses_one_hashed_read_per_required_frame(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    write_v5_run(native_root, "run-read-once")
    activated = activate_v5_run(
        native_root,
        "run-read-once",
        "activation-read-once",
    )
    assert activated["status"] == "failed", activated

    # The compact fixture deliberately lacks the production 100x6 source
    # roster, so activation fails closed.  Publish only a temporary test view
    # to exercise the unchanged runtime replay callsites; all source-contract
    # errors remain present and the resolved universe must stay invalid.
    manifest_path = Path(activated["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "active"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (native_root / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": native.ACTIVE_POINTER_SCHEMA,
                "activation_id": manifest["activation_id"],
                "manifest_relative_path": (
                    f"activations/{manifest['activation_id']}.json"
                ),
                "manifest_sha256": native.sha256_file(manifest_path),
                "activated_at_utc": manifest["created_at_utc"],
                "production_authoritative": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    original_read = native._v5_read_hashed_frame
    original_read_csv = native.pd.read_csv
    original_legacy_read = native._v5_read_frame
    original_sha256_file = native.sha256_file
    reads: list[Path] = []
    parsed_sources: list[object] = []
    legacy_reads: list[Path] = []
    separately_hashed_bars: list[Path] = []
    observed_caches: list[native._V5NormalizedFrameCache] = []

    def counted_read(
        path,
        expected_sha256,
        *,
        parse_on_hash_mismatch,
        frame_cache=None,
    ):
        reads.append(Path(path))
        if frame_cache is not None:
            observed_caches.append(frame_cache)
        return original_read(
            path,
            expected_sha256,
            parse_on_hash_mismatch=parse_on_hash_mismatch,
            frame_cache=frame_cache,
        )

    def counted_read_csv(source, *args, **kwargs):
        parsed_sources.append(source)
        return original_read_csv(source, *args, **kwargs)

    def counted_legacy_read(path):
        legacy_reads.append(Path(path))
        return original_legacy_read(path)

    def counted_sha256_file(path):
        resolved = Path(path)
        if resolved.name == "bars.csv":
            separately_hashed_bars.append(resolved)
        return original_sha256_file(path)

    monkeypatch.setattr(native, "_v5_read_hashed_frame", counted_read)
    monkeypatch.setattr(native.pd, "read_csv", counted_read_csv)
    monkeypatch.setattr(native, "_v5_read_frame", counted_legacy_read)
    monkeypatch.setattr(native, "sha256_file", counted_sha256_file)
    universe = native.resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
    )

    assert not universe.valid
    assert universe.errors
    assert len(reads) == 23
    assert Counter(path.parent.name for path in reads) == {
        "M5": 2,
        "M15": 3,
        "M30": 4,
        "H1": 4,
        "H4": 3,
        "D1": 7,
    }
    assert len(parsed_sources) == 6
    assert all(isinstance(source, BytesIO) for source in parsed_sources)
    assert legacy_reads == []
    assert separately_hashed_bars == []
    assert observed_caches
    assert len({id(cache) for cache in observed_caches}) == 1
    assert observed_caches[0].stats()["frame_count"] == 0
    assert observed_caches[0].stats()["memory_bytes"] == 0

    reads_before = len(reads)
    parses_before = len(parsed_sources)
    uncached_universe = native._resolve_active_native_universe_uncached(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
        _v5_frame_cache_enabled=False,
    )
    assert uncached_universe == universe
    assert len(reads) - reads_before == 23
    assert len(parsed_sources) - parses_before == 23

    exceptional_caches: list[native._V5NormalizedFrameCache] = []

    def fail_after_cache_fill(native_root, **kwargs):
        frame_cache = kwargs.get("v5_frame_cache")
        assert isinstance(frame_cache, native._V5NormalizedFrameCache)
        entry = kwargs["entry"]
        bars_path = Path(native_root) / entry["bars_relative_path"]
        frame, observed_sha256 = original_read(
            bars_path,
            entry["bars_sha256"],
            parse_on_hash_mismatch=False,
            frame_cache=frame_cache,
        )
        assert frame is not None
        assert observed_sha256 == entry["bars_sha256"]
        assert frame_cache.stats()["frame_count"] == 1
        exceptional_caches.append(frame_cache)
        raise RuntimeError("INJECTED_V5_REPLAY_FAILURE")

    monkeypatch.setattr(
        native,
        "_verify_activation_cell_evidence",
        fail_after_cache_fill,
    )
    with pytest.raises(RuntimeError, match="INJECTED_V5_REPLAY_FAILURE"):
        native.resolve_active_native_universe(
            data_dir,
            [SYMBOL],
            verify_exclusion_evidence=True,
        )
    assert len(exceptional_caches) == 1
    assert exceptional_caches[0].stats()["frame_count"] == 0
    assert exceptional_caches[0].stats()["memory_bytes"] == 0
