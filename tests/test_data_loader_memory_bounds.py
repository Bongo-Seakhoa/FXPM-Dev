from __future__ import annotations

import gc
import hashlib
import json
from types import SimpleNamespace
from unittest import mock
import weakref

import numpy as np
import pandas as pd

from pm_core import DataLoader, PipelineConfig
from pm_pipeline import OptimizationPipeline


def _frame(rows: int = 32) -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=rows, freq="5min", tz="UTC")
    values = 1.0 + np.arange(rows, dtype=np.float64) / 10_000.0
    return pd.DataFrame(
        {
            "Open": values,
            "High": values + 0.001,
            "Low": values - 0.001,
            "Close": values,
            "Volume": np.arange(rows, dtype=np.int64) + 1,
        },
        index=index,
    )


def test_bounded_frame_cache_evicts_lru_under_entry_and_byte_caps(tmp_path):
    loader = DataLoader(tmp_path, cache_resampled=False)
    frame = _frame(128)
    frame_bytes = loader._frame_memory_bytes(frame)

    for index in range(10):
        loader._bounded_frame_cache_put(
            loader._cache,
            loader._cache_sizes,
            f"cell-{index}",
            frame,
            max_entries=2,
            max_bytes=frame_bytes * 2,
        )

    assert list(loader._cache) == ["cell-8", "cell-9"]
    assert set(loader._cache_sizes) == set(loader._cache)
    assert sum(loader._cache_sizes.values()) <= frame_bytes * 2


def test_oversize_frame_is_rejected_before_copy_allocation(tmp_path):
    loader = DataLoader(tmp_path, cache_resampled=False)
    frame = _frame(128)
    frame_bytes = loader._frame_memory_bytes(frame)

    with mock.patch.object(
        frame,
        "copy",
        side_effect=AssertionError("oversize cache entry was copied"),
    ):
        evicted = loader._bounded_frame_cache_put(
            loader._cache,
            loader._cache_sizes,
            "oversize",
            frame,
            max_entries=1,
            max_bytes=frame_bytes - 1,
        )

    assert evicted == []
    assert not loader._cache
    assert not loader._cache_sizes


def test_resample_memory_cache_and_metadata_stay_in_the_same_fixed_window(tmp_path):
    loader = DataLoader(tmp_path, cache_resampled=False)
    loader.RESAMPLE_MEMORY_CACHE_MAX_ENTRIES = 2
    loader.RESAMPLE_MEMORY_CACHE_MAX_BYTES = 16 * 1024 * 1024
    frame = _frame(64)
    source_meta = {
        "mtime": 1.0,
        "mtime_ns": 1,
        "size": 1,
        "sha256": "a" * 64,
        "rows": len(frame),
        "last_index": frame.index[-1].isoformat(),
    }

    for index in range(10):
        loader._save_resample_cache(
            f"SYMBOL{index}",
            "H1",
            source_meta,
            frame,
        )

    assert len(loader._resample_cache) == 2
    assert set(loader._resample_meta) == set(loader._resample_cache)
    assert set(loader._resample_cache_sizes) == set(loader._resample_cache)
    assert sum(loader._resample_cache_sizes.values()) <= (
        loader.RESAMPLE_MEMORY_CACHE_MAX_BYTES
    )


def test_source_hash_cache_is_lru_bounded(tmp_path):
    loader = DataLoader(tmp_path, cache_resampled=False)
    loader.SOURCE_HASH_MAX_ENTRIES = 2
    paths = []
    for index in range(3):
        path = tmp_path / f"source-{index}.txt"
        path.write_text(f"payload-{index}", encoding="utf-8")
        paths.append(path)
        loader._file_sha256(path)

    assert len(loader._source_sha256_cache) == 2
    assert str(paths[0].resolve()) not in {
        cache_key[0] for cache_key in loader._source_sha256_cache
    }


def test_native_loader_transfers_frame_ownership_without_retaining_or_copying_it(
    tmp_path,
):
    data_path = tmp_path / "bars.csv"
    source = _frame(64).copy()
    source["_BAR_CLOSE_TS"] = source.index + pd.Timedelta(minutes=5)
    source.reset_index(names="time").to_csv(data_path, index=False)
    activation = SimpleNamespace(
        valid=True,
        errors=[],
        activation_id="activation-1",
        activation_manifest_sha256="a" * 64,
        activation_manifest_path=tmp_path / "activation.json",
        data_path=data_path,
        sidecar_path=None,
        entry={"sidecar_sha256": "b" * 64},
    )
    loader = DataLoader(tmp_path, cache_resampled=False)

    with (
        mock.patch(
            "pm_core.resolve_active_native_artifact",
            return_value=activation,
        ),
        mock.patch("pm_core.validate_native_provenance", return_value=[]),
    ):
        first, first_meta = loader.load_native_timeframe("EURUSD", "M5")
        assert first is not None
        original_close = float(first.iloc[0]["Close"])
        first.iloc[0, first.columns.get_loc("Close")] = 999.0
        second, second_meta = loader.load_native_timeframe("EURUSD", "M5")

    assert second is not None
    assert float(second.iloc[0]["Close"]) == original_close
    assert first is not second
    assert first_meta["provenance_valid"] is True
    assert second_meta["provenance_valid"] is True
    assert not loader._cache
    assert not loader._cache_sizes
    assert not loader._source_meta


_PIPELINE_TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")


class _FrameRetentionProbe:
    def __init__(self) -> None:
        self.live_frames = 0
        self.live_bytes = 0
        self.peak_frames = 0
        self.peak_bytes = 0
        self.max_frame_bytes = 0

    def track(self, frame: pd.DataFrame) -> pd.DataFrame:
        size = int(frame.memory_usage(index=True, deep=True).sum())
        self.live_frames += 1
        self.live_bytes += size
        self.peak_frames = max(self.peak_frames, self.live_frames)
        self.peak_bytes = max(self.peak_bytes, self.live_bytes)
        self.max_frame_bytes = max(self.max_frame_bytes, size)

        def release() -> None:
            self.live_frames -= 1
            self.live_bytes -= size

        weakref.finalize(frame, release)
        return frame


def _pipeline_native_frame(timeframe: str) -> pd.DataFrame:
    position = _PIPELINE_TIMEFRAMES.index(timeframe)
    rows = 768 + position * 53
    index = pd.date_range(
        "2012-01-01T00:00:00Z",
        "2025-12-31T00:00:00Z",
        periods=rows,
    )
    values = 1.0 + position / 10.0 + np.arange(rows, dtype=np.float64) / 100_000.0
    durations = {
        "M5": pd.Timedelta(minutes=5),
        "M15": pd.Timedelta(minutes=15),
        "M30": pd.Timedelta(minutes=30),
        "H1": pd.Timedelta(hours=1),
        "H4": pd.Timedelta(hours=4),
        "D1": pd.Timedelta(days=1),
    }
    return pd.DataFrame(
        {
            "Open": values,
            "High": values + 0.001,
            "Low": values - 0.001,
            "Close": values + 0.0002,
            "Volume": np.arange(rows, dtype=np.int64) + 1,
            "_BAR_CLOSE_TS": index + durations[timeframe],
        },
        index=index,
    )


def _pipeline_for_native_memory_test(tmp_path) -> OptimizationPipeline:
    return OptimizationPipeline(
        PipelineConfig(
            data_dir=tmp_path,
            output_dir=tmp_path / "out",
            timeframes=list(_PIPELINE_TIMEFRAMES),
            production_split_mode="calendar_lockbox_v2",
            evidence_as_of_utc="2026-01-02T00:00:00Z",
            evidence_require_session_coverage=False,
            feature_disk_cache_enabled=False,
        )
    )


def _native_meta(timeframe: str, frame: pd.DataFrame) -> dict:
    return {
        "source_kind": "broker_native",
        "symbol": "EURUSD",
        "timeframe": timeframe,
        "path": f"synthetic/{timeframe}.csv",
        "sha256": hashlib.sha256(timeframe.encode("ascii")).hexdigest(),
        "rows": len(frame),
        "provenance_valid": True,
        "provenance_errors": [],
    }


def _frame_digest(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(list(frame.columns)).encode("utf-8"))
    digest.update(json.dumps([str(value) for value in frame.dtypes]).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    return digest.hexdigest()


def _pipeline_output_and_evidence(
    pipeline: OptimizationPipeline,
    source,
    metadata: dict,
) -> dict:
    output = {}
    for timeframe in tuple(source):
        frame = source.pop(timeframe)
        windows = metadata["timeframe_windows"][timeframe]["windows"]
        partitions = {
            name: pipeline._slice_time_window(
                frame,
                pd.Timestamp(windows[name]["start_ts"]),
                pd.Timestamp(windows[name]["end_ts"]),
            )
            for name in ("optimizer_train", "tuning_validation", "final_lockbox")
        }
        output[timeframe] = {
            "digest": _frame_digest(frame),
            "rows": len(frame),
            "evidence": pipeline._timeframe_evidence_verdict(
                timeframe, partitions, metadata
            ).to_dict(),
        }
        del partitions, frame
    return output


def test_optimizer_native_loader_retains_one_timeframe_with_eager_parity(
    tmp_path,
    monkeypatch,
):
    control = _pipeline_for_native_memory_test(tmp_path)
    monkeypatch.setattr(
        control.data_loader,
        "load_native_timeframe",
        lambda _symbol, timeframe: (
            (frame := _pipeline_native_frame(timeframe)),
            _native_meta(timeframe, frame),
        ),
    )
    control_source, control_meta = control._load_symbol_timeframe_data("EURUSD")
    eager_frames = {
        timeframe: control_source.pop(timeframe)
        for timeframe in tuple(control_source)
    }
    control_output = _pipeline_output_and_evidence(control, eager_frames, control_meta)
    del eager_frames, control_source
    gc.collect()

    measured = _pipeline_for_native_memory_test(tmp_path)
    probe = _FrameRetentionProbe()

    def load_measured(_symbol: str, timeframe: str):
        frame = probe.track(_pipeline_native_frame(timeframe))
        return frame, _native_meta(timeframe, frame)

    monkeypatch.setattr(
        measured.data_loader,
        "load_native_timeframe",
        load_measured,
    )
    measured_source, measured_meta = measured._load_symbol_timeframe_data("EURUSD")
    measured_output = _pipeline_output_and_evidence(
        measured, measured_source, measured_meta
    )
    gc.collect()

    assert measured_output == control_output
    assert measured_meta == control_meta
    measurement = (
        f"peak_frames={probe.peak_frames}, peak_bytes={probe.peak_bytes}, "
        f"max_frame_bytes={probe.max_frame_bytes}"
    )
    assert probe.peak_frames <= 1, measurement
    assert probe.peak_bytes <= probe.max_frame_bytes, measurement
    assert probe.live_frames == 0, measurement
