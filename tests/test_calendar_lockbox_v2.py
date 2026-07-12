import hashlib
import json
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from pm_core import PipelineConfig
from pm_evidence import (
    CALENDAR_SPLIT_MODE,
    EVIDENCE_CONTRACT_VERSION,
    build_calendar_boundaries,
    calendar_window_meta,
    latest_closed_bar_end,
    trim_to_closed_bars,
    validate_timeframe_evidence,
)
from pm_pipeline import OptimizationPipeline
from pm_regime import (
    REGIME_PROVENANCE_FIT_SCOPE,
    REGIME_PROVENANCE_SCHEMA_VERSION,
    RegimeParams,
    regime_params_provenance_path,
    regime_params_sha256,
    validate_regime_params_provenance,
)
from pm_regime_tuner import RegimeParamTuner


def _ohlc(index: pd.DatetimeIndex) -> pd.DataFrame:
    values = 100.0 + np.linspace(0.0, 1.0, len(index))
    return pd.DataFrame(
        {
            "Open": values,
            "High": values + 0.2,
            "Low": values - 0.2,
            "Close": values,
            "Volume": 1,
        },
        index=index,
    )


def _business_h4(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    days = pd.date_range(start.normalize(), end.normalize(), freq="B", inclusive="left")
    index = days.repeat(2) + np.tile(
        np.array([pd.Timedelta(0), pd.Timedelta(hours=4)]),
        len(days),
    )
    return _ohlc(pd.DatetimeIndex(index))


def _source_meta(path: str = "native.csv") -> dict:
    return {
        "source_kind": "broker_native",
        "path": path,
        "size": 123,
        "sha256": "a" * 64,
        "provenance_valid": True,
        "expected_session_coverage_by_partition": {
            "optimizer_train": 0.99,
            "tuning_validation": 0.99,
            "final_lockbox": 0.99,
        },
    }


def test_closed_bar_cutoff_excludes_forming_bar():
    frame = _ohlc(
        pd.date_range("2026-01-01T00:00:00Z", periods=3, freq="5min")
    )
    cutoff = pd.Timestamp("2026-01-01T00:08:00Z")

    as_of = latest_closed_bar_end(frame, "M5", not_after_utc=cutoff)
    trimmed = trim_to_closed_bars(frame, "M5", as_of_utc=as_of)

    assert as_of == pd.Timestamp("2026-01-01T00:05:00Z")
    assert trimmed.index.tolist() == [pd.Timestamp("2026-01-01T00:00:00Z")]


def test_d1_and_h4_use_identical_calendar_boundaries_and_pass_evidence():
    boundaries = build_calendar_boundaries(
        "2026-01-01T00:00:00Z",
        embargo=pd.Timedelta(days=7),
    )
    d1_frames = {}
    h4_frames = {}
    for partition, (start, end) in boundaries.windows.items():
        d1_frames[partition] = _ohlc(
            pd.date_range(start.normalize(), end.normalize(), freq="B", inclusive="left")
        )
        h4_frames[partition] = _business_h4(start, end)
        d1_meta = calendar_window_meta(d1_frames[partition], start, end)
        h4_meta = calendar_window_meta(h4_frames[partition], start, end)
        assert d1_meta["start_ts"] == h4_meta["start_ts"] == start.isoformat()
        assert d1_meta["end_ts"] == h4_meta["end_ts"] == end.isoformat()

    kwargs = {
        "source_meta": _source_meta(),
        "partition_bounds": boundaries.windows,
    }
    d1_verdict = validate_timeframe_evidence("D1", d1_frames, **kwargs)
    h4_verdict = validate_timeframe_evidence("H4", h4_frames, **kwargs)

    assert d1_verdict.passed, d1_verdict.reason_codes
    assert h4_verdict.passed, h4_verdict.reason_codes


def test_calendar_loader_does_not_apply_legacy_max_bars_tail():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        frame = _ohlc(
            pd.date_range("2025-12-01T00:00:00Z", periods=4000, freq="5min")
        )
        meta = _source_meta(str(root / "native" / "EURUSD_M5.csv"))
        meta.update({"rows": len(frame), "last_index": frame.index[-1].isoformat()})
        cfg = PipelineConfig(
            data_dir=root,
            output_dir=root / "out",
            timeframes=["M5"],
            max_bars=1000,
            production_split_mode=CALENDAR_SPLIT_MODE,
            evidence_as_of_utc=(frame.index[-1] + pd.Timedelta(minutes=5)).isoformat(),
            feature_disk_cache_enabled=False,
        )
        pipeline = OptimizationPipeline(cfg)
        with mock.patch.object(
            pipeline.data_loader,
            "load_native_timeframe",
            return_value=(frame.copy(), dict(meta)),
        ):
            by_tf, window_meta = pipeline._load_symbol_timeframe_data("EURUSD")

        assert len(by_tf["M5"]) == 4000
        assert not window_meta["max_bars_applied"]
        assert window_meta["windows"]["workflow_window"]["rows"] == 4000


def test_calendar_tuner_fits_requested_native_timeframe_and_records_authority():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cfg = PipelineConfig(
            data_dir=root,
            output_dir=root / "out",
            production_split_mode=CALENDAR_SPLIT_MODE,
            evidence_as_of_utc="2026-01-01T00:00:00Z",
        )
        tuner = RegimeParamTuner(root, config=cfg)
        m5_index = pd.date_range("2016-01-01T00:00:00Z", "2025-12-31T00:00:00Z", freq="D")
        m5_index = m5_index.append(pd.DatetimeIndex([pd.Timestamp("2025-12-31T23:55:00Z")]))
        m5 = _ohlc(m5_index)
        h4 = _business_h4(
            pd.Timestamp("2016-01-01T00:00:00Z"),
            pd.Timestamp("2026-01-01T00:00:00Z"),
        )
        m5_meta = _source_meta(str(root / "native" / "EURUSD_M5.csv"))
        h4_meta = _source_meta(str(root / "native" / "EURUSD_H4.csv"))
        h4_meta["sha256"] = "b" * 64

        def load_native(_symbol, timeframe):
            return (m5.copy(), dict(m5_meta)) if timeframe == "M5" else (h4.copy(), dict(h4_meta))

        with mock.patch.object(
            tuner.native_data_loader,
            "load_native_timeframe",
            side_effect=load_native,
        ):
            selected, fit_meta, selected_source = tuner._optimizer_train_native_timeframe(
                "EURUSD", "H4"
            )

        assert not selected.empty
        assert selected_source["sha256"] == "b" * 64
        assert fit_meta["split"]["source_authority"] == "broker_native_timeframe"
        assert fit_meta["split"]["contract_version"] == EVIDENCE_CONTRACT_VERSION
        assert fit_meta["train"]["start_ts"] == "2016-12-18T00:00:00+00:00"
        assert fit_meta["train"]["end_ts_exclusive"] == "2021-12-18T00:00:00+00:00"


def test_calendar_regime_provenance_rejects_m5_derived_authority():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "EURUSD_H4.csv"
        source.write_text("native-h4-snapshot", encoding="utf-8")
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        params = RegimeParams(k_confirm=2, gap_min=0.1, k_hold=3).to_dict()
        params_path = root / "regime_params.json"
        params_path.write_text(json.dumps({"EURUSD": {"H4": params}}), encoding="utf-8")

        boundaries = build_calendar_boundaries(
            "2026-01-01T00:00:00Z", embargo=pd.Timedelta(days=7)
        )
        full = _business_h4(
            pd.Timestamp("2016-01-01T00:00:00Z"),
            pd.Timestamp("2026-01-01T00:00:00Z"),
        )
        train = calendar_window_meta(full, *boundaries.windows["optimizer_train"])
        tuning = calendar_window_meta(full, *boundaries.windows["tuning_validation"])
        lockbox = calendar_window_meta(full, *boundaries.windows["final_lockbox"])
        record = {
            "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
            "status": "FIT",
            "symbol": "EURUSD",
            "timeframe": "H4",
            "fit_scope": REGIME_PROVENANCE_FIT_SCOPE,
            "params_sha256": regime_params_sha256(params),
            "source": {
                "source_kind": "broker_native",
                "path": str(source.resolve()),
                "size": source.stat().st_size,
                "sha256": digest,
            },
            "train": train,
            "split": {
                "contract_version": EVIDENCE_CONTRACT_VERSION,
                "production_split_mode": CALENDAR_SPLIT_MODE,
                "source_authority": "m5_resampled",
                "optimizer_train_indices": [train["start_index"], train["end_index"]],
                "tuning_validation_indices": [tuning["start_index"], tuning["end_index"]],
                "final_lockbox_indices": [lockbox["start_index"], lockbox["end_index"]],
            },
        }
        payload = {
            "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
            "split_contract_version": EVIDENCE_CONTRACT_VERSION,
            "cells": {"EURUSD": {"H4": record}},
        }
        sidecar = regime_params_provenance_path(str(params_path))
        sidecar.write_text(json.dumps(payload), encoding="utf-8")

        rejected = validate_regime_params_provenance("EURUSD", "H4", str(params_path))
        assert not rejected["valid"]
        assert "native-timeframe authoritative" in rejected["reason"]

        record["split"]["source_authority"] = "broker_native_timeframe"
        sidecar.write_text(json.dumps(payload), encoding="utf-8")
        accepted = validate_regime_params_provenance(
            "EURUSD",
            "H4",
            str(params_path),
            expected_source={
                "path": str(source),
                "size": source.stat().st_size,
                "sha256": digest,
            },
            expected_train_window=train,
            expected_split_contract=EVIDENCE_CONTRACT_VERSION,
        )
        assert accepted["valid"], accepted["reason"]
