import hashlib
import json
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from pm_core import DataLoader, PipelineConfig
from pm_native_activation import (
    ACTIVATION_MANIFEST_SCHEMA,
    ACTIVE_POINTER_SCHEMA,
    sha256_file,
)
from pm_evidence import (
    DEFAULT_MIN_NATIVE_BARS,
    build_calendar_boundaries,
    latest_closed_bar_end,
    trim_to_closed_bars,
    validate_native_provenance,
    validate_timeframe_evidence,
)
from pm_pipeline import OptimizationPipeline


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


def test_calendar_boundaries_are_common_half_open_and_disjoint():
    boundaries = build_calendar_boundaries(
        "2026-01-01T00:00:00Z",
        embargo=pd.Timedelta(days=7),
    )

    train = boundaries.windows["optimizer_train"]
    tuning = boundaries.windows["tuning_validation"]
    lockbox = boundaries.windows["final_lockbox"]
    assert train == (
        pd.Timestamp("2016-12-18T00:00:00Z"),
        pd.Timestamp("2021-12-18T00:00:00Z"),
    )
    assert tuning == (
        pd.Timestamp("2021-12-25T00:00:00Z"),
        pd.Timestamp("2023-12-25T00:00:00Z"),
    )
    assert lockbox == (
        pd.Timestamp("2024-01-01T00:00:00Z"),
        pd.Timestamp("2026-01-01T00:00:00Z"),
    )
    assert train[1] < tuning[0] < tuning[1] < lockbox[0] < lockbox[1]


def test_five_two_two_year_d1_native_evidence_passes_necessary_gate():
    frames = {
        "optimizer_train": _ohlc(pd.date_range("2017-01-02", "2021-12-31", freq="B")),
        "tuning_validation": _ohlc(pd.date_range("2022-01-03", "2023-12-29", freq="B")),
        "final_lockbox": _ohlc(pd.date_range("2024-01-01", "2025-12-31", freq="B")),
    }
    source_meta = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "sha256": "a" * 64,
        "expected_session_coverage_by_partition": {
            "optimizer_train": 0.99,
            "tuning_validation": 0.99,
            "final_lockbox": 0.99,
        },
    }

    verdict = validate_timeframe_evidence("D1", frames, source_meta=source_meta)

    assert verdict.passed, verdict.reason_codes
    assert verdict.partition_rows["optimizer_train"] >= 1260
    assert verdict.partition_rows["final_lockbox"] >= 500


def test_roughly_174_daily_lockbox_bars_fail_even_with_claimed_provenance():
    frames = {
        "optimizer_train": _ohlc(pd.date_range("2022-01-03", periods=520, freq="B")),
        "tuning_validation": _ohlc(pd.date_range("2024-01-02", periods=173, freq="B")),
        "final_lockbox": _ohlc(pd.date_range("2025-01-02", periods=174, freq="B")),
    }
    source_meta = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "sha256": "b" * 64,
        "expected_session_coverage": 0.99,
    }

    verdict = validate_timeframe_evidence("D1", frames, source_meta=source_meta)

    assert not verdict.passed
    assert "INSUFFICIENT_BARS:final_lockbox:174<500" in verdict.reason_codes
    assert any(reason.startswith("INSUFFICIENT_CALENDAR_SPAN:final_lockbox") for reason in verdict.reason_codes)


def test_malformed_floor_config_cannot_weaken_safe_defaults():
    cfg = PipelineConfig(
        evidence_window_years={"optimizer_train": 0, "tuning_validation": "bad"},
        evidence_min_native_bars={
            "D1": {
                "optimizer_train": 1,
                "tuning_validation": -1,
                "final_lockbox": "bad",
            }
        },
        evidence_min_expected_session_coverage=float("nan"),
    )

    assert cfg.evidence_window_years == {
        "optimizer_train": 5,
        "tuning_validation": 2,
        "final_lockbox": 2,
    }
    assert cfg.evidence_min_native_bars["D1"] == DEFAULT_MIN_NATIVE_BARS["D1"]
    assert cfg.evidence_min_expected_session_coverage == 0.95


def test_calendar_production_pins_session_coverage_policy_in_both_directions():
    lower = PipelineConfig(evidence_min_expected_session_coverage=0.0)
    higher = PipelineConfig(evidence_min_expected_session_coverage=1.0)

    assert lower.evidence_min_expected_session_coverage == 0.95
    assert higher.evidence_min_expected_session_coverage == 0.95


def test_native_provenance_requires_explicit_bar_open_timestamp_semantics():
    sidecar = {
        "schema_version": "fxpm-native-bars-v1",
        "symbol": "EURUSD",
        "timeframe": "D1",
        "source_kind": "broker_native",
        "timezone": "UTC",
        "sha256": "a" * 64,
        "rows": 2,
        "start_ts": "2025-01-01T22:00:00Z",
        "end_ts": "2025-01-02T22:00:00Z",
        "complete": True,
    }

    errors = validate_native_provenance(
        sidecar,
        symbol="EURUSD",
        timeframe="D1",
        sha256="a" * 64,
        rows=2,
        first_ts="2025-01-01T22:00:00Z",
        last_ts="2025-01-02T22:00:00Z",
    )

    assert "SIDECAR_TIMESTAMP_SEMANTICS_INVALID" in errors


def test_calendar_trim_prefers_audited_broker_close_over_nominal_d1_utc_close():
    index = pd.to_datetime(
        ["2025-03-29T22:00:00Z", "2025-03-30T21:00:00Z"],
        utc=True,
    )
    frame = _ohlc(index)
    frame["_BAR_CLOSE_TS"] = pd.to_datetime(
        ["2025-03-30T21:00:00Z", "2025-03-31T21:00:00Z"],
        utc=True,
    )
    cutoff = "2025-03-30T21:30:00Z"

    trimmed = trim_to_closed_bars(frame, "D1", as_of_utc=cutoff)
    latest = latest_closed_bar_end(frame, "D1", not_after_utc=cutoff)

    assert list(trimmed.index) == [pd.Timestamp("2025-03-29T22:00:00Z")]
    assert latest == pd.Timestamp("2025-03-30T21:00:00Z")


def test_native_loader_requires_hash_pinned_active_manifest_and_sidecar():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        native = root / "native"
        native.mkdir()
        data_file = native / "runs" / "run-1" / "EURUSD" / "H4" / "bars.csv"
        data_file.parent.mkdir(parents=True)
        frame = _ohlc(pd.date_range("2020-01-01", periods=4, freq="4h"))
        frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(hours=4)
        frame.reset_index(names="time").to_csv(data_file, index=False)
        digest = hashlib.sha256(data_file.read_bytes()).hexdigest()
        sidecar = {
            "schema_version": "fxpm-native-bars-v1",
            "symbol": "EURUSD",
            "timeframe": "H4",
            "source_kind": "broker_native",
            "timezone": "UTC",
            "timestamp_semantics": "bar_open_utc",
            "bar_close_column": "_BAR_CLOSE_TS",
            "sha256": digest,
            "rows": len(frame),
            "start_ts": pd.Timestamp(frame.index[0], tz="UTC").isoformat(),
            "end_ts": pd.Timestamp(frame.index[-1], tz="UTC").isoformat(),
            "complete": True,
            "expected_session_coverage_by_partition": {
                "optimizer_train": 0.99,
                "tuning_validation": 0.99,
                "final_lockbox": 0.99,
            },
        }
        sidecar_path = native / "audits" / "activation-1" / "EURUSD" / "H4" / "bars.meta.json"
        sidecar_path.parent.mkdir(parents=True)
        sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
        manifest = {
            "schema_version": ACTIVATION_MANIFEST_SCHEMA,
            "activation_id": "activation-1",
            "run_id": "run-1",
            "status": "active",
            "entries": {
                "EURUSD": {
                    "H4": {
                        "status": "active",
                        "symbol": "EURUSD",
                        "timeframe": "H4",
                        "bars_relative_path": data_file.relative_to(native).as_posix(),
                        "bars_sha256": digest,
                        "sidecar_relative_path": sidecar_path.relative_to(native).as_posix(),
                        "sidecar_sha256": sha256_file(sidecar_path),
                    }
                }
            },
        }
        manifest_path = native / "activations" / "activation-1.json"
        manifest_path.parent.mkdir()
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        pointer = {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": "activation-1",
            "manifest_relative_path": manifest_path.relative_to(native).as_posix(),
            "manifest_sha256": sha256_file(manifest_path),
        }
        (native / "ACTIVE.json").write_text(json.dumps(pointer), encoding="utf-8")

        loader = DataLoader(root, cache_resampled=False)
        loaded, meta = loader.load_native_timeframe("EURUSD", "H4")
        assert loaded is not None
        assert meta["source_kind"] == "broker_native"
        assert meta["provenance_valid"]
        assert meta["sha256"] == digest

        manifest_2 = json.loads(json.dumps(manifest))
        manifest_2["activation_id"] = "activation-2"
        manifest_2_path = native / "activations" / "activation-2.json"
        manifest_2_path.write_text(json.dumps(manifest_2), encoding="utf-8")
        pointer_2 = {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": "activation-2",
            "manifest_relative_path": manifest_2_path.relative_to(native).as_posix(),
            "manifest_sha256": sha256_file(manifest_2_path),
        }
        (native / "ACTIVE.json").write_text(json.dumps(pointer_2), encoding="utf-8")
        mixed, mixed_meta = loader.load_native_timeframe("EURUSD", "H4")
        assert mixed is None
        assert "NATIVE_ACTIVATION_CHANGED_DURING_LOADER_LIFETIME" in mixed_meta[
            "provenance_errors"
        ]

        # Restore the first activation before testing tamper detection with a
        # fresh loader instance.
        (native / "ACTIVE.json").write_text(json.dumps(pointer), encoding="utf-8")

        sidecar["sha256"] = "0" * 64
        sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
        reloaded, bad_meta = DataLoader(root, cache_resampled=False).load_native_timeframe(
            "EURUSD", "H4"
        )
        assert reloaded is None
        assert not bad_meta["provenance_valid"]
        assert "NATIVE_ACTIVE_SIDECAR_SHA256_MISMATCH" in bad_meta["provenance_errors"]

        # A flat file is never a fallback when the active audited cell is invalid.
        (native / "EURUSD_H4.csv").write_text("time,Open,High,Low,Close\n", encoding="utf-8")
        still_missing, strict_meta = DataLoader(root, cache_resampled=False).load_native_timeframe(
            "EURUSD", "H4"
        )
        assert still_missing is None
        assert "NATIVE_ACTIVE_SIDECAR_SHA256_MISMATCH" in strict_meta["provenance_errors"]


def test_pipeline_evidence_failure_emits_four_markers_before_features_or_optimizer():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cfg = PipelineConfig(
            data_dir=root,
            output_dir=root / "out",
            timeframes=["M5"],
            max_bars=1000,
            historical_stress_audit_bars=0,
            active_universe_bars=1000,
            tuning_validation_bars=50,
            final_lockbox_bars=50,
            split_embargo_bars=5,
            evidence_sufficiency_enabled=True,
            feature_disk_cache_enabled=False,
        )
        pipeline = OptimizationPipeline(cfg)
        frame = _ohlc(pd.date_range("2025-01-01", periods=1000, freq="5min", tz="UTC"))
        window_meta = pipeline._build_m5_workflow_meta(frame)
        window_meta["timeframe_sources"] = {
            "M5": {
                "source_kind": "m5_resampled",
                "provenance_valid": False,
                "provenance_errors": ["TEST_UNVERIFIED"],
            }
        }
        captured = {}
        original_marker_builder = pipeline._evidence_no_trade_markers

        def capture_markers(verdict):
            markers = original_marker_builder(verdict)
            captured.update(markers)
            return markers

        with mock.patch.object(
            pipeline,
            "_load_symbol_timeframe_data",
            return_value=({"M5": frame}, window_meta),
        ), mock.patch(
            "pm_pipeline.validate_regime_params_provenance",
            return_value={"valid": True, "reason": "test", "provenance_path": "test"},
            create=True,
        ), mock.patch.object(
            pipeline,
            "_evidence_no_trade_markers",
            side_effect=capture_markers,
        ), mock.patch(
            "pm_pipeline.StrategyRegistry.get_all_instances",
            return_value=[],
        ), mock.patch(
            "pm_pipeline.FeatureComputer.compute_all",
            side_effect=AssertionError("features must be unreachable"),
        ), mock.patch.object(
            pipeline.regime_optimizer,
            "optimize_symbol",
            side_effect=AssertionError("optimizer must be unreachable"),
        ):
            result = pipeline.run_for_symbol("EURUSD")

        assert not result.success
        assert len(captured) == 4
        assert set(captured) == {"TREND", "RANGE", "BREAKOUT", "CHOP"}
        assert all(marker.is_no_trade_marker() for marker in captured.values())
        verdict = result.data_window_meta["timeframe_evidence"]["M5"]
        assert not verdict["passed"]
        assert any(code.startswith("NATIVE_SOURCE_REQUIRED") for code in verdict["reason_codes"])
