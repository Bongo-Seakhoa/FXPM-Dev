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
    resolve_active_native_artifact,
    sha256_file,
)
from pm_evidence import (
    DEFAULT_MIN_NATIVE_BARS,
    DEFAULT_TIMEFRAME_WINDOW_YEARS,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    build_calendar_boundaries,
    build_timeframe_calendar_boundaries,
    calendar_window_meta,
    earliest_timeframe_evidence_start,
    ensure_utc_index,
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


def test_ensure_utc_index_read_only_fast_path_avoids_full_frame_copy():
    frame = _ohlc(pd.date_range("2025-01-01", periods=10, freq="5min", tz="UTC"))

    borrowed = ensure_utc_index(frame, copy=False)
    isolated = ensure_utc_index(frame)

    assert borrowed is frame
    assert isolated is not frame


def test_calendar_window_meta_borrows_normalized_frame_without_deep_copy():
    frame = _ohlc(pd.date_range("2025-01-01", periods=10_000, freq="5min", tz="UTC"))

    with mock.patch.object(
        frame,
        "copy",
        side_effect=AssertionError("calendar metadata copied the full native frame"),
    ):
        metadata = calendar_window_meta(
            frame,
            frame.index[1_000],
            frame.index[9_000],
        )

    assert metadata["start_index"] == 1_000
    assert metadata["end_index"] == 9_000
    assert metadata["rows"] == 8_000


def test_closed_bar_trim_can_transfer_an_already_closed_frame_without_copy():
    frame = _ohlc(pd.date_range("2025-01-01", periods=100, freq="5min", tz="UTC"))
    frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(minutes=5)

    borrowed = trim_to_closed_bars(
        frame,
        "M5",
        as_of_utc=frame["_BAR_CLOSE_TS"].iloc[-1],
        copy=False,
    )
    isolated = trim_to_closed_bars(
        frame,
        "M5",
        as_of_utc=frame["_BAR_CLOSE_TS"].iloc[-1],
    )

    assert borrowed is frame
    assert isolated is not frame
    pd.testing.assert_frame_equal(isolated, frame)


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


def test_v3_timeframe_windows_lengthen_without_changing_information_cutoff():
    as_of = "2026-01-01T00:00:00Z"
    m5 = build_timeframe_calendar_boundaries(as_of, "M5", embargo="7d")
    d1 = build_timeframe_calendar_boundaries(as_of, "D1", embargo="7d")

    assert m5.contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    assert d1.contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    assert m5.as_of_utc == d1.as_of_utc == pd.Timestamp(as_of)
    assert m5.windows["final_lockbox"][0] == pd.Timestamp("2024-01-01T00:00:00Z")
    assert d1.windows["final_lockbox"] == m5.windows["final_lockbox"]
    assert d1.windows["tuning_validation"] == m5.windows["tuning_validation"]
    assert d1.windows["optimizer_train"][0] < m5.windows["optimizer_train"][0]
    assert (
        earliest_timeframe_evidence_start(as_of, embargo="7d")
        == d1.windows["optimizer_train"][0]
    )


def test_v3_timeframe_window_config_may_tighten_but_not_weaken_priors():
    attempted = {
        "M5": {"optimizer_train": 1, "tuning_validation": 1, "final_lockbox": 1},
        "D1": {"optimizer_train": 15, "tuning_validation": 5, "final_lockbox": 5},
    }
    m5 = build_timeframe_calendar_boundaries(
        "2026-01-01T00:00:00Z",
        "M5",
        timeframe_window_years=attempted,
    )
    d1 = build_timeframe_calendar_boundaries(
        "2026-01-01T00:00:00Z",
        "D1",
        timeframe_window_years=attempted,
    )

    assert (
        m5.windows["optimizer_train"][1].year - m5.windows["optimizer_train"][0].year
    ) == 5
    assert (
        d1.windows["optimizer_train"][1].year - d1.windows["optimizer_train"][0].year
    ) == 15
    assert d1.windows["tuning_validation"] == m5.windows["tuning_validation"]
    assert d1.windows["final_lockbox"] == m5.windows["final_lockbox"]
    assert DEFAULT_TIMEFRAME_WINDOW_YEARS["D1"] == {
        "optimizer_train": 12,
        "tuning_validation": 2,
        "final_lockbox": 2,
    }


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


def test_roughly_174_daily_lockbox_bars_are_diagnostic_not_a_trading_verdict():
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

    assert verdict.passed
    assert verdict.reason_codes == []
    assert "INSUFFICIENT_BARS:final_lockbox:174<500" in verdict.diagnostic_reason_codes
    assert any(
        reason.startswith("INSUFFICIENT_CALENDAR_SPAN:final_lockbox")
        for reason in verdict.diagnostic_reason_codes
    )
    assert verdict.search_sufficiency_authority == (
        "DEFERRED_TO_AUTHENTICATED_HORIZON_ESS_TRADE_POWER_MULTIPLICITY"
    )


def test_low_session_coverage_and_boundary_shortfall_do_not_veto_native_cell():
    frames = {
        "optimizer_train": _ohlc(
            pd.date_range("2024-01-02", periods=520, freq="B", tz="UTC")
        ),
        "tuning_validation": _ohlc(
            pd.date_range("2025-01-02", periods=260, freq="B", tz="UTC")
        ),
        "final_lockbox": _ohlc(
            pd.date_range("2026-01-02", periods=120, freq="B", tz="UTC")
        ),
    }
    source_meta = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "sha256": "c" * 64,
        "expected_session_coverage_by_partition": {
            "optimizer_train": 0.92,
            "tuning_validation": 0.91,
            "final_lockbox": 0.70,
        },
        "observed_session_bars_by_partition": {
            partition: len(frame) for partition, frame in frames.items()
        },
    }
    bounds = {
        "optimizer_train": (
            "2017-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        ),
        "tuning_validation": (
            "2022-01-01T00:00:00Z",
            "2024-01-01T00:00:00Z",
        ),
        "final_lockbox": (
            "2024-01-01T00:00:00Z",
            "2026-07-01T00:00:00Z",
        ),
    }

    verdict = validate_timeframe_evidence(
        "D1",
        frames,
        source_meta=source_meta,
        partition_bounds=bounds,
    )

    assert verdict.passed
    assert verdict.reason_codes == []
    assert any(
        reason.startswith("SESSION_COVERAGE_LOW:final_lockbox")
        for reason in verdict.diagnostic_reason_codes
    )
    assert any(
        reason.startswith("PARTITION_START_GAP:optimizer_train")
        for reason in verdict.diagnostic_reason_codes
    )


def test_declared_observed_row_mismatch_and_empty_partition_remain_blocking():
    frames = {
        "optimizer_train": _ohlc(
            pd.date_range("2020-01-02", periods=520, freq="B", tz="UTC")
        ),
        "tuning_validation": _ohlc(
            pd.date_range("2023-01-02", periods=260, freq="B", tz="UTC")
        ),
        "final_lockbox": pd.DataFrame(),
    }
    source_meta = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "observed_session_bars_by_partition": {
            "optimizer_train": len(frames["optimizer_train"]) + 1,
            "tuning_validation": len(frames["tuning_validation"]),
            "final_lockbox": 0,
        },
    }

    verdict = validate_timeframe_evidence("D1", frames, source_meta=source_meta)

    assert not verdict.passed
    assert "PARTITION_FRAME_EMPTY:final_lockbox" in verdict.reason_codes
    assert any(
        reason.startswith("SESSION_OBSERVED_COUNT_MISMATCH:optimizer_train")
        for reason in verdict.reason_codes
    )


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


def test_native_loader_rejects_canonical_nonauthoritative_activation_without_fallback():
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
        sidecar_path = (
            native / "audits" / "activation-1" / "EURUSD" / "H4" / "bars.meta.json"
        )
        sidecar_path.parent.mkdir(parents=True)
        sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
        manifest = {
            "schema_version": ACTIVATION_MANIFEST_SCHEMA,
            "activation_id": "activation-1",
            "run_id": "run-1",
            "status": "active",
            "production_authoritative": False,
            "entries": {
                "EURUSD": {
                    "H4": {
                        "status": "active",
                        "symbol": "EURUSD",
                        "timeframe": "H4",
                        "bars_relative_path": data_file.relative_to(native).as_posix(),
                        "bars_sha256": digest,
                        "sidecar_relative_path": sidecar_path.relative_to(
                            native
                        ).as_posix(),
                        "sidecar_sha256": sha256_file(sidecar_path),
                    }
                }
            },
        }
        manifest_path = native / "activations" / "activation-1.json"
        manifest_path.parent.mkdir()
        manifest_path.write_bytes(
            (
                json.dumps(
                    manifest,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                )
                + "\n"
            ).encode("utf-8")
        )
        pointer = {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": "activation-1",
            "manifest_relative_path": manifest_path.relative_to(native).as_posix(),
            "manifest_sha256": sha256_file(manifest_path),
            "activated_at_utc": "2025-01-01T00:00:00Z",
            "production_authoritative": False,
        }
        (native / "ACTIVE.json").write_bytes(
            (
                json.dumps(
                    pointer,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                )
                + "\n"
            ).encode("utf-8")
        )

        forensic = resolve_active_native_artifact(
            root,
            "EURUSD",
            "H4",
            production_contract_required=False,
        )
        assert forensic.valid, forensic.errors

        loader = DataLoader(root, cache_resampled=False)
        loaded, meta = loader.load_native_timeframe("EURUSD", "H4")
        assert loaded is None
        assert not meta["provenance_valid"]
        assert meta["provenance_errors"] == [
            "NATIVE_SEMANTIC_PRODUCTION_AUTHORITY_REQUIRED"
        ]

        # A flat file never bypasses the semantic production-authority gate.
        (native / "EURUSD_H4.csv").write_text(
            "time,Open,High,Low,Close\n", encoding="utf-8"
        )
        still_missing, strict_meta = DataLoader(
            root, cache_resampled=False
        ).load_native_timeframe("EURUSD", "H4")
        assert still_missing is None
        assert strict_meta["provenance_errors"] == [
            "NATIVE_SEMANTIC_PRODUCTION_AUTHORITY_REQUIRED"
        ]


def test_pipeline_evidence_failure_is_technical_and_never_emits_trade_verdict():
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
        with (
            mock.patch.object(
                pipeline,
                "_load_symbol_timeframe_data",
                return_value=({"M5": frame}, window_meta),
            ),
            mock.patch(
                "pm_pipeline.validate_regime_params_provenance",
                return_value={
                    "valid": True,
                    "reason": "test",
                    "provenance_path": "test",
                },
                create=True,
            ),
            mock.patch(
                "pm_pipeline.StrategyRegistry.get_all_instances",
                return_value=[],
            ),
            mock.patch(
                "pm_pipeline.FeatureComputer.compute_all",
                side_effect=AssertionError("features must be unreachable"),
            ),
            mock.patch.object(
                pipeline.regime_optimizer,
                "optimize_symbol",
                side_effect=AssertionError("optimizer must be unreachable"),
            ),
        ):
            result = pipeline.run_for_symbol("EURUSD")

        # Invalid source/provenance is an infrastructure/data-integrity failure,
        # not evidence that a strategy should abstain.  The run must fail before
        # feature/search and leave the semantic winner ledger untouched.
        assert not result.success
        assert result.config is None
        assert result.error_message.startswith("TECHNICAL_EVIDENCE_FAILURE:M5:")
        verdict = result.data_window_meta["timeframe_evidence"]["M5"]
        assert not verdict["passed"]
        assert any(
            code.startswith("NATIVE_SOURCE_REQUIRED")
            for code in verdict["reason_codes"]
        )
