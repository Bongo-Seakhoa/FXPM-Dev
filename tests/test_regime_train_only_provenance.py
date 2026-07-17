import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from pm_core import DataSplitter, PipelineConfig
from pm_evidence import (
    CALENDAR_SPLIT_MODE,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    build_timeframe_calendar_boundaries,
)
from pm_regime import (
    REGIME_PROVENANCE_FIT_SCOPE,
    REGIME_PROVENANCE_SCHEMA_VERSION,
    RegimeParams,
    has_tuned_regime_params,
    regime_params_provenance_path,
    regime_params_sha256,
    validate_regime_params_provenance,
)
from pm_regime_tuner import (
    REGIME_METADATA_MAX_STRING_BYTES,
    RegimeParamTuner,
    RegimeTuningIncompleteError,
)


def _config(data_dir: Path) -> PipelineConfig:
    return PipelineConfig(
        research_mode=True,
        data_dir=data_dir,
        output_dir=data_dir / "out",
        timeframes=["M5"],
        max_bars=1000,
        historical_stress_audit_bars=0,
        active_universe_bars=1000,
        production_split_mode="chronological_lockbox_v1",
        tuning_validation_bars=200,
        final_lockbox_bars=200,
        split_embargo_bars=10,
        overlap_pct=0.0,
        regime_params_file=str(data_dir / "regime_params.json"),
    )


def _bars(rows: int = 1000) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq="5min")
    close = 100.0 + np.linspace(0.0, 5.0, rows)
    return pd.DataFrame(
        {
            "Open": close - 0.1,
            "High": close + 0.2,
            "Low": close - 0.2,
            "Close": close,
            "Volume": np.arange(rows) + 1,
        },
        index=index,
    )


def _write_source(path: Path, frame: pd.DataFrame) -> None:
    payload = frame.reset_index().rename(columns={"index": "time"})
    payload.to_csv(path, index=False)


def _calendar_fit_result(
    tuner: RegimeParamTuner,
    symbol: str,
    timeframe: str,
):
    universe = tuner._native_universe()
    params = RegimeParams()
    source = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "provenance_errors": [],
        "symbol": symbol,
        "timeframe": timeframe,
        "path": str(tuner.data_dir / f"{symbol}_{timeframe}.csv"),
        "size": 0,
        "sha256": "b" * 64,
        "activation_id": universe.activation_id,
        "activation_manifest_sha256": universe.activation_manifest_sha256,
    }
    train = {
        "start_index": 0,
        "end_index": 600,
        "rows": 600,
        "start_ts": "2020-01-01T00:00:00+00:00",
        "end_ts_inclusive": "2021-08-22T00:00:00+00:00",
        "end_ts_exclusive": "2021-08-23T00:00:00+00:00",
    }
    split = {
        "contract_version": universe.calendar_contract["window_contract_version"],
        "production_split_mode": CALENDAR_SPLIT_MODE,
        "source_authority": "broker_native_timeframe",
        "cutoff_authority": "authenticated_native_activation",
        "activation_id": universe.activation_id,
        "activation_manifest_sha256": universe.activation_manifest_sha256,
        "optimizer_train_indices": [0, 600],
        "tuning_validation_indices": [610, 710],
        "final_lockbox_indices": [720, 820],
    }
    receipt = {
        "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
        "status": "FIT",
        "symbol": symbol,
        "timeframe": timeframe,
        "fit_scope": REGIME_PROVENANCE_FIT_SCOPE,
        "generated_at": "2026-01-01T00:00:00+00:00",
        "source": source,
        "train": train,
        "split": split,
        "params_sha256": regime_params_sha256(params.to_dict()),
    }
    receipt["train_regime_support"] = tuner._build_train_regime_support(
        _bars(600),
        params,
        source,
        train,
        symbol,
        timeframe,
    )
    return params, {"provenance": receipt}


class RegimeTrainOnlyTunerTests(unittest.TestCase):
    def test_optimizer_train_is_cut_before_detector_selection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tuner = RegimeParamTuner(root, config=_config(root))
            frame = _bars()

            selected, meta = tuner._optimizer_train_m5(frame)

            self.assertEqual(len(selected), 580)
            self.assertEqual(selected.index[-1], frame.index[579])
            self.assertEqual(meta["train"]["end_ts_exclusive"], frame.index[580].isoformat())
            self.assertEqual(meta["split"]["optimizer_train_indices"], [0, 580])
            self.assertEqual(meta["split"]["tuning_validation_indices"], [590, 790])
            self.assertEqual(meta["split"]["final_lockbox_indices"], [800, 1000])

    def test_resample_drops_partial_boundary_candles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tuner = RegimeParamTuner(root, config=_config(root))
            selected, meta = tuner._optimizer_train_m5(_bars())

            hourly = tuner.data_loader.resample(
                selected,
                "H1",
                start_ts=meta["train"]["start_ts"],
                end_ts_exclusive=meta["train"]["end_ts_exclusive"],
            )

            self.assertTrue(len(hourly) > 0)
            end_exclusive = pd.Timestamp(meta["train"]["end_ts_exclusive"])
            self.assertTrue(((hourly.index + pd.Timedelta(hours=1)) <= end_exclusive).all())

    def test_tune_never_passes_post_train_rows_to_detector(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "EURUSD_M5.csv"
            frame = _bars()
            _write_source(source, frame)
            seen_maxima = []

            class FakeDetector:
                warmup_bars = 10

                def __init__(self, params=None):
                    self.params = params

                def compute_regime_scores(self, data):
                    seen_maxima.append(data.index.max())
                    return pd.DataFrame(
                        {
                            "REGIME": "CHOP",
                            "REGIME_GAP": 0.1,
                            "REGIME_LIVE": "CHOP",
                            "REGIME_WARMUP": np.arange(len(data)) < self.warmup_bars,
                        },
                        index=data.index,
                    )

            tuner = RegimeParamTuner(root, config=_config(root))
            with mock.patch("pm_regime_tuner.MarketRegimeDetector", FakeDetector):
                _params, result = tuner.tune_symbol_timeframe(
                    "EURUSD",
                    "M5",
                    param_space={"k_confirm": [2], "gap_min": [0.1], "k_hold": [3]},
                )

            self.assertTrue(result.get("provenance"))
            self.assertTrue(result["provenance"].get("train_regime_support"))
            self.assertTrue(seen_maxima)
            self.assertLessEqual(max(seen_maxima), frame.index[579])
            self.assertEqual(
                result["provenance"]["internal_validation"]["scope"],
                "newest_20pct_within_optimizer_train",
            )

    def test_native_timeframe_uses_activation_cutoff_without_loading_m5(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = PipelineConfig(
                research_mode=True,
                data_dir=root,
                output_dir=root / "out",
                timeframes=["M5", "M15", "M30", "H1", "H4", "D1"],
                production_split_mode=CALENDAR_SPLIT_MODE,
                evidence_purge_days=7.0,
                regime_params_file=str(root / "regime_params.json"),
            )
            as_of = pd.Timestamp("2026-01-01T00:00:00Z")
            boundaries = build_timeframe_calendar_boundaries(
                as_of,
                "D1",
                timeframe_window_years=config.evidence_timeframe_window_years,
                embargo=pd.Timedelta(days=7),
            )
            frame = _bars(6000)
            frame.index = pd.date_range(
                "2009-01-01", periods=len(frame), freq="1D", tz="UTC"
            )

            class Universe:
                activation_id = "activation-1"
                activation_manifest_sha256 = "a" * 64
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                    "common_as_of_utc": as_of.isoformat(),
                    "timeframe_window_years": config.evidence_timeframe_window_years,
                    "timeframe_calendar_boundaries": {
                        "D1": boundaries.to_dict()["windows"]
                    },
                }

                @staticmethod
                def cell_status(symbol, timeframe):
                    return (
                        "evidence_candidate"
                        if (symbol, timeframe) == ("EURUSD", "D1")
                        else "source_unavailable"
                    )

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()

            def load_native(symbol, timeframe):
                self.assertEqual(timeframe, "D1", "M5 must not be used as a cutoff spine")
                return frame, {
                    "source_kind": "broker_native",
                    "provenance_valid": True,
                    "provenance_errors": [],
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "path": str(root / "native_d1.csv"),
                    "size": 123,
                    "sha256": "b" * 64,
                    "activation_id": Universe.activation_id,
                    "activation_manifest_sha256": (
                        Universe.activation_manifest_sha256
                    ),
                }

            tuner.native_data_loader.load_native_timeframe = load_native
            selected, fit_meta, _source = tuner._optimizer_train_native_timeframe(
                "EURUSD", "D1"
            )

            self.assertGreater(len(selected), 2500)
            self.assertEqual(
                fit_meta["split"]["contract_version"],
                TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            )
            self.assertEqual(
                pd.Timestamp(fit_meta["train"]["start_ts"]),
                boundaries.windows["optimizer_train"][0],
            )

    def test_complete_generation_tunes_only_d5_evidence_candidate_cells(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["M5", "D1"]

            class Universe:
                valid = True
                explicit_contract = True
                errors = []
                activation_id = "activation-d5"
                activation_manifest_sha256 = "d" * 64
                requested_symbols = ("EURUSD", "GBPUSD")
                required_timeframes = ("M5", "D1")
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }
                statuses = {
                    "EURUSD": {"M5": "evidence_candidate", "D1": "evidence_candidate"},
                    "GBPUSD": {"M5": "source_unavailable", "D1": "evidence_candidate"},
                }

                @classmethod
                def cell_status(cls, symbol, timeframe):
                    return cls.statuses[symbol][timeframe]

                @classmethod
                def active_timeframes(cls, symbol):
                    return tuple(
                        timeframe
                        for timeframe in cls.required_timeframes
                        if cls.statuses[symbol][timeframe] == "evidence_candidate"
                    )

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            tuner.tune_symbol_timeframe = mock.Mock(
                side_effect=lambda symbol, timeframe: _calendar_fit_result(
                    tuner,
                    symbol,
                    timeframe,
                )
            )
            params_path = root / "regime_params.json"

            tuner.tune_all(
                ["EURUSD", "GBPUSD"],
                ["M5", "D1"],
                str(params_path),
            )

            self.assertEqual(
                tuner.tune_symbol_timeframe.call_args_list,
                [
                    mock.call("EURUSD", "M5"),
                    mock.call("EURUSD", "D1"),
                    mock.call("GBPUSD", "D1"),
                ],
            )
            sidecar = json.loads(
                regime_params_provenance_path(str(params_path)).read_text(
                    encoding="utf-8"
                )
            )
            expected = ["EURUSD|M5", "EURUSD|D1", "GBPUSD|D1"]
            self.assertEqual(sidecar["generation_status"], "COMPLETE")
            self.assertEqual(sidecar["required_active_cells"], expected)
            self.assertEqual(sidecar["required_evidence_candidate_cells"], expected)
            self.assertEqual(sidecar["fitted_active_cell_count"], 3)

    def test_short_native_cell_is_typed_non_search_not_default_params(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["D1"]

            class Universe:
                activation_id = "activation-short-d1"
                activation_manifest_sha256 = "a" * 64
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            short_frame = _bars(499)
            fit_meta = {
                "train": {"rows": len(short_frame)},
                "split": {
                    "contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                    "production_split_mode": CALENDAR_SPLIT_MODE,
                    "activation_id": Universe.activation_id,
                    "activation_manifest_sha256": (
                        Universe.activation_manifest_sha256
                    ),
                },
            }
            source_meta = {
                "source_kind": "broker_native",
                "provenance_valid": True,
                "provenance_errors": [],
                "symbol": "EURUSD",
                "timeframe": "D1",
                "path": str(root / "native_d1.csv"),
                "size": 123,
                "sha256": "b" * 64,
                "rows": len(short_frame),
                "activation_id": Universe.activation_id,
                "activation_manifest_sha256": Universe.activation_manifest_sha256,
            }
            tuner._optimizer_train_native_timeframe = mock.Mock(
                return_value=(short_frame, fit_meta, source_meta)
            )

            params, result = tuner.tune_symbol_timeframe("EURUSD", "D1")

            self.assertIsNone(params)
            self.assertNotIn("provenance", result)
            decision = result["non_search"]
            self.assertEqual(decision["status"], "NON_SEARCH")
            self.assertEqual(
                decision["decision_kind"], "INSUFFICIENT_TRAIN_HISTORY"
            )
            self.assertEqual(
                decision["reason_code"], "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS"
            )
            self.assertEqual(decision["activation_id"], Universe.activation_id)
            self.assertEqual(
                decision["activation_manifest_sha256"],
                Universe.activation_manifest_sha256,
            )
            self.assertEqual(
                decision["diagnostics"]["observed_optimizer_train_rows"],
                499,
            )
            self.assertFalse(decision["economic_evidence_used"])
            self.assertFalse(decision["economic_trade_verdict_emitted"])
            self.assertFalse(decision["trade_verdict"])
            self.assertFalse(decision["no_trade"])
            self.assertFalse(decision["strategy_search_authorized"])
            tuner._validate_non_search_decision(decision, "EURUSD", "D1")

    def test_native_source_must_match_cached_activation_exactly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE

            class Universe:
                activation_id = "activation-a"
                activation_manifest_sha256 = "a" * 64
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            source = {
                "source_kind": "broker_native",
                "provenance_valid": True,
                "provenance_errors": [],
                "symbol": "EURUSD",
                "timeframe": "D1",
                "path": str(root / "native_d1.csv"),
                "size": 123,
                "sha256": "b" * 64,
                "activation_id": "activation-b",
                "activation_manifest_sha256": "c" * 64,
            }

            with self.assertRaisesRegex(ValueError, "activation id changed"):
                tuner._validate_native_source_identity(source, "EURUSD", "D1")

    def test_activation_switch_before_publication_preserves_previous_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["D1"]

            class UniverseA:
                valid = True
                explicit_contract = True
                errors = []
                activation_id = "activation-a"
                activation_manifest_sha256 = "a" * 64
                requested_symbols = ("EURUSD",)
                required_timeframes = ("D1",)
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

                @staticmethod
                def active_timeframes(symbol):
                    return ("D1",)

            class UniverseB(UniverseA):
                activation_id = "activation-b"
                activation_manifest_sha256 = "b" * 64

            params_path = Path(config.regime_params_file)
            sidecar_path = regime_params_provenance_path(str(params_path))
            old_params = '{"LAST_GOOD": {}}'
            old_sidecar = '{"last": "good"}'
            params_path.write_text(old_params, encoding="utf-8")
            sidecar_path.write_text(old_sidecar, encoding="utf-8")
            tuner = RegimeParamTuner(root, config=config)
            tuner.tune_symbol_timeframe = mock.Mock(
                side_effect=lambda symbol, timeframe: _calendar_fit_result(
                    tuner,
                    symbol,
                    timeframe,
                )
            )

            with mock.patch(
                "pm_regime_tuner.resolve_active_native_universe",
                side_effect=[UniverseA(), UniverseB()],
            ), self.assertRaises(RegimeTuningIncompleteError):
                tuner.tune_all(["EURUSD"], ["D1"], str(params_path))

            self.assertEqual(params_path.read_text(encoding="utf-8"), old_params)
            self.assertEqual(sidecar_path.read_text(encoding="utf-8"), old_sidecar)
            staged = json.loads(
                Path(str(sidecar_path) + ".staging").read_text(encoding="utf-8")
            )
            self.assertIn(
                "active native universe changed during tuning",
                staged["failures"][-1]["reason"],
            )

    def test_rehashed_non_search_cannot_change_scope_or_window_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE

            class Universe:
                activation_id = "activation-non-search"
                activation_manifest_sha256 = "d" * 64
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            original = tuner._build_non_search_decision(
                "EURUSD",
                "D1",
                decision_kind="INSUFFICIENT_TRAIN_HISTORY",
                reason_code="INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
                reason="short train window",
            )
            for field_name, forged_value in (
                ("fit_scope", "final_lockbox"),
                ("window_contract_version", "attacker-contract"),
            ):
                forged = dict(original)
                forged[field_name] = forged_value
                body = dict(forged)
                body.pop("decision_sha256", None)
                forged["decision_sha256"] = hashlib.sha256(
                    json.dumps(
                        body,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                with self.subTest(field=field_name), self.assertRaises(ValueError):
                    tuner._validate_non_search_decision(forged, "EURUSD", "D1")

    def test_malformed_fit_never_replaces_last_promoted_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["D1"]

            class Universe:
                valid = True
                explicit_contract = True
                errors = []
                activation_id = "activation-fit-validation"
                activation_manifest_sha256 = "e" * 64
                requested_symbols = ("EURUSD",)
                required_timeframes = ("D1",)
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

                @staticmethod
                def active_timeframes(symbol):
                    return ("D1",)

            params_path = Path(config.regime_params_file)
            sidecar_path = regime_params_provenance_path(str(params_path))
            old_params = '{"LAST_GOOD": {}}'
            old_sidecar = '{"last": "good"}'
            params_path.write_text(old_params, encoding="utf-8")
            sidecar_path.write_text(old_sidecar, encoding="utf-8")
            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            tuner.tune_symbol_timeframe = mock.Mock(
                return_value=(
                    RegimeParams(),
                    {
                        "provenance": {
                            "status": "FIT",
                            "symbol": "WRONG",
                            "timeframe": "M5",
                            "fit_scope": "final_lockbox",
                        }
                    },
                )
            )

            with self.assertRaises(RegimeTuningIncompleteError):
                tuner.tune_all(["EURUSD"], ["D1"], str(params_path))

            self.assertEqual(params_path.read_text(encoding="utf-8"), old_params)
            self.assertEqual(sidecar_path.read_text(encoding="utf-8"), old_sidecar)

    def test_pair_publication_restores_previous_files_on_each_replace_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for fail_call in (1, 2):
                with self.subTest(fail_call=fail_call):
                    params_path = root / f"params-{fail_call}.json"
                    sidecar_path = root / f"sidecar-{fail_call}.json"
                    params_stage = root / f"params-{fail_call}.staging"
                    sidecar_stage = root / f"sidecar-{fail_call}.staging"
                    old_params = f"old-params-{fail_call}"
                    old_sidecar = f"old-sidecar-{fail_call}"
                    params_path.write_text(old_params, encoding="utf-8")
                    sidecar_path.write_text(old_sidecar, encoding="utf-8")
                    params_stage.write_text("new-params", encoding="utf-8")
                    sidecar_stage.write_text("new-sidecar", encoding="utf-8")
                    real_replace = os.replace
                    calls = 0

                    def fail_once(source, target):
                        nonlocal calls
                        calls += 1
                        if calls == fail_call:
                            raise OSError(f"injected replace failure {fail_call}")
                        return real_replace(source, target)

                    with mock.patch(
                        "pm_regime_tuner.os.replace",
                        side_effect=fail_once,
                    ), self.assertRaises(RegimeTuningIncompleteError):
                        RegimeParamTuner._promote_artifact_pair(
                            params_stage,
                            params_path,
                            sidecar_stage,
                            sidecar_path,
                        )

                    self.assertEqual(
                        params_path.read_text(encoding="utf-8"),
                        old_params,
                    )
                    self.assertEqual(
                        sidecar_path.read_text(encoding="utf-8"),
                        old_sidecar,
                    )

    def test_artifact_paths_must_be_distinct(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shared = root / "shared.json"
            shared.write_text("last-good", encoding="utf-8")
            with self.assertRaisesRegex(RegimeTuningIncompleteError, "distinct"):
                RegimeParamTuner._promote_artifact_pair(
                    root / "params.staging",
                    shared,
                    root / "sidecar.staging",
                    shared,
                )
            self.assertEqual(shared.read_text(encoding="utf-8"), "last-good")

    def test_metadata_bounds_reject_instead_of_truncating(self):
        exact = {"value": "x" * REGIME_METADATA_MAX_STRING_BYTES}
        self.assertEqual(RegimeParamTuner._json_safe_mapping(exact), exact)
        with self.assertRaisesRegex(ValueError, "string exceeds"):
            RegimeParamTuner._json_safe_mapping(
                {"value": "x" * (REGIME_METADATA_MAX_STRING_BYTES + 1)}
            )

    def test_train_support_records_natural_unobserved_regimes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tuner = RegimeParamTuner(root, config=_config(root))
            frame = _bars(100)
            source = {"sha256": "b" * 64}
            train = {
                "start_index": 0,
                "end_index": 100,
                "rows": 100,
                "start_ts": frame.index[0].isoformat(),
                "end_ts_inclusive": frame.index[-1].isoformat(),
                "end_ts_exclusive": (frame.index[-1] + pd.Timedelta(minutes=5)).isoformat(),
            }

            class ChopOnlyDetector:
                warmup_bars = 10

                def __init__(self, params=None):
                    self.params = params

                def compute_regime_scores(self, data):
                    return pd.DataFrame(
                        {
                            "REGIME_LIVE": "CHOP",
                            "REGIME_WARMUP": np.arange(len(data)) < self.warmup_bars,
                        },
                        index=data.index,
                    )

            with mock.patch(
                "pm_regime_tuner.MarketRegimeDetector",
                ChopOnlyDetector,
            ):
                support = tuner._build_train_regime_support(
                    frame,
                    RegimeParams(),
                    source,
                    train,
                    "EURUSD",
                    "M5",
                )
                checked = tuner._validate_train_regime_support(
                    support,
                    source=source,
                    train=train,
                    params_dict=RegimeParams().to_dict(),
                    symbol="EURUSD",
                    timeframe="M5",
                )

            self.assertEqual(checked["eligible_rows"], 89)
            self.assertEqual(checked["counts"]["CHOP"], 89)
            self.assertEqual(checked["observed_regimes"], ["CHOP"])
            self.assertEqual(
                checked["unobserved_regimes"],
                ["TREND", "RANGE", "BREAKOUT"],
            )
            self.assertEqual(
                checked["support_state_by_regime"],
                {
                    "TREND": "UNOBSERVED",
                    "RANGE": "UNOBSERVED",
                    "BREAKOUT": "UNOBSERVED",
                    "CHOP": "EMPIRICALLY_OBSERVED",
                },
            )

    def test_train_support_rejects_future_regime_label(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tuner = RegimeParamTuner(root, config=_config(root))
            frame = _bars(100)
            train = {
                "start_index": 0,
                "end_index": 100,
                "rows": 100,
                "start_ts": frame.index[0].isoformat(),
                "end_ts_inclusive": frame.index[-1].isoformat(),
                "end_ts_exclusive": (frame.index[-1] + pd.Timedelta(minutes=5)).isoformat(),
            }

            class FutureLabelDetector:
                def __init__(self, params=None):
                    self.params = params

                def compute_regime_scores(self, data):
                    labels = np.full(len(data), "CHOP", dtype=object)
                    labels[-1] = "FUTURE_REGIME"
                    return pd.DataFrame(
                        {
                            "REGIME_LIVE": labels,
                            "REGIME_WARMUP": np.arange(len(data)) < 10,
                        },
                        index=data.index,
                    )

            with mock.patch(
                "pm_regime_tuner.MarketRegimeDetector",
                FutureLabelDetector,
            ), self.assertRaisesRegex(ValueError, "unknown label"):
                tuner._build_train_regime_support(
                    frame,
                    RegimeParams(),
                    {"sha256": "b" * 64},
                    train,
                    "EURUSD",
                    "M5",
                )

    def test_rehashed_train_support_tamper_fails_writer_authority(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE

            class Universe:
                activation_id = "activation-support"
                activation_manifest_sha256 = "f" * 64
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            params, result = _calendar_fit_result(tuner, "EURUSD", "D1")
            forged = json.loads(json.dumps(result["provenance"]))
            support = forged["train_regime_support"]
            support["vector_sha256"] = "0" * 64
            body = dict(support)
            body.pop("receipt_sha256", None)
            support["receipt_sha256"] = tuner._canonical_mapping_sha256(body)

            with self.assertRaisesRegex(ValueError, "writer authority"):
                tuner._validate_fit_provenance(
                    forged,
                    params.to_dict(),
                    "EURUSD",
                    "D1",
                )

    def test_calendar_generation_promotes_fit_cells_despite_typed_cell_exclusions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["M5", "D1"]

            class Universe:
                valid = True
                explicit_contract = True
                errors = []
                activation_id = "activation-independent-cells"
                activation_manifest_sha256 = "c" * 64
                requested_symbols = ("EURUSD", "GBPUSD")
                required_timeframes = ("M5", "D1")
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }
                statuses = {
                    "EURUSD": {
                        "M5": "evidence_candidate",
                        "D1": "evidence_candidate",
                    },
                    "GBPUSD": {
                        "M5": "source_unavailable",
                        "D1": "evidence_candidate",
                    },
                }

                @classmethod
                def active_timeframes(cls, symbol):
                    return tuple(
                        timeframe
                        for timeframe in cls.required_timeframes
                        if cls.statuses[symbol][timeframe] == "evidence_candidate"
                    )

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()

            def tune_cell(symbol, timeframe):
                if (symbol, timeframe) == ("EURUSD", "M5"):
                    return _calendar_fit_result(tuner, symbol, timeframe)
                if (symbol, timeframe) == ("EURUSD", "D1"):
                    return None, {
                        "non_search": tuner._build_non_search_decision(
                            symbol,
                            timeframe,
                            decision_kind="INSUFFICIENT_TRAIN_HISTORY",
                            reason_code="INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
                            reason="authenticated train partition has 329 rows",
                            diagnostics={
                                "observed_optimizer_train_rows": 329,
                                "required_optimizer_train_rows": 500,
                            },
                        )
                    }
                return None, {
                    "non_search": tuner._build_non_search_decision(
                        symbol,
                        timeframe,
                        decision_kind="INSUFFICIENT_TRAIN_HISTORY",
                        reason_code="INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
                        reason="authenticated train partition has 250 rows",
                        diagnostics={
                            "observed_optimizer_train_rows": 250,
                            "required_optimizer_train_rows": 500,
                        },
                    )
                }

            tuner.tune_symbol_timeframe = mock.Mock(side_effect=tune_cell)
            params_path = root / "regime_params.json"

            result = tuner.tune_all(
                ["EURUSD", "GBPUSD"],
                ["M5", "D1"],
                str(params_path),
            )

            self.assertEqual(set(result), {"EURUSD"})
            self.assertEqual(set(result["EURUSD"]), {"M5"})
            params_payload = json.loads(params_path.read_text(encoding="utf-8"))
            self.assertEqual(set(params_payload), {"EURUSD"})
            self.assertEqual(set(params_payload["EURUSD"]), {"M5"})
            sidecar = json.loads(
                regime_params_provenance_path(str(params_path)).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(sidecar["generation_status"], "COMPLETE")
            self.assertEqual(sidecar["fitted_active_cell_count"], 1)
            self.assertEqual(sidecar["non_search_active_cell_count"], 2)
            self.assertEqual(sidecar["resolved_active_cell_count"], 3)
            self.assertEqual(sidecar["failures"], [])
            self.assertEqual(
                sidecar["non_search_active_cells"],
                ["EURUSD|D1", "GBPUSD|D1"],
            )
            sparse = sidecar["non_search_cells"]["EURUSD"]["D1"]
            second_sparse = sidecar["non_search_cells"]["GBPUSD"]["D1"]
            self.assertEqual(
                sparse["decision_kind"], "INSUFFICIENT_TRAIN_HISTORY"
            )
            self.assertEqual(
                second_sparse["decision_kind"], "INSUFFICIENT_TRAIN_HISTORY"
            )
            for decision in (sparse, second_sparse):
                self.assertTrue(decision["generation_terminal"])
                self.assertFalse(decision["retryable"])
                self.assertFalse(decision["economic_trade_verdict_emitted"])
                self.assertFalse(decision["no_trade"])
                self.assertFalse(decision["strategy_search_authorized"])

    def test_retryable_technical_cell_blocks_promotion_after_fit_sibling_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["M5", "D1"]

            class Universe:
                valid = True
                explicit_contract = True
                errors = []
                activation_id = "activation-retryable"
                activation_manifest_sha256 = "9" * 64
                requested_symbols = ("EURUSD",)
                required_timeframes = ("M5", "D1")
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

                @staticmethod
                def active_timeframes(symbol):
                    return ("M5", "D1")

            params_path = Path(config.regime_params_file)
            sidecar_path = regime_params_provenance_path(str(params_path))
            old_params = '{"LAST_GOOD": {}}'
            old_sidecar = '{"last": "good"}'
            params_path.write_text(old_params, encoding="utf-8")
            sidecar_path.write_text(old_sidecar, encoding="utf-8")
            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()

            def tune_cell(symbol, timeframe):
                if timeframe == "M5":
                    return _calendar_fit_result(tuner, symbol, timeframe)
                raise OSError("temporary native read failure")

            tuner.tune_symbol_timeframe = mock.Mock(side_effect=tune_cell)

            with self.assertRaises(RegimeTuningIncompleteError):
                tuner.tune_all(["EURUSD"], ["M5", "D1"], str(params_path))

            self.assertEqual(
                tuner.tune_symbol_timeframe.call_args_list,
                [mock.call("EURUSD", "M5"), mock.call("EURUSD", "D1")],
            )
            self.assertEqual(params_path.read_text(encoding="utf-8"), old_params)
            self.assertEqual(sidecar_path.read_text(encoding="utf-8"), old_sidecar)
            staged = json.loads(
                Path(str(sidecar_path) + ".staging").read_text(encoding="utf-8")
            )
            technical = staged["non_search_cells"]["EURUSD"]["D1"]
            self.assertEqual(staged["generation_status"], "INCOMPLETE")
            self.assertEqual(staged["fitted_active_cell_count"], 1)
            self.assertEqual(staged["non_search_active_cell_count"], 1)
            self.assertEqual(technical["decision_kind"], "TECHNICAL_EXCLUSION")
            self.assertFalse(technical["generation_terminal"])
            self.assertTrue(technical["retryable"])
            self.assertTrue(staged["failures"])

    def test_technical_non_search_cannot_be_relabelled_as_insufficient_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE

            class Universe:
                activation_id = "activation-relabel"
                activation_manifest_sha256 = "8" * 64
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            decision = tuner._build_non_search_decision(
                "EURUSD",
                "D1",
                decision_kind="TECHNICAL_EXCLUSION",
                reason_code="REGIME_FIT_RAISED",
                reason="temporary worker failure",
                error=OSError("temporary"),
            )
            decision["decision_kind"] = "INSUFFICIENT_TRAIN_HISTORY"
            decision["generation_terminal"] = True
            decision["retryable"] = False
            body = dict(decision)
            body.pop("decision_sha256", None)
            decision["decision_sha256"] = hashlib.sha256(
                json.dumps(
                    body,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()

            with self.assertRaisesRegex(ValueError, "reason code"):
                tuner._validate_non_search_decision(decision, "EURUSD", "D1")

    def test_calendar_zero_fit_generation_preserves_last_promoted_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config.production_split_mode = CALENDAR_SPLIT_MODE
            config.timeframes = ["D1"]

            class Universe:
                valid = True
                explicit_contract = True
                errors = []
                activation_id = "activation-zero-fit"
                activation_manifest_sha256 = "e" * 64
                requested_symbols = ("EURUSD",)
                required_timeframes = ("D1",)
                calendar_contract = {
                    "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                }

                @staticmethod
                def active_timeframes(symbol):
                    return ("D1",)

            params_path = Path(config.regime_params_file)
            sidecar_path = regime_params_provenance_path(str(params_path))
            params_path.write_text('{"LAST_GOOD": {}}', encoding="utf-8")
            sidecar_path.write_text('{"last": "good"}', encoding="utf-8")
            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()
            tuner.tune_symbol_timeframe = mock.Mock(
                side_effect=RuntimeError("systemic detector failure")
            )

            with self.assertRaises(RegimeTuningIncompleteError):
                tuner.tune_all(["EURUSD"], ["D1"], str(params_path))

            self.assertEqual(
                params_path.read_text(encoding="utf-8"), '{"LAST_GOOD": {}}'
            )
            self.assertEqual(
                sidecar_path.read_text(encoding="utf-8"), '{"last": "good"}'
            )
            staged = json.loads(
                Path(str(sidecar_path) + ".staging").read_text(encoding="utf-8")
            )
            self.assertEqual(staged["generation_status"], "INCOMPLETE")
            self.assertEqual(staged["fitted_active_cell_count"], 0)
            self.assertEqual(staged["non_search_active_cell_count"], 1)
            self.assertEqual(staged["resolved_active_cell_count"], 1)

    def test_incomplete_generation_never_overwrites_promoted_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            params_path = Path(config.regime_params_file)
            sidecar_path = regime_params_provenance_path(str(params_path))
            params_path.write_text('{"LAST_GOOD": {}}', encoding="utf-8")
            sidecar_path.write_text('{"last": "good"}', encoding="utf-8")
            tuner = RegimeParamTuner(root, config=config)
            tuner.tune_symbol_timeframe = mock.Mock(
                return_value=(RegimeParams(), {})
            )

            with self.assertRaises(RegimeTuningIncompleteError):
                tuner.tune_all(
                    ["EURUSD"],
                    ["M5"],
                    str(params_path),
                )

            self.assertEqual(
                params_path.read_text(encoding="utf-8"), '{"LAST_GOOD": {}}'
            )
            self.assertEqual(
                sidecar_path.read_text(encoding="utf-8"), '{"last": "good"}'
            )
            staged = json.loads(
                Path(str(sidecar_path) + ".staging").read_text(encoding="utf-8")
            )
            self.assertEqual(staged["generation_status"], "INCOMPLETE")
            self.assertEqual(staged["fitted_active_cell_count"], 0)


class RegimeProvenanceValidatorTests(unittest.TestCase):
    def _write_valid_artifacts(self, root: Path):
        config = _config(root)
        frame = _bars()
        source = root / "EURUSD_M5.csv"
        _write_source(source, frame)
        source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
        params_path = Path(config.regime_params_file)
        params = RegimeParams(k_confirm=2, gap_min=0.1, k_hold=3).to_dict()
        params_path.write_text(json.dumps({"EURUSD": {"M5": params}}), encoding="utf-8")

        tuner = RegimeParamTuner(root, config=config)
        _selected, fit_meta = tuner._optimizer_train_m5(frame)
        record = {
            "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
            "status": "FIT",
            "symbol": "EURUSD",
            "timeframe": "M5",
            "fit_scope": REGIME_PROVENANCE_FIT_SCOPE,
            "params_sha256": regime_params_sha256(params),
            "source": {
                "path": str(source.resolve()),
                "size": source.stat().st_size,
                "sha256": source_sha,
                "rows": len(frame),
            },
            "train": fit_meta["train"],
            "split": fit_meta["split"],
        }
        sidecar = regime_params_provenance_path(str(params_path))
        payload = {
            "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
            "cells": {"EURUSD": {"M5": record}},
        }
        sidecar.write_text(json.dumps(payload), encoding="utf-8")
        return config, frame, source, params_path, sidecar, payload, fit_meta

    def test_valid_exact_window_and_source_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, _frame, source, params_path, _sidecar, _payload, fit_meta = (
                self._write_valid_artifacts(root)
            )
            check = validate_regime_params_provenance(
                "eurusd",
                "m5",
                str(params_path),
                expected_source={
                    "path": str(source),
                    "size": source.stat().st_size,
                    "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                },
                expected_train_window=fit_meta["train"],
                expected_split_contract=DataSplitter.SPLIT_CONTRACT_VERSION,
            )
            self.assertTrue(check["valid"], check["reason"])
            self.assertTrue(
                has_tuned_regime_params(
                    "EURUSD",
                    "M5",
                    str(params_path),
                    require_provenance=True,
                )
            )

    def test_changed_params_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _config_obj, _frame, _source, params_path, _sidecar, _payload, _fit_meta = (
                self._write_valid_artifacts(root)
            )
            changed = json.loads(params_path.read_text(encoding="utf-8"))
            changed["EURUSD"]["M5"]["gap_min"] = 0.99
            params_path.write_text(json.dumps(changed), encoding="utf-8")
            check = validate_regime_params_provenance("EURUSD", "M5", str(params_path))
            self.assertFalse(check["valid"])
            self.assertIn("parameter hash mismatch", check["reason"])

    def test_post_train_geometry_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _config_obj, _frame, _source, params_path, sidecar, payload, _fit_meta = (
                self._write_valid_artifacts(root)
            )
            record = payload["cells"]["EURUSD"]["M5"]
            record["train"]["end_index"] = 600
            record["train"]["rows"] = 600
            record["split"]["optimizer_train_indices"] = [0, 600]
            sidecar.write_text(json.dumps(payload), encoding="utf-8")

            check = validate_regime_params_provenance("EURUSD", "M5", str(params_path))

            self.assertFalse(check["valid"])
            self.assertIn("leaks beyond optimizer_train", check["reason"])

    def test_missing_sidecar_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            params_path = root / "regime_params.json"
            params_path.write_text(
                json.dumps({"EURUSD": {"M5": RegimeParams().to_dict()}}),
                encoding="utf-8",
            )
            check = validate_regime_params_provenance("EURUSD", "M5", str(params_path))
            self.assertFalse(check["valid"])
            self.assertIn("sidecar is missing", check["reason"])

    def test_complete_v3_generation_binds_activation_and_whole_params_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _config_obj, _frame, _source, params_path, sidecar, payload, _fit_meta = (
                self._write_valid_artifacts(root)
            )
            record = payload["cells"]["EURUSD"]["M5"]
            record["source"]["source_kind"] = "broker_native"
            record["split"].update({
                "production_split_mode": CALENDAR_SPLIT_MODE,
                "source_authority": "broker_native_timeframe",
                "contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            })
            payload.update({
                "generation_status": "COMPLETE",
                "params_file_sha256": hashlib.sha256(params_path.read_bytes()).hexdigest(),
                "activation_id": "activation-v3",
                "activation_manifest_sha256": "c" * 64,
                "split_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                "required_active_cells": ["EURUSD|M5"],
                "required_active_cell_count": 1,
                "fitted_active_cell_count": 1,
            })
            sidecar.write_text(json.dumps(payload), encoding="utf-8")

            check = validate_regime_params_provenance(
                "EURUSD",
                "M5",
                str(params_path),
                expected_split_contract=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                expected_activation_id="activation-v3",
                expected_activation_manifest_sha256="c" * 64,
                require_complete_generation=True,
            )
            self.assertTrue(check["valid"], check["reason"])

            changed = json.loads(params_path.read_text(encoding="utf-8"))
            changed["UNRELATED"] = {}
            params_path.write_text(json.dumps(changed), encoding="utf-8")
            check = validate_regime_params_provenance(
                "EURUSD",
                "M5",
                str(params_path),
                require_complete_generation=True,
            )
            self.assertFalse(check["valid"])
            self.assertIn("artifact hash mismatch", check["reason"])


if __name__ == "__main__":
    unittest.main()
