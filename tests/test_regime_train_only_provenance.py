import hashlib
import json
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
from pm_regime_tuner import RegimeParamTuner, RegimeTuningIncompleteError


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
                    return "active" if (symbol, timeframe) == ("EURUSD", "D1") else "excluded_no_trade"

            tuner = RegimeParamTuner(root, config=config)
            tuner._active_native_universe = Universe()

            def load_native(symbol, timeframe):
                self.assertEqual(timeframe, "D1", "M5 must not be used as a cutoff spine")
                return frame, {
                    "source_kind": "broker_native",
                    "provenance_valid": True,
                    "provenance_errors": [],
                    "path": str(root / "native_d1.csv"),
                    "size": 123,
                    "sha256": "b" * 64,
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
