import hashlib
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from pm_core import PipelineConfig
from pm_pipeline import (
    OptimizationPipeline,
    PortfolioManager,
    RegimeConfig,
    SymbolConfig,
    build_artifact_meta,
)


def _config(root: Path) -> PipelineConfig:
    return PipelineConfig(
        research_mode=True,
        data_dir=root,
        output_dir=root / "out",
        timeframes=["M5"],
        production_split_mode="chronological_lockbox_v1",
        regime_params_file=str(root / "regime_params.json"),
    )


def _bars(rows: int = 300) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq="5min")
    values = 100.0 + np.linspace(0.0, 1.0, rows)
    return pd.DataFrame(
        {
            "Open": values,
            "High": values + 0.1,
            "Low": values - 0.1,
            "Close": values,
            "Volume": 1,
        },
        index=index,
    )


def test_lockbox_pipeline_rejects_before_feature_computation():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        config = _config(root)
        frame = _bars()
        source = root / "EURUSD_M5.csv"
        frame.reset_index(names="time").to_csv(source, index=False)
        source_meta = {
            "path": str(source.resolve()),
            "size": source.stat().st_size,
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        }
        train_meta = {
            "start_index": 0,
            "end_index": 100,
            "rows": 100,
            "start_ts": frame.index[0].isoformat(),
            "end_ts_inclusive": frame.index[99].isoformat(),
            "end_ts": frame.index[100].isoformat(),
            "end_ts_exclusive": frame.index[100].isoformat(),
        }
        window_meta = {
            "source": source_meta,
            "windows": {
                "historical_stress_audit": {},
                "active_universe": {
                    "start_ts": frame.index[0].isoformat(),
                    "end_ts": None,
                },
                "base_selection": {},
                "tuning_validation": {},
                "optimizer_train": train_meta,
                "optimizer_train_with_warmup": train_meta,
                "stage2_selection": {},
                "stage2_selection_with_warmup": {},
                "stage2_training_selection": {},
                "stage2_training_selection_with_warmup": {},
                "final_lockbox": {},
                "final_lockbox_with_warmup": {},
            },
        }
        pipeline = OptimizationPipeline(config)
        with mock.patch.object(
            pipeline,
            "_load_symbol_timeframe_data",
            return_value=({"M5": frame}, window_meta),
        ), mock.patch(
            "pm_pipeline.FeatureComputer.compute_all",
            side_effect=AssertionError("feature computation must be unreachable"),
        ):
            result = pipeline.run_for_symbol("EURUSD")

        assert not result.success
        assert "provenance gate failed" in result.error_message.lower()
        assert "regime params file is missing" in result.error_message.lower()


def test_live_eligibility_rejects_missing_detector_provenance():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        config = _config(root)
        manager = object.__new__(PortfolioManager)
        manager.config = config
        winner = RegimeConfig(
            strategy_name="TestStrategy",
            parameters={},
            quality_score=0.5,
            promotion_status="PROMOTED",
        )
        symbol_config = SymbolConfig(
            symbol="EURUSD",
            regime_configs={"M5": {"CHOP": winner}},
            is_validated=True,
            valid_until=datetime.now() + timedelta(days=1),
        )

        ok, reason = manager._live_eligibility_reason(
            "EURUSD",
            symbol_config,
            now=datetime.now(),
            current_artifact_meta={},
            artifact_policy="block",
            expiry_grace_minutes=0,
        )

        assert not ok
        assert "regime detector provenance invalid" in reason.lower()


def test_artifact_contract_fingerprints_provenance_sidecar():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        meta = build_artifact_meta(_config(root), strategies=[])
        assert "regime_provenance" in meta
        assert meta["regime_provenance"]["sha256"] is None
