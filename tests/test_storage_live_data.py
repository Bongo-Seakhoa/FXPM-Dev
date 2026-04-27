import unittest
import shutil
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock, Mock

import pandas as pd

from pm_core import DataLoader, PipelineConfig
from pm_main import FXPortfolioManagerApp, LiveTrader
from pm_position import PositionConfig


class StorageLiveDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_root = Path(__file__).resolve().parents[1] / ".tmp_storage_live_data"
        self.data_dir = self.tmp_root / "data"
        self.output_dir = self.tmp_root / "pm_outputs"
        self.log_dir = self.tmp_root / "logs"
        for path in (self.data_dir, self.output_dir, self.log_dir):
            path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.tmp_root.exists():
            shutil.rmtree(self.tmp_root, ignore_errors=True)

    def _make_bars(
        self,
        *,
        start: str,
        periods: int,
        freq: str,
        price_start: float = 1.1000,
    ) -> pd.DataFrame:
        index = pd.date_range(start=start, periods=periods, freq=freq)
        values = [price_start + (i * 0.0001) for i in range(periods)]
        return pd.DataFrame(
            {
                "Open": values,
                "High": [v + 0.0003 for v in values],
                "Low": [v - 0.0003 for v in values],
                "Close": [v + 0.0001 for v in values],
                "Volume": [100 + i for i in range(periods)],
                "Spread": [2 for _ in range(periods)],
            },
            index=index,
        )

    def _make_recent_bars(
        self,
        *,
        periods: int,
        freq: str,
        price_start: float = 1.1000,
        end: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        anchor = pd.Timestamp(end) if end is not None else pd.Timestamp.now().floor(freq)
        index = pd.date_range(end=anchor, periods=periods, freq=freq)
        values = [price_start + (i * 0.0001) for i in range(periods)]
        return pd.DataFrame(
            {
                "Open": values,
                "High": [v + 0.0003 for v in values],
                "Low": [v - 0.0003 for v in values],
                "Close": [v + 0.0001 for v in values],
                "Volume": [100 + i for i in range(periods)],
                "Spread": [2 for _ in range(periods)],
            },
            index=index,
        )

    def _make_trader(self, pipeline_config: PipelineConfig, mt5: Mock) -> LiveTrader:
        pm = SimpleNamespace(
            symbols=["EURUSD"],
            pipeline=SimpleNamespace(data_loader=DataLoader(pipeline_config.data_dir)),
        )
        trader = LiveTrader(
            mt5_connector=mt5,
            portfolio_manager=pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=pipeline_config,
        )
        trader.logger = MagicMock()
        return trader

    def test_live_bars_prefer_local_canonical_m5(self) -> None:
        existing = self._make_recent_bars(periods=120, freq="5min", price_start=1.1000)
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        incoming = self._make_recent_bars(periods=60, freq="5min", price_start=1.1300)

        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=60,
            live_bars_count=50,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = incoming

        bars = trader._get_live_bars("EURUSD", "EURUSD", "M5", count=50, min_required=50)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 50)
        mt5.get_bars.assert_called_once_with("EURUSD", "M5", count=100)

        cache_path = trader._get_live_cache_path("EURUSD", "M5")
        self.assertTrue(cache_path.exists())
        persisted = pd.read_csv(cache_path, index_col=0, parse_dates=True)
        self.assertLessEqual(len(persisted), 50)
        self.assertEqual(pd.to_datetime(persisted.index[-1]), incoming.index[-1])

    def test_live_bars_fall_back_to_mt5_when_local_tf_is_missing(self) -> None:
        fallback_h1 = self._make_recent_bars(periods=12, freq="1h")

        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=30,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True

        def _get_bars(symbol: str, timeframe: str, count: int):
            if timeframe == "H1":
                return fallback_h1
            raise AssertionError(f"Unexpected timeframe request: {timeframe}")

        mt5.get_bars.side_effect = _get_bars

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 12)
        self.assertEqual(
            [(call.args, call.kwargs) for call in mt5.get_bars.call_args_list],
            [
                (("EURUSD", "H1"), {"count": 12}),
            ],
        )

    def test_live_h1_uses_bounded_recent_load_instead_of_full_canonical_read(self) -> None:
        existing = self._make_recent_bars(periods=1000, freq="5min")
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        incoming = self._make_recent_bars(periods=40, freq="5min", price_start=1.2500)

        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=40,
            storage_live_sync_overlap_bars=20,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = incoming

        loader = trader.pm.pipeline.data_loader
        loader.load_symbol = Mock(side_effect=AssertionError("full canonical load should not be used in live mode"))

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 12)
        loader.load_symbol.assert_not_called()

        cache_path = trader._get_live_cache_path("EURUSD", "H1")
        self.assertTrue(cache_path.exists())
        live_cache = pd.read_csv(cache_path, index_col=0, parse_dates=True)
        self.assertLessEqual(len(live_cache), 12)

    def test_live_timeframe_cache_upsizes_when_required_bars_increase(self) -> None:
        existing = self._make_recent_bars(periods=1000, freq="5min")
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        incoming = self._make_recent_bars(periods=80, freq="5min", price_start=1.2500)

        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=80,
            storage_live_sync_overlap_bars=20,
            live_bars_count=24,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = incoming

        small = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)
        large = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=24, min_required=10)

        self.assertIsNotNone(small)
        self.assertIsNotNone(large)
        self.assertEqual(len(small), 12)
        self.assertEqual(len(large), 24)

        cache_path = trader._get_live_cache_path("EURUSD", "H1")
        live_cache = pd.read_csv(cache_path, index_col=0, parse_dates=True)
        self.assertGreaterEqual(len(live_cache), 24)

    def test_live_m5_gap_reseed_uses_storage_live_sync_bars_floor(self) -> None:
        stale_end = pd.Timestamp.now().floor("5min") - timedelta(days=3)
        existing = self._make_recent_bars(periods=120, freq="5min", end=stale_end)
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        incoming = self._make_recent_bars(periods=120, freq="5min", price_start=1.2200)

        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=120,
            storage_live_sync_overlap_bars=20,
            live_bars_count=50,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = incoming

        bars = trader._get_live_bars("EURUSD", "EURUSD", "M5", count=50, min_required=50)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 50)
        mt5.get_bars.assert_called_once_with("EURUSD", "M5", count=120)

    def test_live_h1_gap_reseed_uses_direct_timeframe_bars(self) -> None:
        stale_end = pd.Timestamp.now().floor("5min") - timedelta(days=5)
        existing = self._make_recent_bars(periods=1000, freq="5min", end=stale_end)
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        fallback_h1 = self._make_recent_bars(periods=12, freq="1h", price_start=1.2600)

        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=80,
            storage_live_sync_overlap_bars=20,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = fallback_h1

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 12)
        mt5.get_bars.assert_called_once_with("EURUSD", "H1", count=12)

    def test_fetch_historical_data_merges_existing_file_in_place(self) -> None:
        existing = self._make_bars(start="2026-01-01", periods=10, freq="5min")
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        delta = self._make_bars(start="2026-01-01 00:35:00", periods=6, freq="5min", price_start=1.2000)

        cfg = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            max_bars=100,
            storage_delta_sync_overlap_minutes=30,
        )
        app = FXPortfolioManagerApp(symbols=["EURUSD"], pipeline_config=cfg)
        app.portfolio_manager = SimpleNamespace(
            pipeline=SimpleNamespace(data_loader=DataLoader(self.data_dir)),
        )
        app.mt5 = Mock()
        app.mt5.is_connected.return_value = True
        app.mt5.find_broker_symbol.return_value = "EURUSD"
        app.mt5.get_bars_range.return_value = delta

        app._fetch_historical_data(["EURUSD"])

        merged = pd.read_csv(self.data_dir / "EURUSD_M5.csv", index_col=0, parse_dates=True)
        merged.index = pd.to_datetime(merged.index)
        self.assertEqual(len(merged), 13)
        self.assertEqual(merged.index[-1], delta.index[-1])


if __name__ == "__main__":
    unittest.main()
