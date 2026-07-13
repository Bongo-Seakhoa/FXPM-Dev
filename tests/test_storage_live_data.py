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

    def test_calendar_live_m5_uses_fresh_native_window_only(self) -> None:
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
        mt5.get_bars.assert_called_once_with("EURUSD", "M5", count=50)
        self.assertFalse(trader._get_live_cache_path("EURUSD", "M5").exists())
        self.assertEqual(pd.Timestamp(bars.index[-1]), incoming.index[-1])

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

    def test_calendar_live_h1_uses_native_bars_instead_of_m5_resample(self) -> None:
        existing = self._make_recent_bars(periods=1000, freq="5min")
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        incoming = self._make_recent_bars(periods=40, freq="1h", price_start=1.2500)

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
        loader.resample = Mock(side_effect=AssertionError("production live bars must stay broker-native"))

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 12)
        loader.load_symbol.assert_not_called()
        loader.resample.assert_not_called()
        mt5.get_bars.assert_called_once_with("EURUSD", "H1", count=12)

        self.assertFalse(trader._get_live_cache_path("EURUSD", "H1").exists())

    def test_calendar_live_requests_each_exact_native_timeframe(self) -> None:
        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=12,
            storage_live_sync_overlap_bars=4,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        frequencies = {
            "M5": "5min",
            "M15": "15min",
            "M30": "30min",
            "H1": "1h",
            "H4": "4h",
            "D1": "1D",
        }

        def _get_bars(symbol: str, timeframe: str, count: int) -> pd.DataFrame:
            del symbol
            return self._make_recent_bars(
                periods=max(12, count),
                freq=frequencies[timeframe],
                price_start=1.2 + (0.01 * len(mt5.get_bars.call_args_list)),
            )

        mt5.get_bars.side_effect = _get_bars
        loader = trader.pm.pipeline.data_loader
        loader.resample = Mock(side_effect=AssertionError("native calendar mode cannot resample M5"))

        for timeframe in frequencies:
            with self.subTest(timeframe=timeframe):
                bars = trader._get_live_bars(
                    "EURUSD",
                    "EURUSD",
                    timeframe,
                    count=12,
                    min_required=10,
                )
                self.assertIsNotNone(bars)
                self.assertEqual(len(bars), 12)

        self.assertEqual(
            [call.args[1] for call in mt5.get_bars.call_args_list],
            list(frequencies),
        )
        loader.resample.assert_not_called()

    def test_calendar_live_fetch_is_bounded_and_stateless(self) -> None:
        first = self._make_recent_bars(periods=20, freq="1h", price_start=1.2000)
        second = self._make_recent_bars(
            periods=12,
            freq="1h",
            price_start=1.3000,
            end=first.index[-1] + pd.Timedelta(hours=1),
        )
        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=12,
            storage_live_sync_overlap_bars=4,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.side_effect = [first, second]

        initial = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)
        refreshed = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(initial)
        self.assertIsNotNone(refreshed)
        self.assertEqual(len(initial), 12)
        self.assertEqual(len(refreshed), 12)
        self.assertEqual(refreshed.index[-1], second.index[-1])
        self.assertEqual(
            [(call.args, call.kwargs) for call in mt5.get_bars.call_args_list],
            [
                (("EURUSD", "H1"), {"count": 12}),
                (("EURUSD", "H1"), {"count": 12}),
            ],
        )
        self.assertGreater(float(refreshed["Close"].min()), 1.29)
        self.assertFalse(trader._get_live_cache_path("EURUSD", "H1").exists())

    def test_calendar_live_window_never_truncates_below_required_minimum(self) -> None:
        incoming = self._make_recent_bars(periods=20, freq="1h", price_start=1.2000)
        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = incoming

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=5, min_required=10)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 10)
        mt5.get_bars.assert_called_once_with("EURUSD", "H1", count=10)

    def test_calendar_live_never_ingests_a_legacy_resampled_cache(self) -> None:
        legacy = self._make_recent_bars(periods=20, freq="1h", price_start=9.0000)
        incoming = self._make_recent_bars(periods=12, freq="1h", price_start=1.2000)
        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            storage_live_sync_bars=12,
            storage_live_sync_overlap_bars=4,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        legacy_path = trader._get_live_cache_path("EURUSD", "H1")
        legacy.to_csv(legacy_path, index_label="time")
        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = incoming

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(bars)
        self.assertTrue(legacy_path.exists())
        self.assertLess(float(bars["Close"].max()), 2.0)

    def test_calendar_live_fails_closed_instead_of_falling_back_to_stale_cache(self) -> None:
        legacy = self._make_recent_bars(periods=20, freq="1h", price_start=9.0000)
        partial_native = self._make_recent_bars(periods=9, freq="1h", price_start=1.2000)
        pipeline_config = PipelineConfig(
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            storage_local_data_first_enabled=True,
            live_bars_count=12,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        trader = self._make_trader(pipeline_config, mt5)
        legacy.to_csv(trader._get_live_cache_path("EURUSD", "H1"), index_label="time")

        disconnected = trader._get_live_bars(
            "EURUSD", "EURUSD", "H1", count=12, min_required=10
        )
        self.assertIsNone(disconnected)
        mt5.get_bars.assert_not_called()

        mt5.is_connected.return_value = True
        mt5.get_bars.return_value = partial_native
        incomplete = trader._get_live_bars(
            "EURUSD", "EURUSD", "H1", count=12, min_required=10
        )
        self.assertIsNone(incomplete)
        mt5.get_bars.assert_called_once_with("EURUSD", "H1", count=12)

    def test_research_mode_retains_explicit_m5_resample_compatibility(self) -> None:
        existing = self._make_recent_bars(periods=1000, freq="5min")
        existing.to_csv(self.data_dir / "EURUSD_M5.csv")
        incoming = self._make_recent_bars(periods=80, freq="5min", price_start=1.2500)
        pipeline_config = PipelineConfig(
            research_mode=True,
            production_split_mode="bleeding_edge",
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
        mt5.get_bars.return_value = incoming
        loader = trader.pm.pipeline.data_loader
        original_resample = loader.resample
        loader.resample = Mock(wraps=original_resample)

        bars = trader._get_live_bars("EURUSD", "EURUSD", "H1", count=12, min_required=10)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 12)
        loader.resample.assert_called()
        self.assertTrue(all(call.args[1] == "M5" for call in mt5.get_bars.call_args_list))

    def test_calendar_live_native_window_upsizes_with_requested_count(self) -> None:
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
        self.assertEqual(
            [call.kwargs["count"] for call in mt5.get_bars.call_args_list],
            [12, 24],
        )
        self.assertFalse(trader._get_live_cache_path("EURUSD", "H1").exists())

    def test_calendar_live_m5_does_not_use_legacy_sync_floor(self) -> None:
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
        mt5.get_bars.assert_called_once_with("EURUSD", "M5", count=50)

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
            research_mode=True,
            production_split_mode="bleeding_edge",
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
