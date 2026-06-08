from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pm_main import FXPortfolioManagerApp


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


class _FakeMT5:
    def __init__(self, full_bars, range_bars=None):
        self.full_bars = full_bars
        self.range_bars = full_bars if range_bars is None else range_bars
        self.full_fetches = []
        self.range_fetches = []

    def is_connected(self):
        return True

    def find_broker_symbol(self, symbol):
        return symbol

    def get_bars(self, broker_symbol, timeframe, count):
        self.full_fetches.append((broker_symbol, timeframe, count))
        return self.full_bars.copy()

    def get_bars_range(self, broker_symbol, timeframe, start_date, end_date):
        self.range_fetches.append((broker_symbol, timeframe, start_date, end_date))
        return self.range_bars.copy()


def _bars(n):
    idx = pd.date_range("2026-01-01", periods=n, freq="5min")
    return pd.DataFrame(
        {
            "Open": range(n),
            "High": range(1, n + 1),
            "Low": range(n),
            "Close": range(n),
            "Volume": [100] * n,
        },
        index=idx,
    )


def test_fetch_historical_data_backfills_when_existing_file_below_target(tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    existing = _bars(3)
    existing.to_csv(data_dir / "EURUSD_M5.csv", index_label="time")

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.mt5 = _FakeMT5(_bars(5))
    app.symbols = ["EURUSD"]
    app.data_dir = data_dir
    app.pipeline_config = SimpleNamespace(
        max_bars=5,
        storage_delta_sync_overlap_minutes=1440,
        optimization_min_m5_bars=5,
        historical_fetch_retry_attempts=1,
    )
    app.portfolio_manager = SimpleNamespace(pipeline=None)
    app.logger = _Logger()

    stats = app._fetch_historical_data(["EURUSD"])

    saved = pd.read_csv(data_dir / "EURUSD_M5.csv", index_col=0, parse_dates=True)
    assert len(saved) == 5
    assert app.mt5.full_fetches == []
    assert len(app.mt5.range_fetches) == 1
    assert app.mt5.range_fetches[0][0:2] == ("EURUSD", "M5")
    assert stats["ready_symbols"] == ["EURUSD"]
    assert stats["incomplete_symbols"] == {}


def test_fetch_historical_data_marks_short_pull_incomplete_after_retries(tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.mt5 = _FakeMT5(_bars(3))
    app.symbols = ["EURUSD"]
    app.data_dir = data_dir
    app.pipeline_config = SimpleNamespace(
        max_bars=5,
        storage_delta_sync_overlap_minutes=1440,
        optimization_min_m5_bars=5,
        historical_fetch_retry_attempts=3,
        historical_fetch_retry_sleep_seconds=0.0,
    )
    app.portfolio_manager = SimpleNamespace(pipeline=None)
    app.logger = _Logger()

    stats = app._fetch_historical_data(["EURUSD"])

    saved = pd.read_csv(data_dir / "EURUSD_M5.csv", index_col=0, parse_dates=True)
    assert len(saved) == 3
    assert len(app.mt5.full_fetches) == 3
    assert stats["ready_symbols"] == []
    assert stats["incomplete_symbols"] == {"EURUSD": 3}


def test_fetch_historical_data_deep_backfills_short_first_pass(tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.mt5 = _FakeMT5(_bars(3), range_bars=_bars(5))
    app.symbols = ["EURUSD"]
    app.data_dir = data_dir
    app.pipeline_config = SimpleNamespace(
        max_bars=5,
        storage_delta_sync_overlap_minutes=1440,
        optimization_min_m5_bars=5,
        historical_fetch_retry_attempts=3,
        historical_fetch_retry_sleep_seconds=0.0,
        historical_fetch_deep_backfill_enabled=True,
        historical_fetch_deep_backfill_years=18,
    )
    app.portfolio_manager = SimpleNamespace(pipeline=None)
    app.logger = _Logger()

    stats = app._fetch_historical_data(["EURUSD"])

    saved = pd.read_csv(data_dir / "EURUSD_M5.csv", index_col=0, parse_dates=True)
    assert len(saved) == 5
    assert len(app.mt5.full_fetches) == 3
    assert len(app.mt5.range_fetches) == 1
    assert stats["ready_symbols"] == ["EURUSD"]
    assert stats["incomplete_symbols"] == {}


def test_fetch_historical_data_marks_write_failure_without_aborting(tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.mt5 = _FakeMT5(_bars(5))
    app.symbols = ["EURUSD"]
    app.data_dir = data_dir
    app.pipeline_config = SimpleNamespace(
        max_bars=5,
        storage_delta_sync_overlap_minutes=1440,
        optimization_min_m5_bars=5,
        historical_fetch_retry_attempts=1,
        historical_fetch_retry_sleep_seconds=0.0,
    )
    app.portfolio_manager = SimpleNamespace(pipeline=None)
    app.logger = _Logger()
    app._write_history_csv_atomic = lambda *args, **kwargs: False

    stats = app._fetch_historical_data(["EURUSD"])

    assert stats["ready_symbols"] == []
    assert stats["write_failed_symbols"] == ["EURUSD"]
    assert not (data_dir / "EURUSD_M5.csv").exists()
