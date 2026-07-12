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

    def get_bars(self, broker_symbol, timeframe, count, force_full=False):
        self.full_fetches.append((broker_symbol, timeframe, count))
        return self.full_bars.copy()

    def get_bars_range(self, broker_symbol, timeframe, start_date, end_date, sync_retry=False):
        self.range_fetches.append((broker_symbol, timeframe, start_date, end_date))
        return self.range_bars.copy()


class _OptimizationPortfolio:
    def __init__(self, symbols):
        self.symbols = list(symbols)
        self.optimization_rosters = []
        self.saved_no_trade = []

    def initial_optimization(self, overwrite=False):
        self.optimization_rosters.append(list(self.symbols))
        return {}

    def save_data_no_trade_config(self, symbol, reason):
        self.saved_no_trade.append((symbol, reason))

    def print_status(self):
        return None

    def get_live_eligible_configs(self):
        return {"EURUSD": object()}


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


def test_run_optimization_calendar_mode_never_invokes_legacy_m5_roster_filter(
    tmp_path: Path,
    monkeypatch,
):
    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = SimpleNamespace(is_connected=lambda: True)
    app.symbols = ["EURUSD", "SOLUSD"]
    app.portfolio_manager = _OptimizationPortfolio(["EURUSD", "SOLUSD"])
    app.config_file = str(tmp_path / "pm_configs.json")
    (tmp_path / "pm_configs.json").write_text("{}", encoding="utf-8")
    app.storage_manager = None
    app._save_results = lambda _results: None
    app._fetch_historical_data = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("legacy M5 fetch must not run in calendar production")
    )
    monkeypatch.setattr(
        "pm_main._phase_a_optimization_complete",
        lambda _ledger, _symbols: (True, []),
    )

    assert app.run_optimization()
    assert app.portfolio_manager.optimization_rosters == [["EURUSD", "SOLUSD"]]
    assert app.portfolio_manager.symbols == ["EURUSD", "SOLUSD"]
    assert app.portfolio_manager.saved_no_trade == []


def test_run_optimization_legacy_mode_retains_m5_readiness_filter():
    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(
        production_split_mode="chronological_lockbox_v1",
        historical_fetch_retry_attempts=1,
        optimization_min_m5_bars=5,
        optimization_min_m5_bars_fraction=0.0,
        max_bars=5,
    )
    app.mt5 = SimpleNamespace(is_connected=lambda: True)
    app.portfolio_manager = _OptimizationPortfolio(["EURUSD", "SOLUSD"])
    app.storage_manager = None
    app._save_results = lambda _results: None
    app._fetch_historical_data = lambda *_args, **_kwargs: {
        "ready_symbols": ["EURUSD"],
        "incomplete_symbols": {"SOLUSD": 2},
        "no_data_symbols": [],
    }

    assert app.run_optimization()
    assert app.portfolio_manager.optimization_rosters == [["EURUSD"]]
    assert app.portfolio_manager.symbols == ["EURUSD", "SOLUSD"]
    assert [row[0] for row in app.portfolio_manager.saved_no_trade] == ["SOLUSD"]


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


def test_effective_min_m5_bars_uses_fraction_of_max_bars():
    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.pipeline_config = SimpleNamespace(
        max_bars=300000, optimization_min_m5_bars_fraction=0.85, optimization_min_m5_bars=300000
    )
    assert app._effective_min_m5_bars() == 255000

    # No fraction -> absolute floor.
    app.pipeline_config = SimpleNamespace(
        max_bars=300000, optimization_min_m5_bars_fraction=0.0, optimization_min_m5_bars=300000
    )
    assert app._effective_min_m5_bars() == 300000


def test_fetch_marks_close_but_short_symbol_ready_under_fraction(tmp_path: Path):
    # floor = ceil(5 * 0.6) = 3; a 4-bar pull (80%) is valid, not excluded.
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.mt5 = _FakeMT5(_bars(4))
    app.symbols = ["EURUSD"]
    app.data_dir = data_dir
    app.pipeline_config = SimpleNamespace(
        max_bars=5,
        optimization_min_m5_bars_fraction=0.6,
        optimization_min_m5_bars=5,
        storage_delta_sync_overlap_minutes=1440,
        historical_fetch_retry_attempts=1,
        historical_fetch_retry_sleep_seconds=0.0,
    )
    app.portfolio_manager = SimpleNamespace(pipeline=None)
    app.logger = _Logger()

    stats = app._fetch_historical_data(["EURUSD"])

    assert stats["ready_symbols"] == ["EURUSD"]
    assert stats["incomplete_symbols"] == {}


def test_backfill_start_clamped_for_corrupt_old_index(tmp_path: Path):
    # A stray pre-1970 row at index[0] must not push backfill_start out of range
    # (real MT5 raises OSError Errno 22 on pre-1970 dates).
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    idx = pd.DatetimeIndex(
        ["1970-01-01 00:00:00", "2026-01-01 00:00:00", "2026-01-01 00:05:00"]
    )
    existing = pd.DataFrame(
        {"Open": [1, 2, 3], "High": [1, 2, 3], "Low": [1, 2, 3], "Close": [1, 2, 3], "Volume": [1, 1, 1]},
        index=idx,
    )
    existing.to_csv(data_dir / "ALCOA_M5.csv", index_label="time")

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.mt5 = _FakeMT5(_bars(5))
    app.symbols = ["ALCOA"]
    app.data_dir = data_dir
    app.pipeline_config = SimpleNamespace(
        max_bars=10,
        storage_delta_sync_overlap_minutes=1440,
        optimization_min_m5_bars=3,
        historical_fetch_retry_attempts=1,
        historical_fetch_retry_sleep_seconds=0.0,
        historical_fetch_deep_backfill_years=18,
    )
    app.portfolio_manager = SimpleNamespace(pipeline=None)
    app.logger = _Logger()

    stats = app._fetch_historical_data(["ALCOA"])  # must not raise

    assert app.mt5.range_fetches, "range backfill should be attempted"
    start_date = app.mt5.range_fetches[0][2]
    assert pd.Timestamp(start_date).year >= 1971
