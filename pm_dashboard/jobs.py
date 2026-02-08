"""
PM Dashboard Background Jobs
=============================

Daily data download scheduler and background tasks for historical data management.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Import MT5 connector with fallback
try:
    from pm_mt5 import MT5Connector, MT5_AVAILABLE
except ImportError:
    MT5_AVAILABLE = False
    MT5Connector = None


class HistoricalDataDownloader:
    """
    Manages historical data downloads for PM dashboard analytics.

    Downloads OHLC data for all symbols and timeframes used in the PM system.
    Stores data in pm_outputs/historical_data/ directory.
    """

    # Timeframes to download
    TIMEFRAMES = ['M5', 'M15', 'M30', 'H1', 'H4', 'D1']

    # Default symbols (will be overridden by config)
    DEFAULT_SYMBOLS = [
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD',
        'AUDUSD', 'NZDUSD', 'AUDNZD', 'EURGBP', 'EURJPY',
        'GBPJPY', 'AUDJPY', 'XAUUSD', 'XAGUSD', 'US30', 'US100'
    ]

    def __init__(self, pm_root: str, mt5_connector: Optional[MT5Connector] = None):
        """
        Initialize historical data downloader.

        Args:
            pm_root: Root directory of PM project
            mt5_connector: Optional MT5 connector instance
        """
        self.pm_root = pm_root
        self.data_dir = os.path.join(pm_root, "pm_outputs", "historical_data")
        self.mt5 = mt5_connector
        self._ensure_data_dir()

    def _ensure_data_dir(self):
        """Create historical data directory if it doesn't exist."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)
            logger.info(f"Created historical data directory: {self.data_dir}")

    def get_symbols_from_config(self) -> List[str]:
        """
        Get list of symbols from PM configuration.

        Returns:
            List of symbol strings
        """
        try:
            import json
            config_path = os.path.join(self.pm_root, "config.json")
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    instrument_specs = config.get('instrument_specs', {})
                    if instrument_specs:
                        return list(instrument_specs.keys())
        except Exception as e:
            logger.warning(f"Failed to load symbols from config: {e}")

        return self.DEFAULT_SYMBOLS

    def get_data_filepath(self, symbol: str, timeframe: str, date: datetime) -> str:
        """
        Get filepath for historical data file.

        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            date: Date for data

        Returns:
            Absolute file path
        """
        date_str = date.strftime("%Y%m%d")
        filename = f"{symbol}_{timeframe}_{date_str}.csv"
        return os.path.join(self.data_dir, filename)

    def is_data_cached(self, symbol: str, timeframe: str, date: datetime) -> bool:
        """
        Check if data is already cached locally.

        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            date: Date to check

        Returns:
            True if data exists and is valid
        """
        filepath = self.get_data_filepath(symbol, timeframe, date)
        if not os.path.exists(filepath):
            return False

        # Check if file is not empty
        try:
            df = pd.read_csv(filepath)
            return len(df) > 0
        except Exception:
            return False

    def download_day_data(self, symbol: str, timeframe: str, date: datetime) -> Optional[pd.DataFrame]:
        """
        Download one day of data for symbol/timeframe.

        Args:
            symbol: Symbol to download
            timeframe: Timeframe to download
            date: Date to download (will get full day)

        Returns:
            DataFrame with OHLCV data or None
        """
        if not MT5_AVAILABLE or not self.mt5:
            logger.error("MT5 not available for data download")
            return None

        try:
            # Calculate date range for the day
            start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)

            # Download data from MT5
            df = self.mt5.get_bars_range(symbol, timeframe, start_date, end_date)

            if df is None or len(df) == 0:
                logger.warning(f"No data downloaded for {symbol} {timeframe} on {date.strftime('%Y-%m-%d')}")
                return None

            # Save to cache
            filepath = self.get_data_filepath(symbol, timeframe, date)
            df.to_csv(filepath)
            logger.debug(f"Saved {len(df)} bars to {filepath}")

            return df

        except Exception as e:
            logger.error(f"Failed to download data for {symbol} {timeframe}: {e}")
            return None

    def download_historical_range(self,
                                  symbol: str,
                                  timeframe: str,
                                  start_date: datetime,
                                  end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Download historical data for a date range.

        Args:
            symbol: Symbol to download
            timeframe: Timeframe to download
            start_date: Start date
            end_date: End date

        Returns:
            Combined DataFrame or None
        """
        if not MT5_AVAILABLE or not self.mt5:
            return None

        try:
            df = self.mt5.get_bars_range(symbol, timeframe, start_date, end_date)
            return df
        except Exception as e:
            logger.error(f"Failed to download historical range for {symbol}: {e}")
            return None

    def download_all_symbols(self, date: datetime = None):
        """
        Download data for all symbols and timeframes.

        Args:
            date: Date to download (default: yesterday)
        """
        if date is None:
            date = datetime.now() - timedelta(days=1)

        symbols = self.get_symbols_from_config()
        logger.info(f"Starting download for {len(symbols)} symbols on {date.strftime('%Y-%m-%d')}")

        success_count = 0
        fail_count = 0
        skip_count = 0

        for symbol in symbols:
            for timeframe in self.TIMEFRAMES:
                # Check cache first
                if self.is_data_cached(symbol, timeframe, date):
                    logger.debug(f"Skipping {symbol} {timeframe} - already cached")
                    skip_count += 1
                    continue

                # Download
                df = self.download_day_data(symbol, timeframe, date)

                if df is not None:
                    success_count += 1
                else:
                    fail_count += 1

                # Small delay to avoid overwhelming MT5
                time.sleep(0.1)

        logger.info(f"Download complete: {success_count} succeeded, {fail_count} failed, {skip_count} cached")

    def load_historical_data(self,
                            symbol: str,
                            timeframe: str,
                            start_date: datetime,
                            end_date: datetime = None) -> Optional[pd.DataFrame]:
        """
        Load historical data from cache or download if needed.

        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            start_date: Start date
            end_date: End date (default: now)

        Returns:
            DataFrame with OHLCV data or None
        """
        if end_date is None:
            end_date = datetime.now()

        # Try to load from cached files
        all_dfs = []
        current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while current_date <= end_date:
            filepath = self.get_data_filepath(symbol, timeframe, current_date)

            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                    all_dfs.append(df)
                except Exception as e:
                    logger.warning(f"Failed to load cached file {filepath}: {e}")

            current_date += timedelta(days=1)

        if all_dfs:
            combined_df = pd.concat(all_dfs, axis=0)
            # Filter to exact date range
            combined_df = combined_df[(combined_df.index >= start_date) & (combined_df.index <= end_date)]
            return combined_df

        # No cached data, try to download from MT5
        logger.info(f"No cached data found, downloading {symbol} {timeframe} from MT5")
        return self.download_historical_range(symbol, timeframe, start_date, end_date)


class DataDownloadScheduler:
    """
    Background scheduler for daily data downloads.

    Runs data downloads at specified time each day (default: 00:05).
    """

    def __init__(self, downloader: HistoricalDataDownloader, run_time: str = "00:05"):
        """
        Initialize scheduler.

        Args:
            downloader: HistoricalDataDownloader instance
            run_time: Time to run downloads (HH:MM format)
        """
        self.downloader = downloader
        self.run_time = run_time
        self._running = False
        self._thread = None

    def start(self):
        """Start the background scheduler thread."""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"Data download scheduler started (runs daily at {self.run_time})")

    def stop(self):
        """Stop the background scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Data download scheduler stopped")

    def _run_loop(self):
        """Main scheduler loop."""
        while self._running:
            try:
                # Check if it's time to run
                now = datetime.now()
                target_hour, target_minute = map(int, self.run_time.split(':'))

                if now.hour == target_hour and now.minute == target_minute:
                    logger.info("Starting scheduled data download")
                    self.downloader.download_all_symbols()
                    # Sleep for 60 seconds to avoid running multiple times in same minute
                    time.sleep(60)
                else:
                    # Check every 30 seconds
                    time.sleep(30)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                time.sleep(60)

    def run_now(self):
        """Trigger an immediate download (for testing/manual runs)."""
        logger.info("Manual data download triggered")
        self.downloader.download_all_symbols()


def initialize_data_jobs(pm_root: str,
                        mt5_connector: Optional[MT5Connector] = None,
                        enable_scheduler: bool = False) -> tuple:
    """
    Initialize data download jobs.

    Args:
        pm_root: PM root directory
        mt5_connector: Optional MT5 connector
        enable_scheduler: Whether to start the scheduler

    Returns:
        Tuple of (downloader, scheduler)
    """
    downloader = HistoricalDataDownloader(pm_root, mt5_connector)
    scheduler = DataDownloadScheduler(downloader)

    if enable_scheduler:
        scheduler.start()

    return downloader, scheduler


__all__ = [
    'HistoricalDataDownloader',
    'DataDownloadScheduler',
    'initialize_data_jobs',
]
