"""
PM Dashboard Data Jobs
======================

Maintains PM market data in the existing root `data/` folder and provides
historical data loading for analytics simulation.

Design:
- Single source of truth: `data/*_M5.csv`
- Refresh path mirrors PM main flow (`MT5Connector.get_bars(..., "M5", count=max_bars)`)
- Higher timeframes for analytics are derived by resampling M5 data locally
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# MT5 connector is optional for simulation (local data works without it).
try:
    from pm_mt5 import MT5Connector, MT5_AVAILABLE
except ImportError:
    MT5_AVAILABLE = False
    MT5Connector = None

try:
    from pm_core import DataLoader
except ImportError:
    DataLoader = None


class HistoricalDataDownloader:
    """
    Maintains root `data/` M5 datasets and serves resampled historical data.
    """

    MAINTENANCE_TIMEFRAME = "M5"
    DEFAULT_MAX_BARS = 500000
    DEFAULT_SYMBOLS = [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD",
        "AUDUSD", "NZDUSD", "AUDNZD", "EURGBP", "EURJPY",
        "GBPJPY", "AUDJPY", "XAUUSD", "XAGUSD", "US30", "US100",
    ]
    SUPPORTED_TIMEFRAMES = {"M5", "M15", "M30", "H1", "H4", "D1"}

    def __init__(self, pm_root: str, mt5_connector: Optional[MT5Connector] = None):
        self.pm_root = pm_root
        self.data_dir = os.path.join(pm_root, "data")
        self.mt5 = mt5_connector
        self._data_loader: Optional[DataLoader] = None
        self._config_cache_signature: Optional[Tuple[str, int, int]] = None
        self._config_cache: Dict[str, Any] = {}
        self._ensure_data_dir()

    def _ensure_data_dir(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)

    def _get_data_loader(self) -> Optional[DataLoader]:
        if DataLoader is None:
            return None
        if self._data_loader is None:
            self._data_loader = DataLoader(Path(self.data_dir))
        return self._data_loader

    def _available_data_symbols(self) -> List[str]:
        symbols: List[str] = []
        try:
            for name in os.listdir(self.data_dir):
                if not name.upper().endswith("_M5.CSV"):
                    continue
                sym = name[:-7]  # strip "_M5.csv"
                sym = str(sym).strip().upper()
                if sym:
                    symbols.append(sym)
        except Exception:
            return []
        return sorted(set(symbols))

    def _resolve_symbol_candidates(self, symbol: str) -> List[str]:
        requested = str(symbol or "").strip().upper()
        if not requested:
            return []

        available = self._available_data_symbols()
        available_set = set(available)
        seed_candidates: List[str] = []
        resolved_candidates: List[str] = []

        def _add(value: str) -> None:
            normalized = str(value or "").strip().upper()
            if normalized and normalized not in seed_candidates:
                seed_candidates.append(normalized)

        def _add_resolved(value: str) -> None:
            normalized = str(value or "").strip().upper()
            if normalized and normalized not in resolved_candidates:
                resolved_candidates.append(normalized)

        _add(requested)

        if "." in requested:
            left = requested.split(".", 1)[0].strip()
            _add(left)

        if len(requested) >= 6:
            prefix6 = requested[:6]
            _add(prefix6)

        for candidate in seed_candidates:
            if candidate in available_set:
                _add_resolved(candidate)

        startswith_matches = [sym for sym in available if requested.startswith(sym)]
        prefix_matches = [sym for sym in available if sym.startswith(requested)]
        for match in startswith_matches + prefix_matches:
            _add_resolved(match)

        for candidate in seed_candidates:
            _add_resolved(candidate)

        return resolved_candidates

    def _resolve_symbol_name(self, symbol: str) -> str:
        candidates = self._resolve_symbol_candidates(symbol)
        return candidates[0] if candidates else str(symbol or "").strip().upper()

    def _normalize_symbols_list(self, symbols: Optional[List[str]]) -> List[str]:
        if symbols is None:
            return self.get_symbols_from_config()
        if not isinstance(symbols, list):
            raise ValueError("symbols must be a list")
        normalized = []
        seen = set()
        for item in symbols:
            text = str(item or "").strip().upper()
            if not text or text in seen:
                continue
            seen.add(text)
            normalized.append(text)
        return normalized

    def _normalize_max_bars(self, value: Optional[Any]) -> int:
        if value is None:
            return self.get_max_bars_from_config()
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            raise ValueError("max_bars must be an integer")
        return max(1000, numeric)

    def _normalize_timeframe(self, timeframe: str) -> str:
        tf = str(timeframe or self.MAINTENANCE_TIMEFRAME).strip().upper()
        if tf not in self.SUPPORTED_TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe '{timeframe}'")
        return tf

    def _clear_data_loader_cache(self) -> None:
        if self._data_loader is not None:
            try:
                self._data_loader.clear_cache()
            except Exception:
                pass

    def _load_root_config(self) -> Dict[str, object]:
        config_path = os.path.join(self.pm_root, "config.json")
        if not os.path.isfile(config_path):
            self._config_cache_signature = None
            self._config_cache = {}
            return {}

        try:
            stat = os.stat(config_path)
            signature = (config_path, int(stat.st_mtime_ns), int(stat.st_size))
        except OSError:
            return self._config_cache

        if self._config_cache_signature != signature:
            try:
                with open(config_path, "r", encoding="utf-8") as handle:
                    config = json.load(handle)
                if isinstance(config, dict):
                    self._config_cache = config
                else:
                    self._config_cache = {}
            except Exception as exc:
                logger.warning("Failed to load config.json: %s", exc)
                self._config_cache = {}
            self._config_cache_signature = signature

        return self._config_cache

    def get_symbols_from_config(self) -> List[str]:
        """
        Read symbols from root config.json.
        Preference: `symbols` list -> `instrument_specs` keys -> defaults.
        """
        try:
            config = self._load_root_config()
            symbols = config.get("symbols")
            if isinstance(symbols, list) and symbols:
                return [str(s).strip().upper() for s in symbols if str(s).strip()]

            specs = config.get("instrument_specs")
            if isinstance(specs, dict) and specs:
                return [str(s).strip().upper() for s in specs.keys() if str(s).strip()]
        except Exception as exc:
            logger.warning("Failed to load symbols from config.json: %s", exc)

        return list(self.DEFAULT_SYMBOLS)

    def get_max_bars_from_config(self) -> int:
        """Read maintenance bar depth from root pipeline config."""
        try:
            config = self._load_root_config()
            pipeline = config.get("pipeline", {})
            if isinstance(pipeline, dict):
                value = int(pipeline.get("max_bars", self.DEFAULT_MAX_BARS))
                return max(1000, value)
        except Exception:
            pass
        return self.DEFAULT_MAX_BARS

    def _ensure_mt5_connection(self) -> bool:
        if not MT5_AVAILABLE or self.mt5 is None:
            return False
        try:
            if self.mt5.is_connected():
                return True
        except Exception:
            pass
        try:
            return bool(self.mt5.connect())
        except Exception as exc:
            logger.warning("MT5 reconnect failed for data maintenance: %s", exc)
            return False

    def can_refresh_from_mt5(self) -> bool:
        return self._ensure_mt5_connection()

    def refresh_symbol_m5(self, symbol: str, max_bars: int) -> bool:
        """
        Refresh one symbol's root M5 file using the same MT5 path as PM main app.
        """
        if not self._ensure_mt5_connection():
            logger.warning("Cannot refresh %s: MT5 not connected", symbol)
            return False

        broker_symbol = self.mt5.find_broker_symbol(symbol)
        if not broker_symbol:
            logger.warning("Cannot refresh %s: broker symbol not found", symbol)
            return False

        bars = self.mt5.get_bars(broker_symbol, self.MAINTENANCE_TIMEFRAME, count=max_bars)
        if bars is None or not isinstance(bars, pd.DataFrame) or len(bars) == 0:
            logger.warning("No M5 data returned for %s (%s)", symbol, broker_symbol)
            return False

        filepath = os.path.join(self.data_dir, f"{symbol}_M5.csv")
        try:
            bars.to_csv(filepath)
            logger.info("Updated %s with %d bars", filepath, len(bars))
            return True
        except Exception as exc:
            logger.error("Failed writing %s: %s", filepath, exc)
            return False

    def refresh_all_m5_data(self,
                            symbols: Optional[List[str]] = None,
                            max_bars: Optional[int] = None) -> dict:
        """
        Refresh root M5 data files for configured symbols.
        """
        symbols_to_refresh = self._normalize_symbols_list(symbols)
        bars = self._normalize_max_bars(max_bars)

        logger.info(
            "Starting root data maintenance: symbols=%d, timeframe=%s, bars=%d",
            len(symbols_to_refresh),
            self.MAINTENANCE_TIMEFRAME,
            bars,
        )

        success = 0
        failed = 0
        for symbol in symbols_to_refresh:
            if self.refresh_symbol_m5(symbol, bars):
                success += 1
            else:
                failed += 1
            time.sleep(0.05)

        self._clear_data_loader_cache()

        logger.info(
            "Root data maintenance complete: %d updated, %d failed",
            success,
            failed,
        )
        return {
            "success": failed == 0,
            "updated": success,
            "failed": failed,
            "symbols_total": len(symbols_to_refresh),
            "bars": bars,
            "timeframe": self.MAINTENANCE_TIMEFRAME,
        }

    # Backward-compatible alias used by existing callers.
    def download_all_symbols(self, date: datetime = None):  # noqa: ARG002
        return self.refresh_all_m5_data()

    def _normalize_range(self, start_date: datetime, end_date: datetime) -> Tuple[datetime, datetime]:
        start = start_date
        end = end_date
        if getattr(start, "tzinfo", None) is not None:
            start = start.replace(tzinfo=None)
        if getattr(end, "tzinfo", None) is not None:
            end = end.replace(tzinfo=None)
        return start, end

    def load_historical_data(self,
                             symbol: str,
                             timeframe: str,
                             start_date: datetime,
                             end_date: datetime = None) -> Optional[pd.DataFrame]:
        """
        Load simulation data from root `data/` folder, resampling from M5 as needed.
        """
        if end_date is None:
            end_date = datetime.now()
        start_date, end_date = self._normalize_range(start_date, end_date)

        loader = self._get_data_loader()
        if loader is None:
            logger.error("pm_core.DataLoader unavailable; cannot load historical data")
            return None

        symbol_candidates = self._resolve_symbol_candidates(symbol)
        tf = self._normalize_timeframe(timeframe)
        if not symbol_candidates:
            return None

        base = None
        resolved_symbol = ""
        for candidate in symbol_candidates:
            candidate_frame = loader.load_symbol(candidate, "M5")
            if candidate_frame is not None and len(candidate_frame) > 0:
                base = candidate_frame
                resolved_symbol = candidate
                break
        if base is None or len(base) == 0:
            logger.warning("No local M5 data found for %s (candidates=%s)", symbol, symbol_candidates)
            return None

        if tf == "M5":
            frame = base
        else:
            try:
                frame = loader.resample(base, tf)
            except Exception as exc:
                logger.warning("Failed resample %s %s from M5: %s", resolved_symbol or symbol, tf, exc)
                return None

        sliced = frame[(frame.index >= start_date) & (frame.index <= end_date)]
        if len(sliced) == 0:
            return None
        return sliced


class DataDownloadScheduler:
    """
    Scheduler for root-data maintenance (default: daily at midnight).
    """

    def __init__(self, downloader: HistoricalDataDownloader, run_time: str = "00:00"):
        self.downloader = downloader
        self.run_time = run_time
        self._running = False
        self._thread = None
        self._last_run_date = None

    def _parse_run_time(self) -> Tuple[int, int]:
        try:
            hh_str, mm_str = str(self.run_time).split(":")
            hh = max(0, min(23, int(hh_str)))
            mm = max(0, min(59, int(mm_str)))
            return hh, mm
        except Exception:
            return 0, 0

    def start(self):
        if self._running:
            logger.warning("Data maintenance scheduler already running")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Data maintenance scheduler started (daily at %s)", self.run_time)

    def stop(self, timeout_sec: float = 10.0):
        self._running = False
        if self._thread:
            self._thread.join(timeout=max(0.1, float(timeout_sec)))
            if self._thread.is_alive():
                logger.warning("Data maintenance scheduler thread did not stop within %.1fs", timeout_sec)
        logger.info("Data maintenance scheduler stopped")

    def _should_run_now(self, now: datetime, target_hour: int, target_minute: int) -> bool:
        if self._last_run_date == now.date():
            return False
        if now.hour > target_hour:
            return True
        if now.hour == target_hour and now.minute >= target_minute:
            return True
        return False

    def _run_loop(self):
        target_hour, target_minute = self._parse_run_time()
        while self._running:
            try:
                now = datetime.now()
                if self._should_run_now(now, target_hour, target_minute):
                    self.run_now()
                    self._last_run_date = now.date()
                time.sleep(30)
            except Exception as exc:
                logger.error("Error in data maintenance scheduler: %s", exc, exc_info=True)
                time.sleep(60)

    def run_now(self):
        logger.info("Manual/root data maintenance triggered")
        return self.downloader.refresh_all_m5_data()


def initialize_data_jobs(pm_root: str,
                         mt5_connector: Optional[MT5Connector] = None,
                         enable_scheduler: bool = False,
                         run_time: str = "00:00") -> tuple:
    downloader = HistoricalDataDownloader(pm_root, mt5_connector)
    scheduler = DataDownloadScheduler(downloader, run_time=run_time)

    if enable_scheduler:
        scheduler.start()

    return downloader, scheduler


__all__ = [
    "HistoricalDataDownloader",
    "DataDownloadScheduler",
    "initialize_data_jobs",
]
