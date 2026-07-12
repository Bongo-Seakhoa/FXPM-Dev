"""
FX Portfolio Manager - Regime Parameter Tuner
==============================================

Standalone script to tune regime detection parameters for each symbol/timeframe.

Outputs: regime_params.json + regime_params.provenance.json
    {
        "EURUSD": {
            "H1": {"k_confirm": 2, "gap_min": 0.12, ...},
            "H4": {...}
        },
        ...
    }

Usage:
    python pm_regime_tuner.py --config config.json
    python pm_regime_tuner.py --data-dir ./data --output regime_params.json
    python pm_regime_tuner.py --symbols EURUSD GBPUSD --timeframes H1 H4

This script is separate from the main optimization flow and should be run
periodically (e.g., weekly) to calibrate regime detection parameters.

Version: 4.0 (chronological train-only detector fitting)
"""

import argparse
import hashlib
import json
import logging
import os
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from itertools import product

import pandas as pd

from pm_core import DataLoader, DataSplitter, PipelineConfig
from pm_evidence import (
    CALENDAR_SPLIT_MODE,
    EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    build_calendar_boundaries,
    build_timeframe_calendar_boundaries,
    calendar_window_meta,
    derive_purge_duration,
    trim_to_closed_bars,
)
from pm_native_activation import (
    NATIVE_PRODUCTION_TIMEFRAMES,
    resolve_active_native_universe,
)
from pm_regime import (
    RegimeParams, 
    MarketRegimeDetector, 
    RegimeType,
    DEFAULT_PARAMS_BY_TIMEFRAME,
    REGIME_PROVENANCE_FIT_SCOPE,
    REGIME_PROVENANCE_SCHEMA_VERSION,
    regime_params_provenance_path,
    regime_params_sha256,
)

logger = logging.getLogger(__name__)


class RegimeTuningIncompleteError(RuntimeError):
    """Raised when any required active specialist cell fails detector fitting."""


# =============================================================================
# TUNING CONFIGURATION
# =============================================================================

# Timeframe-specific parameter search spaces
PARAM_SEARCH_SPACE = {
    'M5': {
        'k_confirm': [2, 3, 4],
        'gap_min': [0.08, 0.10, 0.12, 0.15],
        'k_hold': [4, 5, 6, 8]
    },
    'M15': {
        'k_confirm': [2, 3, 4],
        'gap_min': [0.08, 0.10, 0.12],
        'k_hold': [3, 4, 5, 6]
    },
    'M30': {
        'k_confirm': [2, 3],
        'gap_min': [0.08, 0.10, 0.12],
        'k_hold': [3, 4, 5]
    },
    'H1': {
        'k_confirm': [2, 3],
        'gap_min': [0.08, 0.10, 0.12],
        'k_hold': [2, 3, 4]
    },
    'H4': {
        'k_confirm': [1, 2],
        'gap_min': [0.06, 0.08, 0.10],
        'k_hold': [2, 3]
    },
    'D1': {
        'k_confirm': [1, 2],
        'gap_min': [0.05, 0.08, 0.10],
        'k_hold': [1, 2]
    },
}


# =============================================================================
# DATA LOADING
# =============================================================================

class TunerDataLoader:
    """Simple data loader for tuning."""
    
    RESAMPLE_MAP = {
        'M1': '1min', 'M5': '5min', 'M15': '15min', 'M30': '30min',
        'H1': '1h', 'H4': '4h', 'D1': '1D', 'W1': '1W',
    }
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self._cache: Dict[str, pd.DataFrame] = {}
        self._source_meta: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _resolve_data_file(self, symbol: str) -> Optional[Path]:
        """Mirror the production loader's unambiguous M5 source resolution."""
        for candidate in (
            self.data_dir / f"{symbol}_M5.csv",
            self.data_dir / f"{symbol.upper()}_M5.csv",
            self.data_dir / f"{symbol.lower()}_M5.csv",
        ):
            if candidate.exists():
                return candidate
        matches: Dict[str, Path] = {}
        for pattern in (
            f"{symbol}_M5*.csv",
            f"{symbol.upper()}_M5*.csv",
            f"{symbol.lower()}_M5*.csv",
        ):
            for match in sorted(self.data_dir.glob(pattern)):
                matches[str(match.resolve())] = match
        if len(matches) == 1:
            return next(iter(matches.values()))
        if len(matches) > 1:
            logger.error(
                "%s M5: ambiguous source files: %s",
                symbol,
                ", ".join(path.name for path in matches.values()),
            )
        return None
    
    def load_symbol(self, symbol: str) -> Optional[pd.DataFrame]:
        """Load M5 data for a symbol."""
        if symbol in self._cache:
            return self._cache[symbol].copy()
        
        source = self._resolve_data_file(symbol)
        if source is not None:
            try:
                source_path = source.resolve()
                df = pd.read_csv(source_path)
                df = self._standardize(df)
                source_stat = source_path.stat()
                self._cache[symbol] = df
                self._source_meta[symbol] = {
                    "path": str(source_path),
                    "size": int(source_stat.st_size),
                    "sha256": self._sha256(source_path),
                    "rows": int(len(df)),
                }
                logger.info(f"Loaded {symbol}: {len(df)} bars from {source_path.name}")
                return df.copy()
            except Exception as e:
                logger.error(f"Failed to load {symbol}: {e}")
                return None
        
        logger.warning(f"No data file found for {symbol}")
        return None

    def get_source_meta(self, symbol: str) -> Dict[str, Any]:
        return dict(self._source_meta.get(symbol, {}) or {})
    
    def _standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and set datetime index."""
        df.columns = df.columns.str.lower()
        
        column_map = {
            'open': 'Open', 'high': 'High', 'low': 'Low', 
            'close': 'Close', 'volume': 'Volume', 'tick_volume': 'Volume'
        }
        df = df.rename(columns=column_map)
        
        # Find and parse datetime
        for col in ['time', 'datetime', 'date', 'timestamp']:
            if col in df.columns.str.lower():
                time_col = [c for c in df.columns if c.lower() == col][0]
                df[time_col] = pd.to_datetime(df[time_col])
                df = df.set_index(time_col).sort_index()
                break
        
        return df
    
    def resample(self, df: pd.DataFrame, timeframe: str,
                 *,
                 start_ts: Optional[Any] = None,
                 end_ts_exclusive: Optional[Any] = None) -> pd.DataFrame:
        """Resample to target timeframe and discard boundary-straddling bins."""
        if timeframe == 'M5':
            return df.copy()
        
        rule = self.RESAMPLE_MAP.get(timeframe, '1h')
        
        resampler = df.resample(rule)
        resampled = pd.DataFrame(
            {
                'Open': resampler['Open'].first(),
                'High': resampler['High'].max(),
                'Low': resampler['Low'].min(),
                'Close': resampler['Close'].last(),
                'Volume': resampler['Volume'].sum(),
            }
        ).dropna()

        # Slicing M5 first prevents information leakage.  Pandas can still
        # manufacture a partial first/last higher-timeframe candle at the cut
        # boundaries, while the production pipeline drops a full-source candle
        # that straddles either boundary.  Remove those structural partial bins
        # so detector tuning and pipeline feature construction see the same
        # eligible candle set.
        labels = pd.DatetimeIndex(resampled.index)
        complete_mask = pd.Series(True, index=resampled.index)
        if start_ts not in (None, ""):
            complete_mask &= labels >= pd.Timestamp(start_ts)
        if end_ts_exclusive not in (None, ""):
            offset = pd.tseries.frequencies.to_offset(rule)
            complete_mask &= (labels + offset) <= pd.Timestamp(end_ts_exclusive)
        resampled = resampled.loc[complete_mask]
        
        return resampled
    
    def clear_cache(self):
        """Clear the data cache."""
        self._cache.clear()
        self._source_meta.clear()


# =============================================================================
# TUNING METRICS
# =============================================================================

def compute_regime_quality_metrics(regime_series: pd.Series, 
                                    gap_series: pd.Series,
                                    price_series: pd.Series,
                                    warmup_bars: int = 0) -> Dict[str, Any]:
    """
    Compute quality metrics for a regime detection configuration.
    
    Metrics:
    - stability: Fewer regime changes = more stable
    - avg_gap: Higher average gap = more confident detection
    - regime_balance: Reasonable distribution across regimes
    - trend_return_ratio: Returns magnitude in TREND vs RANGE
    
    Returns:
        Dict with quality metrics
    """
    if warmup_bars > 0:
        regime_series = regime_series.iloc[warmup_bars:]
        gap_series = gap_series.iloc[warmup_bars:]
        price_series = price_series.iloc[warmup_bars:]

    n = len(regime_series)
    
    if n < 100:
        return {
            'stability': 0.0, 'avg_gap': 0.0, 'balance_score': 1.0,
            'trend_return_ratio': 1.0, 'regime_counts': {}, 'total_changes': 0
        }
    
    # Filter out None/NaN values
    valid_mask = regime_series.notna()
    regime_valid = regime_series[valid_mask]
    gap_valid = gap_series[valid_mask]
    
    # Stability: count regime changes
    changes = (regime_valid != regime_valid.shift(1)).sum()
    stability = 1.0 - (changes / len(regime_valid)) if len(regime_valid) > 0 else 0.0
    
    # Gap confidence: average gap
    avg_gap = gap_valid.mean() if len(gap_valid) > 0 else 0.0
    
    # Regime distribution
    regime_counts = regime_valid.value_counts(normalize=True)
    
    # Natural concentration is acceptable if it reflects genuine market structure.
    balance_score = 1.0
    
    # Trend return ratio: do TREND periods show larger moves?
    returns = price_series.pct_change().abs()
    
    trend_mask = regime_series == RegimeType.TREND
    range_mask = regime_series == RegimeType.RANGE
    
    trend_returns_mean = returns[trend_mask].mean() if trend_mask.sum() > 20 else 0.0
    range_returns_mean = returns[range_mask].mean() if range_mask.sum() > 20 else trend_returns_mean
    
    if range_returns_mean > 1e-10:
        trend_return_ratio = trend_returns_mean / range_returns_mean
    else:
        trend_return_ratio = 1.0
    
    return {
        'stability': float(stability),
        'avg_gap': float(avg_gap),
        'balance_score': float(balance_score),
        'trend_return_ratio': float(min(2.0, trend_return_ratio)),
        'regime_counts': dict(regime_counts),
        'total_changes': int(changes)
    }


def compute_tuning_score(metrics: Dict[str, Any]) -> float:
    """
    Compute composite score for tuning.
    
    Higher = better regime detection configuration.
    """
    # Weights
    w_stability = 0.35
    w_gap = 0.25
    w_trend_ratio = 0.40
    
    stability = metrics.get('stability', 0.0)
    avg_gap = metrics.get('avg_gap', 0.0)
    trend_ratio = metrics.get('trend_return_ratio', 1.0)
    
    # Normalize gap to 0-1 (gaps typically range 0-0.2)
    gap_normalized = min(1.0, avg_gap * 5)
    
    # Trend ratio bonus (want > 1.0)
    trend_bonus = min(1.0, (trend_ratio - 0.8) / 0.7) if trend_ratio > 0.8 else 0.0
    
    score = (
        w_stability * stability +
        w_gap * gap_normalized +
        w_trend_ratio * trend_bonus
    )
    
    return float(score)


# =============================================================================
# TUNER CLASS
# =============================================================================

class RegimeParamTuner:
    """
    Tunes regime detection parameters for each symbol/timeframe.
    """
    
    def __init__(self, data_dir: Path, config: Optional[PipelineConfig] = None):
        self.data_dir = Path(data_dir)
        self.data_loader = TunerDataLoader(data_dir)
        self.native_data_loader = DataLoader(Path(data_dir), cache_resampled=False)
        self.config = config or PipelineConfig(data_dir=Path(data_dir))
        split_mode = str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower()
        if split_mode not in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
            raise ValueError(
                "RegimeParamTuner requires a production lockbox split so "
                "detector fitting cannot see tuning-validation or final-lockbox rows"
            )
        self.split_mode = split_mode
        self.splitter = DataSplitter(self.config)
        self._active_native_universe = None

    def _native_universe(self):
        """Resolve and cache the single authenticated production activation."""
        if self._active_native_universe is None:
            universe = resolve_active_native_universe(
                self.data_dir,
                required_timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
                production_contract_required=True,
            )
            if not universe.valid:
                reasons = ",".join(universe.errors[:12]) or "unknown"
                raise ValueError(f"active native universe is invalid: {reasons}")
            if not universe.explicit_contract:
                raise ValueError("active native universe lacks an explicit cell contract")
            self._active_native_universe = universe
        return self._active_native_universe

    @staticmethod
    def _window_meta(df: pd.DataFrame, start: int, end: int) -> Dict[str, Any]:
        frame = df.iloc[start:end]
        end_exclusive = (
            pd.Timestamp(df.index[end]).isoformat()
            if 0 <= end < len(df)
            else None
        )
        return {
            "start_index": int(start),
            "end_index": int(end),
            "rows": int(max(0, end - start)),
            "start_ts": pd.Timestamp(frame.index[0]).isoformat() if len(frame) else None,
            "end_ts_inclusive": pd.Timestamp(frame.index[-1]).isoformat() if len(frame) else None,
            "end_ts_exclusive": end_exclusive,
        }

    def _optimizer_train_m5(
        self,
        df_m5: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Return the only M5 rows detector parameter selection may inspect."""
        ordered = df_m5.sort_index()
        max_bars = max(1, int(getattr(self.config, "max_bars", 300000) or 300000))
        workflow = ordered.tail(max_bars).copy() if len(ordered) > max_bars else ordered.copy()
        indices = self.splitter.get_workflow_indices(len(workflow))
        train_start, train_end = indices["optimizer_train"]
        validation_start, validation_end = indices["tuning_validation"]
        lockbox_start, lockbox_end = indices["final_lockbox"]
        train_meta = self._window_meta(workflow, train_start, train_end)
        if train_meta["rows"] <= 0 or train_meta["end_ts_exclusive"] is None:
            raise ValueError("optimizer_train is empty or has no exclusive end boundary")
        source_offset = int(len(ordered) - len(workflow))
        fit_meta = {
            "train": train_meta,
            "split": {
                "contract_version": DataSplitter.SPLIT_CONTRACT_VERSION,
                "production_split_mode": "chronological_lockbox_v1",
                "workflow_rows": int(len(workflow)),
                "workflow_source_start_index": source_offset,
                "optimizer_train_indices": [int(train_start), int(train_end)],
                "optimizer_train_source_indices": [
                    source_offset + int(train_start),
                    source_offset + int(train_end),
                ],
                "tuning_validation_indices": [int(validation_start), int(validation_end)],
                "final_lockbox_indices": [int(lockbox_start), int(lockbox_end)],
                "split_embargo_bars": int(
                    getattr(self.config, "split_embargo_bars", 0) or 0
                ),
                "tuning_validation_bars": int(
                    getattr(self.config, "tuning_validation_bars", 0) or 0
                ),
                "final_lockbox_bars": int(
                    getattr(self.config, "final_lockbox_bars", 0) or 0
                ),
            },
        }
        return workflow.iloc[train_start:train_end].copy(), fit_meta

    def _optimizer_train_native_timeframe(
        self,
        symbol: str,
        timeframe: str,
    ) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
        """Return one active native cell's authenticated optimizer-train slice.

        The activation manifest, not M5, supplies the common information
        cutoff. Under the v3 contract each specialist has its own training
        start while tuning and final windows remain concurrent. This permits a
        valid H1/H4/D1 cell to be tuned even when that symbol's M5 cell is an
        authenticated availability exclusion.
        """
        timeframe = str(timeframe or "").strip().upper()
        symbol = str(symbol or "").strip().upper()
        universe = self._native_universe()
        if universe.cell_status(symbol, timeframe) != "active":
            raise ValueError(
                f"native cell is not active in activation: {symbol}|{timeframe}"
            )
        contract = dict(universe.calendar_contract or {})
        as_of_raw = str(contract.get("common_as_of_utc") or "").strip()
        if not as_of_raw:
            raise ValueError("authenticated native activation has no information cutoff")
        as_of = pd.Timestamp(as_of_raw)
        as_of = as_of.tz_localize("UTC") if as_of.tzinfo is None else as_of.tz_convert("UTC")

        configured_cutoff = str(
            getattr(self.config, "evidence_as_of_utc", "") or ""
        ).strip()
        if configured_cutoff:
            configured_as_of = pd.Timestamp(configured_cutoff)
            configured_as_of = (
                configured_as_of.tz_localize("UTC")
                if configured_as_of.tzinfo is None
                else configured_as_of.tz_convert("UTC")
            )
            if configured_as_of != as_of:
                raise ValueError(
                    "configured evidence cutoff does not match authenticated activation"
                )

        native, source_meta = self.native_data_loader.load_native_timeframe(
            symbol,
            timeframe,
        )
        if native is None or native.empty:
            raise ValueError(f"native {timeframe} source is missing")
        if str((source_meta or {}).get("source_kind", "")).lower() != "broker_native":
            raise ValueError(f"native {timeframe} source authority is invalid")
        if not bool((source_meta or {}).get("provenance_valid", False)):
            errors = ",".join((source_meta or {}).get("provenance_errors", []) or [])
            raise ValueError(
                f"native {timeframe} provenance is invalid: {errors or 'unknown'}"
            )

        workflow = trim_to_closed_bars(native, timeframe, as_of_utc=as_of)
        purge = derive_purge_duration(
            configured_days=getattr(self.config, "evidence_purge_days", 7.0),
            split_embargo_bars=getattr(self.config, "split_embargo_bars", 0),
        )
        contract_version = str(contract.get("window_contract_version") or "").strip()
        timeframe_boundaries = contract.get("timeframe_calendar_boundaries")
        if contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            if not isinstance(timeframe_boundaries, dict):
                raise ValueError("authenticated v3 timeframe boundaries are missing")
            raw_boundaries = timeframe_boundaries.get(timeframe)
            if not isinstance(raw_boundaries, dict):
                raise ValueError(
                    f"authenticated v3 boundaries are missing for {timeframe}"
                )
            # Independently derive the expected v3 geometry from the pinned
            # policy and reject a locally edited/semantically inconsistent map.
            expected = build_timeframe_calendar_boundaries(
                as_of,
                timeframe,
                timeframe_window_years=contract.get("timeframe_window_years"),
                embargo=purge,
            )
        else:
            raw_boundaries = contract.get("common_calendar_boundaries")
            expected = build_calendar_boundaries(
                as_of,
                window_years=getattr(self.config, "evidence_window_years", None),
                embargo=purge,
            )
            contract_version = EVIDENCE_CONTRACT_VERSION
        if not isinstance(raw_boundaries, dict):
            raise ValueError("authenticated calendar boundaries are missing")

        parsed_windows: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
            raw = raw_boundaries.get(partition)
            if not isinstance(raw, dict):
                raise ValueError(f"authenticated calendar boundary is missing: {partition}")
            start = pd.Timestamp(raw.get("start_ts"))
            end = pd.Timestamp(raw.get("end_ts"))
            start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
            end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
            if end <= start:
                raise ValueError(f"authenticated calendar boundary is invalid: {partition}")
            expected_start, expected_end = expected.windows[partition]
            if start != expected_start or end != expected_end:
                raise ValueError(
                    f"authenticated {timeframe} {partition} boundary disagrees with policy"
                )
            parsed_windows[partition] = (start, end)

        train_start, train_end = parsed_windows["optimizer_train"]
        validation_start, validation_end = parsed_windows["tuning_validation"]
        lockbox_start, lockbox_end = parsed_windows["final_lockbox"]
        train_meta = calendar_window_meta(workflow, train_start, train_end)
        validation_meta = calendar_window_meta(
            workflow, validation_start, validation_end
        )
        lockbox_meta = calendar_window_meta(workflow, lockbox_start, lockbox_end)
        if train_meta["rows"] <= 0 or train_meta["end_ts_exclusive"] is None:
            raise ValueError(f"native {timeframe} optimizer_train is empty")

        fit_meta = {
            "train": train_meta,
            "split": {
                "contract_version": contract_version,
                "production_split_mode": CALENDAR_SPLIT_MODE,
                "source_authority": "broker_native_timeframe",
                "cutoff_authority": "authenticated_native_activation",
                "as_of_utc": pd.Timestamp(as_of).isoformat(),
                "activation_id": universe.activation_id,
                "activation_manifest_sha256": universe.activation_manifest_sha256,
                "workflow_rows": int(len(workflow)),
                "workflow_source_start_index": 0,
                "optimizer_train_indices": [
                    int(train_meta["start_index"]),
                    int(train_meta["end_index"]),
                ],
                "optimizer_train_source_indices": [
                    int(train_meta["start_index"]),
                    int(train_meta["end_index"]),
                ],
                "tuning_validation_indices": [
                    int(validation_meta["start_index"]),
                    int(validation_meta["end_index"]),
                ],
                "final_lockbox_indices": [
                    int(lockbox_meta["start_index"]),
                    int(lockbox_meta["end_index"]),
                ],
                "evidence_purge_seconds": float(purge.total_seconds()),
                "evidence_window_years": (
                    dict((contract.get("timeframe_window_years") or {}).get(timeframe) or {})
                    if contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                    else dict(getattr(self.config, "evidence_window_years", {}) or {})
                ),
                "calendar_boundaries": {
                    "contract_version": contract_version,
                    "as_of_utc": as_of.isoformat(),
                    "embargo_seconds": float(purge.total_seconds()),
                    "windows": {
                        name: {
                            "start_ts": start.isoformat(),
                            "end_ts": end.isoformat(),
                        }
                        for name, (start, end) in parsed_windows.items()
                    },
                    "source": "authenticated_native_activation",
                },
            },
        }
        start_index = int(train_meta["start_index"])
        end_index = int(train_meta["end_index"])
        return workflow.iloc[start_index:end_index].copy(), fit_meta, dict(source_meta or {})
    
    def tune_symbol_timeframe(self, symbol: str, timeframe: str,
                               param_space: Optional[Dict] = None) -> Tuple[RegimeParams, Dict]:
        """
        Tune parameters for a single symbol/timeframe combination.
        
        Args:
            symbol: Symbol name
            timeframe: Timeframe string
            param_space: Optional custom parameter search space
            
        Returns:
            Tuple of (best_params, tuning_results)
        """
        timeframe = str(timeframe or "").strip().upper()
        if self.split_mode == CALENDAR_SPLIT_MODE:
            try:
                df, fit_meta, source_meta = self._optimizer_train_native_timeframe(
                    symbol,
                    timeframe,
                )
            except Exception as exc:
                logger.warning(
                    f"{symbol} {timeframe}: invalid native optimizer-train split: {exc}"
                )
                return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}
        else:
            # Legacy v1 detector authority is the M5 optimizer-train slice,
            # resampled only after the split has been applied.
            df_m5 = self.data_loader.load_symbol(symbol)
            if df_m5 is None:
                logger.warning(f"No data for {symbol}, using defaults")
                return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}
            try:
                fit_m5, fit_meta = self._optimizer_train_m5(df_m5)
            except Exception as exc:
                logger.warning(f"{symbol} {timeframe}: invalid optimizer-train split: {exc}")
                return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}
            df = self.data_loader.resample(
                fit_m5,
                timeframe,
                start_ts=fit_meta["train"]["start_ts"],
                end_ts_exclusive=fit_meta["train"]["end_ts_exclusive"],
            )
            source_meta = self.data_loader.get_source_meta(symbol)

        if len(df) < 500:
            logger.warning(f"{symbol} {timeframe}: Only {len(df)} bars, need 500+, using defaults")
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}

        # ----- Train / Holdout split (B8: evaluate on holdout, not training data) -----
        holdout_pct = 0.20  # 20% most-recent bars reserved for evaluation
        split_idx = int(len(df) * (1.0 - holdout_pct))
        df_train = df.iloc[:split_idx]
        df_holdout = df.iloc[split_idx:]

        default_detector = MarketRegimeDetector()
        min_eval_bars = max(default_detector.warmup_bars + 25, 100)
        if len(df_train) < max(300, min_eval_bars) or len(df_holdout) < min_eval_bars:
            logger.warning(f"{symbol} {timeframe}: Insufficient bars for train/holdout split, using defaults")
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}

        # Get param search space
        if param_space is None:
            param_space = PARAM_SEARCH_SPACE.get(timeframe, {
                'k_confirm': [2, 3],
                'gap_min': [0.08, 0.10, 0.12],
                'k_hold': [3, 4, 5]
            })

        # Generate combinations
        param_names = list(param_space.keys())
        param_values = list(param_space.values())
        combinations = list(product(*param_values))

        logger.info(f"[{symbol}] [{timeframe}] Testing {len(combinations)} param combinations "
                     f"(train={len(df_train)}, holdout={len(df_holdout)} bars)")

        best_score = -float('inf')
        best_params = DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams())
        best_metrics = {}

        for combo in combinations:
            params_dict = dict(zip(param_names, combo))

            # Create params object
            params = RegimeParams(
                k_confirm=params_dict.get('k_confirm', 2),
                gap_min=params_dict.get('gap_min', 0.10),
                k_hold=params_dict.get('k_hold', 3)
            )

            try:
                # Evaluate on both train and holdout while heavily weighting holdout.
                detector = MarketRegimeDetector(params)
                warmup_bars = detector.warmup_bars
                if len(df_train) < warmup_bars + 25 or len(df_holdout) < warmup_bars + 25:
                    continue

                train_result = detector.compute_regime_scores(df_train)

                holdout_result = detector.compute_regime_scores(df_holdout)

                train_metrics = compute_regime_quality_metrics(
                    train_result['REGIME'],
                    train_result['REGIME_GAP'],
                    df_train['Close'],
                    warmup_bars=warmup_bars,
                )
                holdout_metrics = compute_regime_quality_metrics(
                    holdout_result['REGIME'],
                    holdout_result['REGIME_GAP'],
                    df_holdout['Close'],
                    warmup_bars=warmup_bars,
                )
                train_score = compute_tuning_score(train_metrics)
                holdout_score = compute_tuning_score(holdout_metrics)
                dominant_score = max(train_score, holdout_score)
                consistency = (min(train_score, holdout_score) / dominant_score) if dominant_score > 0 else 0.0
                score = (0.30 * train_score + 0.70 * holdout_score) * (0.90 + 0.10 * consistency)
                metrics = {
                    'train_score': float(train_score),
                    'holdout_score': float(holdout_score),
                    'consistency': float(consistency),
                    'combined_score': float(score),
                    'warmup_bars': int(warmup_bars),
                    'train_metrics': train_metrics,
                    'holdout_metrics': holdout_metrics,
                }

                if score > best_score:
                    best_score = score
                    best_params = params
                    best_metrics = metrics

            except Exception as e:
                logger.debug(f"Combo {params_dict} failed: {e}")
                continue
        
        logger.info(f"[{symbol}] [{timeframe}] Best: k_confirm={best_params.k_confirm}, "
                   f"gap_min={best_params.gap_min:.2f}, k_hold={best_params.k_hold} "
                   f"(score={best_score:.3f})")
        
        if best_score == -float('inf'):
            logger.warning(f"{symbol} {timeframe}: no detector combination produced valid evidence")
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}

        provenance = {
            "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
            "status": "FIT",
            "symbol": str(symbol),
            "timeframe": str(timeframe).upper(),
            "fit_scope": REGIME_PROVENANCE_FIT_SCOPE,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": source_meta,
            "train": fit_meta["train"],
            "split": fit_meta["split"],
            "internal_validation": {
                "scope": "newest_20pct_within_optimizer_train",
                "train_rows": int(len(df_train)),
                "validation_rows": int(len(df_holdout)),
            },
            "params_sha256": regime_params_sha256(best_params.to_dict()),
        }
        return best_params, {
            'score': best_score,
            'metrics': best_metrics,
            'provenance': provenance,
        }
    
    def tune_all(self, symbols: List[str], timeframes: List[str],
                 output_path: str = "regime_params.json",
                 provenance_output_path: Optional[str] = None
                 ) -> Dict[str, Dict[str, RegimeParams]]:
        """Tune and atomically promote one complete active-cell generation.

        Availability-excluded cells are deliberately absent. Any active cell
        that cannot produce a successful train-only fit makes the generation
        incomplete: fixed-size staging artifacts are retained for diagnosis,
        while the last promoted parameter/provenance pair remains untouched.
        """
        normalized_symbols = list(dict.fromkeys(
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        ))
        normalized_timeframes = list(dict.fromkeys(
            str(timeframe or "").strip().upper()
            for timeframe in timeframes
            if str(timeframe or "").strip()
        ))
        activation_meta: Dict[str, Any] = {}
        if self.split_mode == CALENDAR_SPLIT_MODE:
            universe = self._native_universe()
            if set(normalized_symbols) != set(universe.requested_symbols):
                missing = sorted(set(universe.requested_symbols) - set(normalized_symbols))
                extra = sorted(set(normalized_symbols) - set(universe.requested_symbols))
                raise RegimeTuningIncompleteError(
                    "production regime generation must cover the complete activated symbol "
                    f"roster (missing={missing[:12]}, extra={extra[:12]})"
                )
            if set(normalized_timeframes) != set(universe.required_timeframes):
                raise RegimeTuningIncompleteError(
                    "production regime generation must cover the complete activated timeframe roster"
                )
            unknown_symbols = sorted(
                set(normalized_symbols) - set(universe.requested_symbols)
            )
            if unknown_symbols:
                raise RegimeTuningIncompleteError(
                    "requested symbols are unbound from the active native universe: "
                    + ",".join(unknown_symbols)
                )
            unknown_timeframes = sorted(
                set(normalized_timeframes) - set(universe.required_timeframes)
            )
            if unknown_timeframes:
                raise RegimeTuningIncompleteError(
                    "requested timeframes are unbound from the active native universe: "
                    + ",".join(unknown_timeframes)
                )
            required_cells = [
                (symbol, timeframe)
                for symbol in normalized_symbols
                for timeframe in normalized_timeframes
                if universe.cell_status(symbol, timeframe) == "active"
            ]
            activation_meta = {
                "activation_id": universe.activation_id,
                "activation_manifest_sha256": universe.activation_manifest_sha256,
                "window_contract_version": str(
                    (universe.calendar_contract or {}).get("window_contract_version")
                    or EVIDENCE_CONTRACT_VERSION
                ),
            }
        else:
            required_cells = [
                (symbol, timeframe)
                for symbol in normalized_symbols
                for timeframe in normalized_timeframes
            ]
        if not required_cells:
            raise RegimeTuningIncompleteError("no active regime cells were selected")

        all_params: Dict[str, Dict[str, RegimeParams]] = {}
        provenance_cells: Dict[str, Dict[str, Dict[str, Any]]] = {}
        failures: List[Dict[str, str]] = []

        total = len(required_cells)
        for current, (symbol, tf_key) in enumerate(required_cells, start=1):
            logger.info(f"Progress: {current}/{total} active cells")
            try:
                best_params, tuning_result = self.tune_symbol_timeframe(symbol, tf_key)
            except Exception as exc:
                logger.exception("%s %s regime fitting raised", symbol, tf_key)
                best_params, tuning_result = None, {}
                failures.append({
                    "symbol": symbol,
                    "timeframe": tf_key,
                    "reason": f"fit raised {type(exc).__name__}: {exc}",
                })
            else:
                provenance = dict(tuning_result.get("provenance", {}) or {})
                if provenance and best_params is not None:
                    # Hash the exact row that will be serialized below.
                    provenance["params_sha256"] = regime_params_sha256(best_params.to_dict())
                    all_params.setdefault(symbol, {})[tf_key] = best_params
                    provenance_cells.setdefault(symbol, {})[tf_key] = provenance
                else:
                    failures.append({
                        "symbol": symbol,
                        "timeframe": tf_key,
                        "reason": "no successful optimizer_train-only fit",
                    })

            next_symbol = (
                required_cells[current][0] if current < total else None
            )
            if next_symbol != symbol:
                # One symbol is the bounded memory lifecycle. No frame or
                # resample cache survives into the next symbol.
                self.data_loader.clear_cache()
                self.native_data_loader.clear_cache()
        params_path = Path(output_path)
        params_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path = (
            Path(provenance_output_path)
            if provenance_output_path
            else regime_params_provenance_path(output_path)
        )
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        params_stage = params_path.with_name(params_path.name + ".staging")
        sidecar_stage = sidecar_path.with_name(sidecar_path.name + ".staging")
        serialized_params = {
            symbol: {
                timeframe: params.to_dict()
                for timeframe, params in sorted(tf_map.items())
            }
            for symbol, tf_map in sorted(all_params.items())
            if tf_map
        }
        params_bytes = json.dumps(
            serialized_params,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        params_stage.write_bytes(params_bytes)
        params_sha256 = hashlib.sha256(params_bytes).hexdigest()
        required_cell_ids = [
            f"{symbol}|{timeframe}" for symbol, timeframe in required_cells
        ]
        sidecar_payload = {
            "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "params_path": str(Path(output_path).resolve()),
            "params_file_sha256": params_sha256,
            "fit_scope": REGIME_PROVENANCE_FIT_SCOPE,
            "generation_status": "INCOMPLETE" if failures else "COMPLETE",
            "required_active_cells": required_cell_ids,
            "required_active_cell_count": len(required_cell_ids),
            "fitted_active_cell_count": sum(
                len(tf_map) for tf_map in provenance_cells.values()
            ),
            **activation_meta,
            "split_contract_version": (
                activation_meta.get("window_contract_version", EVIDENCE_CONTRACT_VERSION)
                if self.split_mode == CALENDAR_SPLIT_MODE
                else DataSplitter.SPLIT_CONTRACT_VERSION
            ),
            "cells": provenance_cells,
            "failures": failures,
        }
        sidecar_stage.write_text(
            json.dumps(sidecar_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        logger.info(
            "Staged regime fit provenance for %d cells at %s (%d failed cells)",
            sum(len(tf_map) for tf_map in provenance_cells.values()),
            sidecar_stage,
            len(failures),
        )

        if failures:
            raise RegimeTuningIncompleteError(
                f"regime generation incomplete: {len(failures)}/{total} active cells failed; "
                f"last promoted artifacts were not changed; diagnostics: {sidecar_stage}"
            )

        # Both staged artifacts are fully written before either promoted path
        # changes. Readers validate the pair hash and therefore fail closed in
        # the tiny two-replace interval; an incomplete run never reaches here.
        os.replace(sidecar_stage, sidecar_path)
        os.replace(params_stage, params_path)
        logger.info(
            "Promoted complete regime generation: %d active cells -> %s",
            total,
            params_path,
        )
        return all_params


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Tune regime detection parameters for FX Portfolio Manager"
    )
    parser.add_argument(
        '--config', type=str, default='config.json',
        help='FXPM config containing the chronological lockbox split'
    )
    parser.add_argument(
        '--data-dir', type=str, default=None,
        help='Data directory with M5 CSV files (overrides config.pipeline.data_dir)'
    )
    parser.add_argument(
        '--output', type=str, default=None,
        help='Output file path (defaults to config.pipeline.regime_params_file)'
    )
    parser.add_argument(
        '--provenance-output', type=str, default=None,
        help='Optional explicit provenance sidecar path'
    )
    parser.add_argument(
        '--symbols', type=str, nargs='+', default=None,
        help='Symbols to tune (default: all in data dir)'
    )
    parser.add_argument(
        '--timeframes', type=str, nargs='+', 
        default=['M5', 'M15', 'M30', 'H1', 'H4', 'D1'],
        help='Timeframes to tune'
    )
    parser.add_argument(
        '--log-level', type=str, default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    
    config_path = Path(args.config).resolve()
    try:
        config_payload = json.loads(config_path.read_text(encoding="utf-8"))
        pipeline_payload = config_payload.get("pipeline", config_payload)
        allowed = {item.name for item in fields(PipelineConfig)}
        pipeline_config = PipelineConfig(**{
            key: value for key, value in dict(pipeline_payload or {}).items()
            if key in allowed
        })
    except Exception as exc:
        logger.error(f"Failed to load pipeline split config from {config_path}: {exc}")
        return 2

    configured_data_dir = Path(str(getattr(pipeline_config, "data_dir", "./data")))
    if not configured_data_dir.is_absolute():
        configured_data_dir = config_path.parent / configured_data_dir
    data_dir = Path(args.data_dir).resolve() if args.data_dir else configured_data_dir.resolve()
    output_setting = args.output or str(
        getattr(pipeline_config, "regime_params_file", "regime_params.json")
    )
    output_path = Path(output_setting)
    if not output_path.is_absolute():
        output_path = config_path.parent / output_path
    
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return 1
    
    # Calendar production tunes the configured institutional roster, then the
    # authenticated activation narrows it to active symbol/timeframe cells.
    # Filesystem globbing is a legacy fallback only; it silently missed native
    # run layouts and was one cause of incomplete detector books.
    if args.symbols is None:
        configured_symbols = config_payload.get("symbols")
        if isinstance(configured_symbols, list) and configured_symbols:
            symbols = list(dict.fromkeys(
                str(symbol or "").strip().upper()
                for symbol in configured_symbols
                if str(symbol or "").strip()
            ))
        else:
            csv_files = list(data_dir.glob("*_M5*.csv"))
            symbols = sorted(set(f.name.split('_')[0].upper() for f in csv_files))
    else:
        symbols = [s.upper() for s in args.symbols]
    
    if not symbols:
        logger.error("No symbols found. Check --data-dir path.")
        return 1
    
    logger.info("Tuning regime parameters")
    logger.info(f"  Symbols: {len(symbols)}")
    logger.info(f"  Timeframes: {args.timeframes}")
    logger.info(f"  Output: {output_path}")
    
    # Run tuning
    try:
        tuner = RegimeParamTuner(data_dir, config=pipeline_config)
    except ValueError as exc:
        logger.error(str(exc))
        return 2
    provenance_output = Path(args.provenance_output) if args.provenance_output else None
    if provenance_output is not None and not provenance_output.is_absolute():
        provenance_output = config_path.parent / provenance_output
    try:
        tuner.tune_all(
            symbols,
            args.timeframes,
            str(output_path),
            provenance_output_path=(
                str(provenance_output) if provenance_output is not None else None
            ),
        )
    except RegimeTuningIncompleteError as exc:
        logger.error(str(exc))
        return 3
    
    logger.info("Tuning complete!")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
