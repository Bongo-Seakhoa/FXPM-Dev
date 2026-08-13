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
    python pm_regime_tuner.py --config config.json --data-dir ./data
    python pm_regime_tuner.py --symbols EURUSD GBPUSD --timeframes H1 H4

Calendar production reads independently acquired native timeframe leaves from
the authenticated ``<data-dir>/native/ACTIVE.json`` activation. Flat M5 CSV
input is supported only by the legacy chronological-lockbox mode.

Version: 4.0 (chronological train-only detector fitting)
"""

import argparse
import hashlib
import inspect
import json
import logging
import math
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from itertools import product

import pandas as pd

from pm_core import (
    DataLoader,
    DataSplitter,
    PipelineConfig,
    strict_pipeline_config_kwargs,
)
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
    NATIVE_EVIDENCE_CANDIDATE,
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
    """Raised when a regime generation cannot be safely promoted as a whole."""


REGIME_NON_SEARCH_SCHEMA_VERSION = "fxpm.regime-detector-non-search/v1"
REGIME_TRAIN_SUPPORT_SCHEMA_VERSION = "fxpm.regime-train-support/v1"
REGIME_TRAIN_SUPPORT_VECTOR_ENCODING = "fxpm.regime-live-labels-nul/v1"
REGIME_TRAIN_SUPPORT_WARMUP_POLICY = "CURRENT_AND_SOURCE_BAR_NOT_WARMUP"
REGIME_TUNER_MIN_TRAIN_ROWS = 500
REGIME_METADATA_MAX_DEPTH = 10
REGIME_METADATA_MAX_NODES = 2048
REGIME_METADATA_MAX_CONTAINER_ITEMS = 512
REGIME_METADATA_MAX_STRING_BYTES = 4096
REGIME_METADATA_MAX_SERIALIZED_BYTES = 64 * 1024
_NON_SEARCH_DECISION_KINDS = {
    "INSUFFICIENT_TRAIN_HISTORY",
    "TECHNICAL_EXCLUSION",
}
_NON_SEARCH_REASON_CODES_BY_KIND = {
    "INSUFFICIENT_TRAIN_HISTORY": {
        "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
        "INSUFFICIENT_DETECTOR_HOLDOUT_ROWS",
    },
    "TECHNICAL_EXCLUSION": {
        "NATIVE_OPTIMIZER_TRAIN_INVALID",
        "DETECTOR_FIT_NO_VALID_COMBINATION",
        "REGIME_FIT_RAISED",
        "REGIME_FIT_RESULT_INVALID",
        "REGIME_FIT_RESULT_PROCESSING_FAILED",
    },
}


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
        self._native_universe_resolved_from_disk = False
        self._detector_code_sha256_cache: Optional[str] = None
        self._fit_support_authority: Dict[Tuple[str, str], str] = {}

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
            self._native_universe_resolved_from_disk = True
        return self._active_native_universe

    def _active_native_identity_still_current(self) -> Tuple[bool, str]:
        if not self._native_universe_resolved_from_disk:
            return True, "injected test universe"
        pinned = self._native_universe()
        current = resolve_active_native_universe(
            self.data_dir,
            required_timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
            production_contract_required=True,
        )
        if not current.valid or not current.explicit_contract:
            return False, "active native universe became invalid during tuning"
        if (
            str(current.activation_id) != str(pinned.activation_id)
            or str(current.activation_manifest_sha256).lower()
            != str(pinned.activation_manifest_sha256).lower()
        ):
            return False, "active native universe changed during tuning"
        return True, "active native universe identity unchanged"

    @staticmethod
    def _evidence_candidate_timeframes(universe: Any, symbol: str) -> Tuple[str, ...]:
        """Return data-authentic cells without converting them to trade verdicts."""
        reader = getattr(universe, "active_timeframes", None)
        if callable(reader):
            return tuple(
                dict.fromkeys(str(value or "").strip().upper() for value in reader(symbol))
            )
        required_timeframes = tuple(
            getattr(universe, "required_timeframes", ())
            or NATIVE_PRODUCTION_TIMEFRAMES
        )
        return tuple(
            timeframe
            for timeframe in required_timeframes
            if str(universe.cell_status(symbol, timeframe)).lower()
            in {"active", NATIVE_EVIDENCE_CANDIDATE}
        )

    @staticmethod
    def _bounded_text(value: Any, limit: int = 512) -> str:
        """Return deterministic, bounded diagnostic text for persisted artifacts."""
        text = str(value or "").replace("\x00", "").strip()
        return text[: max(0, int(limit))]

    @classmethod
    def _json_safe_mapping(
        cls,
        value: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Copy exact metadata within fixed structural and byte bounds.

        Oversized metadata is rejected instead of truncated so source and
        split identities cannot lose accuracy while memory and artifacts stay
        bounded.
        """
        if not isinstance(value, dict):
            return {}

        nodes = 0

        def copy_value(item: Any, depth: int) -> Any:
            nonlocal nodes
            if depth > REGIME_METADATA_MAX_DEPTH:
                raise ValueError("regime metadata exceeds the depth bound")
            nodes += 1
            if nodes > REGIME_METADATA_MAX_NODES:
                raise ValueError("regime metadata exceeds the node bound")
            if isinstance(item, dict):
                if len(item) > REGIME_METADATA_MAX_CONTAINER_ITEMS:
                    raise ValueError("regime metadata mapping exceeds the item bound")
                copied: Dict[str, Any] = {}
                for key, nested in item.items():
                    if not isinstance(key, str):
                        raise ValueError("regime metadata keys must be strings")
                    if len(key.encode("utf-8")) > REGIME_METADATA_MAX_STRING_BYTES:
                        raise ValueError("regime metadata key exceeds the byte bound")
                    copied[key] = copy_value(nested, depth + 1)
                return copied
            if isinstance(item, (list, tuple)):
                if len(item) > REGIME_METADATA_MAX_CONTAINER_ITEMS:
                    raise ValueError("regime metadata sequence exceeds the item bound")
                return [copy_value(nested, depth + 1) for nested in item]
            if item is None or isinstance(item, (bool, int)):
                return item
            if isinstance(item, float):
                if not math.isfinite(item):
                    raise ValueError("regime metadata contains a non-finite number")
                return item
            text = item if isinstance(item, str) else str(item)
            if len(text.encode("utf-8")) > REGIME_METADATA_MAX_STRING_BYTES:
                raise ValueError("regime metadata string exceeds the byte bound")
            return text

        copied = copy_value(value, 0)
        serialized = json.dumps(
            copied,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(serialized) > REGIME_METADATA_MAX_SERIALIZED_BYTES:
            raise ValueError("regime metadata exceeds the serialized byte bound")
        return copied

    @staticmethod
    def _is_sha256(value: Any) -> bool:
        text = str(value or "").strip().lower()
        if len(text) != 64:
            return False
        try:
            int(text, 16)
        except ValueError:
            return False
        return True

    @staticmethod
    def _canonical_mapping_sha256(value: Dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _detector_code_sha256(self) -> str:
        if self._detector_code_sha256_cache is None:
            source_file = inspect.getsourcefile(MarketRegimeDetector)
            if not source_file:
                raise ValueError("regime detector source file cannot be resolved")
            self._detector_code_sha256_cache = TunerDataLoader._sha256(
                Path(source_file).resolve()
            )
        return self._detector_code_sha256_cache

    def _build_train_regime_support(
        self,
        frame: pd.DataFrame,
        params: RegimeParams,
        source_meta: Dict[str, Any],
        train_meta: Dict[str, Any],
        symbol: str,
        timeframe: str,
    ) -> Dict[str, Any]:
        """Hash the exact post-warmup optimizer-train REGIME_LIVE surface."""
        detector = MarketRegimeDetector(params)
        surface_frame = detector.compute_regime_scores(frame)
        if not surface_frame.index.equals(frame.index):
            raise ValueError("regime support surface index is inconsistent")
        required = {"REGIME_LIVE", "REGIME_WARMUP"}
        if not required.issubset(surface_frame.columns):
            raise ValueError("regime support surface columns are missing")
        warmup = surface_frame["REGIME_WARMUP"]
        if warmup.isna().any() or not warmup.isin([True, False]).all():
            raise ValueError("regime support warmup markers are invalid")
        warmup = warmup.astype(bool)
        source_warmup = warmup.shift(1, fill_value=True)
        eligible_mask = (~warmup) & (~source_warmup)
        surface = surface_frame.loc[eligible_mask, "REGIME_LIVE"]
        if surface.empty:
            raise ValueError("regime support surface has no post-warmup rows")
        if surface.isna().any():
            raise ValueError("regime support surface contains missing labels")

        label_order = list(RegimeType.ALL)
        counts = {label: 0 for label in label_order}
        vector_digest = hashlib.sha256()
        vector_digest.update(b"fxpm.regime-live-labels-nul/v1\0")
        for value in surface.array:
            if not isinstance(value, str) or value not in counts:
                raise ValueError(f"regime support surface has unknown label: {value}")
            counts[value] += 1
            vector_digest.update(value.encode("ascii"))
            vector_digest.update(b"\0")
        observed = [label for label in label_order if counts[label] > 0]
        unobserved = [label for label in label_order if counts[label] == 0]
        support_state_by_regime = {
            label: (
                "EMPIRICALLY_OBSERVED" if counts[label] > 0 else "UNOBSERVED"
            )
            for label in label_order
        }
        source = self._json_safe_mapping(source_meta)
        train = self._json_safe_mapping(train_meta)
        params_sha256 = regime_params_sha256(params.to_dict())
        bindings = {
            "symbol": str(symbol or "").strip().upper(),
            "timeframe": str(timeframe or "").strip().upper(),
            "activation_id": source.get("activation_id"),
            "activation_manifest_sha256": source.get(
                "activation_manifest_sha256"
            ),
            "source_sha256": source.get("sha256"),
            "train_window_sha256": self._canonical_mapping_sha256(train),
            "params_sha256": params_sha256,
            "detector_code_sha256": self._detector_code_sha256(),
        }
        receipt: Dict[str, Any] = {
            "schema_version": REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
            "symbol": str(symbol or "").strip().upper(),
            "timeframe": str(timeframe or "").strip().upper(),
            "surface": "REGIME_LIVE",
            "warmup_policy": REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
            "label_order": label_order,
            "eligible_rows": int(len(surface)),
            "counts": counts,
            "observed_regimes": observed,
            "unobserved_regimes": unobserved,
            "support_state_by_regime": support_state_by_regime,
            "first_ts": pd.Timestamp(surface.index[0]).isoformat(),
            "last_ts": pd.Timestamp(surface.index[-1]).isoformat(),
            "vector_encoding": REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
            "vector_sha256": vector_digest.hexdigest(),
            "bindings": bindings,
        }
        receipt["receipt_sha256"] = self._canonical_mapping_sha256(receipt)
        self._fit_support_authority[
            (
                str(symbol or "").strip().upper(),
                str(timeframe or "").strip().upper(),
            )
        ] = receipt["receipt_sha256"]
        return receipt

    def _validate_train_regime_support(
        self,
        support_record: Dict[str, Any],
        *,
        source: Dict[str, Any],
        train: Dict[str, Any],
        params_dict: Dict[str, Any],
        symbol: str,
        timeframe: str,
    ) -> Dict[str, Any]:
        support = self._json_safe_mapping(support_record)
        if support.get("schema_version") != REGIME_TRAIN_SUPPORT_SCHEMA_VERSION:
            raise ValueError("regime train-support schema is invalid")
        expected_symbol = str(symbol or "").strip().upper()
        expected_timeframe = str(timeframe or "").strip().upper()
        if support.get("symbol") != expected_symbol:
            raise ValueError("regime train-support symbol is invalid")
        if support.get("timeframe") != expected_timeframe:
            raise ValueError("regime train-support timeframe is invalid")
        if support.get("surface") != "REGIME_LIVE":
            raise ValueError("regime train-support surface is invalid")
        if support.get("warmup_policy") != REGIME_TRAIN_SUPPORT_WARMUP_POLICY:
            raise ValueError("regime train-support warmup policy is invalid")
        label_order = list(RegimeType.ALL)
        if support.get("label_order") != label_order:
            raise ValueError("regime train-support label order is invalid")
        counts = support.get("counts")
        if not isinstance(counts, dict) or set(counts) != set(label_order):
            raise ValueError("regime train-support counts are invalid")
        normalized_counts: Dict[str, int] = {}
        for label in label_order:
            value = counts.get(label)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("regime train-support count is invalid")
            normalized_counts[label] = value
        eligible_rows = support.get("eligible_rows")
        if (
            isinstance(eligible_rows, bool)
            or not isinstance(eligible_rows, int)
            or eligible_rows <= 0
            or sum(normalized_counts.values()) != eligible_rows
        ):
            raise ValueError("regime train-support row accounting is invalid")
        expected_observed = [
            label for label in label_order if normalized_counts[label] > 0
        ]
        expected_unobserved = [
            label for label in label_order if normalized_counts[label] == 0
        ]
        if support.get("observed_regimes") != expected_observed:
            raise ValueError("regime train-support observed set is invalid")
        if support.get("unobserved_regimes") != expected_unobserved:
            raise ValueError("regime train-support unobserved set is invalid")
        expected_states = {
            label: (
                "EMPIRICALLY_OBSERVED"
                if normalized_counts[label] > 0
                else "UNOBSERVED"
            )
            for label in label_order
        }
        if support.get("support_state_by_regime") != expected_states:
            raise ValueError("regime train-support state mapping is invalid")
        for field_name in ("first_ts", "last_ts"):
            try:
                timestamp = pd.Timestamp(support.get(field_name))
            except Exception as exc:
                raise ValueError(
                    f"regime train-support timestamp is invalid: {field_name}"
                ) from exc
            if pd.isna(timestamp):
                raise ValueError(
                    f"regime train-support timestamp is missing: {field_name}"
                )
        if support.get("vector_encoding") != REGIME_TRAIN_SUPPORT_VECTOR_ENCODING:
            raise ValueError("regime train-support vector encoding is invalid")
        if not self._is_sha256(support.get("vector_sha256")):
            raise ValueError("regime train-support vector hash is invalid")
        bindings = support.get("bindings")
        if not isinstance(bindings, dict):
            raise ValueError("regime train-support bindings are missing")
        expected_bindings = {
            "symbol": expected_symbol,
            "timeframe": expected_timeframe,
            "activation_id": source.get("activation_id"),
            "activation_manifest_sha256": source.get(
                "activation_manifest_sha256"
            ),
            "source_sha256": source.get("sha256"),
            "train_window_sha256": self._canonical_mapping_sha256(train),
            "params_sha256": regime_params_sha256(params_dict),
            "detector_code_sha256": self._detector_code_sha256(),
        }
        if bindings != expected_bindings:
            raise ValueError("regime train-support bindings are invalid")
        declared_hash = str(support.get("receipt_sha256") or "")
        body = dict(support)
        body.pop("receipt_sha256", None)
        if declared_hash != self._canonical_mapping_sha256(body):
            raise ValueError("regime train-support receipt hash is invalid")
        return support

    def _authenticated_window_contract_version(self) -> str:
        universe = self._native_universe()
        version = str(
            (universe.calendar_contract or {}).get("window_contract_version") or ""
        ).strip()
        if version not in {
            EVIDENCE_CONTRACT_VERSION,
            TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        }:
            raise ValueError("authenticated native window contract is invalid")
        return version

    def _validate_native_source_identity(
        self,
        source_meta: Dict[str, Any],
        symbol: str,
        timeframe: str,
    ) -> Dict[str, Any]:
        """Bind a native source fingerprint to the cached activation exactly."""
        source = self._json_safe_mapping(source_meta)
        universe = self._native_universe()
        expected_symbol = str(symbol or "").strip().upper()
        expected_timeframe = str(timeframe or "").strip().upper()
        trusted_defaults = {
            "symbol": expected_symbol,
            "timeframe": expected_timeframe,
            "activation_id": str(universe.activation_id),
            "activation_manifest_sha256": str(
                universe.activation_manifest_sha256
            ),
            "provenance_errors": [],
        }
        for field_name, trusted_value in trusted_defaults.items():
            if field_name not in source:
                source[field_name] = trusted_value
        if str(source.get("source_kind") or "").strip().lower() != "broker_native":
            raise ValueError("native source authority is invalid")
        if source.get("provenance_valid") is not True:
            raise ValueError("native source provenance is not valid")
        if list(source.get("provenance_errors") or []):
            raise ValueError("native source carries provenance errors")
        if str(source.get("symbol") or "").strip().upper() != expected_symbol:
            raise ValueError("native source symbol identity is invalid")
        if str(source.get("timeframe") or "").strip().upper() != expected_timeframe:
            raise ValueError("native source timeframe identity is invalid")
        if str(source.get("activation_id") or "") != str(universe.activation_id):
            raise ValueError("native source activation id changed during tuning")
        if str(source.get("activation_manifest_sha256") or "").lower() != str(
            universe.activation_manifest_sha256
        ).lower():
            raise ValueError("native source activation hash changed during tuning")
        if not str(source.get("path") or "").strip():
            raise ValueError("native source path is missing")
        try:
            source_size = int(source.get("size"))
        except (TypeError, ValueError) as exc:
            raise ValueError("native source size is invalid") from exc
        if source_size < 0 or not self._is_sha256(source.get("sha256")):
            raise ValueError("native source fingerprint is invalid")
        return source

    @staticmethod
    def _validate_train_geometry(train: Dict[str, Any]) -> Tuple[int, int]:
        try:
            start = int(train.get("start_index"))
            end = int(train.get("end_index"))
            rows = int(train.get("rows"))
        except (TypeError, ValueError) as exc:
            raise ValueError("regime FIT train geometry is invalid") from exc
        if start < 0 or end <= start or rows != end - start:
            raise ValueError("regime FIT train geometry is inconsistent")
        for field_name in ("start_ts", "end_ts_inclusive", "end_ts_exclusive"):
            try:
                timestamp = pd.Timestamp(train.get(field_name))
            except Exception as exc:
                raise ValueError(
                    f"regime FIT train timestamp is invalid: {field_name}"
                ) from exc
            if pd.isna(timestamp):
                raise ValueError(f"regime FIT train timestamp is missing: {field_name}")
        return start, end

    def _validate_fit_provenance(
        self,
        record: Dict[str, Any],
        params_dict: Dict[str, Any],
        symbol: str,
        timeframe: str,
    ) -> Dict[str, Any]:
        """Validate a complete detector FIT receipt before it can be promoted."""
        receipt = self._json_safe_mapping(record)
        expected_symbol = str(symbol or "").strip().upper()
        expected_timeframe = str(timeframe or "").strip().upper()
        if receipt.get("schema_version") != REGIME_PROVENANCE_SCHEMA_VERSION:
            raise ValueError("regime FIT provenance schema is invalid")
        if str(receipt.get("status") or "").upper() != "FIT":
            raise ValueError("regime FIT provenance status is invalid")
        if str(receipt.get("symbol") or "").upper() != expected_symbol:
            raise ValueError("regime FIT provenance symbol is invalid")
        if str(receipt.get("timeframe") or "").upper() != expected_timeframe:
            raise ValueError("regime FIT provenance timeframe is invalid")
        if str(receipt.get("fit_scope") or "").lower() != REGIME_PROVENANCE_FIT_SCOPE:
            raise ValueError("regime FIT provenance scope is invalid")
        if receipt.get("params_sha256") != regime_params_sha256(params_dict):
            raise ValueError("regime FIT parameter hash is invalid")
        try:
            generated_at = pd.Timestamp(receipt.get("generated_at"))
        except Exception as exc:
            raise ValueError("regime FIT generation timestamp is invalid") from exc
        if pd.isna(generated_at):
            raise ValueError("regime FIT generation timestamp is missing")

        source = receipt.get("source")
        train = receipt.get("train")
        split = receipt.get("split")
        if not isinstance(source, dict) or not isinstance(train, dict) or not isinstance(split, dict):
            raise ValueError("regime FIT source/train/split provenance is incomplete")
        train_start, train_end = self._validate_train_geometry(train)
        try:
            optimizer_train = [int(value) for value in split.get("optimizer_train_indices", [])]
            tuning_validation = [
                int(value) for value in split.get("tuning_validation_indices", [])
            ]
            final_lockbox = [int(value) for value in split.get("final_lockbox_indices", [])]
        except (TypeError, ValueError) as exc:
            raise ValueError("regime FIT split indices are invalid") from exc
        if optimizer_train != [train_start, train_end]:
            raise ValueError("regime FIT train and split geometry disagree")
        if len(tuning_validation) != 2 or len(final_lockbox) != 2:
            raise ValueError("regime FIT validation/lockbox geometry is missing")
        if not (
            train_end <= tuning_validation[0]
            < tuning_validation[1]
            <= final_lockbox[0]
            < final_lockbox[1]
        ):
            raise ValueError("regime FIT split geometry leaks beyond optimizer_train")

        if self.split_mode == CALENDAR_SPLIT_MODE:
            source = self._validate_native_source_identity(
                source,
                expected_symbol,
                expected_timeframe,
            )
            receipt["source"] = source
            universe = self._native_universe()
            expected_contract = self._authenticated_window_contract_version()
            if split.get("contract_version") != expected_contract:
                raise ValueError("regime FIT window contract is invalid")
            if str(split.get("production_split_mode") or "").lower() != CALENDAR_SPLIT_MODE:
                raise ValueError("regime FIT production split mode is invalid")
            if str(split.get("source_authority") or "").lower() != "broker_native_timeframe":
                raise ValueError("regime FIT source authority is invalid")
            if str(split.get("cutoff_authority") or "").lower() != "authenticated_native_activation":
                raise ValueError("regime FIT cutoff authority is invalid")
            if str(split.get("activation_id") or "") != str(universe.activation_id):
                raise ValueError("regime FIT split activation id is invalid")
            if str(split.get("activation_manifest_sha256") or "").lower() != str(
                universe.activation_manifest_sha256
            ).lower():
                raise ValueError("regime FIT split activation hash is invalid")
            contract = dict(universe.calendar_contract or {})
            expected_as_of = str(contract.get("common_as_of_utc") or "").strip()
            if expected_as_of:
                try:
                    observed_as_of = pd.Timestamp(split.get("as_of_utc"))
                    pinned_as_of = pd.Timestamp(expected_as_of)
                except Exception as exc:
                    raise ValueError("regime FIT information cutoff is invalid") from exc
                if observed_as_of != pinned_as_of:
                    raise ValueError("regime FIT information cutoff changed")
            calendar_boundaries = split.get("calendar_boundaries")
            if expected_as_of or contract.get("timeframe_calendar_boundaries"):
                if not isinstance(calendar_boundaries, dict):
                    raise ValueError("regime FIT calendar boundaries are missing")
                if calendar_boundaries.get("contract_version") != expected_contract:
                    raise ValueError("regime FIT calendar boundary contract is invalid")
        else:
            if split.get("contract_version") != DataSplitter.SPLIT_CONTRACT_VERSION:
                raise ValueError("legacy regime FIT split contract is invalid")
            if str(split.get("production_split_mode") or "").lower() != "chronological_lockbox_v1":
                raise ValueError("legacy regime FIT split mode is invalid")
            if not str(source.get("path") or "").strip():
                raise ValueError("legacy regime FIT source path is missing")
            try:
                source_size = int(source.get("size"))
            except (TypeError, ValueError) as exc:
                raise ValueError("legacy regime FIT source size is invalid") from exc
            if source_size < 0 or not self._is_sha256(source.get("sha256")):
                raise ValueError("legacy regime FIT source fingerprint is invalid")
        support = receipt.get("train_regime_support")
        if not isinstance(support, dict):
            raise ValueError("regime FIT train-support receipt is missing")
        receipt["train_regime_support"] = self._validate_train_regime_support(
            support,
            source=source,
            train=train,
            params_dict=params_dict,
            symbol=expected_symbol,
            timeframe=expected_timeframe,
        )
        support_hash = receipt["train_regime_support"].get("receipt_sha256")
        if self._fit_support_authority.get(
            (expected_symbol, expected_timeframe)
        ) != support_hash:
            raise ValueError("regime FIT train-support writer authority is invalid")
        return receipt

    @staticmethod
    def _promote_artifact_pair(
        params_stage: Path,
        params_path: Path,
        sidecar_stage: Path,
        sidecar_path: Path,
    ) -> None:
        """Publish both files and restore the exact previous pair on failure."""
        if params_path.resolve() == sidecar_path.resolve():
            raise RegimeTuningIncompleteError(
                "regime params and provenance paths must be distinct"
            )
        token = f"{os.getpid()}-{uuid.uuid4().hex}"
        targets = (
            (params_stage, params_path),
            (sidecar_stage, sidecar_path),
        )
        backups: List[Tuple[Path, Path, bool]] = []
        try:
            for _stage, target in targets:
                existed = target.exists()
                backup = target.with_name(f".{target.name}.{token}.rollback")
                if existed:
                    shutil.copy2(target, backup)
                backups.append((target, backup, existed))
        except Exception as exc:
            for _target, backup, _existed in backups:
                backup.unlink(missing_ok=True)
            raise RegimeTuningIncompleteError(
                f"could not snapshot the previous regime artifact pair: {exc}"
            ) from exc

        preserve_backups = False
        try:
            for stage, target in targets:
                os.replace(stage, target)
        except Exception as exc:
            rollback_errors: List[str] = []
            for target, backup, existed in backups:
                try:
                    if existed:
                        os.replace(backup, target)
                    else:
                        target.unlink(missing_ok=True)
                except Exception as rollback_exc:
                    rollback_errors.append(
                        f"{target}:{type(rollback_exc).__name__}:{rollback_exc}"
                    )
            preserve_backups = bool(rollback_errors)
            suffix = (
                "; rollback errors=" + "|".join(rollback_errors)
                if rollback_errors
                else "; previous pair restored"
            )
            raise RegimeTuningIncompleteError(
                f"regime artifact pair publication failed: {exc}{suffix}"
            ) from exc
        finally:
            if not preserve_backups:
                for _target, backup, _existed in backups:
                    backup.unlink(missing_ok=True)

    def _build_non_search_decision(
        self,
        symbol: str,
        timeframe: str,
        *,
        decision_kind: str,
        reason_code: str,
        reason: str,
        fit_meta: Optional[Dict[str, Any]] = None,
        source_meta: Optional[Dict[str, Any]] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
        error: Optional[BaseException] = None,
    ) -> Dict[str, Any]:
        """Build a provenance-bound non-search decision, never a trade verdict.

        A calendar-native cell can be authentic but still unable to support a
        detector fit (for example, a D1 optimizer-train partition with fewer
        than 500 rows).  That is a data/search-authorization decision, not
        evidence that the symbol or strategy should be marked NO_TRADE.
        """
        if self.split_mode != CALENDAR_SPLIT_MODE:
            raise ValueError("typed non-search decisions require calendar-native mode")
        universe = self._native_universe()
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_timeframe = str(timeframe or "").strip().upper()
        normalized_kind = str(decision_kind or "").strip().upper()
        if normalized_kind not in _NON_SEARCH_DECISION_KINDS:
            raise ValueError(f"unsupported non-search decision kind: {decision_kind}")
        normalized_reason_code = self._bounded_text(reason_code, 128).upper()
        if normalized_reason_code not in _NON_SEARCH_REASON_CODES_BY_KIND[
            normalized_kind
        ]:
            raise ValueError("non-search reason code does not match its decision kind")
        terminal_data_state = normalized_kind == "INSUFFICIENT_TRAIN_HISTORY"

        record: Dict[str, Any] = {
            "schema_version": REGIME_NON_SEARCH_SCHEMA_VERSION,
            "status": "NON_SEARCH",
            "symbol": normalized_symbol,
            "timeframe": normalized_timeframe,
            "fit_scope": REGIME_PROVENANCE_FIT_SCOPE,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "decision_kind": normalized_kind,
            "reason_code": normalized_reason_code,
            "reason": self._bounded_text(reason),
            "activation_id": str(universe.activation_id),
            "activation_manifest_sha256": str(
                universe.activation_manifest_sha256
            ),
            "window_contract_version": self._authenticated_window_contract_version(),
            "economic_evidence_used": False,
            "economic_trade_verdict_emitted": False,
            "trade_verdict": False,
            "no_trade": False,
            "strategy_search_authorized": False,
            "generation_terminal": terminal_data_state,
            "retryable": not terminal_data_state,
        }
        safe_fit_meta = self._json_safe_mapping(fit_meta)
        if safe_fit_meta.get("train"):
            record["train"] = safe_fit_meta["train"]
        if safe_fit_meta.get("split"):
            record["split"] = safe_fit_meta["split"]
        safe_source = self._json_safe_mapping(source_meta)
        if safe_source:
            record["source"] = safe_source
        safe_diagnostics = self._json_safe_mapping(diagnostics)
        if safe_diagnostics:
            record["diagnostics"] = safe_diagnostics
        if error is not None:
            record["error"] = {
                "type": type(error).__name__,
                "message": self._bounded_text(error),
            }

        canonical = json.dumps(record, sort_keys=True, separators=(",", ":"))
        record["decision_sha256"] = hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()
        return record

    def _validate_non_search_decision(
        self,
        record: Dict[str, Any],
        symbol: str,
        timeframe: str,
    ) -> None:
        """Reject an unbound or verdict-bearing non-search record."""
        universe = self._native_universe()
        expected_symbol = str(symbol or "").strip().upper()
        expected_timeframe = str(timeframe or "").strip().upper()
        if not isinstance(record, dict):
            raise ValueError("non-search decision is not a mapping")
        record = self._json_safe_mapping(record)
        if record.get("schema_version") != REGIME_NON_SEARCH_SCHEMA_VERSION:
            raise ValueError("non-search decision schema is invalid")
        if record.get("status") != "NON_SEARCH":
            raise ValueError("non-search decision status is invalid")
        if record.get("symbol") != expected_symbol or record.get("timeframe") != expected_timeframe:
            raise ValueError("non-search decision cell identity is invalid")
        decision_kind = str(record.get("decision_kind") or "")
        if decision_kind not in _NON_SEARCH_DECISION_KINDS:
            raise ValueError("non-search decision kind is invalid")
        reason_code = str(record.get("reason_code") or "").strip()
        if reason_code not in _NON_SEARCH_REASON_CODES_BY_KIND[decision_kind]:
            raise ValueError("non-search reason code does not match its decision kind")
        if str(record.get("fit_scope") or "").lower() != REGIME_PROVENANCE_FIT_SCOPE:
            raise ValueError("non-search decision fit scope is invalid")
        if str(record.get("window_contract_version") or "") != (
            self._authenticated_window_contract_version()
        ):
            raise ValueError("non-search decision window contract is invalid")
        if str(record.get("activation_id") or "") != str(universe.activation_id):
            raise ValueError("non-search decision activation id is invalid")
        if str(record.get("activation_manifest_sha256") or "") != str(
            universe.activation_manifest_sha256
        ):
            raise ValueError("non-search decision activation hash is invalid")
        for field_name in (
            "economic_evidence_used",
            "economic_trade_verdict_emitted",
            "trade_verdict",
            "no_trade",
            "strategy_search_authorized",
        ):
            if record.get(field_name) is not False:
                raise ValueError(
                    f"non-search decision cannot set {field_name}"
                )
        terminal_data_state = decision_kind == "INSUFFICIENT_TRAIN_HISTORY"
        if record.get("generation_terminal") is not terminal_data_state:
            raise ValueError("non-search terminal-state marker is invalid")
        if record.get("retryable") is not (not terminal_data_state):
            raise ValueError("non-search retry marker is invalid")
        split = record.get("split")
        if split is not None:
            if not isinstance(split, dict):
                raise ValueError("non-search decision split metadata is invalid")
            if split.get("contract_version") != self._authenticated_window_contract_version():
                raise ValueError("non-search decision split contract is invalid")
            if str(split.get("production_split_mode") or "").lower() != CALENDAR_SPLIT_MODE:
                raise ValueError("non-search decision split mode is invalid")
            if str(split.get("activation_id") or "") != str(universe.activation_id):
                raise ValueError("non-search decision split activation id is invalid")
            if str(split.get("activation_manifest_sha256") or "").lower() != str(
                universe.activation_manifest_sha256
            ).lower():
                raise ValueError("non-search decision split activation hash is invalid")
        source = record.get("source")
        if source is not None:
            self._validate_native_source_identity(
                source,
                expected_symbol,
                expected_timeframe,
            )
        claimed_hash = str(record.get("decision_sha256") or "")
        canonical_record = dict(record)
        canonical_record.pop("decision_sha256", None)
        expected_hash = hashlib.sha256(
            json.dumps(
                canonical_record,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if claimed_hash != expected_hash:
            raise ValueError("non-search decision hash is invalid")

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
        if timeframe not in self._evidence_candidate_timeframes(universe, symbol):
            raise ValueError(
                "native cell is not an authenticated evidence candidate: "
                f"{symbol}|{timeframe}"
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
        source_meta = self._validate_native_source_identity(
            dict(source_meta or {}),
            symbol,
            timeframe,
        )

        workflow = trim_to_closed_bars(
            native,
            timeframe,
            as_of_utc=as_of,
            copy=False,
        )
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
    
    def tune_symbol_timeframe(
        self,
        symbol: str,
        timeframe: str,
        param_space: Optional[Dict] = None,
    ) -> Tuple[Optional[RegimeParams], Dict]:
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
                return None, {
                    "non_search": self._build_non_search_decision(
                        symbol,
                        timeframe,
                        decision_kind="TECHNICAL_EXCLUSION",
                        reason_code="NATIVE_OPTIMIZER_TRAIN_INVALID",
                        reason="native optimizer-train partition could not be authenticated",
                        error=exc,
                    )
                }
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

        if len(df) < REGIME_TUNER_MIN_TRAIN_ROWS:
            logger.warning(
                "%s %s: only %d optimizer-train bars; %d are required; "
                "cell is not authorized for strategy search",
                symbol,
                timeframe,
                len(df),
                REGIME_TUNER_MIN_TRAIN_ROWS,
            )
            if self.split_mode == CALENDAR_SPLIT_MODE:
                return None, {
                    "non_search": self._build_non_search_decision(
                        symbol,
                        timeframe,
                        decision_kind="INSUFFICIENT_TRAIN_HISTORY",
                        reason_code="INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
                        reason="optimizer-train partition is below the detector-fit minimum",
                        fit_meta=fit_meta,
                        source_meta=source_meta,
                        diagnostics={
                            "observed_optimizer_train_rows": int(len(df)),
                            "required_optimizer_train_rows": REGIME_TUNER_MIN_TRAIN_ROWS,
                        },
                    )
                }
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}

        # ----- Train / Holdout split (B8: evaluate on holdout, not training data) -----
        holdout_pct = 0.20  # 20% most-recent bars reserved for evaluation
        split_idx = int(len(df) * (1.0 - holdout_pct))
        df_train = df.iloc[:split_idx]
        df_holdout = df.iloc[split_idx:]

        default_detector = MarketRegimeDetector()
        min_eval_bars = max(default_detector.warmup_bars + 25, 100)
        if len(df_train) < max(300, min_eval_bars) or len(df_holdout) < min_eval_bars:
            logger.warning(
                "%s %s: insufficient detector train/holdout rows; "
                "cell is not authorized for strategy search",
                symbol,
                timeframe,
            )
            if self.split_mode == CALENDAR_SPLIT_MODE:
                return None, {
                    "non_search": self._build_non_search_decision(
                        symbol,
                        timeframe,
                        decision_kind="INSUFFICIENT_TRAIN_HISTORY",
                        reason_code="INSUFFICIENT_DETECTOR_HOLDOUT_ROWS",
                        reason="optimizer-train partition cannot support the detector holdout",
                        fit_meta=fit_meta,
                        source_meta=source_meta,
                        diagnostics={
                            "detector_train_rows": int(len(df_train)),
                            "detector_holdout_rows": int(len(df_holdout)),
                            "required_detector_train_rows": int(
                                max(300, min_eval_bars)
                            ),
                            "required_detector_holdout_rows": int(min_eval_bars),
                        },
                    )
                }
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
            if self.split_mode == CALENDAR_SPLIT_MODE:
                return None, {
                    "non_search": self._build_non_search_decision(
                        symbol,
                        timeframe,
                        decision_kind="TECHNICAL_EXCLUSION",
                        reason_code="DETECTOR_FIT_NO_VALID_COMBINATION",
                        reason="all detector parameter combinations failed to produce valid fit evidence",
                        fit_meta=fit_meta,
                        source_meta=source_meta,
                        diagnostics={
                            "tested_parameter_combinations": int(len(combinations)),
                            "detector_train_rows": int(len(df_train)),
                            "detector_holdout_rows": int(len(df_holdout)),
                        },
                    )
                }
            return DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, RegimeParams()), {}

        train_regime_support = self._build_train_regime_support(
            df,
            best_params,
            source_meta,
            fit_meta["train"],
            symbol,
            timeframe,
        )
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
            "train_regime_support": train_regime_support,
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
        """Tune and atomically promote one resolved active-cell generation.

        Availability-excluded cells are deliberately absent. Any active cell
        is resolved either by a successful train-only fit or, in native
        calendar mode, by an activation-bound NON_SEARCH decision.  The latter
        authorizes no strategy search and is never an economic NO_TRADE
        verdict. Global roster/artifact failures remain fail-closed.
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
        params_path = Path(output_path)
        sidecar_path = (
            Path(provenance_output_path)
            if provenance_output_path
            else regime_params_provenance_path(output_path)
        )
        if params_path.resolve() == sidecar_path.resolve():
            raise RegimeTuningIncompleteError(
                "regime params and provenance paths must be distinct"
            )
        params_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        params_stage = params_path.with_name(params_path.name + ".staging")
        sidecar_stage = sidecar_path.with_name(sidecar_path.name + ".staging")
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
            evidence_candidates_by_symbol = {
                symbol: set(self._evidence_candidate_timeframes(universe, symbol))
                for symbol in normalized_symbols
            }
            required_cells = [
                (symbol, timeframe)
                for symbol in normalized_symbols
                for timeframe in normalized_timeframes
                if timeframe in evidence_candidates_by_symbol[symbol]
            ]
            activation_meta = {
                "activation_id": universe.activation_id,
                "activation_manifest_sha256": universe.activation_manifest_sha256,
                "window_contract_version": (
                    self._authenticated_window_contract_version()
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
        non_search_cells: Dict[str, Dict[str, Dict[str, Any]]] = {}
        failures: List[Dict[str, str]] = []

        total = len(required_cells)
        for current, (symbol, tf_key) in enumerate(required_cells, start=1):
            logger.info(f"Progress: {current}/{total} active cells")
            support_key = (symbol, tf_key)
            self._fit_support_authority.pop(support_key, None)
            try:
                best_params, tuning_result = self.tune_symbol_timeframe(symbol, tf_key)
            except Exception as exc:
                logger.exception("%s %s regime fitting raised", symbol, tf_key)
                best_params, tuning_result = None, {}
                if self.split_mode == CALENDAR_SPLIT_MODE:
                    non_search_cells.setdefault(symbol, {})[tf_key] = (
                        self._build_non_search_decision(
                            symbol,
                            tf_key,
                            decision_kind="TECHNICAL_EXCLUSION",
                            reason_code="REGIME_FIT_RAISED",
                            reason="regime detector fitting raised for this cell",
                            error=exc,
                        )
                    )
                else:
                    failures.append({
                        "symbol": symbol,
                        "timeframe": tf_key,
                        "reason": f"fit raised {type(exc).__name__}: {exc}",
                    })
            else:
                try:
                    if not isinstance(tuning_result, dict):
                        raise TypeError("regime fit result is not a mapping")
                    provenance = dict(tuning_result.get("provenance", {}) or {})
                    if provenance and best_params is not None:
                        params_dict = best_params.to_dict()
                        provenance = self._validate_fit_provenance(
                            provenance,
                            params_dict,
                            symbol,
                            tf_key,
                        )
                        all_params.setdefault(symbol, {})[tf_key] = best_params
                        provenance_cells.setdefault(symbol, {})[tf_key] = provenance
                    elif self.split_mode == CALENDAR_SPLIT_MODE:
                        non_search = dict(tuning_result.get("non_search", {}) or {})
                        try:
                            self._validate_non_search_decision(
                                non_search,
                                symbol,
                                tf_key,
                            )
                        except Exception as exc:
                            non_search = self._build_non_search_decision(
                                symbol,
                                tf_key,
                                decision_kind="TECHNICAL_EXCLUSION",
                                reason_code="REGIME_FIT_RESULT_INVALID",
                                reason=(
                                    "regime fitting returned neither valid params/provenance "
                                    "nor a valid non-search decision"
                                ),
                                error=exc,
                            )
                        non_search_cells.setdefault(symbol, {})[tf_key] = non_search
                    else:
                        failures.append({
                            "symbol": symbol,
                            "timeframe": tf_key,
                            "reason": "no successful optimizer_train-only fit",
                        })
                except Exception as exc:
                    logger.exception(
                        "%s %s regime fit result processing raised",
                        symbol,
                        tf_key,
                    )
                    if self.split_mode == CALENDAR_SPLIT_MODE:
                        non_search_cells.setdefault(symbol, {})[tf_key] = (
                            self._build_non_search_decision(
                                symbol,
                                tf_key,
                                decision_kind="TECHNICAL_EXCLUSION",
                                reason_code="REGIME_FIT_RESULT_PROCESSING_FAILED",
                                reason="regime fit result could not be authenticated",
                                error=exc,
                            )
                        )
                    else:
                        failures.append({
                            "symbol": symbol,
                            "timeframe": tf_key,
                            "reason": (
                                "fit result processing raised "
                                f"{type(exc).__name__}: {exc}"
                            ),
                        })

            cell_non_search = non_search_cells.get(symbol, {}).get(tf_key)
            if (
                isinstance(cell_non_search, dict)
                and cell_non_search.get("decision_kind") == "TECHNICAL_EXCLUSION"
            ):
                failures.append({
                    "symbol": symbol,
                    "timeframe": tf_key,
                    "reason": (
                        "retryable technical exclusion: "
                        f"{cell_non_search.get('reason_code', 'UNKNOWN')}"
                    ),
                })
            self._fit_support_authority.pop(support_key, None)
            next_symbol = (
                required_cells[current][0] if current < total else None
            )
            if next_symbol != symbol:
                # One symbol is the bounded memory lifecycle. No frame or
                # resample cache survives into the next symbol.
                self.data_loader.clear_cache()
                self.native_data_loader.clear_cache()
        if self.split_mode == CALENDAR_SPLIT_MODE:
            try:
                identity_current, identity_reason = (
                    self._active_native_identity_still_current()
                )
            except Exception as exc:
                identity_current = False
                identity_reason = (
                    "active native universe revalidation raised "
                    f"{type(exc).__name__}: {exc}"
                )
            if not identity_current:
                failures.append({
                    "symbol": "*",
                    "timeframe": "*",
                    "reason": identity_reason,
                })
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
        try:
            params_stage.write_bytes(params_bytes)
        except (OSError, TypeError, ValueError) as exc:
            raise RegimeTuningIncompleteError(
                f"could not stage regime params; previous pair unchanged: {exc}"
            ) from exc
        params_sha256 = hashlib.sha256(params_bytes).hexdigest()
        required_cell_ids = [
            f"{symbol}|{timeframe}" for symbol, timeframe in required_cells
        ]
        fitted_count = sum(len(tf_map) for tf_map in provenance_cells.values())
        non_search_count = sum(
            len(tf_map) for tf_map in non_search_cells.values()
        )
        resolved_count = fitted_count + non_search_count
        if resolved_count != len(required_cells):
            failures.append({
                "symbol": "*",
                "timeframe": "*",
                "reason": (
                    "generation outcome accounting mismatch: "
                    f"resolved={resolved_count}, required={len(required_cells)}"
                ),
            })
        if self.split_mode == CALENDAR_SPLIT_MODE and fitted_count == 0:
            failures.append({
                "symbol": "*",
                "timeframe": "*",
                "reason": "generation produced zero successful detector fits",
            })
        non_search_cell_ids = [
            f"{symbol}|{timeframe}"
            for symbol, timeframe in required_cells
            if timeframe in non_search_cells.get(symbol, {})
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
            "required_evidence_candidate_cells": required_cell_ids,
            "required_evidence_candidate_cell_count": len(required_cell_ids),
            "fitted_active_cell_count": fitted_count,
            "non_search_active_cells": non_search_cell_ids,
            "non_search_active_cell_count": non_search_count,
            "resolved_active_cell_count": resolved_count,
            **activation_meta,
            "split_contract_version": (
                activation_meta.get("window_contract_version", EVIDENCE_CONTRACT_VERSION)
                if self.split_mode == CALENDAR_SPLIT_MODE
                else DataSplitter.SPLIT_CONTRACT_VERSION
            ),
            "cells": provenance_cells,
            "non_search_cells": non_search_cells,
            "failures": failures,
        }
        try:
            sidecar_stage.write_text(
                json.dumps(sidecar_payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except (OSError, TypeError, ValueError) as exc:
            raise RegimeTuningIncompleteError(
                f"could not stage regime provenance; previous pair unchanged: {exc}"
            ) from exc
        logger.info(
            "Staged regime outcomes at %s (%d fit, %d non-search, %d failed)",
            sidecar_stage,
            fitted_count,
            non_search_count,
            len(failures),
        )

        if failures:
            raise RegimeTuningIncompleteError(
                f"regime generation incomplete: {len(failures)}/{total} active cells failed; "
                f"last promoted artifacts were not changed; diagnostics: {sidecar_stage}"
            )

        self._promote_artifact_pair(
            params_stage,
            params_path,
            sidecar_stage,
            sidecar_path,
        )
        logger.info(
            "Promoted resolved regime generation: %d fit + %d non-search -> %s",
            fitted_count,
            non_search_count,
            params_path,
        )
        return all_params


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Tune FXPM regime detectors from authenticated broker-native "
            "timeframe cells"
        )
    )
    parser.add_argument(
        '--config', type=str, default='config.json',
        help='FXPM config containing the production calendar/native split policy'
    )
    parser.add_argument(
        '--data-dir', type=str, default=None,
        help=(
            'FXPM data root containing native/ACTIVE.json; flat M5 CSV files '
            'are legacy chronological-mode input only'
        )
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
        help='Complete activated symbol roster (default: config symbols)'
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
        pipeline_config = PipelineConfig(
            **strict_pipeline_config_kwargs(dict(pipeline_payload or {}))
        )
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
        logger.error(
            "No symbols found. Configure the production roster; calendar mode "
            "also requires data/native/ACTIVE.json."
        )
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
