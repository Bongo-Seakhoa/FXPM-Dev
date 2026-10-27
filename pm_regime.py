"""
FX Portfolio Manager - Regime Detection Module
===============================================

Market regime detection with:
- 4 regimes: TREND, RANGE, BREAKOUT, CHOP
- Per-regime strength scores (0-1)
- Gap calculation (top - second) for crossover awareness
- Symmetric accumulated-evidence confirmation and carried stream state
- Causal computation (no lookahead)
- REGIME_LIVE shift for backtest/live parity
- Numba JIT compilation for performance-critical loops

The pipeline and FeatureComputer share this canonical detector and its
symbol/timeframe-specific parameters.

Version: causal classifier v2, R4 reconstruction from the accepted review narrative
"""

import hashlib
import json
import logging
import math
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

import numpy as np
import pandas as pd
from pm_evidence import (
    CALENDAR_SPLIT_MODE,
    EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
)

logger = logging.getLogger(__name__)

# =============================================================================
# NUMBA JIT COMPILATION (optional - graceful fallback if not available)
# =============================================================================

try:
    from numba import jit, prange
    from numba.extending import register_jitable
    NUMBA_AVAILABLE = True
    logger.debug("Numba JIT compilation available - using optimized loops")
except Exception:
    NUMBA_AVAILABLE = False
    logger.debug("Numba not available - using pure Python loops")
    
    # Create no-op decorator for graceful fallback
    def jit(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    def register_jitable(func):
        return func
    prange = range


# =============================================================================
# REGIME TYPES
# =============================================================================

class RegimeType:
    """Regime type constants."""
    TREND = "TREND"
    RANGE = "RANGE"
    BREAKOUT = "BREAKOUT"
    CHOP = "CHOP"
    
    ALL = [TREND, RANGE, BREAKOUT, CHOP]
    
    # Integer encoding for Numba (must match indices in ALL)
    _TO_INT = {"TREND": 0, "RANGE": 1, "BREAKOUT": 2, "CHOP": 3}
    _FROM_INT = {0: "TREND", 1: "RANGE", 2: "BREAKOUT", 3: "CHOP"}
    
    @classmethod
    def to_int(cls, regime: str) -> int:
        """Convert regime string to integer for Numba."""
        return cls._TO_INT.get(regime, 3)  # Default to CHOP
    
    @classmethod
    def from_int(cls, idx: int) -> str:
        """Convert integer back to regime string."""
        return cls._FROM_INT.get(idx, cls.CHOP)


# Regime labels are drawn from a closed set of four, so storing them as pandas
# object columns keeps 250k interned Python strings alive per frame per column.
# Measured on a 250k-bar M5 frame: REGIME_RAW/REGIME/REGIME_LIVE cost 44.0 MB as
# object and 6.4 MB as category - 37.6 MB, 22% of the whole feature frame, and
# regime equality checks become int8 comparisons instead of string compares.
#
# The category list is the complete RegimeType.ALL, so assigning any legitimate
# regime value into these columns stays valid. Values outside the set become NaN
# rather than silently widening the dtype - which is the correct failure for a
# label that is supposed to be one of four things.
REGIME_DTYPE = pd.CategoricalDtype(categories=RegimeType.ALL, ordered=False)


def as_regime_category(values: Any) -> pd.Categorical:
    """Build a regime column in the closed-set categorical dtype."""
    return pd.Categorical(values, dtype=REGIME_DTYPE)


# =============================================================================
# REGIME PARAMETERS
# =============================================================================

@dataclass
class RegimeParams:
    """
    Parameters for regime detection.
    
    Can be customized per symbol/timeframe via regime_params.json.
    """
    # Hysteresis parameters
    k_confirm: int = 3          # Evidence-budget multiplier
    gap_min: float = 0.10       # Evidence budget per multiplier
    k_hold: int = 5             # Minimum bars to hold a regime
    
    # Lookback windows
    adx_period: int = 14        # ADX calculation period
    atr_period: int = 14        # ATR calculation period
    bb_period: int = 20         # Bollinger Band period
    bb_std: float = 2.0         # Bollinger Band std deviation
    efficiency_period: int = 10 # Directional efficiency lookback
    
    # Adaptive thresholds
    bb_squeeze_lookback: int = 200  # Lookback for BB width percentile
    bb_squeeze_percentile: float = 20.0  # Percentile for "squeeze" detection (0-100)
    atr_lookback: int = 100     # Lookback for ATR percentile
    structure_period: int = 20  # Period for structure break detection
    structure_atr_mult: float = 0.1  # ATR multiplier for structure break margin
    
    # Score weights (for each regime)
    trend_adx_weight: float = 0.40
    trend_efficiency_weight: float = 0.35
    trend_slope_weight: float = 0.25
    
    range_adx_weight: float = 0.35
    range_efficiency_weight: float = 0.30
    range_containment_weight: float = 0.35
    
    breakout_squeeze_weight: float = 0.35
    breakout_atr_weight: float = 0.30
    breakout_structure_weight: float = 0.35
    
    chop_adx_weight: float = 0.30
    chop_efficiency_weight: float = 0.30
    chop_whipsaw_weight: float = 0.40

    # Asset-class defaults can override this unless regime_params.json pins it.
    adx_trend_threshold: float = 25.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RegimeParams':
        """Create from dictionary, ignoring unknown keys."""
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


# Default parameters by timeframe.
DEFAULT_PARAMS_BY_TIMEFRAME: Dict[str, RegimeParams] = {
    'M5':  RegimeParams(k_confirm=3, gap_min=0.10, k_hold=5, bb_squeeze_lookback=50),
    'M15': RegimeParams(k_confirm=3, gap_min=0.10, k_hold=4, bb_squeeze_lookback=80),
    'M30': RegimeParams(k_confirm=2, gap_min=0.10, k_hold=4),
    'H1':  RegimeParams(k_confirm=2, gap_min=0.10, k_hold=3, bb_squeeze_lookback=50),
    'H4':  RegimeParams(k_confirm=2, gap_min=0.08, k_hold=2, bb_squeeze_lookback=50),
    # D1 uses slightly stronger hysteresis to avoid one-bar regime flips.
    'D1':  RegimeParams(k_confirm=1, gap_min=0.08, k_hold=3, bb_squeeze_lookback=60),
}


# Asset-class defaults for the ADX trend anchor. JSON-pinned values win.
DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS: Dict[str, float] = {
    'fx':     25.0,
    'metal':  22.0,
    'crypto': 30.0,
    'index':  25.0,
}


def _infer_asset_class(symbol: str) -> str:
    """Infer asset class from symbol name (pure string-match; no I/O).

    Returns one of ``{'fx', 'metal', 'crypto', 'index'}``. Unknown symbols
    default to ``'fx'`` so the classic `25.0` threshold still applies — safest
    fallback for a broad-discovery engine that shouldn't silently shift a
    symbol's trend anchor on a name collision.
    """
    up = (symbol or "").upper().strip()
    if not up:
        return 'fx'
    # Metals — XAU/XAG/XPT/XPD plus legacy GOLD/SILVER spellings.
    if up.startswith(('XAU', 'XAG', 'XPT', 'XPD')) or up in {'GOLD', 'SILVER'}:
        return 'metal'
    # Crypto — prefix match covers majors (BTC, ETH, LTC, XRP, SOL, ADA, DOT,
    # DOGE, BCH) plus any pair that crosses two crypto tickers (BTCETH, BTCXAU
    # is treated as crypto because its volatility profile is crypto-driven).
    crypto_prefixes = ('BTC', 'ETH', 'LTC', 'XRP', 'SOL', 'ADA', 'DOT', 'DOGE', 'DOG', 'BCH', 'TRX', 'TON')
    if up.startswith(crypto_prefixes):
        return 'crypto'
    # Indices — common ticker shorthand used by MT5 brokers.
    index_tickers = {
        'SP500', 'SPX500', 'US500', 'US30', 'US100', 'NAS100', 'USTEC',
        'DAX', 'DE30', 'DE40', 'GER30', 'GER40', 'FTSE', 'UK100',
        'NIKKEI', 'JP225', 'AUS200', 'AU200', 'HK50', 'CHINA50',
        'EU50', 'ES35', 'FR40',
    }
    if up in index_tickers or 'INDEX' in up:
        return 'index'
    return 'fx'


def _resolve_adx_trend_threshold(symbol: str) -> float:
    """Asset-class-derived default for `RegimeParams.adx_trend_threshold`."""
    return DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS.get(
        _infer_asset_class(symbol),
        DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS['fx'],
    )


# Global cache for loaded regime params
_REGIME_PARAMS_CACHE: Dict[str, Dict[str, RegimeParams]] = {}
_REGIME_PARAMS_LOADED: bool = False
_REGIME_PARAMS_LOADED_PATH: Optional[str] = None
# Track fallback use so the startup summary can surface tuning gaps once.
_REGIME_FALLBACK_LOGGED: Set[Tuple[str, str]] = set()
_REGIME_FALLBACK_RECORD: Dict[Tuple[str, str], str] = {}
# Pairs whose JSON explicitly pinned adx_trend_threshold.
_REGIME_ADX_THRESHOLD_EXPLICIT: Set[Tuple[str, str]] = set()


# A regime detector is part of the model-selection surface: changing its
# parameters changes which trades belong to every regime bucket.  An explicit
# JSON row is therefore not evidence that the row was fitted without seeing
# tuning-validation or final-lockbox observations.  The companion provenance
# artifact below makes that boundary machine-verifiable.
REGIME_PROVENANCE_SCHEMA_VERSION = "fxpm.regime-detector-fit-provenance/v1"
REGIME_PROVENANCE_FIT_SCOPE = "optimizer_train"
_REGIME_SOURCE_DIGEST_CACHE: Dict[Tuple[str, int, int], str] = {}


def regime_params_provenance_path(filepath: str = "regime_params.json") -> Path:
    """Return the deterministic sidecar path for a regime-parameter artifact."""
    params_path = Path(str(filepath or "regime_params.json"))
    return params_path.with_name(f"{params_path.stem}.provenance.json")


def regime_params_sha256(params: Dict[str, Any]) -> str:
    """Hash one symbol/timeframe parameter row using canonical JSON."""
    encoded = json.dumps(
        params,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _case_insensitive_nested_cell(payload: Any, symbol: str, timeframe: str) -> Any:
    """Read ``payload[symbol][timeframe]`` without depending on broker-key case."""
    if not isinstance(payload, dict):
        return None
    symbol_key = next(
        (
            key for key in payload
            if str(key or "").strip().upper() == str(symbol or "").strip().upper()
        ),
        None,
    )
    if symbol_key is None or not isinstance(payload.get(symbol_key), dict):
        return None
    tf_map = payload[symbol_key]
    timeframe_key = next(
        (
            key for key in tf_map
            if str(key or "").strip().upper() == str(timeframe or "").strip().upper()
        ),
        None,
    )
    return tf_map.get(timeframe_key) if timeframe_key is not None else None


def _normalized_timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return pd.Timestamp(value).isoformat()
    except Exception:
        return None


def _source_sha256(path: Path) -> str:
    stat = path.stat()
    key = (str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns))
    cached = _REGIME_SOURCE_DIGEST_CACHE.get(key)
    if cached:
        return cached
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    value = digest.hexdigest()
    _REGIME_SOURCE_DIGEST_CACHE[key] = value
    return value


# Mirrors the tuner writer's non-search schema (pm_regime_tuner); the
# cross-suite regression pins the two constants together.
REGIME_NON_SEARCH_SCHEMA_VERSION = "fxpm.regime-detector-non-search/v1"
REGIME_TRAIN_SUPPORT_SCHEMA_VERSION = "fxpm.regime-train-support/v1"
_REGIME_LABEL_ORDER = ("TREND", "RANGE", "BREAKOUT", "CHOP")


def _tuner_canonical_sha256(value: Dict[str, Any]) -> str:
    # Must byte-match pm_regime_tuner._canonical_mapping_sha256.
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def recompute_train_regime_support_digest(
    frame: "pd.DataFrame", params_cell: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Recompute counts/vector from the exact train REGIME_LIVE surface.

    Byte-mirrors the tuner writer; the consumer never trusts a returned
    receipt without this independent recompute (seq1024).
    """
    try:
        detector = MarketRegimeDetector(RegimeParams.from_dict(dict(params_cell)))
        surface_frame = detector.compute_regime_scores(frame)
        if not surface_frame.index.equals(frame.index):
            return None
        if not {"REGIME_LIVE", "REGIME_WARMUP"}.issubset(surface_frame.columns):
            return None
        warmup = surface_frame["REGIME_WARMUP"]
        if warmup.isna().any() or not warmup.isin([True, False]).all():
            return None
        warmup = warmup.astype(bool)
        eligible = (~warmup) & (~warmup.shift(1, fill_value=True))
        surface = surface_frame.loc[eligible, "REGIME_LIVE"]
        if surface.isna().any():
            return None
        counts = {label: 0 for label in _REGIME_LABEL_ORDER}
        digest = hashlib.sha256()
        digest.update(b"fxpm.regime-live-labels-nul/v1\0")
        for value in surface.array:
            if not isinstance(value, str) or value not in counts:
                return None
            counts[value] += 1
            digest.update(value.encode("ascii"))
            digest.update(b"\0")
        return {
            "counts": counts,
            "eligible_rows": int(len(surface)),
            "vector_sha256": digest.hexdigest(),
            "first_ts": (
                None if surface.empty else pd.Timestamp(surface.index[0]).isoformat()
            ),
            "last_ts": (
                None if surface.empty else pd.Timestamp(surface.index[-1]).isoformat()
            ),
        }
    except Exception:
        return None


_DETECTOR_CODE_SHA256_CACHE: Optional[str] = None


def detector_code_sha256() -> Optional[str]:
    """Hash of this module's source file; mirrors the tuner binding."""
    global _DETECTOR_CODE_SHA256_CACHE
    if _DETECTOR_CODE_SHA256_CACHE is None:
        try:
            import inspect

            source_file = inspect.getsourcefile(MarketRegimeDetector)
            if not source_file:
                return None
            _DETECTOR_CODE_SHA256_CACHE = _source_sha256(
                Path(source_file).resolve()
            )
        except Exception:
            return None
    return _DETECTOR_CODE_SHA256_CACHE


def _validated_train_regime_support(
    support: Any,
    symbol: str,
    timeframe: str,
    params_cell: Optional[Dict[str, Any]],
    expected_activation_id: Optional[str],
    expected_activation_manifest_sha256: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Return validated support receipt or None (caller rejects).

    seq1000/1005/1006: consumer validates the exact train-only support
    receipt; leaf inference is never a substitute.
    """
    if not isinstance(support, dict):
        return None
    if support.get("schema_version") != REGIME_TRAIN_SUPPORT_SCHEMA_VERSION:
        return None
    if str(support.get("symbol", "")).upper() != str(symbol).upper():
        return None
    if str(support.get("timeframe", "")).upper() != str(timeframe).upper():
        return None
    if support.get("surface") != "REGIME_LIVE":
        return None
    if list(support.get("label_order") or []) != list(_REGIME_LABEL_ORDER):
        return None
    counts = support.get("counts")
    if not isinstance(counts, dict) or set(counts) != set(_REGIME_LABEL_ORDER):
        return None
    for value in counts.values():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
    eligible = support.get("eligible_rows")
    if isinstance(eligible, bool) or not isinstance(eligible, int) or eligible < 0:
        return None
    if sum(counts.values()) != eligible:
        return None
    observed = [lab for lab in _REGIME_LABEL_ORDER if counts[lab] > 0]
    unobserved = [lab for lab in _REGIME_LABEL_ORDER if counts[lab] == 0]
    if list(support.get("observed_regimes") or []) != observed:
        return None
    if list(support.get("unobserved_regimes") or []) != unobserved:
        return None
    state = support.get("support_state_by_regime")
    if not isinstance(state, dict) or set(state) != set(_REGIME_LABEL_ORDER):
        return None
    for label in _REGIME_LABEL_ORDER:
        want = "EMPIRICALLY_OBSERVED" if counts[label] > 0 else "UNOBSERVED"
        if state.get(label) != want:
            return None
    if support.get("warmup_policy") != "CURRENT_AND_SOURCE_BAR_NOT_WARMUP":
        return None
    if support.get("vector_encoding") != "fxpm.regime-live-labels-nul/v1":
        return None
    if eligible == 0:
        if support.get("first_ts") is not None or support.get("last_ts") is not None:
            return None
    else:
        for ts_key in ("first_ts", "last_ts"):
            if not isinstance(support.get(ts_key), str) or not support[ts_key]:
                return None
    vector_sha = str(support.get("vector_sha256") or "").lower()
    if len(vector_sha) != 64 or any(c not in "0123456789abcdef" for c in vector_sha):
        return None
    bindings = support.get("bindings")
    if not isinstance(bindings, dict):
        return None
    if set(bindings) != {
        "symbol",
        "timeframe",
        "activation_id",
        "activation_manifest_sha256",
        "source_sha256",
        "train_window_sha256",
        "params_sha256",
        "detector_code_sha256",
    }:
        return None
    for sha_key in (
        "source_sha256",
        "train_window_sha256",
        "params_sha256",
        "detector_code_sha256",
    ):
        value = str(bindings.get(sha_key) or "").lower()
        if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            return None
    if str(bindings.get("symbol", "")).upper() != str(symbol).upper():
        return None
    if str(bindings.get("timeframe", "")).upper() != str(timeframe).upper():
        return None
    if expected_activation_id is not None and str(
        bindings.get("activation_id") or ""
    ) != str(expected_activation_id):
        return None
    if expected_activation_manifest_sha256 is not None and str(
        bindings.get("activation_manifest_sha256") or ""
    ).lower() != str(expected_activation_manifest_sha256).lower():
        return None
    if params_cell is not None and bindings.get("params_sha256") != (
        regime_params_sha256(params_cell)
    ):
        return None
    declared = str(support.get("receipt_sha256") or "").lower()
    body = {k: v for k, v in support.items() if k != "receipt_sha256"}
    if declared != _tuner_canonical_sha256(body):
        return None
    return support


def _validated_non_search_record(
    record: Any,
    symbol: str,
    timeframe: str,
    expected_activation_id: Optional[str],
    expected_activation_manifest_sha256: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Return the record iff it is a bound, verdict-free NON_SEARCH decision."""
    if not isinstance(record, dict):
        return None
    if record.get("schema_version") != REGIME_NON_SEARCH_SCHEMA_VERSION:
        return None
    if str(record.get("status", "")).upper() != "NON_SEARCH":
        return None
    if str(record.get("symbol", "")).upper() != str(symbol).upper():
        return None
    if str(record.get("timeframe", "")).upper() != str(timeframe).upper():
        return None
    # A non-search decision must never smuggle an economic verdict.
    if record.get("no_trade") is not False:
        return None
    if record.get("trade_verdict") is not False:
        return None
    if record.get("strategy_search_authorized") is not False:
        return None
    # seq1012/seq1028: kind-bound vocabulary + terminal/retryable coherence.
    kind = str(record.get("decision_kind", "")).strip().upper()
    reason_code = str(record.get("reason_code", "")).strip().upper()
    kind_reasons = {
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
    if kind not in kind_reasons or reason_code not in kind_reasons[kind]:
        return None
    terminal = kind == "INSUFFICIENT_TRAIN_HISTORY"
    if record.get("generation_terminal") is not terminal:
        return None
    if record.get("retryable") is not (not terminal):
        return None
    if str(record.get("fit_scope", "")).strip().lower() != "optimizer_train":
        return None
    if not str(record.get("window_contract_version", "") or "").strip():
        return None
    if expected_activation_id is not None and str(
        record.get("activation_id") or ""
    ) != str(expected_activation_id):
        return None
    if expected_activation_manifest_sha256 is not None and str(
        record.get("activation_manifest_sha256") or ""
    ).lower() != str(expected_activation_manifest_sha256).lower():
        return None
    declared_sha = str(record.get("decision_sha256") or "").strip().lower()
    body = {key: value for key, value in record.items() if key != "decision_sha256"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    if declared_sha != hashlib.sha256(canonical.encode("utf-8")).hexdigest():
        return None
    return record


def validate_regime_params_provenance(
    symbol: str,
    timeframe: str,
    filepath: str = "regime_params.json",
    *,
    provenance_filepath: Optional[str] = None,
    expected_source: Optional[Dict[str, Any]] = None,
    expected_train_window: Optional[Dict[str, Any]] = None,
    expected_split_contract: Optional[str] = None,
    expected_activation_id: Optional[str] = None,
    expected_activation_manifest_sha256: Optional[str] = None,
    require_complete_generation: bool = False,
) -> Dict[str, Any]:
    """Fail-closed validation of one detector-fit provenance record.

    ``expected_source`` and ``expected_train_window`` are supplied by the
    chronological pipeline.  With them, validation proves that the detector
    was fitted on the exact same M5 optimizer-train partition the strategy
    optimizer is about to use.  Without them (the lightweight live-readiness
    check), the function still verifies the fit scope, split geometry,
    parameter hash, and current source-file hash.
    """
    params_path = Path(str(filepath or "regime_params.json"))
    sidecar_path = (
        Path(provenance_filepath)
        if provenance_filepath
        else regime_params_provenance_path(str(params_path))
    )

    def reject(reason: str, record: Any = None) -> Dict[str, Any]:
        return {
            "valid": False,
            "reason": reason,
            "symbol": str(symbol),
            "timeframe": str(timeframe).upper(),
            "params_path": str(params_path),
            "provenance_path": str(sidecar_path),
            "record": record if isinstance(record, dict) else {},
        }

    try:
        params_payload = json.loads(params_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return reject("regime params file is missing")
    except Exception as exc:
        return reject(f"regime params file is unreadable: {exc}")
    # NOTE: a NON_SEARCH cell legitimately has no params entry; the
    # params-cell requirement is enforced on the FIT path below.
    params_cell = _case_insensitive_nested_cell(params_payload, symbol, timeframe)

    try:
        provenance_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return reject("regime provenance sidecar is missing")
    except Exception as exc:
        return reject(f"regime provenance sidecar is unreadable: {exc}")
    if not isinstance(provenance_payload, dict):
        return reject("regime provenance sidecar is not an object")
    if provenance_payload.get("schema_version") != REGIME_PROVENANCE_SCHEMA_VERSION:
        return reject("regime provenance schema mismatch")
    if require_complete_generation and str(
        provenance_payload.get("generation_status") or ""
    ).upper() != "COMPLETE":
        return reject("regime provenance generation is not complete")
    declared_params_sha = str(
        provenance_payload.get("params_file_sha256") or ""
    ).strip().lower()
    if declared_params_sha:
        if len(declared_params_sha) != 64:
            return reject("regime params artifact hash is invalid")
        try:
            current_params_sha = _source_sha256(params_path).lower()
        except OSError as exc:
            return reject(f"regime params artifact cannot be hashed: {exc}")
        if current_params_sha != declared_params_sha:
            return reject("regime params artifact hash mismatch")
    elif require_complete_generation:
        return reject("regime params artifact hash is missing")
    if expected_activation_id is not None and str(
        provenance_payload.get("activation_id") or ""
    ) != str(expected_activation_id):
        return reject("regime provenance activation id mismatch")
    if expected_activation_manifest_sha256 is not None and str(
        provenance_payload.get("activation_manifest_sha256") or ""
    ).lower() != str(expected_activation_manifest_sha256).lower():
        return reject("regime provenance activation manifest mismatch")

    # seq936-specific fields only: legacy v3 sidecars carry
    # fitted_active_cell_count/required_active_cell_count without the
    # non-search vocabulary and must not trip the new identity.
    _SEQ936_COUNT_FIELDS = (
        "non_search_active_cell_count",
        "resolved_active_cell_count",
        "required_evidence_candidate_cell_count",
    )
    declares_counts = any(
        count_key in provenance_payload for count_key in _SEQ936_COUNT_FIELDS
    ) or bool(provenance_payload.get("non_search_cells"))
    if require_complete_generation and declares_counts:
        # seq936 counts identity: fitted + non_search = resolved = required.
        # Legacy FIT-only sidecars predate the count contract and stay valid
        # (each FIT cell is still individually validated below); any payload
        # that carries counts or non_search_cells must satisfy the identity.
        try:
            fitted = int(provenance_payload.get("fitted_active_cell_count"))
            non_search = int(provenance_payload.get("non_search_active_cell_count"))
            resolved = int(provenance_payload.get("resolved_active_cell_count"))
            required = int(
                provenance_payload.get("required_evidence_candidate_cell_count")
            )
        except (TypeError, ValueError):
            return reject("regime provenance cell counts are missing or invalid")
        if fitted + non_search != resolved or resolved != required:
            return reject(
                "regime provenance cell counts identity broken: "
                f"{fitted}+{non_search}!={resolved} or {resolved}!={required}"
            )

    record = _case_insensitive_nested_cell(
        provenance_payload.get("cells", {}), symbol, timeframe
    )
    if not isinstance(record, dict):
        non_search_record = _validated_non_search_record(
            _case_insensitive_nested_cell(
                provenance_payload.get("non_search_cells", {}), symbol, timeframe
            ),
            symbol,
            timeframe,
            expected_activation_id,
            expected_activation_manifest_sha256,
        )
        if non_search_record is not None:
            # Typed non-search authorization decision: the cell is VALIDLY
            # excluded from strategy search.  Never an economic verdict and
            # never a gate failure; the pipeline must skip search for this
            # cell without touching the winner ledger.
            return {
                "valid": True,
                "status": "NON_SEARCH",
                "reason": str(non_search_record.get("reason_code")),
                "decision_kind": str(non_search_record.get("decision_kind")),
                "symbol": str(symbol),
                "timeframe": str(timeframe).upper(),
                "params_path": str(params_path),
                "provenance_path": str(sidecar_path),
                "record": dict(non_search_record),
            }
        return reject("regime provenance cell is missing")
    if not isinstance(params_cell, dict):
        return reject("regime params cell is missing")
    if record.get("schema_version") != REGIME_PROVENANCE_SCHEMA_VERSION:
        return reject("regime provenance cell schema mismatch", record)
    if str(record.get("symbol", "")).upper() != str(symbol).upper():
        return reject("regime provenance symbol mismatch", record)
    if str(record.get("timeframe", "")).upper() != str(timeframe).upper():
        return reject("regime provenance timeframe mismatch", record)
    if str(record.get("fit_scope", "")).strip().lower() != REGIME_PROVENANCE_FIT_SCOPE:
        return reject("regime detector was not fitted on optimizer_train", record)
    if str(record.get("status", "")).strip().upper() != "FIT":
        return reject("regime detector provenance is not a successful fit", record)
    if record.get("params_sha256") != regime_params_sha256(params_cell):
        return reject("regime parameter hash mismatch", record)

    source = record.get("source")
    if not isinstance(source, dict):
        return reject("regime provenance source metadata is missing", record)
    source_path_raw = str(source.get("path", "") or "").strip()
    source_sha = str(source.get("sha256", "") or "").strip().lower()
    try:
        source_size = int(source.get("size"))
    except (TypeError, ValueError):
        source_size = -1
    if not source_path_raw or source_size < 0 or len(source_sha) != 64:
        return reject("regime provenance source fingerprint is incomplete", record)
    source_path = Path(source_path_raw)
    try:
        current_size = int(source_path.stat().st_size)
    except OSError as exc:
        return reject(f"regime provenance source is unavailable: {exc}", record)
    if current_size != source_size:
        return reject("regime provenance source size mismatch", record)

    if expected_source is not None:
        expected_path_raw = str(expected_source.get("path", "") or "").strip()
        expected_sha = str(expected_source.get("sha256", "") or "").strip().lower()
        try:
            expected_size = int(expected_source.get("size"))
        except (TypeError, ValueError):
            expected_size = -1
        if not expected_path_raw or expected_size < 0 or len(expected_sha) != 64:
            return reject("current pipeline source fingerprint is incomplete", record)
        try:
            paths_match = source_path.resolve() == Path(expected_path_raw).resolve()
        except OSError:
            paths_match = source_path.absolute() == Path(expected_path_raw).absolute()
        if not paths_match or expected_size != source_size or expected_sha != source_sha:
            return reject("regime provenance does not match current pipeline source", record)
    else:
        try:
            current_sha = _source_sha256(source_path).lower()
        except OSError as exc:
            return reject(f"regime provenance source cannot be hashed: {exc}", record)
        if current_sha != source_sha:
            return reject("regime provenance source SHA256 mismatch", record)

    train = record.get("train")
    split = record.get("split")
    if not isinstance(train, dict) or not isinstance(split, dict):
        return reject("regime provenance train/split metadata is missing", record)
    try:
        start_index = int(train.get("start_index"))
        end_index = int(train.get("end_index"))
        train_rows = int(train.get("rows"))
    except (TypeError, ValueError):
        return reject("regime provenance train indices are invalid", record)
    if start_index < 0 or end_index <= start_index or train_rows != end_index - start_index:
        return reject("regime provenance train geometry is invalid", record)
    if not _normalized_timestamp(train.get("start_ts")):
        return reject("regime provenance train start timestamp is missing", record)
    if not _normalized_timestamp(train.get("end_ts_inclusive")):
        return reject("regime provenance train inclusive end timestamp is missing", record)
    if not _normalized_timestamp(train.get("end_ts_exclusive")):
        return reject("regime provenance train exclusive end timestamp is missing", record)

    try:
        split_train = [int(value) for value in split.get("optimizer_train_indices", [])]
        split_validation = [int(value) for value in split.get("tuning_validation_indices", [])]
        split_lockbox = [int(value) for value in split.get("final_lockbox_indices", [])]
    except (TypeError, ValueError):
        return reject("regime provenance split indices are invalid", record)
    if split_train != [start_index, end_index]:
        return reject("regime provenance train indices disagree with split metadata", record)
    if len(split_validation) != 2 or len(split_lockbox) != 2:
        return reject("regime provenance validation/lockbox indices are missing", record)
    if not (
        end_index <= split_validation[0]
        < split_validation[1]
        <= split_lockbox[0]
        < split_lockbox[1]
    ):
        return reject("regime provenance leaks beyond optimizer_train", record)
    split_mode = str(split.get("production_split_mode", "")).lower()
    if split_mode not in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
        return reject("regime provenance was not produced by a lockbox split", record)
    if split_mode == CALENDAR_SPLIT_MODE:
        if str(split.get("source_authority", "")).lower() != "broker_native_timeframe":
            return reject(
                "calendar regime provenance is not native-timeframe authoritative",
                record,
            )
        if str(source.get("source_kind", "")).lower() != "broker_native":
            return reject("calendar regime provenance source is not broker-native", record)
        if split.get("contract_version") not in {
            EVIDENCE_CONTRACT_VERSION,
            TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        }:
            return reject("calendar regime provenance contract is stale", record)
    top_contract = provenance_payload.get("split_contract_version")
    if top_contract and top_contract != split.get("contract_version"):
        return reject("regime provenance top-level split contract mismatch", record)
    if expected_split_contract and split.get("contract_version") != expected_split_contract:
        return reject("regime provenance split-contract mismatch", record)

    if expected_train_window is not None:
        expected_values = {
            "start_index": expected_train_window.get("start_index"),
            "end_index": expected_train_window.get("end_index"),
            "rows": expected_train_window.get("rows"),
        }
        try:
            expected_values = {key: int(value) for key, value in expected_values.items()}
        except (TypeError, ValueError):
            return reject("current optimizer-train indices are incomplete", record)
        if expected_values != {
            "start_index": start_index,
            "end_index": end_index,
            "rows": train_rows,
        }:
            return reject("regime provenance optimizer-train window mismatch", record)
        for key in ("start_ts", "end_ts_inclusive", "end_ts_exclusive"):
            expected_ts = _normalized_timestamp(expected_train_window.get(key))
            record_ts = _normalized_timestamp(train.get(key))
            if expected_ts is None or record_ts != expected_ts:
                return reject(f"regime provenance optimizer-train {key} mismatch", record)

    # seq1005: consume the train-only support receipt; leaf inference is
    # never a substitute.  Required whenever the sidecar speaks the seq936
    # vocabulary; validated whenever present.
    raw_support = record.get("train_regime_support")
    validated_support: Optional[Dict[str, Any]] = None
    if raw_support is not None or declares_counts:
        validated_support = _validated_train_regime_support(
            raw_support,
            symbol,
            timeframe,
            params_cell,
            expected_activation_id,
            expected_activation_manifest_sha256,
        )
        if validated_support is None:
            return reject("regime train support receipt is missing or invalid", record)

    return {
        "valid": True,
        "status": "FIT",
        "reason": "ok",
        "symbol": str(symbol),
        "timeframe": str(timeframe).upper(),
        "params_path": str(params_path),
        "provenance_path": str(sidecar_path),
        "record": record,
        "train_regime_support": validated_support,
    }


def _ensure_regime_params_loaded(filepath: str = "regime_params.json") -> None:
    """Load tuned regime params once for the requested path without recording fallbacks."""
    global _REGIME_PARAMS_CACHE, _REGIME_PARAMS_LOADED, _REGIME_PARAMS_LOADED_PATH

    cache_path = str(filepath or "")
    if _REGIME_PARAMS_LOADED and _REGIME_PARAMS_LOADED_PATH == cache_path:
        return

    _REGIME_PARAMS_CACHE = {}
    _REGIME_ADX_THRESHOLD_EXPLICIT.clear()
    try:
        with open(cache_path, 'r') as f:
            raw_data = json.load(f)

        for sym, tf_dict in raw_data.items():
            _REGIME_PARAMS_CACHE[sym] = {}
            if not isinstance(tf_dict, dict):
                continue
            for tf, params_dict in tf_dict.items():
                _REGIME_PARAMS_CACHE[sym][tf] = RegimeParams.from_dict(params_dict)
                # Preserve JSON-pinned ADX anchors for this pair.
                if isinstance(params_dict, dict) and 'adx_trend_threshold' in params_dict:
                    _REGIME_ADX_THRESHOLD_EXPLICIT.add((sym, tf))

        logger.info(f"Loaded regime params for {len(_REGIME_PARAMS_CACHE)} symbols from {cache_path}")
    except FileNotFoundError:
        logger.debug(f"No regime_params.json found at {cache_path}, using defaults")
    except Exception as e:
        logger.warning(f"Failed to load regime params from {cache_path}: {e}")

    _REGIME_PARAMS_LOADED = True
    _REGIME_PARAMS_LOADED_PATH = cache_path


def _find_tuned_regime_params(symbol: str, timeframe: str) -> Optional[Tuple[Tuple[str, str], RegimeParams]]:
    """Return the tuned JSON entry for symbol/timeframe without falling back."""
    symbol_key = str(symbol or "").strip()
    timeframe_key = str(timeframe or "").strip()
    if not symbol_key or not timeframe_key:
        return None

    if symbol_key in _REGIME_PARAMS_CACHE and timeframe_key in _REGIME_PARAMS_CACHE[symbol_key]:
        return (symbol_key, timeframe_key), _REGIME_PARAMS_CACHE[symbol_key][timeframe_key]

    symbol_upper = symbol_key.upper()
    timeframe_upper = timeframe_key.upper()
    for cached_symbol, tf_map in _REGIME_PARAMS_CACHE.items():
        if str(cached_symbol or "").upper() != symbol_upper or not isinstance(tf_map, dict):
            continue
        for cached_tf, params in tf_map.items():
            if str(cached_tf or "").upper() == timeframe_upper:
                return (cached_symbol, cached_tf), params
    return None


def has_tuned_regime_params(symbol: str, timeframe: str,
                            filepath: str = "regime_params.json",
                            *,
                            require_provenance: bool = False,
                            provenance_filepath: Optional[str] = None) -> bool:
    """
    True only when regime params contain an explicit tuned entry.

    This intentionally does not call ``load_regime_params`` because live
    readiness must distinguish trained params from research/default fallbacks.
    """
    _ensure_regime_params_loaded(filepath)
    if _find_tuned_regime_params(symbol, timeframe) is None:
        return False
    if not require_provenance:
        return True
    return bool(validate_regime_params_provenance(
        symbol,
        timeframe,
        filepath,
        provenance_filepath=provenance_filepath,
    ).get("valid", False))


def validated_regime_params_for_timeframe(symbol: str, timeframe: str) -> RegimeParams:
    """
    Return the explicit detector row matching the optimizer's no-file surface.

    When optimization validated winners without a tuned ``regime_params.json``
    file, the operative detector was the timeframe regime surface plus the
    asset-class ADX anchor. Materializing that row into JSON makes live carry
    over the same surface without relying on implicit fallback behavior.
    """
    tf_key = str(timeframe or "").strip().upper()
    base = DEFAULT_PARAMS_BY_TIMEFRAME.get(tf_key, RegimeParams())
    return _with_asset_class_adx_threshold(base, str(symbol or "").strip().upper())


def materialize_regime_params_for_cells(
    cells: List[Tuple[str, str]],
    filepath: str = "regime_params.json",
) -> int:
    """
    Ensure ``filepath`` has explicit regime params for each symbol/timeframe.

    Existing tuned rows are preserved. Missing rows are filled with the exact
    no-file detector surface used during optimization/backtesting.
    """
    path_raw = str(filepath or "").strip()
    if not path_raw:
        return 0
    path = Path(path_raw)
    payload: Dict[str, Dict[str, Any]] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = {
                    str(sym): dict(tf_map)
                    for sym, tf_map in loaded.items()
                    if isinstance(tf_map, dict)
                }
        except Exception as exc:
            logger.warning("Failed to read existing regime params from %s: %s", path, exc)

    added = 0
    # Preserve the broker's original symbol case so regime-param keys match the
    # ledger exactly (live lookup is case-insensitive, but the persisted keys
    # should mirror config/ledger casing, e.g. "Platinum" not "PLATINUM").
    for symbol, timeframe in sorted({(str(s).strip(), str(tf).strip().upper()) for s, tf in cells}):
        if not symbol or not timeframe:
            continue
        symbol_payload = payload.setdefault(symbol, {})
        if timeframe in symbol_payload:
            continue
        symbol_payload[timeframe] = validated_regime_params_for_timeframe(symbol, timeframe).to_dict()
        added += 1

    if added <= 0:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)
    clear_regime_params_cache()
    logger.info("Materialized %d explicit validated regime-param rows into %s", added, path)
    return added


def _object_get(payload: Any, key: str, default: Any = None) -> Any:
    if isinstance(payload, dict):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _regime_payload_tradeable(payload: Any) -> bool:
    name = str(_object_get(payload, "strategy_name", "") or "").upper()
    if not name or name == "NO_TRADE":
        return False
    artifact_meta = _object_get(payload, "artifact_meta", {}) or {}
    if isinstance(artifact_meta, dict) and artifact_meta.get("no_trade"):
        return False
    status = str(_object_get(payload, "promotion_status", "BLOCKED") or "BLOCKED").upper()
    return status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}


def materialize_regime_params_from_symbol_config(
    symbol_config: Any,
    filepath: str = "regime_params.json",
) -> int:
    """Materialize explicit detector rows for a saved symbol winner config."""
    symbol = str(_object_get(symbol_config, "symbol", "") or "").strip()
    regimes = _object_get(symbol_config, "regime_configs", {}) or {}
    cells: Set[Tuple[str, str]] = set()
    if symbol and isinstance(regimes, dict):
        for timeframe, regime_map in regimes.items():
            if not isinstance(regime_map, dict):
                continue
            if any(_regime_payload_tradeable(cfg) for cfg in regime_map.values()):
                cells.add((symbol, str(timeframe or "").strip().upper()))
    return materialize_regime_params_for_cells(list(cells), filepath=filepath)


def load_regime_params(symbol: str, timeframe: str, 
                       filepath: str = "regime_params.json") -> RegimeParams:
    """
    Load regime parameters for a symbol/timeframe.
    
    Priority:
    1. regime_params.json (symbol -> timeframe -> params)
    2. DEFAULT_PARAMS_BY_TIMEFRAME
    3. RegimeParams() defaults
    
    Args:
        symbol: Symbol name
        timeframe: Timeframe string
        filepath: Path to regime_params.json
        
    Returns:
        RegimeParams for the symbol/timeframe
    """
    _ensure_regime_params_loaded(filepath)

    # Try symbol + timeframe specific
    tuned = _find_tuned_regime_params(symbol, timeframe)
    if tuned is not None:
        tuned_key, cached = tuned
        # Respect explicit JSON thresholds; otherwise apply the asset-class default.
        if tuned_key in _REGIME_ADX_THRESHOLD_EXPLICIT:
            return cached
        return _with_asset_class_adx_threshold(cached, symbol)

    # Record fallback sources for the startup summary.
    fallback_key = (symbol, timeframe)
    if timeframe in DEFAULT_PARAMS_BY_TIMEFRAME:
        _record_regime_fallback(fallback_key, source="timeframe_default")
        return _with_asset_class_adx_threshold(
            DEFAULT_PARAMS_BY_TIMEFRAME[timeframe], symbol
        )

    _record_regime_fallback(fallback_key, source="hardcoded_default")
    return _with_asset_class_adx_threshold(RegimeParams(), symbol)


def _with_asset_class_adx_threshold(base: RegimeParams, symbol: str) -> RegimeParams:
    """Return a copy of `base` with the asset-class ADX threshold applied."""
    target = _resolve_adx_trend_threshold(symbol)
    if float(base.adx_trend_threshold) == float(target):
        return base
    # RegimeParams is a plain dataclass — copy via replace().
    from dataclasses import replace
    return replace(base, adx_trend_threshold=target)


def _record_regime_fallback(key: Tuple[str, str], source: str) -> None:
    """Log INFO once per (symbol, timeframe) fallback and stash for the startup summary."""
    if key in _REGIME_FALLBACK_LOGGED:
        return
    _REGIME_FALLBACK_LOGGED.add(key)
    _REGIME_FALLBACK_RECORD[key] = source
    symbol, timeframe = key
    logger.info(
        "regime_params fallback for %s/%s -> %s (no tuned entry; using defaults)",
        symbol, timeframe, source,
    )


def get_regime_fallback_record() -> Dict[Tuple[str, str], str]:
    """Snapshot of (symbol, timeframe) → fallback source for the startup summary."""
    return dict(_REGIME_FALLBACK_RECORD)


def clear_regime_fallback_log() -> None:
    """Reset the fallback log (used by tests)."""
    _REGIME_FALLBACK_LOGGED.clear()
    _REGIME_FALLBACK_RECORD.clear()


def save_regime_params(params_dict: Dict[str, Dict[str, RegimeParams]], 
                       filepath: str = "regime_params.json"):
    """Save regime parameters to JSON file."""
    output = {}
    for symbol, tf_dict in params_dict.items():
        output[symbol] = {}
        for tf, params in tf_dict.items():
            output[symbol][tf] = params.to_dict()
    
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)
    
    logger.info(f"Saved regime params to {filepath}")


def clear_regime_params_cache():
    """Clear the cached regime parameters (useful for reloading)."""
    global _REGIME_PARAMS_CACHE, _REGIME_PARAMS_LOADED, _REGIME_PARAMS_LOADED_PATH
    _REGIME_PARAMS_CACHE = {}
    _REGIME_PARAMS_LOADED = False
    _REGIME_PARAMS_LOADED_PATH = None
    _REGIME_ADX_THRESHOLD_EXPLICIT.clear()
    _REGIME_SOURCE_DIGEST_CACHE.clear()
    clear_regime_fallback_log()


# =============================================================================
# REGIME DETECTOR
# =============================================================================

REGIME_STREAM_SCHEMA_VERSION = "fxpm.regime-stream-state.v2"
REGIME_STREAM_TRANSITION_VERSION = "fxpm.regime-stream-transition.v2"
REGIME_ALGORITHM_VERSION = "fxpm.causal-regime.v2-reconstruction-r4"
REGIME_OUTPUT_COLUMNS = (
    "TREND_SCORE", "RANGE_SCORE", "BREAKOUT_SCORE", "CHOP_SCORE",
    "REGIME_RAW", "REGIME", "REGIME_STRENGTH", "REGIME_GAP",
    "REGIME_WARMUP", "REGIME_LIVE", "REGIME_STRENGTH_LIVE",
    "REGIME_RAW_STRENGTH", "REGIME_RAW_STRENGTH_LIVE", "REGIME_ADX",
    "REGIME_ATR", "REGIME_BB_WIDTH", "REGIME_CONFIRM_EVIDENCE",
)
_REGIME_INTEGER_LIMITS = {
    "k_confirm": 64, "k_hold": 256, "adx_period": 512,
    "atr_period": 512, "bb_period": 512, "efficiency_period": 512,
    "bb_squeeze_lookback": 10_000, "atr_lookback": 10_000,
    "structure_period": 512,
}
_REGIME_PARAM_NAMES = tuple(RegimeParams.__dataclass_fields__)
_REGIME_RUNTIME_CODE_SHA256 = _source_sha256(Path(__file__).resolve())


def validated_regime_params(value: Any = None) -> RegimeParams:
    """Validate and isolate one complete numeric detector parameter row."""
    if value is None:
        value = RegimeParams()
    if isinstance(value, RegimeParams):
        payload = value.to_dict()
    elif type(value) is dict:
        payload = dict(value)
    else:
        raise TypeError("regime parameters must be RegimeParams or dict")
    if set(payload) != set(_REGIME_PARAM_NAMES):
        raise ValueError("regime parameters require the complete canonical keyset")
    for name, raw in payload.items():
        if isinstance(raw, (bool, np.bool_)):
            raise ValueError(f"regime parameter {name} must be numeric")
        if name in _REGIME_INTEGER_LIMITS:
            if not isinstance(raw, (int, np.integer)) or not 1 <= int(raw) <= _REGIME_INTEGER_LIMITS[name]:
                raise ValueError(f"regime parameter {name} must be a positive integer")
        elif not isinstance(raw, (int, float, np.integer, np.floating)):
            raise ValueError(f"regime parameter {name} must be numeric")
        if not math.isfinite(float(raw)):
            raise ValueError(f"regime parameter {name} must be finite")
        if isinstance(raw, np.generic):
            payload[name] = raw.item()
    weights = {name for name in payload if name.endswith("_weight")}
    for name in weights | {"gap_min"}:
        if not 0 <= payload[name] <= 1:
            raise ValueError(f"regime parameter {name} must be within [0, 1]")
    if not 0 <= payload["bb_squeeze_percentile"] <= 100:
        raise ValueError("regime parameter bb_squeeze_percentile must be within [0, 100]")
    if not 0 < payload["adx_trend_threshold"] <= 100:
        raise ValueError("regime parameter adx_trend_threshold must be positive")
    if not 0 < payload["bb_std"] <= 10:
        raise ValueError("regime parameter bb_std must be positive")
    if not 0 <= payload["structure_atr_mult"] <= 10:
        raise ValueError("regime parameter structure_atr_mult must be nonnegative")
    for prefix in ("trend", "range", "breakout", "chop"):
        total = sum(payload[name] for name in weights if name.startswith(prefix))
        if not math.isclose(total, 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError(f"regime {prefix} weights must sum to 1")
    return RegimeParams(**payload)


def _regime_warmup(params: RegimeParams) -> int:
    return max(
        50, 2 * params.adx_period, params.atr_period + params.atr_lookback,
        params.bb_period + params.bb_squeeze_lookback,
        params.efficiency_period, params.structure_period,
    )


@register_jitable
def _trend_adx_evidence(adx: float, threshold: float) -> float:
    return max(0.0, min(1.0, adx / (1.6 * threshold)))


@register_jitable
def _chop_adx_evidence(adx: float, threshold: float) -> float:
    x = adx / threshold
    if x <= 0.0:
        return 0.0
    if x < 0.6:
        return 0.5 * x / 0.6
    if x < 0.9:
        return 0.5 + 0.5 * (x - 0.6) / 0.3
    if x < 1.2:
        return 1.0 - 0.5 * (x - 0.9) / 0.3
    return max(0.0, 0.5 * (2.8 - x) / 1.6)


@register_jitable
def _confirm_regime(scores, current, held, challenger, evidence, k_confirm, gap_min, k_hold):
    leader = int(np.argmax(scores))
    unique = True
    for label in range(4):
        if label != leader and scores[label] == scores[leader]:
            unique = False
    # Reconstructed startup: unique leader or CHOP on ties, held=1, no warmup credit.
    if current < 0:
        return (leader if unique else 3), 1, -1, 0.0
    if unique and leader != current:
        if challenger != leader:
            challenger, evidence = leader, 0.0
        evidence += scores[leader] - scores[current]
        # held counts assignments completed before this observation.
        if held >= k_hold and evidence >= k_confirm * gap_min:
            return leader, 1, -1, 0.0
    else:
        challenger, evidence = -1, 0.0
    return current, held + 1, challenger, evidence


@jit(nopython=True, cache=True)
def _regime_numeric_kernel(bars, p, warmup, state, ohlc, atr_ring, width_ring, squeeze_ring):
    """One carried closed-bar implementation for both batch and incremental use."""
    adx_period, atr_period, bb_period = int(p[3]), int(p[4]), int(p[5])
    efficiency_period, squeeze_lookback = int(p[7]), int(p[8])
    atr_lookback, structure_period = int(p[10]), int(p[11])
    capacity = len(ohlc)
    output = np.zeros((len(bars), 17), dtype=np.float64)
    for row in range(len(bars)):
        i = int(state[0])
        open_p, high, low, close = bars[row, 0], bars[row, 1], bars[row, 2], bars[row, 3]
        tr, plus_dm, minus_dm = 0.0, 0.0, 0.0
        if i > 0:
            prev = ohlc[(i - 1) % capacity]
            tr = max(high - low, abs(high - prev[3]), abs(low - prev[3]))
            up, down = high - prev[1], prev[2] - low
            if up > down and up > 0:
                plus_dm = up
            if down > up and down > 0:
                minus_dm = down
        ohlc[i % capacity] = bars[row]
        if i < atr_period:
            state[1] += tr
            if i == atr_period - 1:
                state[1] /= atr_period
        else:
            state[1] += (tr - state[1]) / atr_period
        atr = state[1] if i >= atr_period - 1 else 0.0
        if i < adx_period:
            state[2] += tr
            state[3] += plus_dm
            state[4] += minus_dm
            if i == adx_period - 1:
                state[2:5] /= adx_period
        else:
            state[2] += (tr - state[2]) / adx_period
            state[3] += (plus_dm - state[3]) / adx_period
            state[4] += (minus_dm - state[4]) / adx_period
        dx = 0.0
        if i >= adx_period and state[2] > 1e-10:
            plus_di, minus_di = 100 * state[3] / state[2], 100 * state[4] / state[2]
            if plus_di + minus_di > 1e-10:
                dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        if i >= adx_period:
            state[5] += (dx - state[5]) / adx_period
        adx = state[5]
        width, mean, sigma = 0.0, 0.0, 0.0
        if i >= bb_period - 1:
            for offset in range(bb_period):
                mean += ohlc[(i - offset) % capacity, 3]
            mean /= bb_period
            for offset in range(bb_period):
                sigma += (ohlc[(i - offset) % capacity, 3] - mean) ** 2
            sigma = math.sqrt(sigma / bb_period)
            if mean > 1e-10:
                width = 2 * p[6] * sigma / mean
        width_ring[i % squeeze_lookback] = width
        squeeze = 0.0
        if i >= squeeze_lookback:
            positive_widths = width_ring[width_ring > 0]
            if len(positive_widths) > 10 and width > 0:
                threshold = np.percentile(positive_widths, p[9])
                squeeze = min(1.0, threshold / width)
        release = 0.0
        if i >= 5:
            recent_squeeze = np.max(squeeze_ring)
            if recent_squeeze > 0.5 and squeeze < 0.5:
                release = min(1.0, recent_squeeze * (1.0 - squeeze) * 2)
        squeeze_ring[i % 5] = squeeze
        atr_percentile = 0.0
        if i >= atr_lookback and atr > 0:
            positive, lower = 0, 0
            for offset in range(1, atr_lookback):
                previous_atr = atr_ring[(i - offset) % atr_lookback]
                if previous_atr > 0:
                    positive += 1
                    if previous_atr <= atr:
                        lower += 1
            if positive > 10:
                atr_percentile = lower / positive
        atr_ring[i % atr_lookback] = atr
        efficiency, slope, containment, structure, whipsaw, flips = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        if i >= efficiency_period:
            moves = 0.0
            for offset in range(efficiency_period):
                moves += abs(ohlc[(i - offset) % capacity, 3] - ohlc[(i - offset - 1) % capacity, 3])
            if moves > 1e-10:
                efficiency = min(1.0, abs(close - ohlc[(i - efficiency_period) % capacity, 3]) / moves)
        if i >= 20:
            positive, negative = 0, 0
            for offset in range(20):
                change = ohlc[(i - offset) % capacity, 3] - ohlc[(i - offset - 1) % capacity, 3]
                if change > 0:
                    positive += 1
                if change < 0:
                    negative += 1
            slope = max(positive, negative) / 20
        if i >= max(bb_period, 10):
            upper, lower = mean + p[6] * sigma, mean - p[6] * sigma
            for offset in range(10):
                bar = ohlc[(i - offset) % capacity]
                if bar[1] <= upper and bar[2] >= lower:
                    containment += 0.1
        if i >= structure_period:
            recent_high, recent_low = -np.inf, np.inf
            for offset in range(1, structure_period + 1):
                bar = ohlc[(i - offset) % capacity]
                recent_high, recent_low = max(recent_high, bar[1]), min(recent_low, bar[2])
            margin = atr * p[12] if atr > 0 else 0.0
            if close > recent_high + margin or close < recent_low - margin:
                structure = 1.0
            elif margin > 0:
                structure = max(0.0, min(1.0, max(close - recent_high, recent_low - close) / (margin + 1e-10)))
        if i >= 10:
            wick_sum, range_sum = 0.0, 0.0
            for offset in range(10):
                bar = ohlc[(i - offset) % capacity]
                bar_range = bar[1] - bar[2]
                if bar_range > 1e-10:
                    wick_sum += bar_range - abs(bar[3] - bar[0])
                    range_sum += bar_range
            if range_sum > 1e-10:
                whipsaw = wick_sum / range_sum
        if i >= 11:
            for offset in range(9):
                current_move = ohlc[(i - offset) % capacity, 3] - ohlc[(i - offset - 1) % capacity, 3]
                previous_move = ohlc[(i - offset - 1) % capacity, 3] - ohlc[(i - offset - 2) % capacity, 3]
                if current_move * previous_move < 0:
                    flips += 1 / 9
        scores = np.full(4, 0.25)
        if i >= warmup:
            trend_adx = _trend_adx_evidence(adx, p[25])
            chop_adx = _chop_adx_evidence(adx, p[25])
            scores[0] = p[13] * trend_adx + p[14] * efficiency + p[15] * slope
            scores[1] = p[16] * (1 - trend_adx) + p[17] * (1 - efficiency) + p[18] * containment
            scores[2] = p[19] * release + p[20] * atr_percentile + p[21] * structure
            scores[3] = p[22] * chop_adx + p[23] * (1 - efficiency) + p[24] * (whipsaw + flips) * 0.5
            total = np.sum(scores)
            if total > 1e-10:
                scores /= total
            else:
                scores[:] = 0.25
        raw = int(np.argmax(scores)) if i >= warmup else 3
        raw_strength = scores[raw]
        ordered = np.sort(scores)
        gap = ordered[3] - ordered[2]
        if i >= warmup:
            current, held, challenger, evidence = _confirm_regime(
                scores, int(state[6]), int(state[7]), int(state[8]), state[9], int(p[0]), p[1], int(p[2])
            )
            state[6], state[7], state[8], state[9] = current, held, challenger, evidence
        confirmed = int(state[6]) if state[6] >= 0 else 3
        strength = scores[confirmed]
        output[row, :4] = scores
        output[row, 4:9] = np.array([raw, confirmed, strength, gap, 1.0 if i < warmup else 0.0])
        output[row, 9], output[row, 10] = state[10], state[11] if i else np.nan
        output[row, 11], output[row, 12] = raw_strength, state[12] if i else np.nan
        output[row, 13:17] = np.array([adx, atr, width, state[9]])
        state[10], state[11], state[12] = confirmed, strength, raw_strength
        state[0] += 1
    return output


class RegimeStream:
    """Bounded causal classifier state; timestamps denote bar opens."""

    def __init__(self, params: Any = None, *, stream_key: str, use_numba: Optional[bool] = None):
        if type(stream_key) is not str or not stream_key.strip():
            raise ValueError("stream_key must be a nonempty canonical string")
        if use_numba is not None and type(use_numba) is not bool:
            raise TypeError("use_numba must be bool or None")
        isolated = validated_regime_params(params)
        self._params_payload = isolated.to_dict()
        self._params_sha256 = regime_params_sha256(self._params_payload)
        self._stream_key = stream_key
        self._code_sha256 = _source_sha256(Path(__file__).resolve())
        if self._code_sha256 != _REGIME_RUNTIME_CODE_SHA256:
            raise ValueError("regime source changed after module load; restart before processing")
        self._warmup = _regime_warmup(isolated)
        self._numeric_params = np.array([self._params_payload[name] for name in _REGIME_PARAM_NAMES], dtype=np.float64)
        self._use_numba = NUMBA_AVAILABLE if use_numba is None else (use_numba and NUMBA_AVAILABLE)
        self._state = np.zeros(13, dtype=np.float64)
        self._state[[6, 8, 10]] = -1
        self._ohlc = np.zeros((max(isolated.bb_period, isolated.efficiency_period + 1, isolated.structure_period + 1, 21), 4))
        self._atr = np.zeros(isolated.atr_lookback)
        self._width = np.zeros(isolated.bb_squeeze_lookback)
        self._squeeze = np.zeros(5)
        self._last_index = None
        self._index_kind = None
        self._last_bar = None
        self._last_output = None

    @property
    def params(self) -> RegimeParams:
        return RegimeParams(**self._params_payload)

    @property
    def stream_key(self) -> str:
        return self._stream_key

    @property
    def warmup_bars(self) -> int:
        return self._warmup

    @property
    def bars_processed(self) -> int:
        return int(self._state[0])

    @property
    def numeric_state_nbytes(self) -> int:
        return sum(array.nbytes for array in (self._numeric_params, self._state, self._ohlc, self._atr, self._width, self._squeeze))

    @staticmethod
    def _index_type(index: pd.Index) -> str:
        if isinstance(index, pd.DatetimeIndex):
            return "datetime-aware" if index.tz is not None else "datetime-naive"
        if index.dtype.kind in "iu":
            return "integer"
        if index.dtype.kind == "f":
            return "float"
        raise ValueError("bar index must be datetime or numeric")

    @staticmethod
    def _frame(values: np.ndarray, index: pd.Index) -> pd.DataFrame:
        result = pd.DataFrame(values, index=index, columns=REGIME_OUTPUT_COLUMNS)
        for column in ("REGIME_RAW", "REGIME", "REGIME_LIVE"):
            codes = result[column].to_numpy(dtype=np.int64)
            result[column] = pd.Categorical.from_codes(codes, dtype=REGIME_DTYPE)
        result["REGIME_WARMUP"] = result["REGIME_WARMUP"].astype(bool)
        return result

    def update(self, closed_bars: pd.DataFrame, *, observed_at: Any = None, bar_duration: Any = None) -> pd.DataFrame:
        if _source_sha256(Path(__file__).resolve()) != self._code_sha256:
            raise ValueError("regime source changed after module load; restart before processing")
        if not isinstance(closed_bars, pd.DataFrame):
            raise TypeError("closed_bars must be a DataFrame")
        missing = set(("Open", "High", "Low", "Close")) - set(closed_bars.columns)
        if missing:
            raise ValueError(f"closed bars missing OHLC columns: {sorted(missing)}")
        index = closed_bars.index
        kind = self._index_type(index)
        if index.hasnans or not index.is_unique or not index.is_monotonic_increasing:
            raise ValueError("bars require unique, finite, strictly increasing times")
        if kind in ("integer", "float") and not np.isfinite(index.to_numpy()).all():
            raise ValueError("bars require finite numeric times")
        if self._index_kind is not None and kind != self._index_kind:
            raise ValueError("bar index type/timezone awareness changed within stream")
        try:
            bars = closed_bars.loc[:, ["Open", "High", "Low", "Close"]].to_numpy(dtype=np.float64, copy=True)
        except (TypeError, ValueError) as exc:
            raise ValueError("OHLC values must be finite numeric values") from exc
        if bars.shape != (len(index), 4) or not np.isfinite(bars).all():
            raise ValueError("OHLC values must be finite numeric values")
        if np.any(bars[:, 1] < np.maximum(bars[:, 0], bars[:, 3])) or np.any(bars[:, 2] > np.minimum(bars[:, 0], bars[:, 3])):
            raise ValueError("invalid OHLC envelope")
        if (observed_at is None) != (bar_duration is None):
            raise ValueError("observed_at and bar_duration must be supplied together")
        if observed_at is not None:
            observed = pd.Timestamp(observed_at)
            duration = pd.Timedelta(bar_duration)
            if kind != "datetime-aware" or observed.tzinfo is None:
                raise ValueError("closed-time validation requires timezone-aware bars and observed_at")
            if pd.isna(observed) or pd.isna(duration) or duration <= pd.Timedelta(0):
                raise ValueError("observed_at and bar_duration must be valid")
            if np.any(index + duration > observed):
                raise ValueError("unfinished bar supplied before its close")
        if len(index) == 0:
            return self._frame(np.empty((0, 17)), index)
        replay = False
        if self._last_index is not None:
            if index[0] < self._last_index:
                raise ValueError("reversed or overlapping history requires verified replay")
            if index[0] == self._last_index:
                if not np.array_equal(bars[0], self._last_bar):
                    raise ValueError("revised last bar requires verified replay")
                replay = True
        # All input validation precedes mutation, including revisions in a batch.
        offset = 1 if replay else 0
        kernel = _regime_numeric_kernel
        if not self._use_numba and hasattr(kernel, "py_func"):
            kernel = kernel.py_func
        values = kernel(bars[offset:], self._numeric_params, self._warmup, self._state, self._ohlc, self._atr, self._width, self._squeeze)
        if replay:
            values = np.vstack((self._last_output, values))
        self._index_kind, self._last_index = kind, index[-1]
        self._last_bar, self._last_output = bars[-1].copy(), values[-1].copy()
        return self._frame(values, index)

    def snapshot(self) -> Dict[str, Any]:
        last_index = self._last_index
        if isinstance(last_index, pd.Timestamp):
            last_index = last_index.isoformat()
        elif isinstance(last_index, np.generic):
            last_index = last_index.item()
        payload = {
            "schema_version": REGIME_STREAM_SCHEMA_VERSION,
            "algorithm_version": REGIME_ALGORITHM_VERSION,
            "code_sha256": self._code_sha256,
            "params": dict(self._params_payload), "params_sha256": self._params_sha256,
            "stream_key": self._stream_key, "state": self._state.tolist(),
            "ohlc_ring": self._ohlc.tolist(), "atr_ring": self._atr.tolist(),
            "width_ring": self._width.tolist(), "squeeze_ring": self._squeeze.tolist(),
            "index_kind": self._index_kind, "last_index": last_index,
            "last_bar": None if self._last_bar is None else self._last_bar.tolist(),
            "last_output": None if self._last_output is None else [float(value) if np.isfinite(value) else None for value in self._last_output],
        }
        payload["state_sha256"] = _tuner_canonical_sha256(payload)
        return payload

    @classmethod
    def restore(cls, checkpoint: Any, *, expected_params: Any, expected_stream_key: str, use_numba: Optional[bool] = None) -> 'RegimeStream':
        if isinstance(checkpoint, str):
            try:
                checkpoint = json.loads(checkpoint)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid JSON regime checkpoint") from exc
        if type(checkpoint) is not dict:
            raise ValueError("checkpoint must be a JSON object")
        stream = cls(expected_params, stream_key=expected_stream_key, use_numba=use_numba)
        expected_keys = set(stream.snapshot())
        if set(checkpoint) != expected_keys:
            raise ValueError("checkpoint has an invalid schema keyset")
        payload = {key: value for key, value in checkpoint.items() if key != "state_sha256"}
        try:
            encoded = json.dumps(payload, allow_nan=False)
            del encoded
        except (TypeError, ValueError) as exc:
            raise ValueError("checkpoint must contain finite JSON values") from exc
        if checkpoint["state_sha256"] != _tuner_canonical_sha256(payload):
            raise ValueError("checkpoint state digest mismatch")
        bindings = {
            "schema_version": REGIME_STREAM_SCHEMA_VERSION,
            "algorithm_version": REGIME_ALGORITHM_VERSION,
            "code_sha256": stream._code_sha256, "stream_key": stream.stream_key,
            "params_sha256": stream._params_sha256,
        }
        for name, expected in bindings.items():
            if type(checkpoint[name]) is not str or checkpoint[name] != expected:
                raise ValueError(f"checkpoint {name} binding mismatch")
        params = validated_regime_params(checkpoint["params"])
        if regime_params_sha256(params.to_dict()) != stream._params_sha256:
            raise ValueError("checkpoint parameter binding mismatch")
        for name, attr in (("state", "_state"), ("ohlc_ring", "_ohlc"), ("atr_ring", "_atr"), ("width_ring", "_width"), ("squeeze_ring", "_squeeze")):
            try:
                array = np.asarray(checkpoint[name], dtype=np.float64)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid checkpoint {name}") from exc
            if array.shape != getattr(stream, attr).shape or not np.isfinite(array).all():
                raise ValueError(f"invalid checkpoint {name}")
            setattr(stream, attr, array.copy())
        state = stream._state
        if any(state[pos] != int(state[pos]) for pos in (0, 6, 7, 8, 10)) or not 0 <= state[0] < 2**53:
            raise ValueError("invalid checkpoint integral state")
        if any(not -1 <= state[pos] <= 3 for pos in (6, 8, 10)) or not 0 <= state[7] <= state[0] or state[9] < 0 or np.any(state[1:6] < 0):
            raise ValueError("invalid checkpoint confirmation or Wilder state")
        if np.any(stream._atr < 0) or np.any(stream._width < 0) or np.any((stream._squeeze < 0) | (stream._squeeze > 1)):
            raise ValueError("invalid checkpoint indicator rings")
        kind = checkpoint["index_kind"]
        if state[0] == 0:
            if any(checkpoint[key] is not None for key in ("index_kind", "last_index", "last_bar", "last_output")):
                raise ValueError("empty checkpoint has a last observation")
            initial = np.zeros(13)
            initial[[6, 8, 10]] = -1
            if not np.array_equal(state, initial) or any(np.any(array) for array in (stream._ohlc, stream._atr, stream._width, stream._squeeze)):
                raise ValueError("empty checkpoint has nonempty numeric state")
        else:
            if kind not in ("datetime-aware", "datetime-naive", "integer", "float"):
                raise ValueError("invalid checkpoint index kind")
            try:
                index_value = pd.Timestamp(checkpoint["last_index"]) if kind.startswith("datetime") else checkpoint["last_index"]
                probe_index = pd.DatetimeIndex([index_value]) if kind.startswith("datetime") else pd.Index([index_value])
                last_bar = np.asarray(checkpoint["last_bar"], dtype=np.float64)
                last_output = np.asarray(checkpoint["last_output"], dtype=np.float64)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid checkpoint last observation") from exc
            allowed_missing = np.zeros(17, dtype=bool)
            if state[0] == 1:
                allowed_missing[[10, 12]] = True
            if cls._index_type(probe_index) != kind or probe_index.hasnans or last_bar.shape != (4,) or last_output.shape != (17,) or not np.isfinite(last_bar).all():
                raise ValueError("invalid checkpoint last observation")
            if np.any(~np.isfinite(last_output) & ~allowed_missing):
                raise ValueError("invalid checkpoint last output")
            if not np.array_equal(last_bar, stream._ohlc[(int(state[0]) - 1) % len(stream._ohlc)]):
                raise ValueError("checkpoint last bar and ring disagree")
            if last_bar[1] < max(last_bar[0], last_bar[3]) or last_bar[2] > min(last_bar[0], last_bar[3]):
                raise ValueError("invalid checkpoint last OHLC envelope")
            for position in (4, 5, 9):
                lower = -1 if position == 9 and state[0] == 1 else 0
                if last_output[position] != int(last_output[position]) or not lower <= last_output[position] <= 3:
                    raise ValueError("invalid checkpoint output label")
            scores = last_output[:4]
            if np.any((scores < 0) | (scores > 1)) or not math.isclose(float(scores.sum()), 1, abs_tol=1e-12):
                raise ValueError("invalid checkpoint output scores")
            if last_output[8] != float(state[0] <= stream._warmup):
                raise ValueError("checkpoint readiness and count disagree")
            if last_output[6] != scores[int(last_output[5])] or last_output[11] != scores[int(last_output[4])]:
                raise ValueError("checkpoint output strength ownership mismatch")
            if state[10] != last_output[5] or state[11] != last_output[6] or state[12] != last_output[11]:
                raise ValueError("checkpoint carried output and state disagree")
            if state[0] <= stream._warmup:
                if state[6] != -1 or state[7] != 0 or state[8] != -1 or state[9] != 0:
                    raise ValueError("checkpoint accumulated warmup evidence")
            elif state[6] != last_output[5] or state[7] < 1:
                raise ValueError("checkpoint confirmed output and state disagree")
            if state[8] == -1 and state[9] != 0:
                raise ValueError("checkpoint evidence has no challenger")
            stream._last_index, stream._index_kind = index_value, kind
            stream._last_bar, stream._last_output = last_bar.copy(), last_output.copy()
        return stream


class MarketRegimeDetector:
    """Batch adapter for the same closed-bar sequential RegimeStream kernel."""

    def __init__(self, params: Any = None):
        self.params = validated_regime_params(params)

    @property
    def warmup_bars(self) -> int:
        return _regime_warmup(self.params)

    def compute_regime_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        # Public inputs always derive indicators from OHLC, never supplied ADX/ATR.
        return RegimeStream(self.params, stream_key="batch").update(df)

    def _normalize_adx(self, adx_value: float, threshold: float = 25.0) -> float:
        return float(_trend_adx_evidence(adx_value, threshold))

    def _normalize_adx_vectorized(self, adx: np.ndarray, threshold: float = 25.0) -> np.ndarray:
        return np.array([self._normalize_adx(float(value), threshold) for value in adx], dtype=float)

    def _normalize_adx_mid(self, adx_value: float, threshold: float = 25.0) -> float:
        return float(_chop_adx_evidence(adx_value, threshold))

    def _normalize_adx_mid_vectorized(self, adx: np.ndarray, threshold: float = 25.0) -> np.ndarray:
        return np.array([self._normalize_adx_mid(float(value), threshold) for value in adx], dtype=float)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def compute_regime_for_features(df: pd.DataFrame, 
                                 symbol: str = "", 
                                 timeframe: str = "H1",
                                 params_file: str = "regime_params.json") -> pd.DataFrame:
    """
    Compute regime features for a DataFrame.
    
    This is the main entry point for adding regime columns to a feature DataFrame.
    It loads symbol/timeframe-specific parameters if available.
    
    Args:
        df: DataFrame with OHLCV data (and optionally indicators)
        symbol: Symbol name (for parameter lookup)
        timeframe: Timeframe string (for parameter lookup)
        params_file: Path to regime_params.json
        
    Returns:
        DataFrame with all original columns plus regime columns
    """
    # Load params for this symbol/timeframe
    params = load_regime_params(symbol, timeframe, params_file)
    
    # Create detector and compute
    detector = MarketRegimeDetector(params)
    regime_df = detector.compute_regime_scores(df)
    
    # Merge regime columns into original df
    result = df.copy()
    for col in regime_df.columns:
        result[col] = regime_df[col]
    
    return result


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'RegimeType',
    'RegimeParams',
    'MarketRegimeDetector',
    'RegimeStream',
    'REGIME_OUTPUT_COLUMNS',
    'REGIME_STREAM_SCHEMA_VERSION',
    'REGIME_STREAM_TRANSITION_VERSION',
    'validated_regime_params',
    'load_regime_params',
    'has_tuned_regime_params',
    'validate_regime_params_provenance',
    'regime_params_provenance_path',
    'regime_params_sha256',
    'REGIME_PROVENANCE_SCHEMA_VERSION',
    'REGIME_PROVENANCE_FIT_SCOPE',
    'validated_regime_params_for_timeframe',
    'materialize_regime_params_for_cells',
    'materialize_regime_params_from_symbol_config',
    'save_regime_params',
    'clear_regime_params_cache',
    'compute_regime_for_features',
    'DEFAULT_PARAMS_BY_TIMEFRAME',
]


if __name__ == "__main__":
    # Test regime detection
    logging.basicConfig(level=logging.INFO)
    
    print("Regime Detection Module")
    print(f"Regime types: {RegimeType.ALL}")
    
    # Test with synthetic data
    np.random.seed(42)
    n = 500
    
    # Generate trending data
    trend = np.cumsum(np.random.randn(n) * 0.5 + 0.1)
    
    df = pd.DataFrame({
        'Open': 100 + trend,
        'High': 100 + trend + np.random.rand(n) * 2,
        'Low': 100 + trend - np.random.rand(n) * 2,
        'Close': 100 + trend + np.random.randn(n) * 0.5,
        'Volume': np.random.randint(1000, 10000, n)
    }, index=pd.date_range('2024-01-01', periods=n, freq='1h'))
    
    detector = MarketRegimeDetector()
    result = detector.compute_regime_scores(df)
    
    print(f"\nSample regime detection (last 10 bars):")
    print(result[['REGIME', 'REGIME_STRENGTH', 'REGIME_GAP']].tail(10))
    
    print("\nRegime distribution:")
    print(result['REGIME'].value_counts())
    
    print("\npm_regime.py loaded successfully!")


def load_regime_params_cell(
    filepath: str, symbol: str, timeframe: str
) -> Optional[Dict[str, Any]]:
    """Exact params cell for one symbol/timeframe, or None."""
    try:
        payload = json.loads(Path(str(filepath)).read_text(encoding="utf-8"))
    except Exception:
        return None
    cell = _case_insensitive_nested_cell(payload, symbol, timeframe)
    return cell if isinstance(cell, dict) else None
