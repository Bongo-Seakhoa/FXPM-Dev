import json

import numpy as np
import pandas as pd
import pytest

from pm_core import FeatureComputer
from pm_regime import (
    MarketRegimeDetector,
    RegimeParams,
    clear_regime_params_cache,
    regime_params_sha256,
)


def _bars(rows: int = 360) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=rows, freq="h")
    phase = np.linspace(0.0, 18.0, rows)
    close = 1.10 + np.linspace(0.0, 0.08, rows) + 0.012 * np.sin(phase)
    spread = 0.0015 + 0.0004 * np.cos(phase * 0.7)
    return pd.DataFrame(
        {
            "Open": close - 0.0002,
            "High": close + spread,
            "Low": close - spread,
            "Close": close,
            "Volume": np.arange(rows, dtype=float) + 100.0,
        },
        index=index,
    )


@pytest.fixture(autouse=True)
def _isolated_feature_cache():
    FeatureComputer.configure_cache(max_entries=24, disk_enabled=False)
    FeatureComputer.clear_cache()
    clear_regime_params_cache()
    yield
    FeatureComputer.clear_cache()
    clear_regime_params_cache()


def test_direct_override_matches_identical_file_loaded_surface(tmp_path):
    params = RegimeParams(k_confirm=2, gap_min=0.08, k_hold=3)
    params_path = tmp_path / "regime_params.json"
    params_path.write_text(
        json.dumps({"EURUSD": {"H1": params.to_dict()}}),
        encoding="utf-8",
    )

    file_loaded = FeatureComputer.compute_all(
        _bars(),
        symbol="EURUSD",
        timeframe="H1",
        regime_params_file=str(params_path),
    )
    FeatureComputer.clear_cache()
    direct = FeatureComputer.compute_all(
        _bars(),
        symbol="EURUSD",
        timeframe="H1",
        regime_params_override=params,
        regime_params_sha256=regime_params_sha256(params.to_dict()),
    )

    for column in (
        "REGIME_LIVE",
        "REGIME_STRENGTH",
        "REGIME_STRENGTH_LIVE",
        "REGIME_GAP",
    ):
        pd.testing.assert_series_equal(file_loaded[column], direct[column])


def test_direct_params_sha_separates_feature_cache_entries():
    first = RegimeParams(k_confirm=2, gap_min=0.08, k_hold=3)
    second = RegimeParams(k_confirm=4, gap_min=0.15, k_hold=6)
    bars = _bars()

    first_kwargs = {
        "regime_params_override": first.to_dict(),
        "regime_params_sha256": regime_params_sha256(first.to_dict()),
    }
    FeatureComputer.compute_all(bars, symbol="EURUSD", timeframe="H1", **first_kwargs)
    FeatureComputer.compute_all(bars, symbol="EURUSD", timeframe="H1", **first_kwargs)
    FeatureComputer.compute_all(
        bars,
        symbol="EURUSD",
        timeframe="H1",
        regime_params_override=second.to_dict(),
        regime_params_sha256=regime_params_sha256(second.to_dict()),
    )

    stats = FeatureComputer.get_cache_stats()
    assert stats["memory_entries"] == 2
    assert stats["memory_hits"] == 1
    assert stats["memory_misses"] == 2


def test_direct_override_rejects_wrong_hash_nonfinite_and_incomplete_rows():
    payload = RegimeParams().to_dict()
    bars = _bars(40)

    with pytest.raises(ValueError, match="does not match"):
        FeatureComputer.compute_all(
            bars,
            regime_params_override=payload,
            regime_params_sha256="0" * 64,
        )

    nonfinite = dict(payload)
    nonfinite["gap_min"] = float("nan")
    with pytest.raises(ValueError, match="must be finite"):
        FeatureComputer.compute_all(
            bars,
            regime_params_override=nonfinite,
            regime_params_sha256=regime_params_sha256(nonfinite),
        )

    incomplete = dict(payload)
    incomplete.pop("k_hold")
    with pytest.raises(ValueError, match="complete canonical keyset"):
        FeatureComputer.compute_all(
            bars,
            regime_params_override=incomplete,
            regime_params_sha256=regime_params_sha256(incomplete),
        )


def test_direct_override_rejects_hash_subclass_and_unsafe_integer():
    class LyingHash(str):
        def __ne__(self, _other):
            return False

    payload = RegimeParams().to_dict()
    with pytest.raises(ValueError, match="does not match"):
        FeatureComputer.compute_all(
            _bars(40),
            regime_params_override=payload,
            regime_params_sha256=LyingHash("0" * 64),
        )
    payload["k_confirm"] = 10**30
    with pytest.raises(ValueError, match="positive integer"):
        FeatureComputer.compute_all(
            _bars(40),
            regime_params_override=payload,
            regime_params_sha256=regime_params_sha256(payload),
        )


def test_direct_override_uses_raw_canonical_detector_periods():
    params = RegimeParams(adx_period=7, atr_period=21)
    bars = _bars()
    direct = FeatureComputer.compute_all(
        bars,
        symbol="EURUSD",
        timeframe="H1",
        regime_params_override=params,
        regime_params_sha256=regime_params_sha256(params.to_dict()),
    )
    expected = MarketRegimeDetector(params).compute_regime_scores(bars)
    for column in FeatureComputer._REGIME_FEATURE_COLUMNS:
        pd.testing.assert_series_equal(direct[column], expected[column])


def test_direct_override_never_uses_disk_cache(tmp_path):
    FeatureComputer.configure_cache(
        max_entries=0,
        disk_enabled=True,
        disk_dir=tmp_path,
        disk_max_gb=0.1,
        disk_min_free_gb=0.0,
    )
    params = RegimeParams()
    kwargs = {
        "regime_params_override": params,
        "regime_params_sha256": regime_params_sha256(params.to_dict()),
    }
    FeatureComputer.compute_all(_bars(), **kwargs)
    FeatureComputer.compute_all(_bars(), **kwargs)
    stats = FeatureComputer.get_cache_stats()
    assert stats["disk_hits"] == 0
    assert stats["disk_writes"] == 0
    assert list(tmp_path.iterdir()) == []
