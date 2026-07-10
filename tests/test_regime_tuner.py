import pandas as pd

from pm_regime_tuner import TunerDataLoader


def test_tuner_resample_uses_explicit_resampler_methods(tmp_path):
    idx = pd.date_range("2026-01-01", periods=48, freq="5min")
    df = pd.DataFrame(
        {
            "Open": range(48),
            "High": range(1, 49),
            "Low": range(48),
            "Close": range(1, 49),
            "Volume": [10] * 48,
        },
        index=idx,
    )

    loader = TunerDataLoader(tmp_path)
    out = loader.resample(df, "H1")

    assert len(out) == 4
    assert out.iloc[0]["Open"] == 0
    assert out.iloc[0]["Close"] == 12
    assert out.iloc[0]["Volume"] == 120
