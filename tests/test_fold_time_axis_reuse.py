"""INVENTED hourly partition geometry; no market/model or financial authority."""
from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution


def _case(*, missing_close=None, naive_index=False):
    utc = pd.date_range("2026-01-01T00:00:00Z", periods=30, freq="h")
    frame = pd.DataFrame({
        "Open": [100.0 + i / 100 for i in range(30)],
        "High": [101.0 + i / 100 for i in range(30)],
        "Low": [99.0 + i / 100 for i in range(30)],
        "Close": [100.5 + i / 100 for i in range(30)],
        "_BAR_CLOSE_TS": utc + pd.Timedelta(hours=1), "REGIME_LIVE": "TREND",
    }, index=utc.tz_localize(None) if naive_index else utc)
    if missing_close is not None:
        frame.iloc[missing_close, frame.columns.get_loc("_BAR_CLOSE_TS")] = pd.NaT
    attempt = SimpleNamespace(ordinal=0,
        artifacts=SimpleNamespace(refit_recipe={"data_requirements": {"warmup_bars": 2}}))
    fold = {"fit_start_utc": utc[0].isoformat(), "fit_end_exclusive_utc": utc[20].isoformat(),
        "validation_warmup_start_utc": utc[20].isoformat(),
        "validation_start_utc": utc[25].isoformat(),
        "validation_end_exclusive_utc": (utc[-1] + pd.Timedelta(hours=1)).isoformat()}
    bounds = [
        ("optimizer_train", utc[0], utc[5], utc[15]),
        ("tuning_validation", utc[10], utc[15], utc[20]),
        ("final_lockbox", utc[20], utc[25], utc[-1] + pd.Timedelta(hours=1)),
    ]
    arguments = [dict(role=role, frame_start_utc=start.isoformat(),
        scored_start_utc=scored.isoformat(), end_exclusive_utc=end.isoformat(),
        minimum_warmup_bars=5) for role, start, scored, end in bounds]
    return SimpleNamespace(frame=frame, attempt=attempt, fold=fold, arguments=arguments)


def _fold(case):
    return execution._fold_partitions(frame=case.frame, fold=case.fold,
        attempt=case.attempt, dependency_warmup_bars=5)


def _axes(frame):
    return (pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce")),
        pd.DatetimeIndex(pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")))


@pytest.mark.parametrize("missing_close", (None, 6), ids=("utc", "nat-scored-row"))
def test_fold_reuses_full_frame_axes_and_preserves_exact_partitions(monkeypatch, missing_close):
    case = _case(missing_close=missing_close)
    original_frame = case.frame.copy(deep=True)
    expected = [execution._partition_slice(case.frame, **arguments) for arguments in case.arguments]
    original_convert, original_slice = pd.to_datetime, execution._partition_slice
    full_conversions, shared_axes = [], []

    def convert(values, *args, **kwargs):
        if values is case.frame.index:
            full_conversions.append("opens")
        elif (isinstance(values, pd.Series) and values.name == "_BAR_CLOSE_TS"
                and len(values) == len(case.frame)):
            full_conversions.append("closes")
        return original_convert(values, *args, **kwargs)

    def partition(*args, **kwargs):
        shared_axes.append(kwargs["_full_frame_time_axes"])
        return original_slice(*args, **kwargs)

    monkeypatch.setattr(execution.pd, "to_datetime", convert)
    monkeypatch.setattr(execution, "_partition_slice", partition)
    actual = _fold(case)
    assert full_conversions == ["opens", "closes"]
    assert len(shared_axes) == 3 and all(axes is shared_axes[0] for axes in shared_axes)
    for received, wanted in zip(actual, expected, strict=True):
        pd.testing.assert_frame_equal(received.frame, wanted.frame, check_exact=True)
        assert received.geometry_receipt == wanted.geometry_receipt
        assert (received.role, received.warmup_bars, received.scored_start_utc,
            received.end_exclusive_utc) == (wanted.role, wanted.warmup_bars,
            wanted.scored_start_utc, wanted.end_exclusive_utc)
    pd.testing.assert_frame_equal(case.frame, original_frame, check_exact=True)


def test_shared_axes_keep_naive_index_refusals():
    case = _case(naive_index=True)
    axes = _axes(case.frame)
    for extra in ({}, {"_full_frame_time_axes": axes}):
        with pytest.raises(TypeError):
            execution._partition_slice(case.frame, **case.arguments[0], **extra)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as refused:
        _fold(case)
    assert refused.value.code == "PARTITION_SCORE_START_INVALID"


def test_shared_axes_keep_nat_endpoint_refusal():
    case = _case(missing_close=29)
    axes = _axes(case.frame)
    calls = (
        lambda: execution._partition_slice(case.frame, **case.arguments[-1]),
        lambda: execution._partition_slice(case.frame, **case.arguments[-1],
            _full_frame_time_axes=axes),
        lambda: _fold(case),
    )
    for call in calls:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as refused:
            call()
        assert refused.value.code == "PARTITION_AXIS_NOT_EXACTLY_BACKED"
