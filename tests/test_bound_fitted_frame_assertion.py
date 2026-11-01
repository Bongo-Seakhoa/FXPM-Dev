"""Synthetic regression for the genuine-helper frame assertion.

Invented owned frames only. The target source is parsed, never imported. This
isolates the existing helper's contiguous assertion block; it does not exercise
native reopening, actual saved fits, source custody, or the full genuine proof.
"""

import ast
import copy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


HELPER_SOURCE = Path(__file__).with_name("test_bound_fitted_reopen.py")
HELPER_FUNCTION = "_assert_genuine_declared_bound_fit_roundtrip"


def _attribute_path(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _attribute_path(node.value)
        return parent + "." + node.attr if parent else None
    return None


@pytest.fixture(scope="module")
def actual_frame_assertion():
    source_bytes = HELPER_SOURCE.read_bytes()
    tree = ast.parse(source_bytes, filename=str(HELPER_SOURCE))
    matches = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == HELPER_FUNCTION
    ]
    assert len(matches) == 1
    statements = matches[0].body
    starts = [
        index for index, node in enumerate(statements)
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and _attribute_path(node.value.func) == "pd.testing.assert_frame_equal"
    ]
    assert len(starts) == 1
    block = statements[starts[0]:starts[0] + 3]
    assert len(block) == 3
    frame_call, column_loop, index_assert = block
    assert isinstance(column_loop, ast.For)
    assert _attribute_path(column_loop.iter) == "original_fitted._frame"
    assert isinstance(index_assert, ast.Assert)
    assert isinstance(index_assert.test, ast.Compare)
    assert isinstance(index_assert.test.left, ast.Call)
    assert _attribute_path(index_assert.test.left.func) == "original_fitted._frame.index.asi8.tobytes"
    assert len(index_assert.test.comparators) == 1
    comparator = index_assert.test.comparators[0]
    assert isinstance(comparator, ast.Call)
    assert _attribute_path(comparator.func) == "actual._frame.index.asi8.tobytes"

    # Preserve the target statements exactly; only supply their two parameters.
    function = ast.FunctionDef(
        name="_selected_actual_frame_assertion",
        args=ast.arguments(
            posonlyargs=[], args=[ast.arg(arg="original_fitted"), ast.arg(arg="actual")],
            vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[],
        ),
        body=copy.deepcopy(block),
        decorator_list=[],
    )
    isolated = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    assert not any(isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(isolated))
    namespace = {"pd": pd}
    exec(compile(isolated, str(HELPER_SOURCE) + "::selected-frame-assertions", "exec"), namespace)
    selected = namespace["_selected_actual_frame_assertion"]

    def compare(left, right):
        return selected(SimpleNamespace(_frame=left), SimpleNamespace(_frame=right))

    return compare


def _invented_frame(temporal_kind="aware"):
    index = pd.date_range("2000-02-01", periods=4, tz="UTC", name="invented_sample_utc")
    if temporal_kind == "timedelta":
        temporal = pd.to_timedelta([0, None, 2, 3], unit="ns")
    else:
        temporal = pd.to_datetime([
            "2000-01-01T00:00:00.000000001Z", None,
            "2000-01-01T00:00:00.000000003Z",
            "2000-01-01T00:00:00.000000004Z",
        ], utc=True)
        if temporal_kind == "naive":
            temporal = temporal.tz_localize(None)
    numeric_bits = np.array([
        0x0000000000000000, 0x8000000000000000,
        0x7FF8000000000042, 0x3FF0000000000000,
    ], dtype=np.uint64)
    return pd.DataFrame({
        "invented_event_time": temporal.copy(),
        "invented_numeric": numeric_bits.view(np.float64).copy(),
    }, index=index)


@pytest.mark.parametrize("temporal_kind", ["aware", "naive", "timedelta"])
def test_equal_independently_allocated_temporal_frames_with_nat_are_accepted(
        actual_frame_assertion, temporal_kind):
    left = _invented_frame(temporal_kind)
    right = _invented_frame(temporal_kind)
    assert left is not right
    assert left["invented_event_time"].array is not right["invented_event_time"].array
    assert not np.shares_memory(
        left["invented_event_time"].array.asi8,
        right["invented_event_time"].array.asi8,
    )
    assert not np.shares_memory(
        left["invented_numeric"].to_numpy(), right["invented_numeric"].to_numpy(),
    )
    assert left["invented_event_time"].isna().tolist() == [False, True, False, False]
    actual_frame_assertion(left, right)


@pytest.mark.parametrize("mutation", [
    "timestamp_value", "nat_value", "temporal_naive_dtype", "temporal_timezone_dtype",
    "numeric_value", "numeric_dtype", "index_value", "index_timezone", "index_name",
])
def test_temporal_value_dtype_and_index_changes_are_rejected(actual_frame_assertion, mutation):
    left = _invented_frame()
    right = _invented_frame()
    if mutation == "timestamp_value":
        right.iloc[0, right.columns.get_loc("invented_event_time")] += pd.Timedelta(1, unit="ns")
    elif mutation == "nat_value":
        right.iloc[1, right.columns.get_loc("invented_event_time")] = pd.Timestamp("2000-01-01", tz="UTC")
    elif mutation == "temporal_naive_dtype":
        right["invented_event_time"] = right["invented_event_time"].dt.tz_localize(None)
    elif mutation == "temporal_timezone_dtype":
        right["invented_event_time"] = right["invented_event_time"].dt.tz_convert("Europe/Budapest")
    elif mutation == "numeric_value":
        right.iloc[3, right.columns.get_loc("invented_numeric")] = 2.0
    elif mutation == "numeric_dtype":
        right["invented_numeric"] = right["invented_numeric"].astype(np.float32)
    elif mutation == "index_value":
        right.index = pd.DatetimeIndex(
            [right.index[0] + pd.Timedelta(1, unit="ns"), *right.index[1:]],
            name=right.index.name,
        )
    elif mutation == "index_timezone":
        right.index = right.index.tz_convert("Europe/Budapest")
    else:
        right.index = right.index.rename("invented_changed_index_name")
    with pytest.raises(AssertionError):
        actual_frame_assertion(left, right)


@pytest.mark.parametrize("mutation", ["signed_zero", "nan_payload"])
def test_numeric_bits_remain_stricter_than_pandas_semantic_equality(actual_frame_assertion, mutation):
    left = _invented_frame()
    right = _invented_frame()
    changed_bits = right["invented_numeric"].to_numpy(copy=True).view(np.uint64)
    if mutation == "signed_zero":
        changed_bits[0] ^= np.uint64(0x8000000000000000)
    else:
        changed_bits[2] ^= np.uint64(1)
    right["invented_numeric"] = changed_bits.view(np.float64)
    pd.testing.assert_frame_equal(left, right, check_exact=True, check_dtype=True)
    with pytest.raises(AssertionError):
        actual_frame_assertion(left, right)
