"""INVENTED 24-row geometry and callback fixtures; no source/model/financial authority."""
import copy
from types import SimpleNamespace
import sys

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution


_ROLES = ("optimizer_train", "tuning_validation", "final_lockbox")


def _case():
    index = pd.date_range("2026-01-01T00:00:00Z", periods=24, freq="h")
    frame = pd.DataFrame({"Open": [100.0 + i / 100 for i in range(24)],
        "High": [101.0 + i / 100 for i in range(24)],
        "Low": [99.0 + i / 100 for i in range(24)],
        "Close": [100.5 + i / 100 for i in range(24)],
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1), "REGIME_LIVE": "TREND"}, index=index)
    folds = []
    for ordinal in range(4):
        end = 10 + ordinal * 2
        folds.append({"fold_id": "INVENTED-" + str(ordinal),
            "fit_start_utc": index[0].isoformat(), "fit_end_exclusive_utc": index[end].isoformat(),
            "validation_warmup_start_utc": index[end-2].isoformat(),
            "validation_start_utc": index[end].isoformat(),
            "validation_end_exclusive_utc": index[end+4].isoformat()})
    attempt = SimpleNamespace(ordinal=0,
        artifacts=SimpleNamespace(refit_recipe={"data_requirements": {"warmup_bars": 2}}))
    return SimpleNamespace(frame=frame, folds=folds, attempt=attempt)


def _get(case, templates, *, ordinal=0, fold=None, dependency=2, role=None):
    return execution._route_a_owner_fold_partitions(frame=case.frame,
        fold=case.folds[ordinal] if fold is None else fold, attempt=case.attempt,
        dependency_warmup_bars=dependency, fold_ordinal=ordinal, templates=templates,
        partition_role=role)


def _same(actual, expected):
    assert (actual.role, actual.warmup_bars, actual.scored_start_utc, actual.end_exclusive_utc) == (
        expected.role, expected.warmup_bars, expected.scored_start_utc, expected.end_exclusive_utc)
    pd.testing.assert_frame_equal(actual.frame, expected.frame, check_exact=True)
    assert actual.geometry_receipt == expected.geometry_receipt
    geometry = actual.geometry_receipt
    assert execution._sha256({k: v for k, v in geometry.items() if k != "receipt_sha256"}) == geometry["receipt_sha256"]


def _watch(monkeypatch):
    original, calls = execution._fold_partitions, []

    def derive(**kwargs):
        calls.append((kwargs["attempt"], kwargs["frame"], dict(kwargs["fold"]), kwargs["dependency_warmup_bars"]))
        return original(**kwargs)

    monkeypatch.setattr(execution, "_fold_partitions", derive)
    return original, calls


def test_twenty_four_request_partitions_match_uncached_real_geometry(monkeypatch):
    case = _case()
    original, calls = _watch(monkeypatch)
    expected = [original(frame=case.frame, fold=fold, attempt=case.attempt,
        dependency_warmup_bars=2) for fold in case.folds]
    specifications = [(ordinal, role, tier) for ordinal in range(4)
        for role in _ROLES for tier in ("P75", "P95")]
    templates, seen = {}, {}
    assert len(specifications) == 24
    for ordinal, role, _tier in specifications:
        actual = _get(case, templates, ordinal=ordinal, role=role)
        wanted = next(value for value in expected[ordinal] if value.role == role)
        _same(actual, wanted)
        if (ordinal, role) in seen:
            assert actual.frame is not seen[(ordinal, role)].frame
            assert actual.geometry_receipt is not seen[(ordinal, role)].geometry_receipt
        seen[(ordinal, role)] = actual
        assert len(templates["folds"]) <= 4
    assert len(calls) == 4 and set(templates["folds"]) == {0, 1, 2, 3}


def test_cached_single_and_triple_partition_returns_are_detached(monkeypatch):
    case = _case()
    source = case.frame.copy(deep=True)
    original, calls = _watch(monkeypatch)
    expected = original(frame=case.frame, fold=case.folds[0], attempt=case.attempt,
        dependency_warmup_bars=2)
    templates = {}
    first = _get(case, templates)
    first[0].frame.iloc[0, first[0].frame.columns.get_loc("Open")] = -999.0
    first[0].geometry_receipt["frame_rows"] = -1
    first[0].geometry_receipt["INVENTED_MUTATION"] = ["caller"]
    single = _get(case, templates, role="optimizer_train")
    _same(single, expected[0])
    single.frame.iloc[0, single.frame.columns.get_loc("Close")] = -777.0
    single.geometry_receipt["role"] = "INVENTED_MUTATION"
    fresh = _get(case, templates)
    for actual, wanted in zip(fresh, expected, strict=True):
        _same(actual, wanted)
    pd.testing.assert_frame_equal(case.frame, source, check_exact=True)
    assert len(calls) == 1


def test_every_bound_and_both_warmups_control_template_reuse(monkeypatch):
    case = _case()
    original, calls = _watch(monkeypatch)
    changes = {"fit_start_utc": case.frame.index[1].isoformat(),
        "fit_end_exclusive_utc": case.frame.index[9].isoformat(),
        "validation_warmup_start_utc": case.frame.index[7].isoformat(),
        "validation_start_utc": case.frame.index[11].isoformat(),
        "validation_end_exclusive_utc": case.frame.index[13].isoformat()}
    for field, value in changes.items():
        templates = {}
        before = len(calls)
        _get(case, templates)
        fold = dict(case.folds[0], **{field: value})
        actual = _get(case, templates, fold=fold)
        expected = original(frame=case.frame, fold=fold, attempt=case.attempt, dependency_warmup_bars=2)
        for a, b in zip(actual, expected, strict=True):
            _same(a, b)
        assert len(calls) == before + 2
    templates = {}
    case.attempt.artifacts.refit_recipe["data_requirements"]["warmup_bars"] = 1
    before = len(calls)
    first = _get(case, templates, dependency=2)
    case.attempt.artifacts.refit_recipe["data_requirements"]["warmup_bars"] = 2
    second = _get(case, templates, dependency=1)
    assert len(calls) == before + 2
    for a, b in zip(first, second, strict=True):
        _same(a, b)
    # True must not alias the already cached integer dependency=1.
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as cold:
        original(frame=case.frame, fold=case.folds[0], attempt=case.attempt, dependency_warmup_bars=True)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as cached:
        _get(case, templates, dependency=True)
    assert cached.value.code == cold.value.code == "DEPENDENCY_WARMUP_INVALID"
    # Legacy recipe coercion remains uncached, preserving the original implementation.
    case.attempt.artifacts.refit_recipe["data_requirements"]["warmup_bars"] = True
    before = len(calls)
    for _ in range(2):
        actual = _get(case, templates, dependency=1)
        expected = original(frame=case.frame, fold=case.folds[0], attempt=case.attempt, dependency_warmup_bars=1)
        for a, b in zip(actual, expected, strict=True):
            _same(a, b)
    assert len(calls) == before + 2


def test_templates_evict_on_actual_attempt_or_frame_replacement(monkeypatch):
    case = _case()
    original, calls = _watch(monkeypatch)
    templates = {}
    for ordinal in range(4):
        _get(case, templates, ordinal=ordinal)
    assert len(calls) == 4 and len(templates["folds"]) == 4
    first_attempt = case.attempt
    case.attempt = copy.deepcopy(case.attempt)
    assert case.attempt.ordinal == first_attempt.ordinal and case.attempt is not first_attempt
    _get(case, templates)
    assert len(calls) == 5 and len(templates["folds"]) == 1
    assert templates["attempt"] is case.attempt
    previous = case.frame
    case.frame = case.frame.copy(deep=True)
    case.frame.loc[case.frame.index[0], ["Open", "High", "Low", "Close"]] += 0.125
    actual = _get(case, templates)
    assert len(calls) == 6 and len(templates["folds"]) == 1
    assert templates["frame"] is case.frame and templates["frame"] is not previous
    expected = original(frame=case.frame, fold=case.folds[0], attempt=case.attempt, dependency_warmup_bars=2)
    for a, b in zip(actual, expected, strict=True):
        _same(a, b)


def test_real_continuation_decodes_private_snapshot_once_and_detaches_public_projection(tmp_path, monkeypatch):
    # Reuse the existing labelled one-period callback fixture; no source/model is run.
    from test_authoritative_search_execution import _attempt_boundary_invented_continuation
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch)
    snapshotter = execution._route_a_owner_membership_snapshot
    rebind = execution._route_a_rebind_full_panel_owner_membership
    prepare = execution._FullPanelOwnerPreparation.prepare_owned
    loads = execution.json.loads
    encoded, decodes, private = set(), [], []

    def snapshot(**kwargs):
        value = snapshotter(**kwargs)
        encoded.add(execution._canonical_bytes(value).decode("ascii"))
        return value

    def decode(value, *args, **kwargs):
        if (type(value) is str and value in encoded
                and sys._getframe(1).f_globals.get("__name__") == execution.__name__):
            decodes.append(value)
        return loads(value, *args, **kwargs)

    def rebound(**kwargs):
        private.append(kwargs["snapshot"])
        return rebind(**kwargs)

    def prepared(self, **kwargs):
        result = prepare(self, **kwargs)
        assert private and all(value is private[0] for value in private)
        public = kwargs["owner_snapshot"]
        assert public is not private[0]
        public["INVENTED_PUBLIC_MUTATION"] = ["detached"]
        assert "INVENTED_PUBLIC_MUTATION" not in private[0]
        return result

    monkeypatch.setattr(execution, "_route_a_owner_membership_snapshot", snapshot)
    monkeypatch.setattr(execution, "_route_a_rebind_full_panel_owner_membership", rebound)
    monkeypatch.setattr(execution._FullPanelOwnerPreparation, "prepare_owned", prepared)
    monkeypatch.setattr(execution.json, "loads", decode)
    result = case.run()
    assert type(result) is execution._PreparedRouteAFullPanelStage and len(case.staged) == 1
    assert len(encoded) == len(decodes) == 1
    assert sum(row[0] == "probe" for row in case.events) == 24
    assert sum(row[0] == "run" for row in case.events) == 24
    assert case.reads == {"config": 2, "stream": 4}
