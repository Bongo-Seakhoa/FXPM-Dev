"""SYNTHETIC_FITTED_RECIPE_DATA_ONLY_NO_NATIVE_OR_CASE005_PROOF.

Actual Core feature math/direct-override validation and an actual strategy's
request method use invented OHLCV. RecipeDecisionUnitData is ordinary metadata,
not a book verifier or authenticated fit/source capability. Cache I/O is mocked;
no broker terms, Native reader, fit/predict/source API or issuer is exercised.
"""
import ast
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "pm_authoritative_search_execution.py"


class RecipeDecisionUnitData:
    """Held recipe-hash DATA only; does not authenticate or verify any source."""
    def __init__(self, sha256):
        self.sha256 = sha256
    def to_dict(self):
        return {"regime_params_sha256": self.sha256}


def definitions():
    import numpy as np
    import pandas as pd
    import pm_core
    import pm_regime
    tree = ast.parse(SOURCE.read_bytes(), filename=str(SOURCE))
    names = {"AuthoritativeSearchExecutionError", "_fail", "_canonical_bytes", "_frame_sha256",
        "_default_regime_parameters", "_FullPrefixStrategySurface",
        "_compute_fitted_strategy_features", "_build_full_prefix_strategy_surface"}
    selected = [node for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual fitted feature helpers missing")
    regime_columns = next(node for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(isinstance(target, ast.Name) and target.id == "_REGIME_COLUMNS"
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])))
    scope = {"pd": pd, "np": np, "pm_regime": pm_regime, "json": json, "hashlib": hashlib,
        "dataclass": dataclass, "FeatureComputer": pm_core.FeatureComputer,
        "__name__": __name__, "__file__": str(SOURCE)}
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, regime_columns, *selected], type_ignores=[])), str(SOURCE), "exec"), scope)
    return scope


def owned_fit_recipe_data(h):
    import numpy as np
    import pandas as pd
    import pm_regime
    axis = pd.date_range("2020-01-02T00:00:00Z", periods=240, freq="h")
    close = 100.0 + np.arange(240) * 0.03 + np.sin(np.arange(240) / 9.0)
    raw = pd.DataFrame({"Open": close-0.05, "High": close+0.2, "Low": close-0.2,
        "Close": close, "Volume": np.arange(240, dtype="int64")+100}, index=axis)
    raw["_BAR_CLOSE_TS"] = axis + pd.Timedelta(hours=1)
    parameters = h["_default_regime_parameters"]("H1")
    params_sha = pm_regime.regime_params_sha256(parameters)
    surface = pm_regime.MarketRegimeDetector(
        pm_regime.RegimeParams.from_dict(parameters)).compute_regime_scores(raw)
    frame = raw.copy(deep=True)
    for name in h["_REGIME_COLUMNS"]:
        frame.loc[:, name] = surface[name]
    fitted_data = SimpleNamespace(regime_parameters=deepcopy(parameters),
        regime_fit_decision=RecipeDecisionUnitData(params_sha))
    return frame, fitted_data


@contextmanager
def isolated_cache_io():
    import pm_core
    fc = pm_core.FeatureComputer
    # Numeric feature methods remain real. No real memory/disk cache is read or written.
    with patch.object(fc, "_cache_get", return_value=None) as get, \
         patch.object(fc, "_cache_put", return_value=None) as put:
        yield get, put


class FittedRegimeFeatureBindingTests(unittest.TestCase):
    def test_actual_core_and_strategy_preserve_held_recipe_with_competing_lookup(self):
        import pm_core
        import pm_regime
        from pm_strategies import RegimeTransitionBreakoutStrategy
        h = definitions()
        frame, fitted = owned_fit_recipe_data(h)
        original = frame.copy(deep=True)
        original_parameters = deepcopy(fitted.regime_parameters)
        original_hash = h["_frame_sha256"](frame)
        alternate = deepcopy(fitted.regime_parameters)
        alternate["adx_trend_threshold"] += 1.0  # Invented competing UNIT data, not policy.
        self.assertNotEqual(pm_regime.regime_params_sha256(alternate),
            fitted.regime_fit_decision.to_dict()["regime_params_sha256"])
        for unavailable in (True, False):
            strategy = RegimeTransitionBreakoutStrategy()
            with self.subTest(competing_lookup_unavailable=unavailable), isolated_cache_io() as (get, put), \
                 patch.object(pm_regime, "load_regime_params",
                    side_effect=AssertionError("UNBOUND_LOOKUP_FORBIDDEN") if unavailable else None,
                    return_value=pm_regime.RegimeParams.from_dict(alternate)) as lookup, \
                 patch.object(pm_core.FeatureComputer, "_regime_params_fingerprint",
                    side_effect=AssertionError("FILE_FINGERPRINT_FORBIDDEN")), \
                 patch.object(pm_core.FeatureComputer, "compute_all",
                    wraps=pm_core.FeatureComputer.compute_all) as compute, \
                 patch.object(strategy, "get_feature_request", wraps=strategy.get_feature_request) as request:
                features = h["_compute_fitted_strategy_features"](frame=frame, strategy=strategy,
                    fitted=fitted, symbol="SYNTHETIC", timeframe="H1")
                self.assertEqual(request.call_count, 1)
                self.assertEqual(compute.call_count, 1)
                self.assertEqual(lookup.call_count, 0)
                self.assertEqual(compute.call_args.kwargs["regime_params_override"], fitted.regime_parameters)
                self.assertEqual(compute.call_args.kwargs["regime_params_sha256"],
                    fitted.regime_fit_decision.to_dict()["regime_params_sha256"])
                self.assertIs(get.call_args.kwargs["allow_disk"], False)
                self.assertIs(put.call_args.kwargs["allow_disk"], False)
                self.assertTrue(features.loc[:, list(frame.columns)].equals(frame))
                self.assertEqual(h["_frame_sha256"](features.loc[:, list(frame.columns)]), original_hash)
                self.assertTrue(frame.equals(original))
                self.assertEqual(h["_frame_sha256"](frame), original_hash)
                self.assertEqual(fitted.regime_parameters, original_parameters)

    def test_original_no_request_nonregime_and_none_dispatch_and_request_counts(self):
        import pm_core
        import pandas as pd
        h = definitions()
        frame = pd.DataFrame({"Open": [2.0]*40, "High": [3.0]*40, "Low": [1.0]*40,
            "Close": [2.0]*40, "Volume": [10]*40},
            index=pd.date_range("2020-01-02T00:00:00Z", periods=40, freq="h"))
        class EmptyRequestUnitData:
            calls = 0
            def get_feature_request(self):
                self.calls += 1
                return set()
            def get_required_features(self):
                raise AssertionError("fallback must not be called")
        strategy = EmptyRequestUnitData()
        with patch.object(pm_core.FeatureComputer, "compute_all", side_effect=AssertionError("no full call")), \
             patch.object(pm_core.FeatureComputer, "compute_required", side_effect=AssertionError("no required call")):
            copy = h["_compute_fitted_strategy_features"](frame=frame, strategy=strategy,
                fitted=None, symbol="SYNTHETIC", timeframe="H1")
        self.assertEqual(strategy.calls, 1)
        self.assertIsNot(copy, frame)
        self.assertTrue(copy.equals(frame))
        class RequiredUnitData:
            calls = 0
            def get_required_features(self):
                self.calls += 1
                return {"EMA_5"}
            def calculate_stops(self):
                raise AssertionError("only feature-request presence is inspected")
        strategy = RequiredUnitData()
        with isolated_cache_io(), \
             patch.object(pm_core.FeatureComputer, "_regime_params_fingerprint",
                return_value=("SYNTHETIC_NO_CONFIG_READ",)), \
             patch.object(pm_core.FeatureComputer, "compute_required",
                wraps=pm_core.FeatureComputer.compute_required) as required, \
             patch.object(pm_core.FeatureComputer, "compute_all", side_effect=AssertionError("no full call")):
            features = h["_compute_fitted_strategy_features"](frame=frame, strategy=strategy,
                fitted=None, symbol="SYNTHETIC", timeframe="H1")
        self.assertEqual(strategy.calls, 1)
        self.assertEqual(required.call_count, 1)
        self.assertEqual(required.call_args.args[1], {"EMA_5", "ATR_14"})
        self.assertTrue({"EMA_5", "ATR_14"}.issubset(features.columns))
        self.assertTrue(features.loc[:, list(frame.columns)].equals(frame))
        parameters = h["_default_regime_parameters"]("H1")
        import pm_regime
        fitted = SimpleNamespace(regime_parameters=parameters,
            regime_fit_decision=RecipeDecisionUnitData(pm_regime.regime_params_sha256(parameters)))
        with patch.object(pm_core.FeatureComputer, "compute_all", return_value=frame) as full:
            self.assertIs(h["_compute_fitted_strategy_features"](frame=frame, strategy=None,
                fitted=fitted, symbol="SYNTHETIC", timeframe="H1"), frame)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(full.call_args.kwargs["regime_params_sha256"],
            fitted.regime_fit_decision.to_dict()["regime_params_sha256"])

    def test_existing_core_override_validation_refuses_recipe_hash_mismatch(self):
        import pm_core
        import pandas as pd
        h = definitions()
        frame = pd.DataFrame({"Close": [1.0]}, index=pd.date_range("2020-01-02", periods=1, tz="UTC"))
        parameters = h["_default_regime_parameters"]("H1")
        fitted = SimpleNamespace(regime_parameters=parameters,
            regime_fit_decision=RecipeDecisionUnitData("0"*64))
        class RegimeRequestUnitData:
            def get_feature_request(self):
                return {"REGIME_LIVE"}
        original = frame.copy(deep=True)
        with patch.object(pm_core.FeatureComputer, "_append_regime_features",
                side_effect=AssertionError("must reject before calculation")) as append:
            with self.assertRaisesRegex(ValueError, "does not match the canonical override"):
                h["_compute_fitted_strategy_features"](frame=frame, strategy=RegimeRequestUnitData(),
                    fitted=fitted, symbol="SYNTHETIC", timeframe="H1")
        self.assertEqual(append.call_count, 0)
        self.assertTrue(frame.equals(original))

    def test_original_guard_still_refuses_changed_values_and_dtype_without_restoration(self):
        import pm_core
        import pandas as pd
        h = definitions()
        frame = pd.DataFrame({"Close": [2.0, 3.0],
            "REGIME_LIVE": pd.Categorical(["TREND", "RANGE"])},
            index=pd.date_range("2020-01-02", periods=2, freq="h", tz="UTC"))
        class RegimeRequestUnitData:
            calls = 0
            signals = 0
            def get_feature_request(self):
                self.calls += 1
                return {"REGIME_LIVE"}
            def generate_signals(self, frame, symbol):
                self.signals += 1
                raise AssertionError("signals cannot follow an axis refusal")
        changed_value = frame.copy(deep=True)
        changed_value.loc[:, "Close"] += 1
        changed_dtype = frame.copy(deep=True)
        changed_dtype["REGIME_LIVE"] = changed_dtype["REGIME_LIVE"].astype(object)
        fitted = SimpleNamespace(regime_parameters={},
            regime_fit_decision=RecipeDecisionUnitData("0"*64))
        for number, output in enumerate((changed_value, changed_dtype)):
            strategy = RegimeRequestUnitData()
            with self.subTest(injected_feature_output=number), \
                 patch.object(pm_core.FeatureComputer, "compute_all", return_value=output) as compute:
                with self.assertRaises(h["AuthoritativeSearchExecutionError"]) as refused:
                    h["_build_full_prefix_strategy_surface"](frame=frame, strategy=strategy,
                        fitted=fitted, symbol="SYNTHETIC", timeframe="H1")
            self.assertEqual(refused.exception.code, "FULL_PREFIX_FEATURE_AXIS_CHANGED")
            self.assertEqual(refused.exception.details, ())
            self.assertEqual(strategy.calls, 1)
            self.assertEqual(strategy.signals, 0)
            self.assertEqual(compute.call_count, 1)
            self.assertIs(compute.return_value, output)


if __name__ == "__main__":
    unittest.main()
