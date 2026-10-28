from __future__ import annotations

import copy
import ast
import inspect
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pytest

import pm_pipeline as pipeline
from pm_current_refit_artifacts import (
    CurrentRefitArtifactError,
    _normalized_parameters,
    build_current_refit_search_artifacts_v2,
    canonical_artifact_bytes,
)
from pm_current_refit_parameters import (
    CurrentRefitParameterCodecError,
    canonical_parameter_bytes,
    canonicalize_parameter_mapping,
    decode_parameter_mapping,
    build_parameter_domain,
    validate_parameter_domain_member,
    parameter_domain_point,
    parameter_domain_default_ordinal,
    iter_parameter_domain_projection,
    load_retained_strategy_domain_v1,
    decode_retained_parameter_metadata,
)
from pm_strategies import StrategyRegistry


REGIME_STRATEGY = "RegimeTransitionBreakoutStrategy"


def _regime_artifacts(parameters):
    return build_current_refit_search_artifacts_v2(
        symbol="EURUSD",
        timeframe="H1",
        regime="BREAKOUT",
        strategy_id=REGIME_STRATEGY,
        parameters=parameters,
        lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=1_000,
        fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-02T00:00:00Z",
        latest_closed_bar_utc="2026-01-01T23:00:00Z",
        source_snapshot_sha256="a" * 64,
        source_bar_count=1_000,
        seed=20260714,
    )


def test_all_registered_defaults_and_grid_choices_round_trip_exactly() -> None:
    names = StrategyRegistry.list_all()
    assert names
    for strategy_id in names:
        base = StrategyRegistry.get(strategy_id)
        defaults = base.get_default_params()
        candidates = [defaults]
        domain = build_parameter_domain(defaults, base.get_param_grid(),
            coupled_groups=base.get_param_couplings())
        for axis in domain["axes"]:
            for row in axis["choices"]:
                candidate = copy.deepcopy(defaults)
                candidate.update(zip(axis["names"], row))
                candidates.append(candidate)

        for candidate in candidates:
            wire = _normalized_parameters(strategy_id, candidate)
            assert wire == pipeline._current_refit_json_parameters(candidate)
            runtime = decode_parameter_mapping(wire, defaults)
            fitted = StrategyRegistry.get(strategy_id, **runtime)
            assert canonical_parameter_bytes(fitted.get_params()) == canonical_parameter_bytes(wire)
            for name, value in runtime.items():
                if type(defaults[name]) in (tuple, list):
                    assert type(value) is type(defaults[name])


def test_regime_transition_tuple_and_list_inputs_have_one_artifact_identity() -> None:
    tuple_parameters = StrategyRegistry.get(REGIME_STRATEGY).get_params()
    list_parameters = copy.deepcopy(tuple_parameters)
    list_parameters["source_regimes"] = list(list_parameters["source_regimes"])
    list_parameters["target_regimes"] = list(list_parameters["target_regimes"])

    from_tuple = _regime_artifacts(tuple_parameters)
    from_list = _regime_artifacts(list_parameters)
    assert from_tuple == from_list
    assert canonical_parameter_bytes(tuple_parameters) == canonical_parameter_bytes(list_parameters)
    assert from_tuple.trial_definition["parameters"]["source_regimes"] == ["CHOP", "RANGE"]
    assert from_tuple.trial_definition["parameters"]["target_regimes"] == ["BREAKOUT"]

    _, process_sha = pipeline._current_refit_validate_process(
        from_tuple.winner_process,
        strategy_id=REGIME_STRATEGY,
    )
    request = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "fit_start_utc": from_tuple.horizon_process["fit_start_utc"],
        "fit_end_utc": from_tuple.horizon_process["fit_end_utc"],
        "latest_closed_bar_utc": from_tuple.horizon_process["latest_closed_bar_utc"],
    }
    _, horizon_sha = pipeline._current_refit_validate_horizon(
        from_tuple.horizon_process,
        request=request,
        source_sha256="a" * 64,
        source_bar_count=1_000,
    )
    trial, wire = pipeline._current_refit_validate_trial(
        from_tuple.trial_definition,
        strategy_id=REGIME_STRATEGY,
        process_sha256=process_sha,
        horizon_sha256=horizon_sha,
    )
    runtime = decode_parameter_mapping(
        wire, StrategyRegistry.get(REGIME_STRATEGY).get_default_params()
    )
    assert type(runtime["source_regimes"]) is tuple
    assert type(runtime["target_regimes"]) is tuple
    assert StrategyRegistry.get(REGIME_STRATEGY, **runtime).get_params() == tuple_parameters
    assert canonical_artifact_bytes(trial) == pipeline._current_refit_canonical_bytes(trial)


@pytest.mark.parametrize(
    "parameters",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": float("-inf")},
        {"value": 2**63},
        {"value": -(2**63) - 1},
        {"value": []},
        {"value": ["x"] * 65},
        {"value": [["nested"]]},
        {"value": ("x", ("nested",))},
        {"value": {"nested": "mapping"}},
        {"value": {"set"}},
        {"value": "\ud800"},
        {1: "non-string-key"},
    ],
)
def test_hostile_parameter_values_fail_closed(parameters) -> None:
    with pytest.raises(CurrentRefitParameterCodecError):
        canonicalize_parameter_mapping(parameters)


@pytest.mark.parametrize(
    ("wire", "templates"),
    [
        ({"period": [20]}, {"period": 20}),
        ({"regimes": "BREAKOUT"}, {"regimes": ("BREAKOUT",)}),
        ({"regimes": [1]}, {"regimes": ("BREAKOUT",)}),
        ({"regimes": ("BREAKOUT",)}, {"regimes": ("BREAKOUT",)}),
        ({"ratio": 2}, {"ratio": 2.0}),
        ({"ratio": 2.0}, {"ratio": 2}),
        ({"ratio": -0.0}, {"ratio": 0.0}),
        ({"unknown": 1}, {"period": 1}),
    ],
)
def test_wire_shape_and_scalar_type_splices_fail_closed(wire, templates) -> None:
    with pytest.raises(CurrentRefitParameterCodecError):
        decode_parameter_mapping(wire, templates)


def test_regime_sequence_normalization_and_non_grid_shapes_are_rejected() -> None:
    parameters = StrategyRegistry.get(REGIME_STRATEGY).get_params()
    lower_case = copy.deepcopy(parameters)
    lower_case["target_regimes"] = ["breakout"]
    with pytest.raises(CurrentRefitArtifactError, match="PARAMETERS_NOT_FULL_CANONICAL_SET"):
        _regime_artifacts(lower_case)

    outside_grid = copy.deepcopy(parameters)
    outside_grid["source_regimes"] = ["CHOP", "TREND"]
    with pytest.raises(CurrentRefitArtifactError, match="PARAMETER_VALUE_OUTSIDE_PROCESS"):
        _regime_artifacts(outside_grid)


def test_numpy_grid_scalars_and_key_order_have_stable_bytes() -> None:
    left = {"period": np.int64(20), "ratio": np.float64(2.0)}
    right = {"ratio": 2.0, "period": 20}
    assert canonical_parameter_bytes(left) == canonical_parameter_bytes(right)
    assert canonicalize_parameter_mapping(left) == {"period": 20, "ratio": 2.0}


def _small_coupled_domain():
    return build_parameter_domain(
        {"fast": 12, "slow": 26, "signal": 9, "fixed": True},
        {"fast": [8, 12, 16], "slow": [21, 26, 34], "signal": [7, 9]},
        coupled_groups=({"names": ("fast", "slow"),
                         "choices": ((8, 21), (12, 26), (16, 34))},))


def test_coupled_domain_uses_legal_rows_and_flat_wire_only():
    domain = _small_coupled_domain()
    assert domain["point_count"] == 6
    points = [parameter_domain_point(domain, ordinal) for ordinal in range(6)]
    assert {(p["fast"], p["slow"]) for p in points} == {(8, 21), (12, 26), (16, 34)}
    assert all(set(point) == {"fast", "slow", "signal", "fixed"} for point in points)
    assert len({canonical_parameter_bytes(point) for point in points}) == 6
    assert parameter_domain_point(domain, parameter_domain_default_ordinal(domain)) == domain["defaults"]
    for point in points:
        assert validate_parameter_domain_member(domain, point) == point
    for invalid_ordinal in (-1, 6, True, "0"):
        with pytest.raises(CurrentRefitParameterCodecError, match="PARAMETER_DOMAIN_ORDINAL_INVALID"):
            parameter_domain_point(domain, invalid_ordinal)
    for invalid in (dict(points[0], slow=34), dict(points[0], fixed=False),
                    {key: value for key, value in points[0].items() if key != "fixed"},
                    dict(points[0], extra=1)):
        with pytest.raises(CurrentRefitParameterCodecError, match="PARAMETER_DOMAIN_MEMBER_INVALID"):
            validate_parameter_domain_member(domain, invalid)


@pytest.mark.parametrize("names,rows", [
    (("fast", "slow"), ((8, 21), (12, 26))),  # Omits both flat marginals' final value.
    (("fast", "slow"), ((8, 21), (12, 26), (16, 26))),  # Omits slow=34 only.
    (("fast", "slow"), ((8, 21), (12, 26), (16, 34), (12, 26))),
    (("fast", "slow"), ((8, 21), (12, 34), (16, 26))),  # Default pair absent.
    (("fast", "slow"), ((8, 21), (12, 26), (99, 34))),
    (("fast", "fast"), ((8, 8), (12, 12), (16, 16))),
    (("fast", ["slow"]), ((8, 21),)),
])
def test_coupled_domain_rejects_incomplete_or_hostile_group_metadata(names, rows):
    with pytest.raises(CurrentRefitParameterCodecError, match="PARAMETER_DOMAIN_INVALID"):
        build_parameter_domain({"fast": 12, "slow": 26},
            {"fast": [8, 12, 16], "slow": [21, 26, 34]},
            coupled_groups=({"names": names, "choices": rows},))


def test_domain_projection_keeps_complete_legal_witness_without_cartesian_cross_pairs():
    domain = _small_coupled_domain()
    rows = list(iter_parameter_domain_projection(domain, ("slow", "fixed")))
    assert len(rows) == 3
    assert {reduced["slow"] for _, reduced in rows} == {21, 26, 34}
    for witness, reduced in rows:
        assert validate_parameter_domain_member(domain, witness) == witness
        assert reduced == {"slow": witness["slow"], "fixed": True}
        assert witness["signal"] == 9
    assert list(iter_parameter_domain_projection(domain, ("unrelated",))) == [(domain["defaults"], {})]
    repeated = build_parameter_domain({"fast": 12, "slow": 26},
        {"fast": [8, 12], "slow": [21, 26]}, coupled_groups=(
            {"names": ("fast", "slow"), "choices": ((8, 21), (8, 26), (12, 26))},))
    projected = list(iter_parameter_domain_projection(repeated, ("fast",)))
    assert len(projected) == 2
    assert {reduced["fast"] for _, reduced in projected} == {8, 12}
    assert all(validate_parameter_domain_member(repeated, witness) == witness for witness, _ in projected)


def test_original_v1_table_preserves_all_metadata_except_explicit_five_domain_editions():
    table = load_retained_strategy_domain_v1()
    allowed = {"RegimeTransitionBreakoutStrategy", "EMARibbonADXStrategy",
               "StochRSITrendGateStrategy", "SessionVWAPReversionStrategy",
               "MACDHistogramMomentumStrategy"}
    assert StrategyRegistry.list_all() == table["strategy_names"]
    changed = set()
    for record in table["strategies"]:
        name = record["strategy_id"]
        strategy = StrategyRegistry.get(name)
        defaults = decode_retained_parameter_metadata(record["typed_metadata"]["defaults"])
        grid = decode_retained_parameter_metadata(record["typed_metadata"]["grid"])
        assert canonical_parameter_bytes(strategy.get_default_params()) == canonical_parameter_bytes(defaults)
        if strategy.get_param_grid() != grid or strategy.get_param_couplings():
            changed.add(name)
    assert changed == allowed


def test_retained_v1_loader_refuses_changed_source_side_bytes(monkeypatch):
    import io
    import pm_current_refit_parameters as codec
    monkeypatch.setattr(codec.Path, "open", lambda *args, **kwargs: io.BytesIO(b"{}"))
    with pytest.raises(CurrentRefitParameterCodecError, match="RETAINED_STRATEGY_DOMAIN_IDENTITY_CHANGED"):
        load_retained_strategy_domain_v1()


def test_parameter_domain_requires_explicit_grouping_keyword():
    assert inspect.signature(build_parameter_domain).parameters["coupled_groups"].default is inspect.Parameter.empty
    with pytest.raises(TypeError, match="coupled_groups"):
        build_parameter_domain({"period": 20}, {"period": [10, 20]})
    assert build_parameter_domain({"period": 20}, {"period": [10, 20]}, coupled_groups=())["point_count"] == 2


# Every production grid read has one reviewed purpose. New readers need review;
# metadata inspection must not quietly become a Cartesian sampler.
_GRID_CONSUMER_POLICY = {
    ("pm_current_refit_artifacts.py", "_normalized_parameters"): ("grouped", 1, ("build_parameter_domain",)),
    ("pm_history_dependencies.py", "find_unrecognized_dependency_like_keys"): ("metadata", 1, ()),
    ("pm_history_dependencies.py", "_strategy_inventory"): ("metadata", 1, ()),
    ("pm_pipeline.py", "_current_refit_validate_trial"): ("grouped", 2, ("build_parameter_domain",)),
    ("pm_pipeline.py", "_strategy_surface_fingerprint"): ("fingerprint", 1, ()),
    ("pm_pipeline.py", "HyperparameterOptimizer.optimize"): ("forward", 1, ("_optimize_with_optuna", "_optimize_grid_search")),
    ("pm_pipeline.py", "RegimeOptimizer._family_probe_param_specs"): ("grouped", 1, ("build_parameter_domain",)),
    ("pm_pipeline.py", "RegimeOptimizer._collect_candidates"): ("forward", 1, ("_tune_strategy_params",)),
    ("pm_pipeline.py", "OptimizationPipeline._build_edge_challengers_for_timeframe"): ("forward", 1, ("_tune_strategy_params",)),
    ("pm_production_search_planner.py", "_strategy_domain"): ("grouped", 1, ("build_parameter_domain",)),
    ("pm_production_search_planner.py", "_live_candidate_domain_sha256"): ("fingerprint", 1, ()),
    ("pm_strategies.py", "BaseStrategy.cartesian_grid_size"): ("grouped", 1, ("build_parameter_domain",)),
    ("pm_strategies.py", "<module>"): ("metadata", 2, ()),
    ("tools/optuna_convergence.py", "_largest_spaces"): ("grouped", 1, ("ParameterSpace",)),
    ("tools/optuna_convergence.py", "main"): ("forward", 1, ("optimize",)),
    ("tools/pbo.py", "build_returns_matrix"): ("grouped", 1, ("build_parameter_domain",)),
    ("tools/strategy_prospect.py", "screen"): ("grouped", 1, ("build_parameter_domain",)),
}


def _assert_grid_consumer_ast_contract(trees, policy):
    reads, calls = Counter(), defaultdict(list)
    forbidden_metadata_calls = {"product", "prod", "permutations", "combinations", "choice", "choices", "sample", "shuffle",
                                "suggest_categorical", "suggest_int", "suggest_float", "_bounded_cartesian_combinations"}
    for path, tree in trees.items():
        aliases = {alias.asname or alias.name: alias.name for node in ast.walk(tree)
                   if isinstance(node, ast.ImportFrom) for alias in node.names}
        def call_name(node):
            return aliases.get(node.func.id, node.func.id) if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
        direct = {id(node.func) for node in ast.walk(tree) if isinstance(node, ast.Call)}
        class Census(ast.NodeVisitor):
            def __init__(self):
                self.scope = []
            def visit_ClassDef(self, node):
                self.scope.append(node.name)
                self.generic_visit(node)
                self.scope.pop()
            visit_FunctionDef = visit_ClassDef
            visit_AsyncFunctionDef = visit_ClassDef
            def visit_Attribute(self, node):
                if node.attr == "get_param_grid":
                    assert id(node) in direct, f"indirect grid reader: {path}:{node.lineno}"
                    reads[(path, ".".join(self.scope) or "<module>")] += 1
                self.generic_visit(node)
            def visit_Call(self, node):
                name = call_name(node)
                key = (path, ".".join(self.scope) or "<module>")
                calls[key].append((name, node))
                if name in {"getattr", "setattr"} and len(node.args) > 1:
                    assert not (isinstance(node.args[1], ast.Constant) and node.args[1].value == "get_param_grid"), "reflective grid reader"
                if name in {"build_parameter_domain", "ParameterSpace"}:
                    assert any(arg.arg == "coupled_groups" for arg in node.keywords), f"grouping keyword omitted: {path}:{node.lineno}"
                self.generic_visit(node)
        Census().visit(tree)
    assert reads == Counter({key: value[1] for key, value in policy.items()}), f"unreviewed grid consumers: {reads}"
    for key, (kind, _, seams) in policy.items():
        names = {name for name, _ in calls[key]}
        if kind in {"metadata", "fingerprint", "forward"}:
            assert not names & forbidden_metadata_calls, f"flat-grid enumeration in {key}"
        if kind in {"grouped", "fingerprint"}:
            assert "get_param_couplings" in names, f"coupling metadata omitted in {key}"
        assert set(seams) <= names, f"group-aware seam omitted in {key}"
        if kind == "grouped":
            for name, call in calls[key]:
                if name in seams:
                    group = next(arg.value for arg in call.keywords if arg.arg == "coupled_groups")
                    assert not isinstance(group, (ast.Tuple, ast.List)) or group.elts, f"current grouping discarded in {key}"
        if kind == "forward":
            for seam in seams:
                forwarded = [call for name, call in calls[key] if name == seam]
                assert any(any(isinstance(node, ast.Name) and node.id == "param_grid"
                               or isinstance(node, ast.Attribute) and node.attr == "get_param_grid"
                               for node in ast.walk(call)) for call in forwarded), f"grid forwarding omitted in {key}"


def test_production_grid_consumer_census_and_group_routing_is_exhaustive():
    root = Path(__file__).resolve().parents[1]
    # Product modules and tool entrypoints; tests and retained QA/preimages are not consumers.
    paths = [*root.glob("*.py"), *(root / "tools").rglob("*.py")]
    trees = {path.relative_to(root).as_posix(): ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path)) for path in paths}
    _assert_grid_consumer_ast_contract(trees, _GRID_CONSUMER_POLICY)


@pytest.mark.parametrize("source,policy,error", [
    ("def new(s): return s.get_param_grid()", {}, "unreviewed grid consumers"),
    ("def inspect(s):\n g=s.get_param_grid()\n return itertools.product(*g.values())",
     {("fixture.py", "inspect"): ("metadata", 1, ())}, "flat-grid enumeration"),
    ("def points(s):\n g=s.get_param_grid()\n c=s.get_param_couplings()\n return build_parameter_domain({},g,coupled_groups=())",
     {("fixture.py", "points"): ("grouped", 1, ("build_parameter_domain",))}, "current grouping discarded"),
    ("def inspect(s):\n getter=s.get_param_grid\n return getter()", {}, "indirect grid reader"),
    ("def points(s):\n return build_parameter_domain({},s.get_param_grid())", {}, "grouping keyword omitted"),
])
def test_grid_consumer_guard_rejects_unreviewed_or_uncoupled_enumeration(source, policy, error):
    with pytest.raises(AssertionError, match=error):
        _assert_grid_consumer_ast_contract({"fixture.py": ast.parse(source)}, policy)
