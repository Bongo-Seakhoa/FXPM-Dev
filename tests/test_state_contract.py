from collections import Counter
import hashlib

from pm_core import FeatureComputer
from pm_state_contract import (
    ACTUAL_STRATEGY_NAMES,
    ContractionFormula,
    ContractionTerm,
    DEFAULT_AUTHORITY,
    EvidenceMode,
    FiniteWindowRule,
    RecursiveRule,
    StateContract,
    StateContractAuthority,
    StateEvidence,
    StateKind,
)
from pm_strategies import StrategyRegistry


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _evidence(authority: StateContractAuthority, node: str) -> StateEvidence:
    contract = authority._contracts[node]
    return StateEvidence(
        node=node,
        mode=EvidenceMode.CHECKPOINT,
        identity_sha256=_digest(node),
        registry_hash=authority.registry_hash,
        state_schema=str(contract.checkpoint_schema),
        transition_schema=str(contract.transition_schema),
    )


def test_default_authority_covers_actual_strategy_and_feature_registries() -> None:
    assert set(DEFAULT_AUTHORITY.strategy_names) == set(StrategyRegistry.list_all())
    assert set(ACTUAL_STRATEGY_NAMES) == set(StrategyRegistry.list_all())
    for feature in FeatureComputer._FEATURE_DEPS:
        assert f"feature:{feature}" in DEFAULT_AUTHORITY.contract_names
    for feature in FeatureComputer._REGIME_FEATURE_COLUMNS:
        assert f"feature:{feature}" in DEFAULT_AUTHORITY.contract_names
    assert Counter(
        DEFAULT_AUTHORITY._contracts[f"strategy:{name}"].kind.value
        for name in ACTUAL_STRATEGY_NAMES
    ) == Counter({
        "FINITE_WINDOW": 46,
        "RECURSIVE_CONTRACTIVE": 47,
        "PATH_STATE": 7,
    })


def test_every_registered_default_cell_resolves_with_all_required_state_evidence() -> None:
    evidence = {
        name: _evidence(DEFAULT_AUTHORITY, name)
        for name, contract in DEFAULT_AUTHORITY._contracts.items()
        if contract.kind == StateKind.PATH_STATE
    }
    for name in StrategyRegistry.list_all():
        strategy = StrategyRegistry.get(name)
        result = DEFAULT_AUTHORITY.resolve_cell(
            name,
            parameters=strategy.params,
            requested_features=sorted(strategy.get_feature_request()),
            timeframe="M5",
            include_regime_controller=True,
            initial_error_bound=100.0,
            tolerance=0.000005,
            state_evidence=evidence,
        )
        assert result.admissible, (name, result.reason_codes)


def test_registry_hash_is_deterministic_and_order_independent() -> None:
    contracts = (
        StateContract("b", StateKind.FINITE_WINDOW, finite_rule=FiniteWindowRule(2)),
        StateContract("a", StateKind.FINITE_WINDOW, finite_rule=FiniteWindowRule(1)),
    )
    first = StateContractAuthority(contracts)
    second = StateContractAuthority(tuple(reversed(contracts)))
    assert first.registry_hash == second.registry_hash
    assert DEFAULT_AUTHORITY.registry_hash == DEFAULT_AUTHORITY.registry_hash


def test_unknown_missing_ambiguous_and_cycle_all_quarantine() -> None:
    unknown = StateContractAuthority((StateContract("x", StateKind.UNKNOWN),))
    assert "CONTRACT_UNKNOWN:x" in unknown.resolve(("x",), timeframe="M5").reason_codes

    missing = StateContractAuthority((
        StateContract("x", StateKind.FINITE_WINDOW, dependencies=("absent",), finite_rule=FiniteWindowRule(1)),
    ))
    assert "CONTRACT_MISSING:absent" in missing.resolve(("x",), timeframe="M5").reason_codes

    ambiguous = StateContractAuthority((
        StateContract("x", StateKind.FINITE_WINDOW, finite_rule=FiniteWindowRule(1)),
        StateContract("x", StateKind.FINITE_WINDOW, finite_rule=FiniteWindowRule(2)),
    ))
    assert "AMBIGUOUS_CONTRACT:x" in ambiguous.resolve(("x",), timeframe="M5").reason_codes

    cycle = StateContractAuthority((
        StateContract("a", StateKind.FINITE_WINDOW, dependencies=("b",), finite_rule=FiniteWindowRule(1)),
        StateContract("b", StateKind.FINITE_WINDOW, dependencies=("a",), finite_rule=FiniteWindowRule(1)),
    ))
    result = cycle.resolve(("a",), timeframe="M5")
    assert not result.admissible
    assert any(reason.startswith("CONTRACT_CYCLE:a->b->a") for reason in result.reason_codes)


def test_parameter_sensitive_ema_analytic_bound() -> None:
    authority = StateContractAuthority((
        StateContract(
            "ema",
            StateKind.RECURSIVE_CONTRACTIVE,
            recursive_rule=RecursiveRule((
                ContractionTerm(ContractionFormula.EMA_SPAN, ("period",)),
            )),
        ),
    ))
    short = authority.resolve(
        ("ema",), parameters={"period": 20}, timeframe="M5",
        initial_error_bound=100.0, tolerance=0.000005,
    )
    long = authority.resolve(
        ("ema",), parameters={"period": 200}, timeframe="M5",
        initial_error_bound=100.0, tolerance=0.000005,
    )
    assert short.admissible and long.admissible
    assert long.bounded_context_bars > short.bounded_context_bars
    assert long.nodes[0].contraction_coefficients[0] > short.nodes[0].contraction_coefficients[0]


def test_kama_slow_parameter_changes_convergence_requirement() -> None:
    authority = StateContractAuthority((
        StateContract(
            "kama", StateKind.RECURSIVE_CONTRACTIVE,
            recursive_rule=RecursiveRule((
                ContractionTerm(ContractionFormula.KAMA_SLOW, ("slow_period",)),
            )),
        ),
    ))
    fast = authority.resolve(
        ("kama",), parameters={"slow_period": 20}, timeframe="H1",
        initial_error_bound=10.0, tolerance=0.00005,
    )
    slow = authority.resolve(
        ("kama",), parameters={"slow_period": 50}, timeframe="H1",
        initial_error_bound=10.0, tolerance=0.00005,
    )
    assert fast.admissible and slow.admissible
    assert slow.bounded_context_bars > fast.bounded_context_bars


def test_malformed_contraction_never_bypasses_when_error_is_already_small() -> None:
    authority = StateContractAuthority((
        StateContract(
            "bad", StateKind.RECURSIVE_CONTRACTIVE,
            recursive_rule=RecursiveRule((
                ContractionTerm(ContractionFormula.CONSTANT, fixed_coefficient=1.0),
            )),
        ),
    ))
    result = authority.resolve(
        ("bad",), parameters={}, timeframe="M5",
        initial_error_bound=1.0, tolerance=2.0,
    )
    assert not result.admissible
    assert any("CONTRACTION_COEFFICIENT_INVALID" in reason for reason in result.reason_codes)


def test_recursive_contract_fails_without_cell_error_or_tolerance() -> None:
    result = DEFAULT_AUTHORITY.resolve_cell(
        "EMACrossoverStrategy",
        parameters=StrategyRegistry.get("EMACrossoverStrategy").params,
        requested_features=("EMA_10", "EMA_20"),
        timeframe="M5",
        include_regime_controller=False,
    )
    assert not result.admissible
    assert any("INITIAL_ERROR_BOUND_REQUIRED" in reason for reason in result.reason_codes)


def test_finite_strategy_bound_is_parameter_sensitive() -> None:
    short = DEFAULT_AUTHORITY.resolve_cell(
        "DonchianBreakoutStrategy", parameters={"period": 20},
        timeframe="H1", include_regime_controller=False,
    )
    long = DEFAULT_AUTHORITY.resolve_cell(
        "DonchianBreakoutStrategy", parameters={"period": 80},
        timeframe="H1", include_regime_controller=False,
    )
    assert short.admissible and long.admissible
    assert long.bounded_context_bars > short.bounded_context_bars


def test_path_state_requires_exact_registry_bound_checkpoint_or_replay() -> None:
    node = "strategy:SupertrendStrategy"
    missing = DEFAULT_AUTHORITY.resolve_cell(
        "SupertrendStrategy",
        parameters=StrategyRegistry.get("SupertrendStrategy").params,
        timeframe="M5",
        include_regime_controller=False,
    )
    assert not missing.admissible
    assert missing.required_path_state_nodes == (node,)

    wrong = _evidence(DEFAULT_AUTHORITY, node)
    wrong = StateEvidence(
        node=wrong.node, mode=wrong.mode, identity_sha256=wrong.identity_sha256,
        registry_hash="0" * 64, state_schema=wrong.state_schema,
        transition_schema=wrong.transition_schema,
    )
    mismatch = DEFAULT_AUTHORITY.resolve_cell(
        "SupertrendStrategy",
        parameters=StrategyRegistry.get("SupertrendStrategy").params,
        timeframe="M5", include_regime_controller=False,
        state_evidence={node: wrong},
    )
    assert not mismatch.admissible
    assert any("STATE_EVIDENCE_REGISTRY_MISMATCH" in reason for reason in mismatch.reason_codes)

    admitted = DEFAULT_AUTHORITY.resolve_cell(
        "SupertrendStrategy",
        parameters=StrategyRegistry.get("SupertrendStrategy").params,
        timeframe="M5", include_regime_controller=False,
        state_evidence={node: _evidence(DEFAULT_AUTHORITY, node)},
    )
    assert admitted.admissible
    assert admitted.bounded_context_bars is not None


def test_default_cell_requires_independent_regime_controller_state() -> None:
    strategy = "DonchianBreakoutStrategy"
    missing = DEFAULT_AUTHORITY.resolve_cell(
        strategy, parameters=StrategyRegistry.get(strategy).params,
        timeframe="M15", include_regime_controller=True,
    )
    assert not missing.admissible
    assert "controller:REGIME_HYSTERESIS" in missing.required_path_state_nodes

    node = "controller:REGIME_HYSTERESIS"
    admitted = DEFAULT_AUTHORITY.resolve_cell(
        strategy, parameters=StrategyRegistry.get(strategy).params,
        timeframe="M15", include_regime_controller=True,
        state_evidence={node: _evidence(DEFAULT_AUTHORITY, node)},
    )
    assert admitted.admissible
    assert admitted.bounded_context_bars >= 400


def test_session_cumulative_state_requires_authenticated_reset_boundary() -> None:
    strategy = "SessionCloseReversionStrategy"
    missing = DEFAULT_AUTHORITY.resolve_cell(
        strategy, parameters=StrategyRegistry.get(strategy).params,
        timeframe="M5", include_regime_controller=False,
    )
    node = "feature:SESSION_RESET"
    assert not missing.admissible
    assert node in missing.required_path_state_nodes

    admitted = DEFAULT_AUTHORITY.resolve_cell(
        strategy, parameters=StrategyRegistry.get(strategy).params,
        timeframe="M5", include_regime_controller=False,
        state_evidence={node: _evidence(DEFAULT_AUTHORITY, node)},
    )
    assert admitted.admissible


def test_fractal_confirmed_pivot_is_checkpoint_required_under_production_veto() -> None:
    strategy = "FractalChannelBreakoutStrategy"
    node = f"strategy:{strategy}"
    result = DEFAULT_AUTHORITY.resolve_cell(
        strategy, parameters=StrategyRegistry.get(strategy).params,
        timeframe="H1", include_regime_controller=False,
    )
    assert not result.admissible
    assert result.required_path_state_nodes == (node,)


def test_unknown_strategy_or_feature_is_quarantined() -> None:
    strategy = DEFAULT_AUTHORITY.resolve_cell(
        "NotRegistered", parameters={}, timeframe="M5",
        include_regime_controller=False,
    )
    feature = DEFAULT_AUTHORITY.resolve(
        ("feature:NOT_REAL",), timeframe="M5",
    )
    assert strategy.status == "QUARANTINED"
    assert feature.status == "QUARANTINED"
    assert "CONTRACT_MISSING:strategy:NotRegistered" in strategy.reason_codes
    assert "CONTRACT_MISSING:feature:NOT_REAL" in feature.reason_codes


def test_decision_hash_changes_with_parameters_but_is_repeatable() -> None:
    kwargs = dict(
        roots=("feature:EMA_20",), timeframe="M5",
        initial_error_bound=100.0, tolerance=0.000005,
    )
    first = DEFAULT_AUTHORITY.resolve(parameters={}, **kwargs)
    second = DEFAULT_AUTHORITY.resolve(parameters={}, **kwargs)
    different = DEFAULT_AUTHORITY.resolve(
        ("feature:EMA_200",), parameters={}, timeframe="M5",
        initial_error_bound=100.0, tolerance=0.000005,
    )
    assert first.decision_hash == second.decision_hash
    assert first.decision_hash != different.decision_hash
