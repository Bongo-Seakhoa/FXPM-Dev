from __future__ import annotations

import copy
import hashlib
import json
import math
from typing import Any, Callable

import pytest

from pm_history_dependencies import (
    CANONICAL_TIMEFRAMES,
    HISTORY_DEPENDENCY_CONTRACT_VERSION,
    WARMUP_SAFETY_BUFFER_BARS,
    WARMUP_SAFETY_MULTIPLIER,
    HistoryDependencyContractError,
    UnknownDependencyAxisError,
    assert_history_dependency_contract_production_ready,
    build_history_dependency_contract,
    contract_content_sha256,
    find_unrecognized_dependency_like_keys,
    serialize_history_dependency_contract,
    validate_history_dependency_contract_hash,
)
from pm_strategies import StrategyRegistry


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    return build_history_dependency_contract()


class _StrategyProxy:
    def __init__(
        self,
        wrapped: Any,
        mutate_grid: Callable[[dict[str, Any]], None],
    ) -> None:
        self._wrapped = wrapped
        self._mutate_grid = mutate_grid

    def get_param_grid(self) -> dict[str, Any]:
        grid = copy.deepcopy(self._wrapped.get_param_grid() or {})
        self._mutate_grid(grid)
        return grid

    def get_default_params(self) -> dict[str, Any]:
        return copy.deepcopy(self._wrapped.get_default_params() or {})


class _RegistryProxy:
    def __init__(
        self,
        target: str,
        mutate_grid: Callable[[dict[str, Any]], None],
    ) -> None:
        self._target = target
        self._mutate_grid = mutate_grid

    @staticmethod
    def list_all() -> list[str]:
        return StrategyRegistry.list_all()

    def get(self, name: str) -> Any:
        strategy = StrategyRegistry.get(name)
        if name == self._target:
            return _StrategyProxy(strategy, self._mutate_grid)
        return strategy


def test_contract_inventories_exact_registered_grids(contract: dict[str, Any]) -> None:
    registry = contract["components"]["strategy_registry"]
    actual_names = sorted(StrategyRegistry.list_all())
    assert len(actual_names) == 100
    assert registry["registry_count"] == len(actual_names)

    inventory_by_name = {item["strategy"]: item for item in registry["strategies"]}
    assert sorted(inventory_by_name) == actual_names
    actual_all_grid_keys: set[str] = set()
    for name in actual_names:
        expected_keys = sorted((StrategyRegistry.get(name).get_param_grid() or {}).keys())
        assert inventory_by_name[name]["grid_keys"] == expected_keys
        actual_all_grid_keys.update(expected_keys)

    key_inventory = registry["grid_key_inventory"]
    assert key_inventory["all_grid_keys"] == sorted(actual_all_grid_keys)
    classified = set().union(
        key_inventory["native_bar_dependency"],
        key_inventory["implied_native_bar_dependency"],
        key_inventory["calendar_or_session_dependency"],
        key_inventory["reviewed_non_history_parameter"],
        key_inventory["non_history_parameter"],
    )
    assert classified == set(key_inventory["all_grid_keys"]) | set(
        key_inventory["all_default_keys"]
    )


def test_no_current_dependency_like_key_escapes_review() -> None:
    assert find_unrecognized_dependency_like_keys() == ()


def test_unknown_dependency_axis_fails_closed() -> None:
    target = StrategyRegistry.list_all()[0]

    def add_unknown(grid: dict[str, Any]) -> None:
        grid["mystery_lookback"] = [10, 20]

    with pytest.raises(UnknownDependencyAxisError, match="mystery_lookback"):
        build_history_dependency_contract(
            registry=_RegistryProxy(target, add_unknown)
        )


def test_grid_change_alters_grid_and_contract_hash(contract: dict[str, Any]) -> None:
    target = StrategyRegistry.list_all()[0]

    def change_existing_axis(grid: dict[str, Any]) -> None:
        grid["sl_atr_mult"] = [*grid["sl_atr_mult"], 9.0]

    changed = build_history_dependency_contract(
        registry=_RegistryProxy(target, change_existing_axis)
    )
    original_hashes = contract["hashes"]
    changed_hashes = changed["hashes"]
    assert changed_hashes["strategy_grid_sha256"] != original_hashes["strategy_grid_sha256"]
    assert changed["contract_sha256"] != contract["contract_sha256"]


def test_requirements_cover_all_components_and_stacked_maximum(
    contract: dict[str, Any],
) -> None:
    components = contract["components"]
    strategy_max = components["strategy_registry"]["strategy_maxima"]
    assert strategy_max["maximum_stacked_dependency_bars"] >= 300
    assert strategy_max["maximum_single_axis_bars"] >= 200
    assert components["feature_computer"]["required_bars"] >= 200
    assert components["regime_detector"]["base_required_bars"] >= 200
    assert components["live_minimum"]["live_min_bars"] == 300

    assert tuple(contract["timeframes"]) == CANONICAL_TIMEFRAMES
    for timeframe in CANONICAL_TIMEFRAMES:
        requirement = contract["per_timeframe"][timeframe]
        raw_required = max(requirement["component_bars"].values())
        assert requirement["raw_required_bars"] == raw_required
        assert requirement["candidate_pretrain_bars"] == (
            math.ceil(raw_required * WARMUP_SAFETY_MULTIPLIER)
            + WARMUP_SAFETY_BUFFER_BARS
        )
        assert requirement["candidate_pretrain_bars"] > raw_required


def test_candidate_contract_is_explicitly_vetoed_for_production(
    contract: dict[str, Any],
) -> None:
    assert contract["production_status"] == "RESEARCH_ONLY_VETOED"
    assert contract["production_eligible"] is False
    assert {
        "PATH_DEPENDENT_STATE_HAS_NO_PROVEN_PREFIX_BOUND",
        "RECURSIVE_NUMERICAL_CONVERGENCE_NOT_DERIVED",
        "CANDIDATE_COMPOSITION_RULE_NOT_PROVEN",
        "NUMERICAL_ERROR_NOT_BOUND_TO_SIGNAL_STABILITY",
        "LIVE_INITIALIZATION_CONTEXT_NOT_AUTHENTICATED",
        "CONTRACT_CHECKSUM_IS_NOT_TRUSTED_SOURCE_ATTESTATION",
    } == set(contract["production_blockers"])
    with pytest.raises(HistoryDependencyContractError, match="not production ready"):
        assert_history_dependency_contract_production_ready(contract)

    fabricated = {
        "contract_version": "fabricated-v0",
        "production_status": "PRODUCTION_READY",
        "production_eligible": True,
        "production_blockers": [],
        "per_timeframe": {"D1": {"candidate_pretrain_bars": 1}},
    }
    fabricated["contract_sha256"] = contract_content_sha256(fabricated)
    validate_history_dependency_contract_hash(fabricated)
    with pytest.raises(HistoryDependencyContractError, match="code authority vetoed"):
        assert_history_dependency_contract_production_ready(fabricated)


def test_default_only_history_axes_are_included(contract: dict[str, Any]) -> None:
    strategies = {
        item["strategy"]: item
        for item in contract["components"]["strategy_registry"]["strategies"]
    }
    variance = strategies["VarianceRatioTrendStrategy"]
    assert variance["bar_axis_maxima"]["structural_sl_lookback"] == 20
    ichimoku = strategies["IchimokuStrategy"]
    assert ichimoku["bar_axis_maxima"]["senkou_b_period"] == 52
    rsi_extremes = strategies["RSIExtremesStrategy"]
    assert rsi_extremes["bar_axis_maxima"]["use_ema200_trend_filter"] == 200


def test_calendar_context_is_not_collapsed_to_bar_estimate(
    contract: dict[str, Any],
) -> None:
    calendar = contract["calendar_context_requirements"]
    assert calendar["current_session_from_boundary_required"] is True
    assert calendar["current_calendar_day_from_boundary_required"] is True
    assert calendar["previous_complete_sessions"] == 1
    assert calendar["previous_complete_trading_days"] == 20
    assert calendar["previous_complete_weeks"] == 1
    assert calendar["previous_complete_months"] == 1
    assert "may not be replaced by an estimated bar count" in calendar["source_start_rule"]

    sources = calendar["context_strategy_sources"]
    assert "ADRExpansionBreakoutStrategy" in sources["previous_complete_trading_days"]
    assert "PreviousWeekRangeBreakoutStrategy" in sources["previous_complete_weeks"]
    assert "WeeklyMonthlyLevelRejectionStrategy" in sources["previous_complete_months"]


def test_serialization_and_content_hash_are_deterministic(
    contract: dict[str, Any],
) -> None:
    rebuilt = build_history_dependency_contract()
    first = serialize_history_dependency_contract(contract)
    second = serialize_history_dependency_contract(rebuilt)
    assert first == second
    assert first.endswith("\n")
    assert len(first.encode("utf-8")) < 200_000
    assert json.loads(first) == contract
    assert contract["contract_version"] == HISTORY_DEPENDENCY_CONTRACT_VERSION
    assert contract_content_sha256(contract) == contract["contract_sha256"]
    assert hashlib.sha256(
        json.dumps(
            {key: value for key, value in contract.items() if key != "contract_sha256"},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest() == contract["contract_sha256"]
    validate_history_dependency_contract_hash(contract)


def test_hash_surface_authenticates_source_grid_defaults_and_contract_code(
    contract: dict[str, Any],
) -> None:
    hashes = contract["hashes"]
    for key in (
        "contract_code_sha256",
        "dependency_spec_sha256",
        "source_bundle_sha256",
        "strategy_grid_sha256",
        "strategy_defaults_sha256",
    ):
        assert len(hashes[key]) == 64
        int(hashes[key], 16)
    assert set(hashes["source_files_sha256"]) == {
        "pm_core.py",
        "pm_regime.py",
        "pm_regime_tuner.py",
        "pm_strategies.py",
    }


def test_tamper_is_detected(contract: dict[str, Any]) -> None:
    tampered = copy.deepcopy(contract)
    tampered["per_timeframe"]["D1"]["candidate_pretrain_bars"] -= 1
    with pytest.raises(HistoryDependencyContractError, match="hash mismatch"):
        validate_history_dependency_contract_hash(tampered)


def test_noncanonical_timeframe_roster_is_rejected() -> None:
    with pytest.raises(HistoryDependencyContractError, match="requires"):
        build_history_dependency_contract(timeframes=("M5", "H1"))
