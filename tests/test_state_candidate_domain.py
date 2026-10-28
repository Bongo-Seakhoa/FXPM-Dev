from __future__ import annotations

from copy import deepcopy
from enum import Enum
import hashlib
import json
from typing import Any

from pm_current_refit_parameters import (
    decode_retained_parameter_metadata,
    load_retained_strategy_domain_v1,
    parameter_values_equal,
)
from pm_core import FeatureComputer
from pm_state_contract import (
    ACTUAL_STRATEGY_NAMES,
    DEFAULT_AUTHORITY,
    ORIGINAL_STATE_CANDIDATE_DOMAIN_SCHEMA_VERSION,
    ORIGINAL_STRATEGY_CANDIDATE_DOMAIN_SHA256,
    STATE_FEATURE_ALIASES,
    STRATEGY_CANDIDATE_DOMAIN_SHA256,
    strategy_candidate_domain_sha256,
    retained_v1_authority,
)
from pm_strategies import StrategyRegistry


def _normalize(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {
            str(key): _normalize(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, set):
        return sorted(_normalize(item) for item in value)
    return value


def _live_candidate_documents() -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for name in sorted(StrategyRegistry.list_all()):
        strategy = StrategyRegistry.get(name)
        documents.append(
            {
                "name": name,
                "defaults": _normalize(strategy.get_default_params()),
                "grid": _normalize(strategy.get_param_grid()),
                "coupled_groups": _normalize(strategy.get_param_couplings()),
            }
        )
    return documents


def test_exact_registry_order_and_candidate_domain_hash_are_pinned() -> None:
    assert tuple(StrategyRegistry.list_all()) == ACTUAL_STRATEGY_NAMES
    live_hash = strategy_candidate_domain_sha256(_live_candidate_documents())
    expected_current_hash = "a4305a9d256602158563c9edb8eb4ee2c857eba710825555a3f621327f0f25ef"
    assert live_hash == expected_current_hash
    assert live_hash == STRATEGY_CANDIDATE_DOMAIN_SHA256
    assert DEFAULT_AUTHORITY.candidate_domain_hash == live_hash


def test_candidate_domain_hash_detects_one_axis_value_change() -> None:
    documents = _live_candidate_documents()
    changed = deepcopy(documents)
    first_grid = changed[0]["grid"]
    first_key = sorted(first_grid)[0]
    first_grid[first_key] = list(first_grid[first_key]) + ["DOMAIN_DRIFT_SENTINEL"]
    assert strategy_candidate_domain_sha256(changed) != STRATEGY_CANDIDATE_DOMAIN_SHA256


def test_candidate_domain_hash_binds_coupled_partner_rows_not_only_marginals() -> None:
    documents = _live_candidate_documents()
    index = next(index for index, row in enumerate(documents) if row["coupled_groups"])
    changed = deepcopy(documents)
    group = changed[index]["coupled_groups"][0]
    rows = group["choices"]
    original_marginals = [set(row[column] for row in rows) for column in range(2)]
    default_row = [changed[index]["defaults"][name] for name in group["names"]]
    left, right = [position for position, row in enumerate(rows) if row != default_row][:2]
    rows[left][1], rows[right][1] = rows[right][1], rows[left][1]
    assert [set(row[column] for row in rows) for column in range(2)] == original_marginals
    assert changed[index]["grid"] == documents[index]["grid"]
    assert strategy_candidate_domain_sha256(changed) != STRATEGY_CANDIDATE_DOMAIN_SHA256
    removed = deepcopy(documents)
    removed[index]["coupled_groups"][0]["choices"].pop(left)
    assert strategy_candidate_domain_sha256(removed) != STRATEGY_CANDIDATE_DOMAIN_SHA256


def test_coupled_rows_preserve_typed_axis_members_and_default() -> None:
    coupled_names = []
    for name in StrategyRegistry.list_all():
        strategy = StrategyRegistry.get(name)
        defaults, grid = strategy.get_default_params(), strategy.get_param_grid()
        for group in strategy.get_param_couplings():
            coupled_names.append(name)
            assert any(all(type(value) is type(defaults[key]) and parameter_values_equal(value, defaults[key])
                           for key, value in zip(group["names"], row))
                       for row in group["choices"])
            for row in group["choices"]:
                assert len(row) == len(group["names"])
                for key, value in zip(group["names"], row):
                    assert any(type(value) is type(choice) and parameter_values_equal(value, choice)
                               for choice in grid[key])
    assert set(coupled_names) == {
        "StochRSITrendGateStrategy", "SessionVWAPReversionStrategy", "MACDHistogramMomentumStrategy",
    }


def test_retained_original_candidate_hash_and_authority_are_recomputed() -> None:
    table = load_retained_strategy_domain_v1()
    documents = sorted([
        {"name": row["strategy_id"],
         "defaults": _normalize(decode_retained_parameter_metadata(row["typed_metadata"]["defaults"])),
         "grid": _normalize(decode_retained_parameter_metadata(row["typed_metadata"]["grid"]))}
        for row in table["strategies"]
    ], key=lambda row: row["name"])
    encoded = json.dumps(
        {"schema_version": ORIGINAL_STATE_CANDIDATE_DOMAIN_SCHEMA_VERSION, "strategies": documents},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
    ).encode("ascii")
    original_hash = hashlib.sha256(encoded).hexdigest()
    assert original_hash == ORIGINAL_STRATEGY_CANDIDATE_DOMAIN_SHA256 == (
        "d4d8ae2deee050989bdc8e93b21d779c8d68ddb4666668a9c34c5d65177803e3"
    )
    assert strategy_candidate_domain_sha256(documents) != original_hash
    original = retained_v1_authority()
    assert original.candidate_domain_hash == original_hash
    assert original.registry_hash == "ccd2635409bb3fb86effaf9274f8c06ea60e1e77bde71e245082321582c7ab29"
    assert original.registry_hash == table["process_template"]["state_contract_registry_sha256"]
    assert original.registry_hash != DEFAULT_AUTHORITY.registry_hash
    assert original.candidate_domain_hash != DEFAULT_AUTHORITY.candidate_domain_hash


def test_every_grid_axis_contains_its_default_candidate_value() -> None:
    """The mandatory default attempt must pass the strict process-grid verifier."""

    for name in StrategyRegistry.list_all():
        strategy = StrategyRegistry.get(name)
        defaults = strategy.get_default_params()
        for axis_name, choices in strategy.get_param_grid().items():
            assert axis_name in defaults, (name, axis_name)
            assert any(
                parameter_values_equal(defaults[axis_name], choice)
                for choice in choices
            ), (name, axis_name, defaults[axis_name])


def test_candidate_domain_rejects_unsorted_or_duplicate_names() -> None:
    documents = _live_candidate_documents()
    try:
        strategy_candidate_domain_sha256(list(reversed(documents)))
    except ValueError as exc:
        assert "unique sorted names" in str(exc)
    else:
        raise AssertionError("unsorted candidate domain was accepted")

    duplicate = documents + [deepcopy(documents[-1])]
    try:
        strategy_candidate_domain_sha256(duplicate)
    except ValueError as exc:
        assert "unique sorted names" in str(exc)
    else:
        raise AssertionError("duplicate candidate domain was accepted")


def test_regime_alias_domain_is_exact_and_nonduplicative() -> None:
    assert set(STATE_FEATURE_ALIASES) == FeatureComputer._REGIME_FEATURE_COLUMNS
    assert set(STATE_FEATURE_ALIASES.values()) == {"controller:REGIME_HYSTERESIS"}
    for feature in STATE_FEATURE_ALIASES:
        assert f"feature:{feature}" not in DEFAULT_AUTHORITY.contract_names
    assert "feature:REGIME_SCORE_SURFACE" not in DEFAULT_AUTHORITY.contract_names
