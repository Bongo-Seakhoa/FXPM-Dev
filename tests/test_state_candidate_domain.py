from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Any

from pm_state_contract import (
    ACTUAL_STRATEGY_NAMES,
    DEFAULT_AUTHORITY,
    STATE_FEATURE_ALIASES,
    STRATEGY_CANDIDATE_DOMAIN_SHA256,
    strategy_candidate_domain_sha256,
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
            }
        )
    return documents


def test_exact_registry_order_and_candidate_domain_hash_are_pinned() -> None:
    assert tuple(StrategyRegistry.list_all()) == ACTUAL_STRATEGY_NAMES
    live_hash = strategy_candidate_domain_sha256(_live_candidate_documents())
    assert live_hash == STRATEGY_CANDIDATE_DOMAIN_SHA256
    assert DEFAULT_AUTHORITY.candidate_domain_hash == live_hash


def test_candidate_domain_hash_detects_one_axis_value_change() -> None:
    documents = _live_candidate_documents()
    changed = deepcopy(documents)
    first_grid = changed[0]["grid"]
    first_key = sorted(first_grid)[0]
    first_grid[first_key] = list(first_grid[first_key]) + ["DOMAIN_DRIFT_SENTINEL"]
    assert strategy_candidate_domain_sha256(changed) != STRATEGY_CANDIDATE_DOMAIN_SHA256


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
    assert dict(STATE_FEATURE_ALIASES) == {
        "REGIME": "controller:REGIME_HYSTERESIS",
        "REGIME_LIVE": "controller:REGIME_HYSTERESIS",
    }
    assert "feature:REGIME" not in DEFAULT_AUTHORITY.contract_names
    assert "feature:REGIME_LIVE" not in DEFAULT_AUTHORITY.contract_names

