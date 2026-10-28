from pathlib import Path

import pytest

import pm_demo_management
import pm_main
import pm_optimization_authority
import pm_production_config_binding
from agent_audit.performance.scripts import run_promoted_cell_replay
from agent_audit.performance.scripts import run_synchronized_portfolio_replay
from pm_core import (
    RETIRED_SCORING_CONFIG_KEYS,
    RetiredScoringConfigError,
    strict_pipeline_config_kwargs,
)
from tools import (
    build_postlockbox_edge_challenger,
    issue_promotion_authorization,
    precompute_feature_cache,
    seal_prelockbox_roster,
)


REPO = Path(__file__).resolve().parents[1]
RETIRED_EXECUTION_CONFIG_KEYS = frozenset(
    {"use_commission", "use_slippage", "slippage_pips"}
)


def test_retired_execution_keys_share_the_fail_closed_projection_boundary() -> None:
    assert RETIRED_EXECUTION_CONFIG_KEYS.issubset(RETIRED_SCORING_CONFIG_KEYS)


@pytest.mark.parametrize("retired_key", sorted(RETIRED_SCORING_CONFIG_KEYS))
def test_shared_projection_rejects_every_retired_pipeline_key(retired_key: str) -> None:
    with pytest.raises(RetiredScoringConfigError) as caught:
        strict_pipeline_config_kwargs({retired_key: "retired"})

    assert caught.value.keys == (retired_key,)


def test_authority_and_authoring_loaders_reject_retired_model() -> None:
    payload = {"pipeline": {"regime_scoring_model": "specialist_spokes_v2"}}
    config_path = REPO / "config.json"

    with pytest.raises(pm_demo_management._Blocked, match="MANAGEMENT_CONFIG_RETIRED_SCORING_KEY"):
        pm_demo_management._pipeline_config(payload, config_path)
    with pytest.raises(pm_optimization_authority._Invalid, match="CONFIG_RETIRED_SCORING_KEY"):
        pm_optimization_authority._pipeline_config(payload, config_path)
    with pytest.raises(issue_promotion_authorization.AuthoringFailure) as caught:
        issue_promotion_authorization._pipeline_config(config_path, payload)
    assert caught.value.errors == (
        "CONFIG_RETIRED_SCORING_KEY:regime_scoring_model",
    )


@pytest.mark.parametrize(
    "loader",
    (
        lambda path, payload: seal_prelockbox_roster._pipeline_config(path, payload),
        lambda path, payload: build_postlockbox_edge_challenger._pipeline_config(path, payload),
        lambda _path, payload: precompute_feature_cache._pipeline_config(payload),
    ),
)
def test_operational_loaders_reject_retired_model(loader) -> None:
    payload = {"pipeline": {"regime_scoring_model": "specialist_spokes_v2"}}
    with pytest.raises(RetiredScoringConfigError):
        loader(REPO / "config.json", payload)


def test_nested_storage_cannot_hide_retired_scoring_key() -> None:
    with pytest.raises(RetiredScoringConfigError):
        pm_main._normalize_storage_section_for_pipeline(
            {"regime_scoring_model": "specialist_spokes_v2"}
        )
    with pytest.raises(
        pm_production_config_binding.ProductionConfigBindingError
    ) as caught:
        pm_production_config_binding._derive_pipeline_config(
            {
                "pipeline": {},
                "storage": {
                    "regime_scoring_model": "specialist_spokes_v2",
                },
            },
            REPO / "config.json",
        )
    assert caught.value.code == "RETIRED_SCORING_CONFIG_KEY"


@pytest.mark.parametrize(
    "loader,args",
    (
        (run_promoted_cell_replay._pipeline_config, (10_000.0,)),
        (run_synchronized_portfolio_replay._pipeline_config, ()),
    ),
)
def test_active_replay_loaders_reject_retired_model(
    tmp_path: Path,
    loader,
    args: tuple,
) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        '{"pipeline":{"regime_scoring_model":"specialist_spokes_v2"}}',
        encoding="utf-8",
    )
    with pytest.raises(RetiredScoringConfigError):
        loader(path, *args)


@pytest.mark.parametrize(
    "relative",
    (
        "pm_demo_management.py",
        "pm_main.py",
        "pm_optimization_authority.py",
        "pm_production_config_binding.py",
        "pm_regime_tuner.py",
        "agent_audit/performance/scripts/run_promoted_cell_replay.py",
        "agent_audit/performance/scripts/run_synchronized_portfolio_replay.py",
        "tools/benchmark.py",
        "tools/build_postlockbox_edge_challenger.py",
        "tools/governance_eval_common.py",
        "tools/issue_promotion_authorization.py",
        "tools/precompute_feature_cache.py",
        "tools/purged_kfold_replay.py",
        "tools/seal_prelockbox_roster.py",
    ),
)
def test_all_active_pipeline_config_projections_use_shared_strict_boundary(
    relative: str,
) -> None:
    source = (REPO / relative).read_text(encoding="utf-8")
    assert "strict_pipeline_config_kwargs" in source
