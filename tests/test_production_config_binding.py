from __future__ import annotations

import json
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_cap_calibration as cap
import pm_production_config_binding as binding
from pm_core import PipelineConfig, RETIRED_SCORING_CONFIG_KEYS


def _payload(tmp_path: Path) -> dict:
    return {
        "_meta": {"scope": binding.FUNDED_SCOPE},
        "pipeline": {
            "data_dir": "data",
            "output_dir": "outputs",
            "log_dir": "logs",
            "backtest_spread_mode": "stress_p95",
            "oos_validation_mode": "fixed_holdout",
            "false_discovery_symbol_count": 1,
        },
        "symbols": ["EURUSD"],
        "position": {},
        "storage": {},
    }


def _write(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _config(path: Path, payload: dict) -> PipelineConfig:
    value = PipelineConfig(**payload["pipeline"])
    value._deployment_config_path = str(path.resolve())
    value._funded_live_execution = False
    value.data_dir = (path.parent / value.data_dir).resolve()
    return value


def _context(tmp_path: Path, payload: dict):
    return spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="production-config-binding-test",
        native_universe_sha256="a" * 64,
        config_sha256=cap.canonical_json_sha256(payload),
    )


def test_public_loader_reproduces_complete_effective_config(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    payload["pipeline"].pop("false_discovery_symbol_count")
    payload["symbols"] = ["EURUSD", "GBPUSD"]
    payload["position"] = {
        "regime_tp_multipliers": {
            "TREND": 1.2,
            "BREAKOUT": 1.1,
            "RANGE": 0.9,
            "CHOP": 0.8,
        }
    }
    path = tmp_path / "config.json"
    _write(path, payload)

    owned, effective = binding.load_effective_pipeline_config(path)

    assert owned == payload
    assert effective._deployment_config_path == str(path.resolve())
    assert effective.data_dir == (tmp_path / "data").resolve()
    assert effective.false_discovery_symbol_count == 2
    assert (
        effective.regime_tp_multipliers == payload["position"]["regime_tp_multipliers"]
    )
    assert effective._funded_live_execution is False

    context = _context(tmp_path, payload)
    frozen = binding.freeze_production_pipeline_config(
        context,
        active_config_path=path,
        config_payload=owned,
        supplied_config=effective,
    )
    assert frozen.materialize(context).data_dir == (tmp_path / "data").resolve()


@pytest.mark.parametrize("retired_key", sorted(RETIRED_SCORING_CONFIG_KEYS))
def test_public_loader_rejects_retired_scoring_config_keys(
    tmp_path: Path,
    retired_key: str,
) -> None:
    payload = _payload(tmp_path)
    payload["pipeline"][retired_key] = "retired"
    path = tmp_path / "config.json"
    _write(path, payload)

    with pytest.raises(binding.ProductionConfigBindingError) as caught:
        binding.load_effective_pipeline_config(path)

    assert caught.value.code == "RETIRED_SCORING_CONFIG_KEY"
    assert retired_key in caught.value.details


def test_binding_owns_effective_config_and_ignores_caller_mutation(
    tmp_path: Path,
) -> None:
    payload = _payload(tmp_path)
    path = tmp_path / "config.json"
    _write(path, payload)
    context = _context(tmp_path, payload)
    supplied = _config(path, payload)

    frozen = binding.freeze_production_pipeline_config(
        context,
        active_config_path=path,
        config_payload=payload,
        supplied_config=supplied,
    )
    supplied.backtest_spread_mode = "stress_max"
    payload["pipeline"]["backtest_spread_mode"] = "historical_max"

    first = frozen.materialize(context)
    second = frozen.materialize(context)
    assert first is not second
    assert first.backtest_spread_mode == second.backtest_spread_mode == "stress_p95"
    first.backtest_spread_mode = "stress_max"
    assert frozen.materialize(context).backtest_spread_mode == "stress_p95"


def test_initial_effective_config_mismatch_is_typed(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    path = tmp_path / "config.json"
    _write(path, payload)
    context = _context(tmp_path, payload)
    supplied = _config(path, payload)
    supplied.backtest_spread_mode = "stress_max"

    with pytest.raises(binding.ProductionConfigBindingError) as caught:
        binding.freeze_production_pipeline_config(
            context,
            active_config_path=path,
            config_payload=payload,
            supplied_config=supplied,
        )
    assert caught.value.code == "EFFECTIVE_PIPELINE_CONFIG_MISMATCH"
    assert "backtest_spread_mode" in caught.value.details


def test_binding_reproduces_main_position_multiplier_projection(
    tmp_path: Path,
) -> None:
    payload = _payload(tmp_path)
    multipliers = {
        "TREND": 1.2,
        "BREAKOUT": 1.1,
        "RANGE": 0.9,
        "CHOP": 0.8,
    }
    payload["position"] = {"regime_tp_multipliers": multipliers}
    path = tmp_path / "config.json"
    _write(path, payload)
    context = _context(tmp_path, payload)
    supplied = _config(path, payload)
    supplied.regime_tp_multipliers = dict(multipliers)

    frozen = binding.freeze_production_pipeline_config(
        context,
        active_config_path=path,
        config_payload=payload,
        supplied_config=supplied,
    )
    assert frozen.materialize(context).regime_tp_multipliers == multipliers


def test_active_config_drift_is_rejected_before_materialization(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    path = tmp_path / "config.json"
    _write(path, payload)
    context = _context(tmp_path, payload)
    frozen = binding.freeze_production_pipeline_config(
        context,
        active_config_path=path,
        config_payload=payload,
        supplied_config=_config(path, payload),
    )
    changed = json.loads(json.dumps(payload))
    changed["pipeline"]["backtest_spread_mode"] = "stress_max"
    _write(path, changed)

    with pytest.raises(binding.ProductionConfigBindingError) as caught:
        frozen.materialize(context)
    assert caught.value.code == "ACTIVE_CONFIG_BYTES_CHANGED"


def test_payload_and_context_hash_mismatch_fail_closed(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    path = tmp_path / "config.json"
    _write(path, payload)
    supplied = _config(path, payload)
    context = _context(tmp_path, payload)
    mismatch = json.loads(json.dumps(payload))
    mismatch["pipeline"]["backtest_spread_mode"] = "stress_max"

    with pytest.raises(binding.ProductionConfigBindingError) as caught:
        binding.freeze_production_pipeline_config(
            context,
            active_config_path=path,
            config_payload=mismatch,
            supplied_config=supplied,
        )
    assert caught.value.code == "ACTIVE_CONFIG_PAYLOAD_MISMATCH"

    wrong_context = spool.freeze_generation_context(
        (tmp_path / "wrong-spool").resolve(),
        generation_id="production-config-binding-wrong",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )
    with pytest.raises(binding.ProductionConfigBindingError) as caught:
        binding.freeze_production_pipeline_config(
            wrong_context,
            active_config_path=path,
            config_payload=payload,
            supplied_config=supplied,
        )
    assert caught.value.code == "ACTIVE_CONFIG_CONTEXT_SHA256_MISMATCH"
