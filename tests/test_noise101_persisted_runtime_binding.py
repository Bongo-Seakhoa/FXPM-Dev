"""INVENTED parameter transport only; no data, model run, or trading authority."""
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as execution
import pm_forward_oos_replay_producer as forward
from pm_strategies import NoiseBandBreakoutStrategy, StrategyRegistry, resolve_strategy_parameters


def _materialize(runtime, strategy_id, parameters):
    template = StrategyRegistry.get(strategy_id)
    if runtime == "owner":
        attempt = SimpleNamespace(ordinal=0, artifacts=SimpleNamespace(
            winner_process={"strategy_id": strategy_id},
            trial_definition={"parameters": parameters}))
        return execution._runtime_strategy(attempt, timeframe="M15")[0]
    specialist = SimpleNamespace(strategy_id=strategy_id, timeframe="M15",
        parameters=parameters, cell_id="INVENTED-NOISE101-RUNTIME-BINDING",
        strategy_class=type(template).__name__, strategy_module=type(template).__module__)
    return forward._strategy(specialist)


@pytest.mark.parametrize("runtime", ("owner", "forward"))
@pytest.mark.parametrize("strategy_id", ("NoiseBandBreakoutStrategy", "EMACrossoverStrategy"))
def test_runtime_materialization_preserves_bound_parameters(runtime, strategy_id):
    defaults, _, _ = resolve_strategy_parameters(
        StrategyRegistry.get(strategy_id), "M15", defaults_only=True)
    strategy = _materialize(runtime, strategy_id, dict(defaults))
    assert all(strategy.params[key] == value for key, value in defaults.items())
    if strategy_id == "NoiseBandBreakoutStrategy":
        assert strategy.params["bar_minutes"] == 15


@pytest.mark.parametrize("runtime", ("owner", "forward"))
@pytest.mark.parametrize("mutation", ("crossed", "missing", "float", "bool"))
def test_runtime_rejects_persisted_timeframe_binding_before_decode(
        runtime, mutation, monkeypatch):
    wire, _, _ = resolve_strategy_parameters(
        NoiseBandBreakoutStrategy(), "M15", defaults_only=True)
    wire = dict(wire)
    if mutation == "missing":
        del wire["bar_minutes"]
    else:
        wire["bar_minutes"] = {"crossed": 5, "float": 15.0, "bool": True}[mutation]
    module = execution if runtime == "owner" else forward
    error_type = (execution.AuthoritativeSearchExecutionError if runtime == "owner"
                  else forward.ForwardOOSReplayProductionError)

    def forbidden(*args, **kwargs):
        pytest.fail("persisted timeframe mismatch reached parameter decoding")

    monkeypatch.setattr(module, "decode_parameter_mapping", forbidden)
    with pytest.raises(error_type) as error:
        _materialize(runtime, "NoiseBandBreakoutStrategy", wire)
    assert str(error.value.__cause__) == "NOISE101_TIMEFRAME_BINDING_MISMATCH"


def test_persisted_binding_uses_one_defaults_resolution_without_unused_domain():
    class CountedNoise(NoiseBandBreakoutStrategy):
        calls = 0

        def get_default_params_for_timeframe(self, timeframe):
            self.calls += 1
            return super().get_default_params_for_timeframe(timeframe)

        def get_param_grid_for_timeframe(self, timeframe):
            pytest.fail("defaults-only runtime constructed an unused grid")

        def get_param_couplings(self):
            pytest.fail("defaults-only runtime constructed unused couplings")

    strategy = CountedNoise()
    defaults, grid, coupled = resolve_strategy_parameters(strategy, "M15",
        defaults_only=True, persisted_parameters={"bar_minutes": 15})
    assert strategy.calls == 1 and defaults["bar_minutes"] == 15
    assert grid is coupled is None


def test_persisted_binding_rejects_before_full_mode_grid_construction():
    class NoGridNoise(NoiseBandBreakoutStrategy):
        def get_param_grid_for_timeframe(self, timeframe):
            pytest.fail("crossed persisted binding reached grid construction")

    with pytest.raises(ValueError, match="NOISE101_TIMEFRAME_BINDING_MISMATCH"):
        resolve_strategy_parameters(NoGridNoise(), "M15", persisted_parameters={"bar_minutes": 5})
