import json
from pathlib import Path

from pm_strategies import StrategyRegistry


def _collect_strategy_names(obj, out):
    if isinstance(obj, dict):
        strategy_name = obj.get("strategy_name")
        if isinstance(strategy_name, str) and strategy_name:
            out.add(strategy_name)
        for value in obj.values():
            _collect_strategy_names(value, out)
    elif isinstance(obj, list):
        for value in obj:
            _collect_strategy_names(value, out)


def test_registry_contains_new_strategy_set():
    names = set(StrategyRegistry.list_all())
    required = {
        "InsideBarBreakoutStrategy",
        "NarrowRangeBreakoutStrategy",
        "TurtleSoupReversalStrategy",
        "PinBarReversalStrategy",
        "EngulfingPatternStrategy",
        "VolumeSpikeMomentumStrategy",
        "RSIDivergenceStrategy",
        "MACDDivergenceStrategy",
        "OBVDivergenceStrategy",
        "KeltnerFadeStrategy",
        "ROCExhaustionReversalStrategy",
        "EMAPullbackContinuationStrategy",
        "ParabolicSARTrendStrategy",
        "ATRPercentileBreakoutStrategy",
    }
    assert required.issubset(names)


def test_registry_migrates_legacy_vwap_names():
    assert StrategyRegistry.get("VWAPDeviationReversionStrategy").name == "ZScoreVWAPReversionStrategy"
    assert StrategyRegistry.get("VWAPDeviationReversalStrategy").name == "ZScoreVWAPReversionStrategy"


def test_pm_configs_strategy_names_remain_loadable():
    pm_configs_path = Path("pm_configs.json")
    assert pm_configs_path.exists()

    with pm_configs_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    strategy_names = set()
    _collect_strategy_names(payload, strategy_names)
    assert strategy_names

    for strategy_name in strategy_names:
        StrategyRegistry.get(strategy_name)
