"""Full selected-source PAPER construction with an empty real portfolio and fake native data.

No account, risk or authority validator is replaced. No market loop is started,
and the owned empty configuration/ledger has no winner or trading authority.
"""

import json
import os
from pathlib import Path
import sys
from unittest.mock import Mock


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_core as core
import pm_main as main
import pm_storage as storage
import pm_live_outcomes as outcomes
from pm_pipeline import PortfolioManager
from pm_position import PositionConfig
from test_operational_execution import world


assert Path(main.__file__).resolve() == PRODUCT_ROOT / "pm_main.py"
assert Path(storage.__file__).resolve() == PRODUCT_ROOT / "pm_storage.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"


def test_full_bound_paper_trader_constructs_without_winners_or_funded_authority(world, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Isolate input caches rather than inheriting another test's deployment data.
    # The real loaders see only these owned invented/public files or empty paths.
    monkeypatch.setenv("PM_CONFIG_PATH", str(tmp_path / "absent-owned-config.json"))
    for name, value in {
        "_CONFIG_INSTRUMENT_SPECS": {}, "_CONFIG_SPEC_DEFAULTS": {},
        "_CONFIG_SPECS_LOADED": False, "_CONFIG_SPECS_LOADED_PATH": None,
        "_BROKER_SPECS_CACHE": {}, "_BROKER_SPECS_LOADED": False,
        "_BROKER_SPECS_LOADED_PATH": None,
        "_BROKER_SPECS_PATH": str(tmp_path / "invented-broker-specs.json"),
    }.items():
        monkeypatch.setattr(core, name, value)
    fake = world.fake.symbol
    (tmp_path / "invented-broker-specs.json").write_text(json.dumps({"EURUSD": {
        "classification": "SYNTHETIC_CONSTRUCTION_FIXTURE_NOT_BROKER_EVIDENCE",
        "digits": fake.digits, "point": fake.point, "tick_value": fake.trade_tick_value,
        "tick_size": fake.trade_tick_size, "contract_size": fake.trade_contract_size,
        "volume_min": fake.volume_min, "volume_max": fake.volume_max,
        "volume_step": fake.volume_step, "spread": fake.spread,
        "swap_long": fake.swap_long, "swap_short": fake.swap_short, "swap_mode": fake.swap_mode,
    }}), encoding="utf-8")
    config = core.PipelineConfig(data_dir=tmp_path / "empty-data", output_dir=tmp_path / "output",
        log_dir=tmp_path / "logs", winner_ledger_path=str(tmp_path / "absent-winner-ledger.json"))
    config._live_execution_destination = "PAPER"
    config._funded_live_execution = False
    for path in (config.data_dir, config.output_dir, config.log_dir):
        path.mkdir(parents=True, exist_ok=True)
    portfolio = PortfolioManager(config=config, symbols=["EURUSD"], config_file=config.winner_ledger_path)
    assert portfolio.symbol_configs == {} and not Path(config.winner_ledger_path).exists()
    assert config.production_require_promotion_gate is True

    account = world.open()
    # Use the actual config and namespace for all runtime components.
    account.ledger.close()
    manager = storage.StorageManager(config, data_dir=config.data_dir,
        output_dir=config.output_dir, log_dir=config.log_dir, runtime_identity=account.identity)
    ledger = outcomes.LiveOutcomeLedger(config,
        ledger_path=manager.output_dir / "closed_positions.jsonl",
        state_path=manager.output_dir / "closed_positions.state.json", runtime_identity=account.identity)
    try:
        connector = main.PaperExecutionConnector(account.data, ledger, account.identity)
        sampled = Mock(wraps=world.fake.account_info)
        monkeypatch.setattr(world.fake, "account_info", sampled)
        positions = PositionConfig()
        trader = main.LiveTrader(mt5_connector=connector, portfolio_manager=portfolio,
            position_config=positions, pipeline_config=config, storage_manager=manager,
            execution_book=main.CHAMPION_EXECUTION_BOOK, execution_destination="PAPER",
            runtime_identity=account.identity, outcome_ledger=ledger, enable_trading=True)

        assert trader.pm is portfolio and trader.position_config is positions
        assert trader.pipeline_config is config and config._funded_live_execution is False
        assert trader.execution_destination == "PAPER" and trader._is_current_refit_demo is False
        assert trader._runtime_account_identity == manager.runtime_identity == ledger.runtime_identity == account.identity
        assert trader._live_outcome_ledger is ledger and trader._live_outcome_feedback_initialized is True
        assert trader._runtime_state_dir == manager.output_dir
        assert account.identity.namespace in ledger.state_path.parts
        assert trader._running is False and not trader._shutdown_event.is_set()
        assert trader.trade_log == [] and trader._held_owner_contexts == {}
        assert ledger.paper_positions() == ledger.unresolved_pre_send_intents() == []
        assert sampled.call_count > 0
        fresh, reason = trader._fresh_entry_account_validation(connector.get_account_info())
        assert reason is None and fresh.login == account.identity.login
        assert fresh.runtime_namespace == account.identity.namespace
        assert fresh.sampled_broker_equity == fresh.equity == 10000.0
        assert fresh.destination == "PAPER"
        assert trader._get_live_configs() == {}
        assert portfolio.symbol_configs == {} and not config._funded_live_execution
        assert getattr(portfolio, "_last_promotion_authorization").valid is False
        assert ledger.paper_positions() == []
        # Fresh missing margin history retains the existing protective latch.
        assert trader._margin_reopen_required is True and trader._margin_state == "BLOCKED"
    finally:
        ledger.close()
    replacement = world.ledger(account.identity, config)
    assert replacement.runtime_identity == account.identity
