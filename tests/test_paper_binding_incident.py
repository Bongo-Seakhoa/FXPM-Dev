"""One synthetic native-binding incident through the real PAPER adapter and ledger.

Accounts and native replies are invented. This is durable software metadata,
not broker evidence, trading authority or an acknowledgement/reset protocol.
"""

from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
import sys

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import test_paper_tick_protection as tape_fixture
from test_paper_tick_protection import tick_world, world

backend, outcomes = tape_fixture.backend, tape_fixture.outcomes
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"


def test_actual_binding_incident_overwrites_bounded_metadata_and_survives_cold_reopen(tick_world):
    account = tick_world.account("EURUSD")
    tape_fixture._open(account, tick_world.tapes["EURUSD"])
    positions = deepcopy(account.ledger.paper_positions())
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    assert account.ledger.paper_binding_incident() is None
    bound = {name: getattr(account.identity, name) for name in ("login", "server", "currency", "destination")}
    native_account = tick_world.fake.accounts[account.login]
    native_account.login = account.login + 100
    with pytest.raises(backend.NativeAccountBindingError) as first_error:
        account.paper.get_account_info()
    first = account.ledger.paper_binding_incident()
    assert set(first) == {"first_seen_utc", "last_seen_utc", "count", "observed_identity", "bound_identity", "reason"}
    assert first["count"] == 1 and first["first_seen_utc"] == first["last_seen_utc"]
    assert first["observed_identity"] == dict(login=account.login + 100,
        server=native_account.server.strip(), currency=native_account.currency.strip().upper())
    assert first["bound_identity"] == bound and first["reason"] == str(first_error.value)

    native_account.login = account.login + 101
    with pytest.raises(backend.NativeAccountBindingError) as second_error:
        account.paper.get_account_info()
    retained = account.ledger.paper_binding_incident()
    assert retained["count"] == 2 and retained["first_seen_utc"] == first["first_seen_utc"]
    assert datetime.fromisoformat(retained["last_seen_utc"].replace("Z", "+00:00")) >= datetime.fromisoformat(first["last_seen_utc"].replace("Z", "+00:00"))
    assert retained["observed_identity"] == {**first["observed_identity"], "login": account.login + 101}
    assert retained["bound_identity"] == bound and retained["reason"] == str(second_error.value)
    copied = account.ledger.paper_binding_incident()
    copied["count"] = 0
    copied["observed_identity"]["login"] = -1
    copied["bound_identity"]["server"] = "FORGED"
    assert account.ledger.paper_binding_incident() == retained

    prior_bytes = account.ledger.state_path.read_bytes()
    with pytest.raises(ValueError, match="another namespace"):
        account.ledger.record_paper_binding_incident(
            observed_identity=None, bound_identity={**bound, "login": account.login + 1},
            reason="SYNTHETIC_INVALID_BOUND", observed_at_utc=retained["last_seen_utc"])
    assert account.ledger.state_path.read_bytes() == prior_bytes
    assert account.ledger.paper_binding_incident() == retained
    assert account.ledger.paper_positions() == positions
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals
    assert {path.name for path in account.ledger.state_path.parent.iterdir()} == {account.ledger.state_path.name}

    native_account.login = account.login
    assert account.paper.get_account_info().login == account.login
    assert account.ledger.paper_binding_incident() == retained
    account.ledger.close()
    account.ledger = tick_world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    assert account.ledger.paper_binding_incident() == retained
    assert account.ledger.paper_positions() == positions
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals
