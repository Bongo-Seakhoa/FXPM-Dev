from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_main import LiveTrader, RuntimeObservabilityStore
from pm_demo_readiness import _requested_symbol_errors
from pm_pipeline import PortfolioManager


class _RuntimePortfolio:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def update_live_observability(
        self, symbol, timeframe, regime, updates, *, save=False
    ):
        self.calls.append(save)
        return {"baseline": 1, **updates}


def test_live_runtime_observability_never_mutates_authority_ledger(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "winner_book.json"
    ledger.write_bytes(b'{"authority":"promoted"}\n')
    before = hashlib.sha256(ledger.read_bytes()).hexdigest()

    trader = LiveTrader.__new__(LiveTrader)
    trader.pm = _RuntimePortfolio()
    trader.logger = logging.getLogger("runtime-authority-isolation")
    trader._runtime_observability = RuntimeObservabilityStore(
        tmp_path / "runtime" / "live_observability.json"
    )

    observed = trader._record_runtime_observability(
        "EURUSD",
        "M15",
        "TREND",
        {"recent_consecutive_losses": 2},
    )

    assert trader.pm.calls == [False]
    assert observed == {"baseline": 1, "recent_consecutive_losses": 2}
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
    assert trader._runtime_observability.get("EURUSD", "M15", "TREND") == observed

    persisted = RuntimeObservabilityStore(
        tmp_path / "runtime" / "live_observability.json"
    )
    assert persisted.get("EURUSD", "M15", "TREND") == observed


def test_portfolio_manager_rejects_telemetry_ledger_save() -> None:
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.symbol_configs = {
        "EURUSD": SimpleNamespace(
            has_regime_configs=lambda: True,
            get_regime_config=lambda _tf, _regime: SimpleNamespace(
                live_observability={}
            ),
        )
    }

    with pytest.raises(
        RuntimeError, match="RUNTIME_OBSERVABILITY_AUTHORITY_MUTATION_FORBIDDEN"
    ):
        manager.update_live_observability(
            "EURUSD", "M15", "TREND", {"x": 1}, save=True
        )


def test_runtime_observability_store_is_content_bound_and_record_bounded(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observability.json"
    store = RuntimeObservabilityStore(path)
    store.update("EURUSD", "H1", "RANGE", {"metric": 1.25})
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert len(payload["content_sha256"]) == 64

    payload["records"]["EURUSD|H1|RANGE"]["observability"]["metric"] = 9.0
    path.write_text(json.dumps(payload), encoding="utf-8")
    rejected = RuntimeObservabilityStore(path)
    assert rejected.get("EURUSD", "H1", "RANGE") == {}

    bounded = RuntimeObservabilityStore(tmp_path / "bounded.json")
    with pytest.raises(ValueError, match="RUNTIME_OBSERVABILITY_RECORD_TOO_LARGE"):
        bounded.update("EURUSD", "H1", "RANGE", {"blob": "x" * (65 * 1024)})
    assert not (tmp_path / "bounded.json").exists()


def test_authority_symbol_roster_is_canonical_without_alphabetic_reordering() -> None:
    assert _requested_symbol_errors(["XAUUSD", "EURUSD", "USDJPY"]) == []
    assert _requested_symbol_errors(["xauusd", "EURUSD"]) == [
        "DEMO_SYMBOL_ROSTER_NOT_CANONICAL"
    ]
