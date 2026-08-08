"""Pytest test-path bootstrap.

Ensures repo-root modules (e.g. pm_core.py) are importable when tests are
invoked via `pytest` as well as `python -m pytest`.
"""

from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
ROOT_STR = str(ROOT)
if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)


@pytest.fixture(autouse=True)
def isolate_default_decision_throttle_log(monkeypatch, tmp_path):
    """Keep tests from reading/writing the production throttle state file."""
    monkeypatch.setenv(
        "PM_DECISION_THROTTLE_LOG_PATH",
        str(tmp_path / "last_trade_log.json"),
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_mt5: test genuinely needs a live MetaTrader 5 terminal; "
        "exempt from the fast-fail connect stub",
    )


@pytest.fixture(autouse=True)
def block_real_mt5_terminal(monkeypatch, request):
    """Never let a test wait on a MetaTrader 5 terminal that is not running.

    ``mt5.initialize()`` blocks for ~102 seconds before failing with
    "Pipe server didn't answer in 60 sec" when no terminal is present. The
    readiness path retries, so a single readiness test costs ~25 minutes of
    pure waiting. Measured 2026-08-08: six tests in
    test_profitability_readiness_blueprint accounted for 12,236 of 12,241
    seconds - 99.96% of a 3h24m suite - and every one of those seconds was
    spent waiting on a pipe, not computing anything.

    The stub preserves the outcome exactly. A real absent terminal returns
    ``False``; so does this. Tests asserting "MT5 unavailable blocks funded
    startup" still assert precisely that, they just stop paying 102 seconds
    per attempt to find out.

    Mark a test ``@pytest.mark.real_mt5`` to opt back into the real connector.
    """
    if request.node.get_closest_marker("real_mt5"):
        return
    try:
        import MetaTrader5 as mt5
    except Exception:
        return  # connector absent entirely; nothing to guard

    def _unavailable(*_args, **_kwargs):
        return False

    def _stub_last_error():
        return (-10003, "IPC initialize failed (stubbed: no terminal under test)")

    # pm_mt5 binds this module object at import, so patching the attributes
    # here reaches every production call site.
    monkeypatch.setattr(mt5, "initialize", _unavailable, raising=False)
    monkeypatch.setattr(mt5, "login", _unavailable, raising=False)
    monkeypatch.setattr(mt5, "last_error", _stub_last_error, raising=False)
    monkeypatch.setattr(mt5, "shutdown", lambda *a, **k: None, raising=False)

