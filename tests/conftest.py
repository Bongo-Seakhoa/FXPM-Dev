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

