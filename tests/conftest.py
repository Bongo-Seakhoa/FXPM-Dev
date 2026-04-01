"""Pytest test-path bootstrap.

Ensures repo-root modules (e.g. pm_core.py) are importable when tests are
invoked via `pytest` as well as `python -m pytest`.
"""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
ROOT_STR = str(ROOT)
if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)

