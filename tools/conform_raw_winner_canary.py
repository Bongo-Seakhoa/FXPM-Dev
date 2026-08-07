#!/usr/bin/env python3
"""Seal the exact preserved-root raw-Winner canary conformance replay."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reopen the exact failed EURUSD/D1 canary root, reproduce its "
            "old-release Horizon receipt, verify all 128 frozen bundles under "
            "the reviewed mapping, and write one non-authorizing receipt."
        )
    )
    parser.add_argument(
        "--failed-root",
        default=str(
            PROJECT_ROOT
            / ".pytest-tmp"
            / "raw-winner-canary"
            / "20260723T082546Z-8e64815fd7cf0b7d"
        ),
    )
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config.json"))
    parser.add_argument(
        "--output-root",
        required=True,
        help=(
            "Fresh, nonexistent sibling directory of --failed-root. "
            "Existing paths are never reused or overwritten."
        ),
    )
    args = parser.parse_args(argv)

    from pm_raw_winner_canary_conformance import (
        RawWinnerCanaryConformanceError,
        conform_preserved_raw_winner_canary,
    )

    try:
        receipt = conform_preserved_raw_winner_canary(
            failed_root=Path(args.failed_root),
            config_path=Path(args.config),
            output_root=Path(args.output_root),
        )
    except RawWinnerCanaryConformanceError as exc:
        failure = {
            "schema_version": (
                "fxpm.raw-winner-canary-preserved-root-conformance-failure.v1"
            ),
            "status": "FAILED_CLOSED",
            "failure_code": exc.code,
            "details": list(exc.details[:8]),
            "raw_winner_invoked": False,
            "closes_named_raw_winner_gate": False,
        }
        print(json.dumps(failure, indent=2, sort_keys=True))
        return 2
    except Exception as exc:
        failure = {
            "schema_version": (
                "fxpm.raw-winner-canary-preserved-root-conformance-failure.v1"
            ),
            "status": "FAILED_CLOSED",
            "failure_code": "UNEXPECTED_CONFORMANCE_FAILURE",
            "details": [type(exc).__name__, str(exc)[:2000]],
            "raw_winner_invoked": False,
            "closes_named_raw_winner_gate": False,
        }
        print(json.dumps(failure, indent=2, sort_keys=True))
        return 2
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
