"""Warm FxPM's disk-backed feature cache for optimization runs.

This is intentionally evidence-only: it computes/retrieves feature frames and
does not write winner ledgers or promote strategies.
"""

import argparse
import json
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict


REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from pm_core import PipelineConfig
from pm_pipeline import PortfolioManager


def _load_config(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be an object: {path}")
    return data


def _pipeline_config(data: Dict[str, Any]) -> PipelineConfig:
    pipeline = data.get("pipeline", {})
    if not isinstance(pipeline, dict):
        pipeline = {}
    allowed = {field.name for field in fields(PipelineConfig)}
    return PipelineConfig(**{key: value for key, value in pipeline.items() if key in allowed})


def main() -> None:
    parser = argparse.ArgumentParser(description="Precompute disk-backed FxPM feature cache.")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--workers", type=int, default=None)
    args = parser.parse_args()

    config_path = (REPO / args.config).resolve()
    data = _load_config(config_path)
    cfg = _pipeline_config(data)
    if args.workers is not None:
        cfg.feature_disk_cache_precompute_workers = max(1, int(args.workers))
    symbols = args.symbols or list(data.get("symbols", []) or [])
    if not symbols:
        raise SystemExit("No symbols supplied and config has no symbols list")

    manager = PortfolioManager(
        cfg,
        symbols,
        config_file=str(REPO / "pm_outputs" / "production_live" / "precompute_feature_cache_status.json"),
    )
    stats = manager._precompute_feature_cache_for_symbols(symbols)
    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
