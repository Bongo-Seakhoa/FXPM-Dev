"""Inventory the funded configuration surface and its runtime consumers.

This is a static governance audit, not a trading or optimization command.  It
maps every shipped top-level/section key plus the grouped instrument-spec leaf
schema to the Python files that mention it and labels its current production
decision effect.  The labels are deliberately conservative: a key is called a
no-op only when the audit has a reviewed explicit entry for it.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pm_core import PipelineConfig  # noqa: E402
from pm_mt5 import MT5Config  # noqa: E402
from pm_position import PositionConfig  # noqa: E402


REVIEWED_NO_OPS = {
    "pipeline.walk_forward_audit_mode": (
        "normalized by PipelineConfig but never read by the audit builder, "
        "promotion classifier, readiness gate, or live runtime"
    ),
    "pipeline.walk_forward_do_not_displace_latest_surface": (
        "writes a descriptive validation-evidence boolean but never changes "
        "selection, promotion, or live behavior"
    ),
    "pipeline.false_discovery_scope_power": (
        "normalized and fingerprinted but not used by DSR, PBO, bootstrap, "
        "candidate scoring, or promotion gates"
    ),
}

CALENDAR_INACTIVE_ROW_SPLIT_KEYS = {
    "active_stage2_pct",
    "active_universe_bars",
    "clean_holdout_pct",
    "final_lockbox_bars",
    "historical_stress_audit_bars",
    "max_bars",
    "oos_aggregate_policy",
    "oos_embargo_bars",
    "oos_kfold_splits",
    "overlap_pct",
    "split_embargo_bars",
    "tuning_validation_bars",
    "walk_forward_validation_bars",
}

CONDITIONALLY_INACTIVE_KEYS = {
    "feature_disk_cache_precompute_enabled": "disabled in the shipped profile",
    "feature_disk_cache_precompute_workers": "precompute is disabled",
    "local_governance_live_mode": "local governance is disabled",
    "local_governance_tournament_enabled": "disabled in the shipped profile",
    "promote_small_risk_multiplier": "PROMOTE_SMALL is not admitted live",
    "risk_management_optimization_enabled": "disabled in the shipped profile",
}


def _source_files(root: Path) -> List[Path]:
    files = list(root.glob("*.py"))
    files.extend((root / "pm_dashboard").glob("*.py"))
    files.extend((root / "tools").glob("*.py"))
    return sorted(
        path
        for path in files
        if "archive" not in path.parts and path.name != Path(__file__).name
    )


def _consumer_locations(
    key: str,
    *,
    root: Path,
    sources: Sequence[Path],
) -> Tuple[int, str]:
    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(key)}(?![A-Za-z0-9_])")
    locations: List[str] = []
    hits = 0
    for path in sources:
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        matching = [index for index, line in enumerate(lines, 1) if pattern.search(line)]
        if matching:
            hits += len(matching)
            shown = ",".join(str(index) for index in matching[:8])
            suffix = ",..." if len(matching) > 8 else ""
            locations.append(f"{path.relative_to(root)}:{shown}{suffix}")
    return hits, "; ".join(locations)


def _stage(section: str, key: str) -> str:
    if section == "_meta":
        return "documentation"
    if section == "mt5":
        return "deployment_identity"
    if section in {"instrument_specs", "instrument_spec_defaults"}:
        return "broker_execution_math"
    if section == "position":
        return "live_position_math"
    if key.startswith(("evidence_", "false_discovery_", "oos_", "lockbox_")):
        return "statistical_evidence"
    if key.startswith(("production_promotion_", "production_replay_", "production_portfolio_")):
        return "promotion_authority"
    if key.startswith(("live_", "manual_", "margin_", "probation_")):
        return "live_control"
    if key.startswith(("feature_", "optimization_", "optuna_")):
        return "compute_or_search_budget"
    if key.startswith(("regime_", "scoring_", "fx_", "train_", "exceptional_")):
        return "candidate_selection"
    if key.startswith("storage_") or key.endswith("_filename"):
        return "storage_operations"
    if key in {"data_dir", "output_dir", "log_dir", "winner_ledger_path"}:
        return "artifact_path"
    return "runtime_policy"


def _effect(section: str, key: str, pipeline: Mapping[str, Any]) -> Tuple[str, str]:
    path = f"{section}.{key}"
    if path in REVIEWED_NO_OPS:
        return "reviewed_no_op", REVIEWED_NO_OPS[path]
    if section == "_meta":
        return "documentation_only", "not passed to a runtime dataclass"
    if section == "pipeline" and key in CALENDAR_INACTIVE_ROW_SPLIT_KEYS:
        if str(pipeline.get("production_split_mode", "")).lower() == "calendar_lockbox_v2":
            reason = (
                "purged-k-fold-only control; funded OOS mode is the fixed one-shot lockbox"
                if key.startswith("oos_")
                else "legacy fixed-row splitter input; calendar windows use native timestamps"
            )
            return (
                "research_only_in_current_profile",
                reason,
            )
    if section == "pipeline" and key in CONDITIONALLY_INACTIVE_KEYS:
        return "inactive_current_profile", CONDITIONALLY_INACTIVE_KEYS[key]
    if section == "pipeline" and key == "research_mode":
        return "fixed_profile_marker", "must remain false for funded production"
    if section in {"symbols", "broker_specs_path"}:
        return "active", "deployment universe/provenance input"
    return "active_or_fail_closed_gate", "value is consumed, fingerprinted, or readiness-gated"


def _grouped_spec_leaf_keys(specs: Mapping[str, Any]) -> List[str]:
    result = set()
    for payload in specs.values():
        if isinstance(payload, Mapping):
            result.update(str(key) for key in payload)
    return sorted(result)


def build_rows(root: Path, config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    sources = _source_files(root)
    pipeline = config.get("pipeline") if isinstance(config.get("pipeline"), Mapping) else {}
    rows: List[Dict[str, Any]] = []

    sections: Iterable[Tuple[str, Any]] = (
        ("_meta", config.get("_meta") or {}),
        ("pipeline", pipeline),
        ("position", config.get("position") or {}),
        ("mt5", config.get("mt5") or {}),
        ("instrument_spec_defaults", config.get("instrument_spec_defaults") or {}),
    )
    for section, payload in sections:
        if not isinstance(payload, Mapping):
            continue
        for key, value in payload.items():
            hits, locations = _consumer_locations(str(key), root=root, sources=sources)
            effect, reason = _effect(section, str(key), pipeline)
            rows.append(
                {
                    "path": f"{section}.{key}",
                    "section": section,
                    "key": str(key),
                    "stage": _stage(section, str(key)),
                    "current_effect": effect,
                    "effect_reason": reason,
                    "consumer_hits": hits,
                    "consumer_locations": locations,
                    "configured_value": json.dumps(value, sort_keys=True, default=str),
                }
            )

    for key in ("broker_specs_path", "symbols"):
        if key not in config:
            continue
        hits, locations = _consumer_locations(key, root=root, sources=sources)
        effect, reason = _effect(key, key, pipeline)
        rows.append(
            {
                "path": key,
                "section": "top_level",
                "key": key,
                "stage": "deployment_universe",
                "current_effect": effect,
                "effect_reason": reason,
                "consumer_hits": hits,
                "consumer_locations": locations,
                "configured_value": json.dumps(config[key], sort_keys=True, default=str),
            }
        )

    specs = config.get("instrument_specs") or {}
    if isinstance(specs, Mapping):
        for symbol in specs:
            rows.append(
                {
                    "path": f"instrument_specs.{symbol}",
                    "section": "instrument_specs",
                    "key": str(symbol),
                    "stage": "broker_execution_math",
                    "current_effect": "active_or_fail_closed_gate",
                    "effect_reason": "symbol-specific pip, tick, volume, spread, and swap contract",
                    "consumer_hits": 0,
                    "consumer_locations": "pm_main.set_instrument_specs -> pm_core instrument registry",
                    "configured_value": json.dumps(specs[symbol], sort_keys=True, default=str),
                }
            )
        for leaf in _grouped_spec_leaf_keys(specs):
            hits, locations = _consumer_locations(leaf, root=root, sources=sources)
            rows.append(
                {
                    "path": f"instrument_specs.*.{leaf}",
                    "section": "instrument_specs",
                    "key": leaf,
                    "stage": "broker_execution_math",
                    "current_effect": "active_or_fail_closed_gate",
                    "effect_reason": "grouped instrument-spec schema leaf",
                    "consumer_hits": hits,
                    "consumer_locations": locations,
                    "configured_value": "<per-symbol>",
                }
            )
    return sorted(rows, key=lambda row: row["path"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--output", default="agent_audit/configuration/funded_config_consumer_map.csv")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    config_path = (root / args.config).resolve()
    output_path = (root / args.output).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    rows = build_rows(root, config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "row_count": len(rows),
        "configured_pipeline_keys": len(config.get("pipeline") or {}),
        "pipeline_dataclass_fields": len(fields(PipelineConfig)),
        "configured_position_keys": len(config.get("position") or {}),
        "position_dataclass_fields": len(fields(PositionConfig)),
        "configured_mt5_keys": len(config.get("mt5") or {}),
        "mt5_dataclass_fields": len(fields(MT5Config)),
        "reviewed_no_ops": [row["path"] for row in rows if row["current_effect"] == "reviewed_no_op"],
        "research_only_current_profile": [
            row["path"]
            for row in rows
            if row["current_effect"] == "research_only_in_current_profile"
        ],
        "output": str(output_path),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
