#!/usr/bin/env python3
"""Seal the complete Phase-A winner ledger before final-lockbox access.

Run this only after the calendar-lockbox optimizer has completed every configured
symbol.  The command is offline and never reads final-lockbox returns.  It emits
a content-addressed global roster that both tuning cap calibration and the one-
shot joint final replay must consume verbatim.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pm_cap_calibration import cap_neutral_config_sha256  # noqa: E402
from pm_core import PipelineConfig, strict_pipeline_config_kwargs  # noqa: E402
from pm_pipeline import build_artifact_meta  # noqa: E402
from pm_portfolio_replay import (  # noqa: E402
    active_native_portfolio_contract,
    canonical_json_sha256,
)
from pm_two_phase_lockbox import (  # noqa: E402
    build_prelockbox_roster_artifact,
    critical_module_code_contract,
    global_final_lockbox_window,
    sha256_file,
    write_immutable_prelockbox_roster,
)


def _pipeline_config(config_path: Path, payload: Dict[str, Any]) -> PipelineConfig:
    pipeline = dict(payload.get("pipeline") or {})
    cfg = PipelineConfig(**strict_pipeline_config_kwargs(pipeline))
    cfg._deployment_config_path = str(config_path.resolve())
    return cfg


def _utc(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _nonoverlapping_registry_contract(
    cfg: PipelineConfig,
    config_path: Path,
    frozen_ledger: Mapping[str, Any],
    symbols: list[str],
) -> Dict[str, Any]:
    raw = str(getattr(cfg, "lockbox_access_registry_path", "") or "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = config_path.parent / path
    else:
        output = Path(getattr(cfg, "output_dir", "pm_outputs/production_live"))
        if not output.is_absolute():
            output = config_path.parent / output
        path = output / "lockbox_access_registry.json"
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            accesses = payload.get("accesses")
        except Exception as exc:
            raise SystemExit(
                f"Cannot prove lockbox registry pristine: {type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(accesses, dict):
            raise SystemExit("Cannot prove lockbox registry state: invalid schema")
        state_sha = sha256_file(path)
    else:
        accesses = {}
        state_sha = hashlib.sha256(b"ABSENT_PRISTINE_REGISTRY").hexdigest()
    requested_start_raw, requested_end_raw = global_final_lockbox_window(frozen_ledger)
    requested_start, requested_end = _utc(requested_start_raw), _utc(requested_end_raw)
    configured = {str(symbol).upper() for symbol in symbols}
    overlapping: list[str] = []
    for access_id, record in accesses.items():
        if not isinstance(record, Mapping):
            raise SystemExit("Cannot prove lockbox registry state: invalid access record")
        lineage = record.get("lineage") if isinstance(record.get("lineage"), Mapping) else {}
        try:
            prior_start = _utc(lineage.get("window_start"))
            prior_end = _utc(lineage.get("window_end"))
        except Exception:
            raise SystemExit("Cannot prove lockbox registry state: invalid prior interval")
        overlap = max(requested_start, prior_start) < min(requested_end, prior_end)
        if not overlap:
            continue
        cell = record.get("cell") if isinstance(record.get("cell"), Mapping) else {}
        prior_symbol = str(cell.get("symbol") or "").upper()
        if prior_symbol == "__GLOBAL_PORTFOLIO__" or prior_symbol in configured:
            overlapping.append(str(access_id))
    if overlapping:
        raise SystemExit(
            "PRELOCKBOX_FREEZE_AFTER_OVERLAPPING_FINAL_ACCESS_REFUSED: "
            + ",".join(sorted(overlapping))
        )
    return {
        "path": str(path.resolve()),
        "no_overlapping_access_at_freeze": True,
        "historical_access_count_at_freeze": len(accesses),
        "state_sha256_at_freeze": state_sha,
        "requested_window_start": requested_start.isoformat(),
        "requested_window_end": requested_end.isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config.json"))
    parser.add_argument("--ledger", default=str(ROOT / "pm_configs.json"))
    parser.add_argument("--broker-specs", default=str(ROOT / "broker_specs.json"))
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "agent_audit" / "performance" / "data" / "prelockbox_rosters"),
    )
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    ledger_path = Path(args.ledger).resolve()
    broker_path = Path(args.broker_specs).resolve()
    output_dir = Path(args.out_dir).resolve()
    config_payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    ledger = json.loads(ledger_path.read_text(encoding="utf-8-sig"))
    symbols = [
        str(symbol).strip().upper()
        for symbol in (config_payload.get("symbols") or [])
        if str(symbol).strip()
    ]
    cfg = _pipeline_config(config_path, config_payload)
    native_contract, native_errors = active_native_portfolio_contract(cfg.data_dir, symbols)
    if native_errors:
        raise SystemExit(
            "Cannot seal a global roster without a complete active native contract: "
            + ", ".join(sorted(set(native_errors)))
        )
    artifact_meta = build_artifact_meta(cfg)
    deployment_contract = dict(artifact_meta.get("deployment_contract") or {})
    registry_contract = _nonoverlapping_registry_contract(
        cfg, config_path, ledger, symbols
    )
    code_contract = critical_module_code_contract(ROOT)
    if code_contract.get("missing"):
        raise SystemExit(
            "Cannot seal roster with missing critical modules: "
            + ", ".join(code_contract["missing"])
        )
    lineage = {
        "cap_neutral_config_sha256": cap_neutral_config_sha256(config_payload),
        "source_ledger_file_sha256": sha256_file(ledger_path),
        "source_ledger_semantic_sha256": canonical_json_sha256(ledger),
        "artifact_contract_sha256": canonical_json_sha256(artifact_meta),
        "deployment_contract_sha256": canonical_json_sha256(deployment_contract),
        "broker_specs_sha256": sha256_file(broker_path),
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "lockbox_registry_at_freeze": registry_contract,
        "critical_module_code_contract": code_contract,
        "critical_module_code_contract_sha256": code_contract.get(
            "aggregate_sha256"
        ),
    }
    artifact = build_prelockbox_roster_artifact(
        ledger=ledger,
        symbols=symbols,
        lineage=lineage,
    )
    path = write_immutable_prelockbox_roster(output_dir, artifact)
    result = {
        "status": "FROZEN",
        "path": str(path),
        "file_sha256": sha256_file(path),
        "roster_sha256": (artifact.get("roster") or {}).get("roster_sha256"),
        "tradeable_cells": len((artifact.get("roster") or {}).get("tradeable_cell_ids") or []),
        "next_step": (
            "Pin path/file_sha256 as pipeline.production_prelockbox_roster_path/"
            "production_prelockbox_roster_sha256, then run synchronized replay "
            "with --partition tuning_validation."
        ),
    }
    (output_dir / "latest_prelockbox_roster_manifest.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
