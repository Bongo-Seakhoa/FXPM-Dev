#!/usr/bin/env python3
"""Build the separate latest-data PAPER_ONLY book after a joint final PASS.

The command never changes champion cell bytes.  It reads the released all-or-
none funded ledger, reuses only each champion's strategy family, fits a latest
five-year challenger, and writes a new content-addressed ledger whose
``edge_challenger_book`` is executable only by ``pm_main.py --trade --paper``
on a broker-verified demo account.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Mapping

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pm_core import FeatureComputer, PipelineConfig  # noqa: E402
from pm_model_lifecycle import (  # noqa: E402
    EDGE_CHALLENGER_ROLE,
    build_empty_edge_challenger_book,
    build_lockbox_champion_lineage,
)
from pm_pipeline import OptimizationPipeline, SymbolConfig, _stable_hash  # noqa: E402
from pm_portfolio_replay import portfolio_replay_evidence_seal_valid  # noqa: E402
from pm_two_phase_lockbox import (  # noqa: E402
    canonical_json_sha256,
    load_pinned_prelockbox_roster,
    sha256_file,
    validate_global_final_lockbox_access,
    validate_released_ledger_against_prelockbox,
)


def _pipeline_config(config_path: Path, payload: Mapping[str, Any]) -> PipelineConfig:
    allowed = {field.name for field in fields(PipelineConfig)}
    pipeline = dict(payload.get("pipeline") or {})
    cfg = PipelineConfig(**{key: value for key, value in pipeline.items() if key in allowed})
    cfg._deployment_config_path = str(config_path.resolve())
    return cfg


def _resolve(raw: str, config_path: Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else config_path.resolve().parent / path


def _champion_cells(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(payload.get("regime_configs") or {}))


def source_excluded_edge_book(
    symbol_payload: Mapping[str, Any],
    *,
    portfolio_evidence_sha256: str,
    prelockbox_roster_sha256: str,
) -> Dict[str, Any]:
    """Build a non-executable empty edge book for a source-excluded symbol."""
    artifact = (
        symbol_payload.get("artifact_meta")
        if isinstance(symbol_payload.get("artifact_meta"), Mapping)
        else {}
    )
    data_window = (
        symbol_payload.get("data_window_meta")
        if isinstance(symbol_payload.get("data_window_meta"), Mapping)
        else {}
    )
    binding = dict(
        artifact.get("native_exclusion_binding")
        or data_window.get("native_exclusion_binding")
        or {}
    )
    if not binding:
        raise ValueError("symbol is not manifest-bound as source excluded")
    reason_codes = sorted(
        {str(value) for value in (binding.get("reason_codes") or []) if str(value)}
    )
    book = build_empty_edge_challenger_book(data_window)
    book.update(
        {
            "generation_status": "SOURCE_EXCLUDED_NO_TRADE",
            "funded_role_eligible": False,
            "paper_only": True,
            "source_joint_portfolio_evidence_sha256": str(
                portfolio_evidence_sha256
            ).lower(),
            "source_prelockbox_roster_sha256": str(
                prelockbox_roster_sha256
            ).lower(),
            "native_exclusion_binding": binding,
            "native_exclusion_binding_sha256": canonical_json_sha256(binding),
            "source_exclusion_reason_codes": reason_codes,
            "regime_configs": {},
            "generated_cells": 0,
            "generation_failures": [
                "source_excluded_before_model_search"
                + (":" + ",".join(reason_codes) if reason_codes else "")
            ],
        }
    )
    return book


def validate_edge_book_is_paper_only(book: Mapping[str, Any]) -> list[str]:
    """Reject any challenger bytes that could masquerade as funded winners."""
    errors: list[str] = []
    if str(book.get("book_role") or "").upper() != EDGE_CHALLENGER_ROLE:
        errors.append("EDGE_BOOK_ROLE_INVALID")
    if book.get("paper_only") is not True:
        errors.append("EDGE_BOOK_NOT_PAPER_ONLY")
    if book.get("funded_role_eligible") is not False:
        errors.append("EDGE_BOOK_CLAIMS_FUNDED_ELIGIBILITY")
    timeframes = book.get("regime_configs")
    if not isinstance(timeframes, Mapping):
        return errors + ["EDGE_BOOK_GRID_INVALID"]
    for timeframe, regimes in timeframes.items():
        if not isinstance(regimes, Mapping):
            errors.append(f"EDGE_BOOK_REGIME_MAP_INVALID:{timeframe}")
            continue
        for regime, cell in regimes.items():
            cell_id = f"{str(timeframe).upper()}|{str(regime).upper()}"
            if not isinstance(cell, Mapping):
                errors.append(f"EDGE_CELL_INVALID:{cell_id}")
                continue
            artifact = (
                cell.get("artifact_meta")
                if isinstance(cell.get("artifact_meta"), Mapping)
                else {}
            )
            evidence = (
                cell.get("validation_evidence")
                if isinstance(cell.get("validation_evidence"), Mapping)
                else {}
            )
            temporal = (
                evidence.get("temporal_lineage")
                if isinstance(evidence.get("temporal_lineage"), Mapping)
                else {}
            )
            if str(cell.get("promotion_status") or "").upper() != "PAPER_ONLY":
                errors.append(f"EDGE_CELL_STATUS_INVALID:{cell_id}")
            if artifact.get("edge_challenger") is not True:
                errors.append(f"EDGE_CELL_FLAG_MISSING:{cell_id}")
            if artifact.get("funded_role_eligible") is not False:
                errors.append(f"EDGE_CELL_CLAIMS_FUNDED_ELIGIBILITY:{cell_id}")
            if str(temporal.get("book_role") or "").upper() != EDGE_CHALLENGER_ROLE:
                errors.append(f"EDGE_CELL_TEMPORAL_ROLE_INVALID:{cell_id}")
            if str(temporal.get("contract_status") or "").upper() != "PAPER_ONLY":
                errors.append(f"EDGE_CELL_TEMPORAL_STATUS_INVALID:{cell_id}")
            if temporal.get("funded_role_eligible") is not False:
                errors.append(f"EDGE_CELL_TEMPORAL_FUNDED_INVALID:{cell_id}")
            if temporal.get("paper_only") is not True:
                errors.append(f"EDGE_CELL_TEMPORAL_PAPER_INVALID:{cell_id}")
            if temporal.get("performance_evidenced_through_utc") not in (None, ""):
                errors.append(f"EDGE_CELL_INHERITED_EVIDENCE:{cell_id}")
            if temporal.get("champion_lockbox_verdict_inherited") is not False:
                errors.append(f"EDGE_CELL_INHERITED_VERDICT:{cell_id}")
            candidate_id = str(artifact.get("candidate_id") or "").strip()
            fit_cutoff = temporal.get("fit_cutoff_utc")
            expected_id = _stable_hash(
                {
                    "book_role": EDGE_CHALLENGER_ROLE,
                    "symbol": str(book.get("symbol") or "").upper(),
                    "timeframe": str(timeframe).upper(),
                    "regime": str(regime).upper(),
                    "strategy_name": str(cell.get("strategy_name") or ""),
                    "parameters": dict(cell.get("parameters") or {}),
                    "fit_cutoff_utc": fit_cutoff,
                }
            )
            # ``symbol`` is attached to the book immediately before this
            # validator is called.  Identity mismatch is therefore fatal.
            if not candidate_id or candidate_id != expected_id:
                errors.append(f"EDGE_CELL_IDENTITY_INVALID:{cell_id}")
    return errors


def attach_edge_book_preserving_champions(
    ledger: Mapping[str, Any],
    books_by_symbol: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    output = copy.deepcopy(dict(ledger))
    before = {
        str(symbol).upper(): _champion_cells(payload)
        for symbol, payload in ledger.items()
        if isinstance(payload, Mapping)
    }
    for raw_symbol, book in books_by_symbol.items():
        symbol = str(raw_symbol).upper()
        if symbol not in output or not isinstance(output[symbol], dict):
            raise ValueError(f"released ledger symbol missing: {symbol}")
        canonical_book = copy.deepcopy(dict(book))
        canonical_book["symbol"] = symbol
        book_errors = validate_edge_book_is_paper_only(canonical_book)
        if book_errors:
            raise ValueError(
                f"edge challenger contract invalid for {symbol}: "
                + ", ".join(book_errors)
            )
        output[symbol]["edge_challenger_book"] = canonical_book
    after = {
        str(symbol).upper(): _champion_cells(payload)
        for symbol, payload in output.items()
        if isinstance(payload, Mapping)
    }
    if canonical_json_sha256(before) != canonical_json_sha256(after):
        raise RuntimeError("post-lockbox edge build mutated champion cell bytes")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config.json"))
    parser.add_argument("--ledger", default="")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "agent_audit" / "performance" / "data" / "edge_challengers"),
    )
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    config_payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    pipeline_payload = dict(config_payload.get("pipeline") or {})
    ledger_raw = str(args.ledger or pipeline_payload.get("winner_ledger_path") or "").strip()
    if not ledger_raw:
        raise SystemExit("released funded ledger path is required")
    ledger_path = _resolve(ledger_raw, config_path)
    ledger = json.loads(ledger_path.read_text(encoding="utf-8-sig"))
    symbols = [str(value).strip().upper() for value in config_payload.get("symbols") or []]
    prelockbox, pre_path, pre_errors = load_pinned_prelockbox_roster(
        config_payload=config_payload,
        config_path=config_path,
        expected_symbols=symbols,
    )
    if pre_errors or not isinstance(prelockbox, Mapping):
        raise SystemExit("pre-lockbox roster invalid: " + ", ".join(pre_errors))
    evidence_raw = str(
        pipeline_payload.get("production_portfolio_replay_evidence_path") or ""
    ).strip()
    if not evidence_raw:
        raise SystemExit("joint final portfolio evidence is not configured")
    evidence_path = _resolve(evidence_raw, config_path)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8-sig"))
    evidence_file_sha = sha256_file(evidence_path)
    if (
        not portfolio_replay_evidence_seal_valid(evidence)
        or str(evidence.get("verdict") or "").upper() != "PASS"
        or evidence.get("failures")
    ):
        raise SystemExit("joint final portfolio evidence is not a sealed PASS")
    cfg = _pipeline_config(config_path, config_payload)
    expected_pre_file_sha = str(
        pipeline_payload.get("production_prelockbox_roster_sha256") or ""
    ).lower()
    pre_lineage = evidence.get("lineage") if isinstance(evidence.get("lineage"), Mapping) else {}
    replay = evidence.get("replay") if isinstance(evidence.get("replay"), Mapping) else {}
    replay_window = replay.get("window") if isinstance(replay.get("window"), Mapping) else {}
    roster = prelockbox.get("roster") if isinstance(prelockbox.get("roster"), Mapping) else {}
    binding_errors: list[str] = []
    if pre_path is None or sha256_file(pre_path).lower() != expected_pre_file_sha:
        binding_errors.append("PRELOCKBOX_FILE_BINDING_INVALID")
    if str(pre_lineage.get("prelockbox_artifact_file_sha256") or "").lower() != expected_pre_file_sha:
        binding_errors.append("JOINT_PASS_PRELOCKBOX_FILE_MISMATCH")
    if str(pre_lineage.get("prelockbox_artifact_seal_sha256") or "").lower() != str(
        (prelockbox.get("content_seal") or {}).get("sha256") or ""
    ).lower():
        binding_errors.append("JOINT_PASS_PRELOCKBOX_SEAL_MISMATCH")
    if str(pre_lineage.get("prelockbox_roster_sha256") or "").lower() != str(
        roster.get("roster_sha256") or ""
    ).lower():
        binding_errors.append("JOINT_PASS_ROSTER_MISMATCH")
    if bool(replay.get("partial_run")) or bool(replay.get("allow_cell_errors")):
        binding_errors.append("JOINT_PASS_WAS_PARTIAL_OR_PERMISSIVE")
    if str(replay_window.get("partition") or "") != "final_lockbox":
        binding_errors.append("JOINT_PASS_PARTITION_INVALID")
    if sorted(replay.get("replayed_cell_ids") or []) != sorted(
        roster.get("tradeable_cell_ids") or []
    ):
        binding_errors.append("JOINT_PASS_REPLAYED_ROSTER_MISMATCH")
    binding_errors.extend(
        validate_global_final_lockbox_access(
            config=cfg,
            config_path=config_path,
            access_id=str(replay.get("global_lockbox_access_id") or ""),
            frozen_ledger=prelockbox.get("frozen_ledger") or {},
            roster_sha256=str(roster.get("roster_sha256") or ""),
            prelockbox_file_sha256=expected_pre_file_sha,
        )
    )
    release_errors = validate_released_ledger_against_prelockbox(
        ledger,
        prelockbox,
        expected_portfolio_evidence_sha256=evidence_file_sha,
    )
    if binding_errors:
        raise SystemExit(
            "joint PASS is not bound to the exact global roster/access: "
            + ", ".join(sorted(set(binding_errors)))
        )
    if release_errors:
        raise SystemExit(
            "released ledger is not the exact all-or-none PASS release: "
            + ", ".join(release_errors)
        )

    pipeline = OptimizationPipeline(cfg)
    books: Dict[str, Dict[str, Any]] = {}
    for index, symbol in enumerate(symbols, start=1):
        raw_symbol = ledger.get(symbol)
        if not isinstance(raw_symbol, Mapping):
            raise SystemExit(f"released ledger missing symbol {symbol}")
        raw_artifact = (
            raw_symbol.get("artifact_meta")
            if isinstance(raw_symbol.get("artifact_meta"), Mapping)
            else {}
        )
        if raw_artifact.get("native_exclusion_binding"):
            book = source_excluded_edge_book(
                raw_symbol,
                portfolio_evidence_sha256=evidence_file_sha,
                prelockbox_roster_sha256=str(roster.get("roster_sha256") or ""),
            )
            books[symbol] = book
            print(
                f"[{index}/{len(symbols)}] {symbol}: source-excluded NO_TRADE; "
                "empty PAPER_ONLY edge book preserved"
            )
            continue
        champion = SymbolConfig.from_dict(dict(raw_symbol))
        parent_temporal = build_lockbox_champion_lineage(
            dict(raw_symbol.get("data_window_meta") or {})
        )
        if str(parent_temporal.get("contract_status") or "").upper() != "PASS":
            raise SystemExit(f"invalid released champion temporal lineage: {symbol}")
        data_by_tf, data_meta = pipeline._load_symbol_timeframe_data(symbol)
        if not data_by_tf:
            raise SystemExit(f"no active native data for post-lockbox refit: {symbol}")
        fit_end = pd.Timestamp(data_meta.get("as_of_utc"))
        fit_start = fit_end - pd.DateOffset(years=5)
        generated: Dict[str, Dict[str, Any]] = {}
        failures: list[str] = []
        for timeframe, regime_configs in champion.regime_configs.items():
            data = data_by_tf.get(timeframe)
            if data is None or len(data) < 200:
                failures.append(f"{timeframe}:native_data_missing_or_short")
                continue
            features = FeatureComputer.compute_all(
                data,
                symbol=symbol,
                timeframe=timeframe,
                regime_params_file=cfg.regime_params_file,
            )
            edge, edge_failures = pipeline._build_edge_challengers_for_timeframe(
                symbol=symbol,
                timeframe=timeframe,
                champion_configs=regime_configs,
                full_features=features,
                fit_start=fit_start,
                fit_end=fit_end,
                champion_temporal_lineage=parent_temporal,
            )
            if edge:
                generated[timeframe] = edge
            failures.extend(edge_failures)
            FeatureComputer.clear_cache()
        book = build_empty_edge_challenger_book(data_meta)
        book.update(
            {
                "generation_status": (
                    "COMPLETE" if generated and not failures else "PARTIAL" if generated else "NO_ELIGIBLE_CHALLENGER"
                ),
                "funded_role_eligible": False,
                "paper_only": True,
                "source_joint_portfolio_evidence_sha256": evidence_file_sha,
                "source_prelockbox_roster_sha256": roster.get("roster_sha256"),
                "parent_champion_temporal_lineage_sha256": canonical_json_sha256(
                    parent_temporal
                ),
                "regime_configs": generated,
                "generated_cells": sum(len(regimes) for regimes in generated.values()),
                "generation_failures": failures,
            }
        )
        books[symbol] = book
        print(f"[{index}/{len(symbols)}] {symbol}: {book['generated_cells']} PAPER_ONLY cells")
        pipeline.data_loader.clear_cache()

    output = attach_edge_book_preserving_champions(ledger, books)
    post_errors = validate_released_ledger_against_prelockbox(output, prelockbox)
    if post_errors:
        raise SystemExit("edge output mutated funded roster: " + ", ".join(post_errors))
    digest = canonical_json_sha256(output)
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"released_ledger_with_edge_challengers_{digest}.json"
    serialized = json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False)
    try:
        with out_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.write("\n")
    except FileExistsError:
        if json.loads(out_path.read_text(encoding="utf-8")) != output:
            raise RuntimeError("content-addressed edge ledger collision")
    print(json.dumps({
        "path": str(out_path),
        "sha256": sha256_file(out_path),
        "paper_only_cells": sum(book["generated_cells"] for book in books.values()),
        "champion_cells_mutated": False,
        "next_step": "Optionally pin this ledger, renew detached authorization for its exact bytes, then use --trade --paper on DEMO only.",
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
