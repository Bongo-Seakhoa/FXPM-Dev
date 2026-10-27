"""Synchronized, shared-capital portfolio replay primitives.

The single-strategy backtester remains responsible for producing causal entry,
exit and cost outcomes.  This module consumes those outcomes as *candidates* and
replays their admission and cash flows through one account.  It deliberately
does not sum standalone strategy returns.

The engine is broker-independent and has no live-trading imports.  It cannot
place an order.  Its conservative stop-equity curve is balance less all open
initial-stop risks; its mark-to-market curve uses only mark observations carried
by accepted candidates.  Missing margin inputs fail closed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from itertools import islice
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

import numpy as np
import pandas as pd

from pm_metrics import EquityReductionError, reduce_equity_path
from pm_risk_admission import (
    GrossRiskLimits,
    StopRiskExposure,
    conservative_risk_capital,
    evaluate_gross_stop_risk,
    evaluate_projected_margin,
    infer_factor_groups,
)


LEGACY_PORTFOLIO_REPLAY_SCHEMA_VERSION = "2026-07-13-synchronized-shared-capital-v7"
PORTFOLIO_REPLAY_SCHEMA_VERSION = "2026-08-24-synchronized-spread-swap-v10"
PORTFOLIO_REPLAY_EVIDENCE_SCHEMA_VERSION = "2026-08-24-funded-portfolio-evidence-v8"
PORTFOLIO_REPLAY_EVIDENCE_SEAL_ALGORITHM = "sha256-canonical-json-v1"
PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION = (
    "2026-08-24-native-final-lockbox-spread-swap-position-v6"
)
PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
PORTFOLIO_REPLAY_REQUIRED_REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION = "fxpm.portfolio-replay-roster.v2"
PORTFOLIO_REPLAY_BOOK_ACCOUNTING_SCHEMA_VERSION = (
    "fxpm.portfolio-replay-book-accounting.v2"
)
CURRENT_REFIT_REPLAY_SUCCESSOR_SCHEMA_VERSION = (
    "fxpm.current-refit-synchronized-replay.v2"
)
CURRENT_REFIT_REPLAY_RECONCILIATION_SCHEMA_VERSION = (
    "fxpm.current-refit-replay-reconciliation.v2"
)
MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS = 500
MTM_MARK_COST_CONTRACT_VERSION = "causal-common-valuation-tape-v2"
MTM_SWAP_ACCRUAL_CONTRACT_VERSION = "causal-calendar-rollover-accrual-v2"
PORTFOLIO_REPLAY_INPUT_RECEIPT_VERSION = "fxpm.synchronized-replay-input-receipt.v2"
PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION = "fxpm.synchronized-capital-path.v1"
MAX_PORTFOLIO_REPLAY_CANDIDATES = 500_000
MAX_PORTFOLIO_REPLAY_TIMESTAMPS = 2_000_000
MAX_CANDIDATE_RECORD_BYTES = 64 * 1024 * 1024
CorrelationLookup = Callable[[str, str, pd.Timestamp], Optional[float]]


def canonical_json_sha256(payload: Any) -> str:
    """Hash a JSON contract deterministically (paths/mtimes are not normalized)."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _bounded_canonical_record_sha256(payload: Any) -> str:
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    digest = hashlib.sha256()
    size = 0
    for chunk in encoder.iterencode(payload):
        encoded = chunk.encode("utf-8")
        size += len(encoded)
        if size > MAX_CANDIDATE_RECORD_BYTES:
            raise ValueError("candidate record exceeds its canonical byte bound")
        digest.update(encoded)
    return digest.hexdigest()


def _framed_text_digest(domain: bytes, values: Iterable[str]) -> str:
    digest = hashlib.sha256(domain + b"\0")
    count = 0
    for value in values:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        count += 1
    digest.update(count.to_bytes(8, "big"))
    return digest.hexdigest()


def portfolio_replay_candidate_ids_sha256(identifiers: Iterable[str]) -> str:
    ordered = sorted(str(identifier) for identifier in identifiers)
    if len(ordered) > MAX_PORTFOLIO_REPLAY_CANDIDATES:
        raise ValueError("portfolio replay candidate id roster exceeds its bound")
    if len(set(ordered)) != len(ordered):
        raise ValueError("candidate_id values must be unique")
    return _framed_text_digest(b"fxpm.synchronized-replay-candidate-ids.v1", ordered)


def seal_portfolio_replay_evidence(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a content-sealed evidence record.

    The seal is intentionally not a signature.  Its purpose is to make partial
    edits and accidental overwrites detectable while exact input hashes make a
    previously valid replay stale as soon as the live deployment changes.
    """
    record = json.loads(json.dumps(dict(payload), default=str))
    record.pop("content_seal", None)
    record["schema_version"] = PORTFOLIO_REPLAY_EVIDENCE_SCHEMA_VERSION
    digest = canonical_json_sha256(record)
    record["content_seal"] = {
        "algorithm": PORTFOLIO_REPLAY_EVIDENCE_SEAL_ALGORITHM,
        "sha256": digest,
    }
    return record


def portfolio_replay_evidence_seal_valid(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    seal = payload.get("content_seal")
    if not isinstance(seal, Mapping):
        return False
    if str(seal.get("algorithm") or "") != PORTFOLIO_REPLAY_EVIDENCE_SEAL_ALGORITHM:
        return False
    unsigned = dict(payload)
    unsigned.pop("content_seal", None)
    expected = canonical_json_sha256(unsigned)
    return str(seal.get("sha256") or "").lower() == expected.lower()


def write_immutable_portfolio_replay_evidence(
    output_dir: Path,
    payload: Mapping[str, Any],
) -> Path:
    """Write one content-addressed evidence file without replacing old proof."""
    sealed = seal_portfolio_replay_evidence(payload)
    digest = str(sealed["content_seal"]["sha256"])
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"portfolio_replay_evidence_{digest}.json"
    serialized = json.dumps(sealed, indent=2, sort_keys=True, ensure_ascii=False)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.write("\n")
    except FileExistsError:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != sealed:
            raise RuntimeError(
                f"immutable evidence path already contains different bytes: {path}"
            )
    return path


def _sparse_physical_winner_rows(
    ledger: Any,
    symbols: Iterable[str],
    *,
    allowed_promotion_statuses: Iterable[str] = ("PROMOTED",),
) -> Tuple[Dict[str, Any], List[str]]:
    """Project only physical Winner leaves; possible slots live in Book."""
    requested = sorted(
        {str(symbol).strip() for symbol in symbols if str(symbol).strip()}
    )
    allowed = {str(status).strip().upper() for status in allowed_promotion_statuses}
    payload_by_symbol: Dict[str, Mapping[str, Any]] = {}
    errors: List[str] = []
    if not isinstance(ledger, Mapping):
        return {
            "symbols": requested,
            "rows": [],
            "tradeable_cell_ids": [],
            "physical_winner_cell_count": 0,
        }, ["LEDGER_NOT_OBJECT"]
    if isinstance(ledger, Mapping):
        for raw_symbol, raw_payload in ledger.items():
            if not isinstance(raw_payload, Mapping):
                errors.append(
                    f"LEDGER_SYMBOL_PAYLOAD_INVALID:{str(raw_symbol)}"
                )
                continue
            ledger_symbol = str(raw_symbol).strip()
            symbol = str(raw_payload.get("symbol") or ledger_symbol).strip()
            if symbol != ledger_symbol:
                errors.append(
                    f"LEDGER_SYMBOL_IDENTITY_MISMATCH:{ledger_symbol}:{symbol}"
                )
                continue
            if symbol:
                if symbol in payload_by_symbol:
                    errors.append(f"LEDGER_SYMBOL_DUPLICATE:{symbol}")
                payload_by_symbol[symbol] = raw_payload

    rows: List[Dict[str, Any]] = []
    tradeable_ids: List[str] = []
    requested_set = set(requested)
    for symbol in sorted(payload_by_symbol):
        if symbol not in requested_set:
            errors.append(f"LEDGER_SYMBOL_OUTSIDE_BOOK:{symbol}")
            continue
        symbol_payload = payload_by_symbol[symbol]
        grid = symbol_payload.get("regime_configs")
        if not isinstance(grid, Mapping):
            errors.append(f"LEDGER_CELL_GRID_INVALID:{symbol}")
            continue
        for raw_timeframe, timeframe_payload in grid.items():
            timeframe = str(raw_timeframe).strip().upper()
            if timeframe not in PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES:
                errors.append(f"LEDGER_TIMEFRAME_INVALID:{symbol}|{timeframe}")
                continue
            if not isinstance(timeframe_payload, Mapping):
                errors.append(f"LEDGER_TIMEFRAME_PAYLOAD_INVALID:{symbol}|{timeframe}")
                continue
            for raw_regime, cell in timeframe_payload.items():
                regime = str(raw_regime).strip().upper()
                cell_id = f"{symbol}|{timeframe}|{regime}"
                if regime not in PORTFOLIO_REPLAY_REQUIRED_REGIMES:
                    errors.append(f"LEDGER_REGIME_INVALID:{cell_id}")
                    continue
                if not isinstance(cell, Mapping):
                    errors.append(f"LEDGER_CELL_INVALID:{cell_id}")
                    continue
                strategy = str(cell.get("strategy_name") or "").strip()
                status = str(cell.get("promotion_status") or "BLOCKED").strip().upper()
                artifact = (
                    cell.get("artifact_meta")
                    if isinstance(cell.get("artifact_meta"), Mapping)
                    else {}
                )
                no_trade = (
                    bool(artifact.get("no_trade")) or strategy.upper() == "NO_TRADE"
                )
                if no_trade:
                    errors.append(f"LEDGER_NON_WINNER_PLACEHOLDER_FORBIDDEN:{cell_id}")
                    continue
                if not strategy:
                    errors.append(f"LEDGER_WINNER_STRATEGY_MISSING:{cell_id}")
                    continue
                if not str(cell.get("promotion_status") or "").strip():
                    errors.append(f"LEDGER_PROMOTION_STATUS_MISSING:{cell_id}")
                if status not in allowed:
                    errors.append(
                        f"LEDGER_WINNER_STATUS_NOT_ALLOWED:{cell_id}:{status}"
                    )
                    continue
                if cell_id in tradeable_ids:
                    errors.append(f"LEDGER_WINNER_SLOT_DUPLICATE:{cell_id}")
                    continue
                tradeable_ids.append(cell_id)
                rows.append(
                    {
                        "cell_id": cell_id,
                        "strategy_name": strategy,
                        "promotion_status": status,
                        "current_refit_binding": dict(
                            artifact.get("current_refit_binding")
                            if isinstance(
                                artifact.get("current_refit_binding"), Mapping
                            )
                            else {}
                        ),
                        "artifact_sha256": canonical_json_sha256(artifact),
                        "cell_sha256": canonical_json_sha256(cell),
                    }
                )

    extra_symbols = sorted(set(payload_by_symbol) - set(requested))
    projection = {
        "symbols": requested,
        "tradeable_cell_ids": sorted(tradeable_ids),
        "rows": sorted(rows, key=lambda row: row["cell_id"]),
        "extra_ledger_symbols": extra_symbols,
        "physical_winner_cell_count": len(rows),
    }
    return projection, list(dict.fromkeys(errors))


def build_book_possible_slot_accounting(
    book_decision: Any,
    *,
    evidence_family: Any,
) -> Tuple[Dict[str, Any], List[str]]:
    """Verify one complete Book and expose its possible-slot accounting."""
    accounting: Dict[str, Any] = {
        "schema_version": PORTFOLIO_REPLAY_BOOK_ACCOUNTING_SCHEMA_VERSION,
        "possible_cell_count": 0,
        "possible_cell_ids": [],
        "winner_slot_ids": [],
        "source_unavailable_binding_requirement": (
            "ACTIVE_NATIVE_DATA_STATE_TERMINAL_NON_SEARCH_V1"
        ),
        "terminal_technical_binding_requirement": (
            "ACTIVE_NATIVE_DATA_STATE_AUTHENTICATED_TERMINAL_TECHNICAL_V1"
        ),
        "source_unavailable_slot_ids": [],
        "insufficient_evidence_slot_ids": [],
        "terminal_technical_slot_ids": [],
        "state_counts": {},
    }
    try:
        import pm_book_multiplicity as book
        import pm_winner_selection as winner
    except Exception as exc:
        return accounting, [f"BOOK_AUTHORITY_IMPORT_FAILED:{type(exc).__name__}"]

    accounting.update(
        {
            "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
            "book_pinned_winner_contract_version": (
                book.PINNED_WINNER_SELECTION_CONTRACT_VERSION
            ),
            "runtime_winner_contract_version": winner.WINNER_SELECTION_CONTRACT_VERSION,
        }
    )
    if (
        book.PINNED_WINNER_SELECTION_CONTRACT_VERSION
        != winner.WINNER_SELECTION_CONTRACT_VERSION
    ):
        return accounting, [
            "BOOK_WINNER_CONTRACT_VERSION_MISMATCH:"
            f"{book.PINNED_WINNER_SELECTION_CONTRACT_VERSION}!="
            f"{winner.WINNER_SELECTION_CONTRACT_VERSION}"
        ]
    if not book.verify_historical_book_multiplicity_decision(
        book_decision,
        evidence_family,
    ):
        return accounting, ["BOOK_DECISION_INVALID_OR_FORGED"]

    body = book_decision.to_dict()
    cells = body.get("cells") if isinstance(body.get("cells"), list) else []
    errors: List[str] = []
    if (
        body.get("valid") is not True
        or body.get("optimization_run_complete") is not True
        or len(cells) != book.FAMILY_SIZE
    ):
        errors.append("BOOK_POSSIBLE_SLOT_ACCOUNTING_INCOMPLETE")

    accepted_states = {
        "WINNER",
        "NO_WINNER",
        book.REGIME_UNOBSERVED,
        book.INSUFFICIENT_EVIDENCE,
        book.TERMINAL_TECHNICAL,
    }
    compact_rows: List[Dict[str, Any]] = []
    source_unavailable_ids: List[str] = []
    insufficient_evidence_ids: List[str] = []
    terminal_technical_ids: List[str] = []
    for cell in cells:
        if not isinstance(cell, Mapping):
            errors.append("BOOK_CELL_INVALID")
            continue
        slot_id = (
            f"{str(cell.get('symbol') or '')}|"
            f"{str(cell.get('timeframe') or '').upper()}|"
            f"{str(cell.get('regime') or '').upper()}"
        )
        state = str(cell.get("historical_state") or "").upper()
        if state == "SOURCE_UNAVAILABLE":
            source_unavailable_ids.append(slot_id)
        elif state == book.INSUFFICIENT_EVIDENCE:
            insufficient_evidence_ids.append(slot_id)
        elif state == book.TERMINAL_TECHNICAL:
            terminal_technical_ids.append(slot_id)
        elif state not in accepted_states:
            errors.append(f"BOOK_CELL_NOT_TERMINAL:{slot_id}:{state or 'MISSING'}")
        compact_rows.append(
            {
                "slot_id": slot_id,
                "book_cell_id": cell.get("cell_id"),
                "state": state,
                "book_entry_sha256": cell.get("historical_book_entry_sha256"),
                "winner_decision_sha256": cell.get("winner_decision_sha256"),
            }
        )

    possible_ids = [row["slot_id"] for row in compact_rows]
    if len(set(possible_ids)) != book.FAMILY_SIZE:
        errors.append("BOOK_POSSIBLE_SLOT_ROSTER_NOT_UNIQUE")
    state_counts = dict(Counter(row["state"] for row in compact_rows))
    winner_ids = [row["slot_id"] for row in compact_rows if row["state"] == "WINNER"]
    accounting.update(
        {
            "book_receipt_sha256": book_decision.receipt_sha256,
            "book_generation_sha256": body.get("book_generation_sha256"),
            "family_precommitment_sha256": body.get("family_precommitment_sha256"),
            "possible_cell_count": len(compact_rows),
            "possible_cell_ids": possible_ids,
            "winner_slot_ids": winner_ids,
            "source_unavailable_slot_ids": source_unavailable_ids,
            "insufficient_evidence_slot_ids": insufficient_evidence_ids,
            "terminal_technical_slot_ids": terminal_technical_ids,
            "state_counts": state_counts,
            "cell_outcomes": compact_rows,
        }
    )
    accounting["accounting_sha256"] = canonical_json_sha256(
        {
            "book_receipt_sha256": accounting.get("book_receipt_sha256"),
            "cell_outcomes": compact_rows,
        }
    )
    return accounting, list(dict.fromkeys(errors))


def validate_causal_process_authority(
    historical_process_decision: Mapping[str, Any],
    *,
    precommit_payload: dict[str, Any],
    evidence_payload: dict[str, Any],
    reconciliation_payload: dict[str, Any],
) -> List[str]:
    """Recompute the bounded causal decision from its raw payload triplet."""
    try:
        from pm_causal_process import verify_historical_process_decision

        verified = verify_historical_process_decision(
            historical_process_decision,
            precommit_payload,
            evidence_payload,
            reconciliation_payload,
        )
        if verified is not True:
            return ["CAUSAL_PROCESS_DECISION_INVALID_OR_FORGED"]
        if historical_process_decision.get("decision") != (
            "OFFLINE_PROCESS_VALIDATION_PASS"
        ):
            return ["CAUSAL_PROCESS_DECISION_NOT_PASS"]
        return []
    except Exception:
        return ["CAUSAL_PROCESS_DECISION_INVALID_OR_FORGED"]


def validate_current_refit_execution_authority(
    current_refit_receipt: Any,
    *,
    raw_execution_manifest: Any = None,
    artifact_root: Any = None,
) -> List[str]:
    """Require raw replay through the fixed pipeline executor seam."""
    try:
        from pm_current_refit_execution import verify_raw_current_refit_execution

        result = verify_raw_current_refit_execution(
            current_refit_receipt,
            raw_execution_manifest,
            artifact_root=artifact_root,
        )
    except Exception:
        return ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"]
    errors = result.get("errors") if isinstance(result, Mapping) else None
    if isinstance(result, Mapping) and result.get("verified") is True and errors == []:
        return []
    if not isinstance(errors, list) or not errors:
        return ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"]
    return list(dict.fromkeys(str(error) for error in errors))


def build_ledger_cell_roster(
    ledger: Any,
    symbols: Iterable[str],
    *,
    allowed_promotion_statuses: Iterable[str] = ("PROMOTED",),
) -> Tuple[Dict[str, Any], List[str]]:
    """Return a sparse physical projection without granting replay authority."""
    projection, errors = _sparse_physical_winner_rows(
        ledger,
        symbols,
        allowed_promotion_statuses=allowed_promotion_statuses,
    )
    requested = projection["symbols"]
    possible_ids = [
        f"{symbol}|{timeframe}|{regime}"
        for symbol in requested
        for timeframe in PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES
        for regime in PORTFOLIO_REPLAY_REQUIRED_REGIMES
    ]
    roster = {
        "schema_version": PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION,
        "authoritative": False,
        "authority_status": "BOOK_AND_CURRENT_REFIT_REQUIRED",
        "symbols": requested,
        "required_timeframes": list(PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES),
        "required_regimes": list(PORTFOLIO_REPLAY_REQUIRED_REGIMES),
        "possible_regimes": list(PORTFOLIO_REPLAY_REQUIRED_REGIMES),
        "expected_cell_count": len(possible_ids),
        "expected_cell_ids": possible_ids,
        **projection,
    }
    roster["roster_sha256"] = canonical_json_sha256(
        {
            "authority_status": roster["authority_status"],
            "rows": roster["rows"],
        }
    )
    errors.append("BOOK_CURRENT_REFIT_REPLAY_AUTHORITY_REQUIRED")
    return roster, list(dict.fromkeys(errors))


def build_current_refit_replay_roster(
    ledger: Any,
    symbols: Iterable[str],
    *,
    current_book_decision: Any,
    book_evidence_family: Any,
    historical_process_decision: Mapping[str, Any],
    causal_precommit_payload: dict[str, Any],
    causal_evidence_payload: dict[str, Any],
    causal_reconciliation_payload: dict[str, Any],
    accepted_book_generation_receipt: Mapping[str, Any],
    current_refit_receipt: Mapping[str, Any],
    as_of_utc: Any,
    current_refit_raw_manifest: Any = None,
    current_refit_artifact_root: Any = None,
) -> Tuple[Mapping[str, Any], List[str]]:
    """Bind sparse physical winners to Book and current-refit authority."""
    projection, errors = _sparse_physical_winner_rows(
        ledger,
        symbols,
        allowed_promotion_statuses=("PROCESS_VALIDATED_CURRENT_REFIT",),
    )
    errors.extend(
        validate_causal_process_authority(
            historical_process_decision,
            precommit_payload=causal_precommit_payload,
            evidence_payload=causal_evidence_payload,
            reconciliation_payload=causal_reconciliation_payload,
        )
    )
    try:
        from pm_current_refit_execution import verify_raw_current_refit_execution

        refit_verification = verify_raw_current_refit_execution(
            current_refit_receipt,
            current_refit_raw_manifest,
            artifact_root=current_refit_artifact_root,
        )
    except Exception:
        refit_verification = {
            "verified": False,
            "errors": ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"],
        }
    if not isinstance(refit_verification, Mapping):
        refit_verification = {
            "verified": False,
            "errors": ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"],
        }
    refit_errors = refit_verification.get("errors")
    if not isinstance(refit_errors, list):
        refit_errors = ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"]
    errors.extend(str(error) for error in refit_errors)
    accounting, book_errors = build_book_possible_slot_accounting(
        current_book_decision,
        evidence_family=book_evidence_family,
    )
    errors.extend(book_errors)
    roster: Dict[str, Any] = {
        "schema_version": PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION,
        "authoritative": False,
        "current_refit_execution_reverified": False,
        "authority_status": "INVALID",
        "symbols": projection["symbols"],
        "required_timeframes": list(PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES),
        "required_regimes": list(PORTFOLIO_REPLAY_REQUIRED_REGIMES),
        "possible_regimes": list(PORTFOLIO_REPLAY_REQUIRED_REGIMES),
        "expected_cell_count": accounting.get("possible_cell_count", 0),
        "expected_cell_ids": accounting.get("possible_cell_ids", []),
        **projection,
        "book_accounting": accounting,
        "current_refit_execution_verification": dict(refit_verification),
    }
    if book_errors:
        roster["authority_status"] = "BOOK_BLOCKED"
        roster["roster_sha256"] = canonical_json_sha256(roster)
        return roster, list(dict.fromkeys(errors))

    try:
        from pm_model_lifecycle import (
            CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
            PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
            build_process_validated_current_refit_lineage,
            open_typed_receipt,
        )

        lineage = build_process_validated_current_refit_lineage(
            historical_process_decision=historical_process_decision,
            accepted_book_generation_receipt=accepted_book_generation_receipt,
            current_refit_receipt=current_refit_receipt,
            as_of_utc=as_of_utc,
        )
        accepted_book, accepted_book_receipt_sha = open_typed_receipt(
            accepted_book_generation_receipt,
            expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
        )
    except Exception as exc:
        roster["authority_status"] = "CURRENT_REFIT_LINEAGE_INVALID"
        errors.append(f"CURRENT_REFIT_LINEAGE_INVALID:{type(exc).__name__}")
        roster["roster_sha256"] = canonical_json_sha256(roster)
        return roster, list(dict.fromkeys(errors))

    lineage_errors = list(lineage.get("contract_errors") or [])
    if (
        lineage.get("book_role") != PROCESS_VALIDATED_CURRENT_REFIT_ROLE
        or lineage.get("contract_status") != "PROBATION_ONLY"
        or lineage.get("probation_candidate_eligible") is not True
        or lineage_errors
    ):
        errors.append("CURRENT_REFIT_LINEAGE_NOT_AUTHORIZED")
    if lineage.get("current_process_precommitted_cell_count") != 2_400:
        errors.append("CURRENT_REFIT_PRECOMMITTED_ROSTER_INCOMPLETE")
    if accepted_book.get("book_multiplicity_receipt_sha256") != accounting.get(
        "book_receipt_sha256"
    ):
        errors.append("CURRENT_BOOK_MULTIPLICITY_RECEIPT_MISMATCH")
    if accepted_book_receipt_sha != lineage.get(
        "accepted_book_generation_receipt_sha256"
    ):
        errors.append("CURRENT_BOOK_GENERATION_RECEIPT_MISMATCH")

    expected_symbols = sorted(
        {
            str(slot).split("|", 1)[0]
            for slot in accounting.get("possible_cell_ids", [])
        }
    )
    if projection["symbols"] != expected_symbols:
        errors.append("REPLAY_SYMBOL_ROSTER_NOT_COMPLETE_BOOK_UNIVERSE")

    refit_cells = lineage.get("current_refit_cells")
    if not isinstance(refit_cells, list):
        refit_cells = []
        errors.append("CURRENT_REFIT_CELL_ROSTER_INVALID")
    refit_by_slot: Dict[str, Mapping[str, Any]] = {}
    for cell in refit_cells:
        if not isinstance(cell, Mapping):
            errors.append("CURRENT_REFIT_CELL_INVALID")
            continue
        slot = (
            f"{str(cell.get('symbol') or '')}|"
            f"{str(cell.get('timeframe') or '').upper()}|"
            f"{str(cell.get('regime') or '').upper()}"
        )
        if slot in refit_by_slot:
            errors.append(f"CURRENT_REFIT_SLOT_DUPLICATE:{slot}")
        refit_by_slot[slot] = cell

    book_outcomes = {
        str(row.get("slot_id")): row
        for row in accounting.get("cell_outcomes", [])
        if isinstance(row, Mapping)
    }
    book_winners = set(accounting.get("winner_slot_ids") or [])
    refit_slots = set(refit_by_slot)
    physical_slots = set(projection.get("tradeable_cell_ids") or [])
    if refit_slots != book_winners:
        errors.append("CURRENT_REFIT_WINNER_ROSTER_MISMATCH")
    if physical_slots != refit_slots:
        errors.append("PHYSICAL_LEDGER_CURRENT_REFIT_ROSTER_MISMATCH")
    if lineage.get("current_refit_cell_count") != len(refit_slots):
        errors.append("CURRENT_REFIT_CELL_COUNT_MISMATCH")

    binding_fields = (
        "historical_process_decision_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_cell_id",
        "winner_decision_sha256",
        "winner_book_entry_sha256",
        "source_snapshot_sha256",
        "parameter_sha256",
        "output_artifact_sha256",
        "regime_fit_receipt_sha256",
        "regime_detector_record_sha256",
        "regime_detector_parameters_sha256",
        "regime_detector_code_sha256",
    )
    for row in projection.get("rows", []):
        slot = str(row.get("cell_id") or "")
        refit = refit_by_slot.get(slot)
        if not isinstance(refit, Mapping):
            continue
        if row.get("strategy_name") != refit.get("strategy_id"):
            errors.append(f"PHYSICAL_LEDGER_STRATEGY_MISMATCH:{slot}")
        binding = row.get("current_refit_binding")
        expected_binding = {name: refit.get(name) for name in binding_fields}
        if not isinstance(binding, Mapping) or dict(binding) != expected_binding:
            errors.append(f"PHYSICAL_LEDGER_REFIT_BINDING_MISMATCH:{slot}")
        outcome = book_outcomes.get(slot)
        if not isinstance(outcome, Mapping):
            errors.append(f"BOOK_WINNER_SLOT_MISSING:{slot}")
            continue
        if (
            outcome.get("state") != "WINNER"
            or outcome.get("book_entry_sha256") != refit.get("winner_book_entry_sha256")
            or outcome.get("winner_decision_sha256")
            != refit.get("winner_decision_sha256")
        ):
            errors.append(f"CURRENT_REFIT_BOOK_ENTRY_MISMATCH:{slot}")

    physical_ledger_sha = canonical_json_sha256(ledger)

    roster.update(
        {
            "current_refit_lineage": {
                "historical_process_decision_sha256": lineage.get(
                    "historical_process_decision_sha256"
                ),
                "accepted_book_generation_receipt_sha256": lineage.get(
                    "accepted_book_generation_receipt_sha256"
                ),
                "current_refit_receipt_sha256": lineage.get(
                    "current_refit_receipt_sha256"
                ),
                "current_refit_execution_sha256": lineage.get(
                    "current_refit_execution_sha256"
                ),
                "current_refit_cell_count": lineage.get("current_refit_cell_count"),
                "current_selected_winner_roster_sha256": lineage.get(
                    "current_selected_winner_roster_sha256"
                ),
                "current_refit_output_book_sha256": lineage.get(
                    "current_refit_output_book_sha256"
                ),
            },
            "physical_ledger_sha256": physical_ledger_sha,
        }
    )
    if errors == ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"]:
        roster["authority_status"] = "CURRENT_REFIT_REVERIFIER_REQUIRED"
    elif not errors and refit_verification.get("verified") is True:
        roster["authoritative"] = True
        roster["current_refit_execution_reverified"] = True
        roster["authority_status"] = "PROCESS_VALIDATED_CURRENT_REFIT"
    roster["roster_sha256"] = canonical_json_sha256(
        {
            "authority_status": roster["authority_status"],
            "current_refit_execution_reverified": roster[
                "current_refit_execution_reverified"
            ],
            "book_accounting_sha256": accounting.get("accounting_sha256"),
            "current_refit_lineage": roster.get("current_refit_lineage"),
            "current_refit_execution_verification": roster.get(
                "current_refit_execution_verification"
            ),
            "physical_ledger_sha256": physical_ledger_sha,
            "rows": roster["rows"],
        }
    )
    unique_errors = list(dict.fromkeys(errors))
    return roster, unique_errors


def _current_refit_roster_errors(roster: Any) -> List[str]:
    if not isinstance(roster, Mapping):
        return ["CURRENT_REFIT_ROSTER_NOT_OBJECT"]
    errors: List[str] = []
    if roster.get("schema_version") != PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION:
        errors.append("CURRENT_REFIT_ROSTER_SCHEMA_INVALID")
    if (
        roster.get("authoritative") is not True
        or roster.get("current_refit_execution_reverified") is not True
        or roster.get("authority_status") != "PROCESS_VALIDATED_CURRENT_REFIT"
    ):
        errors.append("CURRENT_REFIT_ROSTER_NOT_AUTHORITATIVE")
    verification = roster.get("current_refit_execution_verification")
    if (
        not isinstance(verification, Mapping)
        or verification.get("verified") is not True
        or verification.get("errors") != []
    ):
        errors.append("CURRENT_REFIT_RAW_EXECUTION_NOT_REVERIFIED")

    selected = roster.get("tradeable_cell_ids")
    rows = roster.get("rows")
    if (
        not isinstance(selected, list)
        or any(not isinstance(value, str) for value in selected)
        or selected != sorted(selected)
        or len(selected) != len(set(selected))
        or not isinstance(rows, list)
    ):
        errors.append("CURRENT_REFIT_SELECTED_ROSTER_INVALID")
        selected = []
        rows = []
    row_ids = [row.get("cell_id") for row in rows if isinstance(row, Mapping)]
    if (
        len(row_ids) != len(rows)
        or row_ids != selected
        or roster.get("physical_winner_cell_count") != len(selected)
    ):
        errors.append("CURRENT_REFIT_SPARSE_ROSTER_MISMATCH")
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        cell_id = str(row.get("cell_id") or "")
        parts = cell_id.split("|")
        if (
            len(parts) != 3
            or parts[1] not in PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES
            or parts[2] not in PORTFOLIO_REPLAY_REQUIRED_REGIMES
            or not str(row.get("strategy_name") or "").strip()
            or row.get("promotion_status") != "PROCESS_VALIDATED_CURRENT_REFIT"
        ):
            errors.append(f"CURRENT_REFIT_ROSTER_ROW_INVALID:{cell_id}")

    lineage = roster.get("current_refit_lineage")
    lineage_fields = {
        "historical_process_decision_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_receipt_sha256",
        "current_refit_execution_sha256",
        "current_refit_cell_count",
        "current_selected_winner_roster_sha256",
        "current_refit_output_book_sha256",
    }
    if not isinstance(lineage, Mapping) or set(lineage) != lineage_fields:
        errors.append("CURRENT_REFIT_ROSTER_LINEAGE_INVALID")
    else:
        for name in lineage_fields - {"current_refit_cell_count"}:
            value = lineage.get(name)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value.lower())
            ):
                errors.append(f"CURRENT_REFIT_ROSTER_LINEAGE_HASH_INVALID:{name}")
        if lineage.get("current_refit_cell_count") != len(selected):
            errors.append("CURRENT_REFIT_ROSTER_LINEAGE_COUNT_MISMATCH")
    accounting = roster.get("book_accounting")
    if (
        not isinstance(accounting, Mapping)
        or sorted(accounting.get("winner_slot_ids") or []) != selected
    ):
        errors.append("CURRENT_REFIT_ROSTER_BOOK_WINNERS_MISMATCH")
    expected_sha = canonical_json_sha256(
        {
            "authority_status": roster.get("authority_status"),
            "current_refit_execution_reverified": roster.get(
                "current_refit_execution_reverified"
            ),
            "book_accounting_sha256": (
                accounting.get("accounting_sha256")
                if isinstance(accounting, Mapping)
                else None
            ),
            "current_refit_lineage": lineage,
            "current_refit_execution_verification": verification,
            "physical_ledger_sha256": roster.get("physical_ledger_sha256"),
            "rows": rows,
        }
    )
    if roster.get("roster_sha256") != expected_sha:
        errors.append("CURRENT_REFIT_ROSTER_SEAL_INVALID")
    return list(dict.fromkeys(errors))


def validate_current_refit_replay_candidates(
    candidates: Iterable["CandidateTrade"],
    roster: Mapping[str, Any],
    *,
    replayed_cell_ids: Iterable[str],
) -> List[str]:
    """Bind current-refit candidates to the raw-reverified sparse roster."""
    errors = _current_refit_roster_errors(roster)
    selected = roster.get("tradeable_cell_ids")
    selected_ids = selected if isinstance(selected, list) else []
    replayed = [str(value) for value in replayed_cell_ids]
    if len(replayed) != len(set(replayed)) or sorted(replayed) != sorted(selected_ids):
        errors.append("CURRENT_REFIT_REPLAYED_CELL_ROSTER_MISMATCH")

    rows = roster.get("rows") if isinstance(roster.get("rows"), list) else []
    row_by_cell = {
        str(row.get("cell_id")): row
        for row in rows
        if isinstance(row, Mapping) and isinstance(row.get("cell_id"), str)
    }
    seen: set[str] = set()
    for offset, raw in enumerate(candidates):
        try:
            candidate = raw.normalized()
        except Exception:
            errors.append(f"CURRENT_REFIT_CANDIDATE_INVALID:{offset}")
            continue
        if candidate.candidate_id in seen:
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_ID_DUPLICATE:{candidate.candidate_id}"
            )
        seen.add(candidate.candidate_id)
        row = row_by_cell.get(candidate.cell_id)
        if row is None:
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_CELL_NOT_SELECTED:{candidate.cell_id}"
            )
            continue
        parts = candidate.cell_id.split("|")
        if len(parts) != 3 or parts != [
            candidate.symbol,
            candidate.timeframe,
            candidate.regime,
        ]:
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_CELL_IDENTITY_MISMATCH:{candidate.candidate_id}"
            )
        if candidate.strategy_name != row.get("strategy_name"):
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_STRATEGY_MISMATCH:{candidate.candidate_id}"
            )
        if (
            candidate.promotion_status != "PROCESS_VALIDATED_CURRENT_REFIT"
            or row.get("promotion_status") != "PROCESS_VALIDATED_CURRENT_REFIT"
        ):
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_STATUS_INVALID:{candidate.candidate_id}"
            )
        if candidate.source_schema != PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION:
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_SCHEMA_INVALID:{candidate.candidate_id}"
            )
        if not candidate.margin_model.strip():
            errors.append(
                f"CURRENT_REFIT_CANDIDATE_MARGIN_MODEL_MISSING:{candidate.candidate_id}"
            )
    return list(dict.fromkeys(errors))


def portfolio_cost_contract(config_payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Resolve the replay cost contract exactly as ``PipelineConfig`` does.

    Funded JSON intentionally omits several code-owned invariants.  An omitted
    key therefore means the production dataclass default, not ``False``.  The
    replay contract binds only the costs the historical engine can legitimately
    reconstruct: executable spread and causal swap.
    """
    from pm_core import PipelineConfig, strict_pipeline_config_kwargs

    pipeline = (
        config_payload.get("pipeline")
        if isinstance(config_payload.get("pipeline"), Mapping)
        else {}
    )

    resolved = PipelineConfig(**strict_pipeline_config_kwargs(pipeline))

    return {
        "use_spread": bool(resolved.use_spread),
        "backtest_spread_mode": str(resolved.backtest_spread_mode or ""),
        "swap_model_enabled": bool(resolved.swap_model_enabled),
        "swap_model_min_holding_hours": _finite(
            resolved.swap_model_min_holding_hours, 0.0
        ),
        "swap_model_triple_rollover_day": str(
            resolved.swap_model_triple_rollover_day or ""
        ).lower(),
    }


def portfolio_mark_cost_contract(
    cost_contract: Mapping[str, Any],
) -> Dict[str, Any]:
    """Return the exact funded causal valuation/mark contract.

    A single code-owned builder prevents the evidence writer and validator from
    drifting on the economically important price, clock and event-order rules.
    """
    return {
        "valuation_tape_contract": MTM_MARK_COST_CONTRACT_VERSION,
        "valuation_timeframe": "M5",
        "valuation_price_field": "Open",
        "valuation_timestamp_semantics": "bar_open_utc",
        "observation_time_rule": "observation_time_lte_effective_time",
        "event_order": "close_existing_then_mark_existing_then_admit",
        "spread_at_mark": bool(cost_contract.get("use_spread")),
        "swap_accrued_through_mark": bool(cost_contract.get("swap_model_enabled")),
        "swap_accrual_model": MTM_SWAP_ACCRUAL_CONTRACT_VERSION,
    }


def portfolio_policy_from_config_payload(
    config_payload: Mapping[str, Any],
    starting_balance: float,
) -> "PortfolioPolicy":
    """Build the sole funded replay policy from the live risk configuration."""
    pipeline = (
        config_payload.get("pipeline")
        if isinstance(config_payload.get("pipeline"), Mapping)
        else {}
    )
    position = (
        config_payload.get("position")
        if isinstance(config_payload.get("position"), Mapping)
        else {}
    )
    target_risk = _finite(position.get("risk_per_trade_pct"), 0.25)
    max_trade_risk = _finite(position.get("max_risk_pct"), 0.50)
    open_entry_threshold = _finite(
        pipeline.get("max_new_entries_when_total_open_risk_above_pct"), 2.0
    )
    configured_open_cap = _finite(pipeline.get("max_total_open_risk_pct"), 3.0)
    open_risk_action = (
        str(pipeline.get("open_risk_action") or "pause_new_entries").strip().lower()
    )
    if open_risk_action == "pause_new_entries":
        effective_open_cap = (
            min(configured_open_cap, open_entry_threshold)
            if open_entry_threshold > 0.0
            else configured_open_cap
        )
    elif open_risk_action == "observe":
        effective_open_cap = configured_open_cap
    elif open_risk_action == "derate":
        raise ValueError(
            "funded synchronized replay does not support open_risk_action='derate'; "
            "use a frozen hard cap or implement exact chronological derating parity"
        )
    else:
        raise ValueError(f"unsupported open_risk_action={open_risk_action!r}")
    factor_group_cap = _finite(
        pipeline.get(
            "max_factor_group_open_risk_pct",
            pipeline.get("max_combined_risk_pct"),
        ),
        0.0,
    )
    margin_entry_block_level = _finite(pipeline.get("margin_entry_block_level"), 200.0)
    if margin_entry_block_level <= 100.0:
        raise ValueError(
            "margin_entry_block_level must be greater than 100% to retain "
            "positive free-margin headroom"
        )
    max_margin_utilization_pct = 10_000.0 / margin_entry_block_level
    if not (0.0 < max_margin_utilization_pct < 100.0):
        raise ValueError("derived margin utilization must be in (0, 100)")
    raw_symbols = config_payload.get("symbols")
    configured_symbols = {
        str(symbol).strip().upper()
        for symbol in (
            raw_symbols if isinstance(raw_symbols, (list, tuple, set)) else ()
        )
        if str(symbol).strip()
    }
    try:
        target_symbol_count = max(
            0, int(pipeline.get("production_symbol_pool_target_count", 0) or 0)
        )
    except (TypeError, ValueError, OverflowError):
        target_symbol_count = 0
    # The count is only a structural/resource impossibility bound.  It must not
    # become a hidden capital-allocation decision: economic admission is
    # governed by total, factor, same-symbol, and margin axes.  The production
    # topology contains at most one concurrent position for each independently
    # authenticated symbol x native-timeframe x empirically observed-regime
    # specialist slot.
    structural_symbol_count = max(1, len(configured_symbols), target_symbol_count)
    structural_position_ceiling = (
        structural_symbol_count
        * len(PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES)
        * len(PORTFOLIO_REPLAY_REQUIRED_REGIMES)
    )
    return PortfolioPolicy(
        starting_balance=float(starting_balance),
        target_risk_per_trade_pct=target_risk,
        max_risk_per_trade_pct=max_trade_risk,
        max_open_risk_pct=effective_open_cap,
        max_concurrent_positions=structural_position_ceiling,
        max_same_symbol_open_risk_pct=_finite(
            pipeline.get("max_combined_risk_pct"), 1.0
        ),
        max_factor_group_open_risk_pct=factor_group_cap,
        # Funded live has one deterministic shared factor taxonomy in
        # evaluate_gross_stop_risk(). A replay-only trailing-correlation gate
        # would reject orders that live admits and invalidate parity.
        enforce_pair_correlation=False,
        correlation_threshold=0.75,
        max_correlated_cluster_risk_pct=factor_group_cap,
        correlation_min_observations=30,
        block_unknown_correlation=False,
        max_margin_utilization_pct=max_margin_utilization_pct,
        min_margin_level_pct=margin_entry_block_level,
        min_free_margin_pct=100.0 - max_margin_utilization_pct,
        ruin_equity_pct=50.0,
        allowed_promotion_statuses=tuple(
            str(status).upper()
            for status in (
                pipeline.get("live_allowed_promotion_statuses") or ["PROMOTED"]
            )
        ),
    )


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _required_finite(value: Any, field: str) -> float:
    """Parse a required evidence number without neutralizing corruption."""
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def _timestamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError(f"invalid timestamp: {value!r}")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp


def floor_volume(volume: float, step: float, maximum: float) -> float:
    """Floor a requested volume without ever clamping it up to broker minimum."""
    volume = _finite(volume)
    step = _finite(step, 0.01)
    maximum = _finite(maximum, 0.0)
    if volume <= 0.0 or step <= 0.0 or maximum <= 0.0:
        return 0.0
    units = math.floor((volume + (step * 1e-10)) / step)
    return round(min(maximum, units * step), 10)


@dataclass(frozen=True)
class ExitLeg:
    """One partial or final close, normalized to one original entry lot.

    ``net_pnl_per_entry_lot`` already includes the leg fraction.  For example,
    a half close that makes USD 40 per closed lot contributes USD 20 per
    original entry lot.
    """

    exit_time: Any
    fraction: float
    net_pnl_per_entry_lot: float
    price_pnl_per_entry_lot: float = 0.0
    swap_per_entry_lot: float = 0.0
    exit_reason: str = ""

    def normalized(self) -> "ExitLeg":
        return ExitLeg(
            exit_time=_timestamp(self.exit_time),
            fraction=_required_finite(self.fraction, "exit leg fraction"),
            net_pnl_per_entry_lot=_required_finite(
                self.net_pnl_per_entry_lot, "exit leg net P&L"
            ),
            price_pnl_per_entry_lot=_required_finite(
                self.price_pnl_per_entry_lot, "exit leg price P&L"
            ),
            swap_per_entry_lot=_required_finite(
                self.swap_per_entry_lot, "exit leg swap"
            ),
            exit_reason=str(self.exit_reason or ""),
        )


@dataclass(frozen=True)
class MarkPoint:
    """One causal unrealized-P&L observation for a currently-open lot.

    ``time`` is the effective portfolio decision timestamp.  The optional
    ``observation_time`` keeps older two-argument constructors source-compatible,
    but normalizes them to an observation made at the effective timestamp.  A
    future observation can never be back-stamped onto an earlier decision.
    """

    time: Any
    pnl_per_current_lot: float
    observation_time: Any = None

    def normalized(self) -> "MarkPoint":
        effective_time = _timestamp(self.time)
        observation_time = (
            effective_time
            if self.observation_time is None
            else _timestamp(self.observation_time)
        )
        if observation_time > effective_time:
            raise ValueError(
                "mark observation time cannot be after its effective decision time"
            )
        return MarkPoint(
            time=effective_time,
            pnl_per_current_lot=_required_finite(self.pnl_per_current_lot, "mark P&L"),
            observation_time=observation_time,
        )


@dataclass(frozen=True)
class CandidateTrade:
    candidate_id: str
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    strategy_name: str
    promotion_status: str
    asset_class: str
    risk_groups: Tuple[str, ...]
    entry_time: Any
    direction: int
    stop_risk_per_lot: float
    margin_per_lot: float
    volume_min: float
    volume_max: float
    volume_step: float
    priority_score: float
    legs: Tuple[ExitLeg, ...]
    marks: Tuple[MarkPoint, ...] = ()
    source_schema: str = ""
    margin_model: str = ""

    def normalized(self) -> "CandidateTrade":
        direction = 1 if int(self.direction) > 0 else -1
        legs = tuple(
            sorted(
                (leg.normalized() for leg in self.legs), key=lambda leg: leg.exit_time
            )
        )
        marks = tuple(
            sorted(
                (mark.normalized() for mark in self.marks), key=lambda mark: mark.time
            )
        )
        entry = _timestamp(self.entry_time)
        if not str(self.candidate_id or "").strip():
            raise ValueError("candidate_id is required")
        if not legs:
            raise ValueError(f"candidate {self.candidate_id} has no exit legs")
        if any(leg.exit_time < entry for leg in legs):
            raise ValueError(f"candidate {self.candidate_id} exits before entry")
        fraction_sum = sum(leg.fraction for leg in legs)
        if abs(fraction_sum - 1.0) > 1e-5:
            raise ValueError(
                f"candidate {self.candidate_id} exit fractions sum to {fraction_sum:.8f}, expected 1"
            )
        if any(leg.fraction <= 0.0 for leg in legs):
            raise ValueError(
                f"candidate {self.candidate_id} has a non-positive exit fraction"
            )
        stop_risk = _required_finite(
            self.stop_risk_per_lot, "candidate stop risk per lot"
        )
        if stop_risk <= 0.0:
            raise ValueError(f"candidate {self.candidate_id} has no positive stop risk")
        margin_per_lot = _required_finite(
            self.margin_per_lot, "candidate margin per lot"
        )
        volume_min = _required_finite(self.volume_min, "candidate minimum volume")
        volume_max = _required_finite(self.volume_max, "candidate maximum volume")
        volume_step = _required_finite(self.volume_step, "candidate volume step")
        priority_score = _required_finite(
            self.priority_score, "candidate priority score"
        )
        if margin_per_lot <= 0.0:
            raise ValueError(f"candidate {self.candidate_id} has no positive margin")
        if volume_min <= 0.0 or volume_max < volume_min or volume_step <= 0.0:
            raise ValueError(f"candidate {self.candidate_id} has invalid volume bounds")
        return CandidateTrade(
            candidate_id=str(self.candidate_id),
            cell_id=str(self.cell_id),
            symbol=str(self.symbol),
            timeframe=str(self.timeframe).upper(),
            regime=str(self.regime).upper(),
            strategy_name=str(self.strategy_name),
            promotion_status=str(self.promotion_status).upper(),
            asset_class=str(self.asset_class or "unknown").lower(),
            risk_groups=infer_factor_groups(self.symbol, self.asset_class),
            entry_time=entry,
            direction=direction,
            stop_risk_per_lot=stop_risk,
            margin_per_lot=margin_per_lot,
            volume_min=volume_min,
            volume_max=volume_max,
            volume_step=volume_step,
            priority_score=priority_score,
            legs=legs,
            marks=tuple(
                mark for mark in marks if entry <= mark.time <= legs[-1].exit_time
            ),
            source_schema=str(self.source_schema or ""),
            margin_model=str(self.margin_model or ""),
        )


@dataclass(frozen=True)
class PortfolioPolicy:
    starting_balance: float = 10_000.0
    target_risk_per_trade_pct: float = 0.25
    max_risk_per_trade_pct: float = 0.50
    max_open_risk_pct: float = 2.0
    max_concurrent_positions: int = 6
    max_same_symbol_open_risk_pct: float = 1.0
    max_factor_group_open_risk_pct: float = 1.0
    # Research-only overlay. The funded policy builder fixes this false because
    # live admission uses the shared deterministic factor-group contract.
    enforce_pair_correlation: bool = False
    correlation_threshold: float = 0.75
    max_correlated_cluster_risk_pct: float = 1.0
    correlation_min_observations: int = 30
    block_unknown_correlation: bool = False
    max_margin_utilization_pct: float = 30.0
    min_margin_level_pct: float = 300.0
    min_free_margin_pct: float = 65.0
    ruin_equity_pct: float = 50.0
    allowed_promotion_statuses: Tuple[str, ...] = ("PROMOTED",)

    def validate(self) -> None:
        if _finite(self.starting_balance) <= 0.0:
            raise ValueError("starting_balance must be positive")
        if not (0.0 < self.target_risk_per_trade_pct <= self.max_risk_per_trade_pct):
            raise ValueError(
                "target risk must be positive and no greater than per-trade cap"
            )
        if self.max_open_risk_pct < self.max_risk_per_trade_pct:
            raise ValueError("max open risk cannot be less than max per-trade risk")
        if self.max_concurrent_positions < 1:
            raise ValueError("max_concurrent_positions must be positive")
        if not (0.0 < self.max_same_symbol_open_risk_pct <= self.max_open_risk_pct):
            raise ValueError(
                "same-symbol open-risk cap must be positive and no greater than total open-risk cap"
            )
        if not (0.0 < self.max_factor_group_open_risk_pct <= self.max_open_risk_pct):
            raise ValueError(
                "factor-group open-risk cap must be positive and no greater than total open-risk cap"
            )
        if self.enforce_pair_correlation and not (
            0.0 < self.correlation_threshold <= 1.0
        ):
            raise ValueError("correlation_threshold must be in (0, 1]")
        if not (0.0 < self.ruin_equity_pct < 100.0):
            raise ValueError("ruin_equity_pct must be in (0, 100)")


_CANDIDATE_RECORD_FIELDS = {
    "candidate_id",
    "cell_id",
    "symbol",
    "timeframe",
    "regime",
    "strategy_name",
    "promotion_status",
    "asset_class",
    "risk_groups",
    "entry_time",
    "direction",
    "stop_risk_per_lot",
    "margin_per_lot",
    "volume_min",
    "volume_max",
    "volume_step",
    "priority_score",
    "legs",
    "marks",
    "source_schema",
    "margin_model",
}
_EXIT_RECORD_FIELDS = {
    "exit_time",
    "fraction",
    "net_pnl_per_entry_lot",
    "price_pnl_per_entry_lot",
    "swap_per_entry_lot",
    "exit_reason",
}
_MARK_RECORD_FIELDS = {"time", "pnl_per_current_lot", "observation_time"}


def current_refit_candidate_record(candidate: CandidateTrade) -> Dict[str, Any]:
    value = candidate.normalized()
    return {
        "candidate_id": value.candidate_id,
        "cell_id": value.cell_id,
        "symbol": value.symbol,
        "timeframe": value.timeframe,
        "regime": value.regime,
        "strategy_name": value.strategy_name,
        "promotion_status": value.promotion_status,
        "asset_class": value.asset_class,
        "risk_groups": list(value.risk_groups),
        "entry_time": value.entry_time.isoformat(),
        "direction": value.direction,
        "stop_risk_per_lot": value.stop_risk_per_lot,
        "margin_per_lot": value.margin_per_lot,
        "volume_min": value.volume_min,
        "volume_max": value.volume_max,
        "volume_step": value.volume_step,
        "priority_score": value.priority_score,
        "legs": [
            {
                "exit_time": leg.exit_time.isoformat(),
                "fraction": leg.fraction,
                "net_pnl_per_entry_lot": leg.net_pnl_per_entry_lot,
                "price_pnl_per_entry_lot": leg.price_pnl_per_entry_lot,
                "swap_per_entry_lot": leg.swap_per_entry_lot,
                "exit_reason": leg.exit_reason,
            }
            for leg in value.legs
        ],
        "marks": [
            {
                "time": mark.time.isoformat(),
                "pnl_per_current_lot": mark.pnl_per_current_lot,
                "observation_time": mark.observation_time.isoformat(),
            }
            for mark in value.marks
        ],
        "source_schema": value.source_schema,
        "margin_model": value.margin_model,
    }


def portfolio_replay_candidate_roster_receipt(
    candidates: Sequence[CandidateTrade],
) -> Dict[str, Any]:
    if not 1 <= len(candidates) <= MAX_PORTFOLIO_REPLAY_CANDIDATES:
        raise ValueError("portfolio replay candidate roster is outside its bound")
    ordered = sorted(candidates, key=lambda item: item.candidate_id)
    identifiers = [item.candidate_id for item in ordered]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("candidate_id values must be unique")
    record_rows = (
        f"{item.candidate_id}\0{_bounded_canonical_record_sha256(current_refit_candidate_record(item))}"
        for item in ordered
    )
    return {
        "schema_version": PORTFOLIO_REPLAY_INPUT_RECEIPT_VERSION,
        "candidate_count": len(ordered),
        "candidate_ids_sha256": portfolio_replay_candidate_ids_sha256(identifiers),
        "candidate_roster_sha256": _framed_text_digest(
            b"fxpm.synchronized-replay-candidate-roster.v1", record_rows
        ),
        "candidate_roster_hash_algorithm": (
            "sha256-domain-length-framed-canonical-record-sha256-v1"
        ),
    }


def candidate_trade_from_record(record: Any) -> CandidateTrade:
    """Strictly reconstruct one canonical :class:`CandidateTrade` record.

    The decoder is deliberately shared by current-refit and historical Capital
    evidence consumers so persisted candidate bytes have one validation law.
    It derives a normalized object and then requires an exact record round trip;
    it never supplies defaults or grants replay/trading authority.
    """
    if not isinstance(record, Mapping) or set(record) != _CANDIDATE_RECORD_FIELDS:
        raise ValueError("candidate record fields invalid")
    legs = record.get("legs")
    marks = record.get("marks")
    groups = record.get("risk_groups")
    if (
        not isinstance(legs, list)
        or not isinstance(marks, list)
        or not isinstance(groups, list)
        or any(not isinstance(value, str) for value in groups)
    ):
        raise ValueError("candidate record collections invalid")
    if any(
        not isinstance(row, Mapping) or set(row) != _EXIT_RECORD_FIELDS for row in legs
    ):
        raise ValueError("candidate exit fields invalid")
    if any(
        not isinstance(row, Mapping) or set(row) != _MARK_RECORD_FIELDS for row in marks
    ):
        raise ValueError("candidate mark fields invalid")
    candidate = CandidateTrade(
        candidate_id=record["candidate_id"],
        cell_id=record["cell_id"],
        symbol=record["symbol"],
        timeframe=record["timeframe"],
        regime=record["regime"],
        strategy_name=record["strategy_name"],
        promotion_status=record["promotion_status"],
        asset_class=record["asset_class"],
        risk_groups=tuple(groups),
        entry_time=record["entry_time"],
        direction=record["direction"],
        stop_risk_per_lot=record["stop_risk_per_lot"],
        margin_per_lot=record["margin_per_lot"],
        volume_min=record["volume_min"],
        volume_max=record["volume_max"],
        volume_step=record["volume_step"],
        priority_score=record["priority_score"],
        legs=tuple(
            ExitLeg(
                exit_time=row["exit_time"],
                fraction=row["fraction"],
                net_pnl_per_entry_lot=row["net_pnl_per_entry_lot"],
                price_pnl_per_entry_lot=row["price_pnl_per_entry_lot"],
                swap_per_entry_lot=row["swap_per_entry_lot"],
                exit_reason=row["exit_reason"],
            )
            for row in legs
        ),
        marks=tuple(
            MarkPoint(
                time=row["time"],
                pnl_per_current_lot=row["pnl_per_current_lot"],
                observation_time=row["observation_time"],
            )
            for row in marks
        ),
        source_schema=record["source_schema"],
        margin_model=record["margin_model"],
    ).normalized()
    if current_refit_candidate_record(candidate) != dict(record):
        raise ValueError("candidate record is not canonical")
    return candidate


def _daily_mtm_rows(path: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    by_day: Dict[str, Mapping[str, Any]] = {}
    for row in path:
        stamp = _timestamp(row.get("timestamp"))
        by_day[stamp.date().isoformat()] = row
    return [
        {
            "date": day,
            "timestamp": row.get("timestamp"),
            "balance": row.get("balance"),
            "mtm_equity": row.get("mtm_equity"),
            "stop_equity": row.get("stop_equity"),
            "open_risk": row.get("open_risk"),
            "margin_used": row.get("margin_used"),
            "open_positions": row.get("open_positions"),
            "realized_pnl_at_timestamp": row.get("realized_pnl_at_timestamp"),
        }
        for day, row in sorted(by_day.items())
    ]


def _current_refit_scenario_reconciliation(
    result: Mapping[str, Any],
    *,
    candidate_records_sha256: str,
) -> Dict[str, Any]:
    path = result.get("path") if isinstance(result.get("path"), list) else []
    fills = (
        result.get("closed_positions")
        if isinstance(result.get("closed_positions"), list)
        else []
    )
    blocked = (
        result.get("blocked_positions")
        if isinstance(result.get("blocked_positions"), list)
        else []
    )
    daily = _daily_mtm_rows(path)
    policy = result.get("policy") if isinstance(result.get("policy"), Mapping) else {}
    summary = (
        result.get("summary") if isinstance(result.get("summary"), Mapping) else {}
    )
    return {
        "schema_version": CURRENT_REFIT_REPLAY_RECONCILIATION_SCHEMA_VERSION,
        "starting_balance": policy.get("starting_balance"),
        "candidate_records_sha256": candidate_records_sha256,
        "policy_sha256": canonical_json_sha256(policy),
        "summary_sha256": canonical_json_sha256(summary),
        "event_stream_sha256": candidate_records_sha256,
        "fill_ledger_sha256": canonical_json_sha256(fills),
        "blocked_ledger_sha256": canonical_json_sha256(blocked),
        "exposure_path_sha256": canonical_json_sha256(path),
        "daily_mtm_sha256": canonical_json_sha256(daily),
        "synchronized_result_sha256": canonical_json_sha256(result),
        "fill_count": len(fills),
        "blocked_count": len(blocked),
        "exposure_observation_count": len(path),
        "daily_mtm_observation_count": len(daily),
    }


def build_current_refit_replay_successor(
    candidates: Iterable[CandidateTrade],
    roster: Mapping[str, Any],
    *,
    replayed_cell_ids: Iterable[str],
    expected_lineage: Mapping[str, Any],
    expected_cost_contract: Mapping[str, Any],
    expected_policies: Mapping[float, Mapping[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    values = [candidate.normalized() for candidate in candidates]
    values.sort(key=lambda value: value.candidate_id)
    errors = validate_current_refit_replay_candidates(
        values, roster, replayed_cell_ids=replayed_cell_ids
    )
    if errors:
        raise ValueError(";".join(errors))
    records = [current_refit_candidate_record(value) for value in values]
    records_sha = canonical_json_sha256(records)
    scenarios: List[Dict[str, Any]] = []
    reconciliations: List[Dict[str, Any]] = []
    for balance, raw_policy in sorted(expected_policies.items()):
        policy_value = dict(raw_policy)
        policy_value["allowed_promotion_statuses"] = tuple(
            policy_value.get("allowed_promotion_statuses") or ()
        )
        policy = PortfolioPolicy(**policy_value)
        if policy.starting_balance != float(
            balance
        ) or policy.allowed_promotion_statuses != ("PROCESS_VALIDATED_CURRENT_REFIT",):
            raise ValueError("current-refit policy identity invalid")
        result = SynchronizedPortfolioSimulator(policy)._run(
            values, allow_current_refit=True
        )
        scenarios.append(
            {
                "starting_balance": float(balance),
                "policy": asdict(policy),
                "summary": result["summary"],
            }
        )
        reconciliations.append(
            _current_refit_scenario_reconciliation(
                result, candidate_records_sha256=records_sha
            )
        )
    lineage = roster.get("current_refit_lineage")
    verification = roster.get("current_refit_execution_verification")
    native_identity = {
        name: expected_lineage.get(name)
        for name in (
            "native_activation_id",
            "native_activation_manifest_sha256",
            "native_activation_roster_sha256",
            "native_bars_sha256_by_source",
            "native_sidecar_sha256_by_source",
        )
    }
    successor: Dict[str, Any] = {
        "schema_version": CURRENT_REFIT_REPLAY_SUCCESSOR_SCHEMA_VERSION,
        "authority_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        "roster_sha256": roster.get("roster_sha256"),
        "physical_ledger_sha256": roster.get("physical_ledger_sha256"),
        "selected_cell_ids": list(roster.get("tradeable_cell_ids") or []),
        "current_refit_lineage": lineage,
        "current_refit_lineage_sha256": canonical_json_sha256(lineage),
        "raw_current_refit_verification_sha256": canonical_json_sha256(verification),
        "native_contract_sha256": canonical_json_sha256(native_identity),
        "cost_contract_sha256": canonical_json_sha256(dict(expected_cost_contract)),
        "mark_cost_contract_sha256": canonical_json_sha256(
            portfolio_mark_cost_contract(expected_cost_contract)
        ),
        "policy_contracts_sha256": canonical_json_sha256(
            {
                str(float(key)): dict(value)
                for key, value in sorted(expected_policies.items())
            }
        ),
        "candidate_records": records,
        "candidate_records_sha256": records_sha,
        "scenario_reconciliations": reconciliations,
    }
    successor["contract_sha256"] = canonical_json_sha256(successor)
    return successor, scenarios


def active_native_portfolio_contract(
    data_dir: Path,
    symbols: Iterable[str],
    *,
    timeframes: Iterable[str] = PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES,
    verify_exclusion_evidence: bool = True,
) -> Tuple[Dict[str, Any], List[str]]:
    """Validate the active manifest's complete symbol/timeframe roster.

    By default the universe resolver also rehashes authority files for every
    excluded symbol. A caller that already performed that exact verification
    in the same control flow may disable only the duplicate pass.
    """
    from pm_native_activation import (
        ACTIVE_POINTER_SCHEMA,
        ACTIVATION_MANIFEST_SCHEMA,
        NATIVE_EVIDENCE_CANDIDATE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_TECHNICAL_ERROR,
        UNIVERSE_AUTHENTICITY_SCHEMA,
        resolve_active_native_universe,
        sha256_file,
    )

    configured_symbols = list(
        dict.fromkeys(
            str(symbol).strip() for symbol in symbols if str(symbol).strip()
        )
    )
    requested_timeframes = [
        str(timeframe).strip().upper()
        for timeframe in timeframes
        if str(timeframe).strip()
    ]
    errors: List[str] = []
    native_root = (Path(data_dir) / "native").resolve()
    pointer_path = native_root / "ACTIVE.json"
    contract: Dict[str, Any] = {
        "pointer_schema": ACTIVE_POINTER_SCHEMA,
        "manifest_schema": ACTIVATION_MANIFEST_SCHEMA,
        "symbols": configured_symbols,
        "timeframes": requested_timeframes,
    }
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return contract, [f"NATIVE_ACTIVE_POINTER_UNREADABLE:{type(exc).__name__}"]
    if not isinstance(pointer, Mapping):
        return contract, ["NATIVE_ACTIVE_POINTER_NOT_OBJECT"]
    if str(pointer.get("schema_version") or "") != ACTIVE_POINTER_SCHEMA:
        errors.append("NATIVE_ACTIVE_POINTER_SCHEMA_INVALID")
    relative = str(pointer.get("manifest_relative_path") or "").strip()
    manifest_path = (native_root / relative).resolve() if relative else native_root
    try:
        manifest_path.relative_to(native_root)
    except ValueError:
        return contract, errors + ["NATIVE_ACTIVE_MANIFEST_PATH_UNSAFE"]
    if manifest_path == native_root or not manifest_path.is_file():
        return contract, errors + ["NATIVE_ACTIVE_MANIFEST_MISSING"]
    observed_sha = sha256_file(manifest_path)
    declared_sha = str(pointer.get("manifest_sha256") or "").lower()
    if observed_sha.lower() != declared_sha:
        errors.append("NATIVE_ACTIVE_MANIFEST_SHA256_MISMATCH")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return contract, errors + [
            f"NATIVE_ACTIVE_MANIFEST_UNREADABLE:{type(exc).__name__}"
        ]
    if not isinstance(manifest, Mapping):
        return contract, errors + ["NATIVE_ACTIVE_MANIFEST_NOT_OBJECT"]
    if str(manifest.get("schema_version") or "") != ACTIVATION_MANIFEST_SCHEMA:
        errors.append("NATIVE_ACTIVE_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("status") or "").lower() != "active":
        errors.append("NATIVE_ACTIVE_MANIFEST_NOT_ACTIVE")
    activation_id = str(manifest.get("activation_id") or "")
    if not activation_id or activation_id != str(pointer.get("activation_id") or ""):
        errors.append("NATIVE_ACTIVE_ACTIVATION_ID_MISMATCH")
    entries = (
        manifest.get("entries") if isinstance(manifest.get("entries"), Mapping) else {}
    )
    universe = resolve_active_native_universe(
        Path(data_dir),
        configured_symbols,
        required_timeframes=requested_timeframes,
        verify_exclusion_evidence=verify_exclusion_evidence,
        production_contract_required=True,
    )
    errors.extend(universe.errors)
    requested_symbols = (
        list(configured_symbols)
        if universe.exact_symbol_identity
        else sorted({value.upper() for value in configured_symbols})
    )
    contract["symbols"] = requested_symbols
    eligible_symbols = set(universe.eligible_symbols)
    excluded_symbols = dict(universe.excluded_symbols)
    authenticity_contract = (
        universe.eligibility_schema == UNIVERSE_AUTHENTICITY_SCHEMA
    )
    entry_contract: List[Dict[str, Any]] = []
    bars_sha_by_source: Dict[str, str] = {}
    sidecar_sha_by_source: Dict[str, str] = {}
    for symbol in requested_symbols:
        if not authenticity_contract and symbol in excluded_symbols:
            exclusion = excluded_symbols[symbol]
            entry_contract.append(
                {
                    "cell_id": f"{symbol}|EXCLUDED_NO_TRADE",
                    "status": "excluded_no_trade",
                    "decision_basis": "source_availability_only",
                    "reason_codes": list(exclusion.get("reason_codes") or []),
                }
            )
            continue
        if not authenticity_contract and symbol not in eligible_symbols:
            errors.append(f"NATIVE_UNIVERSE_SYMBOL_NOT_ELIGIBLE_OR_EXCLUDED:{symbol}")
            continue
        symbol_entries = (
            entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
        )
        for timeframe in requested_timeframes:
            cell_id = f"{symbol}|{timeframe}"
            entry = symbol_entries.get(timeframe)
            if not isinstance(entry, Mapping):
                errors.append(f"NATIVE_ACTIVE_CELL_MISSING:{cell_id}")
                continue
            status = str(entry.get("status") or "").lower()
            bars_sha = str(entry.get("bars_sha256") or "").lower()
            sidecar_sha = str(entry.get("sidecar_sha256") or "").lower()
            if authenticity_contract:
                resolved_status = universe.cell_status(symbol, timeframe)
                if status != resolved_status:
                    errors.append(f"NATIVE_ACTIVE_CELL_STATUS_MISMATCH:{cell_id}")
                if status not in {
                    NATIVE_EVIDENCE_CANDIDATE,
                    NATIVE_SOURCE_UNAVAILABLE,
                    NATIVE_TECHNICAL_ERROR,
                }:
                    errors.append(
                        f"NATIVE_ACTIVE_CELL_NOT_TERMINAL_AUTHENTICITY:{cell_id}:{status}"
                    )
            elif status != "active":
                errors.append(f"NATIVE_ACTIVE_CELL_NOT_ACTIVE:{cell_id}")
            entry_symbol = str(entry.get("symbol") or "")
            if (
                entry_symbol != symbol
                if universe.exact_symbol_identity
                else entry_symbol.upper() != symbol
            ):
                errors.append(f"NATIVE_ACTIVE_CELL_SYMBOL_MISMATCH:{cell_id}")
            if str(entry.get("timeframe") or "").upper() != timeframe:
                errors.append(f"NATIVE_ACTIVE_CELL_TIMEFRAME_MISMATCH:{cell_id}")
            requires_loadable_source = (
                status == NATIVE_EVIDENCE_CANDIDATE
                if authenticity_contract
                else status == "active"
            )
            if requires_loadable_source and (
                len(bars_sha) != 64 or len(sidecar_sha) != 64
            ):
                errors.append(f"NATIVE_ACTIVE_CELL_HASH_MISSING:{cell_id}")
            if authenticity_contract:
                entry_contract.append(
                    {
                        "cell_id": cell_id,
                        "status": status,
                        "bars_sha256": bars_sha,
                        "sidecar_sha256": sidecar_sha,
                        "reason_codes": list(entry.get("reason_codes") or []),
                    }
                )
            else:
                entry_contract.append(
                    {
                        "cell_id": cell_id,
                        "status": status,
                        "bars_sha256": bars_sha,
                        "sidecar_sha256": sidecar_sha,
                    }
                )
            if requires_loadable_source:
                bars_sha_by_source[cell_id] = bars_sha
                sidecar_sha_by_source[cell_id] = sidecar_sha
    contract.update(
        {
            "activation_id": activation_id,
            "activation_manifest_sha256": observed_sha,
            "activation_entry_count": len(entry_contract),
            "eligible_symbols": sorted(eligible_symbols & set(requested_symbols)),
            "excluded_symbols": sorted(set(excluded_symbols) & set(requested_symbols)),
            "universe_eligibility_explicit": bool(universe.explicit_contract),
            "activation_roster_sha256": canonical_json_sha256(
                sorted(entry_contract, key=lambda row: row["cell_id"])
            ),
            "bars_sha256_by_source": dict(sorted(bars_sha_by_source.items())),
            "sidecar_sha256_by_source": dict(sorted(sidecar_sha_by_source.items())),
        }
    )
    return contract, list(dict.fromkeys(errors))


def _validate_current_refit_successor(
    replay: Mapping[str, Any],
    *,
    expected_lineage: Mapping[str, Any],
    expected_roster: Mapping[str, Any],
    expected_cost_contract: Mapping[str, Any],
    expected_policies: Mapping[float, Mapping[str, Any]],
) -> List[str]:
    errors = _current_refit_roster_errors(expected_roster)
    successor = replay.get("current_refit_successor")
    fields = {
        "schema_version",
        "authority_status",
        "roster_sha256",
        "physical_ledger_sha256",
        "selected_cell_ids",
        "current_refit_lineage",
        "current_refit_lineage_sha256",
        "raw_current_refit_verification_sha256",
        "native_contract_sha256",
        "cost_contract_sha256",
        "mark_cost_contract_sha256",
        "policy_contracts_sha256",
        "candidate_records",
        "candidate_records_sha256",
        "scenario_reconciliations",
        "contract_sha256",
    }
    if not isinstance(successor, Mapping) or set(successor) != fields:
        return list(
            dict.fromkeys(errors + ["CURRENT_REFIT_SUCCESSOR_CONTRACT_INVALID"])
        )
    if (
        successor.get("schema_version") != CURRENT_REFIT_REPLAY_SUCCESSOR_SCHEMA_VERSION
        or successor.get("authority_status") != "PROCESS_VALIDATED_CURRENT_REFIT"
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_SCHEMA_INVALID")
    unsigned = dict(successor)
    claimed_contract_sha = unsigned.pop("contract_sha256", None)
    if claimed_contract_sha != canonical_json_sha256(unsigned):
        errors.append("CURRENT_REFIT_SUCCESSOR_SEAL_INVALID")
    for name in ("roster_sha256", "physical_ledger_sha256"):
        if successor.get(name) != expected_roster.get(name):
            errors.append(f"CURRENT_REFIT_SUCCESSOR_ROSTER_MISMATCH:{name}")
    selected = list(expected_roster.get("tradeable_cell_ids") or [])
    if successor.get("selected_cell_ids") != selected:
        errors.append("CURRENT_REFIT_SUCCESSOR_SELECTED_ROSTER_MISMATCH")
    lineage = expected_roster.get("current_refit_lineage")
    verification = expected_roster.get("current_refit_execution_verification")
    if successor.get("current_refit_lineage") != lineage or successor.get(
        "current_refit_lineage_sha256"
    ) != canonical_json_sha256(lineage):
        errors.append("CURRENT_REFIT_SUCCESSOR_LINEAGE_MISMATCH")
    if successor.get("raw_current_refit_verification_sha256") != canonical_json_sha256(
        verification
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_RAW_REVERIFICATION_MISMATCH")
    native_identity = {
        name: expected_lineage.get(name)
        for name in (
            "native_activation_id",
            "native_activation_manifest_sha256",
            "native_activation_roster_sha256",
            "native_bars_sha256_by_source",
            "native_sidecar_sha256_by_source",
        )
    }
    if successor.get("native_contract_sha256") != canonical_json_sha256(
        native_identity
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_NATIVE_CONTRACT_MISMATCH")
    if successor.get("cost_contract_sha256") != canonical_json_sha256(
        dict(expected_cost_contract)
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_COST_CONTRACT_MISMATCH")
    if successor.get("mark_cost_contract_sha256") != canonical_json_sha256(
        portfolio_mark_cost_contract(expected_cost_contract)
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_MARK_CONTRACT_MISMATCH")
    expected_policy_sha = canonical_json_sha256(
        {
            str(float(key)): dict(value)
            for key, value in sorted(expected_policies.items())
        }
    )
    if successor.get("policy_contracts_sha256") != expected_policy_sha:
        errors.append("CURRENT_REFIT_SUCCESSOR_POLICY_CONTRACT_MISMATCH")

    records = successor.get("candidate_records")
    if not isinstance(records, list) or len(records) > 2_000_000:
        errors.append("CURRENT_REFIT_SUCCESSOR_CANDIDATE_ROSTER_INVALID")
        records = []
    records_sha = canonical_json_sha256(records)
    if successor.get("candidate_records_sha256") != records_sha:
        errors.append("CURRENT_REFIT_SUCCESSOR_CANDIDATE_SEAL_INVALID")
    candidates: List[CandidateTrade] = []
    for offset, record in enumerate(records):
        try:
            candidates.append(candidate_trade_from_record(record))
        except Exception:
            errors.append(f"CURRENT_REFIT_SUCCESSOR_CANDIDATE_INVALID:{offset}")
    if [value.candidate_id for value in candidates] != sorted(
        value.candidate_id for value in candidates
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_CANDIDATE_ORDER_INVALID")
    errors.extend(
        validate_current_refit_replay_candidates(
            candidates,
            expected_roster,
            replayed_cell_ids=replay.get("replayed_cell_ids") or [],
        )
    )

    reconciliations = successor.get("scenario_reconciliations")
    scenarios = replay.get("scenarios")
    if not isinstance(reconciliations, list) or not isinstance(scenarios, list):
        errors.append("CURRENT_REFIT_SUCCESSOR_SCENARIO_ROSTER_INVALID")
        reconciliations = []
        scenarios = []
    balances = [
        _finite(row.get("starting_balance"), float("nan"))
        for row in reconciliations
        if isinstance(row, Mapping)
    ]
    if (
        len(reconciliations) != len(expected_policies)
        or len(balances) != len(reconciliations)
        or any(not math.isfinite(value) for value in balances)
        or len(set(balances)) != len(balances)
    ):
        errors.append("CURRENT_REFIT_SUCCESSOR_SCENARIO_ROSTER_INVALID")
    for balance, raw_policy in expected_policies.items():
        label = f"{float(balance):.2f}"
        reconciliation = next(
            (
                row
                for row in reconciliations
                if isinstance(row, Mapping)
                and abs(
                    _finite(row.get("starting_balance"), float("nan")) - float(balance)
                )
                <= 1e-6
            ),
            None,
        )
        scenario = next(
            (
                row
                for row in scenarios
                if isinstance(row, Mapping)
                and abs(
                    _finite(row.get("starting_balance"), float("nan")) - float(balance)
                )
                <= 1e-6
            ),
            None,
        )
        if not isinstance(reconciliation, Mapping) or not isinstance(scenario, Mapping):
            errors.append(f"CURRENT_REFIT_SUCCESSOR_SCENARIO_MISSING:{label}")
            continue
        policy_value = dict(raw_policy)
        policy_value["allowed_promotion_statuses"] = tuple(
            policy_value.get("allowed_promotion_statuses") or ()
        )
        try:
            policy = PortfolioPolicy(**policy_value)
            if policy.starting_balance != float(
                balance
            ) or policy.allowed_promotion_statuses != (
                "PROCESS_VALIDATED_CURRENT_REFIT",
            ):
                raise ValueError("policy identity")
            result = SynchronizedPortfolioSimulator(policy)._run(
                candidates, allow_current_refit=True
            )
            derived = _current_refit_scenario_reconciliation(
                result,
                candidate_records_sha256=records_sha,
            )
        except Exception:
            errors.append(f"CURRENT_REFIT_SUCCESSOR_REPLAY_FAILED:{label}")
            continue
        if dict(reconciliation) != derived:
            errors.append(f"CURRENT_REFIT_SUCCESSOR_RECONCILIATION_MISMATCH:{label}")
        if canonical_json_sha256(scenario.get("policy")) != canonical_json_sha256(
            asdict(policy)
        ):
            errors.append(f"CURRENT_REFIT_SUCCESSOR_SCENARIO_POLICY_MISMATCH:{label}")
        if scenario.get("summary") != result.get("summary"):
            errors.append(f"CURRENT_REFIT_SUCCESSOR_SUMMARY_MISMATCH:{label}")
    return list(dict.fromkeys(errors))


def validate_funded_portfolio_replay_evidence(
    evidence: Any,
    *,
    expected_lineage: Mapping[str, Any],
    expected_roster: Mapping[str, Any],
    expected_cost_contract: Mapping[str, Any],
    expected_policies: Mapping[float, Mapping[str, Any]],
    minimum_calendar_days: float,
    minimum_accepted_positions: int,
    minimum_profit_factor: float,
    minimum_return_pct: float,
    maximum_drawdown_pct: float,
    require_mtm: bool = True,
    maximum_bootstrap_ruin_upper_95pct: float = 1.0,
    maximum_bootstrap_p95_drawdown_pct: Optional[float] = None,
    minimum_bootstrap_daily_return_observations: int = (
        MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS
    ),
) -> List[str]:
    """Re-evaluate the legacy prelockbox funded evidence contract."""
    errors: List[str] = []
    current_refit = expected_roster.get("authoritative") is True
    if not isinstance(evidence, Mapping):
        return ["PORTFOLIO_EVIDENCE_NOT_OBJECT"]
    if (
        str(evidence.get("schema_version") or "")
        != PORTFOLIO_REPLAY_EVIDENCE_SCHEMA_VERSION
    ):
        errors.append("PORTFOLIO_EVIDENCE_SCHEMA_INVALID")
    if not portfolio_replay_evidence_seal_valid(evidence):
        errors.append("PORTFOLIO_EVIDENCE_SEAL_INVALID")
    if str(evidence.get("verdict") or "").upper() != "PASS":
        errors.append("PORTFOLIO_EVIDENCE_VERDICT_NOT_PASS")
    if evidence.get("failures"):
        errors.append("PORTFOLIO_EVIDENCE_FAILURES_PRESENT")
    bootstrap_ruin_limit = _finite(maximum_bootstrap_ruin_upper_95pct, float("inf"))
    if not (0.0 <= bootstrap_ruin_limit <= 1.0):
        errors.append("PORTFOLIO_EVIDENCE_BOOTSTRAP_RUIN_POLICY_INVALID")
    minimum_bootstrap_observations = _finite(
        minimum_bootstrap_daily_return_observations, float("nan")
    )
    if not (
        math.isfinite(minimum_bootstrap_observations)
        and minimum_bootstrap_observations.is_integer()
        and minimum_bootstrap_observations
        >= MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS
    ):
        errors.append("PORTFOLIO_EVIDENCE_BOOTSTRAP_DEPTH_POLICY_INVALID")
    bootstrap_drawdown_limit = _finite(
        maximum_bootstrap_p95_drawdown_pct
        if maximum_bootstrap_p95_drawdown_pct is not None
        else maximum_drawdown_pct,
        float("inf"),
    )
    if not (0.0 < bootstrap_drawdown_limit <= float(maximum_drawdown_pct) <= 20.0):
        errors.append("PORTFOLIO_EVIDENCE_BOOTSTRAP_DRAWDOWN_POLICY_INVALID")

    lineage = (
        evidence.get("lineage") if isinstance(evidence.get("lineage"), Mapping) else {}
    )
    for key, expected in expected_lineage.items():
        if lineage.get(key) != expected:
            errors.append(f"PORTFOLIO_EVIDENCE_LINEAGE_MISMATCH:{key}")

    roster = (
        evidence.get("roster") if isinstance(evidence.get("roster"), Mapping) else {}
    )
    for key in (
        "symbols",
        "required_timeframes",
        "required_regimes",
        "expected_cell_count",
        "expected_cell_ids",
        "tradeable_cell_ids",
        "roster_sha256",
    ):
        if roster.get(key) != expected_roster.get(key):
            errors.append(f"PORTFOLIO_EVIDENCE_ROSTER_MISMATCH:{key}")
    if current_refit:
        for key in (
            "schema_version",
            "authoritative",
            "current_refit_execution_reverified",
            "authority_status",
            "possible_regimes",
            "physical_winner_cell_count",
            "physical_ledger_sha256",
            "book_accounting",
            "current_refit_lineage",
            "rows",
        ):
            if roster.get(key) != expected_roster.get(key):
                errors.append(f"PORTFOLIO_EVIDENCE_ROSTER_MISMATCH:{key}")
        if (
            expected_roster.get("schema_version")
            != PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION
            or expected_roster.get("authority_status")
            != "PROCESS_VALIDATED_CURRENT_REFIT"
        ):
            errors.append("PORTFOLIO_EVIDENCE_EXPECTED_ROSTER_NOT_AUTHORITATIVE")

    replay = (
        evidence.get("replay") if isinstance(evidence.get("replay"), Mapping) else {}
    )
    if bool(replay.get("partial_run")):
        errors.append("PORTFOLIO_EVIDENCE_PARTIAL_RUN")
    if bool(replay.get("allow_cell_errors")):
        errors.append("PORTFOLIO_EVIDENCE_ALLOWED_CELL_ERRORS")
    if replay.get("cell_generation_errors"):
        errors.append("PORTFOLIO_EVIDENCE_CELL_ERRORS_PRESENT")
    expected_status = (
        "PROCESS_VALIDATED_CURRENT_REFIT" if current_refit else "FROZEN_PRELOCKBOX"
    )
    if str(replay.get("prelockbox_source_status") or "").upper() != expected_status:
        errors.append("PORTFOLIO_EVIDENCE_PRELOCKBOX_STATUS_INVALID")
    if list(replay.get("evidence_only_status_allowlist") or []) != [expected_status]:
        errors.append("PORTFOLIO_EVIDENCE_STATUS_ALLOWLIST_INVALID")
    if current_refit:
        if not str(replay.get("current_refit_execution_access_id") or "").strip():
            errors.append("PORTFOLIO_EVIDENCE_CURRENT_REFIT_ACCESS_MISSING")
    elif not str(replay.get("global_lockbox_access_id") or "").strip():
        errors.append("PORTFOLIO_EVIDENCE_GLOBAL_LOCKBOX_ACCESS_MISSING")
    if sorted(replay.get("replayed_cell_ids") or []) != sorted(
        expected_roster.get("tradeable_cell_ids") or []
    ):
        errors.append("PORTFOLIO_EVIDENCE_REPLAYED_CELL_ROSTER_MISMATCH")
    if current_refit:
        if replay.get("current_refit_roster_sha256") != expected_roster.get(
            "roster_sha256"
        ):
            errors.append("PORTFOLIO_EVIDENCE_CURRENT_REFIT_ROSTER_MISMATCH")
        candidate_cells = replay.get("candidate_cell_ids")
        if (
            not isinstance(candidate_cells, list)
            or any(not isinstance(value, str) for value in candidate_cells)
            or len(set(candidate_cells)) != len(candidate_cells)
            or not set(candidate_cells).issubset(
                set(expected_roster.get("tradeable_cell_ids") or [])
            )
        ):
            errors.append("PORTFOLIO_EVIDENCE_CANDIDATE_CELL_ROSTER_INVALID")
    if (
        str(replay.get("candidate_schema_version") or "")
        != PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION
    ):
        errors.append("PORTFOLIO_EVIDENCE_CANDIDATE_SCHEMA_INVALID")
    if (
        str(replay.get("portfolio_schema_version") or "")
        != PORTFOLIO_REPLAY_SCHEMA_VERSION
    ):
        errors.append("PORTFOLIO_EVIDENCE_PORTFOLIO_SCHEMA_INVALID")
    if replay.get("cost_contract") != dict(expected_cost_contract):
        errors.append("PORTFOLIO_EVIDENCE_COST_CONTRACT_MISMATCH")
    if not all(
        bool(expected_cost_contract.get(key))
        for key in (
            "use_spread",
            "swap_model_enabled",
        )
    ):
        errors.append("PORTFOLIO_EVIDENCE_CURRENT_COST_CONTRACT_DISABLED")
    expected_mark_cost_contract = portfolio_mark_cost_contract(expected_cost_contract)
    if replay.get("mark_cost_contract") != expected_mark_cost_contract:
        errors.append("PORTFOLIO_EVIDENCE_MARK_COST_CONTRACT_MISMATCH")

    window = replay.get("window") if isinstance(replay.get("window"), Mapping) else {}
    if str(window.get("mode") or "") != "common_intersection":
        errors.append("PORTFOLIO_EVIDENCE_WINDOW_NOT_COMMON")
    expected_partition = (
        "current_refit_management_replay" if current_refit else "final_lockbox"
    )
    if str(window.get("partition") or "") != expected_partition:
        errors.append("PORTFOLIO_EVIDENCE_WINDOW_NOT_FINAL_LOCKBOX")
    try:
        start = _timestamp(window.get("evaluation_start"))
        end = _timestamp(window.get("evaluation_end"))
        span_days = (end - start).total_seconds() / 86400.0
        if end <= start or span_days + 1e-9 < float(minimum_calendar_days):
            errors.append("PORTFOLIO_EVIDENCE_CALENDAR_HORIZON_TOO_SHORT")
    except Exception:
        errors.append("PORTFOLIO_EVIDENCE_WINDOW_INVALID")

    native_sources = replay.get("native_sources")
    if not isinstance(native_sources, Mapping):
        native_sources = {}
    for cell_id in expected_roster.get("tradeable_cell_ids") or []:
        source = native_sources.get(cell_id)
        if not isinstance(source, Mapping):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_SOURCE_MISSING:{cell_id}")
            continue
        if str(source.get("source_kind") or "").lower() != "broker_native":
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_SOURCE_INVALID:{cell_id}")
        if not bool(source.get("provenance_valid")):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_PROVENANCE_INVALID:{cell_id}")
        if str(source.get("timestamp_semantics") or "").lower() != "bar_open_utc":
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_TIMESTAMP_INVALID:{cell_id}")
        if source.get("activation_id") != expected_lineage.get("native_activation_id"):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_ACTIVATION_MISMATCH:{cell_id}")
        if source.get("activation_manifest_sha256") != expected_lineage.get(
            "native_activation_manifest_sha256"
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_MANIFEST_MISMATCH:{cell_id}")
        parts = str(cell_id).split("|")
        source_id = "|".join(parts[:2]) if len(parts) >= 2 else ""
        bars_by_source = expected_lineage.get("native_bars_sha256_by_source")
        sidecars_by_source = expected_lineage.get("native_sidecar_sha256_by_source")
        expected_bars_sha = (
            bars_by_source.get(source_id)
            if isinstance(bars_by_source, Mapping)
            else None
        )
        expected_sidecar_sha = (
            sidecars_by_source.get(source_id)
            if isinstance(sidecars_by_source, Mapping)
            else None
        )
        if source.get("source_sha256") != expected_bars_sha:
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_BARS_HASH_MISMATCH:{cell_id}")
        if source.get("sidecar_sha256") != expected_sidecar_sha:
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_SIDECAR_HASH_MISMATCH:{cell_id}")

    valuation_sources = replay.get("valuation_sources")
    if not isinstance(valuation_sources, Mapping):
        valuation_sources = {}
    bars_by_source = expected_lineage.get("native_bars_sha256_by_source")
    sidecars_by_source = expected_lineage.get("native_sidecar_sha256_by_source")
    valuation_symbols = sorted(
        {
            str(cell_id).split("|", 1)[0]
            for cell_id in (expected_roster.get("tradeable_cell_ids") or [])
            if str(cell_id).strip()
        }
    )
    for symbol in valuation_symbols:
        exact_symbol = str(symbol)
        source_id = f"{exact_symbol}|M5"
        source = valuation_sources.get(exact_symbol)
        if not isinstance(source, Mapping):
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_SOURCE_MISSING:{exact_symbol}"
            )
            continue
        if (
            str(source.get("source_kind") or "").lower() != "broker_native"
            or str(source.get("symbol") or "") != exact_symbol
            or str(source.get("timeframe") or "").upper() != "M5"
        ):
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_SOURCE_INVALID:{exact_symbol}"
            )
        if not bool(source.get("provenance_valid")):
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_PROVENANCE_INVALID:{exact_symbol}"
            )
        if str(source.get("timestamp_semantics") or "").lower() != "bar_open_utc":
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_TIMESTAMP_INVALID:{exact_symbol}"
            )
        if source.get("activation_id") != expected_lineage.get("native_activation_id"):
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_ACTIVATION_MISMATCH:{exact_symbol}"
            )
        if source.get("activation_manifest_sha256") != expected_lineage.get(
            "native_activation_manifest_sha256"
        ):
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_MANIFEST_MISMATCH:{exact_symbol}"
            )
        expected_bars_sha = (
            bars_by_source.get(source_id)
            if isinstance(bars_by_source, Mapping)
            else None
        )
        expected_sidecar_sha = (
            sidecars_by_source.get(source_id)
            if isinstance(sidecars_by_source, Mapping)
            else None
        )
        if source.get("source_sha256") != expected_bars_sha:
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_BARS_HASH_MISMATCH:{exact_symbol}"
            )
        if source.get("sidecar_sha256") != expected_sidecar_sha:
            errors.append(
                f"PORTFOLIO_EVIDENCE_VALUATION_SIDECAR_HASH_MISMATCH:{exact_symbol}"
            )

    scenarios = (
        replay.get("scenarios") if isinstance(replay.get("scenarios"), list) else []
    )
    scenario_balances = [
        _finite(row.get("starting_balance"), float("nan"))
        for row in scenarios
        if isinstance(row, Mapping)
    ]
    if (
        len(scenarios) != len(expected_policies)
        or len(scenario_balances) != len(scenarios)
        or any(not math.isfinite(value) for value in scenario_balances)
        or len(set(scenario_balances)) != len(scenario_balances)
    ):
        errors.append("PORTFOLIO_EVIDENCE_SCENARIO_ROSTER_INVALID")
    for required_balance, expected_policy in expected_policies.items():
        required_statuses = (
            ("PROCESS_VALIDATED_CURRENT_REFIT",)
            if current_refit
            else ("FROZEN_PRELOCKBOX",)
        )
        if (
            tuple(expected_policy.get("allowed_promotion_statuses") or ())
            != required_statuses
        ):
            errors.append("PORTFOLIO_EVIDENCE_POLICY_NOT_PRELOCKBOX_ONLY")
        scenario = next(
            (
                row
                for row in scenarios
                if isinstance(row, Mapping)
                and abs(
                    _finite(row.get("starting_balance"), -1.0) - float(required_balance)
                )
                <= 1e-6
            ),
            None,
        )
        label = f"{float(required_balance):.2f}"
        if not isinstance(scenario, Mapping):
            errors.append(f"PORTFOLIO_EVIDENCE_SCENARIO_MISSING:{label}")
            continue
        if canonical_json_sha256(scenario.get("policy")) != canonical_json_sha256(
            dict(expected_policy)
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_POLICY_MISMATCH:{label}")
        summary = (
            scenario.get("summary")
            if isinstance(scenario.get("summary"), Mapping)
            else {}
        )
        if str(summary.get("schema_version") or "") != PORTFOLIO_REPLAY_SCHEMA_VERSION:
            errors.append(f"PORTFOLIO_EVIDENCE_SUMMARY_SCHEMA_INVALID:{label}")
        if (
            abs(
                _finite(summary.get("starting_balance"), float("inf"))
                - float(required_balance)
            )
            > 1e-6
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_SUMMARY_BALANCE_MISMATCH:{label}")
        if int(_finite(summary.get("accepted_positions"), 0.0)) < int(
            minimum_accepted_positions
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_TOO_FEW_POSITIONS:{label}")
        raw_pf = summary.get("profit_factor")
        profit_factor = (
            float("inf") if str(raw_pf).lower() == "inf" else _finite(raw_pf, 0.0)
        )
        if profit_factor + 1e-12 < float(minimum_profit_factor):
            errors.append(f"PORTFOLIO_EVIDENCE_PROFIT_FACTOR_TOO_LOW:{label}")
        if _finite(summary.get("return_pct"), -float("inf")) + 1e-12 < float(
            minimum_return_pct
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_RETURN_TOO_LOW:{label}")
        if (
            _finite(summary.get("max_mark_to_market_drawdown_pct"), float("inf"))
            > float(maximum_drawdown_pct) + 1e-12
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_DRAWDOWN_TOO_HIGH:{label}")
        if (
            _finite(
                summary.get("max_initial_stop_stress_drawdown_pct"),
                float("inf"),
            )
            > float(maximum_drawdown_pct) + 1e-12
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_STOP_STRESS_DRAWDOWN_TOO_HIGH:{label}")
        if bool(summary.get("observed_ruin")):
            errors.append(f"PORTFOLIO_EVIDENCE_OBSERVED_RUIN:{label}")
        bootstrap = summary.get("exploratory_sequence_bootstrap")
        if not isinstance(bootstrap, Mapping):
            errors.append(f"PORTFOLIO_EVIDENCE_BOOTSTRAP_UNAVAILABLE:{label}")
        else:
            daily_observations = _finite(
                bootstrap.get("daily_return_observations"), float("nan")
            )
            if not (
                math.isfinite(daily_observations) and daily_observations.is_integer()
            ):
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_DAILY_OBSERVATIONS_UNAVAILABLE:{label}"
                )
            elif daily_observations < minimum_bootstrap_observations:
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_DAILY_OBSERVATIONS_TOO_LOW:{label}"
                )
            ruin_interval = bootstrap.get("ruin_probability_wilson_95pct")
            ruin_lower = (
                _finite(ruin_interval[0], float("nan"))
                if isinstance(ruin_interval, (list, tuple)) and len(ruin_interval) == 2
                else float("nan")
            )
            ruin_upper = (
                _finite(ruin_interval[1], float("nan"))
                if isinstance(ruin_interval, (list, tuple)) and len(ruin_interval) == 2
                else float("nan")
            )
            if not (
                math.isfinite(ruin_lower)
                and math.isfinite(ruin_upper)
                and 0.0 <= ruin_lower <= ruin_upper <= 100.0
            ):
                errors.append(f"PORTFOLIO_EVIDENCE_BOOTSTRAP_UNAVAILABLE:{label}")
            elif ruin_upper > bootstrap_ruin_limit + 1e-12:
                errors.append(f"PORTFOLIO_EVIDENCE_BOOTSTRAP_RUIN_TOO_HIGH:{label}")
            bootstrap_drawdown = _finite(
                bootstrap.get("p95_max_drawdown_pct"), float("nan")
            )
            if not (
                math.isfinite(bootstrap_drawdown) and 0.0 <= bootstrap_drawdown <= 100.0
            ):
                errors.append(f"PORTFOLIO_EVIDENCE_BOOTSTRAP_UNAVAILABLE:{label}")
            elif bootstrap_drawdown > bootstrap_drawdown_limit + 1e-12:
                errors.append(f"PORTFOLIO_EVIDENCE_BOOTSTRAP_DRAWDOWN_TOO_HIGH:{label}")
        if (
            require_mtm
            and _finite(summary.get("mtm_candidate_coverage_pct"), 0.0) < 100.0 - 1e-9
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_MTM_INCOMPLETE:{label}")
        policy = expected_policy
        if (
            _finite(
                summary.get("max_admission_total_stop_risk_pct"),
                float("inf"),
            )
            > _finite(policy.get("max_open_risk_pct"), 0.0) + 1e-9
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_ADMISSION_RISK_CAP_BREACH:{label}")
        if (
            _finite(
                summary.get("max_admission_factor_group_stop_risk_pct"),
                float("inf"),
            )
            > _finite(policy.get("max_factor_group_open_risk_pct"), 0.0) + 1e-9
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_FACTOR_RISK_CAP_BREACH:{label}")
        if (
            _finite(summary.get("max_margin_utilization_pct"), float("inf"))
            > _finite(policy.get("max_margin_utilization_pct"), 0.0) + 1e-9
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_MARGIN_CAP_BREACH:{label}")
    if current_refit:
        errors.extend(
            _validate_current_refit_successor(
                replay,
                expected_lineage=expected_lineage,
                expected_roster=expected_roster,
                expected_cost_contract=expected_cost_contract,
                expected_policies=expected_policies,
            )
        )
    return list(dict.fromkeys(errors))


@dataclass
class _OpenPosition:
    candidate: CandidateTrade
    volume: float
    initial_risk: float
    initial_margin: float
    remaining_fraction: float = 1.0
    last_mark_per_current_lot: float = 0.0
    realized_pnl: float = 0.0
    price_pnl: float = 0.0
    swap: float = 0.0
    closed_legs: int = 0

    @property
    def open_risk(self) -> float:
        return max(0.0, self.initial_risk * self.remaining_fraction)

    @property
    def open_margin(self) -> float:
        return max(0.0, self.initial_margin * self.remaining_fraction)

    @property
    def unrealized_pnl(self) -> float:
        return self.volume * self.remaining_fraction * self.last_mark_per_current_lot


class TrailingCorrelationBook:
    """Point-in-time daily correlation lookup with no future observations."""

    def __init__(
        self,
        returns_by_symbol: Mapping[str, pd.Series],
        *,
        lookback: int = 60,
        min_observations: int = 30,
    ):
        self.lookback = max(2, int(lookback))
        self.min_observations = max(2, int(min_observations))
        self.returns: Dict[str, pd.Series] = {}
        for symbol, raw in returns_by_symbol.items():
            series = pd.to_numeric(raw, errors="coerce").dropna().sort_index()
            index = pd.DatetimeIndex([_timestamp(value) for value in series.index])
            self.returns[str(symbol).upper()] = pd.Series(
                series.to_numpy(dtype=float), index=index
            )

    def __call__(self, left: str, right: str, asof: pd.Timestamp) -> Optional[float]:
        left_series = self.returns.get(str(left).upper())
        right_series = self.returns.get(str(right).upper())
        if left_series is None or right_series is None:
            return None
        cutoff = _timestamp(asof).normalize()
        joined = (
            pd.concat(
                [
                    left_series[left_series.index < cutoff],
                    right_series[right_series.index < cutoff],
                ],
                axis=1,
                join="inner",
            )
            .dropna()
            .tail(self.lookback)
        )
        if len(joined) < self.min_observations:
            return None
        correlation = float(joined.iloc[:, 0].corr(joined.iloc[:, 1]))
        return correlation if math.isfinite(correlation) else None


class SynchronizedPortfolioSimulator:
    """Chronological admission and cash-flow simulator for one shared account."""

    def __init__(
        self,
        policy: PortfolioPolicy,
        correlation_lookup: Optional[CorrelationLookup] = None,
    ):
        policy.validate()
        self.policy = policy
        self.correlation_lookup = correlation_lookup

    def run_current_refit(
        self,
        raw_candidates: Iterable[CandidateTrade],
        *,
        ledger: Any,
        symbols: Iterable[str],
        current_book_decision: Any,
        book_evidence_family: Any,
        historical_process_decision: Mapping[str, Any],
        causal_precommit_payload: dict[str, Any],
        causal_evidence_payload: dict[str, Any],
        causal_reconciliation_payload: dict[str, Any],
        accepted_book_generation_receipt: Mapping[str, Any],
        current_refit_receipt: Mapping[str, Any],
        as_of_utc: Any,
        current_refit_raw_manifest: Any,
        current_refit_artifact_root: Any,
        replayed_cell_ids: Iterable[str],
    ) -> Dict[str, Any]:
        candidates = list(raw_candidates)
        roster, errors = build_current_refit_replay_roster(
            ledger,
            symbols,
            current_book_decision=current_book_decision,
            book_evidence_family=book_evidence_family,
            historical_process_decision=historical_process_decision,
            causal_precommit_payload=causal_precommit_payload,
            causal_evidence_payload=causal_evidence_payload,
            causal_reconciliation_payload=causal_reconciliation_payload,
            accepted_book_generation_receipt=accepted_book_generation_receipt,
            current_refit_receipt=current_refit_receipt,
            as_of_utc=as_of_utc,
            current_refit_raw_manifest=current_refit_raw_manifest,
            current_refit_artifact_root=current_refit_artifact_root,
        )
        if errors:
            raise ValueError(";".join(errors))
        errors = validate_current_refit_replay_candidates(
            candidates,
            roster,
            replayed_cell_ids=replayed_cell_ids,
        )
        if errors:
            raise ValueError(";".join(errors))
        return self._run(candidates, allow_current_refit=True)

    def run(self, raw_candidates: Iterable[CandidateTrade]) -> Dict[str, Any]:
        return self._run(raw_candidates, allow_current_refit=False)

    def _run(
        self,
        raw_candidates: Iterable[CandidateTrade],
        *,
        allow_current_refit: bool,
    ) -> Dict[str, Any]:
        bounded = list(islice(raw_candidates, MAX_PORTFOLIO_REPLAY_CANDIDATES + 1))
        if len(bounded) > MAX_PORTFOLIO_REPLAY_CANDIDATES:
            raise ValueError("portfolio replay candidate roster exceeds its bound")
        candidates = [candidate.normalized() for candidate in bounded]
        if not candidates:
            raise ValueError("portfolio replay candidate roster is empty")
        if not allow_current_refit and any(
            candidate.promotion_status == "PROCESS_VALIDATED_CURRENT_REFIT"
            for candidate in candidates
        ):
            raise ValueError("CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED")
        identifiers = [candidate.candidate_id for candidate in candidates]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("candidate_id values must be unique")
        # The simulator is not an evidence authenticator, but it must not let a
        # caller evade the exact-cell concurrency rule by relabelling the same
        # specialist slot.  Production producers authenticate these values
        # upstream; this bijection is defense in depth for every replay caller.
        identity_by_cell: Dict[str, Tuple[str, str, str, str]] = {}
        cell_by_slot: Dict[Tuple[str, str, str], str] = {}
        for candidate in candidates:
            cell_id = candidate.cell_id.strip()
            if not cell_id:
                raise ValueError("candidate cell_id is required")
            identity = (
                candidate.symbol,
                candidate.timeframe,
                candidate.regime,
                candidate.strategy_name,
            )
            slot = identity[:3]
            prior_identity = identity_by_cell.setdefault(cell_id, identity)
            if prior_identity != identity:
                raise ValueError(
                    "candidate cell_id maps to multiple specialist identities"
                )
            prior_cell = cell_by_slot.setdefault(slot, cell_id)
            if prior_cell != cell_id:
                raise ValueError(
                    "specialist slot maps to multiple candidate cell_id values"
                )
        input_receipt = {
            **portfolio_replay_candidate_roster_receipt(candidates),
            "policy_sha256": canonical_json_sha256(asdict(self.policy)),
            "execution_authority_mode": (
                "CURRENT_REFIT_REVERIFIED"
                if allow_current_refit
                else "STANDARD_SYNCHRONIZED_REPLAY"
            ),
            "path_contract_version": PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION,
        }

        allowed = {
            str(status).upper() for status in self.policy.allowed_promotion_statuses
        }
        entry_events: Dict[pd.Timestamp, List[CandidateTrade]] = defaultdict(list)
        leg_events: Dict[pd.Timestamp, List[Tuple[str, int]]] = defaultdict(list)
        mark_events: Dict[pd.Timestamp, List[Tuple[str, MarkPoint]]] = defaultdict(list)
        blocked: List[Dict[str, Any]] = []
        for candidate in candidates:
            if candidate.promotion_status not in allowed:
                blocked.append(
                    self._block(
                        candidate, candidate.entry_time, "PROMOTION_STATUS_NOT_ALLOWED"
                    )
                )
                continue
            entry_events[candidate.entry_time].append(candidate)
            for leg_index, leg in enumerate(candidate.legs):
                leg_events[leg.exit_time].append((candidate.candidate_id, leg_index))
            for mark in candidate.marks:
                mark_events[mark.time].append((candidate.candidate_id, mark))

        timeline = sorted(set(entry_events) | set(leg_events) | set(mark_events))
        if len(timeline) > MAX_PORTFOLIO_REPLAY_TIMESTAMPS:
            raise ValueError("portfolio replay timeline exceeds its bound")
        open_positions: Dict[str, _OpenPosition] = {}
        accepted_ids: set[str] = set()
        closed_positions: List[Dict[str, Any]] = []
        path: List[Dict[str, Any]] = []
        balance = float(self.policy.starting_balance)
        peak_balance = balance
        peak_equity = balance
        peak_stop_equity = balance
        max_balance_dd = 0.0
        max_equity_dd = 0.0
        max_stop_equity_dd = 0.0
        max_open_risk_pct = 0.0
        max_conservative_open_risk_pct = 0.0
        max_admission_total_stop_risk_pct = 0.0
        max_admission_factor_group_stop_risk_pct = 0.0
        max_margin_utilization_pct = 0.0
        max_concurrent = 0
        unknown_correlation_checks = 0
        known_correlation_checks = 0
        admission_tie_batches = 0
        mark_updates = 0
        realized_at_time: Dict[pd.Timestamp, float] = defaultdict(float)

        def totals() -> Tuple[float, float, float]:
            open_risk = sum(position.open_risk for position in open_positions.values())
            open_margin = sum(
                position.open_margin for position in open_positions.values()
            )
            unrealized = sum(
                position.unrealized_pnl for position in open_positions.values()
            )
            return open_risk, open_margin, unrealized

        def close_leg(candidate_id: str, leg_index: int, stamp: pd.Timestamp) -> None:
            nonlocal balance
            position = open_positions.get(candidate_id)
            if position is None:
                return
            leg = position.candidate.legs[leg_index]
            cashflow = position.volume * leg.net_pnl_per_entry_lot
            position.realized_pnl += cashflow
            position.price_pnl += position.volume * leg.price_pnl_per_entry_lot
            position.swap += position.volume * leg.swap_per_entry_lot
            position.remaining_fraction = max(
                0.0, position.remaining_fraction - leg.fraction
            )
            position.closed_legs += 1
            balance += cashflow
            realized_at_time[stamp] += cashflow
            if position.remaining_fraction <= 1e-7 or position.closed_legs >= len(
                position.candidate.legs
            ):
                closed_positions.append(
                    {
                        "candidate_id": candidate_id,
                        "cell_id": position.candidate.cell_id,
                        "symbol": position.candidate.symbol,
                        "timeframe": position.candidate.timeframe,
                        "regime": position.candidate.regime,
                        "strategy_name": position.candidate.strategy_name,
                        "entry_time": position.candidate.entry_time.isoformat(),
                        "exit_time": stamp.isoformat(),
                        "direction": position.candidate.direction,
                        "volume": position.volume,
                        "actual_risk": position.initial_risk,
                        "initial_margin": position.initial_margin,
                        "net_pnl": position.realized_pnl,
                        "price_pnl": position.price_pnl,
                        "swap": position.swap,
                        "exit_legs": position.closed_legs,
                        "priority_score": position.candidate.priority_score,
                    }
                )
                del open_positions[candidate_id]

        for stamp in timeline:
            admission_events = 0
            # Existing positions close before new entries at the same timestamp.
            for candidate_id, leg_index in leg_events.get(stamp, []):
                if (
                    candidate_id in open_positions
                    and open_positions[candidate_id].candidate.entry_time < stamp
                ):
                    close_leg(candidate_id, leg_index, stamp)

            # Apply the common causal valuation tape before sizing/admission.
            # Only positions already open before this decision timestamp may
            # consume the observation. A new same-t entry must wait for the next
            # M5 observation rather than inheriting a mark from before it existed.
            for candidate_id, mark in mark_events.get(stamp, []):
                if mark.observation_time > stamp:
                    raise ValueError(
                        "mark observation time cannot be after portfolio decision time"
                    )
                position = open_positions.get(candidate_id)
                if position is not None and position.candidate.entry_time < stamp:
                    position.last_mark_per_current_lot = mark.pnl_per_current_lot
                    mark_updates += 1

            batch = sorted(
                entry_events.get(stamp, []),
                key=lambda candidate: (
                    -candidate.priority_score,
                    candidate.cell_id,
                    candidate.candidate_id,
                ),
            )
            if len(batch) > 1 and len(
                {candidate.priority_score for candidate in batch}
            ) < len(batch):
                admission_tie_batches += 1
            for candidate in batch:
                reason, details, correlation_counts = self._admission_reason(
                    candidate,
                    stamp,
                    balance,
                    open_positions,
                )
                known_correlation_checks += correlation_counts[0]
                unknown_correlation_checks += correlation_counts[1]
                if reason:
                    blocked.append(self._block(candidate, stamp, reason, details))
                    continue
                gross_admission = details.get("gross_stop_risk_admission")
                if isinstance(gross_admission, Mapping):
                    max_admission_total_stop_risk_pct = max(
                        max_admission_total_stop_risk_pct,
                        _finite(gross_admission.get("prospective_total_stop_risk_pct")),
                    )
                    prospective_groups = gross_admission.get(
                        "prospective_candidate_group_stop_risk_pct"
                    )
                    if isinstance(prospective_groups, Mapping):
                        max_admission_factor_group_stop_risk_pct = max(
                            max_admission_factor_group_stop_risk_pct,
                            max(
                                (
                                    _finite(value)
                                    for value in prospective_groups.values()
                                ),
                                default=0.0,
                            ),
                        )
                open_risk, open_margin, unrealized = totals()
                mtm_equity = balance + unrealized
                stop_equity = balance - open_risk
                risk_capital = max(0.0, min(mtm_equity, stop_equity))
                target_risk = risk_capital * (
                    self.policy.target_risk_per_trade_pct / 100.0
                )
                raw_volume = target_risk / candidate.stop_risk_per_lot
                volume = floor_volume(
                    raw_volume, candidate.volume_step, candidate.volume_max
                )
                # _admission_reason already validates volume; keep this fail-closed invariant.
                if volume < candidate.volume_min or volume <= 0.0:
                    blocked.append(
                        self._block(candidate, stamp, "MIN_LOT_EXCEEDS_RISK_BUDGET")
                    )
                    continue
                actual_risk = candidate.stop_risk_per_lot * volume
                margin = candidate.margin_per_lot * volume
                open_positions[candidate.candidate_id] = _OpenPosition(
                    candidate=candidate,
                    volume=volume,
                    initial_risk=actual_risk,
                    initial_margin=margin,
                )
                accepted_ids.add(candidate.candidate_id)
                admission_events += 1

            # Same-bar exits are admitted and then settled at the same timestamp.
            for candidate_id, leg_index in leg_events.get(stamp, []):
                position = open_positions.get(candidate_id)
                if position is not None and position.candidate.entry_time == stamp:
                    close_leg(candidate_id, leg_index, stamp)

            open_risk, open_margin, unrealized = totals()
            equity = balance + unrealized
            stop_equity = balance - open_risk
            peak_balance = max(peak_balance, balance)
            peak_equity = max(peak_equity, equity)
            peak_stop_equity = max(peak_stop_equity, stop_equity)
            max_balance_dd = max(max_balance_dd, self._drawdown(peak_balance, balance))
            max_equity_dd = max(max_equity_dd, self._drawdown(peak_equity, equity))
            max_stop_equity_dd = max(
                max_stop_equity_dd, self._drawdown(peak_stop_equity, stop_equity)
            )
            open_risk_pct = open_risk / max(1e-12, equity) * 100.0
            conservative_open_risk_pct = (
                open_risk / max(1e-12, min(equity, stop_equity)) * 100.0
            )
            risk_capital = min(equity, stop_equity)
            if risk_capital <= 0.0:
                raise RuntimeError("portfolio replay has no positive risk capital")
            factor_risk: Dict[str, float] = defaultdict(float)
            symbol_risk: Dict[str, float] = defaultdict(float)
            for position in open_positions.values():
                symbol_risk[position.candidate.symbol] += position.open_risk
                for group in position.candidate.risk_groups:
                    factor_risk[group] += position.open_risk
            factor_stop_risk_pct = (
                max(factor_risk.values(), default=0.0) / risk_capital * 100.0
            )
            same_symbol_stop_risk_pct = (
                max(symbol_risk.values(), default=0.0) / risk_capital * 100.0
            )
            total_stop_risk_pct = open_risk / risk_capital * 100.0
            margin_utilization = open_margin / max(1e-12, equity) * 100.0
            max_open_risk_pct = max(max_open_risk_pct, open_risk_pct)
            max_conservative_open_risk_pct = max(
                max_conservative_open_risk_pct, conservative_open_risk_pct
            )
            max_margin_utilization_pct = max(
                max_margin_utilization_pct, margin_utilization
            )
            max_concurrent = max(max_concurrent, len(open_positions))
            path.append(
                {
                    "timestamp": stamp.isoformat(),
                    "balance": round(balance, 8),
                    "unrealized_pnl": round(unrealized, 8),
                    "mtm_equity": equity,
                    "stop_equity": round(stop_equity, 8),
                    "open_risk": round(open_risk, 8),
                    "open_risk_pct": round(open_risk_pct, 8),
                    "conservative_open_risk_pct": round(conservative_open_risk_pct, 8),
                    "risk_capital": round(risk_capital, 8),
                    "total_stop_risk_pct": round(total_stop_risk_pct, 8),
                    "factor_stop_risk_pct": round(factor_stop_risk_pct, 8),
                    "same_symbol_stop_risk_pct": round(same_symbol_stop_risk_pct, 8),
                    "margin_used": round(open_margin, 8),
                    "margin_utilization_pct": round(margin_utilization, 8),
                    "margin_level_pct": (
                        round(equity / open_margin * 100.0, 8)
                        if open_margin > 0.0
                        else None
                    ),
                    "free_margin": round(equity - open_margin, 8),
                    "open_positions": len(open_positions),
                    "admission_events": admission_events,
                    "realized_pnl_at_timestamp": round(
                        realized_at_time.get(stamp, 0.0), 8
                    ),
                }
            )

        if open_positions:
            raise RuntimeError(
                "portfolio replay ended with open positions; every candidate must have a final exit"
            )

        equity_growth = self._equity_growth(path)
        daily_returns = [
            math.expm1(value)
            for value in equity_growth["inference_daily_log_returns"]
        ]
        ruin = bootstrap_ruin_probability(
            daily_returns,
            starting_balance=self.policy.starting_balance,
            ruin_equity_pct=self.policy.ruin_equity_pct,
        )
        ruin["input_coverage_status"] = equity_growth["coverage_status"]
        if not daily_returns:
            ruin["unavailable_reason"] = equity_growth["coverage_reason"]
        wins = sum(1 for trade in closed_positions if trade["net_pnl"] > 0.0)
        gross_profit = sum(max(0.0, trade["net_pnl"]) for trade in closed_positions)
        gross_loss = -sum(min(0.0, trade["net_pnl"]) for trade in closed_positions)
        final_equity = path[-1]["mtm_equity"] if path else balance
        min_equity = min((row["mtm_equity"] for row in path), default=balance)
        min_stop_equity = min((row["stop_equity"] for row in path), default=balance)
        block_counts = Counter(item["reason"] for item in blocked)
        mtm_candidates = sum(1 for candidate in candidates if candidate.marks)

        summary = {
            "schema_version": PORTFOLIO_REPLAY_SCHEMA_VERSION,
            "capital_model": "one_shared_compounding_account",
            "portfolio_return_aggregation": "chronological_cashflows_not_summed_cell_returns",
            "valuation_tape_contract": MTM_MARK_COST_CONTRACT_VERSION,
            "valuation_event_order": "close_existing_then_mark_existing_then_admit",
            "valuation_timeframe": "M5",
            "valuation_price_field": "Open",
            "capital_path_contract": PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION,
            "candidate_roster_sha256": input_receipt["candidate_roster_sha256"],
            "stop_equity_contract": "legacy_immutable_initial_stop_risk_v1",
            "starting_balance": self.policy.starting_balance,
            "final_balance": round(balance, 8),
            "final_mtm_equity": round(float(final_equity), 8),
            "net_profit": round(balance - self.policy.starting_balance, 8),
            "return_pct": round(
                (balance / self.policy.starting_balance - 1.0) * 100.0, 8
            ),
            "candidate_positions": len(candidates),
            "accepted_positions": len(closed_positions),
            "blocked_positions": len(blocked),
            "blocked_by_reason": dict(sorted(block_counts.items())),
            "win_rate_pct": round(wins / len(closed_positions) * 100.0, 8)
            if closed_positions
            else 0.0,
            "profit_factor": (
                round(gross_profit / gross_loss, 8)
                if gross_loss > 0.0
                else ("inf" if gross_profit > 0.0 else 0.0)
            ),
            "gross_profit": round(gross_profit, 8),
            "gross_loss": round(gross_loss, 8),
            "max_realized_balance_drawdown_pct": round(max_balance_dd, 8),
            "max_mark_to_market_drawdown_pct": round(max_equity_dd, 8),
            "max_initial_stop_stress_drawdown_pct": round(max_stop_equity_dd, 8),
            "minimum_mtm_equity": round(float(min_equity), 8),
            "minimum_initial_stop_equity": round(float(min_stop_equity), 8),
            "max_open_risk_pct": round(max_open_risk_pct, 8),
            "max_conservative_open_risk_pct": round(max_conservative_open_risk_pct, 8),
            "max_admission_total_stop_risk_pct": round(
                max_admission_total_stop_risk_pct, 8
            ),
            "max_admission_factor_group_stop_risk_pct": round(
                max_admission_factor_group_stop_risk_pct, 8
            ),
            "max_margin_utilization_pct": round(max_margin_utilization_pct, 8),
            "max_concurrent_positions": max_concurrent,
            "total_price_pnl": round(
                sum(trade["price_pnl"] for trade in closed_positions), 8
            ),
            "total_swap": round(sum(trade["swap"] for trade in closed_positions), 8),
            "known_correlation_checks": known_correlation_checks,
            "unknown_correlation_checks": unknown_correlation_checks,
            "admission_tie_batches": admission_tie_batches,
            "mark_updates": mark_updates,
            "candidates_with_mtm_marks": mtm_candidates,
            "mtm_candidate_coverage_pct": round(
                mtm_candidates / len(candidates) * 100.0, 8
            )
            if candidates
            else 0.0,
            "observed_mtm_ruin": bool(
                min_equity
                <= self.policy.starting_balance * self.policy.ruin_equity_pct / 100.0
            ),
            "observed_initial_stop_ruin": bool(
                min_stop_equity
                <= self.policy.starting_balance * self.policy.ruin_equity_pct / 100.0
            ),
            "observed_ruin": bool(
                min(min_equity, min_stop_equity)
                <= self.policy.starting_balance * self.policy.ruin_equity_pct / 100.0
            ),
            "equity_growth": equity_growth,
            "exploratory_sequence_bootstrap": ruin,
            "limitations": [
                "candidate strategies and parameters remain selection-contaminated unless generated solely from an untouched lockbox",
                "offline margin is an approximation unless broker historical margin-per-lot is supplied",
                "mark-to-market uses the independently loaded native M5 Open observable at each bar-open timestamp; intrabar portfolio equity can be worse",
                "replay stop-equity remains the legacy immutable initial-stop model until the separate causal stop-state lane is implemented",
                "spread is embedded in executable price P&L and is not separately identifiable from the current trade schema",
                "bootstrap ruin is a conditional sequence stress test, not a forecast or profitability guarantee",
                "the event-bounded equity path has no authenticated evaluation window or complete temporal valuation coverage; full-day bootstrap input is unavailable",
            ],
        }
        return {
            "input_receipt": input_receipt,
            "summary": summary,
            "policy": asdict(self.policy),
            "closed_positions": closed_positions,
            "blocked_positions": blocked,
            "path": path,
        }

    def _admission_reason(
        self,
        candidate: CandidateTrade,
        stamp: pd.Timestamp,
        balance: float,
        open_positions: Mapping[str, _OpenPosition],
    ) -> Tuple[str, Dict[str, Any], Tuple[int, int]]:
        details: Dict[str, Any] = {}
        same_symbol_positions = [
            position
            for position in open_positions.values()
            if position.candidate.symbol == candidate.symbol
        ]
        if any(
            position.candidate.cell_id == candidate.cell_id
            for position in same_symbol_positions
        ):
            return "SPECIALIST_CELL_ALREADY_OPEN", details, (0, 0)
        if len(open_positions) >= self.policy.max_concurrent_positions:
            return "MAX_CONCURRENT_POSITIONS", details, (0, 0)

        open_risk = sum(position.open_risk for position in open_positions.values())
        open_margin = sum(position.open_margin for position in open_positions.values())
        unrealized = sum(
            position.unrealized_pnl for position in open_positions.values()
        )
        equity = balance + unrealized
        # Legacy replay candidates currently carry only immutable initial-stop
        # loss, not the signed path of governance stop updates.  Pass that
        # existing stop-equity model explicitly so the shared helper no longer
        # reconstructs it, without claiming live timing/state parity.
        legacy_portfolio_stop_equity = balance - open_risk
        risk_capital_decision = conservative_risk_capital(
            balance=balance,
            equity=equity,
            portfolio_stop_equity=legacy_portfolio_stop_equity,
            open_stop_risk_amount=open_risk,
        )
        risk_capital = risk_capital_decision.risk_capital
        if not risk_capital_decision.allow:
            return "NO_POSITIVE_RISK_CAPITAL", details, (0, 0)

        target_risk = risk_capital * self.policy.target_risk_per_trade_pct / 100.0
        raw_volume = target_risk / candidate.stop_risk_per_lot
        volume = floor_volume(raw_volume, candidate.volume_step, candidate.volume_max)
        details.update(
            {
                "raw_volume": raw_volume,
                "floored_volume": volume,
                "risk_capital": risk_capital,
            }
        )
        if volume <= 0.0 or volume < candidate.volume_min:
            return "MIN_LOT_EXCEEDS_RISK_BUDGET", details, (0, 0)
        actual_risk = candidate.stop_risk_per_lot * volume
        actual_risk_pct = actual_risk / risk_capital * 100.0
        details.update({"actual_risk": actual_risk, "actual_risk_pct": actual_risk_pct})
        if actual_risk_pct > self.policy.max_risk_per_trade_pct + 1e-10:
            return "PER_TRADE_RISK_CAP", details, (0, 0)

        shared_risk_decision = evaluate_gross_stop_risk(
            [
                StopRiskExposure(
                    symbol=position.candidate.symbol,
                    stop_risk_pct=(position.open_risk / risk_capital * 100.0),
                    asset_class=position.candidate.asset_class,
                    exposure_id=position.candidate.candidate_id,
                )
                for position in open_positions.values()
            ],
            StopRiskExposure(
                symbol=candidate.symbol,
                stop_risk_pct=actual_risk_pct,
                asset_class=candidate.asset_class,
                exposure_id=candidate.candidate_id,
            ),
            GrossRiskLimits(
                max_total_stop_risk_pct=self.policy.max_open_risk_pct,
                max_factor_group_stop_risk_pct=self.policy.max_factor_group_open_risk_pct,
            ),
        )
        details["gross_stop_risk_admission"] = asdict(shared_risk_decision)
        if not shared_risk_decision.allow:
            return shared_risk_decision.code, details, (0, 0)

        same_symbol_risk = sum(position.open_risk for position in same_symbol_positions)
        if (
            same_symbol_risk + actual_risk
        ) / risk_capital * 100.0 > self.policy.max_same_symbol_open_risk_pct + 1e-10:
            details["same_symbol_open_risk"] = same_symbol_risk
            return "SAME_SYMBOL_OPEN_RISK_CAP", details, (0, 0)

        known_checks = 0
        unknown_checks = 0
        if self.policy.enforce_pair_correlation:
            correlated_risk = 0.0
            for position in open_positions.values():
                raw_correlation = None
                if self.correlation_lookup is not None:
                    raw_correlation = self.correlation_lookup(
                        candidate.symbol,
                        position.candidate.symbol,
                        stamp,
                    )
                if raw_correlation is None or not math.isfinite(
                    _finite(raw_correlation, float("nan"))
                ):
                    unknown_checks += 1
                    if self.policy.block_unknown_correlation:
                        details["correlation_symbol"] = position.candidate.symbol
                        return (
                            "CORRELATION_UNAVAILABLE",
                            details,
                            (known_checks, unknown_checks),
                        )
                    continue
                known_checks += 1
                signed_correlation = (
                    float(raw_correlation)
                    * candidate.direction
                    * position.candidate.direction
                )
                if signed_correlation >= self.policy.correlation_threshold:
                    correlated_risk += position.open_risk
            if (
                (correlated_risk + actual_risk) / risk_capital * 100.0
                > self.policy.max_correlated_cluster_risk_pct + 1e-10
            ):
                details["correlated_open_risk"] = correlated_risk
                return (
                    "CORRELATED_CLUSTER_RISK_CAP",
                    details,
                    (known_checks, unknown_checks),
                )

        margin = candidate.margin_per_lot * volume
        margin_decision = evaluate_projected_margin(
            equity=equity,
            current_used_margin=open_margin,
            candidate_required_margin=margin,
            max_margin_utilization_pct=self.policy.max_margin_utilization_pct,
        )
        details["projected_margin_admission"] = asdict(margin_decision)
        if not margin_decision.allow:
            return margin_decision.code, details, (known_checks, unknown_checks)
        return "", details, (known_checks, unknown_checks)

    @staticmethod
    def _block(
        candidate: CandidateTrade,
        stamp: pd.Timestamp,
        reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "candidate_id": candidate.candidate_id,
            "cell_id": candidate.cell_id,
            "symbol": candidate.symbol,
            "timeframe": candidate.timeframe,
            "regime": candidate.regime,
            "strategy_name": candidate.strategy_name,
            "entry_time": _timestamp(stamp).isoformat(),
            "reason": reason,
            "details": details or {},
        }

    @staticmethod
    def _drawdown(peak: float, value: float) -> float:
        return max(0.0, (peak - value) / peak * 100.0) if peak > 0.0 else 100.0

    def _equity_growth(self, path: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        """Chain the observed path without claiming a complete evaluation window."""
        unavailable = {
            "evaluation_window_role": "EVENT_BOUNDED",
            "coverage_status": "EVENT_BOUNDED",
            "coverage_reason": "EVALUATION_WINDOW_AND_VALUATION_CONTEXT_UNPROVEN",
            "inference_daily_log_returns": [],
            "cumulative_log_growth": None,
            "time_weighted_return": None,
        }
        if len(path) < 2 or path[0]["timestamp"] == path[-1]["timestamp"]:
            return {**unavailable, "failure_code": "EVALUATION_WINDOW_NOT_POSITIVE"}
        # Replay's existing timestamp codec represents UTC without a zone tag.
        marks = [
            (_timestamp(row["timestamp"]).tz_localize("UTC"), row["mtm_equity"])
            for row in path
        ]
        try:
            reduction = reduce_equity_path(
                initial_equity=self.policy.starting_balance,
                marks=marks[:-1],
                evaluation_start=marks[0][0],
                evaluation_end=marks[-1][0],
                terminal_mark=marks[-1],
                complete_valuation_context=False,
            )
        except EquityReductionError as exc:
            if exc.code != "EQUITY_DEPLETED":
                raise
            # Depletion remains visible to the existing observed-ruin controls.
            return {**unavailable, "failure_code": exc.code, "coverage_reason": exc.code}
        return {
            **json.loads(json.dumps(asdict(reduction), allow_nan=False)),
            "evaluation_window_role": "EVENT_BOUNDED",
            "coverage_reason": unavailable["coverage_reason"],
        }


def bootstrap_ruin_probability(
    returns: Sequence[float],
    *,
    starting_balance: float,
    ruin_equity_pct: float = 50.0,
    simulations: int = 2_000,
    block_size: int = 5,
    seed: int = 1729,
) -> Dict[str, Any]:
    """Moving-block sequence stress; conditional diagnostic, never a forecast."""
    clean = np.asarray([_finite(value, float("nan")) for value in returns], dtype=float)
    invalid = ~np.isfinite(clean) | (clean < -1.0)
    if invalid.any():
        return {
            "status": "invalid_daily_returns",
            "daily_return_observations": int(clean.size),
            "invalid_daily_return_observations": int(invalid.sum()),
            "simulations": 0,
            "ruin_probability_pct": None,
            "ruin_probability_wilson_95pct": [None, None],
            "median_max_drawdown_pct": None,
            "p95_max_drawdown_pct": None,
        }
    if clean.size < 2 or simulations <= 0:
        return {
            "status": "insufficient_daily_returns",
            "daily_return_observations": int(clean.size),
            "simulations": 0,
            "ruin_probability_pct": None,
            "ruin_probability_wilson_95pct": [None, None],
            "median_max_drawdown_pct": None,
            "p95_max_drawdown_pct": None,
        }
    simulations = int(simulations)
    block_size = max(1, min(int(block_size), int(clean.size)))
    rng = np.random.default_rng(int(seed))
    ruin_threshold = float(starting_balance) * float(ruin_equity_pct) / 100.0
    ruin_count = 0
    max_drawdowns = np.empty(simulations, dtype=float)
    max_start = max(1, int(clean.size) - block_size + 1)
    for simulation in range(simulations):
        sampled: List[float] = []
        while len(sampled) < clean.size:
            start = int(rng.integers(0, max_start))
            sampled.extend(clean[start : start + block_size].tolist())
        equity = float(starting_balance)
        peak = equity
        max_drawdown = 0.0
        ruined = False
        for value in sampled[: clean.size]:
            equity *= max(0.0, 1.0 + float(value))
            peak = max(peak, equity)
            if peak > 0.0:
                max_drawdown = max(max_drawdown, (peak - equity) / peak * 100.0)
            if equity <= ruin_threshold:
                ruined = True
        ruin_count += int(ruined)
        max_drawdowns[simulation] = max_drawdown
    probability = ruin_count / simulations
    z = 1.959963984540054
    denominator = 1.0 + z * z / simulations
    centre = (probability + z * z / (2.0 * simulations)) / denominator
    half_width = (
        z
        * math.sqrt(
            probability * (1.0 - probability) / simulations
            + z * z / (4.0 * simulations * simulations)
        )
        / denominator
    )
    return {
        "status": "exploratory_conditional_sequence_stress",
        "daily_return_observations": int(clean.size),
        "simulations": simulations,
        "block_size_days": block_size,
        "seed": int(seed),
        "ruin_threshold_pct_of_start": float(ruin_equity_pct),
        "ruin_probability_pct": round(probability * 100.0, 6),
        "ruin_probability_wilson_95pct": [
            round(max(0.0, centre - half_width) * 100.0, 6),
            round(min(1.0, centre + half_width) * 100.0, 6),
        ],
        "median_max_drawdown_pct": round(float(np.quantile(max_drawdowns, 0.50)), 6),
        "p95_max_drawdown_pct": round(float(np.quantile(max_drawdowns, 0.95)), 6),
    }
