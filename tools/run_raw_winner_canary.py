#!/usr/bin/env python3
"""Bounded, non-authorizing one-cell raw-Winner canary CLI.

Composes only released authority builders end-to-end for exactly one
symbol/timeframe cell against the exact ACTIVE V5 production universe:

    resolver guard -> production anchor plan -> diagnostic generation context
    -> frozen search process -> causal precommit -> issuer-sealed canary
    request -> executor facade (source/fit/plan/capture/raw Winner/verify)
    -> sealed diagnostic receipt.

The run refuses to start unless the exact 100x6 production resolver is valid.
It writes only into a dedicated diagnostic spool root, never a production/CAS
root; every downstream authority flag in the request is False by contract, so
no Book/causal/current/capital/pointer/order state can be created.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import NoReturn, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CANARY_PREFIX = "raw-winner-canary-nonauthorizing"
CANARY_SPOOL_MAX_BYTES = 8 * 1024**3
CANARY_SPOOL_MIN_FREE_BYTES = 12 * 1024**3
NATIVE_AUTHORITY_VERIFICATION_SCHEMA = (
    "fxpm.production_native_authority_verification_scope.v1"
)
_NATIVE_AUTHORITY_VERIFICATION_FIELDS = frozenset(
    {
        "schema_version",
        "scope",
        "full_verification_count",
        "identity_revalidation_attempt_count",
        "identity_revalidation_count",
        "identity_revalidation_failure_count",
        "active_pointer_sha256",
        "activation_manifest_sha256",
        "pid",
        "thread_id",
        "poison_reason",
        "closed",
        "poisoned",
    }
)


def _fail(code: str, detail: str = "") -> NoReturn:
    payload = {
        "schema_version": "fxpm.raw-winner-canary-cli-failure.v1",
        "scope": "BOUNDED_CANARY_NONAUTHORIZING",
        "status": "FAILED_CLOSED",
        "failure_code": code,
        "detail": detail[:2000],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    raise SystemExit(2)


def _is_mechanical_success(body: object) -> bool:
    if not isinstance(body, dict):
        return False
    verification = body.get("native_authority_verification")
    if type(verification) is not dict or set(verification) != (
        _NATIVE_AUTHORITY_VERIFICATION_FIELDS
    ):
        return False
    activation_sha = body.get("activation_manifest_sha256")
    hashes = (
        verification.get("scope"),
        verification.get("active_pointer_sha256"),
        verification.get("activation_manifest_sha256"),
        activation_sha,
    )
    if any(
        type(value) is not str
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
        for value in hashes
    ):
        return False
    if (
        verification.get("schema_version") != NATIVE_AUTHORITY_VERIFICATION_SCHEMA
        or verification.get("activation_manifest_sha256") != activation_sha
        or type(verification.get("full_verification_count")) is not int
        or verification.get("full_verification_count") != 1
        or type(verification.get("identity_revalidation_attempt_count")) is not int
        or verification.get("identity_revalidation_attempt_count") != 5
        or type(verification.get("identity_revalidation_count")) is not int
        or verification.get("identity_revalidation_count") != 5
        or type(verification.get("identity_revalidation_failure_count")) is not int
        or verification.get("identity_revalidation_failure_count") != 0
        or type(verification.get("pid")) is not int
        or verification.get("pid") <= 0
        or type(verification.get("thread_id")) is not int
        or verification.get("thread_id") <= 0
        or verification.get("poison_reason") is not None
        or verification.get("closed") is not True
        or verification.get("poisoned") is not False
    ):
        return False
    return bool(
        body.get("status")
        in {
            "RAW_WINNER_VERIFIED",
            "RAW_WINNER_VERIFIED_ALL_PROCESS_REJECTED",
        }
        and body.get("raw_winner_completed") is True
        and body.get("standalone_winner_verifier_accepted") is True
        and body.get("failure_code") is None
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one non-authorizing raw-Winner canary cell."
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--symbol", default="EURUSD")
    parser.add_argument("--timeframe", default="D1")
    parser.add_argument(
        "--data-dir",
        default="",
        help="Optional exact assertion against the config-derived data root; "
        "the deployment config remains authoritative.",
    )
    parser.add_argument(
        "--diagnostic-root",
        default="",
        help="Dedicated spool root; defaults to a fresh timestamped directory "
        "under the host-local temporary directory (never a production root).",
    )
    args = parser.parse_args(argv)

    import pm_authority_spool as authority_spool
    import pm_authoritative_search_execution as search_execution
    import pm_book_multiplicity as book
    import pm_cap_calibration as cap
    import pm_causal_evidence_producer as causal_producer
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_production_adapter as historical_adapter
    import pm_native_activation as native_activation
    import pm_portfolio_replay as portfolio_replay
    import pm_production_anchor_plan as production_anchor_plan
    import pm_production_config_binding as config_binding
    import pm_production_search_planner as search_planner

    try:
        config_path = Path(args.config).resolve(strict=True)
        if not config_path.is_file():
            _fail("CANARY_CLI_CONFIG_MISSING", str(config_path))
        config_payload, supplied_config = config_binding.load_effective_pipeline_config(
            config_path
        )
        data_dir = Path(supplied_config.data_dir).resolve(strict=True)
        if not data_dir.is_dir():
            _fail("CANARY_CLI_DATA_DIR_INVALID", str(data_dir))
        if args.data_dir:
            asserted_data_dir = Path(args.data_dir).resolve(strict=True)
            if asserted_data_dir != data_dir:
                _fail(
                    "CANARY_CLI_DATA_DIR_CONFIG_MISMATCH",
                    f"asserted={asserted_data_dir}; configured={data_dir}",
                )
    except SystemExit:
        raise
    except Exception as exc:
        _fail("CANARY_CLI_CONFIG_INVALID", f"{type(exc).__name__}: {exc}")

    # The six full-grid authority consumers share one process-local scope.
    # Issuance performs calls five and six, then explicitly closes the scope
    # before sealing its counters into the request.  The outer context remains
    # the cleanup safety for every earlier exception and SystemExit.
    with native_activation.production_native_authority_verification_scope() as native_authority_scope:
        # 1) Refuse-to-run guard: exact 100x6 production resolver must be valid.
        try:
            universe = native_activation.resolve_active_native_universe(
                data_dir,
                list(book.PINNED_SYMBOL_ORDER),
                production_contract_required=True,
            )
        except Exception as exc:
            _fail(
                "CANARY_ACTIVE_UNIVERSE_RESOLVE_FAILED",
                f"{type(exc).__name__}: {exc}",
            )
        universe_errors = list(getattr(universe, "errors", []) or [])
        if universe_errors or not getattr(universe, "valid", False):
            _fail("CANARY_ACTIVE_UNIVERSE_INVALID", ";".join(map(str, universe_errors)))

        # 2) Exact active portfolio contract -> context native universe sha.
        contract, contract_errors = portfolio_replay.active_native_portfolio_contract(
            data_dir,
            book.PINNED_SYMBOL_ORDER,
            timeframes=book.TIMEFRAME_ORDER,
        )
        if contract_errors:
            _fail("CANARY_ACTIVE_CONTRACT_INVALID", ";".join(map(str, contract_errors)))
        native_universe_sha256 = portfolio_replay.canonical_json_sha256(contract)

        # 3) Genuine production anchor plan derived from ACTIVE V5 identity.
        try:
            anchor_receipt = production_anchor_plan.derive_production_anchor_plan(
                data_dir
            )
            anchor_body = production_anchor_plan.verify_production_anchor_plan_receipt(
                data_dir, anchor_receipt
            )
        except Exception as exc:
            _fail("CANARY_ANCHOR_PLAN_DERIVE_FAILED", f"{type(exc).__name__}: {exc}")

        # 4) Dedicated diagnostic context; run id == generation id with the
        #    non-authorizing prefix, never a production/CAS root.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_token = secrets.token_hex(8)
        generation_id = f"{CANARY_PREFIX}-{stamp}-{run_token}"
        diagnostic_base = (
            Path(tempfile.gettempdir()) / "fxpm-raw-winner-canary"
        ).resolve()
        try:
            diag_root = (
                Path(args.diagnostic_root).resolve()
                if args.diagnostic_root
                else diagnostic_base / f"{stamp}-{run_token}"
            )
            if diag_root == diagnostic_base:
                _fail("CANARY_DIAGNOSTIC_ROOT_NOT_UNIQUE", str(diag_root))
            try:
                diag_root.relative_to(diagnostic_base)
            except ValueError:
                _fail("CANARY_DIAGNOSTIC_ROOT_OUTSIDE_BOUNDED_BASE", str(diag_root))
            if data_dir in diag_root.parents or diag_root == data_dir:
                _fail("CANARY_DIAGNOSTIC_ROOT_INSIDE_DATA_DIR", str(diag_root))
            diag_root.mkdir(parents=True, exist_ok=False)
        except SystemExit:
            raise
        except Exception as exc:
            _fail(
                "CANARY_DIAGNOSTIC_ROOT_CREATE_FAILED",
                f"{type(exc).__name__}: {exc}",
            )

        config_sha256 = cap.canonical_json_sha256(config_payload)

        try:
            context = authority_spool.freeze_generation_context(
                diag_root / "spool",
                generation_id=generation_id,
                native_universe_sha256=native_universe_sha256,
                config_sha256=config_sha256,
                limits=authority_spool.SpoolLimits(
                    max_artifact_bytes=CANARY_SPOOL_MAX_BYTES,
                    max_symbol_bytes=CANARY_SPOOL_MAX_BYTES,
                    max_generation_bytes=CANARY_SPOOL_MAX_BYTES,
                    min_free_bytes=CANARY_SPOOL_MIN_FREE_BYTES,
                ),
            )
            process = search_planner.freeze_production_search_process(
                context, run_id=generation_id
            )
            causal_precommit = causal_producer.freeze_causal_process_precommit_v9(
                context=context,
                search_process=process,
                process_id=generation_id,
                anchors=anchor_body["anchor_plan"],
            )
        except Exception as exc:
            _fail("CANARY_PREREQUISITE_FREEZE_FAILED", f"{type(exc).__name__}: {exc}")

        # 5) Issuer performs the final two authority calls and seals counters.
        try:
            request = orchestration.issue_bounded_raw_winner_canary_request(
                context,
                data_dir=data_dir,
                anchor_plan_receipt=anchor_receipt,
                search_process=process,
                causal_precommit=causal_precommit,
                symbol=str(args.symbol).upper(),
                timeframe=str(args.timeframe).upper(),
                native_authority_verification_scope=native_authority_scope,
            )
        except Exception as exc:
            _fail("CANARY_REQUEST_ISSUANCE_FAILED", f"{type(exc).__name__}: {exc}")

    try:
        frozen_config = config_binding.freeze_production_pipeline_config(
            context,
            active_config_path=config_path,
            config_payload=config_payload,
            supplied_config=supplied_config,
        )
    except Exception as exc:
        _fail("CANARY_CONFIG_FREEZE_FAILED", f"{type(exc).__name__}: {exc}")

    # 6) The executor facade owns source/fit/plan/capture/raw-Winner/verify.
    try:
        receipt = search_execution.execute_bounded_raw_winner_canary(
            context=context,
            request=request,
            process=process,
            causal_precommit_payload=causal_precommit.to_dict(),
            frozen_pipeline_config=frozen_config,
        )
    except Exception as exc:
        _fail("CANARY_EXECUTION_FAILED", f"{type(exc).__name__}: {exc}")

    try:
        body = historical_adapter.verify_bounded_raw_winner_canary_receipt(
            context,
            request=request,
            receipt=receipt,
        )
    except Exception as exc:
        _fail("CANARY_RECEIPT_REVERIFY_FAILED", f"{type(exc).__name__}: {exc}")
    print(json.dumps(body, indent=2, sort_keys=True))
    return 0 if _is_mechanical_success(body) else 2


if __name__ == "__main__":
    raise SystemExit(main())
