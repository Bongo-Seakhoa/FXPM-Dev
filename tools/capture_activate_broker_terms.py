"""Request, ingest, and activate a direct MQL5 broker-terms export.

This command never launches or controls MetaTrader and never reads price
history from a broker connection.  An installed FXPMTermsExporter service must
observe the request in MetaTrader's Common Files directory and publish the
response.  Until that happens the command returns an honest WAITING state.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pm_broker_terms import (  # noqa: E402
    BrokerTermsError,
    EXPORT_COLUMNS,
    TERMS_EXPORT_SCHEMA,
    activate_broker_terms,
    build_price_receipt_migration_pin,
    build_explicit_stress_snapshot,
    canonical_sha256,
    parse_terms_export,
    prune_terms_store,
    write_capture_request,
)


REQUEST_NAME = "fxpm_terms_request.csv"
RESPONSE_NAME = "fxpm_terms_response.csv"
STATE_NAME = "fxpm_terms_capture_state.json"


def _atomic_state(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_migration_pin(path: Path, value: Mapping[str, Any]) -> str:
    """Create one exact ephemeral pin without replacing an existing pin."""

    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(encoded).hexdigest()


def _load_exact_migration_pin(
    path: Path,
    expected_sha256: str,
    max_bytes: int = 1024 * 1024,
) -> tuple[Dict[str, Any], bytes]:
    """Read one bounded regular pin and authenticate its exact bytes."""

    path = Path(path).absolute()
    if (
        not path.is_file()
        or path.is_symlink()
        or not 0 < path.stat().st_size <= max_bytes
    ):
        raise BrokerTermsError("price receipt migration pin is unsafe or oversized")
    payload = path.read_bytes()
    if (
        len(payload) > max_bytes
        or hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise BrokerTermsError("price receipt migration pin SHA-256 mismatch")
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BrokerTermsError("price receipt migration pin JSON is invalid") from exc
    if not isinstance(value, dict):
        raise BrokerTermsError("price receipt migration pin is not an object")
    return value, payload


def _load_json(path: Path, max_bytes: int = 4 * 1024 * 1024) -> Dict[str, Any]:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > max_bytes:
        raise BrokerTermsError(f"JSON input is missing, unsafe, or oversized: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BrokerTermsError(f"JSON input is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise BrokerTermsError(f"JSON input is not an object: {path}")
    return value


def load_roster(path: Path) -> Dict[str, str]:
    """Accept a direct map or the existing broker_specs.json object shape."""
    payload = _load_json(Path(path))
    roster: Dict[str, str] = {}
    for raw_symbol, raw_value in payload.items():
        canonical = str(raw_symbol or "").strip().upper()
        if isinstance(raw_value, str):
            broker = raw_value.strip()
        elif isinstance(raw_value, Mapping):
            broker = str(raw_value.get("broker_symbol") or raw_value.get("symbol") or canonical).strip()
        else:
            raise BrokerTermsError(f"roster entry for {canonical or raw_symbol!r} is invalid")
        if not canonical or not broker or canonical in roster:
            raise BrokerTermsError("roster contains an empty or duplicate symbol")
        roster[canonical] = broker
    if not roster or len(roster) > 1000:
        raise BrokerTermsError("roster size is outside 1..1000")
    return roster


def _print_status(status: str, **payload: Any) -> None:
    print(json.dumps({"status": status, **payload}, indent=2, sort_keys=True))


def _response_nonce(path: Path, max_bytes: int = 2 * 1024 * 1024) -> str:
    if not path.is_file() or path.is_symlink() or not 0 < path.stat().st_size <= max_bytes:
        raise BrokerTermsError("terms response is missing, unsafe, or oversized")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != EXPORT_COLUMNS:
            raise BrokerTermsError("terms response header is invalid")
        first = next(reader, None)
    if not first or first.get("schema_version") != TERMS_EXPORT_SCHEMA:
        raise BrokerTermsError("terms response contains no valid export row")
    return str(first.get("nonce") or "")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-root", type=Path, default=Path("data/native"))
    parser.add_argument(
        "--exchange-dir",
        type=Path,
        help="MetaTrader Common/Files directory watched by FXPMTermsExporter",
    )
    parser.add_argument("--roster-json", type=Path)
    parser.add_argument("--expected-server", default="")
    parser.add_argument("--expected-company", default="")
    parser.add_argument("--expected-currency", default="")
    parser.add_argument("--max-age-seconds", type=int, default=3600)
    parser.add_argument("--new-request", action="store_true")
    parser.add_argument("--approve-stress-policy", action="store_true",
                        help="Explicitly activate the versioned debit-only stress policy")
    parser.add_argument("--activation-id", default="")
    parser.add_argument("--no-pointer-update", action="store_true")
    parser.add_argument(
        "--prune-after-publication",
        action="store_true",
        help=(
            "Explicitly prune unreferenced terms artifacts after publishing the "
            "new pointer; disabled by default so the predecessor remains available "
            "through canary verification"
        ),
    )
    parser.add_argument("--max-snapshots", type=int, default=16)
    parser.add_argument("--max-terms-bytes", type=int, default=64 * 1024 * 1024)
    migration = parser.add_mutually_exclusive_group()
    migration.add_argument(
        "--prepare-price-receipt-migration-pin",
        type=Path,
        metavar="PATH",
        help=(
            "Before price ACTIVE migration, authenticate and pin the exact "
            "current TERMS snapshot"
        ),
    )
    migration.add_argument(
        "--complete-price-receipt-migration-pin",
        type=Path,
        metavar="PATH",
        help=(
            "After price ACTIVE migration, rebuild TERMS from the exact "
            "prepared pin and consume it on success"
        ),
    )
    parser.add_argument(
        "--price-receipt-migration-pin-sha256",
        default="",
        help="Required exact SHA-256 for --complete-price-receipt-migration-pin",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.max_age_seconds <= 0 or args.max_terms_bytes <= 0 or args.max_snapshots < 1:
        _print_status("INVALID", reason="positive freshness and storage bounds are required")
        return 2
    if args.no_pointer_update and args.prune_after_publication:
        _print_status(
            "INVALID",
            reason="staged activation cannot be combined with post-publication pruning",
        )
        return 2
    migration_prepare = args.prepare_price_receipt_migration_pin
    migration_complete = args.complete_price_receipt_migration_pin
    if migration_prepare is not None or migration_complete is not None:
        if (
            args.exchange_dir is not None
            or args.roster_json is not None
            or args.new_request
            or args.approve_stress_policy
            or args.no_pointer_update
            or args.prune_after_publication
        ):
            _print_status(
                "INVALID",
                reason=(
                    "price-receipt migration mode cannot be combined with "
                    "capture, stress, staging, or retention options"
                ),
            )
            return 2
        try:
            if migration_prepare is not None:
                if args.price_receipt_migration_pin_sha256:
                    raise BrokerTermsError(
                        "prepare mode does not accept an expected migration pin hash"
                    )
                pin = build_price_receipt_migration_pin(args.native_root)
                pin_sha = _atomic_migration_pin(migration_prepare, pin)
                _print_status(
                    "MIGRATION_PIN_PREPARED",
                    path=str(Path(migration_prepare).resolve()),
                    sha256=pin_sha,
                    action=(
                        "Publish the receipt-backed native price ACTIVE, then run "
                        "completion with this exact path and SHA-256."
                    ),
                )
                return 0

            expected_pin_sha = (
                str(args.price_receipt_migration_pin_sha256 or "").strip().lower()
            )
            if len(expected_pin_sha) != 64 or any(
                value not in "0123456789abcdef" for value in expected_pin_sha
            ):
                raise BrokerTermsError(
                    "completion requires a 64-character lowercase migration pin SHA-256"
                )
            pin_path = Path(migration_complete).absolute()
            pin, pin_bytes = _load_exact_migration_pin(
                pin_path,
                expected_pin_sha,
            )
            snapshot_relative = str(
                pin.get("source_terms_snapshot_relative_path") or ""
            )
            native_root = Path(args.native_root).resolve()
            snapshot_path = (native_root / snapshot_relative).resolve()
            try:
                snapshot_path.relative_to(native_root)
            except ValueError as exc:
                raise BrokerTermsError(
                    "price receipt migration snapshot path escapes native root"
                ) from exc
            snapshot = _load_json(snapshot_path, 64 * 1024 * 1024)
            activated = activate_broker_terms(
                native_root,
                snapshot,
                activation_id=args.activation_id or None,
                update_pointer=True,
                max_snapshot_age_seconds=args.max_age_seconds,
                price_receipt_migration_pin=pin,
            )
            consumption: Dict[str, Any]
            try:
                _current_pin, current_pin_bytes = _load_exact_migration_pin(
                    pin_path,
                    expected_pin_sha,
                )
                if current_pin_bytes != pin_bytes:
                    raise BrokerTermsError(
                        "migration pin changed after successful TERMS publication"
                    )
                pin_path.unlink()
                consumption = {"status": "CONSUMED", "path": str(pin_path)}
            except Exception as exc:
                consumption = {
                    "status": "WARNING",
                    "path": str(pin_path),
                    "reason": (
                        "TERMS publication succeeded but the single-use pin could "
                        "not be removed; exact-source checks prevent reuse"
                    ),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            _print_status(
                "COMPLETE",
                migration="PRICE_RECEIPT_REBIND",
                activation_id=activated["activation_id"],
                manifest_sha256=activated["manifest_sha256"],
                pointer_updated=activated["pointer_updated"],
                active_symbols=activated["active_symbols"],
                pin_consumption=consumption,
            )
            return 0
        except (BrokerTermsError, FileExistsError, OSError) as exc:
            _print_status("INVALID", reason=str(exc))
            return 2
    if args.exchange_dir is None or args.roster_json is None:
        _print_status(
            "INVALID",
            reason=(
                "--exchange-dir and --roster-json are required outside explicit "
                "price-receipt migration mode"
            ),
        )
        return 2
    try:
        roster = load_roster(args.roster_json)
        identity = {
            "broker_server": args.expected_server,
            "terminal_company": args.expected_company,
            "account_currency": args.expected_currency,
        }
        exchange = args.exchange_dir.resolve()
        exchange.mkdir(parents=True, exist_ok=True)
        request_path = exchange / REQUEST_NAME
        response_path = exchange / RESPONSE_NAME
        state_path = exchange / STATE_NAME
        roster_sha = canonical_sha256(roster)
        state: Dict[str, Any] = {}
        if state_path.exists() and not args.new_request:
            state = _load_json(state_path, 1024 * 1024)
        if args.new_request or state.get("roster_sha256") != roster_sha or not state.get("nonce"):
            request = write_capture_request(request_path, roster, expected_identity=identity)
            state = {
                "schema_version": "fxpm-terms-capture-state-v1",
                "phase": "WAITING_FOR_MQL5_EXPORT",
                "nonce": request["nonce"],
                "requested_at_utc": request["requested_at_utc"],
                "roster_sha256": roster_sha,
                "request_path": str(request_path),
                "response_path": str(response_path),
            }
            _atomic_state(state_path, state)
            _print_status(
                "WAITING_FOR_MQL5_EXPORT",
                request_path=str(request_path),
                response_path=str(response_path),
                nonce=state["nonce"],
                action=(
                    "Compile and attach mql5/FXPMTermsExporter.mq5 as a service/EA "
                    "using this Common Files directory, then rerun this command."
                ),
            )
            return 3
        if not response_path.exists():
            _print_status(
                "WAITING_FOR_MQL5_EXPORT",
                request_path=str(request_path),
                response_path=str(response_path),
                nonce=state["nonce"],
                action="Keep FXPMTermsExporter running and rerun after the response appears.",
            )
            return 3
        observed_nonce = _response_nonce(response_path)
        if observed_nonce != str(state["nonce"]):
            _print_status(
                "WAITING_FOR_MQL5_EXPORT",
                request_path=str(request_path),
                response_path=str(response_path),
                nonce=state["nonce"],
                observed_stale_nonce=observed_nonce,
                action="The response belongs to an older request; keep the exporter running.",
            )
            return 3

        snapshot = parse_terms_export(
            response_path,
            expected_nonce=str(state["nonce"]),
            expected_roster=roster,
            expected_identity=identity,
            requested_at_utc=state.get("requested_at_utc"),
            now_utc=datetime.now(timezone.utc),
            max_age_seconds=args.max_age_seconds,
        )
        if args.approve_stress_policy:
            snapshot = build_explicit_stress_snapshot(snapshot)
        activated = activate_broker_terms(
            args.native_root,
            snapshot,
            activation_id=args.activation_id or None,
            update_pointer=not args.no_pointer_update,
            max_snapshot_age_seconds=args.max_age_seconds,
        )
        if args.no_pointer_update:
            retention: Dict[str, Any] = {
                "status": "SKIPPED",
                "reason": "staged activation artifacts retained",
            }
        elif not args.prune_after_publication:
            retention = {
                "status": "SKIPPED",
                "reason": "predecessor retained pending explicit post-canary pruning",
            }
        else:
            try:
                retention = {
                    "status": "COMPLETE",
                    **prune_terms_store(
                        args.native_root,
                        max_snapshots=args.max_snapshots,
                        max_total_bytes=args.max_terms_bytes,
                    ),
                }
            except Exception as exc:  # Pointer publication has already succeeded.
                retention = {
                    "status": "WARNING",
                    "reason": (
                        "post-publication retention failed; the published pointer "
                        "remains authoritative"
                    ),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
        state.update({
            "phase": "COMPLETE",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "terms_activation_id": activated["activation_id"],
            "terms_activation_manifest_sha256": activated["manifest_sha256"],
            "export_schema": TERMS_EXPORT_SCHEMA,
        })
        try:
            _atomic_state(state_path, state)
            capture_state = {"status": "COMPLETE", "path": str(state_path)}
        except Exception as exc:  # Publication/staging has already completed.
            capture_state = {
                "status": "WARNING",
                "path": str(state_path),
                "reason": (
                    "capture-state persistence failed after terms activation; "
                    "the activation result remains authoritative"
                ),
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        _print_status(
            "COMPLETE",
            activation_id=activated["activation_id"],
            manifest_sha256=activated["manifest_sha256"],
            pointer_updated=activated["pointer_updated"],
            active_symbols=activated["active_symbols"],
            retention=retention,
            capture_state=capture_state,
        )
        return 0
    except BrokerTermsError as exc:
        _print_status("INVALID", reason=str(exc))
        return 2
    except FileExistsError as exc:
        _print_status("INVALID", reason=str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
