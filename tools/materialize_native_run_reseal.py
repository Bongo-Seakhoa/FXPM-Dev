#!/usr/bin/env python3
"""Emit or verify a detached externally signed native-run reseal.

This utility never signs and never reads secret signing material. ``emit`` writes an
unsigned request to stdout, including the exact base64 payload an external
Ed25519 authority must sign.  ``verify`` accepts the returned envelope plus an
out-of-band public trust root, replays every source binding, and emits a
technical-lineage receipt.  Neither command writes into the native run.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pm_native_run_reseal import (  # noqa: E402
    NativeRunResealError,
    build_run_reseal_signing_request,
    load_bounded_json_file,
    run_reseal_expected_authority_from_mapping,
    run_reseal_trust_root_from_mapping,
    verify_run_reseal_envelope,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detached verify-only externally pinned V5 native-run reseal"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    emit = subparsers.add_parser(
        "emit", help="materialize an unsigned external signing request"
    )
    emit.add_argument("--native-root", required=True, type=Path)
    emit.add_argument(
        "--expected-authority",
        required=True,
        type=Path,
        help="out-of-band JSON pinning policy, source, cutoff, run and manifest hash",
    )
    emit.add_argument("--authority-key-id", required=True)
    emit.add_argument(
        "--issued-at-utc",
        required=True,
        help="explicit UTC issue timestamp; fractional seconds are forbidden",
    )

    verify = subparsers.add_parser(
        "verify", help="verify an external envelope and replay all source pins"
    )
    verify.add_argument("--native-root", required=True, type=Path)
    verify.add_argument(
        "--expected-authority",
        required=True,
        type=Path,
        help="the same out-of-band expected-authority JSON used for emit",
    )
    verify.add_argument("--envelope", required=True, type=Path)
    verify.add_argument("--trust-root", required=True, type=Path)
    verify.add_argument(
        "--validation-time-utc",
        required=True,
        help="trusted explicit UTC verification time; fractional seconds are forbidden",
    )
    return parser


def _emit_json(value: Any, *, stream: Any) -> None:
    json.dump(
        value,
        stream,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    stream.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        authority_mapping = load_bounded_json_file(
            args.expected_authority,
            code="RUN_RESEAL_EXPECTED_AUTHORITY_JSON_INVALID",
        )
        expected_authority = run_reseal_expected_authority_from_mapping(
            authority_mapping
        )
        if args.command == "emit":
            output = build_run_reseal_signing_request(
                args.native_root,
                expected_authority=expected_authority,
                authority_key_id=args.authority_key_id,
                issued_at_utc=args.issued_at_utc,
            )
        else:
            envelope = load_bounded_json_file(
                args.envelope,
                code="RUN_RESEAL_ENVELOPE_JSON_INVALID",
            )
            trust_root_mapping = load_bounded_json_file(
                args.trust_root,
                code="RUN_RESEAL_TRUST_ROOT_JSON_INVALID",
            )
            trust_root = run_reseal_trust_root_from_mapping(trust_root_mapping)
            output = verify_run_reseal_envelope(
                args.native_root,
                envelope,
                trust_root=trust_root,
                expected_authority=expected_authority,
                validation_time_utc=args.validation_time_utc,
            ).to_receipt()
    except NativeRunResealError as exc:
        _emit_json(
            {"ok": False, "error": exc.code},
            stream=sys.stderr,
        )
        return 2
    _emit_json(output, stream=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
