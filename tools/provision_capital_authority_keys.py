"""Explicit operator-only provisioning for capital-authority Ed25519 keys."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import stat
import subprocess
from pathlib import Path
from typing import Any, Sequence

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from pm_capital_authority_factory import canonical_ed25519_key_id


KEY_PROVISION_RECEIPT_SCHEMA = "fxpm.capital-authority-key-provision.v1"
_ROLES = frozenset({"precommit", "replay", "authorization"})
_ROLE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")


class KeyProvisioningError(ValueError):
    pass


def _fail(code: str) -> None:
    raise KeyProvisioningError(code)


def _is_reparse(path: Path) -> bool:
    info = path.lstat()
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(attributes & 0x400)


def _reject_reparse_chain(path: Path) -> None:
    absolute = Path(os.path.abspath(path))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        if current.exists() or current.is_symlink():
            if _is_reparse(current):
                _fail("KEY_DIRECTORY_REPARSE_POINT_FORBIDDEN")


def _current_windows_sid() -> str:
    try:
        result = subprocess.run(
            ["whoami", "/user", "/fo", "csv", "/nh"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        rows = list(csv.reader(result.stdout.splitlines()))
    except (OSError, subprocess.SubprocessError, csv.Error) as exc:
        raise KeyProvisioningError("WINDOWS_IDENTITY_LOOKUP_FAILED") from exc
    if result.returncode != 0 or len(rows) != 1 or len(rows[0]) < 2:
        _fail("WINDOWS_IDENTITY_LOOKUP_FAILED")
    sid = rows[0][1].strip()
    if not re.fullmatch(r"S-\d+(?:-\d+)+", sid):
        _fail("WINDOWS_IDENTITY_LOOKUP_FAILED")
    return sid


def _restrict(path: Path, *, directory: bool, windows_sid: str | None) -> None:
    os.chmod(path, 0o700 if directory else 0o600)
    if os.name != "nt":
        mode = stat.S_IMODE(path.stat().st_mode)
        expected = 0o700 if directory else 0o600
        if mode != expected:
            _fail("KEY_PERMISSION_RESTRICTION_FAILED")
        return
    grant = f"*{windows_sid}:{'(OI)(CI)F' if directory else 'F'}"
    try:
        result = subprocess.run(
            [
                "icacls",
                str(path),
                "/inheritance:r",
                "/grant:r",
                grant,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise KeyProvisioningError("KEY_PERMISSION_RESTRICTION_FAILED") from exc
    if result.returncode != 0:
        _fail("KEY_PERMISSION_RESTRICTION_FAILED")


def _write_exclusive(path: Path, payload: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def provision_key_pair(directory: Path, role: str) -> dict[str, Any]:
    """Create one role key in a new restricted directory without overwriting."""

    root = Path(directory)
    if not root.is_absolute():
        _fail("KEY_ROOT_NOT_ABSOLUTE")
    root = Path(os.path.abspath(root))
    _reject_reparse_chain(root)
    if not root.exists() or _is_reparse(root) or not root.is_dir():
        _fail("KEY_ROOT_NOT_SAFE_DIRECTORY")
    if type(role) is not str or not _ROLE.fullmatch(role) or role not in _ROLES:
        _fail("KEY_ROLE_INVALID")

    role_directory = root / role
    windows_sid = _current_windows_sid() if os.name == "nt" else None
    try:
        role_directory.mkdir(mode=0o700, exist_ok=False)
    except OSError as exc:
        raise KeyProvisioningError("KEY_ROLE_DIRECTORY_ALREADY_EXISTS") from exc
    created: list[Path] = []
    try:
        _restrict(role_directory, directory=True, windows_sid=windows_sid)
        private = Ed25519PrivateKey.generate()
        private_bytes = bytearray(
            private.private_bytes(
                serialization.Encoding.Raw,
                serialization.PrivateFormat.Raw,
                serialization.NoEncryption(),
            )
        )
        public_bytes = private.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        key_id = canonical_ed25519_key_id(public_bytes)
        private_path = role_directory / "private.ed25519"
        public_path = role_directory / "public.ed25519"
        key_id_path = role_directory / "key-id.txt"
        try:
            _write_exclusive(private_path, bytes(private_bytes))
        finally:
            private_bytes[:] = b"\0" * len(private_bytes)
        created.append(private_path)
        _restrict(private_path, directory=False, windows_sid=windows_sid)
        _write_exclusive(public_path, public_bytes)
        created.append(public_path)
        _restrict(public_path, directory=False, windows_sid=windows_sid)
        _write_exclusive(key_id_path, (key_id + "\n").encode("ascii"))
        created.append(key_id_path)
        _restrict(key_id_path, directory=False, windows_sid=windows_sid)
    except Exception:
        for path in reversed(created):
            try:
                path.unlink()
            except OSError:
                pass
        try:
            role_directory.rmdir()
        except OSError:
            pass
        raise
    return {
        "schema_version": KEY_PROVISION_RECEIPT_SCHEMA,
        "role": role,
        "key_id": key_id,
        "public_key_path": str(public_path),
        "key_id_path": str(key_id_path),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--role", choices=sorted(_ROLES), required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    receipt = provision_key_pair(args.directory, args.role)
    print(json.dumps(receipt, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
