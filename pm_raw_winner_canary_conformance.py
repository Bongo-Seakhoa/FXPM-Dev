"""Non-authorizing conformance replay for one preserved raw-Winner canary root.

This module deliberately does less than the raw-Winner canary.  It never calls
the strategy evaluator or raw Winner, and it cannot issue or verify any
downstream authority.  It reopens the exact failed EURUSD/D1 diagnostic root,
reconstructs its frozen old-release Horizon plan, verifies the 128 current-
refit bundles, and proves that the reviewed semantic mapping accepts those
same bundles only under a newly identified release.

The resulting receipt is a diagnostic preservation/conformance artifact.  It
does not replace the bounded-canary receipt verifier and does not close the
named raw-Winner gate.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import stat
import tempfile
from pathlib import Path
from typing import Any, Final, Mapping, NoReturn, Sequence

import pandas as pd

import pm_authoritative_search_execution as search_execution
import pm_authority_spool as authority_spool
import pm_current_refit_artifacts as current_refit
import pm_historical_anchor_orchestration as historical
import pm_native_prefix_production as native_prefix
import pm_production_config_binding as config_binding
import pm_production_search_planner as search_planner
import pm_training_horizon as training_horizon


CONFORMANCE_SCHEMA: Final = "fxpm.raw-winner-canary-preserved-root-conformance.v1"
CONFORMANCE_SCOPE: Final = "PRESERVED_RAW_WINNER_CANARY_DIAGNOSTIC_CONFORMANCE_ONLY"
CONFORMANCE_RECEIPT_NAME: Final = "raw-winner-canary-conformance-receipt.json"
PRESERVED_ROOT_ID: Final = "20260723T082546Z-8e64815fd7cf0b7d"

EXPECTED_INVENTORY_SHA256: Final = (
    "a0ec21a7eaa8f492cfc73bd064aecde07953f8cd8fb2b7ef589ab57d1f5e979e"
)
EXPECTED_INVENTORY_FILE_COUNT: Final = 14
EXPECTED_INVENTORY_TOTAL_BYTES: Final = 949_589

GENERATION_ID: Final = (
    "raw-winner-canary-nonauthorizing-20260723T082546Z-8e64815fd7cf0b7d"
)
NATIVE_UNIVERSE_SHA256: Final = (
    "5b82a7282e2a65b9edad18457efab25047ef54aa182ef162e86a7d1d30d64038"
)
CONFIG_SHA256: Final = (
    "3fceb47e72d3418927545acc45d3e8fd1dcfd0aa7dbb7badc58b8d5aeba4e879"
)
CONFIG_FILE_SHA256: Final = (
    "a4871a3b7feb86b58be9eb3fa451a7a9607ba6fc86dfb4cd87094ddc0dda8de0"
)
EFFECTIVE_CONFIG_SHA256: Final = (
    "4e4398dd842633193045b76dbf40a6a0c7d5ba24ea7918b7b01d27238fcef3b7"
)
GENERATION_CONTEXT_SHA256: Final = (
    "fa9d18f75a9c29808a7bf2f3c9ebf809d05c9096e3465f50a0c42c74285cfc04"
)

ORIGINAL_CANARY_RECEIPT_SHA256: Final = (
    "03f9cee3521eb6b7f6bda8661357ff0c83f50d6fff24506cbb7597310f05d60b"
)
ORIGINAL_FAILURE_SHA256: Final = (
    "f01ffd0e9e6cbd10d390b95d16cc30ca16209ef9a516d601ea965fe754dc1e1a"
)
CELL_PLAN_OBJECT_SHA256: Final = (
    "5c810b4fba0e79ca1e65cfb82ea7940357191d8d945a6849022865b2f3c6f353"
)
CELL_PLAN_RECEIPT_SHA256: Final = (
    "943ba787d699f7980449323c97b3a638149d0b952a1a9bd8798c43f7be9c933d"
)
SOURCE_SESSION_SHA256: Final = (
    "43c875967582d168b9c6c7fc1c6c1466797e9affe815367fa0b54b6fffec4a60"
)
PROCESS_SHA256: Final = (
    "c1b020b6c1575131bfa58f1d03083ecfa9e8639ec2707e00cf3215e8ac1630a9"
)
PREFIX_INDEX_SHA256: Final = (
    "aa30253847b32e205b95f8e0a2a6e855bdb1705b6a3290ca43d7bc10dc780f61"
)
PREFIX_CONTENT_SHA256: Final = (
    "47dc64bb3aa89229c9737a0f73141bc8483ca0d5a1133b7dacbc87e7d5ee2b97"
)
OLD_HORIZON_PLAN_SHA256: Final = (
    "65ed8a7dd5942158c9f30f94f0f181fc7c8df3e9cedc49f998548ba93b65fb89"
)
CAUSAL_PRECOMMIT_SHA256: Final = (
    "4951a7e034d76b850cf6926a8b95a93dcffbc1d4643419050a2b634d86ce79c2"
)

OLD_PLANNER_RELEASE_SHA256: Final = (
    "0b7bad87afaf93804717a2a49fbaafa32dabe1e08d3a4179b7fcf5e2dac54807"
)
OLD_EXECUTOR_RELEASE_SHA256: Final = (
    "ee200073ce92228651a8448d4f846a6fecf45e263570b724bb437f201d2df059"
)
OLD_HISTORICAL_ADAPTER_RELEASE_SHA256: Final = (
    "95281e8d37cb1a48d3407514cc5af75292057772698a41ff62982d51d890d60d"
)

REVIEWED_MAPPING_ARTIFACT: Final = (
    "diff:74e2452d68361a03e2333a74d92f4e7902fdd7e9257d2aa658446cff9f6839e1"
    "@base:e6e8be404277f493619bf5cb50b3350912b11c77"
)
REVIEWED_EXECUTOR_FILE_SHA256: Final = (
    "393e5b0a71431db9822db3137924ab25b087da982a52ae7ec33aaf8b32872000"
)
REVIEWED_PLANNER_FILE_SHA256: Final = (
    "0728d4268437d69c890a67558c8a9f7a738d7f6bf9aa77ca7ca763729bb1efd1"
)
REVIEWED_PLANNER_RELEASE_SHA256: Final = (
    "c4025683f5c287a27b1d43839a251bc4de3fc00e9ba6d09def0c2fe64030d1ed"
)
REVIEWED_EXECUTOR_RELEASE_SHA256: Final = (
    "f153b0218556c590bf258a53fb5456270d6860f2954f88a7c63018468f0b7217"
)

ACTIVATION_ID: Final = "20260719T101758Z-e050a26f"
ACTIVATION_MANIFEST_SHA256: Final = (
    "4763fd9f9b73737e64707544b1a847033cc0c0d857f163fef53ff4c68b8e52da"
)
ACTIVE_POINTER_SHA256: Final = (
    "7e4aedf85819b5ee48540bbc010e7831c4b5a029d400902123d607b36285216d"
)

EXPECTED_FRAME_SHA256: Final = (
    "f2692e4db1ff61049035c310fc4b7738f519d81d3191e4aaa0a66adb5678369f"
)
EXPECTED_SOURCE_DECISION_SHA256: Final = (
    "9eabaa70f96c9ee396ca8b50989b12375dff37bc28204b7f8c501bb5a405aae3"
)
EXPECTED_REGIME_FIT_SHA256: Final = (
    "ca7bf1bdce53d44856e0100c7e38abbc73fc6d5bf64c6b7599a4eac5bc49daf2"
)
EXPECTED_REGIME_SUPPORT_SHA256: Final = (
    "5b6703d081d70e570b0e973bbc3342fc9d1101df2752bfce3ecd44eacb788a53"
)

_MAX_ROOT_FILE_BYTES: Final = 2 * 1024 * 1024
_MAX_EXTERNAL_FILE_BYTES: Final = 16 * 1024 * 1024
_MAX_RECEIPT_BYTES: Final = 8 * 1024 * 1024
_SHA_CHARS: Final = frozenset("0123456789abcdef")
_DESCRIPTOR_FIELDS: Final = frozenset(
    {"schema_version", "format", "relative_path", "sha256", "size_bytes"}
)

_EXPECTED_INVENTORY: Final = (
    (
        "spool/generation-context.json",
        520,
        "fa9d18f75a9c29808a7bf2f3c9ebf809d05c9096e3465f50a0c42c74285cfc04",
    ),
    (
        "spool/objects/sha256/03/"
        "03f9cee3521eb6b7f6bda8661357ff0c83f50d6fff24506cbb7597310f05d60b"
        ".canonical-json",
        4615,
        ORIGINAL_CANARY_RECEIPT_SHA256,
    ),
    (
        "spool/objects/sha256/08/"
        "083b2dd97ff74d5e8430d7596f14f8e248dd7254e479e9bed2fade1fc7ac861f"
        ".jsonl",
        153677,
        "083b2dd97ff74d5e8430d7596f14f8e248dd7254e479e9bed2fade1fc7ac861f",
    ),
    (
        "spool/objects/sha256/39/"
        "39aeb9abcb0e91b44924f80d0f5e6a6870272e36610de7d605be7673e0b83937"
        ".canonical-json",
        981,
        "39aeb9abcb0e91b44924f80d0f5e6a6870272e36610de7d605be7673e0b83937",
    ),
    (
        "spool/objects/sha256/43/"
        "43c875967582d168b9c6c7fc1c6c1466797e9affe815367fa0b54b6fffec4a60"
        ".canonical-json",
        1256,
        SOURCE_SESSION_SHA256,
    ),
    (
        "spool/objects/sha256/47/"
        "47dc64bb3aa89229c9737a0f73141bc8483ca0d5a1133b7dacbc87e7d5ee2b97"
        ".csv",
        633797,
        PREFIX_CONTENT_SHA256,
    ),
    (
        "spool/objects/sha256/5c/"
        "5c810b4fba0e79ca1e65cfb82ea7940357191d8d945a6849022865b2f3c6f353"
        ".canonical-json",
        21126,
        CELL_PLAN_OBJECT_SHA256,
    ),
    (
        "spool/objects/sha256/90/"
        "90ef140b947dd80c7e4bb5d8c2059e195c5216c43ec934217f98c7ccb78bbd82"
        ".canonical-json",
        893,
        "90ef140b947dd80c7e4bb5d8c2059e195c5216c43ec934217f98c7ccb78bbd82",
    ),
    (
        "spool/objects/sha256/aa/"
        "aa30253847b32e205b95f8e0a2a6e855bdb1705b6a3290ca43d7bc10dc780f61"
        ".canonical-json",
        4852,
        PREFIX_INDEX_SHA256,
    ),
    (
        "spool/objects/sha256/c1/"
        "c1b020b6c1575131bfa58f1d03083ecfa9e8639ec2707e00cf3215e8ac1630a9"
        ".canonical-json",
        122581,
        PROCESS_SHA256,
    ),
    (
        "spool/objects/sha256/d4/"
        "d4d4113ae69738ce4c83be7c851282bc996151af0717b05a29c7a671f7e6a7f1"
        ".canonical-json",
        766,
        "d4d4113ae69738ce4c83be7c851282bc996151af0717b05a29c7a671f7e6a7f1",
    ),
    (
        "spool/objects/sha256/f0/"
        "f01ffd0e9e6cbd10d390b95d16cc30ca16209ef9a516d601ea965fe754dc1e1a"
        ".canonical-json",
        906,
        ORIGINAL_FAILURE_SHA256,
    ),
    (
        "spool/symbols/1429febab32745484aa146393fd3042445b2436bf588d9231d00e79b"
        "dbfc10aa/domains/b15f584799e6d595cd91f88524a58fb6d0b4ea073d79cc381048d1"
        "f3ba660225/manifest.json",
        2556,
        "6abac084c9b802e5bfb1407abfb56cb9a5e4a6deaebe4c0f19db01a32885ee36",
    ),
    (
        "spool/symbols/c895dec8aeae3c19846d6e73ee2f39289e6736818ade8574fde353f9ec8b"
        "cb74/domains/b15f584799e6d595cd91f88524a58fb6d0b4ea073d79cc381048d1"
        "f3ba660225/manifest.json",
        1063,
        "ef0c9b67520f1ed39416e1f0f127614f1b7b62fdd6024327d136616a78304ea5",
    ),
)

_OBJECT_SCHEMA_BY_SHA: Final = {
    ORIGINAL_CANARY_RECEIPT_SHA256: "fxpm.bounded-raw-winner-canary-receipt.v1",
    "39aeb9abcb0e91b44924f80d0f5e6a6870272e36610de7d605be7673e0b83937": (
        "fxpm.activated-native-prefix-continuity-policy.v1"
    ),
    SOURCE_SESSION_SHA256: "fxpm.bounded-raw-winner-canary-source-session.v1",
    CELL_PLAN_OBJECT_SHA256: "fxpm.production-search-cell-plan-receipt.v1",
    "90ef140b947dd80c7e4bb5d8c2059e195c5216c43ec934217f98c7ccb78bbd82": (
        "fxpm.activated-native-prefix-continuity-receipt.v1"
    ),
    PREFIX_INDEX_SHA256: "fxpm.activated-native-prefix-index.v1",
    PROCESS_SHA256: "fxpm.production-search-process-precommit.v1",
    "d4d4113ae69738ce4c83be7c851282bc996151af0717b05a29c7a671f7e6a7f1": (
        "fxpm.activated-native-cutoff-prefix.v1"
    ),
    ORIGINAL_FAILURE_SHA256: "fxpm.bounded-raw-winner-canary-failure.v1",
}


class RawWinnerCanaryConformanceError(RuntimeError):
    """Typed, fail-closed preserved-root conformance error."""

    def __init__(self, code: str, details: Sequence[str] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> NoReturn:
    raise RawWinnerCanaryConformanceError(code, tuple(str(value) for value in details))


def _canonical_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise RawWinnerCanaryConformanceError(
            "CANONICAL_JSON_INVALID", (type(exc).__name__, str(exc))
        ) from exc
    if not encoded or len(encoded) > _MAX_RECEIPT_BYTES:
        _fail("CANONICAL_JSON_SIZE_INVALID", len(encoded))
    return encoded


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _strict_json(raw: bytes, *, canonical: bool, code: str) -> Any:
    if type(raw) is not bytes or not raw:
        _fail(code, "EMPTY")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if type(key) is not str or key in result:
                _fail(code, "DUPLICATE_OR_NONSTRING_KEY", key)
            result[key] = value
        return result

    def reject_constant(value: str) -> NoReturn:
        _fail(code, "NONFINITE_NUMBER", value)

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except RawWinnerCanaryConformanceError:
        raise
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise RawWinnerCanaryConformanceError(
            code, (type(exc).__name__, str(exc))
        ) from exc
    if canonical and _canonical_bytes(value) != raw:
        _fail(code, "NONCANONICAL")
    return value


def _is_reparse(point: os.stat_result) -> bool:
    attribute = getattr(point, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attribute & reparse_flag)


def _check_regular(path: Path, *, maximum_bytes: int, code: str) -> os.stat_result:
    try:
        before = path.lstat()
    except OSError as exc:
        raise RawWinnerCanaryConformanceError(
            code, (str(path), type(exc).__name__, str(exc))
        ) from exc
    if (
        path.is_symlink()
        or _is_reparse(before)
        or not stat.S_ISREG(before.st_mode)
        or not 0 < before.st_size <= maximum_bytes
    ):
        _fail(code, str(path), "UNSAFE_OR_OVERSIZED", before.st_size)
    return before


def _read_regular(path: Path, *, maximum_bytes: int, code: str) -> bytes:
    before = _check_regular(path, maximum_bytes=maximum_bytes, code=code)
    try:
        with path.open("rb") as handle:
            raw = handle.read(maximum_bytes + 1)
        after = path.lstat()
    except OSError as exc:
        raise RawWinnerCanaryConformanceError(
            code, (str(path), type(exc).__name__, str(exc))
        ) from exc
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        len(raw) != before.st_size
        or len(raw) > maximum_bytes
        or any(
            getattr(before, name, None) != getattr(after, name, None)
            for name in stable_fields
        )
        or _is_reparse(after)
        or not stat.S_ISREG(after.st_mode)
    ):
        _fail(code, str(path), "CHANGED_WHILE_READING")
    return raw


def _resolve_directory(path: Path, code: str) -> Path:
    try:
        resolved = Path(path).resolve(strict=True)
        info = resolved.lstat()
    except (OSError, RuntimeError) as exc:
        raise RawWinnerCanaryConformanceError(
            code, (str(path), type(exc).__name__, str(exc))
        ) from exc
    if resolved.is_symlink() or _is_reparse(info) or not stat.S_ISDIR(info.st_mode):
        _fail(code, str(resolved), "UNSAFE_DIRECTORY")
    return resolved


def _inventory_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def visit(directory: Path) -> None:
        try:
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise RawWinnerCanaryConformanceError(
                "ROOT_INVENTORY_SCAN_FAILED",
                (str(directory), type(exc).__name__, str(exc)),
            ) from exc
        for entry in entries:
            path = Path(entry.path)
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise RawWinnerCanaryConformanceError(
                    "ROOT_INVENTORY_SCAN_FAILED",
                    (str(path), type(exc).__name__, str(exc)),
                ) from exc
            if entry.is_symlink() or _is_reparse(info):
                _fail("ROOT_INVENTORY_UNSAFE_ENTRY", str(path))
            if stat.S_ISDIR(info.st_mode):
                visit(path)
                continue
            if not stat.S_ISREG(info.st_mode):
                _fail("ROOT_INVENTORY_NONREGULAR_ENTRY", str(path))
            raw = _read_regular(
                path,
                maximum_bytes=_MAX_ROOT_FILE_BYTES,
                code="ROOT_INVENTORY_FILE_READ_FAILED",
            )
            try:
                relative = path.relative_to(root).as_posix()
            except ValueError as exc:
                raise RawWinnerCanaryConformanceError(
                    "ROOT_INVENTORY_PATH_ESCAPE", (str(path),)
                ) from exc
            rows.append(
                {
                    "relative_path": relative,
                    "size_bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            )

    visit(root)
    rows.sort(key=lambda value: value["relative_path"])
    return rows


def _expected_inventory_rows() -> list[dict[str, Any]]:
    rows = [
        {
            "relative_path": relative,
            "size_bytes": size,
            "sha256": digest,
        }
        for relative, size, digest in _EXPECTED_INVENTORY
    ]
    if (
        len(rows) != EXPECTED_INVENTORY_FILE_COUNT
        or sum(item["size_bytes"] for item in rows) != EXPECTED_INVENTORY_TOTAL_BYTES
        or _sha(rows) != EXPECTED_INVENTORY_SHA256
    ):
        _fail("INTERNAL_EXPECTED_INVENTORY_INVALID")
    return rows


def inventory_preserved_root(root: Path) -> dict[str, Any]:
    """Hash the exact regular-file inventory of the preserved root."""

    safe_root = _resolve_directory(Path(root), "PRESERVED_ROOT_INVALID")
    rows = _inventory_rows(safe_root)
    return {
        "algorithm": ("SORTED_REGULAR_FILE_ROWS_CANONICAL_JSON_SHA256_V1"),
        "file_count": len(rows),
        "total_bytes": sum(item["size_bytes"] for item in rows),
        "inventory_sha256": _sha(rows),
        "rows": rows,
    }


def _require_expected_inventory(inventory: Mapping[str, Any], stage: str) -> None:
    expected_rows = _expected_inventory_rows()
    if (
        inventory.get("file_count") != EXPECTED_INVENTORY_FILE_COUNT
        or inventory.get("total_bytes") != EXPECTED_INVENTORY_TOTAL_BYTES
        or inventory.get("inventory_sha256") != EXPECTED_INVENTORY_SHA256
        or inventory.get("rows") != expected_rows
    ):
        _fail(
            "PRESERVED_ROOT_INVENTORY_MISMATCH",
            stage,
            inventory.get("file_count"),
            inventory.get("total_bytes"),
            inventory.get("inventory_sha256"),
        )


def _row_by_path() -> dict[str, dict[str, Any]]:
    return {item["relative_path"]: item for item in _expected_inventory_rows()}


def _object_relative_path(digest: str, suffix: str = "canonical-json") -> str:
    return f"spool/objects/sha256/{digest[:2]}/{digest}.{suffix}"


def _read_root_file(root: Path, relative: str) -> bytes:
    row = _row_by_path().get(relative)
    if row is None:
        _fail("PRESERVED_ROOT_FILE_NOT_FROZEN", relative)
    path = root / Path(relative)
    raw = _read_regular(
        path,
        maximum_bytes=_MAX_ROOT_FILE_BYTES,
        code="PRESERVED_ROOT_FILE_READ_FAILED",
    )
    if (
        len(raw) != row["size_bytes"]
        or hashlib.sha256(raw).hexdigest() != row["sha256"]
    ):
        _fail("PRESERVED_ROOT_FILE_CHANGED", relative)
    return raw


def _read_root_json(root: Path, relative: str) -> dict[str, Any]:
    raw = _read_root_file(root, relative)
    value = _strict_json(raw, canonical=True, code="PRESERVED_ROOT_JSON_INVALID")
    if type(value) is not dict:
        _fail("PRESERVED_ROOT_JSON_OBJECT_REQUIRED", relative)
    return value


def _read_object(root: Path, digest: str) -> dict[str, Any]:
    value = _read_root_json(root, _object_relative_path(digest))
    expected_schema = _OBJECT_SCHEMA_BY_SHA.get(digest)
    if expected_schema is not None and value.get("schema_version") != expected_schema:
        _fail("PRESERVED_OBJECT_SCHEMA_MISMATCH", digest)
    return value


def _verify_jsonl(root: Path) -> None:
    digest = "083b2dd97ff74d5e8430d7596f14f8e248dd7254e479e9bed2fade1fc7ac861f"
    raw = _read_root_file(root, _object_relative_path(digest, "jsonl"))
    lines = raw.splitlines(keepends=True)
    if (
        len(lines) != 795
        or not lines
        or any(not line.endswith(b"\n") for line in lines)
    ):
        _fail("PRESERVED_SEGMENT_LEDGER_SHAPE_INVALID", len(lines))
    for ordinal, line in enumerate(lines):
        value = _strict_json(
            line[:-1],
            canonical=True,
            code="PRESERVED_SEGMENT_LEDGER_JSON_INVALID",
        )
        if type(value) is not dict:
            _fail("PRESERVED_SEGMENT_LEDGER_ROW_INVALID", ordinal)


def _verify_manifests(root: Path) -> list[dict[str, Any]]:
    manifest_rows = [
        row
        for row in _expected_inventory_rows()
        if row["relative_path"].endswith("/manifest.json")
    ]
    summaries: list[dict[str, Any]] = []
    for row in manifest_rows:
        body = _read_root_json(root, row["relative_path"])
        artifacts = body.get("artifacts")
        if (
            body.get("schema_version") != "fxpm.authority-spool-symbol-manifest.v2"
            or body.get("generation_id") != GENERATION_ID
            or body.get("generation_context_sha256") != GENERATION_CONTEXT_SHA256
            or body.get("native_universe_sha256") != NATIVE_UNIVERSE_SHA256
            or body.get("config_sha256") != CONFIG_SHA256
            or type(artifacts) is not list
            or body.get("artifact_count") != len(artifacts)
            or body.get("artifact_roster_sha256") != _sha(artifacts)
        ):
            _fail("PRESERVED_MANIFEST_BINDING_INVALID", row["relative_path"])
        total_bytes = 0
        artifact_hashes: list[str] = []
        for artifact in artifacts:
            descriptor = artifact.get("descriptor") if type(artifact) is dict else None
            if (
                type(descriptor) is not dict
                or set(descriptor) != _DESCRIPTOR_FIELDS
                or descriptor.get("schema_version")
                != "fxpm.authority-spool-artifact-descriptor.v1"
            ):
                _fail("PRESERVED_MANIFEST_DESCRIPTOR_INVALID", row["relative_path"])
            relative = "spool/" + str(descriptor["relative_path"])
            expected = _row_by_path().get(relative)
            if expected is None or any(
                descriptor.get(name) != expected[name]
                for name in ("sha256", "size_bytes")
            ):
                _fail("PRESERVED_MANIFEST_OBJECT_INVALID", relative)
            _read_root_file(root, relative)
            artifact_hashes.append(descriptor["sha256"])
            total_bytes += descriptor["size_bytes"]
        if body.get("artifact_total_bytes") != total_bytes:
            _fail("PRESERVED_MANIFEST_TOTAL_INVALID", row["relative_path"])
        summaries.append(
            {
                "relative_path": row["relative_path"],
                "sha256": row["sha256"],
                "symbol": body["symbol"],
                "artifact_count": len(artifacts),
                "artifact_sha256": artifact_hashes,
            }
        )
    if len(summaries) != 2:
        _fail("PRESERVED_MANIFEST_COUNT_INVALID", len(summaries))
    return summaries


def _verify_all_objects(root: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for digest, schema in _OBJECT_SCHEMA_BY_SHA.items():
        body = _read_object(root, digest)
        if body.get("schema_version") != schema:
            _fail("PRESERVED_OBJECT_SCHEMA_MISMATCH", digest)
        result[digest] = body
    _verify_jsonl(root)
    csv_raw = _read_root_file(root, _object_relative_path(PREFIX_CONTENT_SHA256, "csv"))
    if not csv_raw.startswith(
        b"time,Open,High,Low,Close,Volume,Spread,RealVolume,"
        b"_SOURCE_FIRST_TS,_SOURCE_LAST_TS,_BAR_CLOSE_TS\n"
    ):
        _fail("PRESERVED_PREFIX_CSV_HEADER_INVALID")
    return result


def _file_snapshot(
    path: Path,
    *,
    role: str,
    maximum_bytes: int = _MAX_EXTERNAL_FILE_BYTES,
    expected_sha256: str | None = None,
    expected_size: int | None = None,
) -> dict[str, Any]:
    try:
        resolved = Path(path).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RawWinnerCanaryConformanceError(
            "EXTERNAL_INPUT_PATH_INVALID",
            (role, str(path), type(exc).__name__, str(exc)),
        ) from exc
    raw = _read_regular(
        resolved,
        maximum_bytes=maximum_bytes,
        code="EXTERNAL_INPUT_READ_FAILED",
    )
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        _fail("EXTERNAL_INPUT_SHA256_MISMATCH", role, digest)
    if expected_size is not None and len(raw) != expected_size:
        _fail("EXTERNAL_INPUT_SIZE_MISMATCH", role, len(raw))
    return {
        "role": role,
        "path": str(resolved),
        "size_bytes": len(raw),
        "sha256": digest,
    }


def _external_input_snapshot(
    *,
    config_path: Path,
    data_dir: Path,
    index: Mapping[str, Any],
    broker_specs_path: Path,
) -> list[dict[str, Any]]:
    activation = index["activation"]
    source = index["source"]
    native_root = data_dir / "native"
    specifications = (
        (config_path, "deployment_config", CONFIG_FILE_SHA256, 36896),
        (
            broker_specs_path,
            "broker_specs",
            None,
            None,
        ),
        (
            native_root / "ACTIVE.json",
            "active_pointer",
            ACTIVE_POINTER_SHA256,
            365,
        ),
        (
            native_root / activation["manifest_relative_path"],
            "activation_manifest",
            ACTIVATION_MANIFEST_SHA256,
            5_441_495,
        ),
        (
            native_root / activation["run_manifest_relative_path"],
            "run_manifest",
            activation["run_manifest_sha256"],
            265_662,
        ),
        (
            native_root / source["bars_relative_path"],
            "native_bars",
            source["bars_sha256"],
            source["bars_bytes"],
        ),
        (
            native_root / source["audit_sidecar_relative_path"],
            "native_audit_sidecar",
            source["audit_sidecar_sha256"],
            source["audit_sidecar_bytes"],
        ),
        (
            native_root / source["provenance_relative_path"],
            "native_provenance",
            source["provenance_sha256"],
            source["provenance_bytes"],
        ),
    )
    return [
        _file_snapshot(
            path,
            role=role,
            expected_sha256=digest,
            expected_size=size,
        )
        for path, role, digest, size in specifications
    ]


def _source_file_hashes() -> dict[str, str]:
    executor_path = Path(search_execution.__file__).resolve(strict=True)
    planner_path = Path(search_planner.__file__).resolve(strict=True)
    observed = {
        "pm_authoritative_search_execution.py": hashlib.sha256(
            _read_regular(
                executor_path,
                maximum_bytes=_MAX_ROOT_FILE_BYTES,
                code="REVIEWED_SOURCE_READ_FAILED",
            )
        ).hexdigest(),
        "pm_production_search_planner.py": hashlib.sha256(
            _read_regular(
                planner_path,
                maximum_bytes=_MAX_ROOT_FILE_BYTES,
                code="REVIEWED_SOURCE_READ_FAILED",
            )
        ).hexdigest(),
    }
    expected = {
        "pm_authoritative_search_execution.py": REVIEWED_EXECUTOR_FILE_SHA256,
        "pm_production_search_planner.py": REVIEWED_PLANNER_FILE_SHA256,
    }
    if observed != expected:
        _fail("REVIEWED_MAPPING_SOURCE_CHANGED", observed)
    if (
        search_planner.PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256
        != REVIEWED_PLANNER_RELEASE_SHA256
        or search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
        != REVIEWED_EXECUTOR_RELEASE_SHA256
    ):
        _fail("REVIEWED_MAPPING_RELEASE_CHANGED")
    return observed


def _prefix_descriptor() -> dict[str, Any]:
    return {
        "schema_version": "fxpm.authority-spool-artifact-descriptor.v1",
        "format": "canonical-json",
        "relative_path": (f"objects/sha256/aa/{PREFIX_INDEX_SHA256}.canonical-json"),
        "sha256": PREFIX_INDEX_SHA256,
        "size_bytes": 4852,
    }


def _anchor() -> dict[str, Any]:
    return {
        "anchor_id": "causal-anchor-04-20240609",
        "ordinal": 3,
        "selection_cutoff_utc": "2024-06-09T00:00:00Z",
        "forward_start_utc": "2024-07-14T00:00:00Z",
        "forward_end_exclusive_utc": "2025-07-14T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-23T08:25:49Z",
        "process_precommit_receipt_sha256": CAUSAL_PRECOMMIT_SHA256,
    }


def _trial_from_artifacts(
    ordinal: int,
    design: Mapping[str, Any],
    artifacts: Any,
) -> dict[str, Any]:
    parameter_sha = _sha(artifacts.trial_definition["parameters"])
    identity = {
        "trial_id": f"TRIAL-{ordinal:03d}-{parameter_sha[:16]}",
        "strategy_id": design["strategy_id"],
        "strategy_release_sha256": artifacts.winner_process["strategy_code_sha256"],
        "parameter_set_sha256": parameter_sha,
    }
    return {**identity, "trial_definition_sha256": _sha(identity)}


def _build_bundles(
    *,
    process_body: Mapping[str, Any],
    cell: Mapping[str, Any],
    fitted: search_execution.FittedNativeSearchTimeframe,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    native = dict(fitted.product.native_prefix)
    frame = fitted.frame_copy()
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    candidate = cell["candidate"]
    fit_end = pd.Timestamp(native["prefix_end_exclusive_utc"])
    fit_start, source_count = search_execution._realize_cutoff_refit_window(
        opens=opens,
        closes=closes,
        fit_end=fit_end,
        lookback_kind=candidate["lookback_kind"],
        lookback_value=candidate["lookback_value"],
        state_context_bars=0,
        boundary_limited=True,
    )
    fit_start_utc = fit_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    if (
        fit_start_utc != "2023-06-09T00:00:00Z"
        or source_count != 259
        or native["prefix_end_exclusive_utc"] != "2024-06-08T00:00:00Z"
        or native["prefix_last_close_utc"] != "2024-06-08T00:00:00Z"
    ):
        _fail(
            "CURRENT_REFIT_WINDOW_MISMATCH",
            fit_start_utc,
            source_count,
        )

    designs = search_planner._attempt_designs(
        process_body,
        symbol="EURUSD",
        timeframe="D1",
        regime="TREND",
    )
    if len(designs) != 128:
        _fail("ATTEMPT_DESIGN_COUNT_INVALID", len(designs))

    bundles: list[dict[str, Any]] = []
    trials: list[dict[str, Any]] = []
    verification: list[dict[str, Any]] = []
    expected_hashes = cell["attempt_artifact_sha256"]
    for ordinal, design in enumerate(designs):
        seed_digest = search_planner._cell_hash(
            "EURUSD",
            "D1",
            "TREND",
            design["strategy_id"],
            _sha(design["parameters"]),
            "refit-seed",
        )
        seed = int(seed_digest[:8], 16) & 0x7FFFFFFF
        built = current_refit.build_current_refit_search_artifacts_v2(
            symbol="EURUSD",
            timeframe="D1",
            regime="TREND",
            strategy_id=design["strategy_id"],
            parameters=design["parameters"],
            lookback_kind=candidate["lookback_kind"],
            lookback_value=candidate["lookback_value"],
            fit_start_utc=fit_start_utc,
            fit_end_utc=native["prefix_end_exclusive_utc"],
            latest_closed_bar_utc=native["prefix_last_close_utc"],
            source_snapshot_sha256=native["prefix_content_sha256"],
            source_bar_count=source_count,
            seed=seed,
        )
        raw = {
            "winner_process": built.winner_process,
            "horizon_process": built.horizon_process,
            "trial_definition": built.trial_definition,
            "refit_recipe": built.refit_recipe,
        }
        bundle_sha = _sha(raw)
        if bundle_sha != expected_hashes[ordinal]:
            _fail("FROZEN_BUNDLE_SHA256_MISMATCH", ordinal, bundle_sha)
        try:
            verified = current_refit.verify_current_refit_search_artifacts_bytes_v2(
                winner_process_bytes=current_refit.canonical_artifact_bytes(
                    raw["winner_process"]
                ),
                horizon_process_bytes=current_refit.canonical_artifact_bytes(
                    raw["horizon_process"]
                ),
                trial_definition_bytes=current_refit.canonical_artifact_bytes(
                    raw["trial_definition"]
                ),
                refit_recipe_bytes=current_refit.canonical_artifact_bytes(
                    raw["refit_recipe"]
                ),
                symbol="EURUSD",
                timeframe="D1",
                regime="TREND",
                strategy_id=design["strategy_id"],
                lookback_kind=candidate["lookback_kind"],
                lookback_value=candidate["lookback_value"],
                fit_start_utc=fit_start_utc,
                fit_end_utc=native["prefix_end_exclusive_utc"],
                latest_closed_bar_utc=native["prefix_last_close_utc"],
                source_snapshot_sha256=native["prefix_content_sha256"],
                source_bar_count=source_count,
                seed=seed,
            )
        except Exception as exc:
            raise RawWinnerCanaryConformanceError(
                "FROZEN_BUNDLE_V2_VERIFICATION_FAILED",
                (str(ordinal), type(exc).__name__, str(exc)),
            ) from exc
        component_hashes = {
            "winner_process_sha256": verified.winner_process_sha256,
            "horizon_process_sha256": verified.horizon_process_sha256,
            "trial_definition_sha256": verified.trial_definition_sha256,
            "refit_recipe_sha256": verified.refit_recipe_sha256,
        }
        if component_hashes != {
            "winner_process_sha256": built.winner_process_sha256,
            "horizon_process_sha256": built.horizon_process_sha256,
            "trial_definition_sha256": built.trial_definition_sha256,
            "refit_recipe_sha256": built.refit_recipe_sha256,
        }:
            _fail("FROZEN_BUNDLE_COMPONENT_MISMATCH", ordinal)
        trial = _trial_from_artifacts(ordinal, design, built)
        bundles.append(raw)
        trials.append(trial)
        verification.append(
            {
                "ordinal": ordinal,
                "bundle_sha256": bundle_sha,
                "strategy_id": design["strategy_id"],
                "trial_id": trial["trial_id"],
                "seed": seed,
                **component_hashes,
                "independent_v2_verified": True,
            }
        )
    if expected_hashes != [item["bundle_sha256"] for item in verification] or cell[
        "attempt_roster_sha256"
    ] != _sha(expected_hashes):
        _fail("FROZEN_BUNDLE_ROSTER_INVALID")
    return bundles, trials, verification


def _horizon_request(
    *,
    cell: Mapping[str, Any],
    fitted: search_execution.FittedNativeSearchTimeframe,
    trials: Sequence[Mapping[str, Any]],
    planner_release_sha256: str,
    executor_release_sha256: str,
) -> dict[str, Any]:
    payload = cell["dependency_payload"]
    payload_sha = _sha(payload)
    dependency_receipt = _sha(
        {
            "schema_version": "fxpm.production-dependency-policy-binding.v1",
            "causal_process_precommit_receipt_sha256": CAUSAL_PRECOMMIT_SHA256,
            "search_process_receipt_sha256": PROCESS_SHA256,
            "timeframe": "D1",
            "payload_sha256": payload_sha,
        }
    )
    return {
        "schema_version": "fxpm.historical-horizon-request.v1",
        "run_id": GENERATION_ID,
        "anchor": _anchor(),
        "cell": dict(cell["cell"]),
        "native_prefix": dict(fitted.product.native_prefix),
        "dependency_policy": {
            "schema_version": "fxpm.historical-dependency-policy.v1",
            "authority_key_id": "fxpm-production-search-process-v1",
            "signed_policy_receipt_sha256": dependency_receipt,
            "payload_sha256": payload_sha,
            "payload": payload,
        },
        "horizon_policy": {
            "schema_version": "fxpm.historical-horizon-policy.v1",
            "policy_id": "fxpm-production-horizon-D1-v1",
            "policy_release_sha256": planner_release_sha256,
            "cost_contract_sha256": _sha(dict(fitted.base_cost_policy)),
            "cost_stress_contract_sha256": _sha(dict(fitted.stress_cost_policy)),
            "evaluator_release_sha256": executor_release_sha256,
        },
        "candidates": [dict(cell["candidate"])],
        "trials": [dict(value) for value in trials],
    }


def _map_old_attempts(
    *,
    plan_body: Mapping[str, Any],
    verification: list[dict[str, Any]],
) -> None:
    trials = plan_body["trials"]
    attempts = plan_body["attempts"]
    trial_by_id = {item["trial_id"]: item for item in trials}
    if len(trial_by_id) != 128 or len(attempts) != 128:
        _fail("OLD_PLAN_ATTEMPT_COUNT_INVALID")
    for ordinal, (attempt, bundle) in enumerate(zip(attempts, verification)):
        trial = trial_by_id.get(bundle["trial_id"])
        if (
            trial is None
            or attempt.get("trial_id") != bundle["trial_id"]
            or attempt.get("strategy_id") != bundle["strategy_id"]
            or attempt.get("strategy_release_sha256")
            != trial.get("strategy_release_sha256")
            or attempt.get("status") != "PROCESS_REJECTED"
            or attempt.get("reason_code") != "SOURCE_BOUNDARY_SHORTFALL"
        ):
            _fail("OLD_PLAN_BUNDLE_MAPPING_INVALID", ordinal)
        bundle["old_plan_attempt_id"] = attempt["attempt_id"]
        bundle["old_plan_status"] = attempt["status"]
        bundle["old_plan_reason_code"] = attempt["reason_code"]


def _validate_frozen_bodies(objects: Mapping[str, Mapping[str, Any]]) -> None:
    receipt = objects[ORIGINAL_CANARY_RECEIPT_SHA256]
    failure = objects[ORIGINAL_FAILURE_SHA256]
    cell = objects[CELL_PLAN_OBJECT_SHA256]
    source_session = objects[SOURCE_SESSION_SHA256]
    process = objects[PROCESS_SHA256]
    index = objects[PREFIX_INDEX_SHA256]
    cell_body = {key: value for key, value in cell.items() if key != "receipt_sha256"}
    if (
        receipt.get("status") != "SEARCH_INFRASTRUCTURE_FAILURE"
        or receipt.get("failure_code") != "ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID"
        or receipt.get("raw_winner_completed") is not False
        or receipt.get("standalone_winner_verifier_accepted") is not False
        or receipt.get("horizon_plan_receipt_sha256") != OLD_HORIZON_PLAN_SHA256
        or receipt.get("cell_plan_receipt_sha256") != CELL_PLAN_RECEIPT_SHA256
        or failure.get("failure_code") != "ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID"
        or failure.get("economic_no_trade_or_no_winner") is not False
        or cell.get("receipt_sha256") != _sha(cell_body)
        or cell.get("receipt_sha256") != CELL_PLAN_RECEIPT_SHA256
        or cell.get("attempt_count") != 128
        or cell.get("boundary_limited") is not True
        or cell.get("current_refit_source_capture_sha256") is not None
        or source_session.get("fitted_frame_sha256") != EXPECTED_FRAME_SHA256
        or process.get("planner_release_sha256") != OLD_PLANNER_RELEASE_SHA256
        or process.get("authoritative_executor_release_sha256")
        != OLD_EXECUTOR_RELEASE_SHA256
        or process.get("historical_adapter_release_sha256")
        != OLD_HISTORICAL_ADAPTER_RELEASE_SHA256
        or index.get("activation", {}).get("activation_id") != ACTIVATION_ID
        or index.get("activation", {}).get("manifest_sha256")
        != ACTIVATION_MANIFEST_SHA256
        or index.get("prefix", {}).get("content", {}).get("sha256")
        != PREFIX_CONTENT_SHA256
    ):
        _fail("FROZEN_BODY_BINDING_INVALID")


def _broker_specs_path(config_path: Path, payload: Mapping[str, Any]) -> Path:
    raw = payload.get("broker_specs_path")
    if type(raw) is not str or not raw.strip():
        _fail("BROKER_SPECS_CONFIG_INVALID")
    candidate = Path(raw)
    return (
        candidate.resolve(strict=True)
        if candidate.is_absolute()
        else (config_path.parent / candidate).resolve(strict=True)
    )


def _fit_preserved_prefix_in_scratch(
    *,
    failed_root: Path,
    data_dir: Path,
    pipeline_config: Any,
) -> tuple[
    native_prefix.NativePrefixProduct,
    search_execution.FittedNativeSearchTimeframe,
]:
    """Verify the frozen prefix without creating any file below ``failed_root``."""

    source_spool = _resolve_directory(
        failed_root / "spool",
        "PRESERVED_SPOOL_INVALID",
    )
    source_inventory = _inventory_rows(source_spool)
    try:
        with tempfile.TemporaryDirectory(
            prefix="fxpm-raw-winner-conformance-"
        ) as temporary:
            scratch_root = Path(temporary).resolve(strict=True)
            if scratch_root == failed_root or failed_root in scratch_root.parents:
                _fail(
                    "SCRATCH_ROOT_OVERLAPS_PRESERVED_ROOT",
                    str(scratch_root),
                )
            scratch_spool = scratch_root / "spool"
            shutil.copytree(
                source_spool,
                scratch_spool,
                copy_function=shutil.copy2,
                symlinks=True,
            )
            if _inventory_rows(scratch_spool) != source_inventory:
                _fail("SCRATCH_SPOOL_COPY_MISMATCH")
            scratch_context = authority_spool.reopen_generation_context(
                scratch_spool,
                expected_generation_id=GENERATION_ID,
                expected_native_universe_sha256=NATIVE_UNIVERSE_SHA256,
                expected_config_sha256=CONFIG_SHA256,
            )
            if scratch_context.context_sha256 != GENERATION_CONTEXT_SHA256:
                _fail("SCRATCH_GENERATION_CONTEXT_SHA256_MISMATCH")
            product = native_prefix.verify_activated_native_prefix(
                data_dir=data_dir,
                context=scratch_context,
                index_descriptor=_prefix_descriptor(),
                expected_index_sha256=PREFIX_INDEX_SHA256,
                expected_symbol="EURUSD",
                expected_timeframe="D1",
                expected_cutoff_utc="2024-06-09T00:00:00Z",
                expected_activation_manifest_sha256=ACTIVATION_MANIFEST_SHA256,
                production_contract_required=True,
                require_current_activation=False,
            )
            fitted = search_execution.fit_native_timeframe_for_search(
                product=product,
                config=pipeline_config,
            )
            if _inventory_rows(scratch_spool) != source_inventory:
                _fail("SCRATCH_SPOOL_CHANGED_DURING_PREFIX_VERIFICATION")
            return product, fitted
    except RawWinnerCanaryConformanceError:
        raise
    except Exception as exc:
        raise RawWinnerCanaryConformanceError(
            "SCRATCH_PREFIX_OR_FIT_REOPEN_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc


def _build_conformance_body(
    *,
    failed_root: Path,
    config_path: Path,
    inventory_before: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifests = _verify_manifests(failed_root)
    objects = _verify_all_objects(failed_root)
    _validate_frozen_bodies(objects)
    index = objects[PREFIX_INDEX_SHA256]
    process_body = objects[PROCESS_SHA256]
    cell = objects[CELL_PLAN_OBJECT_SHA256]
    source_session = objects[SOURCE_SESSION_SHA256]

    try:
        context = authority_spool.reopen_generation_context(
            failed_root / "spool",
            expected_generation_id=GENERATION_ID,
            expected_native_universe_sha256=NATIVE_UNIVERSE_SHA256,
            expected_config_sha256=CONFIG_SHA256,
        )
    except Exception as exc:
        raise RawWinnerCanaryConformanceError(
            "GENERATION_CONTEXT_REOPEN_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if context.context_sha256 != GENERATION_CONTEXT_SHA256:
        _fail("GENERATION_CONTEXT_SHA256_MISMATCH")

    config_raw = _file_snapshot(
        config_path,
        role="deployment_config",
        expected_sha256=CONFIG_FILE_SHA256,
        expected_size=36896,
    )
    config_value = _strict_json(
        _read_regular(
            config_path,
            maximum_bytes=_MAX_EXTERNAL_FILE_BYTES,
            code="CONFIG_READ_FAILED",
        ),
        canonical=False,
        code="CONFIG_JSON_INVALID",
    )
    if type(config_value) is not dict or _sha(config_value) != CONFIG_SHA256:
        _fail("CONFIG_CANONICAL_SHA256_MISMATCH")
    try:
        config_payload, supplied_config = config_binding.load_effective_pipeline_config(
            config_path
        )
        frozen_config = config_binding.freeze_production_pipeline_config(
            context,
            active_config_path=config_path,
            config_payload=config_payload,
            supplied_config=supplied_config,
        )
        pipeline_config = frozen_config.materialize(context)
    except Exception as exc:
        raise RawWinnerCanaryConformanceError(
            "PRODUCTION_CONFIG_REOPEN_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        frozen_config.config_sha256 != CONFIG_SHA256
        or frozen_config.effective_config_sha256 != EFFECTIVE_CONFIG_SHA256
    ):
        _fail("PRODUCTION_CONFIG_BINDING_MISMATCH")
    data_dir = _resolve_directory(
        Path(pipeline_config.data_dir), "CONFIG_DATA_DIR_INVALID"
    )
    broker_specs = _broker_specs_path(config_path, config_payload)
    external_before = _external_input_snapshot(
        config_path=config_path,
        data_dir=data_dir,
        index=index,
        broker_specs_path=broker_specs,
    )
    if external_before[0] != config_raw:
        _fail("CONFIG_SNAPSHOT_MISMATCH")

    product, fitted = _fit_preserved_prefix_in_scratch(
        failed_root=failed_root,
        data_dir=data_dir,
        pipeline_config=pipeline_config,
    )
    if (
        product.index_sha256 != PREFIX_INDEX_SHA256
        or product.native_prefix["prefix_content_sha256"] != PREFIX_CONTENT_SHA256
        or fitted.frame_sha256 != EXPECTED_FRAME_SHA256
        or fitted.source_group_decision.receipt_sha256
        != EXPECTED_SOURCE_DECISION_SHA256
        or fitted.regime_fit_decision.receipt_sha256 != EXPECTED_REGIME_FIT_SHA256
        or fitted.train_regime_support.get("receipt_sha256")
        != EXPECTED_REGIME_SUPPORT_SHA256
        or source_session.get("source_group_decision_sha256")
        != fitted.source_group_decision.receipt_sha256
        or source_session.get("regime_fit_decision_sha256")
        != fitted.regime_fit_decision.receipt_sha256
    ):
        _fail("FROZEN_PREFIX_OR_FIT_BINDING_INVALID")

    bundles, trials, verification = _build_bundles(
        process_body=process_body,
        cell=cell,
        fitted=fitted,
    )
    old_request = _horizon_request(
        cell=cell,
        fitted=fitted,
        trials=trials,
        planner_release_sha256=OLD_PLANNER_RELEASE_SHA256,
        executor_release_sha256=OLD_EXECUTOR_RELEASE_SHA256,
    )
    try:
        old_plan = training_horizon.build_historical_horizon_plan(old_request)
        old_binding = historical.historical_horizon_plan_binding(old_plan)
    except Exception as exc:
        raise RawWinnerCanaryConformanceError(
            "OLD_HORIZON_PLAN_RECONSTRUCTION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        old_plan.receipt_sha256 != OLD_HORIZON_PLAN_SHA256
        or old_binding != cell["horizon_plan_binding"]
    ):
        _fail(
            "OLD_HORIZON_PLAN_RECEIPT_MISMATCH",
            old_plan.receipt_sha256,
        )
    old_plan_body = old_plan.to_dict()
    _map_old_attempts(plan_body=old_plan_body, verification=verification)

    current_request = copy.deepcopy(old_request)
    current_request["horizon_policy"]["policy_release_sha256"] = (
        REVIEWED_PLANNER_RELEASE_SHA256
    )
    current_request["horizon_policy"]["evaluator_release_sha256"] = (
        REVIEWED_EXECUTOR_RELEASE_SHA256
    )
    try:
        current_plan = training_horizon.build_historical_horizon_plan(current_request)
        current_binding = historical.historical_horizon_plan_binding(current_plan)
        reviewed_plan_body, reviewed_attempts = search_execution._verify_attempt_roster(
            plan=current_plan,
            attempt_artifacts=bundles,
            fitted=fitted,
            regime="TREND",
        )
    except Exception as exc:
        raise RawWinnerCanaryConformanceError(
            "REVIEWED_MAPPING_REPLAY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        current_plan.receipt_sha256 == OLD_HORIZON_PLAN_SHA256
        or current_binding == old_binding
        or reviewed_plan_body != current_plan.to_dict()
        or len(reviewed_attempts) != 128
        or [item.plan_attempt["attempt_id"] for item in reviewed_attempts]
        != [item["attempt_id"] for item in current_plan.to_dict()["attempts"]]
        or any(
            item.plan_attempt["status"] != "PROCESS_REJECTED"
            or item.plan_attempt["reason_code"] != "SOURCE_BOUNDARY_SHORTFALL"
            for item in reviewed_attempts
        )
    ):
        _fail("REVIEWED_MAPPING_REPLAY_BINDING_INVALID")
    for bundle, reviewed in zip(verification, reviewed_attempts):
        bundle["reviewed_plan_attempt_id"] = reviewed.plan_attempt["attempt_id"]
        bundle["reviewed_mapping_verified"] = True

    source_hashes = _source_file_hashes()
    external_after = _external_input_snapshot(
        config_path=config_path,
        data_dir=data_dir,
        index=index,
        broker_specs_path=broker_specs,
    )
    if external_after != external_before:
        _fail("EXTERNAL_INPUT_CHANGED_DURING_CONFORMANCE")
    inventory_after = inventory_preserved_root(failed_root)
    _require_expected_inventory(inventory_after, "AFTER_REPLAY")
    if inventory_after != inventory_before:
        _fail("PRESERVED_ROOT_CHANGED_DURING_CONFORMANCE")

    bundle_hashes = [item["bundle_sha256"] for item in verification]
    body = {
        "schema_version": CONFORMANCE_SCHEMA,
        "authority_scope": CONFORMANCE_SCOPE,
        "source_failed_root": str(failed_root),
        "preserved_root_id": PRESERVED_ROOT_ID,
        "original_inventory": {
            "algorithm": inventory_before["algorithm"],
            "before_sha256": inventory_before["inventory_sha256"],
            "after_sha256": inventory_after["inventory_sha256"],
            "file_count": inventory_before["file_count"],
            "total_bytes": inventory_before["total_bytes"],
            "rows": inventory_before["rows"],
            "before_after_identical": True,
        },
        "frozen_provenance": {
            "generation_id": GENERATION_ID,
            "generation_context_sha256": GENERATION_CONTEXT_SHA256,
            "native_universe_sha256": NATIVE_UNIVERSE_SHA256,
            "config_sha256": CONFIG_SHA256,
            "effective_pipeline_config_sha256": EFFECTIVE_CONFIG_SHA256,
            "original_canary_receipt_sha256": ORIGINAL_CANARY_RECEIPT_SHA256,
            "original_failure_sha256": ORIGINAL_FAILURE_SHA256,
            "original_failure_code": ("ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID"),
            "cell_plan_object_sha256": CELL_PLAN_OBJECT_SHA256,
            "cell_plan_receipt_sha256": CELL_PLAN_RECEIPT_SHA256,
            "source_session_sha256": SOURCE_SESSION_SHA256,
            "search_process_receipt_sha256": PROCESS_SHA256,
            "prefix_index_sha256": PREFIX_INDEX_SHA256,
            "prefix_content_sha256": PREFIX_CONTENT_SHA256,
            "manifest_verification": manifests,
        },
        "source_reopen": {
            "activation_id": ACTIVATION_ID,
            "activation_manifest_sha256": ACTIVATION_MANIFEST_SHA256,
            "active_pointer_sha256": ACTIVE_POINTER_SHA256,
            "fitted_frame_sha256": fitted.frame_sha256,
            "source_group_decision_sha256": (
                fitted.source_group_decision.receipt_sha256
            ),
            "regime_fit_decision_sha256": (fitted.regime_fit_decision.receipt_sha256),
            "train_regime_support_receipt_sha256": (
                fitted.train_regime_support["receipt_sha256"]
            ),
            "referenced_external_inputs_before": external_before,
            "referenced_external_inputs_after": external_after,
            "referenced_external_inputs_before_after_identical": True,
        },
        "old_release_reconstruction": {
            "planner_release_sha256": OLD_PLANNER_RELEASE_SHA256,
            "executor_release_sha256": OLD_EXECUTOR_RELEASE_SHA256,
            "historical_adapter_release_sha256": (
                OLD_HISTORICAL_ADAPTER_RELEASE_SHA256
            ),
            "horizon_plan_receipt_sha256": old_plan.receipt_sha256,
            "horizon_plan_binding": old_binding,
            "plan_status": old_plan_body["plan_status"],
            "attempt_count_exact": len(old_plan_body["attempts"]),
            "attempt_roster_sha256": old_plan_body["attempt_roster_sha256"],
            "evidence_roster_sha256": old_plan_body["evidence_roster_sha256"],
            "all_attempts_process_rejected": True,
            "all_reason_code": "SOURCE_BOUNDARY_SHORTFALL",
            "exact_frozen_receipt_reproduced": True,
        },
        "bundle_conformance": {
            "bundle_count_exact": len(verification),
            "bundle_sha256": bundle_hashes,
            "bundle_roster_sha256": _sha(bundle_hashes),
            "independent_v2_verification_count_exact": len(verification),
            "mapping": verification,
        },
        "reviewed_mapping_replay": {
            "reviewed_mapping_artifact": REVIEWED_MAPPING_ARTIFACT,
            "reviewed_source_file_sha256": source_hashes,
            "reviewed_planner_release_sha256": (REVIEWED_PLANNER_RELEASE_SHA256),
            "reviewed_executor_release_sha256": (REVIEWED_EXECUTOR_RELEASE_SHA256),
            "reviewed_horizon_plan_receipt_sha256": (current_plan.receipt_sha256),
            "reviewed_horizon_plan_binding": current_binding,
            "reviewed_mapping_verification_count_exact": len(reviewed_attempts),
            "reviewed_release_is_distinct_from_frozen_old_release": True,
            "old_plan_is_not_relabelled_as_reviewed_release": True,
        },
        "safety": {
            "strategy_evaluator_calls_exact": 0,
            "raw_winner_invoked": False,
            "full_bounded_canary_verifier_invoked": False,
            "full_bounded_canary_verifier_replaced": False,
            "closes_named_raw_winner_gate": False,
            "raw_winner_verified_claimed": False,
            "economic_no_trade_or_no_winner_claimed": False,
            "causal_authority": False,
            "current_refit_authority": False,
            "book_projection_authority": False,
            "capital_authority": False,
            "publication_authority": False,
            "order_authority": False,
            "live_or_demo_trading_authority": False,
            "configuration_mutation_authority": False,
            "active_pointer_mutation_authority": False,
            "preserved_root_mutation_authority": False,
            "production_data_mutation_authority": False,
        },
    }
    return body, external_after


def _validate_output_path(
    *, failed_root: Path, output_root: Path, config_path: Path
) -> Path:
    try:
        output = Path(output_root).resolve(strict=False)
        parent = output.parent.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RawWinnerCanaryConformanceError(
            "OUTPUT_ROOT_INVALID",
            (str(output_root), type(exc).__name__, str(exc)),
        ) from exc
    if (
        parent != failed_root.parent
        or output == failed_root
        or output.exists()
        or output.name in {"", ".", ".."}
    ):
        _fail("OUTPUT_ROOT_NOT_FRESH_SIBLING", str(output))
    try:
        config_resolved = config_path.resolve(strict=True)
    except OSError as exc:
        raise RawWinnerCanaryConformanceError(
            "CONFIG_PATH_INVALID", (str(config_path), type(exc).__name__, str(exc))
        ) from exc
    if output == config_resolved or output in config_resolved.parents:
        _fail("OUTPUT_ROOT_CONFIG_OVERLAP", str(output))
    return output


def _write_receipt(output_root: Path, receipt: Mapping[str, Any]) -> Path:
    encoded = _canonical_bytes(receipt)
    created = False
    temp_path = output_root / f".{CONFORMANCE_RECEIPT_NAME}.tmp"
    receipt_path = output_root / CONFORMANCE_RECEIPT_NAME
    try:
        output_root.mkdir(mode=0o700, exist_ok=False)
        created = True
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        descriptor = os.open(temp_path, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        os.replace(temp_path, receipt_path)
        if (
            _read_regular(
                receipt_path,
                maximum_bytes=_MAX_RECEIPT_BYTES,
                code="CONFORMANCE_RECEIPT_REOPEN_FAILED",
            )
            != encoded
        ):
            _fail("CONFORMANCE_RECEIPT_WRITE_MISMATCH")
    except FileExistsError as exc:
        raise RawWinnerCanaryConformanceError(
            "OUTPUT_ROOT_COLLISION", (str(output_root),)
        ) from exc
    except BaseException:
        if created:
            try:
                temp_path.unlink(missing_ok=True)
                receipt_path.unlink(missing_ok=True)
                output_root.rmdir()
            except OSError:
                pass
        raise
    return receipt_path


def conform_preserved_raw_winner_canary(
    *,
    failed_root: Path,
    config_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    """Replay and seal the exact preserved-root conformance evidence."""

    root = _resolve_directory(Path(failed_root), "PRESERVED_ROOT_INVALID")
    if root.name != PRESERVED_ROOT_ID:
        _fail("PRESERVED_ROOT_ID_MISMATCH", root.name)
    try:
        config = Path(config_path).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RawWinnerCanaryConformanceError(
            "CONFIG_PATH_INVALID",
            (str(config_path), type(exc).__name__, str(exc)),
        ) from exc
    output = _validate_output_path(
        failed_root=root, output_root=Path(output_root), config_path=config
    )
    inventory_before = inventory_preserved_root(root)
    _require_expected_inventory(inventory_before, "BEFORE_REPLAY")
    body, external_after = _build_conformance_body(
        failed_root=root,
        config_path=config,
        inventory_before=inventory_before,
    )
    receipt = {**body, "receipt_sha256": _sha(body)}
    receipt_path: Path | None = None
    try:
        receipt_path = _write_receipt(output, receipt)
        final_inventory = inventory_preserved_root(root)
        _require_expected_inventory(final_inventory, "AFTER_PUBLICATION")
        if final_inventory != inventory_before:
            _fail("PRESERVED_ROOT_CHANGED_DURING_PUBLICATION")
        index = _read_object(root, PREFIX_INDEX_SHA256)
        config_payload, supplied_config = config_binding.load_effective_pipeline_config(
            config
        )
        data_dir = _resolve_directory(
            Path(supplied_config.data_dir), "CONFIG_DATA_DIR_INVALID"
        )
        final_external = _external_input_snapshot(
            config_path=config,
            data_dir=data_dir,
            index=index,
            broker_specs_path=_broker_specs_path(config, config_payload),
        )
        if final_external != external_after:
            _fail("EXTERNAL_INPUT_CHANGED_DURING_PUBLICATION")
        return verify_preserved_raw_winner_canary_conformance_receipt(
            receipt_path=receipt_path,
            failed_root=root,
            config_path=config,
        )
    except BaseException:
        if receipt_path is not None:
            try:
                receipt_path.unlink(missing_ok=True)
                output.rmdir()
            except OSError:
                pass
        raise


def verify_preserved_raw_winner_canary_conformance_receipt(
    *,
    receipt_path: Path,
    failed_root: Path,
    config_path: Path,
) -> dict[str, Any]:
    """Verify the standalone conformance receipt without claiming canary closure."""

    root = _resolve_directory(Path(failed_root), "PRESERVED_ROOT_INVALID")
    path = Path(receipt_path).resolve(strict=True)
    if (
        path.name != CONFORMANCE_RECEIPT_NAME
        or path.parent.parent != root.parent
        or path.parent == root
    ):
        _fail("CONFORMANCE_RECEIPT_LOCATION_INVALID", str(path))
    entries = sorted(path.parent.iterdir(), key=lambda value: value.name)
    if entries != [path]:
        _fail("CONFORMANCE_OUTPUT_NOT_STANDALONE", str(path.parent))
    raw = _read_regular(
        path,
        maximum_bytes=_MAX_RECEIPT_BYTES,
        code="CONFORMANCE_RECEIPT_READ_FAILED",
    )
    receipt = _strict_json(raw, canonical=True, code="CONFORMANCE_RECEIPT_JSON_INVALID")
    if type(receipt) is not dict:
        _fail("CONFORMANCE_RECEIPT_OBJECT_REQUIRED")
    receipt_sha = receipt.get("receipt_sha256")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    inventory = inventory_preserved_root(root)
    if (
        receipt.get("schema_version") != CONFORMANCE_SCHEMA
        or receipt.get("authority_scope") != CONFORMANCE_SCOPE
        or receipt.get("source_failed_root") != str(root)
        or receipt.get("preserved_root_id") != PRESERVED_ROOT_ID
        or type(receipt_sha) is not str
        or receipt_sha != _sha(body)
    ):
        _fail("CONFORMANCE_RECEIPT_BINDING_INVALID")
    _require_expected_inventory(inventory, "RECEIPT_REVERIFY")
    config = Path(config_path).resolve(strict=True)
    expected_body, expected_external = _build_conformance_body(
        failed_root=root,
        config_path=config,
        inventory_before=inventory,
    )
    if body != expected_body:
        _fail("CONFORMANCE_RECEIPT_BINDING_INVALID")
    config_payload, supplied_config = config_binding.load_effective_pipeline_config(
        config
    )
    current_external = _external_input_snapshot(
        config_path=config,
        data_dir=_resolve_directory(
            Path(supplied_config.data_dir), "CONFIG_DATA_DIR_INVALID"
        ),
        index=_read_object(root, PREFIX_INDEX_SHA256),
        broker_specs_path=_broker_specs_path(config, config_payload),
    )
    final_inventory = inventory_preserved_root(root)
    _require_expected_inventory(final_inventory, "RECEIPT_REVERIFY_FINAL")
    if final_inventory != inventory or current_external != expected_external:
        _fail("CONFORMANCE_RECEIPT_EXTERNAL_INPUT_CHANGED")
    return copy.deepcopy(receipt)


__all__ = [
    "CONFORMANCE_RECEIPT_NAME",
    "CONFORMANCE_SCHEMA",
    "CONFORMANCE_SCOPE",
    "EXPECTED_INVENTORY_FILE_COUNT",
    "EXPECTED_INVENTORY_SHA256",
    "EXPECTED_INVENTORY_TOTAL_BYTES",
    "PRESERVED_ROOT_ID",
    "RawWinnerCanaryConformanceError",
    "conform_preserved_raw_winner_canary",
    "inventory_preserved_root",
    "verify_preserved_raw_winner_canary_conformance_receipt",
]
