"""End-grid rows: the one durable result per cell per cycle.

Under the operator lifecycle OPEN, USE, PROCESS, RESULT, DELETE the permanent
elements are code, canonical M5 history and the end grids.  This module is the
RESULT step's record.  One row states, for one ``symbol x timeframe x regime``
cell and one selection cycle, what was decided (a winner, NO_TRADE, a typed
STOP, or UNAVAILABLE data) and binds enough identity that the decision can be
rebuilt from code and canonical M5 alone: the frozen process edition, the
source contract and view key, the producing module hashes and the chosen
configuration.  Rows never reference a working spool path.

Schema v2 adds one named field, ``no_trade_run_receipt_sha256``: the saved
producer cell-run receipt that proves every selected slot ended NO_TRADE.  It
is required on a NO_TRADE row and refused on every other outcome, so a cell
that selected nothing is evidenced by its own receipt and never by a trade
contract it did not have.  ``source_contract_sha256`` keeps its meaning as the
per-cell tier-pair trade evidence contract: required on a WINNER, optional on
a NO_TRADE (present only when a verified trade source existed).  A stored v1
line still reads under the v1 key set, hash and rules; the writer appends v2.

The row is a record of a decision that was made elsewhere.  Writing one grants
no qualification, capital, live or order authority, and a row's hash is a
reproducibility checksum, not a signature.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, fields
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Any, Final, Iterable, Iterator, Mapping

if os.name == "nt":
    import msvcrt
else:
    import fcntl

import pm_book_multiplicity as book


END_GRID_SCHEMA_VERSION_V1: Final = "fxpm.end-grid-row.v1"
END_GRID_SCHEMA_VERSION: Final = "fxpm.end-grid-row.v2"
END_GRID_SCHEMA_VERSIONS: Final = (END_GRID_SCHEMA_VERSION_V1, END_GRID_SCHEMA_VERSION)
OUTCOMES: Final = ("WINNER", "NO_TRADE", "STOP", "UNAVAILABLE")
# Canonical engine source kinds live in the hashed row.  The human-facing
# labels are rendered outside the hashed body by source_kind_label().
SOURCE_KINDS: Final = ("broker_native", "m5_derived", "m5_derived_control_clock.v1")
SOURCE_KIND_LABELS: Final = {
    "broker_native": "NATIVE_HTF",
    "m5_derived": "DERIVED_FROM_M5",
    "m5_derived_control_clock.v1": "DERIVED_FROM_M5_CLOCK",
}
# A clock-constructed derived view has no causal-calendar proof yet, so it can
# only ever record a typed STOP or UNAVAILABLE, never a result.
STOP_ONLY_SOURCE_KINDS: Final = ("m5_derived_control_clock.v1",)
COST_TIERS: Final = ("P75", "P95", "NONE")
MAX_CODE_IDENTITY_ENTRIES: Final = 32
MAX_METRIC_ENTRIES: Final = 32
MAX_ROW_BYTES: Final = 16 * 1024
MAX_FILE_BYTES: Final = 64 * 1024 * 1024

_HEX64 = re.compile(r"[0-9a-f]{64}")
_SAFE_CODE = re.compile(r"[A-Z0-9][A-Z0-9_:.\-]{0,127}")
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}")
AUTHORITY_FLAGS: Final = ("qualification_authority", "capital_authority",
                          "live_authority", "order_authority")
MAX_SOURCE_IDENTITY_ENTRIES: Final = 8
_ROW_KEYS_V1: Final = frozenset({
    "schema_version", "cell_id", "symbol", "timeframe", "regime",
    "cycle_start_utc", "cycle_end_utc", "information_cutoff_utc",
    "outcome", "reason_code", "strategy_family", "configuration_sha256",
    "source_kind", "source_contract_sha256", "view_key", "view_contract_sha256",
    "source_identity", "job_binding_sha256", "consumer_binding_sha256", "cost_tier",
    "process_edition_sha256", "code_sha256", "metrics", "produced_at_utc",
    *AUTHORITY_FLAGS, "row_sha256",
})
# v2 adds the named NO_TRADE evidence field; a v1 line must not carry it.
_V2_ONLY_KEYS: Final = frozenset({"no_trade_run_receipt_sha256"})
_ROW_KEYS: Final = _ROW_KEYS_V1 | _V2_ONLY_KEYS
_REPRODUCIBILITY_KEYS: Final = (
    "cell_id", "cycle_start_utc", "cycle_end_utc", "information_cutoff_utc",
    "process_edition_sha256", "source_kind", "source_contract_sha256",
    "no_trade_run_receipt_sha256",
    "view_key", "view_contract_sha256", "source_identity", "job_binding_sha256",
    "consumer_binding_sha256", "cost_tier", "strategy_family",
    "configuration_sha256", "code_sha256",
)
_SAFE_VALUE = re.compile(r"[\x21-\x7e]{1,128}")
_PATH_LIKE = re.compile(r"[/\\:]|\.(jsonl|csv|json|parquet)$")
# The identity each source kind must carry inline so a row rebuilds after every
# spool is gone.  Keys ending in sha256 must be hex; nothing may look like a path.
SOURCE_IDENTITY_REQUIRED: Final = {
    "broker_native": ("activation_id", "native_bars_sha256"),
    "m5_derived": ("activation_id", "m5_source_sha256",
                   "constructor_implementation_sha256", "gap_findings_sha256"),
    "m5_derived_control_clock.v1": ("activation_id", "m5_source_sha256",
                                    "constructor_implementation_sha256", "gap_findings_sha256",
                                    "clock_template_sha256"),
}
LOCK_RETRY_SECONDS: Final = 0.05
LOCK_TIMEOUT_SECONDS: Final = 30.0
CONTRACTS_DIR: Final = "contracts"
MAX_CONTRACT_BYTES: Final = 4 * 1024 * 1024


class EndGridError(ValueError):
    """A malformed, inconsistent or unverifiable row.  Never a trading outcome."""


def _fail(code: str) -> None:
    raise EndGridError(code)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hex64(value: Any, code: str) -> str:
    if type(value) is not str or _HEX64.fullmatch(value) is None:
        _fail(code)
    return value


def _optional_hex64(value: Any, code: str) -> str | None:
    return None if value is None else _hex64(value, code)


def _code(value: Any, code: str) -> str:
    if type(value) is not str or _SAFE_CODE.fullmatch(value) is None:
        _fail(code)
    return value


def _optional_name(value: Any, code: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or _SAFE_NAME.fullmatch(value) is None:
        _fail(code)
    return value


def _choice(value: Any, choices: tuple[str, ...], code: str) -> str:
    if type(value) is not str or value not in choices:
        _fail(code)
    return value


def _utc(value: Any, code: str) -> str:
    """Exact canonical UTC text, the same form the Book uses (+00:00, no Z)."""
    if type(value) is not str or not 20 <= len(value) <= 40:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        _fail(code)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    if parsed.astimezone(timezone.utc).isoformat() != value:
        _fail(code)
    return value


def _code_identity(value: Any) -> dict[str, str]:
    if type(value) is not dict or not 1 <= len(value) <= MAX_CODE_IDENTITY_ENTRIES:
        _fail("CODE_IDENTITY_INVALID")
    out: dict[str, str] = {}
    for name in sorted(value):
        if type(name) is not str or _SAFE_NAME.fullmatch(name) is None:
            _fail("CODE_IDENTITY_NAME_INVALID")
        out[name] = _hex64(value[name], "CODE_IDENTITY_SHA_INVALID")
    return out


def _source_identity(value: Any, source_kind: str, *, required: bool) -> dict[str, str]:
    """Inline identity of the retained inputs, so provenance outlives every spool.

    A result row must carry its source kind's required keys.  A STOP or
    UNAVAILABLE row carries whatever was actually known, possibly nothing: a
    failure before the source was reached must never invent a bars hash.
    Every key ending in sha256 is hex64; other values are short printable
    tokens; anything path-like is refused, because a path into a working
    spool is exactly what must not be relied on.
    """
    if type(value) is not dict or not (1 if required else 0) <= len(value) <= MAX_SOURCE_IDENTITY_ENTRIES:
        _fail("SOURCE_IDENTITY_INVALID")
    out: dict[str, str] = {}
    for name in sorted(value):
        if type(name) is not str or _SAFE_NAME.fullmatch(name) is None:
            _fail("SOURCE_IDENTITY_NAME_INVALID")
        item = value[name]
        if type(item) is not str or _SAFE_VALUE.fullmatch(item) is None:
            _fail("SOURCE_IDENTITY_VALUE_INVALID")
        if _PATH_LIKE.search(item) is not None:
            _fail("SOURCE_IDENTITY_PATH_REFUSED")
        if name.endswith("sha256"):
            _hex64(item, "SOURCE_IDENTITY_SHA_INVALID")
        out[name] = item
    if required:
        for name in SOURCE_IDENTITY_REQUIRED[source_kind]:
            if name not in out:
                _fail(f"SOURCE_IDENTITY_MISSING:{name}")
    return out


def _metrics(value: Any) -> dict[str, Any]:
    """Finite scalars only; metric meaning belongs to the producing process."""
    if type(value) is not dict or len(value) > MAX_METRIC_ENTRIES:
        _fail("METRICS_INVALID")
    out: dict[str, Any] = {}
    for name in sorted(value):
        if type(name) is not str or _SAFE_NAME.fullmatch(name) is None:
            _fail("METRIC_NAME_INVALID")
        item = value[name]
        if item is None or type(item) in (bool, int):
            out[name] = item
        elif type(item) is float:
            if item != item or item in (float("inf"), float("-inf")):
                _fail("METRIC_NOT_FINITE")
            out[name] = item
        else:
            _fail("METRIC_TYPE_INVALID")
    return out


@dataclass(frozen=True, slots=True)
class EndGridRow:
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    cycle_start_utc: str
    cycle_end_utc: str
    information_cutoff_utc: str
    outcome: str
    reason_code: str
    strategy_family: str | None
    configuration_sha256: str | None
    source_kind: str
    # The genuine per-cell tier-pair trade evidence contract.  Required for a
    # WINNER or NO_TRADE; None for an UNAVAILABLE row and for a STOP that
    # happened before a trade source was seeded.
    source_contract_sha256: str | None
    # v2: the saved producer cell-run receipt proving every selected slot ended
    # NO_TRADE.  Required on a NO_TRADE row, refused on every other outcome,
    # and never derived from or written into source_contract_sha256.
    no_trade_run_receipt_sha256: str | None
    # Value identity of the view the row was produced from, and the retained
    # reconstruction body under contracts/.  Present together when a view
    # existed, absent together before one did.  Required for a result row.
    view_key: str | None
    view_contract_sha256: str | None
    source_identity: dict[str, str]
    job_binding_sha256: str | None
    consumer_binding_sha256: str | None
    cost_tier: str
    process_edition_sha256: str
    code_sha256: dict[str, str]
    metrics: dict[str, Any]
    produced_at_utc: str
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False
    schema_version: str = END_GRID_SCHEMA_VERSION

    def body(self) -> dict[str, Any]:
        """Every field of this row's schema except the row hash, in canonical form."""
        out = {f.name: getattr(self, f.name) for f in fields(self)}
        if self.schema_version == END_GRID_SCHEMA_VERSION_V1:
            for key in _V2_ONLY_KEYS:
                out.pop(key, None)
        return out

    @property
    def row_sha256(self) -> str:
        return _sha256(canonical_bytes(self.body()))

    def to_dict(self) -> dict[str, Any]:
        out = self.body()
        out["row_sha256"] = self.row_sha256
        return out

    def to_bytes(self) -> bytes:
        return canonical_bytes(self.to_dict())

    def reproducibility_identity(self) -> dict[str, Any]:
        """The subset from which the decision is rebuilt from code and M5."""
        body = self.body()
        subset = {key: body[key] for key in _REPRODUCIBILITY_KEYS if key in body}
        subset["identity_sha256"] = _sha256(canonical_bytes(subset))
        return subset


def build_end_grid_row(
    *,
    symbol: Any,
    timeframe: Any,
    regime: Any,
    cycle_start_utc: Any,
    cycle_end_utc: Any,
    information_cutoff_utc: Any,
    outcome: Any,
    reason_code: Any,
    source_kind: Any,
    source_contract_sha256: Any,
    view_key: Any,
    source_identity: Mapping[str, Any],
    cost_tier: Any,
    view_contract_sha256: Any = None,
    process_edition_sha256: Any,
    code_sha256: Mapping[str, Any],
    metrics: Mapping[str, Any] | None = None,
    strategy_family: Any = None,
    configuration_sha256: Any = None,
    job_binding_sha256: Any = None,
    consumer_binding_sha256: Any = None,
    produced_at_utc: Any = None,
    no_trade_run_receipt_sha256: Any = None,
    schema_version: Any = END_GRID_SCHEMA_VERSION,
) -> EndGridRow:
    """Validate every field and bind the family cell identity.  Fail closed.

    ``schema_version`` is v2 for every new row; v1 is accepted only so that a
    stored v1 line re-verifies under the rules it was written with.
    """
    schema = _choice(schema_version, END_GRID_SCHEMA_VERSIONS, "ROW_SCHEMA_VERSION_INVALID")
    cell_id = book.compute_cell_id(symbol, timeframe, regime)
    if cell_id is None:
        _fail("CELL_NOT_IN_FAMILY")
    start = _utc(cycle_start_utc, "CYCLE_START_INVALID")
    end = _utc(cycle_end_utc, "CYCLE_END_INVALID")
    cutoff = _utc(information_cutoff_utc, "CUTOFF_INVALID")
    if not datetime.fromisoformat(start) < datetime.fromisoformat(end):
        _fail("CYCLE_EMPTY")
    if datetime.fromisoformat(cutoff) > datetime.fromisoformat(start):
        _fail("CUTOFF_AFTER_CYCLE_START")
    outcome_value = _choice(outcome, OUTCOMES, "OUTCOME_INVALID")
    reason = _code(reason_code, "REASON_CODE_INVALID")
    family = _optional_name(strategy_family, "STRATEGY_FAMILY_INVALID")
    configuration = _optional_hex64(configuration_sha256, "CONFIGURATION_SHA_INVALID")
    tier = _choice(cost_tier, COST_TIERS, "COST_TIER_INVALID")
    if outcome_value == "WINNER":
        if family is None or configuration is None:
            _fail("WINNER_WITHOUT_SELECTION")
        if tier == "NONE":
            _fail("WINNER_WITHOUT_COST_TIER")
    elif family is not None or configuration is not None:
        _fail("SELECTION_WITHOUT_WINNER")
    if tier == "NONE" and outcome_value not in ("STOP", "UNAVAILABLE"):
        _fail("COST_TIER_NONE_REQUIRES_STOP_OR_UNAVAILABLE")
    produced = (_utc(produced_at_utc, "PRODUCED_AT_INVALID") if produced_at_utc is not None
                else datetime.now(timezone.utc).replace(microsecond=0).isoformat())
    kind = _choice(source_kind, SOURCE_KINDS, "SOURCE_KIND_INVALID")
    job = _optional_hex64(job_binding_sha256, "JOB_BINDING_SHA_INVALID")
    consumer = _optional_hex64(consumer_binding_sha256, "CONSUMER_BINDING_SHA_INVALID")
    if kind in STOP_ONLY_SOURCE_KINDS and outcome_value in ("WINNER", "NO_TRADE"):
        _fail("CLOCK_DERIVED_RESULT_NOT_PERMITTED")
    # A derived view only exists inside a finishable job: a WINNER or NO_TRADE
    # result from one must name the job and consumer it was finished under.  A
    # STOP or UNAVAILABLE may have no job (the view never opened), and native
    # rows keep their nullability.
    if kind != "broker_native" and outcome_value in ("WINNER", "NO_TRADE") and (
            job is None or consumer is None):
        _fail("DERIVED_RESULT_WITHOUT_JOB_CONTEXT")
    # Honest outcomes carry only the facts that existed.  A result (WINNER or
    # NO_TRADE) needs the trade contract and a view pair.  A STOP may fall
    # before the source was seeded (no trade contract, no view), after the view
    # but before the trade seed (view pair, no trade contract), or later.  An
    # UNAVAILABLE row reached neither.  The view key and the retained view
    # contract always travel together.
    source_contract = _optional_hex64(source_contract_sha256, "SOURCE_CONTRACT_SHA_INVALID")
    view_key_value = _optional_hex64(view_key, "VIEW_KEY_INVALID")
    view_contract = _optional_hex64(view_contract_sha256, "VIEW_CONTRACT_SHA_INVALID")
    if (view_key_value is None) != (view_contract is None):
        _fail("VIEW_KEY_AND_CONTRACT_MUST_PAIR")
    result = outcome_value in ("WINNER", "NO_TRADE")
    receipt = _optional_hex64(no_trade_run_receipt_sha256, "NO_TRADE_RUN_RECEIPT_SHA_INVALID")
    if schema == END_GRID_SCHEMA_VERSION_V1:
        # The rules the v1 line was written with: a result needs the trade
        # contract, and the v2 field does not exist.
        if receipt is not None:
            _fail("NO_TRADE_RUN_RECEIPT_NOT_IN_V1")
        if result and source_contract is None:
            _fail("RESULT_WITHOUT_SOURCE_CONTRACT")
    elif outcome_value == "WINNER":
        if source_contract is None:
            _fail("RESULT_WITHOUT_SOURCE_CONTRACT")
        if receipt is not None:
            _fail("WINNER_WITH_NO_TRADE_RUN_RECEIPT")
    elif outcome_value == "NO_TRADE":
        # Evidenced by its own saved cell-run receipt; the trade contract is
        # present only when a verified trade source existed.
        if receipt is None:
            _fail("NO_TRADE_WITHOUT_RUN_RECEIPT")
    elif receipt is not None:
        _fail("NO_TRADE_RUN_RECEIPT_REQUIRES_NO_TRADE")
    if result and view_key_value is None:
        _fail("RESULT_WITHOUT_VIEW")
    if outcome_value == "UNAVAILABLE":
        if source_contract is not None:
            _fail("UNAVAILABLE_ROW_WITH_SOURCE_CONTRACT")
        if view_key_value is not None:
            _fail("UNAVAILABLE_ROW_WITH_VIEW")
    return EndGridRow(
        cell_id=cell_id, symbol=symbol, timeframe=timeframe, regime=regime,
        cycle_start_utc=start, cycle_end_utc=end, information_cutoff_utc=cutoff,
        outcome=outcome_value, reason_code=reason,
        strategy_family=family, configuration_sha256=configuration,
        source_kind=kind,
        source_contract_sha256=source_contract,
        no_trade_run_receipt_sha256=receipt,
        view_key=view_key_value,
        view_contract_sha256=view_contract,
        source_identity=_source_identity(dict(source_identity), kind, required=result),
        job_binding_sha256=job,
        consumer_binding_sha256=consumer,
        cost_tier=tier,
        process_edition_sha256=_hex64(process_edition_sha256, "PROCESS_EDITION_SHA_INVALID"),
        code_sha256=_code_identity(dict(code_sha256)),
        metrics=_metrics(dict(metrics or {})),
        produced_at_utc=produced,
        schema_version=schema,
    )


def source_kind_label(row: EndGridRow) -> str:
    """Human-facing label for reports; never part of the hashed row."""
    if type(row) is not EndGridRow:
        _fail("ROW_TYPE_INVALID")
    return SOURCE_KIND_LABELS[row.source_kind]


def render_row(row: EndGridRow) -> dict[str, Any]:
    """The hashed row plus display-only fields, for reports and the flow page."""
    out = row.to_dict()
    out["source_kind_label"] = source_kind_label(row)
    out["cost_lane_label"] = {"P75": "BASE", "P95": "STRESS", "NONE": "NONE"}[row.cost_tier]
    return out


def row_from_dict(value: Any) -> EndGridRow:
    """Rebuild and re-verify a stored row; the stored hash must match."""
    if type(value) is not dict or "schema_version" not in value:
        _fail("ROW_KEYS_INVALID")
    version = value["schema_version"]
    if version not in END_GRID_SCHEMA_VERSIONS:
        _fail("ROW_SCHEMA_VERSION_INVALID")
    expected = _ROW_KEYS_V1 if version == END_GRID_SCHEMA_VERSION_V1 else _ROW_KEYS
    if frozenset(value) != expected:
        _fail("ROW_KEYS_INVALID")
    for flag in AUTHORITY_FLAGS:
        if value[flag] is not False:
            _fail("AUTHORITY_FLAG_SET")
    row = build_end_grid_row(
        symbol=value["symbol"], timeframe=value["timeframe"], regime=value["regime"],
        cycle_start_utc=value["cycle_start_utc"], cycle_end_utc=value["cycle_end_utc"],
        information_cutoff_utc=value["information_cutoff_utc"],
        outcome=value["outcome"], reason_code=value["reason_code"],
        strategy_family=value["strategy_family"],
        configuration_sha256=value["configuration_sha256"],
        source_kind=value["source_kind"],
        source_contract_sha256=value["source_contract_sha256"],
        no_trade_run_receipt_sha256=value.get("no_trade_run_receipt_sha256"),
        schema_version=version,
        view_key=value["view_key"], view_contract_sha256=value["view_contract_sha256"],
        source_identity=value["source_identity"],
        job_binding_sha256=value["job_binding_sha256"],
        consumer_binding_sha256=value["consumer_binding_sha256"],
        cost_tier=value["cost_tier"],
        process_edition_sha256=value["process_edition_sha256"],
        code_sha256=value["code_sha256"], metrics=value["metrics"],
        produced_at_utc=value["produced_at_utc"],
    )
    if row.cell_id != value["cell_id"]:
        _fail("ROW_CELL_ID_MISMATCH")
    if row.row_sha256 != value["row_sha256"]:
        _fail("ROW_SHA_MISMATCH")
    return row


def _refuse_reparse(path: Path) -> None:
    """No symlinks or Windows reparse points anywhere on the output path."""
    for item in (path, *path.parents):
        try:
            identity = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(identity.st_mode) or getattr(identity, "st_file_attributes", 0) & 0x400:
            _fail("GRID_PATH_REPARSE_REFUSED")


def _grid_root(root: Any) -> Path:
    path = Path(root)
    if not path.is_absolute() or not path.is_dir():
        _fail("GRID_ROOT_MUST_BE_EXISTING_ABSOLUTE_DIRECTORY")
    _refuse_reparse(path)
    return path


def _symbol_file(root: Path, symbol: str) -> Path:
    if symbol not in book.PINNED_SYMBOL_ORDER:
        _fail("CELL_NOT_IN_FAMILY")
    return root / (symbol + ".jsonl")


def _contract_path(root: Path, sha: str) -> Path:
    return root / CONTRACTS_DIR / (_hex64(sha, "VIEW_CONTRACT_SHA_INVALID") + ".json")


def store_view_contract(root: Any, body: Mapping[str, Any]) -> str:
    """Retain a view reconstruction body under the grid root, content-addressed and write-once.

    The body is whatever the producer needs beyond code and canonical M5 to
    rebuild the view that produced a row: the requested interval, cutoff,
    stratum or clock axes, spread identification, fit and warmup windows.  It
    is stored as canonical bytes at contracts/<sha256>.json, so the row's
    view_contract_sha256 resolves here after every working spool is gone.
    It is distinct from source_contract_sha256, which keeps its existing
    meaning as the per-cell trade evidence contract.  A second store of the
    same sha with different bytes is refused.
    """
    base = _grid_root(root)
    if type(body) is not dict or not body:
        _fail("VIEW_CONTRACT_BODY_INVALID")
    try:
        raw = canonical_bytes(body)
    except (TypeError, ValueError):
        _fail("VIEW_CONTRACT_BODY_INVALID")
    if len(raw) > MAX_CONTRACT_BYTES:
        _fail("VIEW_CONTRACT_TOO_LARGE")
    sha = _sha256(raw)
    folder = base / CONTRACTS_DIR
    folder.mkdir(exist_ok=True)
    _refuse_reparse(folder)
    path = _contract_path(base, sha)
    if path.exists():
        _refuse_reparse(path)
        if path.read_bytes() != raw:
            _fail("VIEW_CONTRACT_SHA_COLLISION")
        return sha
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    return sha


def load_view_contract(root: Any, sha: Any) -> dict[str, Any]:
    """Read a retained view contract body back and prove it still matches its sha."""
    base = _grid_root(root)
    path = _contract_path(base, sha)
    if not path.is_file():
        _fail(f"CONTRACT_NOT_RETAINED:{sha}")
    _refuse_reparse(path)
    raw = path.read_bytes()
    if len(raw) > MAX_CONTRACT_BYTES or _sha256(raw) != sha:
        _fail(f"CONTRACT_SHA_MISMATCH:{sha}")
    value = json.loads(raw.decode("ascii"))
    if type(value) is not dict or canonical_bytes(value) != raw:
        _fail(f"CONTRACT_NOT_CANONICAL:{sha}")
    return value


def rebuild_inputs(root: Any, row: EndGridRow) -> dict[str, Any]:
    """Everything a rebuild needs, from the row and the retained contract only."""
    if type(row) is not EndGridRow:
        _fail("ROW_TYPE_INVALID")
    contract = (load_view_contract(root, row.view_contract_sha256)
                if row.view_contract_sha256 is not None else None)
    return {"identity": row.reproducibility_identity(), "view_contract": contract}


@contextlib.contextmanager
def _locked(lock_path: Path) -> Iterator[None]:
    """One kernel lock per symbol file, held across read, check, append and readback."""
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
        while True:
            try:
                if os.name == "nt":
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() > deadline:
                    _fail("GRID_LOCK_TIMEOUT")
                time.sleep(LOCK_RETRY_SECONDS)
        try:
            yield
        finally:
            if os.name == "nt":
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


class EndGridWriter:
    """Append rows to per-symbol JSONL files under a caller-owned root.

    The root is any existing directory the caller owns; it is never a working
    spool.  Every append is one locked transaction: the row is re-validated
    from its own bytes, the file is read and verified, conflicts and size are
    checked, the line is appended and fsynced, and the file is read back and
    verified again before success is reported.  An identical row appended twice
    is a no-op, so a retried finish step cannot duplicate a result.  The same
    cell, cycle and process edition with a different row is refused: a result
    is corrected by a new cycle record, never overwritten.
    """

    def __init__(self, root: Any):
        self.root = _grid_root(root)

    def append(self, row: EndGridRow) -> dict[str, Any]:
        if type(row) is not EndGridRow:
            _fail("ROW_TYPE_INVALID")
        if row.schema_version != END_GRID_SCHEMA_VERSION:
            _fail("ROW_SCHEMA_VERSION_NOT_CURRENT")
        # Re-validate from bytes: a frozen dataclass still holds mutable dicts,
        # and a flag flipped after construction must not reach the file.
        try:
            serialized = row.to_bytes()
        except (TypeError, ValueError):
            _fail("ROW_NOT_SERIALIZABLE")
        verified = row_from_dict(json.loads(serialized.decode("ascii")))
        raw = verified.to_bytes()
        if len(raw) > MAX_ROW_BYTES:
            _fail("ROW_TOO_LARGE")
        path = _symbol_file(self.root, verified.symbol)
        _refuse_reparse(path)
        # A row that had a view must resolve to a retained reconstruction body
        # here, not to a hash whose preimage lived in a spool.  Rows that never
        # opened a view carry only what was known.
        if verified.view_contract_sha256 is not None:
            load_view_contract(self.root, verified.view_contract_sha256)
        with _locked(path.with_name(path.name + ".lock")):
            existing = read_end_grid(self.root, verified.symbol) if path.exists() else ()
            for prior in existing:
                if prior.row_sha256 == verified.row_sha256:
                    return {"path": str(path), "row_sha256": verified.row_sha256, "appended": False}
                if (prior.cell_id, prior.cycle_start_utc, prior.process_edition_sha256) == (
                        verified.cell_id, verified.cycle_start_utc, verified.process_edition_sha256):
                    _fail("CELL_CYCLE_ALREADY_RECORDED")
            before = path.stat().st_size if path.exists() else 0
            if before + len(raw) + 1 > MAX_FILE_BYTES:
                _fail("GRID_FILE_TOO_LARGE")
            with path.open("ab") as handle:
                handle.write(raw + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            after = path.read_bytes()
            if len(after) != before + len(raw) + 1 or not after.endswith(raw + b"\n"):
                _fail("GRID_READBACK_MISMATCH")
            rows = read_end_grid(self.root, verified.symbol)
            if len(rows) != len(existing) + 1 or rows[-1] != verified:
                _fail("GRID_READBACK_MISMATCH")
        return {"path": str(path), "row_sha256": verified.row_sha256, "appended": True,
                "rows_in_file": len(rows)}


def read_end_grid(root: Any, symbol: str | None = None) -> tuple[EndGridRow, ...]:
    """Read and re-verify every row; one bad line fails the whole read."""
    base = _grid_root(root)
    if symbol is not None:
        paths = [_symbol_file(base, symbol)]
    else:
        paths = sorted(item for item in base.glob("*.jsonl") if item.stem in book.PINNED_SYMBOL_ORDER)
    rows: list[EndGridRow] = []
    for path in paths:
        if not path.exists():
            continue
        _refuse_reparse(path)
        if path.stat().st_size > MAX_FILE_BYTES:
            _fail("GRID_FILE_TOO_LARGE")
        raw = path.read_bytes()
        if raw and not raw.endswith(b"\n"):
            _fail(f"GRID_PARTIAL_TAIL:{path.name}")
        for number, line in enumerate(raw.split(b"\n")[:-1], 1):
            if not line:
                _fail(f"GRID_BLANK_LINE:{path.name}:{number}")
            if len(line) > MAX_ROW_BYTES:
                _fail(f"ROW_TOO_LARGE:{path.name}:{number}")
            try:
                value = json.loads(line.decode("ascii"))
            except (UnicodeDecodeError, ValueError):
                _fail(f"ROW_NOT_JSON:{path.name}:{number}")
            row = row_from_dict(value)
            if row.symbol != path.stem:
                _fail(f"ROW_SYMBOL_FILE_MISMATCH:{path.name}:{number}")
            rows.append(row)
    return tuple(rows)


def latest_rows_by_cell(rows: Iterable[EndGridRow]) -> dict[str, EndGridRow]:
    """The newest cycle per cell, ties broken by production time, then hash."""
    latest: dict[str, EndGridRow] = {}
    for row in rows:
        current = latest.get(row.cell_id)
        key = (row.cycle_start_utc, row.produced_at_utc, row.row_sha256)
        if current is None or key > (current.cycle_start_utc, current.produced_at_utc,
                                     current.row_sha256):
            latest[row.cell_id] = row
    return latest


def grid_summary(rows: Iterable[EndGridRow]) -> dict[str, Any]:
    """Counts by outcome for the latest row of each cell; a description, not a verdict."""
    latest = latest_rows_by_cell(rows)
    counts = {outcome: 0 for outcome in OUTCOMES}
    for row in latest.values():
        counts[row.outcome] += 1
    return {"cells_recorded": len(latest), "family_cells": len(book.PINNED_SYMBOL_ORDER)
            * len(book.TIMEFRAME_ORDER) * len(book.REGIME_ORDER), "by_outcome": counts}


__all__ = [
    "END_GRID_SCHEMA_VERSION", "END_GRID_SCHEMA_VERSION_V1", "END_GRID_SCHEMA_VERSIONS",
    "OUTCOMES", "SOURCE_KINDS", "SOURCE_KIND_LABELS",
    "STOP_ONLY_SOURCE_KINDS", "COST_TIERS", "AUTHORITY_FLAGS",
    "SOURCE_IDENTITY_REQUIRED", "CONTRACTS_DIR", "MAX_CONTRACT_BYTES",
    "EndGridError", "EndGridRow", "EndGridWriter", "build_end_grid_row",
    "row_from_dict", "read_end_grid", "latest_rows_by_cell", "grid_summary",
    "source_kind_label", "render_row",
    "store_view_contract", "load_view_contract", "rebuild_inputs",
    "canonical_bytes",
]
