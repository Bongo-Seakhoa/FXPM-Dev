"""Historical, anchor-local training-horizon planning and evidence reports.

This module deliberately owns only mechanical planning and exact evidence
collection for one symbol/timeframe/regime cell at one historical anchor.  It
does not choose a strategy or horizon, make an economic decision, or confer
final out-of-sample authority.  Those decisions belong to downstream frozen
authorities.  In the declared isolated single-user runtime, the deterministic
local objects and replay check are valid offline structural evidence, while
remaining insufficient by themselves for trading or capital authority.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


__all__ = (
    "build_historical_horizon_plan",
    "evaluate_historical_horizon_plan",
    "verify_historical_horizon_evidence",
)


_REQUEST_SCHEMA = "fxpm.historical-horizon-request.v1"
_UNOBSERVED_REQUEST_SCHEMA = "fxpm.historical-horizon-unobserved-request.v1"
_PLAN_SCHEMA = "fxpm.historical-horizon-plan.v1"
_EVIDENCE_SCHEMA = "fxpm.historical-horizon-evidence.v1"
_SHARD_SCHEMA = "fxpm.historical-horizon-evidence-shard.v1"
_DEPENDENCY_SCHEMA = "fxpm.historical-dependency-policy.v1"
_DEPENDENCY_PAYLOAD_SCHEMA = "fxpm.historical-dependency-policy-payload.v1"
_HORIZON_POLICY_SCHEMA = "fxpm.historical-horizon-policy.v1"
_REGIME_SUPPORT_BINDING_SCHEMA = "fxpm.historical-horizon-regime-support-binding.v1"
_REGIME_TRAIN_SUPPORT_SCHEMA = "fxpm.regime-train-support/v1"
_REGIME_TRAIN_SUPPORT_VECTOR_ENCODING = "fxpm.regime-live-labels-nul/v1"
_REGIME_TRAIN_SUPPORT_WARMUP_POLICY = "CURRENT_AND_SOURCE_BAR_NOT_WARMUP"
_AUTHORITY_SCOPE = "HISTORICAL_ANCHOR_LOCAL_HORIZON_EVIDENCE_ONLY"
_TRUST_CLASS = "LOCAL_SINGLE_USER_DETERMINISTIC_OFFLINE_EVIDENCE"

_TIMEFRAME_SECONDS = {
    "M5": 300,
    "M15": 900,
    "M30": 1_800,
    "H1": 3_600,
    "H4": 14_400,
    "D1": 86_400,
}
_REGIME_ORDER = ("TREND", "RANGE", "BREAKOUT", "CHOP")
_REGIMES = frozenset(_REGIME_ORDER)
_LOOKBACK_KINDS = frozenset(
    ("NATIVE_OBSERVATIONS", "ACTIVE_SESSIONS", "CALENDAR_INTERVAL")
)
_CAUSAL_ROLE = "CAUSAL_ROLLING_ORIGIN_INNER"
_INTERPOLATION_ROLE = "INTERPOLATION_ONLY"
_TERMINAL_ROLE = "TERMINAL_MECHANICAL_REJECTION"
_FOLD_ROLES = frozenset((_CAUSAL_ROLE, _INTERPOLATION_ROLE))

_AVAILABLE = "AVAILABLE"
_SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
_TECHNICAL_FAILURE = "TECHNICAL_FAILURE"
_TECHNICAL_TERMINAL = "TECHNICAL_TERMINAL"
_SOURCE_STATES = frozenset(
    (_AVAILABLE, _SOURCE_UNAVAILABLE, _TECHNICAL_FAILURE, _TECHNICAL_TERMINAL)
)
_INTERNAL_STATES = frozenset(
    ("CONTINUOUS", "EXPECTED_AUTHENTICATED_DISCONTINUITIES", "UNEXPLAINED")
)
_BOUNDARY_STATES = frozenset(("COMPLETE", "LEFT_TRUNCATED", "RIGHT_TRUNCATED"))

_AUTHENTICATED = "AUTHENTICATED"
_PROCESS_REJECTED = "PROCESS_REJECTED"
_TECHNICAL_SHARD = "TECHNICAL_FAILURE"
_TECHNICAL_REASONS = frozenset(
    (
        "EVALUATOR_OOM",
        "SOURCE_INTEGRITY_FAILURE",
        "EVALUATOR_EXCEPTION",
    )
)
_PROCESS_SCREEN_REASON = "PREREGISTERED_PROCESS_SCREEN_REJECTED"
_BOUNDARY_REASON = "SOURCE_BOUNDARY_SHORTFALL"
_STATE_CONTEXT_BOUNDARY_REASON = "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
_STATE_CONTEXT_DISPOSITION_SCHEMA = (
    "fxpm.production-trial-state-context-disposition.v1"
)

# These are transport/resource ceilings, never statistical acceptance rules.
_MAX_REQUEST_BYTES = 32 * 1024 * 1024
_MAX_EVIDENCE_BYTES = 64 * 1024 * 1024
_MAX_OUTPUT_BYTES = 96 * 1024 * 1024
_MAX_NODES = 2_000_000
_MAX_DEPTH = 48
_MAX_CONTAINER_ITEMS = 250_000
_MAX_STRING_BYTES = 1_000_000
_MAX_JSON_INTEGER_BITS = 128
_MAX_CANDIDATES = 512
_MAX_TRIALS = 8_192
_MAX_FOLDS_PER_CANDIDATE = 256
_MAX_ATTEMPTS = 100_000
_MAX_EXPECTED_EVALUATIONS = 100_000
_MAX_VECTOR_POINTS = 2_000_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_SYMBOL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$", re.ASCII)
_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", re.ASCII
)
_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", re.ASCII)
_DECIMAL_RE = re.compile(
    r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$",
    re.ASCII,
)

_PLAN_SEAL = object()
_EVIDENCE_SEAL = object()


class _HorizonContractError(ValueError):
    """Fail-closed contract violation internal to this authority."""


def _fail(message: str) -> None:
    raise _HorizonContractError(message)


def _require(condition: bool, message: str) -> None:
    if not condition:
        _fail(message)


def _bounded_snapshot(value: Any, max_bytes: int) -> Any:
    """Copy an exact JSON value while enforcing deterministic hard bounds."""

    _require(type(max_bytes) is int and max_bytes > 0, "invalid byte ceiling")
    nodes = 0
    estimated_bytes = 0
    active: set[int] = set()

    def charge(amount: int) -> None:
        nonlocal estimated_bytes
        estimated_bytes += amount
        _require(estimated_bytes <= max_bytes, "input byte ceiling exceeded")

    def walk(item: Any, depth: int) -> Any:
        nonlocal nodes
        nodes += 1
        _require(nodes <= _MAX_NODES, "input node ceiling exceeded")
        _require(depth <= _MAX_DEPTH, "input depth ceiling exceeded")
        item_type = type(item)
        if item is None:
            charge(4)
            return item
        if item_type is bool:
            charge(5)
            return item
        if item_type is int:
            _require(
                item.bit_length() <= _MAX_JSON_INTEGER_BITS,
                "integer bit ceiling exceeded",
            )
            charge(len(str(item)) + 1)
            return item
        if item_type is str:
            encoded_length = len(item.encode("utf-8"))
            _require(encoded_length <= _MAX_STRING_BYTES, "string ceiling exceeded")
            charge(encoded_length * 6 + 2)
            return item
        _require(item_type in (dict, list), "only exact JSON values are accepted")
        identity = id(item)
        _require(identity not in active, "cyclic input is forbidden")
        active.add(identity)
        try:
            if item_type is list:
                _require(len(item) <= _MAX_CONTAINER_ITEMS, "list ceiling exceeded")
                charge(len(item) + 2)
                return [walk(child, depth + 1) for child in item]
            _require(len(item) <= _MAX_CONTAINER_ITEMS, "mapping ceiling exceeded")
            charge(len(item) + 2)
            copied: dict[str, Any] = {}
            for key, child in item.items():
                _require(type(key) is str, "mapping keys must be exact strings")
                _require(key not in copied, "duplicate mapping key")
                key_length = len(key.encode("utf-8"))
                _require(
                    key_length <= _MAX_STRING_BYTES,
                    "mapping key ceiling exceeded",
                )
                charge(key_length * 6 + 3)
                copied[key] = walk(child, depth + 1)
            return copied
        finally:
            active.remove(identity)

    copied = walk(value, 0)
    encoded = _canonical_bytes(copied)
    _require(len(encoded) <= max_bytes, "input byte ceiling exceeded")
    return copied


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise _HorizonContractError("value is not canonical JSON") from exc


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_bytes(value))


def _exact_keys(value: Any, keys: tuple[str, ...], label: str) -> dict[str, Any]:
    _require(type(value) is dict, f"{label} must be an exact mapping")
    expected = set(keys)
    actual = set(value)
    _require(actual == expected, f"{label} keys differ: expected {sorted(expected)}")
    return value


def _safe_id(value: Any, label: str) -> str:
    _require(
        type(value) is str and _SAFE_ID_RE.fullmatch(value) is not None,
        f"invalid {label}",
    )
    return value


def _digest(value: Any, label: str) -> str:
    _require(
        type(value) is str and _SHA256_RE.fullmatch(value) is not None,
        f"invalid {label}",
    )
    return value


def _integer(value: Any, label: str, lower: int, upper: int) -> int:
    _require(type(value) is int and lower <= value <= upper, f"invalid {label}")
    return value


def _text_or_none(value: Any, label: str) -> str | None:
    if value is None:
        return None
    _require(
        type(value) is str and 0 < len(value.encode("utf-8")) <= 512, f"invalid {label}"
    )
    return value


def _utc(value: Any, label: str) -> datetime:
    _require(
        type(value) is str and _UTC_RE.fullmatch(value) is not None, f"invalid {label}"
    )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise _HorizonContractError(f"invalid {label}") from exc
    _require(parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value, f"non-canonical {label}")
    return parsed


def _date(value: Any, label: str) -> datetime:
    _require(
        type(value) is str and _DATE_RE.fullmatch(value) is not None, f"invalid {label}"
    )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise _HorizonContractError(f"invalid {label}") from exc
    _require(parsed.strftime("%Y-%m-%d") == value, f"non-canonical {label}")
    return parsed


def _decimal_text(value: Any, label: str) -> str:
    _require(type(value) is str and 0 < len(value) <= 128, f"invalid {label}")
    _require(_DECIMAL_RE.fullmatch(value) is not None, f"non-canonical {label}")
    _require(value != "-0", f"non-canonical {label}")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise _HorizonContractError(f"invalid {label}") from exc
    _require(parsed.is_finite(), f"non-finite {label}")
    canonical = format(parsed, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if canonical == "-0":
        canonical = "0"
    _require(value == canonical, f"non-canonical {label}")
    return value


class _HistoricalHorizonPlan:
    __slots__ = ("_body_json", "_receipt_sha256", "_seal", "_locked")

    def __init__(self, body: dict[str, Any], seal: object) -> None:
        _require(seal is _PLAN_SEAL, "invalid plan constructor authority")
        body_json = _canonical_bytes(body)
        object.__setattr__(self, "_body_json", body_json)
        object.__setattr__(self, "_receipt_sha256", _sha256_bytes(body_json))
        object.__setattr__(self, "_seal", seal)
        object.__setattr__(self, "_locked", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_locked", False):
            raise AttributeError("sealed historical horizon plan")
        object.__setattr__(self, name, value)

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))


class _HistoricalHorizonEvidence:
    __slots__ = ("_body_json", "_receipt_sha256", "_seal", "_locked")

    def __init__(self, body: dict[str, Any], seal: object) -> None:
        _require(seal is _EVIDENCE_SEAL, "invalid evidence constructor authority")
        body_json = _canonical_bytes(body)
        object.__setattr__(self, "_body_json", body_json)
        object.__setattr__(self, "_receipt_sha256", _sha256_bytes(body_json))
        object.__setattr__(self, "_seal", seal)
        object.__setattr__(self, "_locked", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_locked", False):
            raise AttributeError("sealed historical horizon evidence")
        object.__setattr__(self, name, value)

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))


def _valid_plan(plan: Any) -> bool:
    if type(plan) is not _HistoricalHorizonPlan:
        return False
    try:
        if plan._seal is not _PLAN_SEAL:
            return False
        body = json.loads(plan._body_json.decode("utf-8"))
        if type(body) is not dict or body.get("schema_version") != _PLAN_SCHEMA:
            return False
        return (
            plan._receipt_sha256 == _sha256_bytes(plan._body_json) == _sha256_json(body)
        )
    except (AttributeError, TypeError, ValueError, UnicodeError):
        return False


def _valid_evidence(evidence: Any) -> bool:
    if type(evidence) is not _HistoricalHorizonEvidence:
        return False
    try:
        if evidence._seal is not _EVIDENCE_SEAL:
            return False
        body = json.loads(evidence._body_json.decode("utf-8"))
        if type(body) is not dict or body.get("schema_version") != _EVIDENCE_SCHEMA:
            return False
        return (
            evidence._receipt_sha256
            == _sha256_bytes(evidence._body_json)
            == _sha256_json(body)
        )
    except (AttributeError, TypeError, ValueError, UnicodeError):
        return False


def _normalize_anchor(value: Any) -> dict[str, Any]:
    anchor = _exact_keys(
        value,
        (
            "anchor_id",
            "ordinal",
            "selection_cutoff_utc",
            "forward_start_utc",
            "forward_end_exclusive_utc",
            "evidence_accessed_at_utc",
            "process_precommit_receipt_sha256",
        ),
        "anchor",
    )
    _safe_id(anchor["anchor_id"], "anchor_id")
    _integer(anchor["ordinal"], "anchor ordinal", 0, 1_000_000)
    cutoff = _utc(anchor["selection_cutoff_utc"], "selection cutoff")
    forward_start = _utc(anchor["forward_start_utc"], "forward start")
    forward_end = _utc(anchor["forward_end_exclusive_utc"], "forward end")
    _utc(anchor["evidence_accessed_at_utc"], "evidence accessed at")
    _digest(anchor["process_precommit_receipt_sha256"], "process precommit receipt")
    _require(
        cutoff < forward_start < forward_end,
        "anchor intervals are not strictly ordered",
    )
    return anchor


def _normalize_cell(value: Any) -> dict[str, Any]:
    cell = _exact_keys(value, ("symbol", "timeframe", "regime"), "cell")
    _require(
        type(cell["symbol"]) is str
        and _SYMBOL_RE.fullmatch(cell["symbol"]) is not None,
        "invalid symbol",
    )
    _require(cell["timeframe"] in _TIMEFRAME_SECONDS, "unsupported timeframe")
    _require(cell["regime"] in _REGIMES, "unsupported regime")
    return cell


def _normalize_dependency_policy(value: Any) -> tuple[dict[str, Any], dict[str, int]]:
    wrapper = _exact_keys(
        value,
        (
            "schema_version",
            "authority_key_id",
            "signed_policy_receipt_sha256",
            "payload_sha256",
            "payload",
        ),
        "dependency policy",
    )
    _require(
        wrapper["schema_version"] == _DEPENDENCY_SCHEMA,
        "dependency policy schema mismatch",
    )
    _safe_id(wrapper["authority_key_id"], "dependency authority key id")
    _digest(wrapper["signed_policy_receipt_sha256"], "signed dependency policy receipt")
    _digest(wrapper["payload_sha256"], "dependency payload digest")
    payload = _exact_keys(
        wrapper["payload"],
        (
            "schema_version",
            "feature_lookback_bars",
            "label_lookahead_bars",
            "max_holding_bars",
            "execution_delay_bars",
            "regime_warmup_bars",
            "cost_lookback_bars",
            "max_holding_seconds",
        ),
        "dependency payload",
    )
    _require(
        payload["schema_version"] == _DEPENDENCY_PAYLOAD_SCHEMA,
        "dependency payload schema mismatch",
    )
    for key in (
        "feature_lookback_bars",
        "label_lookahead_bars",
        "max_holding_bars",
        "execution_delay_bars",
        "regime_warmup_bars",
        "cost_lookback_bars",
    ):
        _integer(payload[key], key, 0, 10_000_000)
    _integer(payload["max_holding_seconds"], "max_holding_seconds", 0, 10_000_000_000)
    _require(
        wrapper["payload_sha256"] == _sha256_json(payload),
        "dependency payload digest mismatch",
    )
    derived_bars = {
        "warmup_bars": max(
            payload["feature_lookback_bars"],
            payload["regime_warmup_bars"],
            payload["cost_lookback_bars"],
        ),
        "left_purge_bars": max(
            payload["label_lookahead_bars"],
            payload["execution_delay_bars"] + payload["max_holding_bars"],
        ),
        "right_purge_bars": max(
            payload["feature_lookback_bars"],
            payload["label_lookahead_bars"],
            payload["execution_delay_bars"] + payload["max_holding_bars"],
            payload["regime_warmup_bars"],
            payload["cost_lookback_bars"],
        ),
    }
    return wrapper, derived_bars


def _normalize_horizon_policy(value: Any) -> dict[str, Any]:
    policy = _exact_keys(
        value,
        (
            "schema_version",
            "policy_id",
            "policy_release_sha256",
            "cost_contract_sha256",
            "cost_stress_contract_sha256",
            "evaluator_release_sha256",
        ),
        "horizon policy",
    )
    _require(
        policy["schema_version"] == _HORIZON_POLICY_SCHEMA,
        "horizon policy schema mismatch",
    )
    _safe_id(policy["policy_id"], "horizon policy id")
    for key in (
        "policy_release_sha256",
        "cost_contract_sha256",
        "cost_stress_contract_sha256",
        "evaluator_release_sha256",
    ):
        _digest(policy[key], key)
    return policy


def _normalize_regime_support_binding(
    value: Any,
    cell: dict[str, Any],
    prefix: dict[str, Any],
) -> dict[str, Any]:
    """Verify one exact, receipt-bound zero-support regime fact.

    The binding deliberately carries the complete train-support receipt body,
    rather than a caller-supplied ``observed`` flag.  Its source identities are
    tied to the already-normalized native prefix.  The enclosing regime-fit and
    detector receipts let the production caller compare this plan with the
    independently verified shared fit object before Book assembly.
    """

    binding = _exact_keys(
        value,
        (
            "schema_version",
            "source_group_decision_sha256",
            "regime_fit_decision_sha256",
            "regime_detector_record_sha256",
            "train_regime_support_receipt_sha256",
            "target_regime",
            "target_regime_count",
            "train_regime_support",
            "binding_sha256",
        ),
        "regime support binding",
    )
    _require(
        binding["schema_version"] == _REGIME_SUPPORT_BINDING_SCHEMA,
        "regime support binding schema mismatch",
    )
    for name in (
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "regime_detector_record_sha256",
        "train_regime_support_receipt_sha256",
        "binding_sha256",
    ):
        _digest(binding[name], name)
    _require(
        binding["target_regime"] == cell["regime"],
        "regime support target mismatch",
    )
    _integer(binding["target_regime_count"], "target regime count", 0, 10_000_000_000)

    support = _exact_keys(
        binding["train_regime_support"],
        (
            "schema_version",
            "symbol",
            "timeframe",
            "surface",
            "warmup_policy",
            "label_order",
            "eligible_rows",
            "counts",
            "observed_regimes",
            "unobserved_regimes",
            "support_state_by_regime",
            "first_ts",
            "last_ts",
            "vector_encoding",
            "vector_sha256",
            "bindings",
            "receipt_sha256",
        ),
        "train regime support",
    )
    _require(
        support["schema_version"] == _REGIME_TRAIN_SUPPORT_SCHEMA,
        "train regime support schema mismatch",
    )
    _require(
        support["symbol"] == cell["symbol"]
        and support["timeframe"] == cell["timeframe"],
        "train regime support cell mismatch",
    )
    _require(support["surface"] == "REGIME_LIVE", "regime support surface mismatch")
    _require(
        support["warmup_policy"] == _REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "regime support warmup policy mismatch",
    )
    _require(
        support["label_order"] == list(_REGIME_ORDER),
        "regime support label order mismatch",
    )
    eligible_rows = _integer(
        support["eligible_rows"], "regime support eligible rows", 0, 10_000_000_000
    )
    _require(
        eligible_rows <= prefix["prefix_rows"],
        "regime support rows exceed native prefix backing",
    )
    counts = _exact_keys(support["counts"], _REGIME_ORDER, "regime support counts")
    normalized_counts = {
        regime: _integer(
            counts[regime], f"{regime} regime support count", 0, 10_000_000_000
        )
        for regime in _REGIME_ORDER
    }
    _require(
        sum(normalized_counts.values()) == eligible_rows,
        "regime support row accounting mismatch",
    )
    observed = [regime for regime in _REGIME_ORDER if normalized_counts[regime] > 0]
    unobserved = [regime for regime in _REGIME_ORDER if normalized_counts[regime] == 0]
    states = {
        regime: (
            "EMPIRICALLY_OBSERVED" if normalized_counts[regime] > 0 else "UNOBSERVED"
        )
        for regime in _REGIME_ORDER
    }
    _require(
        support["observed_regimes"] == observed
        and support["unobserved_regimes"] == unobserved
        and support["support_state_by_regime"] == states,
        "regime support observed/unobserved partition mismatch",
    )
    target = cell["regime"]
    _require(
        binding["target_regime_count"] == normalized_counts[target],
        "target regime count does not match support receipt",
    )
    _require(
        normalized_counts[target] == 0 and states[target] == "UNOBSERVED",
        "observed regime cannot use the zero-attempt horizon path",
    )

    if eligible_rows == 0:
        _require(
            support["first_ts"] is None and support["last_ts"] is None,
            "empty regime support must not claim a label time range",
        )
    else:
        for name in ("first_ts", "last_ts"):
            _require(
                type(support[name]) is str and 0 < len(support[name]) <= 128,
                f"invalid regime support {name}",
            )
        try:
            first_ts = datetime.fromisoformat(
                support["first_ts"].replace("Z", "+00:00")
            )
            last_ts = datetime.fromisoformat(
                support["last_ts"].replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise _HorizonContractError("invalid regime support time range") from exc
        _require(
            first_ts.tzinfo is not None
            and first_ts.utcoffset() is not None
            and last_ts.tzinfo is not None
            and last_ts.utcoffset() is not None
            and first_ts <= last_ts,
            "invalid regime support time range",
        )
        prefix_start = _utc(prefix["prefix_start_utc"], "prefix start")
        prefix_end = _utc(prefix["prefix_end_exclusive_utc"], "prefix end")
        _require(
            prefix_start
            <= first_ts.astimezone(timezone.utc)
            <= last_ts.astimezone(timezone.utc)
            < prefix_end,
            "regime support time range lies outside native prefix",
        )
    _require(
        support["vector_encoding"] == _REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "regime support vector encoding mismatch",
    )
    _digest(support["vector_sha256"], "regime support vector")
    source = _exact_keys(
        support["bindings"],
        (
            "symbol",
            "timeframe",
            "activation_id",
            "activation_manifest_sha256",
            "source_sha256",
            "train_window_sha256",
            "params_sha256",
            "detector_code_sha256",
        ),
        "regime support source bindings",
    )
    _require(
        source["symbol"] == cell["symbol"]
        and source["timeframe"] == cell["timeframe"]
        and source["activation_id"] == prefix["activation_id"]
        and source["activation_manifest_sha256"] == prefix["activation_manifest_sha256"]
        and source["source_sha256"] == prefix["artifact_sha256"],
        "regime support native source binding mismatch",
    )
    for name in (
        "activation_manifest_sha256",
        "source_sha256",
        "train_window_sha256",
        "params_sha256",
        "detector_code_sha256",
    ):
        _digest(source[name], f"regime support {name}")
    _require(
        support["receipt_sha256"]
        == _sha256_json(
            {name: support[name] for name in support if name != "receipt_sha256"}
        ),
        "train regime support receipt mismatch",
    )
    _require(
        binding["train_regime_support_receipt_sha256"] == support["receipt_sha256"],
        "regime support receipt binding mismatch",
    )
    _require(
        binding["binding_sha256"]
        == _sha256_json(
            {name: binding[name] for name in binding if name != "binding_sha256"}
        ),
        "regime support binding digest mismatch",
    )
    return binding


def _normalize_native_prefix(
    value: Any,
    cell: dict[str, Any],
    anchor: dict[str, Any],
) -> dict[str, Any]:
    prefix = _exact_keys(
        value,
        (
            "source_state",
            "source_reason",
            "symbol",
            "timeframe",
            "activation_id",
            "activation_manifest_sha256",
            "activation_entry_sha256",
            "artifact_sha256",
            "provenance_sha256",
            "request_map_sha256",
            "source_identity_sha256",
            "continuity_policy_sha256",
            "continuity_receipt_sha256",
            "continuity_geometry_sha256",
            "source_terminal_receipt_sha256",
            "source_accessed_at_utc",
            "request_start_utc",
            "request_end_exclusive_utc",
            "prefix_start_utc",
            "prefix_end_exclusive_utc",
            "prefix_first_open_utc",
            "prefix_last_open_utc",
            "prefix_last_close_utc",
            "artifact_rows",
            "prefix_rows",
            "prefix_schema_sha256",
            "prefix_index_sha256",
            "prefix_content_sha256",
            "internal_status",
            "boundary_status",
            "unexplained_discontinuity_count",
            "segments_sha256",
            "requires_preserved_segment_ledger",
        ),
        "native prefix",
    )
    _require(prefix["source_state"] in _SOURCE_STATES, "invalid source state")
    _text_or_none(prefix["source_reason"], "source reason")
    _require(prefix["symbol"] == cell["symbol"], "native prefix symbol mismatch")
    _require(
        prefix["timeframe"] == cell["timeframe"], "native prefix timeframe mismatch"
    )
    _safe_id(prefix["activation_id"], "activation id")
    _digest(prefix["activation_manifest_sha256"], "activation manifest")
    _digest(prefix["activation_entry_sha256"], "activation entry")
    _digest(prefix["request_map_sha256"], "request map")
    _digest(prefix["source_identity_sha256"], "source identity")
    _digest(prefix["continuity_policy_sha256"], "continuity policy")
    _digest(prefix["continuity_receipt_sha256"], "continuity receipt")
    _digest(prefix["continuity_geometry_sha256"], "continuity geometry")
    _digest(prefix["segments_sha256"], "segments")
    source_accessed = _utc(prefix["source_accessed_at_utc"], "source accessed at")
    evidence_accessed = _utc(
        anchor["evidence_accessed_at_utc"], "anchor evidence accessed at"
    )
    _require(
        source_accessed <= evidence_accessed,
        "source access clock follows evidence access clock",
    )
    request_start = _utc(prefix["request_start_utc"], "request start")
    request_end = _utc(prefix["request_end_exclusive_utc"], "request end")
    _require(request_start < request_end, "source request interval is empty")
    _require(
        prefix["internal_status"] in _INTERNAL_STATES,
        "invalid internal continuity state",
    )
    _require(prefix["boundary_status"] in _BOUNDARY_STATES, "invalid boundary state")
    unexplained = _integer(
        prefix["unexplained_discontinuity_count"],
        "unexplained discontinuity count",
        0,
        1_000_000_000,
    )
    _require(
        type(prefix["requires_preserved_segment_ledger"]) is bool,
        "segment-ledger requirement must be boolean",
    )
    if prefix["internal_status"] == "EXPECTED_AUTHENTICATED_DISCONTINUITIES":
        _require(
            prefix["requires_preserved_segment_ledger"] is True,
            "authenticated discontinuities require a preserved segment ledger",
        )
    if prefix["internal_status"] == "UNEXPLAINED":
        _require(unexplained > 0, "unexplained state requires a positive count")
    else:
        _require(
            unexplained == 0,
            "authenticated continuity state cannot carry unexplained gaps",
        )

    artifact_fields = (
        "artifact_sha256",
        "provenance_sha256",
        "prefix_schema_sha256",
        "prefix_index_sha256",
        "prefix_content_sha256",
    )
    time_fields = (
        "prefix_start_utc",
        "prefix_end_exclusive_utc",
        "prefix_first_open_utc",
        "prefix_last_open_utc",
        "prefix_last_close_utc",
    )
    if prefix["source_state"] == _AVAILABLE:
        _require(
            prefix["source_reason"] is None,
            "available source cannot carry a failure reason",
        )
        _require(
            prefix["source_terminal_receipt_sha256"] is None,
            "available source cannot be terminal",
        )
        for key in artifact_fields:
            _digest(prefix[key], key)
        prefix_start = _utc(prefix["prefix_start_utc"], "prefix start")
        prefix_end = _utc(prefix["prefix_end_exclusive_utc"], "prefix end")
        first_open = _utc(prefix["prefix_first_open_utc"], "prefix first open")
        last_open = _utc(prefix["prefix_last_open_utc"], "prefix last open")
        last_close = _utc(prefix["prefix_last_close_utc"], "prefix last close")
        artifact_rows = _integer(
            prefix["artifact_rows"], "artifact rows", 1, 10_000_000_000
        )
        prefix_rows = _integer(prefix["prefix_rows"], "prefix rows", 1, 10_000_000_000)
        timeframe_seconds = _TIMEFRAME_SECONDS[cell["timeframe"]]
        request_capacity = int(
            (request_end - request_start).total_seconds() // timeframe_seconds
        )
        prefix_capacity = int(
            (prefix_end - prefix_start).total_seconds() // timeframe_seconds
        )
        _require(
            artifact_rows <= request_capacity,
            "artifact rows exceed native timeframe request capacity",
        )
        _require(
            prefix_rows <= prefix_capacity,
            "prefix rows exceed native timeframe prefix capacity",
        )
        _require(
            artifact_rows >= prefix_rows, "prefix rows exceed activated artifact rows"
        )
        _require(
            request_start <= prefix_start < prefix_end <= request_end,
            "prefix lies outside source request",
        )
        _require(
            prefix_start <= first_open <= last_open < prefix_end,
            "prefix open bounds are inconsistent",
        )
        _require(
            last_open < last_close <= prefix_end, "prefix close bounds are inconsistent"
        )
        cutoff = _utc(anchor["selection_cutoff_utc"], "selection cutoff")
        _require(
            prefix_end <= cutoff, "native prefix leaks beyond historical anchor cutoff"
        )
        _require(
            last_close <= cutoff,
            "native bar close leaks beyond historical anchor cutoff",
        )
    else:
        _require(
            prefix["source_reason"] is not None, "unavailable source requires a reason"
        )
        _digest(prefix["source_terminal_receipt_sha256"], "source terminal receipt")
        for key in artifact_fields + time_fields + ("artifact_rows", "prefix_rows"):
            _require(prefix[key] is None, f"terminal source field {key} must be null")
    return prefix


def _derived_geometry(
    timeframe: str,
    derived_bars: dict[str, int],
    dependency_payload: dict[str, Any],
) -> dict[str, int]:
    seconds = _TIMEFRAME_SECONDS[timeframe]
    max_holding_seconds = dependency_payload["max_holding_seconds"]
    delayed_holding_seconds = dependency_payload[
        "execution_delay_bars"
    ] * seconds + max(
        dependency_payload["max_holding_bars"] * seconds,
        max_holding_seconds,
    )
    context = {
        **derived_bars,
        "timeframe_seconds": seconds,
        "warmup_seconds": derived_bars["warmup_bars"] * seconds,
        "left_purge_seconds": max(
            dependency_payload["label_lookahead_bars"] * seconds,
            delayed_holding_seconds,
        ),
        "right_purge_seconds": max(
            derived_bars["right_purge_bars"] * seconds,
            delayed_holding_seconds,
        ),
    }
    return context


def _normalize_fold(
    value: Any,
    candidate_start: datetime,
    prefix_start: datetime,
    prefix_end: datetime,
    cutoff: datetime,
    geometry: dict[str, int],
) -> dict[str, Any]:
    fold = _exact_keys(
        value,
        (
            "fold_id",
            "role",
            "fit_start_utc",
            "fit_end_exclusive_utc",
            "validation_warmup_start_utc",
            "validation_start_utc",
            "validation_end_exclusive_utc",
            "right_fit_start_utc",
            "right_fit_end_exclusive_utc",
            "fit_rows",
            "validation_rows",
            "source_slice_sha256",
            "final_oos_authority",
            "fold_definition_sha256",
        ),
        "fold",
    )
    _safe_id(fold["fold_id"], "fold id")
    _require(fold["role"] in _FOLD_ROLES, "invalid fold role")
    _require(
        fold["final_oos_authority"] is False,
        "historical fold cannot confer final OOS authority",
    )
    fit_start = _utc(fold["fit_start_utc"], "fit start")
    fit_end = _utc(fold["fit_end_exclusive_utc"], "fit end")
    warmup_start = _utc(fold["validation_warmup_start_utc"], "validation warmup start")
    validation_start = _utc(fold["validation_start_utc"], "validation start")
    validation_end = _utc(fold["validation_end_exclusive_utc"], "validation end")
    _integer(fold["fit_rows"], "fit rows", 1, 10_000_000_000)
    _integer(fold["validation_rows"], "validation rows", 1, 10_000_000_000)
    _digest(fold["source_slice_sha256"], "fold source slice")
    _digest(fold["fold_definition_sha256"], "fold definition")
    _require(
        prefix_start <= candidate_start <= fit_start < fit_end,
        "fit interval starts outside candidate prefix",
    )
    _require(
        fit_end + timedelta(seconds=geometry["left_purge_seconds"]) <= validation_start,
        "left purge or embargo is weaker than dependency policy",
    )
    _require(
        max(prefix_start, candidate_start)
        <= warmup_start
        <= validation_start - timedelta(seconds=geometry["warmup_seconds"]),
        "validation warmup is weaker than dependency policy",
    )
    _require(
        validation_start < validation_end <= prefix_end,
        "validation interval lies outside prefix",
    )
    fit_capacity = int(
        (fit_end - fit_start).total_seconds() // geometry["timeframe_seconds"]
    )
    validation_capacity = int(
        (validation_end - validation_start).total_seconds()
        // geometry["timeframe_seconds"]
    )
    _require(
        fold["fit_rows"] <= fit_capacity,
        "fit rows exceed native timeframe fit-window capacity",
    )
    _require(
        fold["validation_rows"] <= validation_capacity,
        "validation rows exceed native timeframe validation-window capacity",
    )
    _require(validation_end <= cutoff, "fold validation leaks beyond historical anchor")

    if fold["role"] == _CAUSAL_ROLE:
        _require(
            fold["right_fit_start_utc"] is None
            and fold["right_fit_end_exclusive_utc"] is None,
            "causal rolling fold cannot use right-side training data",
        )
    else:
        right_start = _utc(fold["right_fit_start_utc"], "right fit start")
        right_end = _utc(fold["right_fit_end_exclusive_utc"], "right fit end")
        _require(
            validation_end + timedelta(seconds=geometry["right_purge_seconds"])
            <= right_start
            < right_end,
            "interpolation right purge or interval is invalid",
        )
        _require(
            right_end <= prefix_end and right_end <= cutoff,
            "interpolation fold leaks beyond anchor",
        )
    definition = {key: fold[key] for key in fold if key != "fold_definition_sha256"}
    _require(
        fold["fold_definition_sha256"] == _sha256_json(definition),
        "fold definition digest mismatch",
    )
    return fold


def _normalize_candidate(
    value: Any,
    prefix: dict[str, Any],
    anchor: dict[str, Any],
    geometry: dict[str, int],
) -> tuple[dict[str, Any], bool]:
    candidate = _exact_keys(
        value,
        (
            "candidate_id",
            "lookback_kind",
            "lookback_value",
            "realized_start_utc",
            "candidate_definition_sha256",
            "folds",
        ),
        "candidate",
    )
    _safe_id(candidate["candidate_id"], "candidate id")
    _require(candidate["lookback_kind"] in _LOOKBACK_KINDS, "invalid lookback kind")
    _integer(candidate["lookback_value"], "lookback value", 1, 10_000_000_000)
    candidate_start = _utc(candidate["realized_start_utc"], "candidate realized start")
    _digest(candidate["candidate_definition_sha256"], "candidate definition")
    _require(type(candidate["folds"]) is list, "candidate folds must be a list")
    _require(
        len(candidate["folds"]) <= _MAX_FOLDS_PER_CANDIDATE,
        "candidate fold ceiling exceeded",
    )
    definition = {
        key: candidate[key]
        for key in candidate
        if key not in ("candidate_definition_sha256", "folds")
    }
    _require(
        candidate["candidate_definition_sha256"] == _sha256_json(definition),
        "candidate definition digest mismatch",
    )
    prefix_start = _utc(prefix["prefix_start_utc"], "prefix start")
    native_row_shortfall = (
        candidate["lookback_kind"] == "NATIVE_OBSERVATIONS"
        and candidate["lookback_value"] > prefix["prefix_rows"]
    )
    boundary_shortfall = candidate_start < prefix_start or native_row_shortfall
    if boundary_shortfall:
        _require(
            candidate["folds"] == [],
            "boundary-trimmed candidate must not fabricate folds",
        )
        return candidate, True
    prefix_end = _utc(prefix["prefix_end_exclusive_utc"], "prefix end")
    cutoff = _utc(anchor["selection_cutoff_utc"], "selection cutoff")
    _require(candidate_start < prefix_end, "candidate starts outside historical prefix")
    _require(len(candidate["folds"]) > 0, "eligible candidate requires folds")
    fold_ids: set[str] = set()
    normalized_folds: list[dict[str, Any]] = []
    causal_count = 0
    previous_validation_end: datetime | None = None
    previous_causal_fit_start: datetime | None = None
    previous_causal_fit_end: datetime | None = None
    for raw_fold in candidate["folds"]:
        fold = _normalize_fold(
            raw_fold, candidate_start, prefix_start, prefix_end, cutoff, geometry
        )
        _require(fold["fold_id"] not in fold_ids, "duplicate fold id within candidate")
        fold_ids.add(fold["fold_id"])
        if fold["role"] == _CAUSAL_ROLE:
            causal_count += 1
            causal_fit_start = _utc(fold["fit_start_utc"], "causal fit start")
            causal_fit_end = _utc(fold["fit_end_exclusive_utc"], "causal fit end")
            if (
                previous_causal_fit_start is not None
                and previous_causal_fit_end is not None
            ):
                _require(
                    previous_causal_fit_start <= causal_fit_start
                    and previous_causal_fit_end < causal_fit_end,
                    "causal rolling-origin fit windows do not advance",
                )
            previous_causal_fit_start = causal_fit_start
            previous_causal_fit_end = causal_fit_end
        validation_start = _utc(fold["validation_start_utc"], "validation start")
        validation_end = _utc(fold["validation_end_exclusive_utc"], "validation end")
        if previous_validation_end is not None:
            _require(
                previous_validation_end <= validation_start,
                "candidate validation folds overlap or reorder",
            )
        previous_validation_end = validation_end
        normalized_folds.append(fold)
    _require(
        causal_count > 0, "eligible candidate requires a causal rolling-origin fold"
    )
    earliest_fit_start = min(
        _utc(fold["fit_start_utc"], "fit start") for fold in normalized_folds
    )
    _require(
        candidate_start == earliest_fit_start,
        "candidate realized start does not equal earliest fit-start geometry",
    )
    if candidate["lookback_kind"] == "NATIVE_OBSERVATIONS":
        _require(
            all(
                fold["fit_rows"] == candidate["lookback_value"]
                for fold in normalized_folds
            ),
            "native-observation lookback does not equal declared fit rows",
        )
    _require(
        all(
            fold["fit_rows"] + fold["validation_rows"] <= prefix["prefix_rows"]
            for fold in normalized_folds
        ),
        "declared fold rows exceed activated native prefix backing",
    )
    candidate["folds"] = normalized_folds
    return candidate, False


def _normalize_state_context_disposition(value: Any) -> dict[str, Any]:
    disposition = _exact_keys(
        value,
        (
            "schema_version",
            "candidate_id",
            "candidate_definition_sha256",
            "state_requirement_sha256",
            "required_context_bars",
            "selected_context_bars",
            "status",
            "reason_code",
            "disposition_sha256",
        ),
        "state context disposition",
    )
    _require(
        disposition["schema_version"] == _STATE_CONTEXT_DISPOSITION_SCHEMA,
        "state context disposition schema mismatch",
    )
    _safe_id(disposition["candidate_id"], "state context candidate id")
    _digest(
        disposition["candidate_definition_sha256"],
        "state context candidate definition",
    )
    _digest(disposition["state_requirement_sha256"], "state requirement")
    for name in ("required_context_bars", "selected_context_bars"):
        _require(
            type(disposition[name]) is int and disposition[name] >= 0,
            f"{name} must be a nonnegative integer",
        )
    _require(
        disposition["selected_context_bars"] >= 1,
        "selected context must be positive",
    )
    within = (
        disposition["required_context_bars"]
        <= disposition["selected_context_bars"]
    )
    expected_status = "PLANNED" if within else _PROCESS_REJECTED
    expected_reason = None if within else _STATE_CONTEXT_BOUNDARY_REASON
    _require(
        disposition["status"] == expected_status
        and disposition["reason_code"] == expected_reason,
        "state context disposition inequality mismatch",
    )
    body = {
        key: disposition[key]
        for key in disposition
        if key != "disposition_sha256"
    }
    _require(
        disposition["disposition_sha256"] == _sha256_json(body),
        "state context disposition digest mismatch",
    )
    return disposition


def _normalize_trial(value: Any) -> dict[str, Any]:
    base_keys = (
        "trial_id",
        "strategy_id",
        "strategy_release_sha256",
        "parameter_set_sha256",
        "trial_definition_sha256",
    )
    extended_keys = (*base_keys[:-1], "state_context_disposition", base_keys[-1])
    if type(value) is not dict:
        _fail("trial must be an exact mapping")
    if set(value) == set(base_keys):
        keys = base_keys
    elif set(value) == set(extended_keys):
        keys = extended_keys
    else:
        _fail("trial fields mismatch")
    trial = _exact_keys(value, keys, "trial")
    _safe_id(trial["trial_id"], "trial id")
    _safe_id(trial["strategy_id"], "strategy id")
    _digest(trial["strategy_release_sha256"], "strategy release")
    _digest(trial["parameter_set_sha256"], "parameter set")
    if "state_context_disposition" in trial:
        trial["state_context_disposition"] = (
            _normalize_state_context_disposition(
                trial["state_context_disposition"]
            )
        )
    _digest(trial["trial_definition_sha256"], "trial definition")
    definition = {key: trial[key] for key in trial if key != "trial_definition_sha256"}
    _require(
        trial["trial_definition_sha256"] == _sha256_json(definition),
        "trial definition digest mismatch",
    )
    return trial


def _paired_fold_axes(candidates: list[dict[str, Any]], boundary_ids: set[str]) -> None:
    reference: list[tuple[Any, ...]] | None = None
    for candidate in candidates:
        if candidate["candidate_id"] in boundary_ids:
            continue
        axes = [
            (
                fold["fold_id"],
                fold["role"],
                fold["fit_end_exclusive_utc"],
                fold["validation_warmup_start_utc"],
                fold["validation_start_utc"],
                fold["validation_end_exclusive_utc"],
                fold["right_fit_start_utc"],
                fold["right_fit_end_exclusive_utc"],
            )
            for fold in candidate["folds"]
        ]
        if reference is None:
            reference = axes
        else:
            _require(
                axes == reference,
                "eligible candidates must share paired information and validation boundaries",
            )


def _normalize_request(request: Any) -> dict[str, Any]:
    normalized = _bounded_snapshot(request, _MAX_REQUEST_BYTES)
    _require(type(normalized) is dict, "request must be an exact mapping")
    request_schema = normalized.get("schema_version")
    if request_schema == _REQUEST_SCHEMA:
        request_keys = (
            "schema_version",
            "run_id",
            "anchor",
            "cell",
            "native_prefix",
            "dependency_policy",
            "horizon_policy",
            "candidates",
            "trials",
        )
        unobserved_request = False
    elif request_schema == _UNOBSERVED_REQUEST_SCHEMA:
        request_keys = (
            "schema_version",
            "run_id",
            "anchor",
            "cell",
            "native_prefix",
            "dependency_policy",
            "horizon_policy",
            "regime_support_binding",
            "candidates",
            "trials",
        )
        unobserved_request = True
    else:
        _fail("request schema mismatch")
    root = _exact_keys(
        normalized,
        request_keys,
        "request",
    )
    _safe_id(root["run_id"], "run id")
    anchor = _normalize_anchor(root["anchor"])
    cell = _normalize_cell(root["cell"])
    dependency, derived_bars = _normalize_dependency_policy(root["dependency_policy"])
    policy = _normalize_horizon_policy(root["horizon_policy"])
    prefix = _normalize_native_prefix(root["native_prefix"], cell, anchor)
    geometry = _derived_geometry(cell["timeframe"], derived_bars, dependency["payload"])
    _require(type(root["candidates"]) is list, "candidates must be a list")
    _require(type(root["trials"]) is list, "trials must be a list")
    _require(len(root["candidates"]) <= _MAX_CANDIDATES, "candidate ceiling exceeded")
    _require(len(root["trials"]) <= _MAX_TRIALS, "trial ceiling exceeded")

    candidates: list[dict[str, Any]] = []
    trials: list[dict[str, Any]] = []
    boundary_ids: set[str] = set()
    candidate_ids: set[str] = set()
    candidate_identities: set[tuple[str, int]] = set()
    trial_ids: set[str] = set()
    trial_identities: set[tuple[str, str, str]] = set()
    regime_support_binding: dict[str, Any] | None = None
    if unobserved_request:
        _require(
            prefix["source_state"] == _AVAILABLE,
            "unobserved regime requires an available native prefix",
        )
        _require(
            prefix["internal_status"] != "UNEXPLAINED",
            "unobserved regime cannot mask source integrity failure",
        )
        _require(
            root["candidates"] == [] and root["trials"] == [],
            "unobserved regime cannot carry experiment roster",
        )
        regime_support_binding = _normalize_regime_support_binding(
            root["regime_support_binding"], cell, prefix
        )
    elif prefix["source_state"] == _AVAILABLE:
        _require(
            len(root["candidates"]) > 0, "available source requires candidate roster"
        )
        _require(len(root["trials"]) > 0, "available source requires trial roster")
        for raw_candidate in root["candidates"]:
            candidate, boundary = _normalize_candidate(
                raw_candidate, prefix, anchor, geometry
            )
            _require(
                candidate["candidate_id"] not in candidate_ids, "duplicate candidate id"
            )
            candidate_ids.add(candidate["candidate_id"])
            candidate_identity = (
                candidate["lookback_kind"],
                candidate["lookback_value"],
            )
            _require(
                candidate_identity not in candidate_identities,
                "duplicate candidate identity",
            )
            candidate_identities.add(candidate_identity)
            if boundary:
                boundary_ids.add(candidate["candidate_id"])
            candidates.append(candidate)
        for raw_trial in root["trials"]:
            trial = _normalize_trial(raw_trial)
            _require(trial["trial_id"] not in trial_ids, "duplicate trial id")
            trial_ids.add(trial["trial_id"])
            trial_identity = (
                trial["strategy_id"],
                trial["strategy_release_sha256"],
                trial["parameter_set_sha256"],
            )
            _require(
                trial_identity not in trial_identities,
                "duplicate strategy-parameter identity",
            )
            trial_identities.add(trial_identity)
            trials.append(trial)
        extended_trials = [
            trial for trial in trials if "state_context_disposition" in trial
        ]
        _require(
            not extended_trials or len(extended_trials) == len(trials),
            "state context dispositions must cover the exact trial roster",
        )
        if extended_trials:
            _require(
                len(candidates) == 1,
                "state context dispositions require exactly one candidate",
            )
            candidate = candidates[0]
            selected_context = dependency["payload"]["feature_lookback_bars"]
            for trial in extended_trials:
                disposition = trial["state_context_disposition"]
                _require(
                    disposition["candidate_id"] == candidate["candidate_id"]
                    and disposition["candidate_definition_sha256"]
                    == candidate["candidate_definition_sha256"]
                    and disposition["selected_context_bars"] == selected_context,
                    "state context disposition candidate or dependency mismatch",
                )
        _paired_fold_axes(candidates, boundary_ids)
        _require(
            len(candidates) * len(trials) <= _MAX_ATTEMPTS, "attempt ceiling exceeded"
        )
        evaluation_axes = sum(
            1
            if candidate["candidate_id"] in boundary_ids
            or (
                len(candidates) == 1
                and extended_trials
                and trial["state_context_disposition"]["status"]
                == _PROCESS_REJECTED
            )
            else len(candidate["folds"])
            for candidate in candidates
            for trial in trials
        )
        _require(
            evaluation_axes <= _MAX_EXPECTED_EVALUATIONS,
            "expected evaluation ceiling exceeded",
        )
    else:
        _require(
            root["candidates"] == [] and root["trials"] == [],
            "terminal source cannot carry experiment roster",
        )

    root["anchor"] = anchor
    root["cell"] = cell
    root["native_prefix"] = prefix
    root["dependency_policy"] = dependency
    root["horizon_policy"] = policy
    root["candidates"] = candidates
    root["trials"] = trials
    root.pop("regime_support_binding", None)
    root["_unobserved_request"] = unobserved_request
    root["_regime_support_binding"] = regime_support_binding
    root["_derived_geometry"] = geometry
    root["_boundary_ids"] = sorted(boundary_ids)
    return root


def _source_binding(prefix: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_state": prefix["source_state"],
        "activation_id": prefix["activation_id"],
        "activation_manifest_sha256": prefix["activation_manifest_sha256"],
        "activation_entry_sha256": prefix["activation_entry_sha256"],
        "artifact_sha256": prefix["artifact_sha256"],
        "provenance_sha256": prefix["provenance_sha256"],
        "request_map_sha256": prefix["request_map_sha256"],
        "source_identity_sha256": prefix["source_identity_sha256"],
        "continuity_policy_sha256": prefix["continuity_policy_sha256"],
        "continuity_receipt_sha256": prefix["continuity_receipt_sha256"],
        "continuity_geometry_sha256": prefix["continuity_geometry_sha256"],
        "source_terminal_receipt_sha256": prefix["source_terminal_receipt_sha256"],
        "prefix_schema_sha256": prefix["prefix_schema_sha256"],
        "prefix_index_sha256": prefix["prefix_index_sha256"],
        "prefix_content_sha256": prefix["prefix_content_sha256"],
        "segments_sha256": prefix["segments_sha256"],
    }


def _attempt_id(
    request: dict[str, Any],
    candidate: dict[str, Any],
    trial: dict[str, Any],
) -> str:
    identity = {
        "run_id": request["run_id"],
        "anchor_id": request["anchor"]["anchor_id"],
        "anchor_ordinal": request["anchor"]["ordinal"],
        "selection_cutoff_utc": request["anchor"]["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": request["anchor"][
            "process_precommit_receipt_sha256"
        ],
        "cell": request["cell"],
        "candidate_definition_sha256": candidate["candidate_definition_sha256"],
        "trial_definition_sha256": trial["trial_definition_sha256"],
        "policy_release_sha256": request["horizon_policy"]["policy_release_sha256"],
        "dependency_policy_receipt_sha256": request["dependency_policy"][
            "signed_policy_receipt_sha256"
        ],
        "source_prefix_receipt_sha256": _sha256_json(
            {
                "cell": request["cell"],
                "anchor_id": request["anchor"]["anchor_id"],
                "selection_cutoff_utc": request["anchor"]["selection_cutoff_utc"],
                "native_prefix": request["native_prefix"],
            }
        ),
        "source_binding": _source_binding(request["native_prefix"]),
    }
    return _sha256_json(identity)


def _evaluation_context(
    request: dict[str, Any],
    attempt_id: str,
    candidate: dict[str, Any],
    trial: dict[str, Any],
    fold: dict[str, Any] | None,
) -> dict[str, Any]:
    if fold is None:
        fold_binding: dict[str, Any] = {
            "fold_id": "TERMINAL",
            "role": _TERMINAL_ROLE,
            "fold_definition_sha256": None,
            "validation_start_utc": None,
            "validation_end_exclusive_utc": None,
            "final_oos_authority": False,
        }
    else:
        fold_binding = {
            "fold_id": fold["fold_id"],
            "role": fold["role"],
            "fold_definition_sha256": fold["fold_definition_sha256"],
            "validation_start_utc": fold["validation_start_utc"],
            "validation_end_exclusive_utc": fold["validation_end_exclusive_utc"],
            "final_oos_authority": False,
        }
    context = {
        "authority_scope": _AUTHORITY_SCOPE,
        "attempt_id": attempt_id,
        "anchor_id": request["anchor"]["anchor_id"],
        "selection_cutoff_utc": request["anchor"]["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": request["anchor"][
            "process_precommit_receipt_sha256"
        ],
        "cell": request["cell"],
        "candidate_id": candidate["candidate_id"],
        "candidate_definition_sha256": candidate["candidate_definition_sha256"],
        "trial_id": trial["trial_id"],
        "trial_definition_sha256": trial["trial_definition_sha256"],
        "strategy_release_sha256": trial["strategy_release_sha256"],
        "parameter_set_sha256": trial["parameter_set_sha256"],
        "fold": fold_binding,
        "source_binding": _source_binding(request["native_prefix"]),
        "source_prefix_receipt_sha256": _sha256_json(
            {
                "cell": request["cell"],
                "anchor_id": request["anchor"]["anchor_id"],
                "selection_cutoff_utc": request["anchor"]["selection_cutoff_utc"],
                "native_prefix": request["native_prefix"],
            }
        ),
        "dependency_policy_receipt_sha256": request["dependency_policy"][
            "signed_policy_receipt_sha256"
        ],
        "dependency_payload_sha256": request["dependency_policy"]["payload_sha256"],
        "horizon_policy_release_sha256": request["horizon_policy"][
            "policy_release_sha256"
        ],
        "evaluator_release_sha256": request["horizon_policy"][
            "evaluator_release_sha256"
        ],
        "cost_contract_sha256": request["horizon_policy"]["cost_contract_sha256"],
        "cost_stress_contract_sha256": request["horizon_policy"][
            "cost_stress_contract_sha256"
        ],
    }
    disposition = trial.get("state_context_disposition")
    if type(disposition) is dict:
        context["state_context_disposition_sha256"] = disposition[
            "disposition_sha256"
        ]
    return context


def _build_plan_body(request: Any) -> dict[str, Any]:
    normalized = _normalize_request(request)
    boundary_ids = set(normalized.pop("_boundary_ids"))
    geometry = normalized.pop("_derived_geometry")
    unobserved_request = normalized.pop("_unobserved_request")
    regime_support_binding = normalized.pop("_regime_support_binding")
    prefix = normalized["native_prefix"]
    candidates = normalized["candidates"]
    trials = normalized["trials"]
    attempts: list[dict[str, Any]] = []
    expected: list[dict[str, Any]] = []

    source_integrity_blocked = (
        prefix["source_state"] == _AVAILABLE
        and prefix["internal_status"] == "UNEXPLAINED"
    )
    if prefix["source_state"] == _AVAILABLE:
        for candidate in candidates:
            for trial in trials:
                attempt_id = _attempt_id(normalized, candidate, trial)
                boundary = candidate["candidate_id"] in boundary_ids
                disposition = trial.get("state_context_disposition")
                state_context_rejected = (
                    type(disposition) is dict
                    and disposition["status"] == _PROCESS_REJECTED
                )
                if source_integrity_blocked:
                    attempt_status = "TECHNICAL_INCOMPLETE"
                    attempt_reason = "SOURCE_INTEGRITY_FAILURE"
                else:
                    attempt_status = (
                        _PROCESS_REJECTED
                        if boundary or state_context_rejected
                        else "PLANNED"
                    )
                    attempt_reason = (
                        _BOUNDARY_REASON
                        if boundary
                        else (
                            _STATE_CONTEXT_BOUNDARY_REASON
                            if state_context_rejected
                            else None
                        )
                    )
                attempt = {
                    "attempt_id": attempt_id,
                    "candidate_id": candidate["candidate_id"],
                    "candidate_definition_sha256": candidate[
                        "candidate_definition_sha256"
                    ],
                    "trial_id": trial["trial_id"],
                    "trial_definition_sha256": trial["trial_definition_sha256"],
                    "strategy_id": trial["strategy_id"],
                    "strategy_release_sha256": trial["strategy_release_sha256"],
                    "parameter_set_sha256": trial["parameter_set_sha256"],
                    "status": attempt_status,
                    "reason_code": attempt_reason,
                }
                if type(disposition) is dict:
                    attempt["state_context_disposition_sha256"] = disposition[
                        "disposition_sha256"
                    ]
                attempts.append(attempt)
                if source_integrity_blocked:
                    continue
                terminal_rejection = boundary or state_context_rejected
                folds: list[dict[str, Any] | None] = (
                    [None] if terminal_rejection else list(candidate["folds"])
                )
                for fold in folds:
                    context = _evaluation_context(
                        normalized, attempt_id, candidate, trial, fold
                    )
                    expected.append(
                        {
                            "ordinal": len(expected),
                            "attempt_id": attempt_id,
                            "fold_id": "TERMINAL" if fold is None else fold["fold_id"],
                            "role": _TERMINAL_ROLE if fold is None else fold["role"],
                            "validation_start_utc": None
                            if fold is None
                            else fold["validation_start_utc"],
                            "validation_end_exclusive_utc": None
                            if fold is None
                            else fold["validation_end_exclusive_utc"],
                            "expected_status": _PROCESS_REJECTED
                            if terminal_rejection
                            else "EVALUATION_REQUIRED",
                            "expected_reason_code": attempt_reason
                            if terminal_rejection
                            else None,
                            "downstream_causal_eligible": False
                            if fold is None
                            else fold["role"] == _CAUSAL_ROLE,
                            "final_oos_authority": False,
                            "context_sha256": _sha256_json(context),
                        }
                    )

    candidate_registry = [
        {
            "candidate_id": item["candidate_id"],
            "candidate_definition_sha256": item["candidate_definition_sha256"],
            "boundary_shortfall": item["candidate_id"] in boundary_ids,
        }
        for item in candidates
    ]
    trial_registry = []
    for item in trials:
        registry_item = {
            "trial_id": item["trial_id"],
            "strategy_id": item["strategy_id"],
            "strategy_release_sha256": item["strategy_release_sha256"],
            "parameter_set_sha256": item["parameter_set_sha256"],
            "trial_definition_sha256": item["trial_definition_sha256"],
        }
        if "state_context_disposition" in item:
            registry_item["state_context_disposition_sha256"] = item[
                "state_context_disposition"
            ]["disposition_sha256"]
        trial_registry.append(registry_item)
    if unobserved_request:
        plan_status = "HISTORICAL_REGIME_UNOBSERVED"
    elif prefix["source_state"] == _SOURCE_UNAVAILABLE:
        plan_status = "HISTORICAL_SOURCE_UNAVAILABLE"
    elif prefix["source_state"] == _TECHNICAL_TERMINAL:
        plan_status = "HISTORICAL_TERMINAL_TECHNICAL"
    elif (
        prefix["source_state"] == _TECHNICAL_FAILURE
        or prefix["internal_status"] == "UNEXPLAINED"
    ):
        plan_status = "HISTORICAL_TECHNICAL_INCOMPLETE"
    elif boundary_ids == {item["candidate_id"] for item in candidates}:
        plan_status = "HISTORICAL_BOUNDARY_LIMITED"
    else:
        plan_status = "READY_FOR_HISTORICAL_EVIDENCE"

    body = {
        "schema_version": _PLAN_SCHEMA,
        "authority_scope": _AUTHORITY_SCOPE,
        "trust_class": _TRUST_CLASS,
        "requires_external_exact_artifact_verification": False,
        "eligible_for_downstream_offline_selection": True,
        "confers_trading_or_capital_authority": False,
        "historical_anchor_local_only": True,
        "selects_within_cell_process": False,
        "confers_final_oos_authority": False,
        "run_id": normalized["run_id"],
        "anchor": normalized["anchor"],
        "cell": normalized["cell"],
        "native_prefix": prefix,
        "source_binding": _source_binding(prefix),
        "source_prefix_receipt_sha256": None,
        "dependency_policy": normalized["dependency_policy"],
        "derived_dependency_geometry": geometry,
        "horizon_policy": normalized["horizon_policy"],
        "candidates": candidates,
        "trials": trials,
        "candidate_registry": candidate_registry,
        "candidate_registry_sha256": _sha256_json(candidate_registry),
        "trial_registry": trial_registry,
        "trial_registry_sha256": _sha256_json(trial_registry),
        "attempts": attempts,
        "attempt_roster_sha256": _sha256_json(attempts),
        "expected_evaluations": expected,
        "evidence_roster_sha256": _sha256_json(expected),
        "plan_status": plan_status,
        "resource_contract": {
            "bounded_transport": True,
            "isolated_worker_count": 1,
            "input_byte_ceiling": _MAX_EVIDENCE_BYTES,
            "output_byte_ceiling": _MAX_OUTPUT_BYTES,
            "vector_point_ceiling": _MAX_VECTOR_POINTS,
            "expected_evaluation_ceiling": _MAX_EXPECTED_EVALUATIONS,
            "tabular_object_transport": False,
            "lossless_exact_shards": True,
            "persistent_storage_written": False,
        },
    }
    if prefix["source_state"] == _AVAILABLE:
        body["source_prefix_receipt_sha256"] = _source_prefix_receipt(body)
    if unobserved_request:
        _require(
            regime_support_binding is not None
            and candidates == []
            and trials == []
            and attempts == []
            and expected == [],
            "unobserved horizon roster must be exactly empty",
        )
        body["regime_support_binding"] = regime_support_binding
    _require(
        len(_canonical_bytes(body)) <= _MAX_OUTPUT_BYTES, "plan output ceiling exceeded"
    )
    return body


def _build_plan(request: Any) -> _HistoricalHorizonPlan:
    return _HistoricalHorizonPlan(_build_plan_body(request), _PLAN_SEAL)


def _source_prefix_receipt(plan_body: dict[str, Any]) -> str:
    return _sha256_json(
        {
            "cell": plan_body["cell"],
            "anchor_id": plan_body["anchor"]["anchor_id"],
            "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
            "native_prefix": plan_body["native_prefix"],
        }
    )


def _empty_vectors(shard: dict[str, Any]) -> bool:
    return all(
        shard[key] == []
        for key in (
            "daily_dates_utc",
            "daily_net_mtm_r",
            "daily_net_mtm_r_cost_stress",
            "completed_trade_close_utc",
            "completed_trade_net_mtm_r",
        )
    )


def _normalize_shard(
    value: Any,
    expected: dict[str, Any],
    plan_body: dict[str, Any],
    remaining_points: int,
) -> tuple[dict[str, Any], int]:
    shard = _exact_keys(
        value,
        (
            "schema_version",
            "attempt_id",
            "fold_id",
            "context_sha256",
            "status",
            "reason_code",
            "source_prefix_sha256",
            "evaluator_release_sha256",
            "cost_contract_sha256",
            "cost_stress_contract_sha256",
            "evidence_role",
            "final_oos_authority",
            "daily_dates_utc",
            "daily_net_mtm_r",
            "daily_net_mtm_r_cost_stress",
            "completed_trade_close_utc",
            "completed_trade_net_mtm_r",
        ),
        "evidence shard",
    )
    _require(shard["schema_version"] == _SHARD_SCHEMA, "evidence shard schema mismatch")
    _require(
        shard["attempt_id"] == expected["attempt_id"],
        "evidence attempt roster mismatch",
    )
    _require(shard["fold_id"] == expected["fold_id"], "evidence fold roster mismatch")
    _require(
        shard["context_sha256"] == expected["context_sha256"],
        "evidence context mismatch",
    )
    _require(
        shard["source_prefix_sha256"] == _source_prefix_receipt(plan_body),
        "source prefix receipt mismatch",
    )
    policy = plan_body["horizon_policy"]
    _require(
        shard["evaluator_release_sha256"] == policy["evaluator_release_sha256"],
        "evaluator release mismatch",
    )
    _require(
        shard["cost_contract_sha256"] == policy["cost_contract_sha256"],
        "cost contract mismatch",
    )
    _require(
        shard["cost_stress_contract_sha256"] == policy["cost_stress_contract_sha256"],
        "cost stress contract mismatch",
    )
    _require(shard["evidence_role"] == expected["role"], "evidence role mismatch")
    _require(
        shard["final_oos_authority"] is False,
        "evidence shard cannot confer final OOS authority",
    )
    _text_or_none(shard["reason_code"], "shard reason code")
    for key in (
        "daily_dates_utc",
        "daily_net_mtm_r",
        "daily_net_mtm_r_cost_stress",
        "completed_trade_close_utc",
        "completed_trade_net_mtm_r",
    ):
        _require(type(shard[key]) is list, f"{key} must be a list")

    point_count = sum(
        len(shard[key])
        for key in (
            "daily_dates_utc",
            "daily_net_mtm_r",
            "daily_net_mtm_r_cost_stress",
            "completed_trade_close_utc",
            "completed_trade_net_mtm_r",
        )
    )
    _require(point_count <= remaining_points, "evidence vector point ceiling exceeded")

    if expected["expected_status"] == _PROCESS_REJECTED:
        _require(
            shard["status"] == _PROCESS_REJECTED,
            "presearch terminal cannot be reclassified",
        )
        _require(
            shard["reason_code"] == expected["expected_reason_code"],
            "presearch terminal reason mismatch",
        )
        _require(
            _empty_vectors(shard), "mechanical rejection cannot carry observations"
        )
        return shard, point_count

    _require(
        shard["status"] in (_AUTHENTICATED, _PROCESS_REJECTED, _TECHNICAL_SHARD),
        "invalid evidence shard status",
    )
    if shard["status"] == _PROCESS_REJECTED:
        _require(
            shard["reason_code"] == _PROCESS_SCREEN_REASON,
            "process rejection reason is not preregistered",
        )
        _require(_empty_vectors(shard), "process rejection cannot carry observations")
        return shard, point_count
    if shard["status"] == _TECHNICAL_SHARD:
        _require(
            shard["reason_code"] in _TECHNICAL_REASONS,
            "invalid technical failure reason",
        )
        _require(_empty_vectors(shard), "technical failure cannot carry observations")
        return shard, point_count

    _require(
        shard["reason_code"] is None,
        "authenticated shard cannot carry a failure reason",
    )
    _require(
        len(shard["daily_dates_utc"]) > 0,
        "authenticated shard requires nonempty daily MTM evidence",
    )
    _require(
        len(shard["daily_dates_utc"])
        == len(shard["daily_net_mtm_r"])
        == len(shard["daily_net_mtm_r_cost_stress"]),
        "daily vectors are not aligned",
    )
    _require(
        len(shard["completed_trade_close_utc"])
        == len(shard["completed_trade_net_mtm_r"]),
        "completed-trade vectors are not aligned",
    )
    validation_start = _utc(
        expected["validation_start_utc"], "expected validation start"
    )
    validation_end = _utc(
        expected["validation_end_exclusive_utc"], "expected validation end"
    )
    if shard["daily_dates_utc"]:
        _require(
            validation_start.time() == datetime.min.time()
            and validation_end.time() == datetime.min.time(),
            "daily evidence requires UTC-midnight validation boundaries",
        )
    previous_day: datetime | None = None
    for index, raw_day in enumerate(shard["daily_dates_utc"]):
        day = _date(raw_day, f"daily date {index}")
        _require(
            validation_start.date() <= day.date() < validation_end.date(),
            "daily observation lies outside validation interval",
        )
        if previous_day is not None:
            _require(
                previous_day < day, "daily dates must be strictly increasing and unique"
            )
        previous_day = day
        _decimal_text(shard["daily_net_mtm_r"][index], f"daily net observation {index}")
        _decimal_text(
            shard["daily_net_mtm_r_cost_stress"][index],
            f"daily cost-stress observation {index}",
        )
    previous_close: datetime | None = None
    for index, raw_close in enumerate(shard["completed_trade_close_utc"]):
        close = _utc(raw_close, f"completed trade close {index}")
        _require(
            validation_start <= close < validation_end,
            "completed trade lies outside validation interval",
        )
        if previous_close is not None:
            _require(previous_close <= close, "completed trade closes must not reorder")
        previous_close = close
        _decimal_text(
            shard["completed_trade_net_mtm_r"][index],
            f"completed trade observation {index}",
        )
    return shard, point_count


def _evidence_body(plan_body: dict[str, Any], raw_shards: Any) -> dict[str, Any]:
    _require(type(raw_shards) is list, "evidence shards must be a list")
    expected = plan_body["expected_evaluations"]
    plan_status = plan_body["plan_status"]
    if plan_status in (
        "HISTORICAL_SOURCE_UNAVAILABLE",
        "HISTORICAL_TERMINAL_TECHNICAL",
        "HISTORICAL_TECHNICAL_INCOMPLETE",
        "HISTORICAL_REGIME_UNOBSERVED",
    ):
        _require(
            raw_shards == [],
            "terminal or unobserved historical plan cannot accept evidence shards",
        )
    else:
        _require(
            len(raw_shards) == len(expected),
            "evidence roster is incomplete or over-complete",
        )
    normalized: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    remaining = _MAX_VECTOR_POINTS
    technical_count = 0
    authenticated_count = 0
    process_rejected_count = 0
    for ordinal, expected_item in enumerate(expected):
        shard, used = _normalize_shard(
            raw_shards[ordinal], expected_item, plan_body, remaining
        )
        remaining -= used
        normalized.append(shard)
        if shard["status"] == _TECHNICAL_SHARD:
            technical_count += 1
        elif shard["status"] == _AUTHENTICATED:
            authenticated_count += 1
        else:
            process_rejected_count += 1
        receipts.append(
            {
                "ordinal": ordinal,
                "attempt_id": shard["attempt_id"],
                "fold_id": shard["fold_id"],
                "evidence_role": shard["evidence_role"],
                "downstream_causal_eligible": expected_item[
                    "downstream_causal_eligible"
                ],
                "final_oos_authority": False,
                "shard_receipt_sha256": _sha256_json(shard),
            }
        )

    if plan_status == "HISTORICAL_SOURCE_UNAVAILABLE":
        evidence_status = "HISTORICAL_SOURCE_UNAVAILABLE"
    elif plan_status == "HISTORICAL_TERMINAL_TECHNICAL":
        evidence_status = "HISTORICAL_TERMINAL_TECHNICAL"
    elif plan_status == "HISTORICAL_REGIME_UNOBSERVED":
        evidence_status = "HISTORICAL_REGIME_UNOBSERVED"
    elif plan_status == "HISTORICAL_TECHNICAL_INCOMPLETE" or technical_count > 0:
        evidence_status = "HISTORICAL_EVIDENCE_TECHNICAL_INCOMPLETE"
    else:
        evidence_status = "HISTORICAL_EVIDENCE_COMPLETE"
    body = {
        "schema_version": _EVIDENCE_SCHEMA,
        "authority_scope": _AUTHORITY_SCOPE,
        "trust_class": _TRUST_CLASS,
        "requires_external_exact_artifact_verification": False,
        "eligible_for_downstream_offline_selection": True,
        "confers_trading_or_capital_authority": False,
        "evidence_completeness_scope": "ROSTER_AND_SCHEMA_ONLY",
        "historical_anchor_local_only": True,
        "confers_final_oos_authority": False,
        "plan_receipt_sha256": _sha256_json(plan_body),
        "run_id": plan_body["run_id"],
        "anchor_id": plan_body["anchor"]["anchor_id"],
        "anchor_ordinal": plan_body["anchor"]["ordinal"],
        "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
        "cell": plan_body["cell"],
        "source_prefix_sha256": _source_prefix_receipt(plan_body)
        if plan_body["native_prefix"]["source_state"] == _AVAILABLE
        else None,
        "dependency_policy_receipt_sha256": plan_body["dependency_policy"][
            "signed_policy_receipt_sha256"
        ],
        "horizon_policy_release_sha256": plan_body["horizon_policy"][
            "policy_release_sha256"
        ],
        "attempt_roster_sha256": plan_body["attempt_roster_sha256"],
        "evidence_roster_sha256": plan_body["evidence_roster_sha256"],
        "evidence_status": evidence_status,
        "authenticated_shard_count": authenticated_count,
        "process_rejected_shard_count": process_rejected_count,
        "technical_failure_shard_count": technical_count,
        "shard_count": len(normalized),
        "shards": normalized,
        "shard_receipts": receipts,
        "shard_receipts_sha256": _sha256_json(receipts),
        "resource_execution": {
            "bounded_transport": True,
            "isolated_worker_count": 1,
            "tabular_object_transport": False,
            "lossless_exact_shards": True,
            "persistent_storage_written": False,
        },
    }
    _require(
        len(_canonical_bytes(body)) <= _MAX_OUTPUT_BYTES,
        "evidence output ceiling exceeded",
    )
    return body


def _evidence_worker(
    send_connection: Any, plan_bytes: bytes, shards_bytes: bytes
) -> None:
    try:
        _require(
            type(plan_bytes) is bytes and type(shards_bytes) is bytes,
            "worker transport must be bytes",
        )
        _require(
            len(plan_bytes) <= _MAX_OUTPUT_BYTES,
            "worker plan transport ceiling exceeded",
        )
        _require(
            len(shards_bytes) <= _MAX_EVIDENCE_BYTES,
            "worker shard transport ceiling exceeded",
        )
        plan_body = json.loads(plan_bytes.decode("utf-8"))
        shards = json.loads(shards_bytes.decode("utf-8"))
        result = _canonical_bytes(_evidence_body(plan_body, shards))
        _require(len(result) <= _MAX_OUTPUT_BYTES, "worker output ceiling exceeded")
        send_connection.send_bytes(b"O" + result)
    except Exception as exc:
        error = _canonical_bytes(
            {
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:2048],
            }
        )
        send_connection.send_bytes(b"E" + error)
    finally:
        send_connection.close()


def _evaluate_plan(plan: Any, evidence_shards: Any) -> _HistoricalHorizonEvidence:
    _require(_valid_plan(plan), "invalid or tampered historical horizon plan")
    shards = _bounded_snapshot(evidence_shards, _MAX_EVIDENCE_BYTES)
    plan_body = plan.to_dict()
    plan_bytes = _canonical_bytes(plan_body)
    shards_bytes = _canonical_bytes(shards)
    context = mp.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_evidence_worker,
        args=(send_connection, plan_bytes, shards_bytes),
        name="fxpm-historical-horizon-evidence",
        daemon=True,
    )
    try:
        process.start()
        send_connection.close()
        response = receive_connection.recv_bytes(_MAX_OUTPUT_BYTES + 1)
        process.join(5)
        _require(not process.is_alive(), "historical evidence worker failed to stop")
        _require(process.exitcode == 0, "historical evidence worker exited abnormally")
    finally:
        receive_connection.close()
        if process.is_alive():
            process.terminate()
            process.join(5)
    _require(len(response) > 1, "historical evidence worker returned an empty response")
    if response[:1] == b"E":
        try:
            error = json.loads(response[1:].decode("utf-8"))
            detail = error.get("error_message", "evidence contract rejected")
        except (ValueError, UnicodeError, AttributeError):
            detail = "evidence contract rejected"
        _fail(f"historical evidence worker rejected input: {detail}")
    _require(response[:1] == b"O", "historical evidence worker protocol mismatch")
    worker_body = json.loads(response[1:].decode("utf-8"))
    parent_body = _evidence_body(plan_body, shards)
    _require(
        _canonical_bytes(worker_body) == _canonical_bytes(parent_body),
        "worker replay mismatch",
    )
    return _HistoricalHorizonEvidence(parent_body, _EVIDENCE_SEAL)


def _verify_evidence(plan: Any, evidence: Any) -> bool:
    if not _valid_plan(plan) or not _valid_evidence(evidence):
        return False
    try:
        plan_body = plan.to_dict()
        evidence_body = evidence.to_dict()
        replay = _evidence_body(plan_body, evidence_body["shards"])
        return _canonical_bytes(replay) == evidence._body_json
    except (KeyError, TypeError, ValueError, UnicodeError, _HorizonContractError):
        return False


def _make_public_authority() -> tuple[Any, Any, Any]:
    sha256_impl = hashlib.sha256
    build_impl = _build_plan
    evaluate_impl = _evaluate_plan
    verify_impl = _verify_evidence
    plan_type = _HistoricalHorizonPlan
    evidence_type = _HistoricalHorizonEvidence
    plan_seal = _PLAN_SEAL
    evidence_seal = _EVIDENCE_SEAL
    critical_names = (
        "_fail",
        "_require",
        "_bounded_snapshot",
        "_canonical_bytes",
        "_sha256_bytes",
        "_sha256_json",
        "_exact_keys",
        "_safe_id",
        "_digest",
        "_integer",
        "_text_or_none",
        "_utc",
        "_date",
        "_decimal_text",
        "_valid_plan",
        "_valid_evidence",
        "_normalize_anchor",
        "_normalize_cell",
        "_normalize_dependency_policy",
        "_normalize_horizon_policy",
        "_normalize_regime_support_binding",
        "_normalize_native_prefix",
        "_derived_geometry",
        "_normalize_fold",
        "_normalize_candidate",
        "_normalize_trial",
        "_paired_fold_axes",
        "_normalize_request",
        "_source_binding",
        "_attempt_id",
        "_evaluation_context",
        "_build_plan_body",
        "_build_plan",
        "_source_prefix_receipt",
        "_empty_vectors",
        "_normalize_shard",
        "_evidence_body",
        "_evidence_worker",
        "_evaluate_plan",
        "_verify_evidence",
    )
    critical = tuple(
        (
            name,
            globals()[name],
            globals()[name].__code__,
            sha256_impl(globals()[name].__code__.co_code).hexdigest(),
        )
        for name in critical_names
    )
    constants = (
        ("_REQUEST_SCHEMA", _REQUEST_SCHEMA),
        ("_UNOBSERVED_REQUEST_SCHEMA", _UNOBSERVED_REQUEST_SCHEMA),
        ("_PLAN_SCHEMA", _PLAN_SCHEMA),
        ("_EVIDENCE_SCHEMA", _EVIDENCE_SCHEMA),
        ("_SHARD_SCHEMA", _SHARD_SCHEMA),
        ("_DEPENDENCY_SCHEMA", _DEPENDENCY_SCHEMA),
        ("_DEPENDENCY_PAYLOAD_SCHEMA", _DEPENDENCY_PAYLOAD_SCHEMA),
        ("_HORIZON_POLICY_SCHEMA", _HORIZON_POLICY_SCHEMA),
        ("_REGIME_SUPPORT_BINDING_SCHEMA", _REGIME_SUPPORT_BINDING_SCHEMA),
        ("_REGIME_TRAIN_SUPPORT_SCHEMA", _REGIME_TRAIN_SUPPORT_SCHEMA),
        (
            "_REGIME_TRAIN_SUPPORT_VECTOR_ENCODING",
            _REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        ),
        (
            "_REGIME_TRAIN_SUPPORT_WARMUP_POLICY",
            _REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        ),
        ("_AUTHORITY_SCOPE", _AUTHORITY_SCOPE),
        ("_TRUST_CLASS", _TRUST_CLASS),
        ("_TIMEFRAME_SECONDS", _TIMEFRAME_SECONDS.copy()),
        ("_REGIME_ORDER", _REGIME_ORDER),
        ("_REGIMES", _REGIMES),
        ("_LOOKBACK_KINDS", _LOOKBACK_KINDS),
        ("_CAUSAL_ROLE", _CAUSAL_ROLE),
        ("_INTERPOLATION_ROLE", _INTERPOLATION_ROLE),
        ("_TERMINAL_ROLE", _TERMINAL_ROLE),
        ("_FOLD_ROLES", _FOLD_ROLES),
        ("_AVAILABLE", _AVAILABLE),
        ("_SOURCE_UNAVAILABLE", _SOURCE_UNAVAILABLE),
        ("_TECHNICAL_FAILURE", _TECHNICAL_FAILURE),
        ("_TECHNICAL_TERMINAL", _TECHNICAL_TERMINAL),
        ("_SOURCE_STATES", _SOURCE_STATES),
        ("_INTERNAL_STATES", _INTERNAL_STATES),
        ("_BOUNDARY_STATES", _BOUNDARY_STATES),
        ("_AUTHENTICATED", _AUTHENTICATED),
        ("_PROCESS_REJECTED", _PROCESS_REJECTED),
        ("_TECHNICAL_SHARD", _TECHNICAL_SHARD),
        ("_TECHNICAL_REASONS", _TECHNICAL_REASONS),
        ("_PROCESS_SCREEN_REASON", _PROCESS_SCREEN_REASON),
        ("_BOUNDARY_REASON", _BOUNDARY_REASON),
        ("_MAX_REQUEST_BYTES", _MAX_REQUEST_BYTES),
        ("_MAX_EVIDENCE_BYTES", _MAX_EVIDENCE_BYTES),
        ("_MAX_OUTPUT_BYTES", _MAX_OUTPUT_BYTES),
        ("_MAX_NODES", _MAX_NODES),
        ("_MAX_DEPTH", _MAX_DEPTH),
        ("_MAX_CONTAINER_ITEMS", _MAX_CONTAINER_ITEMS),
        ("_MAX_STRING_BYTES", _MAX_STRING_BYTES),
        ("_MAX_JSON_INTEGER_BITS", _MAX_JSON_INTEGER_BITS),
        ("_MAX_CANDIDATES", _MAX_CANDIDATES),
        ("_MAX_TRIALS", _MAX_TRIALS),
        ("_MAX_FOLDS_PER_CANDIDATE", _MAX_FOLDS_PER_CANDIDATE),
        ("_MAX_ATTEMPTS", _MAX_ATTEMPTS),
        ("_MAX_EXPECTED_EVALUATIONS", _MAX_EXPECTED_EVALUATIONS),
        ("_MAX_VECTOR_POINTS", _MAX_VECTOR_POINTS),
        ("__all__", __all__),
    )
    identity_globals = (
        ("hashlib", hashlib),
        ("json", json),
        ("mp", mp),
        ("re", re),
        ("datetime", datetime),
        ("timedelta", timedelta),
        ("timezone", timezone),
        ("Decimal", Decimal),
        ("InvalidOperation", InvalidOperation),
        ("_HorizonContractError", _HorizonContractError),
        ("_HistoricalHorizonPlan", plan_type),
        ("_HistoricalHorizonEvidence", evidence_type),
        ("_SHA256_RE", _SHA256_RE),
        ("_SAFE_ID_RE", _SAFE_ID_RE),
        ("_SYMBOL_RE", _SYMBOL_RE),
        ("_UTC_RE", _UTC_RE),
        ("_DATE_RE", _DATE_RE),
        ("_DECIMAL_RE", _DECIMAL_RE),
        ("_PLAN_SEAL", plan_seal),
        ("_EVIDENCE_SEAL", evidence_seal),
    )
    external_members = (
        (hashlib, "sha256", hashlib.sha256),
        (json, "dumps", json.dumps),
        (json, "loads", json.loads),
        (mp, "get_context", mp.get_context),
    )
    class_attributes = (
        (plan_type, "__init__", plan_type.__dict__["__init__"]),
        (plan_type, "__setattr__", plan_type.__dict__["__setattr__"]),
        (plan_type, "receipt_sha256", plan_type.__dict__["receipt_sha256"]),
        (plan_type, "to_dict", plan_type.__dict__["to_dict"]),
        (evidence_type, "__init__", evidence_type.__dict__["__init__"]),
        (evidence_type, "__setattr__", evidence_type.__dict__["__setattr__"]),
        (evidence_type, "receipt_sha256", evidence_type.__dict__["receipt_sha256"]),
        (evidence_type, "to_dict", evidence_type.__dict__["to_dict"]),
    )
    public_refs: dict[str, tuple[Any, Any]] = {}

    def guard_require(condition: bool, message: str) -> None:
        if not condition:
            raise ValueError(message)

    def guard() -> None:
        for name, expected in identity_globals:
            guard_require(
                globals().get(name) is expected, f"runtime identity rebound: {name}"
            )
        for owner, name, expected in external_members:
            current = getattr(owner, name, None)
            same_callable = current is expected or (
                getattr(current, "__self__", None)
                is getattr(expected, "__self__", None)
                and getattr(current, "__func__", None)
                is getattr(expected, "__func__", None)
                and getattr(expected, "__func__", None) is not None
            )
            guard_require(same_callable, f"runtime dependency rebound: {name}")
        for name, function, code_object, digest in critical:
            guard_require(
                globals().get(name) is function, f"runtime authority rebound: {name}"
            )
            guard_require(
                function.__code__ is code_object,
                f"runtime authority code rebound: {name}",
            )
            guard_require(
                sha256_impl(function.__code__.co_code).hexdigest() == digest,
                f"runtime authority code changed: {name}",
            )
            guard_require(
                function.__defaults__ is None,
                f"runtime helper defaults changed: {name}",
            )
            guard_require(
                function.__kwdefaults__ is None,
                f"runtime helper keyword defaults changed: {name}",
            )
        for name, expected in constants:
            current = globals().get(name)
            if type(expected) is dict:
                guard_require(
                    type(current) is dict and current == expected,
                    f"runtime constant changed: {name}",
                )
            else:
                guard_require(
                    current == expected and type(current) is type(expected),
                    f"runtime constant changed: {name}",
                )
        for owner, name, expected in class_attributes:
            guard_require(
                owner.__dict__.get(name) is expected, f"sealed class rebound: {name}"
            )
        for name, (function, code_object) in public_refs.items():
            guard_require(
                globals().get(name) is function, f"public authority rebound: {name}"
            )
            guard_require(
                function.__code__ is code_object,
                f"public authority code rebound: {name}",
            )
            guard_require(
                function.__defaults__ is None,
                f"public authority defaults changed: {name}",
            )
            guard_require(
                function.__kwdefaults__ is None,
                f"public authority keyword defaults changed: {name}",
            )

    def build(request: Any) -> _HistoricalHorizonPlan:
        guard()
        return build_impl(request)

    def evaluate(plan: Any, evidence_shards: Any) -> _HistoricalHorizonEvidence:
        guard()
        return evaluate_impl(plan, evidence_shards)

    def verify(plan: Any, evidence: Any) -> bool:
        guard()
        return verify_impl(plan, evidence)

    build.__name__ = "build_historical_horizon_plan"
    build.__qualname__ = "build_historical_horizon_plan"
    evaluate.__name__ = "evaluate_historical_horizon_plan"
    evaluate.__qualname__ = "evaluate_historical_horizon_plan"
    verify.__name__ = "verify_historical_horizon_evidence"
    verify.__qualname__ = "verify_historical_horizon_evidence"
    public_refs.update(
        {
            "build_historical_horizon_plan": (build, build.__code__),
            "evaluate_historical_horizon_plan": (evaluate, evaluate.__code__),
            "verify_historical_horizon_evidence": (verify, verify.__code__),
        }
    )
    return build, evaluate, verify


(
    build_historical_horizon_plan,
    evaluate_historical_horizon_plan,
    verify_historical_horizon_evidence,
) = _make_public_authority()
