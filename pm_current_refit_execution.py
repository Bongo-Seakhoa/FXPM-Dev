"""Bounded raw-replay contract for current-refit execution."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from pm_book_multiplicity import (
    REGIME_DETECTOR_PARAMETER_KEYS,
    REGIME_ORDER,
    compute_source_group_id,
)
from pm_model_lifecycle import (
    CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    CURRENT_REFIT_RECEIPT_VERSION,
    canonical_receipt_sha256,
    compute_current_refit_cell_id,
    open_typed_receipt,
)


RAW_CURRENT_REFIT_MANIFEST_VERSION = "fxpm.raw-current-refit-manifest.v2"
RAW_CURRENT_REFIT_EXECUTOR_VERSION = "fxpm.pipeline-current-refit-raw-executor.v2"
RAW_CURRENT_REFIT_REPLAY_REQUEST_VERSION = "fxpm.raw-current-refit-replay-request.v2"
RAW_CURRENT_REFIT_REPLAY_RESULT_VERSION = "fxpm.raw-current-refit-replay-result.v2"
RAW_CURRENT_REFIT_VERIFICATION_VERSION = "fxpm.raw-current-refit-verification.v2"
RAW_CURRENT_REFIT_EXECUTOR_FUNCTION = "replay_current_refit_execution_from_raw_manifest"
CURRENT_REGIME_PARAMS_BUNDLE_VERSION = "fxpm.current-regime-params-bundle.v1"
REGIME_DETECTOR_RECORD_VERSION = "fxpm.regime-detector-record/v1"
REGIME_FIT_RECEIPT_VERSION = "fxpm.historical-regime-fit.v4"

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_ARTIFACT_COUNT = 16_384
MAX_ARTIFACT_BYTES = 16 * 1024**3
MAX_TOTAL_ARTIFACT_BYTES = 2 * 1024**4
HASH_CHUNK_BYTES = 8 * 1024 * 1024
MAX_REFIT_CELLS = 2_400
MAX_REGIME_PARAMS_BUNDLE_BYTES = 16 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON_RE = re.compile(r"^[A-Z0-9_.:-]{1,160}$")
_DETECTOR_INTEGER_PARAMETERS = frozenset(
    {
        "k_confirm",
        "k_hold",
        "adx_period",
        "atr_period",
        "bb_period",
        "efficiency_period",
        "bb_squeeze_lookback",
        "atr_lookback",
        "structure_period",
    }
)
_DETECTOR_INTEGER_MAX = {
    "k_confirm": 64,
    "k_hold": 256,
    "adx_period": 512,
    "atr_period": 512,
    "bb_period": 512,
    "efficiency_period": 512,
    "bb_squeeze_lookback": 10_000,
    "atr_lookback": 10_000,
    "structure_period": 512,
}
_REGIME_PARAMS_BUNDLE_FIELDS = {
    "schema_version",
    "current_cutoff_utc",
    "regime_fits",
}
_REGIME_FIT_ENVELOPE_FIELDS = {"payload", "receipt_sha256"}
_REGIME_FIT_FIELDS = {
    "contract_version",
    "source_group_id",
    "source_group_decision_sha256",
    "symbol",
    "timeframe",
    "fit_state",
    "reason_code",
    "regime_fit_policy_sha256",
    "detector_process_sha256",
    "regime_fit_evidence_sha256",
    "four_regime_mapping_sha256",
    "regime_params_sha256",
    "regime_detector_record",
    "regime_detector_record_sha256",
    "train_regime_support",
    "complete",
}
_REGIME_DETECTOR_RECORD_FIELDS = {
    "schema_version",
    "symbol",
    "timeframe",
    "source_group_id",
    "source_group_decision_sha256",
    "source_contract_sha256",
    "source_group_evidence_sha256",
    "activation_id",
    "activation_manifest_sha256",
    "native_source_sha256",
    "fit_cutoff_utc",
    "detector_version",
    "detector_code_sha256",
    "parameters_canonical_json",
    "parameters_sha256",
    "record_sha256",
}
_TRAIN_SUPPORT_FIELDS = {
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
}
_TRAIN_SUPPORT_BINDING_FIELDS = {
    "symbol",
    "timeframe",
    "activation_id",
    "activation_manifest_sha256",
    "source_sha256",
    "train_window_sha256",
    "params_sha256",
    "detector_code_sha256",
}
_REFIT_PAYLOAD_FIELDS = {
    "schema_version",
    "authorization",
    "authorization_sha256",
    "execution",
    "execution_sha256",
}
_AUTHORIZATION_FIELDS = {
    "authorization_id",
    "historical_process_decision_sha256",
    "process_id",
    "process_definition_sha256",
    "accepted_book_generation_receipt_sha256",
    "accepted_book_generation_sha256",
    "current_cutoff_utc",
    "issued_at_utc",
    "source_snapshot_bundle_sha256",
    "regime_params_sha256",
    "horizon_decisions_sha256",
    "selected_winner_roster_sha256",
    "one_touch_nonce_sha256",
}
_EXECUTION_FIELDS = {
    "authorization_sha256",
    "process_definition_sha256",
    "accepted_book_generation_sha256",
    "started_at_utc",
    "completed_at_utc",
    "current_cutoff_utc",
    "output_book_sha256",
    "cell_execution_bundle_sha256",
    "execution_trace_sha256",
    "cell_executions",
    "attempts",
    "status",
    "refit_process_frozen",
    "post_authorization_policy_change",
    "parameter_level_oos_evidence_claimed",
}
_CELL_FIELDS = {
    "current_refit_cell_id",
    "historical_process_decision_sha256",
    "accepted_book_generation_receipt_sha256",
    "symbol",
    "timeframe",
    "regime",
    "strategy_id",
    "winner_process_candidate_id",
    "winner_process_sha256",
    "winner_horizon_process_sha256",
    "winner_trial_definition_sha256",
    "winner_refit_recipe_sha256",
    "winner_decision_sha256",
    "winner_book_entry_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
    "source_snapshot_sha256",
    "parameter_sha256",
    "output_artifact_sha256",
    "execution_trace_sha256",
    "attempts",
    "status",
    "refit_process_frozen",
    "parameter_level_oos_evidence_claimed",
}
_CELL_IDENTITY_FIELDS = _CELL_FIELDS - {
    "current_refit_cell_id",
    "execution_trace_sha256",
    "attempts",
    "status",
    "refit_process_frozen",
    "parameter_level_oos_evidence_claimed",
}
_MANIFEST_FIELDS = {
    "schema_version",
    "executor_contract_version",
    "current_refit_receipt_sha256",
    "current_refit_execution_sha256",
    "current_cutoff_utc",
    "bundle_artifact_ids",
    "artifact_descriptors",
    "cell_requests",
    "manifest_sha256",
}
_ARTIFACT_FIELDS = {
    "artifact_id",
    "relative_path",
    "size_bytes",
    "content_sha256",
    "semantic_sha256",
    "format_version",
}
_BUNDLE_ROLES = {
    "accepted_book_generation",
    "source_snapshot_bundle",
    "regime_params_bundle",
    "horizon_decisions_bundle",
}
_CELL_ARTIFACT_ROLES = {
    "source_snapshot",
    "winner_process",
    "horizon_process",
    "trial_definition",
    "refit_recipe",
    "winner_book_entry",
}
_CELL_REQUEST_FIELDS = {
    "current_refit_cell_id",
    "symbol",
    "timeframe",
    "regime",
    "strategy_id",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
    "source_snapshot_sha256",
    "winner_process_sha256",
    "winner_horizon_process_sha256",
    "winner_trial_definition_sha256",
    "winner_refit_recipe_sha256",
    "winner_book_entry_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
    "artifact_ids",
}
_EXECUTOR_CELL_FIELDS = {
    "current_refit_cell_id",
    "symbol",
    "timeframe",
    "regime",
    "strategy_id",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
    "artifact_ids",
}
_REPLAY_RESULT_FIELDS = {
    "schema_version",
    "executor_contract_version",
    "current_cutoff_utc",
    "cell_results",
    "cell_execution_bundle_sha256",
    "output_book_sha256",
    "execution_trace_sha256",
}
_CELL_RESULT_FIELDS = {
    "current_refit_cell_id",
    "parameter_sha256",
    "output_artifact_sha256",
    "execution_trace_sha256",
    "source_snapshot_sha256",
    "winner_horizon_process_sha256",
    "winner_trial_definition_sha256",
    "winner_refit_recipe_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
}


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise ValueError("RAW_CURRENT_REFIT_MANIFEST_TOO_LARGE")
    return encoded


def _sha(value: Any) -> bool:
    return type(value) is str and _SHA256_RE.fullmatch(value) is not None


def _canonical_ascii_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _exact_dict(value: Any, fields: set[str], code: str) -> Dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError(code)
    return value


def _bounded_text(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 256
        or len(value.encode("utf-8")) > 512
    ):
        raise ValueError(code)
    return value


def _canonical_utc(value: Any, code: str) -> str:
    text = _bounded_text(value, code)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        normalized = parsed.astimezone(timezone.utc).isoformat()
    except Exception as exc:
        raise ValueError(code) from exc
    if text != normalized:
        raise ValueError(code)
    return text


def _normalize_detector_parameters(value: Any) -> Dict[str, int | float]:
    fields = set(REGIME_DETECTOR_PARAMETER_KEYS)
    parameters = _exact_dict(value, fields, "REGIME_DETECTOR_PARAMETERS_INVALID")
    normalized: Dict[str, int | float] = {}
    for name in REGIME_DETECTOR_PARAMETER_KEYS:
        item = parameters[name]
        if name in _DETECTOR_INTEGER_PARAMETERS:
            if type(item) is not int or not 1 <= item <= _DETECTOR_INTEGER_MAX[name]:
                raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
        elif type(item) is not float or not math.isfinite(item):
            raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
        normalized[name] = item
    unit_fields = {"gap_min"}
    percent_fields = {"bb_squeeze_percentile", "adx_trend_threshold"}
    weights = {name for name in parameters if name.endswith("_weight")}
    if any(not 0.0 <= parameters[name] <= 1.0 for name in unit_fields | weights):
        raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
    if any(not 0.0 <= parameters[name] <= 100.0 for name in percent_fields):
        raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
    if not 0.0 < parameters["bb_std"] <= 10.0:
        raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
    if not 0.0 <= parameters["structure_atr_mult"] <= 10.0:
        raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
    for prefix in ("trend", "range", "breakout", "chop"):
        group = [parameters[name] for name in weights if name.startswith(prefix)]
        if not math.isclose(sum(group), 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
    return normalized


def _normalize_detector_record(
    value: Any,
    *,
    current_cutoff_utc: str,
) -> Dict[str, Any]:
    record = _exact_dict(
        value,
        _REGIME_DETECTOR_RECORD_FIELDS,
        "REGIME_DETECTOR_RECORD_FIELDS_INVALID",
    )
    if record["schema_version"] != REGIME_DETECTOR_RECORD_VERSION:
        raise ValueError("REGIME_DETECTOR_RECORD_VERSION_INVALID")
    symbol = record["symbol"]
    timeframe = record["timeframe"]
    group_id = compute_source_group_id(symbol, timeframe)
    if group_id is None or record["source_group_id"] != group_id:
        raise ValueError("REGIME_DETECTOR_SOURCE_GROUP_INVALID")
    for name in (
        "source_group_decision_sha256",
        "source_contract_sha256",
        "source_group_evidence_sha256",
        "activation_manifest_sha256",
        "native_source_sha256",
        "detector_code_sha256",
        "parameters_sha256",
        "record_sha256",
    ):
        if not _sha(record[name]):
            raise ValueError(f"REGIME_DETECTOR_SHA256_INVALID:{name}")
    _bounded_text(record["activation_id"], "REGIME_DETECTOR_ACTIVATION_ID_INVALID")
    _bounded_text(record["detector_version"], "REGIME_DETECTOR_VERSION_ID_INVALID")
    cutoff = _canonical_utc(record["fit_cutoff_utc"], "REGIME_DETECTOR_CUTOFF_INVALID")
    if cutoff != current_cutoff_utc:
        raise ValueError("REGIME_DETECTOR_CURRENT_CUTOFF_MISMATCH")
    parameters_text = record["parameters_canonical_json"]
    if (
        type(parameters_text) is not str
        or not parameters_text.isascii()
        or not 1 <= len(parameters_text.encode("ascii")) <= 8192
    ):
        raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID")
    try:
        parameters = _normalize_detector_parameters(json.loads(parameters_text))
    except Exception as exc:
        raise ValueError("REGIME_DETECTOR_PARAMETERS_INVALID") from exc
    canonical_parameters = _canonical_ascii_bytes(parameters)
    if canonical_parameters.decode("ascii") != parameters_text:
        raise ValueError("REGIME_DETECTOR_PARAMETERS_NONCANONICAL")
    if hashlib.sha256(canonical_parameters).hexdigest() != record["parameters_sha256"]:
        raise ValueError("REGIME_DETECTOR_PARAMETERS_SHA256_INVALID")
    body = dict(record)
    declared_record_sha = body.pop("record_sha256")
    if hashlib.sha256(_canonical_ascii_bytes(body)).hexdigest() != declared_record_sha:
        raise ValueError("REGIME_DETECTOR_RECORD_SHA256_INVALID")
    return json.loads(_canonical_ascii_bytes(record).decode("ascii"))


def _normalize_train_support(
    value: Any,
    *,
    detector_record: Mapping[str, Any],
) -> Dict[str, Any]:
    support = _exact_dict(
        value, _TRAIN_SUPPORT_FIELDS, "REGIME_TRAIN_SUPPORT_FIELDS_INVALID"
    )
    if (
        support["schema_version"] != "fxpm.regime-train-support/v1"
        or support["symbol"] != detector_record["symbol"]
        or support["timeframe"] != detector_record["timeframe"]
        or support["surface"] != "REGIME_LIVE"
        or support["warmup_policy"] != "CURRENT_AND_SOURCE_BAR_NOT_WARMUP"
        or support["label_order"] != list(REGIME_ORDER)
        or support["vector_encoding"] != "fxpm.regime-live-labels-nul/v1"
    ):
        raise ValueError("REGIME_TRAIN_SUPPORT_SEMANTICS_INVALID")
    counts = support["counts"]
    if type(counts) is not dict or set(counts) != set(REGIME_ORDER):
        raise ValueError("REGIME_TRAIN_SUPPORT_COUNTS_INVALID")
    if any(type(counts[name]) is not int or counts[name] < 0 for name in REGIME_ORDER):
        raise ValueError("REGIME_TRAIN_SUPPORT_COUNTS_INVALID")
    eligible = support["eligible_rows"]
    if type(eligible) is not int or not 1 <= eligible <= 10_000_000:
        raise ValueError("REGIME_TRAIN_SUPPORT_COUNTS_INVALID")
    if sum(counts.values()) != eligible:
        raise ValueError("REGIME_TRAIN_SUPPORT_COUNTS_INVALID")
    observed = [name for name in REGIME_ORDER if counts[name] > 0]
    unobserved = [name for name in REGIME_ORDER if counts[name] == 0]
    states = {
        name: "EMPIRICALLY_OBSERVED" if counts[name] > 0 else "UNOBSERVED"
        for name in REGIME_ORDER
    }
    if (
        support["observed_regimes"] != observed
        or support["unobserved_regimes"] != unobserved
        or support["support_state_by_regime"] != states
    ):
        raise ValueError("REGIME_TRAIN_SUPPORT_PARTITION_INVALID")
    try:
        first = datetime.fromisoformat(str(support["first_ts"]).replace("Z", "+00:00"))
        last = datetime.fromisoformat(str(support["last_ts"]).replace("Z", "+00:00"))
        cutoff = datetime.fromisoformat(
            detector_record["fit_cutoff_utc"].replace("Z", "+00:00")
        )
        if first.tzinfo is None or last.tzinfo is None or not first <= last <= cutoff:
            raise ValueError
    except Exception as exc:
        raise ValueError("REGIME_TRAIN_SUPPORT_TIME_RANGE_INVALID") from exc
    if not _sha(support["vector_sha256"]):
        raise ValueError("REGIME_TRAIN_SUPPORT_VECTOR_INVALID")
    bindings = _exact_dict(
        support["bindings"],
        _TRAIN_SUPPORT_BINDING_FIELDS,
        "REGIME_TRAIN_SUPPORT_BINDINGS_INVALID",
    )
    expected_bindings = {
        "symbol": detector_record["symbol"],
        "timeframe": detector_record["timeframe"],
        "activation_id": detector_record["activation_id"],
        "activation_manifest_sha256": detector_record["activation_manifest_sha256"],
        "source_sha256": detector_record["native_source_sha256"],
        "train_window_sha256": bindings.get("train_window_sha256"),
        "params_sha256": detector_record["parameters_sha256"],
        "detector_code_sha256": detector_record["detector_code_sha256"],
    }
    if not _sha(bindings.get("train_window_sha256")) or bindings != expected_bindings:
        raise ValueError("REGIME_TRAIN_SUPPORT_BINDINGS_INVALID")
    receipt_sha = support["receipt_sha256"]
    body = dict(support)
    body.pop("receipt_sha256")
    if (
        not _sha(receipt_sha)
        or hashlib.sha256(_canonical_ascii_bytes(body)).hexdigest() != receipt_sha
    ):
        raise ValueError("REGIME_TRAIN_SUPPORT_RECEIPT_INVALID")
    return json.loads(_canonical_ascii_bytes(support).decode("ascii"))


def _normalize_regime_fit(
    value: Any,
    *,
    current_cutoff_utc: str,
) -> Dict[str, Any]:
    envelope = _exact_dict(
        value, _REGIME_FIT_ENVELOPE_FIELDS, "REGIME_FIT_ENVELOPE_FIELDS_INVALID"
    )
    payload = _exact_dict(
        envelope["payload"], _REGIME_FIT_FIELDS, "REGIME_FIT_FIELDS_INVALID"
    )
    receipt_sha = envelope["receipt_sha256"]
    if (
        not _sha(receipt_sha)
        or hashlib.sha256(_canonical_ascii_bytes(payload)).hexdigest() != receipt_sha
    ):
        raise ValueError("REGIME_FIT_RECEIPT_SHA256_INVALID")
    if (
        payload["contract_version"] != REGIME_FIT_RECEIPT_VERSION
        or payload["fit_state"] != "FITTED"
        or payload["complete"] is not True
        or type(payload["reason_code"]) is not str
        or _SAFE_REASON_RE.fullmatch(payload["reason_code"]) is None
    ):
        raise ValueError("REGIME_FIT_SEMANTICS_INVALID")
    record = _normalize_detector_record(
        payload["regime_detector_record"],
        current_cutoff_utc=current_cutoff_utc,
    )
    if (
        payload["symbol"] != record["symbol"]
        or payload["timeframe"] != record["timeframe"]
        or payload["source_group_id"] != record["source_group_id"]
        or payload["source_group_decision_sha256"]
        != record["source_group_decision_sha256"]
    ):
        raise ValueError("REGIME_FIT_SOURCE_SPLICE")
    for name in (
        "regime_fit_policy_sha256",
        "regime_fit_evidence_sha256",
        "four_regime_mapping_sha256",
    ):
        if not _sha(payload[name]):
            raise ValueError(f"REGIME_FIT_SHA256_INVALID:{name}")
    if (
        payload["detector_process_sha256"] != record["detector_code_sha256"]
        or payload["regime_params_sha256"] != record["parameters_sha256"]
        or payload["regime_detector_record_sha256"] != record["record_sha256"]
    ):
        raise ValueError("REGIME_FIT_DETECTOR_SPLICE")
    _normalize_train_support(payload["train_regime_support"], detector_record=record)
    return json.loads(_canonical_ascii_bytes(envelope).decode("ascii"))


def build_current_regime_params_bundle(
    *,
    current_cutoff_utc: Any,
    regime_fits: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build the selected current Book detector roster."""
    cutoff = _canonical_utc(current_cutoff_utc, "REGIME_PARAMS_BUNDLE_CUTOFF_INVALID")
    if type(regime_fits) is not list or not 1 <= len(regime_fits) <= 600:
        raise ValueError("REGIME_PARAMS_BUNDLE_ROSTER_INVALID")
    fits = [
        _normalize_regime_fit(item, current_cutoff_utc=cutoff) for item in regime_fits
    ]
    order = {
        name: index for index, name in enumerate(("M5", "M15", "M30", "H1", "H4", "D1"))
    }
    try:
        canonical_order = sorted(
            fits,
            key=lambda item: (
                item["payload"]["symbol"],
                order[item["payload"]["timeframe"]],
            ),
        )
    except KeyError as exc:
        raise ValueError("REGIME_PARAMS_BUNDLE_ROSTER_INVALID") from exc
    if fits != canonical_order:
        raise ValueError("REGIME_PARAMS_BUNDLE_ORDER_INVALID")
    groups = [
        (item["payload"]["symbol"], item["payload"]["timeframe"]) for item in fits
    ]
    receipts = [item["receipt_sha256"] for item in fits]
    records = [item["payload"]["regime_detector_record_sha256"] for item in fits]
    if (
        len(set(groups)) != len(groups)
        or len(set(receipts)) != len(receipts)
        or len(set(records)) != len(records)
    ):
        raise ValueError("REGIME_PARAMS_BUNDLE_DUPLICATE_IDENTITY")
    bundle = {
        "schema_version": CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
        "current_cutoff_utc": cutoff,
        "regime_fits": fits,
    }
    _canonical_bytes(bundle)
    return bundle


def validate_current_regime_params_bundle(
    value: Any,
    *,
    current_cutoff_utc: Any,
    selected_cells: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Validate detector records and bind them to every selected cell."""
    bundle = _exact_dict(
        value, _REGIME_PARAMS_BUNDLE_FIELDS, "REGIME_PARAMS_BUNDLE_FIELDS_INVALID"
    )
    if bundle["schema_version"] != CURRENT_REGIME_PARAMS_BUNDLE_VERSION:
        raise ValueError("REGIME_PARAMS_BUNDLE_VERSION_INVALID")
    normalized = build_current_regime_params_bundle(
        current_cutoff_utc=current_cutoff_utc,
        regime_fits=bundle["regime_fits"],
    )
    if normalized["current_cutoff_utc"] != bundle["current_cutoff_utc"]:
        raise ValueError("REGIME_PARAMS_BUNDLE_CUTOFF_MISMATCH")
    if type(selected_cells) is not list or not selected_cells:
        raise ValueError("REGIME_PARAMS_BUNDLE_SELECTED_CELLS_INVALID")
    by_group = {
        (item["payload"]["symbol"], item["payload"]["timeframe"]): item
        for item in normalized["regime_fits"]
    }
    selected_groups: set[tuple[str, str]] = set()
    for cell in selected_cells:
        if not isinstance(cell, Mapping):
            raise ValueError("REGIME_PARAMS_BUNDLE_SELECTED_CELLS_INVALID")
        group = (cell.get("symbol"), cell.get("timeframe"))
        selected_groups.add(group)
        fit = by_group.get(group)
        if fit is None:
            raise ValueError("REGIME_PARAMS_BUNDLE_SELECTED_GROUP_MISSING")
        record = fit["payload"]["regime_detector_record"]
        expected = {
            "regime_fit_receipt_sha256": fit["receipt_sha256"],
            "regime_detector_record_sha256": record["record_sha256"],
            "regime_detector_parameters_sha256": record["parameters_sha256"],
            "regime_detector_code_sha256": record["detector_code_sha256"],
        }
        if any(cell.get(name) != digest for name, digest in expected.items()):
            raise ValueError("REGIME_PARAMS_BUNDLE_SELECTED_CELL_SPLICE")
    if selected_groups != set(by_group):
        raise ValueError("REGIME_PARAMS_BUNDLE_UNSELECTED_GROUP_PRESENT")
    return normalized


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _load_canonical_json_artifact(
    path: Path,
    *,
    expected_size: int,
    expected_content_sha256: str,
    maximum_bytes: int,
    code_prefix: str,
) -> Dict[str, Any]:
    if not 0 < expected_size <= maximum_bytes:
        raise ValueError(f"{code_prefix}_FILE_SIZE_INVALID")
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        raw = handle.read(maximum_bytes + 1)
        after = os.fstat(handle.fileno())
    if (
        before.st_size != expected_size
        or after.st_size != expected_size
        or len(raw) != expected_size
        or len(raw) > maximum_bytes
        or hashlib.sha256(raw).hexdigest() != expected_content_sha256
    ):
        raise ValueError(f"{code_prefix}_FILE_CHANGED")
    try:
        value = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"{code_prefix}_FILE_INVALID") from exc
    if type(value) is not dict or _canonical_bytes(value) != raw:
        raise ValueError(f"{code_prefix}_FILE_NONCANONICAL")
    return value


def load_bounded_raw_current_refit_manifest(path: Any) -> Dict[str, Any]:
    """Load one manifest without unbounded reads."""
    target = Path(path)
    size = target.stat().st_size
    if not 0 < size <= MAX_MANIFEST_BYTES:
        raise ValueError("RAW_CURRENT_REFIT_MANIFEST_FILE_SIZE_INVALID")
    raw = target.read_bytes()
    if len(raw) != size:
        raise ValueError("RAW_CURRENT_REFIT_MANIFEST_FILE_CHANGED")
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("RAW_CURRENT_REFIT_MANIFEST_FILE_INVALID")
    return value


def _uses_reparse_point(root: Path, path: Path) -> bool:
    try:
        current = root
        for part in path.relative_to(root).parts:
            current = current / part
            info = current.lstat()
            attributes = int(getattr(info, "st_file_attributes", 0) or 0)
            if current.is_symlink() or attributes & 0x400:
                return True
        return False
    except Exception:
        return True


def _artifact_identity(descriptor: Mapping[str, Any]) -> str:
    body = {key: descriptor[key] for key in _ARTIFACT_FIELDS - {"artifact_id"}}
    return canonical_receipt_sha256(body)


def build_raw_artifact_descriptor(
    *,
    relative_path: str,
    size_bytes: int,
    content_sha256: str,
    semantic_sha256: str,
    format_version: str,
) -> Dict[str, Any]:
    """Build one content and semantic identity descriptor."""
    descriptor = {
        "relative_path": relative_path,
        "size_bytes": size_bytes,
        "content_sha256": content_sha256,
        "semantic_sha256": semantic_sha256,
        "format_version": format_version,
    }
    descriptor["artifact_id"] = canonical_receipt_sha256(descriptor)
    return descriptor


def build_raw_current_refit_manifest(
    *,
    current_refit_receipt: Mapping[str, Any],
    bundle_artifact_ids: Mapping[str, str],
    artifact_descriptors: Sequence[Mapping[str, Any]],
    cell_requests: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build the exact producer handoff; outputs are deliberately absent."""
    payload, receipt_sha = open_typed_receipt(
        current_refit_receipt,
        expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    execution = payload.get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("CURRENT_REFIT_EXECUTION_INVALID")
    manifest = {
        "schema_version": RAW_CURRENT_REFIT_MANIFEST_VERSION,
        "executor_contract_version": RAW_CURRENT_REFIT_EXECUTOR_VERSION,
        "current_refit_receipt_sha256": receipt_sha,
        "current_refit_execution_sha256": payload.get("execution_sha256"),
        "current_cutoff_utc": execution.get("current_cutoff_utc"),
        "bundle_artifact_ids": dict(bundle_artifact_ids),
        "artifact_descriptors": [dict(item) for item in artifact_descriptors],
        "cell_requests": [dict(item) for item in cell_requests],
    }
    manifest["manifest_sha256"] = canonical_receipt_sha256(manifest)
    _canonical_bytes(manifest)
    return manifest


def _receipt_contract(
    current_refit_receipt: Mapping[str, Any],
) -> Tuple[Optional[Dict[str, Any]], list[str]]:
    errors: list[str] = []
    try:
        payload, receipt_sha = open_typed_receipt(
            current_refit_receipt,
            expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
        )
    except Exception:
        return None, ["CURRENT_REFIT_RECEIPT_INVALID"]
    if set(payload) != _REFIT_PAYLOAD_FIELDS:
        return None, ["CURRENT_REFIT_RECEIPT_FIELDS_INVALID"]
    if payload.get("schema_version") != CURRENT_REFIT_RECEIPT_VERSION:
        return None, ["CURRENT_REFIT_RECEIPT_VERSION_INVALID"]
    authorization = payload.get("authorization")
    execution = payload.get("execution")
    if (
        not isinstance(authorization, Mapping)
        or set(authorization) != _AUTHORIZATION_FIELDS
        or not isinstance(execution, Mapping)
        or set(execution) != _EXECUTION_FIELDS
    ):
        return None, ["CURRENT_REFIT_RECEIPT_BODY_INVALID"]
    authorization_sha = payload.get("authorization_sha256")
    if (
        not _sha(authorization_sha)
        or authorization_sha != canonical_receipt_sha256(dict(authorization))
        or execution.get("authorization_sha256") != authorization_sha
    ):
        errors.append("CURRENT_REFIT_AUTHORIZATION_SHA256_INVALID")
    execution_sha = payload.get("execution_sha256")
    if not _sha(execution_sha) or execution_sha != canonical_receipt_sha256(
        dict(execution)
    ):
        errors.append("CURRENT_REFIT_EXECUTION_SHA256_INVALID")
    cells = execution.get("cell_executions")
    if (
        not isinstance(cells, list)
        or not 1 <= len(cells) <= MAX_REFIT_CELLS
        or any(
            not isinstance(cell, Mapping) or set(cell) != _CELL_FIELDS for cell in cells
        )
    ):
        errors.append("CURRENT_REFIT_CELL_ROSTER_INVALID")
        cells = []
    cell_ids = [cell.get("current_refit_cell_id") for cell in cells]
    if any(not _sha(value) for value in cell_ids) or len(set(cell_ids)) != len(
        cell_ids
    ):
        errors.append("CURRENT_REFIT_CELL_IDENTITIES_INVALID")
    required_top_shas = {
        "accepted_book_generation_receipt_sha256",
        "source_snapshot_bundle_sha256",
        "regime_params_sha256",
        "horizon_decisions_sha256",
    }
    if any(not _sha(authorization.get(name)) for name in required_top_shas):
        errors.append("CURRENT_REFIT_AUTHORIZATION_BINDINGS_INVALID")
    for cell in cells:
        for name in (
            "source_snapshot_sha256",
            "winner_process_sha256",
            "winner_horizon_process_sha256",
            "winner_trial_definition_sha256",
            "winner_refit_recipe_sha256",
            "winner_book_entry_sha256",
            "regime_fit_receipt_sha256",
            "regime_detector_record_sha256",
            "regime_detector_parameters_sha256",
            "regime_detector_code_sha256",
            "parameter_sha256",
            "output_artifact_sha256",
            "execution_trace_sha256",
        ):
            if not _sha(cell.get(name)):
                errors.append(
                    f"CURRENT_REFIT_CELL_BINDING_INVALID:{cell.get('current_refit_cell_id')}:{name}"
                )
        try:
            identity = {name: cell[name] for name in _CELL_IDENTITY_FIELDS}
            expected_id = compute_current_refit_cell_id(identity)
        except Exception:
            expected_id = None
        if expected_id != cell.get("current_refit_cell_id"):
            errors.append(
                f"CURRENT_REFIT_CELL_ID_INVALID:{cell.get('current_refit_cell_id')}"
            )
        if (
            type(cell.get("attempts")) is not int
            or cell.get("attempts") != 1
            or cell.get("status") != "COMPLETE"
            or cell.get("refit_process_frozen") is not True
            or cell.get("parameter_level_oos_evidence_claimed") is not False
        ):
            errors.append(
                f"CURRENT_REFIT_CELL_EXECUTION_STATE_INVALID:{cell.get('current_refit_cell_id')}"
            )
    detector_fields = (
        "regime_fit_receipt_sha256",
        "regime_detector_record_sha256",
        "regime_detector_parameters_sha256",
        "regime_detector_code_sha256",
    )
    detectors: Dict[tuple[Any, Any], tuple[Any, ...]] = {}
    fit_owners: Dict[Any, tuple[Any, Any]] = {}
    record_owners: Dict[Any, tuple[Any, Any]] = {}
    for cell in cells:
        group = (cell.get("symbol"), cell.get("timeframe"))
        binding = tuple(cell.get(name) for name in detector_fields)
        if detectors.setdefault(group, binding) != binding:
            errors.append(
                f"CURRENT_REFIT_SIBLING_DETECTOR_MISMATCH:{group[0]}:{group[1]}"
            )
        for digest, owners in ((binding[0], fit_owners), (binding[1], record_owners)):
            if owners.setdefault(digest, group) != group:
                errors.append("CURRENT_REFIT_DETECTOR_IDENTITY_RELABELLED")
    if execution.get("cell_execution_bundle_sha256") != canonical_receipt_sha256(
        {"cell_executions": cells}
    ):
        errors.append("CURRENT_REFIT_CELL_BUNDLE_SHA256_INVALID")
    if any(
        not _sha(execution.get(name))
        for name in (
            "output_book_sha256",
            "cell_execution_bundle_sha256",
            "execution_trace_sha256",
        )
    ):
        errors.append("CURRENT_REFIT_EXECUTION_OUTPUT_BINDINGS_INVALID")
    if (
        type(execution.get("attempts")) is not int
        or execution.get("attempts") != 1
        or execution.get("status") != "COMPLETE"
        or execution.get("refit_process_frozen") is not True
        or execution.get("post_authorization_policy_change") is not False
        or execution.get("parameter_level_oos_evidence_claimed") is not False
        or execution.get("current_cutoff_utc")
        != authorization.get("current_cutoff_utc")
    ):
        errors.append("CURRENT_REFIT_EXECUTION_STATE_INVALID")
    contract = {
        "receipt_sha256": receipt_sha,
        "execution_sha256": execution_sha,
        "current_cutoff_utc": execution.get("current_cutoff_utc"),
        "authorization": dict(authorization),
        "execution": dict(execution),
        "cells": [dict(cell) for cell in cells],
    }
    return contract, list(dict.fromkeys(errors))


def validate_raw_current_refit_manifest(
    current_refit_receipt: Mapping[str, Any],
    raw_manifest: Any,
    *,
    artifact_root: Any,
) -> Tuple[Optional[Dict[str, Any]], list[str]]:
    """Validate and stream-hash every unique raw input artifact."""
    contract, errors = _receipt_contract(current_refit_receipt)
    if contract is None:
        return None, errors
    if not isinstance(raw_manifest, Mapping) or set(raw_manifest) != _MANIFEST_FIELDS:
        return None, errors + ["RAW_CURRENT_REFIT_MANIFEST_FIELDS_INVALID"]
    try:
        _canonical_bytes(dict(raw_manifest))
    except (TypeError, ValueError, OverflowError):
        return None, errors + ["RAW_CURRENT_REFIT_MANIFEST_TOO_LARGE_OR_INVALID"]
    if raw_manifest.get("schema_version") != RAW_CURRENT_REFIT_MANIFEST_VERSION:
        errors.append("RAW_CURRENT_REFIT_MANIFEST_VERSION_INVALID")
    if (
        raw_manifest.get("executor_contract_version")
        != RAW_CURRENT_REFIT_EXECUTOR_VERSION
    ):
        errors.append("RAW_CURRENT_REFIT_EXECUTOR_VERSION_INVALID")
    manifest_body = dict(raw_manifest)
    manifest_sha = manifest_body.pop("manifest_sha256", None)
    if not _sha(manifest_sha) or manifest_sha != canonical_receipt_sha256(
        manifest_body
    ):
        errors.append("RAW_CURRENT_REFIT_MANIFEST_SHA256_INVALID")
    for field in (
        "current_refit_receipt_sha256",
        "current_refit_execution_sha256",
        "current_cutoff_utc",
    ):
        expected = contract[
            {
                "current_refit_receipt_sha256": "receipt_sha256",
                "current_refit_execution_sha256": "execution_sha256",
                "current_cutoff_utc": "current_cutoff_utc",
            }[field]
        ]
        if raw_manifest.get(field) != expected:
            errors.append(f"RAW_CURRENT_REFIT_{field.upper()}_MISMATCH")

    descriptors = raw_manifest.get("artifact_descriptors")
    if (
        not isinstance(descriptors, list)
        or not 1 <= len(descriptors) <= MAX_ARTIFACT_COUNT
    ):
        return None, errors + ["RAW_CURRENT_REFIT_ARTIFACT_ROSTER_INVALID"]
    root = Path(artifact_root) if artifact_root not in (None, "") else None
    try:
        root = root.resolve(strict=True) if root is not None else None
    except Exception:
        root = None
    if root is None or not root.is_dir():
        errors.append("RAW_CURRENT_REFIT_ARTIFACT_ROOT_INVALID")

    artifacts: Dict[str, Dict[str, Any]] = {}
    artifact_paths: Dict[str, Path] = {}
    total_bytes = 0
    paths: set[str] = set()
    for raw in descriptors:
        if not isinstance(raw, Mapping) or set(raw) != _ARTIFACT_FIELDS:
            errors.append("RAW_CURRENT_REFIT_ARTIFACT_DESCRIPTOR_INVALID")
            continue
        descriptor = dict(raw)
        artifact_id = descriptor.get("artifact_id")
        size = descriptor.get("size_bytes")
        relative = descriptor.get("relative_path")
        if not _sha(artifact_id) or artifact_id != _artifact_identity(descriptor):
            errors.append("RAW_CURRENT_REFIT_ARTIFACT_ID_INVALID")
            continue
        if artifact_id in artifacts:
            errors.append(f"RAW_CURRENT_REFIT_ARTIFACT_DUPLICATE:{artifact_id}")
            continue
        if (
            type(size) is not int
            or not 0 < size <= MAX_ARTIFACT_BYTES
            or not _sha(descriptor.get("content_sha256"))
            or not _sha(descriptor.get("semantic_sha256"))
            or not isinstance(descriptor.get("format_version"), str)
            or not 0 < len(descriptor["format_version"].encode("utf-8")) <= 256
            or not isinstance(relative, str)
            or not relative
        ):
            errors.append(f"RAW_CURRENT_REFIT_ARTIFACT_METADATA_INVALID:{artifact_id}")
            continue
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts or relative in paths:
            errors.append(f"RAW_CURRENT_REFIT_ARTIFACT_PATH_INVALID:{artifact_id}")
            continue
        paths.add(relative)
        total_bytes += size
        if total_bytes > MAX_TOTAL_ARTIFACT_BYTES:
            errors.append("RAW_CURRENT_REFIT_TOTAL_ARTIFACT_BYTES_EXCEEDED")
            continue
        if root is not None:
            try:
                path = (root / candidate).resolve(strict=True)
                path.relative_to(root)
                actual_size = path.stat().st_size
                valid_file = path.is_file() and not _uses_reparse_point(root, path)
            except Exception:
                path = None
                actual_size = -1
                valid_file = False
            if not valid_file or actual_size != size:
                errors.append(f"RAW_CURRENT_REFIT_ARTIFACT_FILE_INVALID:{artifact_id}")
            elif _stream_sha256(path) != descriptor["content_sha256"]:
                errors.append(
                    f"RAW_CURRENT_REFIT_ARTIFACT_CONTENT_MISMATCH:{artifact_id}"
                )
            else:
                artifact_paths[artifact_id] = path
        artifacts[artifact_id] = descriptor

    bundle_ids = raw_manifest.get("bundle_artifact_ids")
    if not isinstance(bundle_ids, Mapping) or set(bundle_ids) != _BUNDLE_ROLES:
        errors.append("RAW_CURRENT_REFIT_BUNDLE_ARTIFACT_ROSTER_INVALID")
        bundle_ids = {}
    authorization = contract["authorization"]
    expected_bundle_semantics = {
        "accepted_book_generation": authorization.get(
            "accepted_book_generation_receipt_sha256"
        ),
        "source_snapshot_bundle": authorization.get("source_snapshot_bundle_sha256"),
        "regime_params_bundle": authorization.get("regime_params_sha256"),
        "horizon_decisions_bundle": authorization.get("horizon_decisions_sha256"),
    }
    for role, expected_sha in expected_bundle_semantics.items():
        descriptor = artifacts.get(bundle_ids.get(role))
        if not descriptor or descriptor.get("semantic_sha256") != expected_sha:
            errors.append(f"RAW_CURRENT_REFIT_BUNDLE_BINDING_INVALID:{role}")

    book_id = bundle_ids.get("accepted_book_generation")
    book_descriptor = artifacts.get(book_id)
    book_path = artifact_paths.get(book_id)
    if book_descriptor and book_path:
        try:
            raw_book_receipt = _load_canonical_json_artifact(
                book_path,
                expected_size=book_descriptor["size_bytes"],
                expected_content_sha256=book_descriptor["content_sha256"],
                maximum_bytes=MAX_MANIFEST_BYTES,
                code_prefix="ACCEPTED_CURRENT_BOOK",
            )
            book_payload, book_receipt_sha = open_typed_receipt(
                raw_book_receipt,
                expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
            )
            book_identity = dict(book_payload)
            declared_generation_sha = book_identity.pop("generation_sha256", None)
            if (
                book_receipt_sha
                != authorization.get("accepted_book_generation_receipt_sha256")
                or declared_generation_sha
                != authorization.get("accepted_book_generation_sha256")
                or declared_generation_sha != canonical_receipt_sha256(book_identity)
                or book_payload.get("regime_params_sha256")
                != authorization.get("regime_params_sha256")
                or book_payload.get("current_cutoff_utc")
                != contract["current_cutoff_utc"]
                or book_payload.get("selected_winner_roster_sha256")
                != authorization.get("selected_winner_roster_sha256")
            ):
                errors.append("RAW_CURRENT_REFIT_ACCEPTED_BOOK_BINDING_INVALID")
        except (TypeError, ValueError, OverflowError) as exc:
            errors.append(f"RAW_CURRENT_REFIT_ACCEPTED_BOOK_INVALID:{exc}")
    else:
        errors.append("RAW_CURRENT_REFIT_ACCEPTED_BOOK_FILE_UNAVAILABLE")

    regime_bundle_id = bundle_ids.get("regime_params_bundle")
    regime_descriptor = artifacts.get(regime_bundle_id)
    regime_path = artifact_paths.get(regime_bundle_id)
    if regime_descriptor and (
        regime_descriptor.get("format_version") != CURRENT_REGIME_PARAMS_BUNDLE_VERSION
    ):
        errors.append("RAW_CURRENT_REFIT_REGIME_PARAMS_FORMAT_INVALID")
    if regime_descriptor and regime_path:
        try:
            raw_regime_bundle = _load_canonical_json_artifact(
                regime_path,
                expected_size=regime_descriptor["size_bytes"],
                expected_content_sha256=regime_descriptor["content_sha256"],
                maximum_bytes=MAX_REGIME_PARAMS_BUNDLE_BYTES,
                code_prefix="REGIME_PARAMS_BUNDLE",
            )
            normalized_regime_bundle = validate_current_regime_params_bundle(
                raw_regime_bundle,
                current_cutoff_utc=contract["current_cutoff_utc"],
                selected_cells=contract["cells"],
            )
            derived_regime_sha = canonical_receipt_sha256(normalized_regime_bundle)
            if derived_regime_sha != regime_descriptor[
                "semantic_sha256"
            ] or derived_regime_sha != authorization.get("regime_params_sha256"):
                errors.append("RAW_CURRENT_REFIT_REGIME_PARAMS_SEMANTIC_MISMATCH")
        except (TypeError, ValueError, OverflowError) as exc:
            errors.append(f"RAW_CURRENT_REFIT_REGIME_PARAMS_INVALID:{exc}")
    else:
        errors.append("RAW_CURRENT_REFIT_REGIME_PARAMS_FILE_UNAVAILABLE")

    requests = raw_manifest.get("cell_requests")
    receipt_cells = contract["cells"]
    if not isinstance(requests, list) or len(requests) != len(receipt_cells):
        errors.append("RAW_CURRENT_REFIT_CELL_REQUEST_ROSTER_INVALID")
        requests = []
    normalized_requests: list[Dict[str, Any]] = []
    semantic_fields = {
        "source_snapshot": "source_snapshot_sha256",
        "winner_process": "winner_process_sha256",
        "horizon_process": "winner_horizon_process_sha256",
        "trial_definition": "winner_trial_definition_sha256",
        "refit_recipe": "winner_refit_recipe_sha256",
        "winner_book_entry": "winner_book_entry_sha256",
    }
    for index, raw in enumerate(requests):
        if not isinstance(raw, Mapping) or set(raw) != _CELL_REQUEST_FIELDS:
            errors.append(f"RAW_CURRENT_REFIT_CELL_REQUEST_FIELDS_INVALID:{index}")
            continue
        request = dict(raw)
        expected = receipt_cells[index]
        for name in _CELL_REQUEST_FIELDS - {"artifact_ids"}:
            if request.get(name) != expected.get(name):
                errors.append(
                    f"RAW_CURRENT_REFIT_CELL_REQUEST_BINDING_MISMATCH:{index}:{name}"
                )
        ids = request.get("artifact_ids")
        if not isinstance(ids, Mapping) or set(ids) != _CELL_ARTIFACT_ROLES:
            errors.append(f"RAW_CURRENT_REFIT_CELL_ARTIFACT_ROSTER_INVALID:{index}")
            continue
        for role, field in semantic_fields.items():
            descriptor = artifacts.get(ids.get(role))
            if not descriptor or descriptor.get("semantic_sha256") != expected.get(
                field
            ):
                errors.append(
                    f"RAW_CURRENT_REFIT_CELL_ARTIFACT_BINDING_INVALID:{index}:{role}"
                )
        normalized_requests.append(request)
    if requests and [item.get("current_refit_cell_id") for item in requests] != [
        cell.get("current_refit_cell_id") for cell in receipt_cells
    ]:
        errors.append("RAW_CURRENT_REFIT_CELL_REQUEST_ORDER_INVALID")

    plan = {
        "schema_version": RAW_CURRENT_REFIT_REPLAY_REQUEST_VERSION,
        "executor_contract_version": RAW_CURRENT_REFIT_EXECUTOR_VERSION,
        "manifest_sha256": manifest_sha,
        "current_refit_receipt_sha256": contract["receipt_sha256"],
        "current_refit_execution_sha256": contract["execution_sha256"],
        "current_cutoff_utc": contract["current_cutoff_utc"],
        "bundle_artifact_ids": dict(bundle_ids),
        "artifact_descriptors": [
            {
                name: artifacts[key][name]
                for name in _ARTIFACT_FIELDS - {"semantic_sha256"}
            }
            for key in sorted(artifacts)
        ],
        "cell_requests": [
            {name: request[name] for name in _EXECUTOR_CELL_FIELDS}
            for request in normalized_requests
        ],
    }
    return plan, list(dict.fromkeys(errors))


def _verification(errors: Sequence[str], **values: Any) -> Dict[str, Any]:
    unique = list(dict.fromkeys(str(error) for error in errors if str(error)))
    return {
        "schema_version": RAW_CURRENT_REFIT_VERIFICATION_VERSION,
        "verified": not unique,
        "errors": unique,
        "executor_contract_version": RAW_CURRENT_REFIT_EXECUTOR_VERSION,
        "manifest_sha256": values.get("manifest_sha256"),
        "current_refit_receipt_sha256": values.get("current_refit_receipt_sha256"),
        "current_refit_execution_sha256": values.get("current_refit_execution_sha256"),
        "recomputed_cell_count": int(values.get("recomputed_cell_count", 0) or 0),
        "recomputed_cell_bundle_sha256": values.get("recomputed_cell_bundle_sha256"),
        "recomputed_output_book_sha256": values.get("recomputed_output_book_sha256"),
        "recomputed_execution_trace_sha256": values.get(
            "recomputed_execution_trace_sha256"
        ),
    }


def verify_raw_current_refit_execution(
    current_refit_receipt: Mapping[str, Any],
    raw_manifest: Any,
    *,
    artifact_root: Any,
) -> Dict[str, Any]:
    """Replay through the fixed pipeline seam; no callback is accepted.

    The executor receives paths and content hashes, never declared semantics or
    outputs. It must derive both from parsed bytes and release each cell's data
    before processing the next cell.
    """
    if raw_manifest is None or artifact_root in (None, ""):
        return _verification(["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"])
    if isinstance(raw_manifest, (str, Path)):
        try:
            raw_manifest = load_bounded_raw_current_refit_manifest(raw_manifest)
        except Exception:
            return _verification(
                [
                    "RAW_CURRENT_REFIT_MANIFEST_FILE_INVALID",
                    "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED",
                ]
            )
    plan, errors = validate_raw_current_refit_manifest(
        current_refit_receipt,
        raw_manifest,
        artifact_root=artifact_root,
    )
    if plan is None or errors:
        return _verification(
            [*errors, "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"],
            manifest_sha256=(plan or {}).get("manifest_sha256"),
            current_refit_receipt_sha256=(plan or {}).get(
                "current_refit_receipt_sha256"
            ),
            current_refit_execution_sha256=(plan or {}).get(
                "current_refit_execution_sha256"
            ),
        )
    try:
        pipeline = importlib.import_module("pm_pipeline")
        if getattr(pipeline, "CURRENT_REFIT_RAW_EXECUTOR_VERSION", None) != (
            RAW_CURRENT_REFIT_EXECUTOR_VERSION
        ):
            raise AttributeError("executor version")
        executor = getattr(pipeline, RAW_CURRENT_REFIT_EXECUTOR_FUNCTION)
        if (
            not callable(executor)
            or getattr(executor, "__module__", None) != "pm_pipeline"
            or getattr(executor, "__name__", None)
            != RAW_CURRENT_REFIT_EXECUTOR_FUNCTION
        ):
            raise AttributeError("executor identity")
    except Exception:
        return _verification(
            [
                "CURRENT_REFIT_RAW_EXECUTOR_UNAVAILABLE",
                "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED",
            ],
            manifest_sha256=plan["manifest_sha256"],
            current_refit_receipt_sha256=plan["current_refit_receipt_sha256"],
            current_refit_execution_sha256=plan["current_refit_execution_sha256"],
        )

    try:
        raw_result = executor(
            plan, artifact_root=Path(artifact_root).resolve(strict=True)
        )
    except Exception as exc:
        return _verification(
            [
                f"CURRENT_REFIT_RAW_REPLAY_FAILED:{type(exc).__name__}",
                "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED",
            ],
            manifest_sha256=plan["manifest_sha256"],
            current_refit_receipt_sha256=plan["current_refit_receipt_sha256"],
            current_refit_execution_sha256=plan["current_refit_execution_sha256"],
        )
    if not isinstance(raw_result, Mapping) or set(raw_result) != _REPLAY_RESULT_FIELDS:
        return _verification(
            [
                "CURRENT_REFIT_RAW_REPLAY_RESULT_INVALID",
                "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED",
            ],
            manifest_sha256=plan["manifest_sha256"],
            current_refit_receipt_sha256=plan["current_refit_receipt_sha256"],
            current_refit_execution_sha256=plan["current_refit_execution_sha256"],
        )

    contract, receipt_errors = _receipt_contract(current_refit_receipt)
    compare_errors = list(receipt_errors)
    result_cells = raw_result.get("cell_results")
    expected_cells = contract["cells"] if contract else []
    if (
        raw_result.get("schema_version") != RAW_CURRENT_REFIT_REPLAY_RESULT_VERSION
        or raw_result.get("executor_contract_version")
        != RAW_CURRENT_REFIT_EXECUTOR_VERSION
        or raw_result.get("current_cutoff_utc")
        != (contract or {}).get("current_cutoff_utc")
        or not isinstance(result_cells, list)
        or len(result_cells) != len(expected_cells)
    ):
        compare_errors.append("CURRENT_REFIT_RAW_REPLAY_RESULT_INVALID")
        result_cells = []
    compare_fields = _CELL_RESULT_FIELDS - {"current_refit_cell_id"}
    for index, result in enumerate(result_cells):
        if not isinstance(result, Mapping) or set(result) != _CELL_RESULT_FIELDS:
            compare_errors.append(f"CURRENT_REFIT_RAW_CELL_RESULT_INVALID:{index}")
            continue
        expected = expected_cells[index]
        if result.get("current_refit_cell_id") != expected.get("current_refit_cell_id"):
            compare_errors.append(f"CURRENT_REFIT_RAW_CELL_ID_MISMATCH:{index}")
        for field in compare_fields:
            if result.get(field) != expected.get(field):
                compare_errors.append(
                    f"CURRENT_REFIT_RAW_CELL_OUTPUT_MISMATCH:{index}:{field}"
                )
    execution = (contract or {}).get("execution", {})
    expected_bundle = execution.get("cell_execution_bundle_sha256")
    expected_book = execution.get("output_book_sha256")
    expected_trace = execution.get("execution_trace_sha256")
    if raw_result.get("cell_execution_bundle_sha256") != expected_bundle:
        compare_errors.append("CURRENT_REFIT_RAW_CELL_BUNDLE_MISMATCH")
    if raw_result.get("output_book_sha256") != expected_book:
        compare_errors.append("CURRENT_REFIT_RAW_OUTPUT_BOOK_MISMATCH")
    if raw_result.get("execution_trace_sha256") != expected_trace:
        compare_errors.append("CURRENT_REFIT_RAW_EXECUTION_TRACE_MISMATCH")
    if compare_errors:
        compare_errors.append("CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED")
    return _verification(
        compare_errors,
        manifest_sha256=plan["manifest_sha256"],
        current_refit_receipt_sha256=plan["current_refit_receipt_sha256"],
        current_refit_execution_sha256=plan["current_refit_execution_sha256"],
        recomputed_cell_count=len(result_cells),
        recomputed_cell_bundle_sha256=raw_result.get("cell_execution_bundle_sha256"),
        recomputed_output_book_sha256=raw_result.get("output_book_sha256"),
        recomputed_execution_trace_sha256=raw_result.get("execution_trace_sha256"),
    )


__all__ = [
    "CURRENT_REGIME_PARAMS_BUNDLE_VERSION",
    "REGIME_DETECTOR_RECORD_VERSION",
    "REGIME_FIT_RECEIPT_VERSION",
    "RAW_CURRENT_REFIT_EXECUTOR_FUNCTION",
    "RAW_CURRENT_REFIT_EXECUTOR_VERSION",
    "RAW_CURRENT_REFIT_MANIFEST_VERSION",
    "RAW_CURRENT_REFIT_REPLAY_REQUEST_VERSION",
    "RAW_CURRENT_REFIT_REPLAY_RESULT_VERSION",
    "RAW_CURRENT_REFIT_VERIFICATION_VERSION",
    "build_raw_artifact_descriptor",
    "build_current_regime_params_bundle",
    "build_raw_current_refit_manifest",
    "load_bounded_raw_current_refit_manifest",
    "validate_raw_current_refit_manifest",
    "validate_current_regime_params_bundle",
    "verify_raw_current_refit_execution",
]
