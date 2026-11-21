"""Cold, descriptive reader for one immutable signal-design tape edition.

The public spool verifier authenticates the GENERAL manifest first. A separate
protected parser handle hashes its own forward bytes; nothing is returned until
COMPLETE, EOF, file identity and that digest agree. Source receipts may be read
again through that same handle using bounded offsets, never a side index file.
This validates the recorded engineering evidence, not its financial hypothesis.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
import os
import re
import stat
from typing import Any, Mapping

import pm_authority_spool as spool


WRITER_SHA256 = "d8258c8e8a1916833f6eddaf4024a477fa973040a022e9b8725fca0803b281fb"
V16_WRITER_SHA256 = "4cf8eae423b360e6a37bac05774df86b854d2eb4702c9bc850b0b632c55e1e3a"
_KNOWN_WRITER_SHA256 = (WRITER_SHA256, V16_WRITER_SHA256)
LABELER_SHA256 = "93216ecede71d00c357f9c7992373082a793433d15f5d9cd4971b3dfa450c59a"
M1_COMPATIBLE_V1_WRITER_SHA256 = "b9dd3a79eb0379e8d772cd8ebfadc700111b5185de02c0c14df939665797f02a"
CURRENT_COMPATIBLE_V1_WRITER_SHA256 = "741469c019f2259f2665cb8219544a23f65405332a67b392620f5a9b07cbc290"
CURRENT_COMPATIBLE_V1_LABELER_SHA256 = "0d9b24f4a5059ee9e7187a8c0b2044d3353dd71fed0605a190c644ef640edb03"
LIVE_REGIME_COMPATIBLE_V1_WRITER_SHA256 = "5e88853231e9bebaf0c9d53680b3a6a7893df42da9559a323bb1e3cc3cfa4313"
LIVE_REGIME_COMPATIBLE_V1_LABELER_SHA256 = "96109937b50467e6837f659945ee1af1a47c9fdbd92bce569c2bcf5baa7a6195"
BASE_ATR_COMPATIBLE_V1_LABELER_SHA256 = "2065ed602b681b9e4f79a9635f63a9d4d61bd487a49bf0f3f48d9bdb8b35000a"
HISTORICAL_COHORT_COMPATIBLE_V1_WRITER_SHA256 = "cbb2deb08422fa82a097a9e73d00e475aecb27d445b71c09e9bab0f72d559956"
HORIZON_PREIMAGE_ARTIFACT_TYPE = "signal_opportunity_design_horizon_preimage"
REQUEST_SCHEMA = "fxpm.signal-opportunity-design-request.v1"
TAPE_SCHEMA = "fxpm.signal-opportunity-train-design-tape.v1"
TAPE_FORMAT = "fxpm-signal-opportunity-design-jsonl-v1"
LABEL_SCHEMA = "fxpm.signal-opportunity-labels.v1"
INDEPENDENT_R_REQUEST_SCHEMA = "fxpm.signal-opportunity-design-request.v2"
INDEPENDENT_R_TAPE_SCHEMA = "fxpm.signal-opportunity-train-design-tape.v2"
INDEPENDENT_R_TAPE_FORMAT = "fxpm-signal-opportunity-design-jsonl-v2"
INDEPENDENT_R_LABEL_SCHEMA = "fxpm.signal-opportunity-labels.v2"
INDEPENDENT_R_METHOD = "fxpm.independent-signal-modeled-r.v1"
INDEPENDENT_R_WRITER_SHA256 = "b9dd3a79eb0379e8d772cd8ebfadc700111b5185de02c0c14df939665797f02a"
INDEPENDENT_R_LABELER_SHA256 = "0d9b24f4a5059ee9e7187a8c0b2044d3353dd71fed0605a190c644ef640edb03"
MAX_LINE_BYTES = 64 * 1024 * 1024
MAX_PREFIX_ROWS = 10_000_000
_REGIMES = frozenset({"TREND", "RANGE", "BREAKOUT", "CHOP"})
_ROLES = ("optimizer_train", "tuning_validation", "final_lockbox")
_TIERS = ("P75", "P95")
_SECONDS = {"M5": 300, "M15": 900, "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400}
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_UTC = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:Z|\+00:00)\Z")
_FLAGS = ("validation_evidence", "qualification_authority", "current_or_trading_authority", "score_or_shortlist_authority")
_ROSTER_FIELDS = frozenset({"ordinal", "horizon_attempt_id", "process_candidate_id", "status", "reason_code",
    "trial_definition_sha256", "refit_recipe_sha256"})
_REQUEST_FIELDS = frozenset({"schema_version", "authority_scope", "judging_scope", "declared_slots_supported",
    "grant_is_separate_from_planning_metadata", *_FLAGS, "generation_context_sha256", "search_process_receipt_sha256",
    "candidate_domain_sha256", "planning_metadata_sha256", "causal_precommit_sha256", "production_anchor_plan_receipt_sha256",
    "production_anchor_plan_sha256", "cell_plan_receipt_sha256", "horizon_plan_receipt_sha256", "cell", "config_sha256",
    "effective_config_sha256", "training_label_cap_utc", "earliest_judging_cutoff_utc", "spread_calibration_cutoff_utc",
    "retrospective_frozen_spread_scenario", "spread_profile_sha256", "frozen_tier_pair_contract_sha256",
    "native_prefix_index_sha256", "native_prefix_content_sha256", "fit_prefix_extent", "native_fitted_frame_sha256",
    "normalized_fitted_frame_sha256", "execution_spec_receipt_sha256", "regime_fit_decision_sha256",
    "train_regime_support_sha256", "source_sha256", "labeler_schema_version", "attempt_count", "attempt_roster",
    "attempt_roster_sha256"})
_GEOMETRY_FIELDS = frozenset({"schema_version", "role", "frame_start_utc", "scored_start_utc", "end_exclusive_utc",
    "warmup_bars", "scored_bars", "frame_rows", "frame_sha256", "receipt_sha256"})
_PROVENANCE_FIELDS = frozenset({"strategy_id", "configuration_sha256", "source_surface_sha256", "spread_tier",
    "spread_model_sha256", "admission_policy_sha256"})
_BATCH_FIELDS = frozenset({"record_type", "attempt_ordinal", "fold_ordinal", "partition", "tier", "partition_geometry",
    "first_entry_bar", "signal_start_utc", "cutoff_utc", "provenance", "pip_size", "numerical_spread_binding",
    "admission_binding", "raw_nonzero_count", "excluded_before_start_count", "window_nonzero_count",
    "unavailable_regime_count", "descriptive_groups", "qualification_authority"})
_LABEL_FIELDS = frozenset({"opportunity_id", "signal_bar", "signal_open_utc", "signal_close_utc", "direction", "run_start",
    "entry_regime", "regime_status", "raw_regime", "stop_loss_pips", "take_profit_pips", "status", "entry_bar",
    "entry_open_utc", "entry_admission", "entry_price", "stop_loss_price", "take_profit_price", "exit_bar",
    "exit_bar_open_utc", "exit_observed_at_utc", "exit_price", "exit_reason", "exit_fill", "signed_price_delta", "signed_pips"})
_TOTAL_FIELDS = ("roster_count", "presearch_rejected_count", "evaluated_attempt_count", "batch_count", "opportunity_count",
    "closed_count", "right_censored_count", "no_next_observation_count", "unavailable_regime_count",
    "source_observation_receipt_count", "source_observation_row_count")
_INDEPENDENT_R_LABEL_FIELDS = _LABEL_FIELDS | {"eligible", "eligibility_reason", "signal_regime", "episode_id",
    "initial_risk_per_unit", "price_pnl_per_unit", "spread_cost_per_unit", "swap_per_unit",
    "price_r", "spread_r", "swap_r", "modeled_net_r", "cost_status"}
_R_VALUE_FIELDS = ("initial_risk_per_unit", "price_pnl_per_unit", "spread_cost_per_unit", "swap_per_unit",
    "price_r", "spread_r", "swap_r", "modeled_net_r")
_SOURCE_EDITION_FIELDS = {"edition_id", "process_schema", "execution_edition", "catalogue_schema",
    "winner_generation_id", "ordered_strategy_ids", "ordered_timeframes", "strategy_count",
    "nondefault_variants_per_strategy", "attempt_count", "candidate_catalogue_sha256",
    "domain_roster_sha256_by_timeframe", "state_registry_sha256", "source_release_sha256"}


class SignalOpportunityTapeError(ValueError):
    def __init__(self, code: str, details: tuple[str, ...] = ()):
        self.code, self.details = code, details
        super().__init__(code + (":" + ",".join(details) if details else ""))


@dataclass(frozen=True, slots=True)
class SignalOpportunityTapeSummary:
    _body_json: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))


def _fail(code):
    raise SignalOpportunityTapeError(code)


def _canonical(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("ascii")


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _fields(value, keys, code):
    if type(value) is not dict or set(value) != set(keys):
        _fail(code)


def _hash(value, code="TAPE_HASH_INVALID"):
    if type(value) is not str or _HEX.fullmatch(value) is None:
        _fail(code)
    return value


def _int(value, minimum=0, maximum=MAX_PREFIX_ROWS, code="TAPE_INTEGER_INVALID"):
    if type(value) is not int or not minimum <= value <= maximum:
        _fail(code)
    return value


def _number(value, *, positive=False):
    if type(value) not in {int, float} or not math.isfinite(value) or (positive and value <= 0):
        _fail("TAPE_NUMBER_INVALID")
    return float(value)


def _time(value):
    """Exact UTC nanoseconds on a Gregorian ordinal axis; no clock conversion."""
    match = _UTC.fullmatch(value) if type(value) is str else None
    if match is None:
        _fail("TAPE_UTC_TIMESTAMP_INVALID")
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    try:
        stamp = datetime(year, month, day, hour, minute, second)
    except ValueError:
        _fail("TAPE_UTC_TIMESTAMP_INVALID")
    fraction = int((match.group(7) or "").ljust(9, "0"))
    return ((stamp.toordinal() * 86400 + hour * 3600 + minute * 60 + second) * 1_000_000_000 + fraction)


def _text(value, maximum=256):
    if type(value) is not str or not value or len(value) > maximum:
        _fail("TAPE_TEXT_INVALID")
    return value


class _Sum:
    """Small error-free partial expansion, rounded with the writer's math.fsum."""
    def __init__(self):
        self.partials = []

    def add(self, value):
        x = _number(value)
        used = 0
        for y in self.partials:
            if abs(x) < abs(y):
                x, y = y, x
            hi = x + y
            if not math.isfinite(hi):
                _fail("TAPE_MOMENT_OVERFLOW")
            lo = y - (hi - x)
            if lo:
                self.partials[used] = lo
                used += 1
            x = hi
        self.partials[used:] = [x]
        if len(self.partials) > 2048:
            _fail("TAPE_SUM_RESOURCE_BOUND")

    def value(self):
        return math.fsum(self.partials)


class _Groups:
    def __init__(self):
        self.rows = {}

    def add(self, label, tier=None):
        key = (label["entry_regime"], label["entry_admission"], label["status"])
        if tier is not None:
            key = (tier, *key)
        row = self.rows.setdefault(key, [0, 0, _Sum(), _Sum()])
        row[0] += 1
        if label["status"] == "CLOSED":
            value = _number(label["signed_pips"])
            row[1] += 1
            row[2].add(value)
            row[3].add(value * value)

    def result(self, selected=None):
        result = []
        for key, row in sorted(self.rows.items(), key=lambda item: tuple("" if value is None else value for value in item[0])):
            tier, regime, admission, status = key if len(key) == 4 else (None, *key)
            item = {"entry_regime": regime, "entry_admission": admission, "status": status,
                "observation_count": row[0], "closed_observation_count": row[1],
                "closed_signed_pip_sum": row[2].value() if row[1] else None,
                "closed_signed_pip_sum_squares": row[3].value() if row[1] else None}
            if tier is not None:
                item.update(tier=tier, regime_partition=("UNAVAILABLE_REGIME" if regime is None
                    else "SELECTED_CELL_REGIME" if regime == selected else "OTHER_REGIME"))
            result.append(item)
        return result


class _Stream:
    def __init__(self, context, descriptor):
        self.context, self.descriptor = context, descriptor
        self.path = spool._safe_regular_file(context.root, descriptor["relative_path"])
        self.path_before = self.path.lstat()
        self.handle = None
        self.digest, self.total = hashlib.sha256(), 0
        self.line_bound = min(context.limits.max_json_bytes, MAX_LINE_BYTES)

    def __enter__(self):
        flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0)) | int(getattr(os, "O_NOFOLLOW", 0))
        descriptor = os.open(self.path, flags)
        self.handle = os.fdopen(descriptor, "rb")
        self.before = os.fstat(self.handle.fileno())
        if (not stat.S_ISREG(self.before.st_mode)
                or spool._file_identity(self.path_before) != spool._file_identity(self.before)
                or self.before.st_size != self.descriptor["size_bytes"]):
            self.handle.close()
            _fail("TAPE_FILE_IDENTITY_INVALID")
        return self

    def __exit__(self, *_):
        if self.handle is not None:
            self.handle.close()

    def _read(self):
        raw = self.handle.readline(self.line_bound + 1)
        if raw and (len(raw) > self.line_bound or not raw.endswith(b"\n")):
            _fail("TAPE_LINE_BOUND_OR_TRUNCATION")
        return raw

    def decode(self, raw):
        try:
            value = spool._strict_json_bytes(raw[:-1], self.line_bound)
            if type(value) is not dict or _canonical(value) + b"\n" != raw:
                _fail("TAPE_CANONICAL_RECORD_INVALID")
            return value
        except SignalOpportunityTapeError:
            raise
        except Exception as exc:
            raise SignalOpportunityTapeError("TAPE_JSON_INVALID", (type(exc).__name__,)) from exc

    def next(self):
        offset = self.handle.tell()
        raw = self._read()
        if not raw:
            return None, offset
        self.total += len(raw)
        if self.total > self.descriptor["size_bytes"]:
            _fail("TAPE_SIZE_CHANGED")
        self.digest.update(raw)
        return self.decode(raw), offset

    def source(self, offset, length, expected_sha):
        forward = self.handle.tell()
        try:
            self.handle.seek(offset)
            raw = self._read()
            if len(raw) != length:
                _fail("TAPE_SOURCE_RELOAD_CHANGED")
            row = self.decode(raw)
            if (set(row) != {"record_type", "sha256", "receipt"}
                    or row["record_type"] != "SOURCE_OBSERVATIONS"
                    or row["sha256"] != expected_sha or _sha(row["receipt"]) != expected_sha):
                _fail("TAPE_SOURCE_RELOAD_CHANGED")
            return row["receipt"]
        finally:
            self.handle.seek(forward)

    def finish(self):
        after = os.fstat(self.handle.fileno())
        path_after = self.path.lstat()
        if (spool._file_identity(self.before) != spool._file_identity(after)
                or spool._file_identity(after) != spool._file_identity(path_after)
                or self.total != self.descriptor["size_bytes"]
                or self.digest.hexdigest() != self.descriptor["sha256"]):
            _fail("TAPE_FINAL_INTEGRITY_MISMATCH")


def _independent_source_edition(body):
    edition = body["source_edition"]
    _fields(edition, _SOURCE_EDITION_FIELDS, "TAPE_SOURCE_EDITION_FIELDS_INVALID")
    strategies, timeframes = edition["ordered_strategy_ids"], edition["ordered_timeframes"]
    if (edition["edition_id"] != "fxpm.noise101-per-timeframe-source-edition.v1"
            or edition["process_schema"] != "fxpm.production-search-process-precommit.v4"
            or edition["execution_edition"] != "fxpm.route-a-noise101-per-timeframe-owner-execution.v1"
            or edition["catalogue_schema"] != "fxpm.strategy-candidate-catalogue-by-timeframe.v1"
            or type(edition["strategy_count"]) is not int or edition["strategy_count"] != 101
            or type(edition["nondefault_variants_per_strategy"]) is not int or edition["nondefault_variants_per_strategy"] != 2
            or type(edition["attempt_count"]) is not int or edition["attempt_count"] != 303
            or body["attempt_count"] != edition["attempt_count"]
            or edition["candidate_catalogue_sha256"] != body["candidate_domain_sha256"]
            or type(strategies) is not list or len(strategies) != edition["strategy_count"]
            or any(type(value) is not str or not value for value in strategies) or len(set(strategies)) != len(strategies)
            or type(timeframes) is not list or any(type(value) is not str or value not in _SECONDS for value in timeframes)
            or len(set(timeframes)) != len(timeframes) or body["cell"]["timeframe"] not in timeframes):
        _fail("TAPE_SOURCE_EDITION_BINDING_INVALID")
    _text(edition["winner_generation_id"])
    for key in ("candidate_catalogue_sha256", "state_registry_sha256", "source_release_sha256"):
        _hash(edition[key])
    rosters = edition["domain_roster_sha256_by_timeframe"]
    _fields(rosters, timeframes, "TAPE_SOURCE_EDITION_ROSTERS_INVALID")
    for value in rosters.values():
        _hash(value)
    if _hash(body["domain_roster_sha256"]) != rosters[body["cell"]["timeframe"]]:
        _fail("TAPE_SOURCE_EDITION_BINDING_INVALID")


def _request(body, context, expected_cell, *, independent=False):
    edition_fields = {"source_edition", "domain_roster_sha256"} if independent and type(body) is dict and (
        "source_edition" in body or "domain_roster_sha256" in body) else set()
    _fields(body, _REQUEST_FIELDS | ({"method_edition"} if independent else set()) | edition_fields,
        "TAPE_REQUEST_FIELDS_INVALID")
    _fields(body["source_sha256"], {"executor", "labeler"}, "TAPE_REQUEST_SOURCE_FIELDS_INVALID")
    source_pair = (body["source_sha256"]["executor"], body["source_sha256"]["labeler"])
    accepted_pairs = ((INDEPENDENT_R_WRITER_SHA256, INDEPENDENT_R_LABELER_SHA256),) if independent else (
        *((writer, LABELER_SHA256) for writer in _KNOWN_WRITER_SHA256),
        (M1_COMPATIBLE_V1_WRITER_SHA256, CURRENT_COMPATIBLE_V1_LABELER_SHA256),
        (CURRENT_COMPATIBLE_V1_WRITER_SHA256, CURRENT_COMPATIBLE_V1_LABELER_SHA256),
        (LIVE_REGIME_COMPATIBLE_V1_WRITER_SHA256, LIVE_REGIME_COMPATIBLE_V1_LABELER_SHA256),
        (LIVE_REGIME_COMPATIBLE_V1_WRITER_SHA256, BASE_ATR_COMPATIBLE_V1_LABELER_SHA256),
        (HISTORICAL_COHORT_COMPATIBLE_V1_WRITER_SHA256, BASE_ATR_COMPATIBLE_V1_LABELER_SHA256))
    if (body["schema_version"] != (INDEPENDENT_R_REQUEST_SCHEMA if independent else REQUEST_SCHEMA)
            or (independent and body["method_edition"] != INDEPENDENT_R_METHOD)
            or body["authority_scope"] != "TRAIN_DESIGN_LABELS_ONLY"
            or body["judging_scope"] != "AUTHENTICATED_HISTORICAL_ANCHOR_PLAN_ONLY"
            or body["declared_slots_supported"] is not False
            or body["grant_is_separate_from_planning_metadata"] is not True
            or body["retrospective_frozen_spread_scenario"] is not True
            or any(body[key] is not False for key in _FLAGS)
            or body["cell"] != expected_cell or body["config_sha256"] != context.config_sha256
            or body["generation_context_sha256"] != context.context_sha256
            or source_pair not in accepted_pairs
            or body["labeler_schema_version"] != (INDEPENDENT_R_LABEL_SCHEMA if independent else LABEL_SCHEMA)):
        _fail("TAPE_REQUEST_BINDING_INVALID")
    for key, value in body.items():
        if key.endswith("_sha256") and key != "source_sha256":
            _hash(value)
    for value in body["source_sha256"].values():
        _hash(value)
    cap = _time(body["training_label_cap_utc"])
    if cap != min(_time(body["earliest_judging_cutoff_utc"]), _time(body["spread_calibration_cutoff_utc"])):
        _fail("TAPE_TRAIN_CAP_INVALID")
    extent = body["fit_prefix_extent"]
    _fields(extent, {"requested_cutoff_utc", "first_open_utc", "last_close_utc", "rows"}, "TAPE_PREFIX_FIELDS_INVALID")
    _int(extent["rows"], 1)
    if (_time(extent["requested_cutoff_utc"]) > cap or _time(extent["last_close_utc"]) > cap
            or _time(extent["first_open_utc"]) >= _time(extent["last_close_utc"])):
        _fail("TAPE_PREFIX_CAP_INVALID")
    roster = body["attempt_roster"]
    count = 303 if edition_fields else 300
    if edition_fields:
        _independent_source_edition(body)
    if (type(body["attempt_count"]) is not int or body["attempt_count"] != count or type(roster) is not list or len(roster) != count
            or _sha(roster) != body["attempt_roster_sha256"]):
        _fail("TAPE_ROSTER_INVALID")
    seen = set()
    for ordinal, row in enumerate(roster):
        _fields(row, _ROSTER_FIELDS, "TAPE_ROSTER_FIELDS_INVALID")
        if row["ordinal"] != ordinal or type(row["ordinal"]) is not int:
            _fail("TAPE_ROSTER_ORDER_INVALID")
        for key in ("horizon_attempt_id", "process_candidate_id"):
            _text(row[key])
        if row["horizon_attempt_id"] in seen:
            _fail("TAPE_ROSTER_DUPLICATE")
        seen.add(row["horizon_attempt_id"])
        for key in ("trial_definition_sha256", "refit_recipe_sha256"):
            _hash(row[key])
        if (row["status"] not in {"PLANNED", "PROCESS_REJECTED"}
                or (row["status"] == "PLANNED" and row["reason_code"] is not None)
                or (row["status"] == "PROCESS_REJECTED" and not _text(row["reason_code"]))):
            _fail("TAPE_ROSTER_DISPOSITION_INVALID")
    return cap


def _json_artifact(context, entry):
    descriptor = entry["descriptor"]
    if descriptor["format"] != spool.CANONICAL_JSON_FORMAT:
        _fail("TAPE_PLAN_PREIMAGE_FORMAT_INVALID")
    digest, _, raw = spool._stream_file(context.root, descriptor["relative_path"],
        maximum_bytes=context.limits.max_json_bytes, expected_size=descriptor["size_bytes"], collect=True)
    if digest != descriptor["sha256"]:
        _fail("TAPE_PLAN_PREIMAGE_HASH_INVALID")
    body = spool._strict_json_bytes(raw, context.limits.max_json_bytes)
    if type(body) is not dict or _canonical(body) != raw:
        _fail("TAPE_PLAN_PREIMAGE_CANONICAL_INVALID")
    return body


def _horizon_artifact_id(sha256):
    return "signal-design-horizon-preimage/" + sha256


def publish_signal_opportunity_horizon_preimage(sink, *, issued_request,
        expected_request_sha256, horizon_plan):
    """Retain an issued request's full Horizon body on its unsealed GENERAL sink.

    Call while the exact plan is in custody, before sealing that same sink. This
    publishes support bytes only; it neither issues a request nor recovers an
    absent historical body or grants search, selection or trading authority.
    """
    try:
        if type(sink) is not spool.SymbolAuthoritySink or sink.manifest_domain != spool.GENERAL_MANIFEST_DOMAIN:
            _fail("TAPE_HORIZON_PUBLICATION_SINK_INVALID")
        context = sink.context
        spool._assert_context_current(context)
        manifest_path = context.root / spool._manifest_relative_path(sink.symbol, sink.manifest_domain)
        if manifest_path.exists():
            _fail("TAPE_HORIZON_PUBLICATION_REQUIRES_UNSEALED_SINK")
        _hash(expected_request_sha256)
        request = issued_request.to_dict() if callable(getattr(issued_request, "to_dict", None)) else issued_request
        if type(request) is not dict:
            _fail("TAPE_HORIZON_PUBLICATION_REQUEST_INVALID")
        request_raw = spool._canonical_json_bytes(request, context.limits.max_json_bytes)
        request = spool._strict_json_bytes(request_raw, context.limits.max_json_bytes)
        if hashlib.sha256(request_raw).hexdigest() != expected_request_sha256:
            _fail("TAPE_HORIZON_PUBLICATION_REQUEST_HASH_INVALID")
        cell = request.get("cell")
        _fields(cell, {"symbol", "timeframe", "regime"}, "TAPE_EXPECTED_CELL_INVALID")
        if (cell["symbol"] != sink.symbol or cell["timeframe"] not in _SECONDS or cell["regime"] not in _REGIMES):
            _fail("TAPE_HORIZON_PUBLICATION_CELL_INVALID")
        _request(request, context, cell, independent=request.get("schema_version") == INDEPENDENT_R_REQUEST_SCHEMA)
        body = horizon_plan.to_dict() if callable(getattr(horizon_plan, "to_dict", None)) else horizon_plan
        if type(body) is not dict:
            _fail("TAPE_HORIZON_PUBLICATION_BODY_INVALID")
        raw = spool._canonical_json_bytes(body, context.limits.max_json_bytes)
        body = spool._strict_json_bytes(raw, context.limits.max_json_bytes)
        sha = hashlib.sha256(raw).hexdigest()
        if sha != request["horizon_plan_receipt_sha256"]:
            _fail("TAPE_HORIZON_PUBLICATION_BODY_HASH_INVALID")
        if body.get("schema_version") != "fxpm.historical-horizon-plan.v3" or body.get("cell") != cell:
            _fail("TAPE_HORIZON_PUBLICATION_BODY_INVALID")
        artifact_id = _horizon_artifact_id(sha)
        entry = sink.put_json(artifact_id, HORIZON_PREIMAGE_ARTIFACT_TYPE, body)
        _fields(entry, {"artifact_id", "artifact_type", "descriptor"}, "TAPE_HORIZON_PUBLICATION_DESCRIPTOR_INVALID")
        descriptor = entry["descriptor"]
        _fields(descriptor, {"schema_version", "relative_path", "sha256", "size_bytes", "format"},
            "TAPE_HORIZON_PUBLICATION_DESCRIPTOR_INVALID")
        if (entry["artifact_id"] != artifact_id or entry["artifact_type"] != HORIZON_PREIMAGE_ARTIFACT_TYPE
                or descriptor["schema_version"] != spool.ARTIFACT_DESCRIPTOR_SCHEMA or descriptor["sha256"] != sha
                or type(descriptor.get("size_bytes")) is not int or descriptor["size_bytes"] != len(raw)
                or descriptor.get("format") != spool.CANONICAL_JSON_FORMAT
                or descriptor.get("relative_path") != f"objects/sha256/{sha[:2]}/{sha}.{spool.CANONICAL_JSON_FORMAT}"):
            _fail("TAPE_HORIZON_PUBLICATION_DESCRIPTOR_INVALID")
        return entry
    except SignalOpportunityTapeError:
        raise
    except Exception as exc:
        raise SignalOpportunityTapeError("TAPE_HORIZON_PUBLICATION_FAILED", (type(exc).__name__,)) from exc


def _plans(context, manifest, request, cap):
    count = request["attempt_count"]
    suffix = "/search-cell-plan/{timeframe}/{regime}".format(**request["cell"])
    cells = []
    for entry in manifest["artifacts"]:
        if entry["artifact_type"] == "production_search_cell_plan_receipt" and entry["artifact_id"].endswith(suffix):
            body = _json_artifact(context, entry)
            if body.get("receipt_sha256") == request["cell_plan_receipt_sha256"]:
                cells.append(body)
    horizons = [entry for entry in manifest["artifacts"]
        if entry["artifact_id"] == _horizon_artifact_id(request["horizon_plan_receipt_sha256"])
        and entry["artifact_type"] == HORIZON_PREIMAGE_ARTIFACT_TYPE
        and entry["descriptor"]["sha256"] == request["horizon_plan_receipt_sha256"]
        and entry["descriptor"]["format"] == spool.CANONICAL_JSON_FORMAT]
    if len(cells) != 1 or len(horizons) != 1:
        _fail("TAPE_PLAN_PREIMAGE_MISSING_OR_AMBIGUOUS")
    cell, horizon = cells[0], _json_artifact(context, horizons[0])
    if (cell.get("schema_version") != "fxpm.production-search-cell-plan-receipt.v2"
            or cell.get("receipt_sha256") != _sha({k: v for k, v in cell.items() if k != "receipt_sha256"})
            or cell.get("authority_scope") != "FROZEN_CELL_SEARCH_PLAN_ONLY" or cell.get("mode") != "HISTORICAL_ANCHOR"
            or cell.get("cell") != request["cell"] or cell.get("generation_id") != context.generation_id
            or cell.get("generation_context_sha256") != context.context_sha256
            or cell.get("search_process_receipt_sha256") != request["search_process_receipt_sha256"]
            or cell.get("confers_winner_causal_current_capital_publication_or_live_authority") is not False
            or cell.get("attempts_frozen_before_search") is not True or type(cell.get("attempt_count")) is not int or cell.get("attempt_count") != count):
        _fail("TAPE_CELL_PLAN_BINDING_INVALID")
    hashes = cell.get("attempt_artifact_sha256")
    if (type(hashes) is not list or len(hashes) != count or len(set(hashes)) != count
            or cell.get("attempt_roster_sha256") != _sha(hashes)):
        _fail("TAPE_CELL_PLAN_ROSTER_INVALID")
    for value in hashes:
        _hash(value)
    if (horizon.get("schema_version") != "fxpm.historical-horizon-plan.v3"
            or horizon.get("authority_scope") != "HISTORICAL_ANCHOR_LOCAL_HORIZON_EVIDENCE_ONLY"
            or horizon.get("cell") != request["cell"] or horizon.get("confers_trading_or_capital_authority") is not False
            or horizon.get("confers_final_oos_authority") is not False
            or horizon.get("plan_status") not in {"READY_FOR_HISTORICAL_EVIDENCE", "HISTORICAL_BOUNDARY_LIMITED"}):
        _fail("TAPE_HORIZON_PLAN_BINDING_INVALID")
    binding = cell.get("horizon_plan_binding")
    if (type(binding) is not dict or binding.get("plan_receipt_sha256") != request["horizon_plan_receipt_sha256"]
            or binding.get("cell") != request["cell"]):
        _fail("TAPE_CELL_HORIZON_LINK_INVALID")
    for key in ("candidate_registry", "trial_registry", "attempt_roster", "evidence_roster"):
        source = {"attempt_roster": "attempts", "evidence_roster": "expected_evaluations"}.get(key, key)
        values = horizon.get(source)
        if type(values) is not list or horizon.get(key + "_sha256") != _sha(values) or binding.get(key + "_sha256") != horizon.get(key + "_sha256"):
            _fail("TAPE_HORIZON_ROSTER_HASH_INVALID")
    anchor = horizon.get("anchor")
    if (type(anchor) is not dict or _time(anchor.get("selection_cutoff_utc")) > cap
            or binding.get("anchor_id") != anchor.get("anchor_id")
            or binding.get("selection_cutoff_utc") != anchor.get("selection_cutoff_utc")
            or binding.get("process_precommit_receipt_sha256") != anchor.get("process_precommit_receipt_sha256")):
        _fail("TAPE_HORIZON_ANCHOR_INVALID")
    attempts, candidates, trials = horizon.get("attempts"), horizon.get("candidates"), horizon.get("trials")
    if type(attempts) is not list or len(attempts) != count or type(candidates) is not list or type(trials) is not list:
        _fail("TAPE_HORIZON_ROSTER_INVALID")
    candidate_by_id = {value.get("candidate_id"): value for value in candidates if type(value) is dict}
    trial_by_id = {value.get("trial_id"): value for value in trials if type(value) is dict}
    if len(candidate_by_id) != len(candidates) or len(trial_by_id) != len(trials):
        _fail("TAPE_HORIZON_ROSTER_INVALID")
    for trial in trials:
        if trial.get("trial_definition_sha256") != _sha({key: value for key, value in trial.items() if key != "trial_definition_sha256"}):
            _fail("TAPE_HORIZON_TRIAL_HASH_INVALID")
    result = []
    for ordinal, (attempt, recorded) in enumerate(zip(attempts, request["attempt_roster"], strict=True)):
        if (type(attempt) is not dict or attempt.get("attempt_id") != recorded["horizon_attempt_id"]
                or attempt.get("status") != recorded["status"] or attempt.get("reason_code") != recorded["reason_code"]):
            _fail("TAPE_REQUEST_HORIZON_ROSTER_MISMATCH")
        candidate, trial = candidate_by_id.get(attempt.get("candidate_id")), trial_by_id.get(attempt.get("trial_id"))
        if (candidate is None or trial is None or attempt.get("candidate_definition_sha256") != candidate.get("candidate_definition_sha256")
                or attempt.get("trial_definition_sha256") != trial.get("trial_definition_sha256")
                or attempt.get("strategy_id") != trial.get("strategy_id")):
            _fail("TAPE_HORIZON_ATTEMPT_LINK_INVALID")
        folds = candidate.get("folds")
        if type(folds) is not list:
            _fail("TAPE_HORIZON_FOLDS_INVALID")
        causal = [fold for fold in folds if type(fold) is dict and fold.get("role") == "CAUSAL_ROLLING_ORIGIN_INNER"]
        if recorded["status"] == "PLANNED":
            if len(causal) != 4 or len(folds) != 4:
                _fail("TAPE_HORIZON_FOLDS_INVALID")
            previous = None
            for fold in causal:
                if fold.get("fold_definition_sha256") != _sha({k: v for k, v in fold.items() if k != "fold_definition_sha256"}):
                    _fail("TAPE_HORIZON_FOLD_HASH_INVALID")
                fit_start, fit_end, warm_start, start, end = (_time(fold.get(k)) for k in
                    ("fit_start_utc", "fit_end_exclusive_utc", "validation_warmup_start_utc", "validation_start_utc", "validation_end_exclusive_utc"))
                if (not fit_start < fit_end < start < end or not warm_start < start or end > cap
                        or (previous is not None and start < previous)):
                    _fail("TAPE_HORIZON_FOLD_GEOMETRY_INVALID")
                _int(fold.get("fit_rows"), 1)
                _int(fold.get("validation_rows"), 1)
                previous = end
        result.append({"strategy_id": attempt.get("strategy_id"), "folds": causal})
    return result


def _observations(body, request, cap):
    _fields(body, {"schema_version", "symbol", "timeframe", "source_timeframe", "source_product", "observations",
        "entry_observations_sha256", "receipt_sha256"}, "TAPE_SOURCE_FIELDS_INVALID")
    if (body["schema_version"] != "fxpm.original-fine-spread-observations.v1"
            or body["symbol"] != request["cell"]["symbol"] or body["timeframe"] != request["cell"]["timeframe"]
            or body["source_timeframe"] not in _SECONDS
            or _SECONDS[body["source_timeframe"]] > _SECONDS[body["timeframe"]]
            or body["receipt_sha256"] != _sha({key: value for key, value in body.items() if key != "receipt_sha256"})):
        _fail("TAPE_SOURCE_BINDING_INVALID")
    _hash(body["entry_observations_sha256"])
    product = body["source_product"]
    _fields(product, {"data_dir", "index_descriptor", "index_sha256", "symbol", "timeframe", "cutoff_utc",
        "activation_manifest_sha256", "production_contract_required"}, "TAPE_SOURCE_PRODUCT_FIELDS_INVALID")
    if (product["symbol"] != body["symbol"] or product["timeframe"] != body["source_timeframe"]
            or product["production_contract_required"] is not True or _time(product["cutoff_utc"]) > cap
            or _time(product["cutoff_utc"]) != _time(request["fit_prefix_extent"]["requested_cutoff_utc"])):
        _fail("TAPE_SOURCE_PRODUCT_INVALID")
    _text(product["data_dir"], spool.MAX_JSON_STRING_BYTES)
    _hash(product["index_sha256"])
    _hash(product["activation_manifest_sha256"])
    spool._validate_descriptor(product["index_descriptor"], content_addressed=True)
    if product["index_descriptor"]["sha256"] != product["index_sha256"]:
        _fail("TAPE_SOURCE_INDEX_IDENTITY_MISMATCH")
    rows = body["observations"]
    if type(rows) is not list or not 0 < len(rows) <= MAX_PREFIX_ROWS:
        _fail("TAPE_SOURCE_AXIS_INVALID")
    axis, prior = [], None
    step = _SECONDS[body["timeframe"]] * 1_000_000_000
    for row in rows:
        _fields(row, {"target_open_utc", "expected_prior_fine_close_utc", "prior_fine_close_utc", "points"}, "TAPE_SOURCE_ROW_FIELDS_INVALID")
        opened = _time(row["target_open_utc"])
        minimum_gap = step // 2 if body["timeframe"] in {"H4", "D1"} else step
        if (_time(row["expected_prior_fine_close_utc"]) != opened or opened >= cap
                or opened < _time(request["fit_prefix_extent"]["first_open_utc"])
                or opened >= _time(request["fit_prefix_extent"]["last_close_utc"])
                or (prior is not None and opened < prior + minimum_gap)):
            _fail("TAPE_SOURCE_AXIS_INVALID")
        if row["prior_fine_close_utc"] is not None and _time(row["prior_fine_close_utc"]) != opened:
            _fail("TAPE_SOURCE_PRIOR_CLOSE_INVALID")
        if row["points"] is not None:
            points = _number(row["points"])
            if points < 0 or points != math.floor(points) or row["prior_fine_close_utc"] is None:
                _fail("TAPE_SOURCE_POINTS_INVALID")
        axis.append(opened)
        prior = opened
    return axis


def _batch(row, request, expected_key, axis, source_sha, entry_observations_sha, cap, source, *, independent=False):
    _fields(row, _BATCH_FIELDS | ({"method_edition", "cost_model", "episode_diagnostics"} if independent else set()),
        "TAPE_BATCH_FIELDS_INVALID")
    if independent:
        _independent_cost_model(row)
    key = (row["attempt_ordinal"], row["fold_ordinal"], row["partition"], row["tier"])
    if key != expected_key or row["qualification_authority"] is not False:
        _fail("TAPE_BATCH_ORDER_INVALID")
    _int(key[0], 0, request["attempt_count"] - 1)
    _int(key[1], 0, 3)
    geometry = row["partition_geometry"]
    _fields(geometry, _GEOMETRY_FIELDS, "TAPE_GEOMETRY_FIELDS_INVALID")
    warmup = _int(geometry["warmup_bars"])
    count = _int(geometry["frame_rows"], 1)
    scored = _int(geometry["scored_bars"], 2)
    if (geometry["schema_version"] != "fxpm.winner-partition-geometry.v1" or geometry["role"] != key[2]
            or geometry["receipt_sha256"] != _sha({k: v for k, v in geometry.items() if k != "receipt_sha256"})
            or count != len(axis) or count != warmup + scored or warmup >= count
            or _time(geometry["frame_start_utc"]) != axis[0]
            or _time(geometry["scored_start_utc"]) != axis[warmup]):
        _fail("TAPE_GEOMETRY_INVALID")
    _hash(geometry["frame_sha256"])
    step = _SECONDS[request["cell"]["timeframe"]] * 1_000_000_000
    cutoff, start = _time(row["cutoff_utc"]), _time(row["signal_start_utc"])
    first = max(1, warmup)
    aligned = request["cell"]["timeframe"] in {"H4", "D1"}
    closes = [axis[i + 1] if aligned and step // 2 <= axis[i + 1] - opened <= step * 3 // 2
        else opened + step for i, opened in enumerate(axis[:-1])]
    last_span = cutoff - axis[-1]
    if not (step // 2 <= last_span <= step * 3 // 2 if aligned else last_span == step):
        _fail("TAPE_LAST_CLOSE_BOUNDARY_INVALID")
    closes.append(cutoff)
    if (type(row["first_entry_bar"]) is not int or row["first_entry_bar"] != first
            or cutoff != _time(geometry["end_exclusive_utc"])
            or cutoff > cap or start != closes[first - 1] or start >= cutoff
            or any(closed > axis[i + 1] for i, closed in enumerate(closes[:-1]))):
        _fail("TAPE_WINDOW_INVALID")
    raw, excluded, window = (_int(row[k]) for k in ("raw_nonzero_count", "excluded_before_start_count", "window_nonzero_count"))
    if raw != excluded + window or raw > count or window > count - first + 1 or excluded > first - 1:
        _fail("TAPE_BATCH_COUNT_INVALID")
    _int(row["unavailable_regime_count"], 0, window)
    _number(row["pip_size"], positive=True)
    provenance = row["provenance"]
    _fields(provenance, _PROVENANCE_FIELDS, "TAPE_PROVENANCE_FIELDS_INVALID")
    _text(provenance["strategy_id"])
    for k in _PROVENANCE_FIELDS - {"strategy_id", "spread_tier"}:
        _hash(provenance[k])
    roster = request["attempt_roster"][key[0]]
    config_sha = _sha({"effective_config_sha256": request["effective_config_sha256"],
        "trial_definition_sha256": roster["trial_definition_sha256"], "refit_recipe_sha256": roster["refit_recipe_sha256"]})
    numerical, admission = row["numerical_spread_binding"], row["admission_binding"]
    _fields(numerical, {"cost_attestation", "spread_model", "entry_half_spread_f64le_sha256", "exit_half_spread_f64le_sha256"}, "TAPE_COST_FIELDS_INVALID")
    _fields(admission, {"entry_admission_policy", "entry_admission_schema", "entry_observations_sha256",
        "entry_admission_mask_sha256", "all_states_sha256", "source_observation_receipt_sha256"}, "TAPE_ADMISSION_FIELDS_INVALID")
    for obj in (numerical, admission):
        for k, value in obj.items():
            if k.endswith("_sha256"):
                _hash(value)
    _cost(numerical, request, key[3])
    frozen = numerical["spread_model"]["frozen_spread_tier"]
    if frozen["source_timeframe"] != source["source_timeframe"]:
        _fail("TAPE_SPREAD_SOURCE_TIMEFRAME_MISMATCH")
    states = ["MISSING" if item["points"] is None else "ADMIT" if item["points"] <= frozen["spread_points"] else "REFUSE"
        for item in source["observations"]]
    if (admission["entry_admission_mask_sha256"] != hashlib.sha256(bytes(value == "ADMIT" for value in states)).hexdigest()
            or admission["all_states_sha256"] != _sha(states)):
        _fail("TAPE_ADMISSION_STATES_MISMATCH")
    if (provenance["spread_tier"] != key[3] or provenance["configuration_sha256"] != config_sha
            or provenance["spread_model_sha256"] != _sha(numerical)
            or provenance["admission_policy_sha256"] != _sha(admission)
            or admission["source_observation_receipt_sha256"] != source_sha
            or admission["entry_observations_sha256"] != entry_observations_sha
            or admission["entry_admission_policy"] != "EXACT_PRIOR_FINE_CLOSE_CAP_ADMIT_ONLY_V2"
            or admission["entry_admission_schema"] != "fxpm.spread-entry-admission.v2"):
        _fail("TAPE_PROVENANCE_BINDING_INVALID")
    return {"row": row, "key": key, "axis": axis, "closes": closes, "step": step, "seen": 0, "unknown": 0,
        "prior_bar": None, "prior_direction": None, "groups": _Groups(), "entry_admission_states": states,
        "independent": independent, "episode_rows": []}


def _cost(numerical, request, tier):
    model, cost = numerical["spread_model"], numerical["cost_attestation"]
    _fields(model, {"mode", "source", "units", "fallback_to_fixed", "asset_class", "broker_fixed_spread_price",
        "entry_column", "bar_column", "entry_model", "intrabar_policy", "spread_provenance_status", "frozen_spread_tier",
        "entry_admission_policy", "entry_admission_schema", "recorded_spread_missing_rows", "qualification_authority"}, "TAPE_SPREAD_MODEL_FIELDS_INVALID")
    if (model["mode"] != "conservative_max" or model["source"] != "frozen_profile_and_recorded_spread_points"
            or model["units"] != "price" or model["fallback_to_fixed"] is not False
            or _number(model["broker_fixed_spread_price"]) != 0 or model["entry_column"] not in {"SpreadOpen", "Spread"}
            or model["bar_column"] not in {"SpreadMax", "Spread"}
            or model["entry_model"] != "max_frozen_tier_and_recorded_bar_scenario"
            or model["intrabar_policy"] != "MAX_TIER_AND_RECORDED_EXIT_SPREAD_PROTECTIVE_EXITS_UNCAPPED"
            or model["spread_provenance_status"] != "RETROSPECTIVE_FIXED_SCENARIO_NOT_EXECUTION_QUOTES"
            or model["entry_admission_policy"] != "EXACT_PRIOR_FINE_CLOSE_CAP_ADMIT_ONLY_V2"
            or model["entry_admission_schema"] != "fxpm.spread-entry-admission.v2" or model["qualification_authority"] is not False):
        _fail("TAPE_SPREAD_MODEL_INVALID")
    _text(model["asset_class"])
    missing = model["recorded_spread_missing_rows"]
    _fields(missing, {model["entry_column"], model["bar_column"]}, "TAPE_SPREAD_MISSING_ROWS_INVALID")
    for value in missing.values():
        _int(value)
    frozen = model["frozen_spread_tier"]
    _fields(frozen, {"schema_version", "profile_sha256", "activation", "protected_cutoff_utc", "symbol", "timeframe", "tier",
        "source_timeframe", "point_price", "spread_points", "full_spread_price", "source_identity_sha256", "source_binding",
        "cost_policy", "admission_policy", "qualification_authority"}, "TAPE_FROZEN_TIER_FIELDS_INVALID")
    point = _number(frozen["point_price"], positive=True)
    points = frozen["spread_points"]
    if type(points) is not int or points < 1:
        _fail("TAPE_FROZEN_TIER_INVALID")
    if (frozen["schema_version"] != "fxpm.frozen-spread-tier.v1" or frozen["profile_sha256"] != request["spread_profile_sha256"]
            or _time(frozen["protected_cutoff_utc"]) != _time(request["spread_calibration_cutoff_utc"])
            or frozen["symbol"] != request["cell"]["symbol"] or frozen["timeframe"] != request["cell"]["timeframe"]
            or frozen["tier"] != tier or frozen["source_timeframe"] not in _SECONDS
            or _SECONDS[frozen["source_timeframe"]] > _SECONDS[frozen["timeframe"]]
            or frozen["full_spread_price"] != points * point
            or frozen["cost_policy"] != "MAX_FROZEN_TIER_AND_RECORDED_SPREAD_BOTH_SIDES"
            or frozen["admission_policy"] != "EXACT_PRIOR_FINE_CLOSE_THEN_LIVE_FIRST_AND_FINAL_TICK_CAP"
            or frozen["qualification_authority"] is not False or type(frozen["source_binding"]) is not dict):
        _fail("TAPE_FROZEN_TIER_INVALID")
    _hash(frozen["source_identity_sha256"])
    _fields(cost, {"schema_version", "use_spread", "spread_mode", "entry_half_spread_values_sha256",
        "intrabar_half_spread_values_sha256", "instrument_spec_sha256", "policy_sha256", "attestation_sha256"}, "TAPE_COST_ATTESTATION_FIELDS_INVALID")
    policy = {k: cost[k] for k in ("schema_version", "use_spread", "spread_mode", "instrument_spec_sha256")}
    for key in ("entry_half_spread_values_sha256", "intrabar_half_spread_values_sha256", "instrument_spec_sha256", "policy_sha256", "attestation_sha256"):
        _hash(cost[key])
    mode = {k: model[k] for k in ("mode", "source", "entry_column", "bar_column", "entry_model", "asset_class",
        "broker_fixed_spread_price", "intrabar_policy", "units", "frozen_spread_tier", "entry_admission_policy", "entry_admission_schema")}
    mode["broker_fixed_spread_price"] = float(mode["broker_fixed_spread_price"])
    mode.update(cost_ladder_level_id="", single_fill_uplift_percentile=0,
        uniform_entry_half_spread_multiplier_float64_hex="", entry_cost_application="", uniform_application="",
        uplift_distribution_basis="", stochastic_scenario=False)
    attested = {**policy, "entry_half_spread_values_sha256": cost["entry_half_spread_values_sha256"],
        "intrabar_half_spread_values_sha256": cost["intrabar_half_spread_values_sha256"], "policy_sha256": cost["policy_sha256"]}
    if (cost["schema_version"] != "fxpm-numerical-execution-cost-v3" or cost["use_spread"] is not True
            or cost["spread_mode"] != _canonical(mode).decode("ascii") or cost["policy_sha256"] != _sha(policy)
            or cost["attestation_sha256"] != _sha(attested)):
        _fail("TAPE_COST_ATTESTATION_INVALID")


def _plan_batch(active, plan, pairing, fold_state):
    batch, axis = active["row"], active["axis"]
    _, ordinal, role, tier = active["key"]
    geometry = batch["partition_geometry"]
    fold = plan["folds"][ordinal]
    if batch["provenance"]["strategy_id"] != plan["strategy_id"]:
        _fail("TAPE_BATCH_STRATEGY_PLAN_MISMATCH")
    pair = {k: batch[k] for k in ("partition_geometry", "first_entry_bar", "signal_start_utc", "cutoff_utc", "pip_size",
        "raw_nonzero_count", "excluded_before_start_count", "window_nonzero_count", "unavailable_regime_count")}
    pair.update(source_sha256=batch["admission_binding"]["source_observation_receipt_sha256"],
        strategy_id=batch["provenance"]["strategy_id"], configuration_sha256=batch["provenance"]["configuration_sha256"],
        source_surface_sha256=batch["provenance"]["source_surface_sha256"])
    if tier == "P95":
        if pairing != pair:
            _fail("TAPE_TIER_GEOMETRY_MISMATCH")
        return None
    if role == "optimizer_train":
        warmup = geometry["warmup_bars"]
        fit_rows = fold["fit_rows"]
        split = max(fit_rows - max(2, math.ceil(fit_rows * .25)), warmup + 2)
        if (_time(geometry["frame_start_utc"]) != _time(fold["fit_start_utc"])
                or geometry["frame_rows"] != split or geometry["scored_bars"] != split - warmup):
            _fail("TAPE_TRAIN_PLAN_GEOMETRY_MISMATCH")
        fold_state.clear()
        fold_state.update(warmup=warmup, split=split, train_end=_time(geometry["end_exclusive_utc"]),
            overlap_sha256=_sha(axis[len(axis) - warmup:] if warmup else []))
    elif role == "tuning_validation":
        warmup = fold_state["warmup"]
        if (geometry["warmup_bars"] != warmup or geometry["scored_bars"] != fold["fit_rows"] - fold_state["split"]
                or _time(geometry["end_exclusive_utc"]) != _time(fold["fit_end_exclusive_utc"])
                or _time(geometry["scored_start_utc"]) < fold_state["train_end"]
                or _sha(axis[:warmup]) != fold_state["overlap_sha256"]):
            _fail("TAPE_TUNING_PLAN_GEOMETRY_MISMATCH")
    else:
        if (geometry["warmup_bars"] < fold_state["warmup"] or geometry["scored_bars"] != fold["validation_rows"]
                or any(_time(geometry[k]) != _time(fold[v]) for k, v in
                    (("frame_start_utc", "validation_warmup_start_utc"), ("scored_start_utc", "validation_start_utc"),
                     ("end_exclusive_utc", "validation_end_exclusive_utc")))):
            _fail("TAPE_FINAL_PLAN_GEOMETRY_MISMATCH")
    return pair


def _independent_cost_model(row):
    model = row["cost_model"]
    _fields(model, {"declared_model", "commission", "swap_contract", "account_compounding"},
        "TAPE_R_COST_MODEL_FIELDS_INVALID")
    if (row["method_edition"] != INDEPENDENT_R_METHOD or model["declared_model"] != "SPREAD_PLUS_SWAP"
            or model["commission"] != "NOT_MODELED" or model["account_compounding"] is not False
            or type(model["swap_contract"]) is not dict
            or type(row["episode_diagnostics"]) is not dict):
        _fail("TAPE_R_COST_MODEL_INVALID")


def _independent_values(label):
    entered = label["status"] in {"CLOSED", "RIGHT_CENSORED"}
    if type(label["eligible"]) is not bool or label["eligible"] is not entered:
        _fail("TAPE_R_ELIGIBILITY_INVALID")
    if label["signal_regime"] is not None and type(label["signal_regime"]) is not str:
        _fail("TAPE_R_SIGNAL_REGIME_INVALID")
    if label["episode_id"] is not None:
        _text(label["episode_id"])
    if not entered:
        reason = ("NO_NEXT_OBSERVATION" if label["status"] == "NO_NEXT_OBSERVATION" else
            "STRATEGY_SESSION_REFUSED" if label["entry_admission"] == "ADMIT" else
            "ENTRY_ADMISSION_" + label["entry_admission"])
        if (label["eligibility_reason"] != reason or label["episode_id"] is not None
                or label["cost_status"] != "NOT_ENTERED" or any(label[k] is not None for k in _R_VALUE_FIELDS)):
            _fail("TAPE_R_NOT_ENTERED_INVALID")
        return
    if label["eligibility_reason"] is not None or label["entry_admission"] != "ADMIT":
        _fail("TAPE_R_ELIGIBILITY_INVALID")
    risk = _number(label["initial_risk_per_unit"], positive=True)
    if label["status"] == "RIGHT_CENSORED":
        if label["cost_status"] != "UNRESOLVED" or any(label[k] is not None for k in _R_VALUE_FIELDS[1:]):
            _fail("TAPE_R_UNRESOLVED_INVALID")
        return
    price, spread = (_number(label[k]) for k in ("price_pnl_per_unit", "spread_cost_per_unit"))
    if (_number(label["price_r"]) != price / risk or _number(label["spread_r"]) != -spread / risk):
        _fail("TAPE_R_ARITHMETIC_INVALID")
    if label["cost_status"] == "MODELED_AVAILABLE":
        swap = _number(label["swap_per_unit"])
        if (_number(label["swap_r"]) != swap / risk or _number(label["modeled_net_r"]) != (price + swap) / risk):
            _fail("TAPE_R_ARITHMETIC_INVALID")
    elif label["cost_status"] != "UNAVAILABLE_SWAP" or any(label[k] is not None for k in
            ("swap_per_unit", "swap_r", "modeled_net_r")):
        _fail("TAPE_R_COST_STATUS_INVALID")


def _label(row, active, request):
    _fields(row, {"record_type", "attempt_ordinal", "fold_ordinal", "partition", "tier", "label"}, "TAPE_OPPORTUNITY_FIELDS_INVALID")
    _int(row["attempt_ordinal"], 0, request["attempt_count"] - 1)
    _int(row["fold_ordinal"], 0, 3)
    if (row["attempt_ordinal"], row["fold_ordinal"], row["partition"], row["tier"]) != active["key"]:
        _fail("TAPE_OPPORTUNITY_TAG_MISMATCH")
    label, batch, axis = row["label"], active["row"], active["axis"]
    independent = active["independent"]
    _fields(label, _INDEPENDENT_R_LABEL_FIELDS if independent else _LABEL_FIELDS, "TAPE_LABEL_FIELDS_INVALID")
    bar = _int(label["signal_bar"], batch["first_entry_bar"] - 1, len(axis) - 1)
    direction = label["direction"]
    if (type(direction) is not int or direction not in {-1, 1} or type(label["run_start"]) is not bool
            or (active["prior_bar"] is not None and bar <= active["prior_bar"])
            or _time(label["signal_open_utc"]) != axis[bar]
            or _time(label["signal_close_utc"]) != active["closes"][bar]):
        _fail("TAPE_LABEL_SIGNAL_GEOMETRY_INVALID")
    if active["prior_bar"] is not None or bar == 0 or bar > batch["first_entry_bar"] - 1:
        run_start = not (active["prior_bar"] == bar - 1 and active["prior_direction"] == direction)
        if label["run_start"] is not run_start:
            _fail("TAPE_LABEL_RUN_START_INVALID")
    raw_regime, regime = label["raw_regime"], label["entry_regime"]
    if raw_regime is not None and type(raw_regime) is not str:
        _fail("TAPE_LABEL_REGIME_INVALID")
    expected_regime = raw_regime if raw_regime in _REGIMES else None
    if regime != expected_regime or label["regime_status"] != ("AVAILABLE" if regime else "UNAVAILABLE"):
        _fail("TAPE_LABEL_REGIME_INVALID")
    active["unknown"] += int(regime is None)
    status = label["status"]
    if independent and status in {"NOT_ELIGIBLE", "NO_NEXT_OBSERVATION"}:
        if any(label[k] is not None for k in ("stop_loss_pips", "take_profit_pips")):
            _fail("TAPE_R_NOT_ENTERED_INVALID")
        stop_pips = target_pips = None
    else:
        stop_pips, target_pips = (_number(label[k], positive=True) for k in ("stop_loss_pips", "take_profit_pips"))
    provenance = batch["provenance"]
    identity = (INDEPENDENT_R_LABEL_SCHEMA if independent else LABEL_SCHEMA,
        provenance["strategy_id"], provenance["configuration_sha256"], provenance["source_surface_sha256"],
        provenance["spread_tier"], provenance["spread_model_sha256"], provenance["admission_policy_sha256"],
        request["cell"]["symbol"], request["cell"]["timeframe"], "pip_size_binary64", float(batch["pip_size"]).hex(),
        label["signal_open_utc"], label["signal_close_utc"], direction)
    if label["opportunity_id"] != hashlib.sha256(json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("ascii")).hexdigest():
        _fail("TAPE_LABEL_ID_INVALID")
    entry_fields = ("entry_bar", "entry_open_utc", "entry_admission", "entry_price", "stop_loss_price", "take_profit_price")
    exit_fields = ("exit_bar", "exit_bar_open_utc", "exit_observed_at_utc", "exit_price", "exit_reason", "exit_fill", "signed_price_delta", "signed_pips")
    if status == "NO_NEXT_OBSERVATION":
        if bar + 1 != len(axis) or any(label[k] is not None for k in (*entry_fields, *exit_fields)):
            _fail("TAPE_LABEL_NO_NEXT_INVALID")
    elif independent and status == "NOT_ELIGIBLE":
        entry = _int(label["entry_bar"], 1, len(axis) - 1)
        if (entry != bar + 1 or _time(label["entry_open_utc"]) != axis[entry]
                or label["entry_admission"] != active["entry_admission_states"][entry]
                or any(label[k] is not None for k in (*entry_fields[3:], *exit_fields))):
            _fail("TAPE_R_NOT_ENTERED_INVALID")
    elif status in {"RIGHT_CENSORED", "CLOSED"}:
        entry = _int(label["entry_bar"], 1, len(axis) - 1)
        price, stop, target = (_number(label[k]) for k in ("entry_price", "stop_loss_price", "take_profit_price"))
        if (entry != bar + 1 or _time(label["entry_open_utc"]) != axis[entry]
                or axis[entry] >= _time(batch["cutoff_utc"])
                or label["entry_admission"] != active["entry_admission_states"][entry]
                or stop != price - direction * stop_pips * batch["pip_size"]
                or target != price + direction * target_pips * batch["pip_size"]
                or direction * (price - stop) <= 0 or direction * (target - price) <= 0):
            _fail("TAPE_LABEL_ENTRY_GEOMETRY_INVALID")
        if status == "RIGHT_CENSORED":
            if any(label[k] is not None for k in exit_fields):
                _fail("TAPE_LABEL_CENSOR_INVALID")
        else:
            exited = _int(label["exit_bar"], entry, len(axis) - 1)
            exited_price, delta, pips = (_number(label[k]) for k in ("exit_price", "signed_price_delta", "signed_pips"))
            session = independent and label["exit_reason"] == "SESSION" and label["exit_fill"] == "SESSION_OPEN"
            if (_time(label["exit_bar_open_utc"]) != axis[exited]
                    or _time(label["exit_observed_at_utc"]) != (axis[exited] if session else active["closes"][exited])
                    or active["closes"][exited] > _time(batch["cutoff_utc"])
                    or delta != direction * (exited_price - price) or pips != delta / batch["pip_size"]
                    or (not session and (label["exit_reason"] not in {"SL", "TP"}
                        or label["exit_fill"] not in {"GAP_OPEN", "STOP_LEVEL", "TARGET_LEVEL"}))):
                _fail("TAPE_LABEL_EXIT_GEOMETRY_INVALID")
            if (label["exit_fill"] == "STOP_LEVEL" and (label["exit_reason"] != "SL" or exited_price != stop)
                    or label["exit_fill"] == "TARGET_LEVEL" and (label["exit_reason"] != "TP" or exited_price != target)
                    or label["exit_fill"] == "GAP_OPEN" and (label["exit_reason"] != "SL" or direction * (exited_price - stop) > 0)):
                _fail("TAPE_LABEL_EXIT_FILL_INVALID")
    else:
        _fail("TAPE_LABEL_STATUS_INVALID")
    if independent:
        _independent_values(label)
        if label["eligible"]:
            active["episode_rows"].append({k: label[k] for k in
                ("eligible", "episode_id", "entry_regime", "status", "modeled_net_r")})
    active["seen"] += 1
    active["prior_bar"], active["prior_direction"] = bar, direction
    active["groups"].add(label)
    return label


def _finish_batch(active):
    if active is None:
        return
    row = active["row"]
    if type(row["descriptive_groups"]) is not list:
        _fail("TAPE_BATCH_RECONCILIATION_FAILED")
    for group in row["descriptive_groups"]:
        _fields(group, {"entry_regime", "entry_admission", "status", "observation_count", "closed_observation_count",
            "closed_signed_pip_sum", "closed_signed_pip_sum_squares"}, "TAPE_BATCH_RECONCILIATION_FAILED")
        if type(group["observation_count"]) is not int or type(group["closed_observation_count"]) is not int:
            _fail("TAPE_BATCH_RECONCILIATION_FAILED")
    if (active["seen"] != row["window_nonzero_count"] or active["unknown"] != row["unavailable_regime_count"]
            or active["groups"].result() != row["descriptive_groups"]):
        _fail("TAPE_BATCH_RECONCILIATION_FAILED")
    if active["independent"]:
        from pm_signal_opportunities import independent_episode_diagnostics
        diagnostics = independent_episode_diagnostics(active["episode_rows"])
        if _canonical(diagnostics) != _canonical(row["episode_diagnostics"]):
            _fail("TAPE_R_EPISODE_RECONCILIATION_FAILED")
        return diagnostics


def _read(context, *, symbol_receipt, expected_request_sha256, expected_tape_sha256, expected_cell, entry_regime,
        independent=False):
    if type(context) is not spool.GenerationSpoolContext:
        _fail("TAPE_CONTEXT_REQUIRED")
    spool._assert_context_current(context)
    _fields(expected_cell, {"symbol", "timeframe", "regime"}, "TAPE_EXPECTED_CELL_INVALID")
    _text(expected_cell["symbol"])
    if (expected_cell["timeframe"] not in _SECONDS or expected_cell["regime"] not in _REGIMES
            or entry_regime != expected_cell["regime"]):
        _fail("TAPE_ENTRY_REGIME_FILTER_INVALID")
    _hash(expected_request_sha256)
    _hash(expected_tape_sha256)
    manifest = spool.verify_symbol_receipt(context.root, symbol_receipt,
        expected_generation_id=context.generation_id, expected_symbol=expected_cell["symbol"],
        expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256,
        expected_manifest_domain=spool.GENERAL_MANIFEST_DOMAIN)
    roster = {row["artifact_id"]: row for row in manifest["artifacts"]}
    request_entry = roster.get("signal-design-request/" + expected_request_sha256)
    tape_entry = roster.get("signal-design-tape/" + expected_request_sha256)
    if (request_entry is None or tape_entry is None
            or request_entry["artifact_type"] != "signal_opportunity_train_design_request"
            or tape_entry["artifact_type"] != "signal_opportunity_train_design_tape"
            or request_entry["descriptor"]["sha256"] != expected_request_sha256
            or request_entry["descriptor"]["format"] != spool.CANONICAL_JSON_FORMAT
            or tape_entry["descriptor"]["sha256"] != expected_tape_sha256
            or tape_entry["descriptor"]["format"] != (INDEPENDENT_R_TAPE_FORMAT if independent else TAPE_FORMAT)):
        _fail("TAPE_ARTIFACT_ROSTER_BINDING_INVALID")
    descriptor = request_entry["descriptor"]
    digest, _, raw = spool._stream_file(context.root, descriptor["relative_path"],
        maximum_bytes=min(context.limits.max_json_bytes, MAX_LINE_BYTES), expected_size=descriptor["size_bytes"], collect=True)
    if digest != expected_request_sha256:
        _fail("TAPE_REQUEST_HASH_MISMATCH")
    request = spool._strict_json_bytes(raw, min(context.limits.max_json_bytes, MAX_LINE_BYTES))
    cap = _request(request, context, expected_cell, independent=independent)
    plans = _plans(context, manifest, request, cap)
    total_fields = (*_TOTAL_FIELDS, "not_eligible_count") if independent else _TOTAL_FIELDS
    schema = INDEPENDENT_R_TAPE_SCHEMA if independent else TAPE_SCHEMA
    swap_status = "PER_TRADE_MODELED" if independent else "UNAVAILABLE_PRICE_UNIT_CONVERSION"
    totals = {key: 0 for key in total_fields}
    refs, used, active, attempt, batch_index, groups = {}, set(), None, None, 0, _Groups()
    episode_diagnostics = []
    pairing, fold_state = None, {}
    keys = tuple((fold, role, tier) for fold in range(4) for role in _ROLES for tier in _TIERS)
    with _Stream(context, tape_entry["descriptor"]) as stream:
        header, _ = stream.next()
        _fields(header, {"record_type", "schema_version", "request_sha256", "request", "complete_cost_net_profit_available",
            "source_observation_reference_semantics", "swap_status"}, "TAPE_HEADER_FIELDS_INVALID")
        if (header["record_type"] != "HEADER" or header["schema_version"] != schema
                or header["request_sha256"] != expected_request_sha256 or _canonical(header["request"]) != _canonical(request)
                or header["complete_cost_net_profit_available"] is not False
                or header["swap_status"] != swap_status
                or header["source_observation_reference_semantics"] != "EXACT_RECEIPT_EMITTED_ONCE_BEFORE_FIRST_SHA256_REFERENCE"):
            _fail("TAPE_HEADER_BINDING_INVALID")
        complete = False
        while True:
            row, offset = stream.next()
            if row is None:
                if not complete:
                    _fail("TAPE_COMPLETE_MISSING")
                break
            if complete:
                _fail("TAPE_TRAILING_RECORD")
            kind = row.get("record_type")
            if kind != "OPPORTUNITY":
                diagnostics = _finish_batch(active)
                if diagnostics is not None:
                    episode_diagnostics.append(dict(zip(("attempt_ordinal", "fold_ordinal", "partition", "tier"),
                        active["key"]), entry_regime=entry_regime, **diagnostics[entry_regime]))
                active = None
            if kind == "ATTEMPT":
                if attempt is not None and batch_index != (24 if attempt["status"] == "PLANNED" else 0):
                    _fail("TAPE_ATTEMPT_BATCH_CENSUS_INVALID")
                ordinal = totals["roster_count"]
                if ordinal >= request["attempt_count"] or type(row.get("ordinal")) is not int or row != {"record_type": "ATTEMPT", **request["attempt_roster"][ordinal]}:
                    _fail("TAPE_ATTEMPT_ORDER_OR_BINDING_INVALID")
                attempt, batch_index = request["attempt_roster"][ordinal], 0
                totals["roster_count"] += 1
                totals["evaluated_attempt_count" if attempt["status"] == "PLANNED" else "presearch_rejected_count"] += 1
            elif kind == "SOURCE_OBSERVATIONS":
                _fields(row, {"record_type", "sha256", "receipt"}, "TAPE_SOURCE_RECORD_FIELDS_INVALID")
                sha = _hash(row["sha256"])
                if (attempt is None or attempt["status"] != "PLANNED" or sha in refs
                        or len(refs) >= request["attempt_count"] * 4 * len(_ROLES) or _sha(row["receipt"]) != sha):
                    _fail("TAPE_SOURCE_DUPLICATE_OR_BINDING_INVALID")
                axis = _observations(row["receipt"], request, cap)
                refs[sha] = (offset, stream.handle.tell() - offset)
                totals["source_observation_receipt_count"] += 1
                totals["source_observation_row_count"] += len(axis)
                del axis
            elif kind == "BATCH":
                if attempt is None or attempt["status"] != "PLANNED" or batch_index >= 24:
                    _fail("TAPE_BATCH_WITHOUT_PLANNED_ATTEMPT")
                admission = row.get("admission_binding")
                sha = admission.get("source_observation_receipt_sha256") if type(admission) is dict else None
                if sha not in refs:
                    _fail("TAPE_SOURCE_REFERENCE_BEFORE_USE_OR_DANGLING")
                source = stream.source(*refs[sha], sha)
                axis = _observations(source, request, cap)
                entry_observations_sha = source["entry_observations_sha256"]
                active = _batch(row, request, (attempt["ordinal"], *keys[batch_index]), axis, sha,
                    entry_observations_sha, cap, source, independent=independent)
                del source
                pairing = _plan_batch(active, plans[attempt["ordinal"]], pairing, fold_state)
                used.add(sha)
                batch_index += 1
                totals["batch_count"] += 1
            elif kind == "OPPORTUNITY":
                if active is None:
                    _fail("TAPE_OPPORTUNITY_WITHOUT_BATCH")
                label = _label(row, active, request)
                groups.add(label, active["key"][3])
                totals["opportunity_count"] += 1
                totals["unavailable_regime_count"] += int(label["entry_regime"] is None)
                totals[{"CLOSED": "closed_count", "RIGHT_CENSORED": "right_censored_count",
                    "NO_NEXT_OBSERVATION": "no_next_observation_count", "NOT_ELIGIBLE": "not_eligible_count"}[label["status"]]] += 1
            elif kind == "COMPLETE":
                _fields(row, {"record_type", "schema_version", "request_sha256", *total_fields,
                    "validation_evidence", "qualification_authority", "current_or_trading_authority"}, "TAPE_COMPLETE_FIELDS_INVALID")
                if (attempt is None or totals["roster_count"] != request["attempt_count"]
                        or batch_index != (24 if attempt["status"] == "PLANNED" else 0)
                        or used != set(refs) or row["schema_version"] != schema
                        or row["request_sha256"] != expected_request_sha256
                        or any(type(row[key]) is not int or row[key] != value for key, value in totals.items())
                        or any(row[key] is not False for key in ("validation_evidence", "qualification_authority", "current_or_trading_authority"))):
                    _fail("TAPE_COMPLETE_RECONCILIATION_FAILED")
                complete = True
            else:
                _fail("TAPE_RECORD_TYPE_INVALID")
        stream.finish()
    grouped = groups.result(entry_regime)
    denominators = [{k: v for k, v in row.items() if k not in {"closed_signed_pip_sum", "closed_signed_pip_sum_squares"}}
        for row in grouped if row["regime_partition"] != "SELECTED_CELL_REGIME"]
    extra = {"method_edition": INDEPENDENT_R_METHOD, "episode_diagnostics": episode_diagnostics} if independent else {}
    return SignalOpportunityTapeSummary(_canonical({"schema_version": "fxpm.signal-opportunity-descriptive-summary.v2"
            if independent else "fxpm.signal-opportunity-descriptive-summary.v1",
        "status": "DESCRIPTIVE_COMPLETE", "writer_sha256": request["source_sha256"]["executor"],
        "request_sha256": expected_request_sha256, "tape_sha256": expected_tape_sha256,
        "cell": dict(expected_cell), "entry_regime_filter": entry_regime, "totals": totals,
        "descriptive_groups": [row for row in grouped if row["regime_partition"] == "SELECTED_CELL_REGIME"],
        "other_regime_denominators": denominators, "quoted_spread_included": True,
        "complete_cost_net_profit_available": False, "swap_status": swap_status,
        "last_row_close_evidence": "RECORDED_BATCH_BOUNDARY_LINKED_TO_HORIZON_PLAN_NOT_NATIVE_SENTINEL_REVERIFICATION",
        "verification_scope": "COLD_STRUCTURAL_AND_CUSTODY_CONSISTENCY_UNDER_TRUSTED_ISSUED_REQUEST_HASH",
        "observation_count_unit": "LABEL_ROWS_NOT_DISTINCT_BARS",
        "source_observation_row_count_unit": "ROWS_ACROSS_UNIQUE_RECEIPTS_NOT_DISTINCT_BARS",
        "parameter_and_release_bridge": "ISSUANCE_TRUSTED_NOT_RECONSTRUCTED_BY_READER",
        "features_signals_ohlc_or_economic_outcomes_replayed": False,
        "validation_evidence": False, "qualification_authority": False, "score_or_shortlist_authority": False,
        "account_or_trading_authority": False, **extra}))


def read_signal_opportunity_tape(context, *, symbol_receipt, expected_request_sha256,
        expected_tape_sha256, expected_cell, entry_regime) -> SignalOpportunityTapeSummary:
    """Return only a completely authenticated descriptive summary, or refuse.

    Receipt verification and parsing each read the tape. Reference-line reloads
    use the parser's same handle and revalidate their exact body hashes. The
    64 MiB/context JSON line bound is a technical limit, never a statistical gate.
    This checks cold structural/custody consistency under the caller's trusted
    issued-request hash. It does not replay features, signals, OHLC or outcomes;
    the upstream label/parity proof is separate evidence.
    """
    try:
        return _read(context, symbol_receipt=symbol_receipt, expected_request_sha256=expected_request_sha256,
            expected_tape_sha256=expected_tape_sha256, expected_cell=expected_cell, entry_regime=entry_regime)
    except SignalOpportunityTapeError:
        raise
    except Exception as exc:
        raise SignalOpportunityTapeError("TAPE_VERIFICATION_FAILED", (type(exc).__name__,)) from exc


def read_independent_signal_r_tape(context, *, symbol_receipt, expected_request_sha256,
        expected_tape_sha256, expected_cell, entry_regime) -> SignalOpportunityTapeSummary:
    """Read explicit v2 cash-free modeled-R evidence through the same custody parser.

    Arithmetic and batch episode diagnostics are reconciled without replaying
    features, price paths, swap inputs, or any account or qualification decision.
    """
    try:
        return _read(context, symbol_receipt=symbol_receipt, expected_request_sha256=expected_request_sha256,
            expected_tape_sha256=expected_tape_sha256, expected_cell=expected_cell, entry_regime=entry_regime,
            independent=True)
    except SignalOpportunityTapeError:
        raise
    except Exception as exc:
        raise SignalOpportunityTapeError("TAPE_VERIFICATION_FAILED", (type(exc).__name__,)) from exc


__all__ = ["SignalOpportunityTapeError", "SignalOpportunityTapeSummary", "read_signal_opportunity_tape",
    "read_independent_signal_r_tape", "publish_signal_opportunity_horizon_preimage"]
