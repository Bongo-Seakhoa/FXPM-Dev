"""Resolve recorded derived costs by output identity, without recalibration.

The existing edition reader authenticates the retained data edition. The live
cell owner supplies original M5 products; this module creates no source scope,
prefix, profile or permission. A full calibration view is not a financial view.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
from typing import Mapping

import pm_explicit_derived_publication as publication
from pm_authority_spool import GenerationSpoolContext, reopen_generation_context, verify_symbol_receipt
from pm_explicit_derived_history import (
    DerivedSeries, Interval, PROTECTED_CUTOFF, SourceIdentity,
    SpreadIdentification, apply_spread_rule, canonical_json_bytes,
)
from pm_native_prefix_production import NativePrefixProduct, reverify_activated_native_prefix_product
from pm_spread_profile import FrozenSpreadTier, TIER_SCHEMA


CALCULATION_POLICY = "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION"
SOURCE_CONVENTION = "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN"
QUANTILE_METHOD = "higher_order_statistic_ceil((n-1)*p)"
_RECEIPT_MAX_BYTES = 64 * 1024


class DerivedCostError(ValueError):
    code = "COST_UNAVAILABLE"

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(self.code, reason_code)


def _require(value: bool, reason: str) -> None:
    if not value:
        raise DerivedCostError(reason)


def _sha(value) -> bool:
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _digest(value) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _detached(value):
    return json.loads(canonical_json_bytes(value).decode("ascii"))


def _utc(value: str) -> datetime:
    _require(type(value) is str, "COST_CUTOFF_INVALID")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise DerivedCostError("COST_CUTOFF_INVALID") from error
    _require(stamp.tzinfo is not None and stamp.utcoffset() == timedelta(0)
             and stamp.microsecond == 0, "COST_CUTOFF_INVALID")
    return stamp


def _positive(value) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value) and value > 0
    except OverflowError:
        return False


@dataclass(frozen=True, slots=True)
class DerivedCostEditionPins:
    root: Path
    receipt_json: bytes
    receipt_sha256: str
    generation_id: str
    native_universe_sha256: str
    plan_sha256: str
    implementation_sha256: str
    profile_sha256: str

    def identity(self) -> dict:
        return {name: getattr(self, name) for name in (
            "receipt_sha256", "generation_id", "native_universe_sha256",
            "plan_sha256", "implementation_sha256", "profile_sha256")}


def _receipt(pins: DerivedCostEditionPins) -> dict:
    _require(type(pins) is DerivedCostEditionPins and isinstance(pins.root, Path)
             and pins.root.is_absolute(), "COST_EDITION_PINS_INVALID")
    _require(type(pins.generation_id) is str and bool(pins.generation_id)
             and all(_sha(getattr(pins, name)) for name in (
                 "receipt_sha256", "native_universe_sha256", "plan_sha256",
                 "implementation_sha256", "profile_sha256")), "COST_EDITION_PINS_INVALID")
    _require(type(pins.receipt_json) is bytes and len(pins.receipt_json) <= _RECEIPT_MAX_BYTES,
             "COST_EDITION_RECEIPT_INVALID")
    try:
        value = publication._strict_json_bytes(pins.receipt_json, _RECEIPT_MAX_BYTES)
    except Exception as error:
        raise DerivedCostError("COST_EDITION_RECEIPT_INVALID") from error
    _require(type(value) is dict and _digest(value) == pins.receipt_sha256,
             "COST_EDITION_RECEIPT_IDENTITY_MISMATCH")
    _require(value.get("generation_id") == pins.generation_id
             and value.get("native_universe_sha256") == pins.native_universe_sha256
             and value.get("config_sha256") == pins.plan_sha256,
             "COST_EDITION_RECEIPT_IDENTITY_MISMATCH")
    return value


def _profile(profile: dict, pins: DerivedCostEditionPins) -> dict:
    _require(type(profile) is dict and _digest(profile) == pins.profile_sha256,
             "COST_PROFILE_IDENTITY_MISMATCH")
    _require(profile.get("source_kind") == "m5_derived"
             and profile.get("recipe_sha256") == publication.RECIPE_SHA256
             and profile.get("implementation_sha256") == pins.implementation_sha256
             and profile.get("plan_sha256") == pins.plan_sha256
             and profile.get("protected_cutoff_utc") == PROTECTED_CUTOFF
             and profile.get("calculation_policy") == CALCULATION_POLICY
             and profile.get("source_convention") == SOURCE_CONVENTION,
             "COST_PROFILE_POLICY_MISMATCH")
    return _detached(profile)


def _identifications(pins: DerivedCostEditionPins, receipt: dict) -> dict:
    manifest = verify_symbol_receipt(
        pins.root, receipt, expected_generation_id=pins.generation_id,
        expected_symbol=publication.EDITION_SYMBOL,
        expected_native_universe_sha256=pins.native_universe_sha256,
        expected_config_sha256=pins.plan_sha256, expected_manifest_domain=publication.DOMAIN)
    context = reopen_generation_context(
        pins.root, expected_generation_id=pins.generation_id,
        expected_native_universe_sha256=pins.native_universe_sha256,
        expected_config_sha256=pins.plan_sha256)
    roster = {row["artifact_id"]: row for row in manifest["artifacts"]}
    _require(len(roster) == len(manifest["artifacts"]), "COST_EDITION_ROSTER_INVALID")

    def registered(entry):
        _require(type(entry) is dict and roster.get(entry.get("artifact_id")) == entry,
                 "COST_EDITION_ROSTER_INVALID")
        return publication._read(context, entry["descriptor"], json_value=True)

    activation = registered(roster["activation"])
    _require(activation.get("implementation_sha256") == pins.implementation_sha256
             and activation.get("plan_sha256") == pins.plan_sha256
             and activation["profile"]["descriptor"]["sha256"] == pins.profile_sha256,
             "COST_EDITION_ACTIVATION_MISMATCH")
    result = {}
    for entry in activation["entries"]:
        if entry["status"] == "STOP":
            continue
        key = entry["symbol"], entry["timeframe"]
        _require(key not in result, "COST_EDITION_ROSTER_INVALID")
        result[key] = publication._restore_identification(registered(entry["identification"]))
    return result


def _apply_min(series: DerivedSeries, identification: SpreadIdentification) -> DerivedSeries:
    _require(type(series) is DerivedSeries and type(identification) is SpreadIdentification,
             "COST_VIEW_TYPE_INVALID")
    _require(identification.selected_spread_rule == "min"
             and identification.matching_candidates == ("min",)
             and identification.cost_status == "SOURCE_RULE_MATCHED",
             "COST_CONTROL_MIN_RULE_UNAVAILABLE")
    try:
        return apply_spread_rule(series, identification)
    except Exception as error:
        raise DerivedCostError("COST_VIEW_RULE_APPLICATION_FAILED") from error


def _validate_full_series(group: dict, full_series: DerivedSeries,
                          identification: SpreadIdentification, source: SourceIdentity,
                          point) -> DerivedSeries:
    full = _apply_min(full_series, identification)
    _require(group.get("source_kind") == "m5_derived" and group.get("cost_status") == publication.COST_READY
             and full.stratum.timeframe in {"M15", "M30", "H1"}
             and full.source.timeframe == "M5" and full.source.spread_unit == "points"
             and bool(full.bars), "COST_CELL_UNAVAILABLE")
    _require(_positive(point) and point == source.point == full.source.point
             and canonical_json_bytes(asdict(source)) == canonical_json_bytes(group["fine_source"])
             and canonical_json_bytes(asdict(full.source)) == canonical_json_bytes(group["fine_source"]),
             "COST_FINE_SOURCE_OR_POINT_MISMATCH")
    _require(asdict(full.interval) == group["available_interval"]
             and full.cutoff_utc == PROTECTED_CUTOFF
             and group["symbol"] == full.source.symbol
             and group["timeframe"] == full.stratum.timeframe,
             "COST_CALIBRATION_INTERVAL_MISMATCH")
    _require(full.membership_sha256 == group["membership_sha256"]
             and identification.evidence_sha256 == group["spread_identification_sha256"]
             and full.spread_identification_sha256 == group["spread_identification_sha256"]
             and full.selected_spread_rule == "min", "COST_MEMBERSHIP_OR_IDENTIFICATION_MISMATCH")
    _require(all(type(row.value.spread) is int and row.value.spread >= 0 for row in full.bars),
             "COST_RECORDED_SPREAD_MISSING")
    digest = hashlib.sha256()
    for chunk in publication._csv_chunks(full):
        digest.update(chunk)
    _require(digest.hexdigest() == group["target_bars_sha256"], "COST_TARGET_OUTPUT_MISMATCH")
    stats = group["statistics"]
    _require(stats.get("status") == "CALCULATED" and stats.get("quantile_method") == QUANTILE_METHOD
             and stats.get("point_price") == point
             and type(stats.get("target_prefix_rows")) is int
             and stats["target_prefix_rows"] == len(full.bars)
             and type(stats.get("invalid_rows")) is int and stats["invalid_rows"] == 0
             and type(stats.get("valid_rows")) is int and stats["valid_rows"] > 0,
             "COST_STATISTICS_INVALID")
    normal = stats["tiers"]["P75"]
    _require(normal.get("usability") == "POSITIVE_QUANTILE"
             and type(normal.get("spread_points")) is int and normal["spread_points"] > 0,
             "COST_REQUIRED_P75_UNAVAILABLE")
    for tier in ("P75", "P95"):
        value = stats["tiers"][tier]
        _require(value.get("usability") == "POSITIVE_QUANTILE"
                 and type(value.get("spread_points")) is int
                 and value["spread_points"] >= normal["spread_points"]
                 and _positive(value.get("full_spread_price"))
                 and value["full_spread_price"] == value["spread_points"] * point
                 and value.get("half_spread_price") == value["full_spread_price"] / 2,
                 "COST_QUANTILE_PRICE_INVALID")
    return full


def _validate_prefix(series: DerivedSeries, full: DerivedSeries,
                     identification: SpreadIdentification) -> DerivedSeries:
    selected = _apply_min(series, identification)
    start = _utc(selected.interval.start_utc)
    end = _utc(selected.interval.end_exclusive_utc)
    cutoff = _utc(selected.cutoff_utc)
    _require(_utc(full.interval.start_utc) <= start < end <= _utc(full.interval.end_exclusive_utc)
             and end <= cutoff <= _utc(PROTECTED_CUTOFF)
             and selected.stratum == full.stratum and selected.source == full.source,
             "COST_REQUEST_PREFIX_BINDING_MISMATCH")
    expected = tuple(row for row in full.bars
                     if _utc(row.value.open_utc) >= start and _utc(row.value.close_utc) <= end
                     and _utc(row.value.close_utc) <= cutoff)
    # Member digests include view-local ordinals, so compare represented bars.
    def represented(rows):
        return [{"value": asdict(row.value), "member_count": row.member_count,
                 "spread_candidates": row.spread_candidates} for row in rows]

    _require(bool(selected.bars) and canonical_json_bytes(represented(selected.bars))
             == canonical_json_bytes(represented(expected)),
             "COST_REQUEST_CAUSAL_PREFIX_MISMATCH")
    _require(selected.source_member_count == sum(row.member_count for row in selected.bars),
             "COST_REQUEST_MEMBERSHIP_COUNT_MISMATCH")
    return selected


def _validate_financial_request(series: DerivedSeries, full: DerivedSeries,
                                identification: SpreadIdentification) -> DerivedSeries:
    selected = _validate_prefix(series, full, identification)
    _require(canonical_json_bytes(asdict(series)) == canonical_json_bytes(asdict(selected)),
             "COST_REQUEST_MIN_RULE_NOT_FROZEN")
    return selected


def _owner_facts(owner, cutoff: str, source: SourceIdentity) -> tuple[NativePrefixProduct, dict]:
    # The owner rejects undeclared cutoffs; this cannot extend its acquisition.
    product = owner.product_at(cutoff)
    route = owner.route_at(cutoff)
    _require(type(product) is NativePrefixProduct and product.source_state == "AVAILABLE"
             and type(route) is dict and _utc(route["cutoff_utc"]) == _utc(cutoff)
             and product.index_sha256 == route["index_sha256"], "COST_M5_PRODUCT_INVALID")
    raw_source, activation = product.index["source"], product.index["activation"]
    _require(product.index["timeframe"] == "M5" and product.index["symbol"] == source.symbol
             and _utc(product.index["cutoff_utc"]) == _utc(cutoff)
             and activation["activation_id"] == source.activation_id == route["activation_id"]
             and activation["manifest_sha256"] == source.activation_sha256 == route["activation_manifest_sha256"]
             and raw_source["bars_sha256"] == source.bars_sha256
             and raw_source["audit_sidecar_sha256"] == source.sidecar_sha256
             and raw_source["provenance_sha256"] == source.provenance_sha256
             and raw_source["source_identity_sha256"] == route["source_identity_sha256"]
             and _sha(route["source_identity_sha256"]), "COST_M5_SOURCE_BINDING_MISMATCH")
    return product, route


def _require_execution_window(window, *, owner, view, product) -> None:
    fields = {"schema_version", "source_kind", "symbol", "timeframe", "m5_view_contract_sha256",
              "view_key", "view_payload_sha256", "view_membership_sha256", "view_frame_sha256",
              "view_start_utc", "view_end_exclusive_utc", "view_last_close_utc", "view_rows",
              "base_native_prefix_index_sha256", "base_native_prefix_content_sha256"}
    _require(type(window) is dict and set(window) == fields, "COST_EXECUTION_VIEW_WINDOW_INVALID")
    _require(window["schema_version"] == "fxpm.m5-derived-price-window.v1"
             and window["source_kind"] == "m5_derived" and window["symbol"] == view.source.symbol
             and window["timeframe"] == view.timeframe
             and window["m5_view_contract_sha256"] == owner.binding.m5_view_contract_sha256
             and window["view_key"] == view.view_key and window["view_payload_sha256"] == view.payload_sha256
             and window["view_membership_sha256"] == view.membership_sha256
             and window["base_native_prefix_index_sha256"] == product.index_sha256
             and window["base_native_prefix_content_sha256"] == product.native_prefix["prefix_content_sha256"]
             and type(window["view_rows"]) is int and window["view_rows"] == len(view.series.bars)
             and all(_sha(window[key]) for key in ("view_key", "view_payload_sha256",
                 "view_membership_sha256", "view_frame_sha256", "m5_view_contract_sha256"))
             and _utc(window["view_start_utc"]) < _utc(window["view_last_close_utc"])
             <= _utc(window["view_end_exclusive_utc"]) <= _utc(view.cutoff_utc),
             "COST_EXECUTION_VIEW_WINDOW_MISMATCH")


class BoundDerivedCosts:
    """Detached cost descriptors; not a data-source or fitted-state capability."""

    def __init__(self, pins: DerivedCostEditionPins, group: dict, source: SourceIdentity,
                 source_identity_sha256: str, point, *, execution_window=None) -> None:
        self.sha256 = pins.profile_sha256
        self._group_json = canonical_json_bytes(group)
        self._point = point
        self._source_identity_sha256 = source_identity_sha256
        self._source_json = canonical_json_bytes(asdict(source))
        self._edition_identity_json = canonical_json_bytes(pins.identity())
        self._execution_window_json = (None if execution_window is None
                                       else canonical_json_bytes(execution_window))

    def resolve(self, symbol, timeframe, tier, *, point, source_identity_sha256):
        try:
            return self._resolve(symbol, timeframe, tier, point=point,
                                 source_identity_sha256=source_identity_sha256)
        except DerivedCostError:
            raise
        except Exception as error:
            raise DerivedCostError("COST_TIER_BINDING_UNAVAILABLE") from error

    def _resolve(self, symbol, timeframe, tier, *, point, source_identity_sha256):
        group = json.loads(self._group_json)
        source = json.loads(self._source_json)
        _require(tier in {"P75", "P95"} and symbol == group["symbol"]
                 and timeframe == group["timeframe"] and _positive(point) and point == self._point
                 and source_identity_sha256 == self._source_identity_sha256,
                 "COST_TIER_BINDING_MISMATCH")
        value = group["statistics"]["tiers"][tier]
        descriptor = {
            "schema_version": TIER_SCHEMA, "profile_sha256": self.sha256,
            "activation": {"id": source["activation_id"], "manifest_sha256": source["activation_sha256"]},
            "protected_cutoff_utc": PROTECTED_CUTOFF, "symbol": symbol,
            "timeframe": timeframe, "tier": tier, "source_timeframe": "M5",
            "point_price": float(point), "spread_points": value["spread_points"],
            "full_spread_price": value["full_spread_price"],
            "source_identity_sha256": source_identity_sha256,
            "source_binding": {
                "bars_sha256": source["bars_sha256"], "sidecar_sha256": source["sidecar_sha256"],
                "provenance_sha256": source["provenance_sha256"],
                "target_bars_sha256": source["bars_sha256"],
                "target_sidecar_sha256": source["sidecar_sha256"],
            },
            "m5_derived_cost_binding": {
                "source_kind": "m5_derived", "cost_edition_identity": json.loads(self._edition_identity_json),
                "recipe_sha256": publication.RECIPE_SHA256, "fine_source": source,
                "available_interval": group["available_interval"],
                "derived_target_bars_sha256": group["target_bars_sha256"],
                "membership_sha256": group["membership_sha256"],
                "spread_identification_sha256": group["spread_identification_sha256"],
                "calculation_policy": CALCULATION_POLICY, "source_convention": SOURCE_CONVENTION,
                "quantile_method": QUANTILE_METHOD,
            },
            "cost_policy": "MAX_FROZEN_TIER_AND_RECORDED_SPREAD_BOTH_SIDES",
            "admission_policy": "EXACT_PRIOR_FINE_CLOSE_THEN_LIVE_FIRST_AND_FINAL_TICK_CAP",
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False,
        }
        if self._execution_window_json is not None:
            descriptor["m5_execution_view_binding"] = json.loads(self._execution_window_json)
        result = FrozenSpreadTier(canonical_json_bytes(descriptor).decode("ascii"))
        result.validate_execution(symbol, timeframe, point)
        return result

    def from_descriptor(self, descriptor, *, symbol, timeframe, point, source_identity_sha256):
        try:
            _require(isinstance(descriptor, Mapping), "COST_TIER_DESCRIPTOR_INVALID")
            resolved = self.resolve(symbol, timeframe, descriptor.get("tier"), point=point,
                                    source_identity_sha256=source_identity_sha256)
            _require(canonical_json_bytes(dict(descriptor)) == resolved._json.encode("ascii"),
                     "COST_TIER_DESCRIPTOR_MISMATCH")
            return resolved
        except DerivedCostError:
            raise
        except Exception as error:
            raise DerivedCostError("COST_TIER_DESCRIPTOR_INVALID") from error


class RecordedDerivedCosts:
    def __init__(self, pins: DerivedCostEditionPins) -> None:
        try:
            receipt = _receipt(pins)
            cold = publication.reopen_derived_edition(
                pins.root, receipt, expected_generation_id=pins.generation_id,
                expected_native_universe_sha256=pins.native_universe_sha256,
                expected_implementation_sha256=pins.implementation_sha256,
                expected_plan_sha256=pins.plan_sha256)
            profile = _profile(cold["profile"], pins)
            self._groups_json = canonical_json_bytes(profile["groups"])
            self._identifications = _identifications(pins, receipt)
            self._pins = pins
            self.sha256 = pins.profile_sha256
            # Do not retain the cold reader's complete higher-timeframe payloads.
            del cold
        except DerivedCostError:
            raise
        except Exception as error:
            raise DerivedCostError("COST_EDITION_READ_FAILED") from error

    def reopen_route(self) -> dict:
        return _cost_reopen_route(self._pins)

    def cell_metadata(self, symbol: str, timeframe: str) -> dict:
        matches = [row for row in json.loads(self._groups_json)
                   if (row["symbol"], row["timeframe"]) == (symbol, timeframe)]
        _require(len(matches) == 1 and matches[0].get("cost_status") == publication.COST_READY,
                 "COST_CELL_UNAVAILABLE")
        return matches[0]

    def identification(self, symbol: str, timeframe: str) -> SpreadIdentification:
        self.cell_metadata(symbol, timeframe)
        value = self._identifications.get((symbol, timeframe))
        _require(type(value) is SpreadIdentification and value.selected_spread_rule == "min"
                 and value.matching_candidates == ("min",) and value.cost_status == "SOURCE_RULE_MATCHED",
                 "COST_CONTROL_MIN_RULE_UNAVAILABLE")
        return value

    def bind_view(self, owner, *, context=None, calibration_lease,
                  request_lease, point) -> BoundDerivedCosts:
        try:
            from pm_m5_job_view import (
                AccountViewBinding, CalibrationViewBinding, M5JobViews, NoTradeAccountViewBinding, SlotViewBinding,
            )

            _require(type(owner) is M5JobViews, "COST_ORIGINAL_CELL_OWNER_REQUIRED")
            _require(type(context) is GenerationSpoolContext, "COST_ORIGINAL_GENERATION_CONTEXT_REQUIRED")
            reopened = reopen_generation_context(context.root,
                expected_generation_id=context.generation_id,
                expected_native_universe_sha256=context.native_universe_sha256,
                expected_config_sha256=context.config_sha256)
            _require(reopened.context_sha256 == context.context_sha256
                     == owner.binding.generation_context_sha256
                     and context.config_sha256 == owner.binding.config_sha256,
                     "COST_GENERATION_CONTEXT_MISMATCH")
            full_view = owner.view(calibration_lease)
            request_view = owner.view(request_lease)
            calibration = owner.consumer_binding(calibration_lease)
            request = owner.consumer_binding(request_lease)
            _require(type(calibration) is CalibrationViewBinding
                     and calibration.profile_sha256 == self.sha256
                     and type(request) in (SlotViewBinding, AccountViewBinding, NoTradeAccountViewBinding),
                     "COST_VIEW_PURPOSE_MISMATCH")
            full_series, request_series = full_view.series, request_view.series
            source = owner.source_identity
            _require(type(source) is SourceIdentity, "COST_M5_SOURCE_BINDING_MISMATCH")
            _require(type(full_series) is DerivedSeries and type(request_series) is DerivedSeries,
                     "COST_VIEW_TYPE_INVALID")
            key = request_series.source.symbol, request_series.stratum.timeframe
            group = self.cell_metadata(*key)
            identification = self.identification(*key)
            # This full product/view must have been predeclared for calibration.
            calibration_product, _ = _owner_facts(owner, full_series.cutoff_utc, source)
            verified_calibration = reverify_activated_native_prefix_product(
                calibration_product, context, require_current_activation=True)
            _require(verified_calibration.index_sha256 == calibration_product.index_sha256,
                     "COST_CALIBRATION_PRODUCT_CHANGED")
            full = _validate_full_series(group, full_series, identification, source, point)
            _validate_financial_request(request_series, full, identification)
            product, route = _owner_facts(owner, request_series.cutoff_utc, source)
            from pm_authoritative_search_execution import m5_view_price_frame
            _frame, window = m5_view_price_frame(context=context, owner=owner, lease=request_lease)
            _require_execution_window(window, owner=owner, view=request_view, product=product)
            return BoundDerivedCosts(self._pins, group, source, route["source_identity_sha256"], point,
                                     execution_window=window)
        except DerivedCostError:
            raise
        except Exception as error:
            raise DerivedCostError("COST_VIEW_BINDING_UNAVAILABLE") from error


def open_recorded_derived_costs(pins: DerivedCostEditionPins) -> RecordedDerivedCosts:
    return RecordedDerivedCosts(pins)


def _cost_reopen_route(pins: DerivedCostEditionPins) -> dict:
    receipt = _receipt(pins)
    return _detached({"schema_version": "fxpm.recorded-derived-cost-route.v1",
        "metadata_kind": "ROUTING_ONLY_NOT_AUTHORITY", "root": str(pins.root),
        "receipt": receipt, "cost_edition_identity": pins.identity()})


def _decode_cost_reopen_route(route) -> DerivedCostEditionPins:
    try:
        encoded = canonical_json_bytes(route)
        _require(len(encoded) <= 96 * 1024, "COST_REOPEN_ROUTE_LIMIT")
        value = json.loads(encoded)
        _require(type(value) is dict and set(value) == {"schema_version", "metadata_kind", "root",
            "receipt", "cost_edition_identity"} and value["schema_version"] == "fxpm.recorded-derived-cost-route.v1"
            and value["metadata_kind"] == "ROUTING_ONLY_NOT_AUTHORITY"
            and type(value["root"]) is str and Path(value["root"]).is_absolute()
            and type(value["receipt"]) is dict, "COST_REOPEN_ROUTE_INVALID")
        identity = value["cost_edition_identity"]
        _require(type(identity) is dict and set(identity) == {"receipt_sha256", "generation_id",
            "native_universe_sha256", "plan_sha256", "implementation_sha256", "profile_sha256"},
            "COST_REOPEN_ROUTE_INVALID")
        pins = DerivedCostEditionPins(Path(value["root"]), canonical_json_bytes(value["receipt"]), **identity)
        _receipt(pins)
        return pins
    except DerivedCostError:
        raise
    except Exception as error:
        raise DerivedCostError("COST_REOPEN_ROUTE_INVALID") from error


def open_recorded_derived_costs_route(route) -> RecordedDerivedCosts:
    return open_recorded_derived_costs(_decode_cost_reopen_route(route))
