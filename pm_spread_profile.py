"""Frozen spread cost identities and causal entry admission, without recalibration."""

from dataclasses import dataclass
from functools import cached_property, lru_cache
import hashlib
import json
import math
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd


PROFILE_SCHEMA = "fxpm.frozen-spread-profile.v1"
TIER_SCHEMA = "fxpm.frozen-spread-tier.v1"
DEFAULT_PROFILE_PATH = Path(__file__).resolve().parent / "data/native/terms/spread_profile.json"
TF_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800,
              "H1": 3600, "H4": 14400, "D1": 86400}
RETIRED_COST_POLICY = "RETIRED_MULTIPLIER_COST_POLICY_INTERVAL_QUALIFICATION_REQUIRED"
ADMISSION_POLICY = "EXACT_PRIOR_FINE_CLOSE_CAP_ADMIT_ONLY_V2"
ADMISSION_SCHEMA = "fxpm.spread-entry-admission.v2"


class SpreadProfileError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SpreadProfileError("SPREAD_PROFILE_DUPLICATE_KEY")
        result[key] = value
    return result


@dataclass(frozen=True)
class FrozenSpreadTier:
    """A cost identity, never a Winner or a trading permission."""

    _json: str

    def descriptor(self):
        return json.loads(self._json)

    @cached_property
    def symbol(self):
        return self.descriptor()["symbol"]

    @cached_property
    def timeframe(self):
        return self.descriptor()["timeframe"]

    @cached_property
    def tier(self):
        return self.descriptor()["tier"]

    @cached_property
    def source_timeframe(self):
        return self.descriptor()["source_timeframe"]

    @cached_property
    def point(self):
        return self.descriptor()["point_price"]

    @cached_property
    def spread_points(self):
        return self.descriptor()["spread_points"]

    @cached_property
    def full_spread_price(self):
        return self.descriptor()["full_spread_price"]

    def validate_execution(self, symbol, timeframe, point):
        body = self.descriptor()
        if (body.get("schema_version") != TIER_SCHEMA
                or body.get("qualification_authority") is not False
                or self.symbol != symbol or self.timeframe != timeframe
                or self.point != point or self.tier not in {"P75", "P95"}
                or type(self.spread_points) is not int or self.spread_points <= 0
                or not math.isfinite(self.full_spread_price)
                or self.full_spread_price != self.spread_points * self.point):
            raise SpreadProfileError("SPREAD_TIER_EXECUTION_BINDING_MISMATCH")


class FrozenSpreadProfile:
    def __init__(self, path):
        raw = Path(path).read_bytes()
        self.sha256 = hashlib.sha256(raw).hexdigest()
        body = json.loads(raw, object_pairs_hook=_unique_object)
        if (body.get("schema_version") != PROFILE_SCHEMA
                or body.get("status") != "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS"
                or body.get("calculation_policy") != "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION"
                or body.get("source_convention") != "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN"
                or body.get("production_authority") is not False):
            raise SpreadProfileError("SPREAD_PROFILE_CONTRACT_INVALID")
        self._body = body
        self._cells = {}
        for cell in body["cells"]:
            key = (cell["symbol"], cell["timeframe"])
            if key in self._cells:
                raise SpreadProfileError("SPREAD_PROFILE_DUPLICATE_CELL")
            self._cells[key] = cell

    def resolve(self, symbol, timeframe, tier, *, point, source_identity_sha256):
        if tier not in {"P75", "P95"}:
            raise SpreadProfileError("SPREAD_TIER_INVALID")
        cell = self._cells.get((symbol, timeframe))
        if cell is None or cell.get("status") != "CALCULATED":
            raise SpreadProfileError("SPREAD_PROFILE_UNAVAILABLE")
        if (isinstance(point, bool) or not isinstance(point, (int, float))
                or not math.isfinite(point) or point <= 0
                or point != cell.get("point_price")
                or not source_identity_sha256
                or source_identity_sha256 != cell["source_identity"].get("identity_sha256")):
            raise SpreadProfileError("SPREAD_PROFILE_POINT_OR_BROKER_MISMATCH")
        # A positive P95 cannot rescue an unusable required P75.
        normal = cell["tiers"]["P75"]
        value = cell["tiers"][tier]
        if (normal.get("usability") != "POSITIVE_QUANTILE"
                or value.get("usability") != "POSITIVE_QUANTILE"
                or type(normal.get("spread_points")) is not int
                or normal["spread_points"] <= 0
                or type(value.get("spread_points")) is not int
                or value["spread_points"] < normal["spread_points"]):
            raise SpreadProfileError("SPREAD_PROFILE_UNUSABLE_QUANTILE")
        source_tf = cell["source_timeframe"]
        if (source_tf not in TF_SECONDS or timeframe not in TF_SECONDS
                or TF_SECONDS[source_tf] > TF_SECONDS[timeframe]
                or value["full_spread_price"] != value["spread_points"] * point
                or value["half_spread_price"] != value["full_spread_price"] / 2):
            raise SpreadProfileError("SPREAD_PROFILE_VALUE_OR_SOURCE_INVALID")
        descriptor = {
            "schema_version": TIER_SCHEMA, "profile_sha256": self.sha256,
            "activation": self._body["activation"],
            "protected_cutoff_utc": self._body["protected_cutoff_utc"],
            "symbol": symbol, "timeframe": timeframe, "tier": tier,
            "source_timeframe": source_tf, "point_price": float(point),
            "spread_points": value["spread_points"],
            "full_spread_price": value["full_spread_price"],
            "source_identity_sha256": source_identity_sha256,
            "source_binding": cell["source_binding"],
            "cost_policy": "MAX_FROZEN_TIER_AND_RECORDED_SPREAD_BOTH_SIDES",
            "admission_policy": "EXACT_PRIOR_FINE_CLOSE_THEN_LIVE_FIRST_AND_FINAL_TICK_CAP",
            "qualification_authority": False,
        }
        return FrozenSpreadTier(_canonical(descriptor))

    def from_descriptor(self, descriptor, *, symbol, timeframe, point,
                        source_identity_sha256):
        if not isinstance(descriptor, Mapping) or descriptor.get("schema_version") != TIER_SCHEMA:
            raise SpreadProfileError("SPREAD_TIER_DESCRIPTOR_MISSING_OR_RETIRED")
        resolved = self.resolve(symbol, timeframe, descriptor.get("tier"), point=point,
                                source_identity_sha256=source_identity_sha256)
        if _canonical(dict(descriptor)) != resolved._json:
            raise SpreadProfileError("SPREAD_TIER_DESCRIPTOR_MISMATCH")
        return resolved


@lru_cache(maxsize=4)
def _load_profile_snapshot(path, size, mtime_ns):
    return FrozenSpreadProfile(path)


def load_spread_profile(path=DEFAULT_PROFILE_PATH):
    path = Path(path).resolve()
    stat = path.stat()
    return _load_profile_snapshot(str(path), stat.st_size, stat.st_mtime_ns)


def _utc_axis(values, name):
    axis = pd.DatetimeIndex(values)
    if axis.tz is None:
        axis = axis.tz_localize("UTC")
    else:
        axis = axis.tz_convert("UTC")
    if axis.hasnans or axis.has_duplicates or not axis.is_monotonic_increasing:
        raise SpreadProfileError("SPREAD_OBSERVATION_AXIS_INVALID:" + name)
    return axis


def align_prior_completed_spreads(target_index, source_frame, source_timeframe, *,
                                  include_matched_closes=False):
    """Return exact-close observations; gaps remain NaN and zeros remain zero."""
    if source_timeframe not in TF_SECONDS:
        raise SpreadProfileError("SPREAD_SOURCE_TIMEFRAME_INVALID")
    target = _utc_axis(target_index, "target")
    opens = _utc_axis(source_frame.index, "source")
    if "_BAR_CLOSE_TS" in source_frame:
        closes = _utc_axis(source_frame["_BAR_CLOSE_TS"], "source_close")
    else:
        closes = opens + pd.to_timedelta(TF_SECONDS[source_timeframe], unit="s")
    if len(opens) and (closes <= opens).any():
        raise SpreadProfileError("SPREAD_SOURCE_CLOSE_INVALID")
    if len(opens) > 1 and (closes[:-1] > opens[1:]).any():
        raise SpreadProfileError("SPREAD_SOURCE_BARS_OVERLAP")
    if "Spread" not in source_frame:
        raise SpreadProfileError("SPREAD_SOURCE_COLUMN_MISSING")
    values = pd.to_numeric(source_frame["Spread"], errors="raise").to_numpy(dtype=float)
    positions = closes.get_indexer(target)
    result = np.full(len(target), np.nan)
    found = positions >= 0
    result[found] = values[positions[found]]
    points = pd.Series(result, index=target_index, name="EntrySpreadPoints")
    if not include_matched_closes:
        return points
    matched_closes = pd.Series(pd.NaT, index=target_index,
                              dtype="datetime64[ns, UTC]", name="EntrySpreadSourceCloseUTC")
    if found.any():
        matched_closes.iloc[found] = closes[positions[found]]
    return points, matched_closes


def spread_admission(tier, observation):
    if observation is None or pd.isna(observation):
        return "MISSING"
    if (isinstance(observation, (bool, np.bool_)) or not isinstance(observation, (int, float, np.number))
            or not math.isfinite(float(observation)) or observation < 0):
        return "INVALID"
    return "ADMIT" if observation <= tier.spread_points else "REFUSE"
