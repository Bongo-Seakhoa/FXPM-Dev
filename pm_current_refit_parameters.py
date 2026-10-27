from __future__ import annotations

import json
import math
import hashlib
import itertools
from pathlib import Path
import struct
from collections.abc import Mapping
from typing import Any

import numpy as np


MAX_PARAMETER_COUNT = 128
MAX_PARAMETER_KEY_BYTES = 128
MAX_PARAMETER_STRING_BYTES = 512
MAX_PARAMETER_SEQUENCE_ITEMS = 64
MAX_PARAMETER_BYTES = 256 * 1024
_MIN_INTEGER = -(2**63)
_MAX_INTEGER = 2**63 - 1


class CurrentRefitParameterCodecError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _fail(code: str) -> None:
    raise CurrentRefitParameterCodecError(code)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CurrentRefitParameterCodecError("PARAMETER_JSON_INVALID") from exc


def _scalar(value: Any) -> bool | int | float | str:
    if type(value) is bool:
        return value
    if isinstance(value, np.bool_):
        return bool(value)
    if type(value) is int or isinstance(value, np.integer):
        result = int(value)
        if not _MIN_INTEGER <= result <= _MAX_INTEGER:
            _fail("PARAMETER_INTEGER_INVALID")
        return result
    if type(value) is float or isinstance(value, np.floating):
        result = float(value)
        if not math.isfinite(result):
            _fail("PARAMETER_FLOAT_INVALID")
        return 0.0 if result == 0.0 else result
    if type(value) is str:
        try:
            encoded = value.encode("utf-8")
        except UnicodeError as exc:
            raise CurrentRefitParameterCodecError("PARAMETER_STRING_INVALID") from exc
        if len(encoded) > MAX_PARAMETER_STRING_BYTES:
            _fail("PARAMETER_STRING_INVALID")
        return value
    _fail("PARAMETER_SCALAR_INVALID")


def canonicalize_parameter_value(
    value: Any,
) -> bool | int | float | str | list[Any]:
    if type(value) in (list, tuple):
        if not 1 <= len(value) <= MAX_PARAMETER_SEQUENCE_ITEMS:
            _fail("PARAMETER_SEQUENCE_INVALID")
        result: list[Any] = []
        for item in value:
            if type(item) in (list, tuple, dict) or isinstance(item, Mapping):
                _fail("PARAMETER_SEQUENCE_NESTED")
            result.append(_scalar(item))
        return result
    return _scalar(value)


def canonicalize_parameter_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not 1 <= len(value) <= MAX_PARAMETER_COUNT:
        _fail("PARAMETER_MAPPING_INVALID")
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if type(key) is not str or not key:
            _fail("PARAMETER_KEY_INVALID")
        try:
            encoded_key = key.encode("utf-8")
        except UnicodeError as exc:
            raise CurrentRefitParameterCodecError("PARAMETER_KEY_INVALID") from exc
        if len(encoded_key) > MAX_PARAMETER_KEY_BYTES:
            _fail("PARAMETER_KEY_INVALID")
        normalized[key] = canonicalize_parameter_value(item)
    normalized = {key: normalized[key] for key in sorted(normalized)}
    if len(_canonical_bytes(normalized)) > MAX_PARAMETER_BYTES:
        _fail("PARAMETER_MAPPING_TOO_LARGE")
    return normalized


def canonical_parameter_bytes(value: Any) -> bytes:
    return _canonical_bytes(canonicalize_parameter_mapping(value))


def _wire_value(value: Any) -> bool:
    if type(value) in (bool, int, float, str):
        return True
    return type(value) is list and all(
        type(item) in (bool, int, float, str) for item in value
    )


def decode_parameter_mapping(
    value: Any,
    templates: Mapping[str, Any],
) -> dict[str, Any]:
    if type(value) is not dict or not isinstance(templates, Mapping):
        _fail("PARAMETER_WIRE_INVALID")
    if not all(type(key) is str and _wire_value(item) for key, item in value.items()):
        _fail("PARAMETER_WIRE_INVALID")
    canonical = canonicalize_parameter_mapping(value)
    if _canonical_bytes(value) != _canonical_bytes(canonical):
        _fail("PARAMETER_WIRE_NONCANONICAL")

    decoded: dict[str, Any] = {}
    for key, item in canonical.items():
        if key not in templates:
            _fail("PARAMETER_TEMPLATE_MISMATCH")
        template = templates[key]
        if type(template) in (tuple, list):
            if type(item) is not list:
                _fail("PARAMETER_TEMPLATE_MISMATCH")
            expected_types = {
                type(canonicalize_parameter_value(element)) for element in template
            }
            if expected_types and any(
                type(element) not in expected_types for element in item
            ):
                _fail("PARAMETER_TEMPLATE_MISMATCH")
            decoded[key] = tuple(item) if type(template) is tuple else list(item)
            continue
        expected = canonicalize_parameter_value(template)
        if type(item) is not type(expected):
            _fail("PARAMETER_TEMPLATE_MISMATCH")
        decoded[key] = item
    return decoded


def parameter_values_equal(left: Any, right: Any) -> bool:
    try:
        left_value = canonicalize_parameter_value(left)
        right_value = canonicalize_parameter_value(right)
    except CurrentRefitParameterCodecError:
        return False
    return _canonical_bytes(left_value) == _canonical_bytes(right_value)


PARAMETER_DOMAIN_SCHEMA = "fxpm.parameter-domain.v1"
RETAINED_STRATEGY_DOMAIN_V1_SHA256 = "b2f36bbc8dfe6d493b5134a6d68778b8e175a75eaa4231b5f7c66b1358278722"
RETAINED_STRATEGY_SOURCE_V1_SHA256 = "326aad2b96979eb00507b3f4e37bf487f9624d7af8e43e16ca44a5d06615c9ad"
RETAINED_STRATEGY_V1_SUCCESSOR_SHA256 = "11a14733b32e20408ba6177523e3282d10285ee6c7198b3e8f3293399f42d625"


def build_parameter_domain(defaults, grid, *, coupled_groups):
    defaults = canonicalize_parameter_mapping(defaults)
    if not isinstance(grid, Mapping) or not set(grid) <= set(defaults):
        _fail("PARAMETER_DOMAIN_INVALID")
    choices_by_name = {}
    for name, choices in grid.items():
        if type(choices) not in (list, tuple) or not choices:
            _fail("PARAMETER_DOMAIN_INVALID")
        normalized = [canonicalize_parameter_value(value) for value in choices]
        keys = [_canonical_bytes(value) for value in normalized]
        if len(keys) != len(set(keys)) or _canonical_bytes(defaults[name]) not in keys:
            _fail("PARAMETER_DOMAIN_INVALID")
        choices_by_name[name] = normalized
    if type(coupled_groups) not in (tuple, list):
        _fail("PARAMETER_DOMAIN_INVALID")
    groups, grouped = [], {}
    for group in coupled_groups:
        if not isinstance(group, Mapping) or set(group) != {"names", "choices"}:
            _fail("PARAMETER_DOMAIN_INVALID")
        names, rows = group["names"], group["choices"]
        if (type(names) not in (list, tuple) or len(names) < 2
                or any(type(name) is not str for name in names) or len(set(names)) != len(names)
                or any(name not in choices_by_name or name in grouped for name in names)
                or type(rows) not in (list, tuple) or not rows):
            _fail("PARAMETER_DOMAIN_INVALID")
        normalized_rows = []
        for row in rows:
            if type(row) not in (list, tuple) or len(row) != len(names):
                _fail("PARAMETER_DOMAIN_INVALID")
            values = [canonicalize_parameter_value(value) for value in row]
            if any(_canonical_bytes(value) not in {_canonical_bytes(choice) for choice in choices_by_name[name]}
                   for name, value in zip(names, values)):
                _fail("PARAMETER_DOMAIN_INVALID")
            normalized_rows.append(values)
        keys = [_canonical_bytes(row) for row in normalized_rows]
        if (len(keys) != len(set(keys))
                or _canonical_bytes([defaults[name] for name in names]) not in keys):
            _fail("PARAMETER_DOMAIN_INVALID")
        for position, name in enumerate(names):
            if ({_canonical_bytes(row[position]) for row in normalized_rows}
                    != {_canonical_bytes(value) for value in choices_by_name[name]}):
                _fail("PARAMETER_DOMAIN_INVALID")
        index = len(groups)
        groups.append({"names": list(names), "choices": normalized_rows})
        grouped.update((name, index) for name in names)
    axes, emitted = [], set()
    for name, choices in choices_by_name.items():
        if name in grouped:
            index = grouped[name]
            if index not in emitted:
                axes.append(groups[index])
                emitted.add(index)
        else:
            axes.append({"names": [name], "choices": [[value] for value in choices]})
    domain = {"schema_version": PARAMETER_DOMAIN_SCHEMA, "defaults": defaults,
              "axes": axes, "point_count": math.prod(len(axis["choices"]) for axis in axes)}
    _validated_parameter_domain(domain)
    return domain


def _validated_parameter_domain(domain):
    if (type(domain) is not dict or set(domain) != {"schema_version", "defaults", "axes", "point_count"}
            or domain["schema_version"] != PARAMETER_DOMAIN_SCHEMA or type(domain["axes"]) is not list):
        _fail("PARAMETER_DOMAIN_INVALID")
    defaults = canonicalize_parameter_mapping(domain["defaults"])
    if _canonical_bytes(defaults) != _canonical_bytes(domain["defaults"]):
        _fail("PARAMETER_DOMAIN_INVALID")
    decode_parameter_mapping(domain["defaults"], defaults)
    seen, count = set(), 1
    for axis in domain["axes"]:
        if type(axis) is not dict or set(axis) != {"names", "choices"}:
            _fail("PARAMETER_DOMAIN_INVALID")
        names, rows = axis["names"], axis["choices"]
        if (type(names) is not list or not names or any(type(name) is not str for name in names)
                or len(set(names)) != len(names)
                or any(name not in defaults or name in seen for name in names)
                or type(rows) is not list or not rows):
            _fail("PARAMETER_DOMAIN_INVALID")
        seen.update(names)
        row_keys = []
        for row in rows:
            if type(row) is not list or len(row) != len(names):
                _fail("PARAMETER_DOMAIN_INVALID")
            normalized = [canonicalize_parameter_value(value) for value in row]
            if _canonical_bytes(row) != _canonical_bytes(normalized):
                _fail("PARAMETER_DOMAIN_INVALID")
            decode_parameter_mapping(dict(zip(names, row)), defaults)
            row_keys.append(_canonical_bytes(row))
        if (len(row_keys) != len(set(row_keys))
                or _canonical_bytes([defaults[name] for name in names]) not in row_keys):
            _fail("PARAMETER_DOMAIN_INVALID")
        count *= len(rows)
    if type(domain["point_count"]) is not int or domain["point_count"] != count:
        _fail("PARAMETER_DOMAIN_INVALID")
    return defaults


def validate_parameter_domain_member(domain, params):
    defaults = _validated_parameter_domain(domain)
    return _validate_parameter_domain_member(domain, params, defaults)


def _validate_parameter_domain_member(domain, params, defaults):
    parameters = canonicalize_parameter_mapping(params)
    if set(parameters) != set(defaults):
        _fail("PARAMETER_DOMAIN_MEMBER_INVALID")
    decode_parameter_mapping(parameters, defaults)
    variable = set()
    for axis in domain["axes"]:
        variable.update(axis["names"])
        row = [parameters[name] for name in axis["names"]]
        if _canonical_bytes(row) not in {_canonical_bytes(value) for value in axis["choices"]}:
            _fail("PARAMETER_DOMAIN_MEMBER_INVALID")
    if any(not parameter_values_equal(parameters[name], value)
           for name, value in defaults.items() if name not in variable):
        _fail("PARAMETER_DOMAIN_MEMBER_INVALID")
    return parameters


def parameter_domain_point(domain, ordinal):
    defaults = _validated_parameter_domain(domain)
    if type(ordinal) is not int or not 0 <= ordinal < domain["point_count"]:
        _fail("PARAMETER_DOMAIN_ORDINAL_INVALID")
    result = dict(defaults)
    for axis in reversed(domain["axes"]):
        ordinal, index = divmod(ordinal, len(axis["choices"]))
        result.update(zip(axis["names"], axis["choices"][index]))
    return canonicalize_parameter_mapping(result)


def parameter_domain_default_ordinal(domain):
    defaults = _validated_parameter_domain(domain)
    result = 0
    for axis in domain["axes"]:
        target = _canonical_bytes([defaults[name] for name in axis["names"]])
        index = next(index for index, row in enumerate(axis["choices"]) if _canonical_bytes(row) == target)
        result = result * len(axis["choices"]) + index
    return result


def iter_parameter_domain_projection(domain, keys):
    defaults = _validated_parameter_domain(domain)
    keys = tuple(keys)
    if any(type(key) is not str for key in keys) or len(set(keys)) != len(keys):
        _fail("PARAMETER_DOMAIN_PROJECTION_INVALID")
    relevant = set(keys) & set(defaults)
    axes = []
    for axis in domain["axes"]:
        selected = [name for name in axis["names"] if name in relevant]
        if not selected:
            continue
        default_row = [defaults[name] for name in axis["names"]]
        rows = [default_row, *axis["choices"]]
        unique = {}
        for row in rows:
            reduced = {name: row[axis["names"].index(name)] for name in selected}
            unique.setdefault(_canonical_bytes(reduced), row)
        axes.append((axis["names"], tuple(unique.values())))
    for rows in itertools.product(*(choices for _, choices in axes)):
        witness = dict(defaults)
        for (names, _), row in zip(axes, rows):
            witness.update(zip(names, row))
        witness = _validate_parameter_domain_member(domain, witness, defaults)
        yield witness, {key: witness[key] for key in keys if key in relevant}


def decode_retained_parameter_metadata(value):
    try:
        tag = value["type"]
        if tag == "numpy_scalar" and set(value) == {"type", "dtype", "bits_hex"}:
            dtype = np.dtype(value["dtype"])
            raw = bytes.fromhex(value["bits_hex"])
            if dtype.hasobject or len(raw) != dtype.itemsize:
                _fail("RETAINED_PARAMETER_METADATA_INVALID")
            return np.frombuffer(raw, dtype=dtype)[0]
        if tag == "float" and set(value) == {"type", "bits_hex"}:
            result = struct.unpack(">d", bytes.fromhex(value["bits_hex"]))[0]
            if not math.isfinite(result):
                _fail("RETAINED_PARAMETER_METADATA_INVALID")
            return result
        if tag == "int" and set(value) == {"type", "value"}:
            return int(value["value"])
        if tag in ("bool", "str") and set(value) == {"type", "value"}:
            if type(value["value"]).__name__ != tag:
                _fail("RETAINED_PARAMETER_METADATA_INVALID")
            return value["value"]
        if tag in ("tuple", "list") and set(value) == {"type", "items"}:
            result = [decode_retained_parameter_metadata(item) for item in value["items"]]
            return tuple(result) if tag == "tuple" else result
        if tag == "dict" and set(value) == {"type", "items"}:
            result = {}
            for key, item in value["items"]:
                key = decode_retained_parameter_metadata(key)
                if key in result:
                    _fail("RETAINED_PARAMETER_METADATA_INVALID")
                result[key] = decode_retained_parameter_metadata(item)
            return result
    except (KeyError, TypeError, ValueError, struct.error) as exc:
        raise CurrentRefitParameterCodecError("RETAINED_PARAMETER_METADATA_INVALID") from exc
    _fail("RETAINED_PARAMETER_METADATA_INVALID")


def load_retained_strategy_domain_v1():
    path = Path(__file__).with_name("pm_strategy_domain_v1.json")
    try:
        with path.open("rb") as stream:
            raw = stream.read(944992)
        if len(raw) != 944991 or hashlib.sha256(raw).hexdigest() != RETAINED_STRATEGY_DOMAIN_V1_SHA256:
            _fail("RETAINED_STRATEGY_DOMAIN_IDENTITY_CHANGED")
        body = json.loads(raw.decode("utf-8"))
        if (body["schema_version"] != "fxpm.strategy-domain-source-edition.v1"
                or body["edition_id"] != "ORIGINAL_V1_326AAD2B" or _canonical_bytes(body) != raw
                or body["registry_source_identity"] != {"sha256": RETAINED_STRATEGY_SOURCE_V1_SHA256, "bytes": 301982}
                or len(body["strategy_names"]) != 100 or len(set(body["strategy_names"])) != 100
                or [row["strategy_id"] for row in body["strategies"]] != body["strategy_names"]):
            _fail("RETAINED_STRATEGY_DOMAIN_INVALID")
        semantics = body["process_template"]["semantics"]
        domains = semantics["strategy_domains"]
        if ([row["strategy_id"] for row in domains] != body["strategy_names"]
                or hashlib.sha256(_canonical_bytes(domains)).hexdigest() != body["planner_strategy_domains_sha256"]
                or hashlib.sha256(_canonical_bytes(semantics)).hexdigest() != body["planner_process_semantics_sha256"]):
            _fail("RETAINED_STRATEGY_DOMAIN_INVALID")
        for ordinal, (record, domain) in enumerate(zip(body["strategies"], domains)):
            if (record["ordinal"] != ordinal or domain["ordinal"] != ordinal
                    or domain["strategy_source_sha256"] != RETAINED_STRATEGY_SOURCE_V1_SHA256):
                _fail("RETAINED_STRATEGY_DOMAIN_INVALID")
            defaults = decode_retained_parameter_metadata(record["typed_metadata"]["defaults"])
            grid = decode_retained_parameter_metadata(record["typed_metadata"]["grid"])
            original = build_parameter_domain(defaults, grid, coupled_groups=())
            if (original["defaults"] != domain["defaults"] or original["point_count"] != domain["grid_size"]
                    or record["default_parameter_wire"] != original["defaults"]):
                _fail("RETAINED_STRATEGY_DOMAIN_INVALID")
        return body
    except CurrentRefitParameterCodecError:
        raise
    except (OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
        raise CurrentRefitParameterCodecError("RETAINED_STRATEGY_DOMAIN_UNAVAILABLE") from exc


__all__ = [
    "CurrentRefitParameterCodecError",
    "canonical_parameter_bytes",
    "canonicalize_parameter_mapping",
    "canonicalize_parameter_value",
    "decode_parameter_mapping",
    "parameter_values_equal",
    "PARAMETER_DOMAIN_SCHEMA",
    "RETAINED_STRATEGY_SOURCE_V1_SHA256",
    "RETAINED_STRATEGY_V1_SUCCESSOR_SHA256",
    "build_parameter_domain",
    "validate_parameter_domain_member",
    "parameter_domain_point",
    "parameter_domain_default_ordinal",
    "iter_parameter_domain_projection",
    "decode_retained_parameter_metadata",
    "load_retained_strategy_domain_v1",
]
