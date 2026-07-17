from __future__ import annotations

import json
import math
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


__all__ = [
    "CurrentRefitParameterCodecError",
    "canonical_parameter_bytes",
    "canonicalize_parameter_mapping",
    "canonicalize_parameter_value",
    "decode_parameter_mapping",
    "parameter_values_equal",
]
