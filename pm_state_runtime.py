"""Authenticated, bounded state runtime foundation for FxPM.

This module is deliberately standalone.  Production integration is withheld
until the execution layer can supply an independently audited deterministic
state engine and reconcile broker-side order effects.  The runtime provides:

* immutable source-epoch and winner-roster identities;
* externally keyed HMAC authentication;
* runtime-owned deterministic bar transitions;
* atomic A/B checkpoints and bar-journal compaction in one SQLite transaction;
* a separate INTENT/RESULT/APPLIED order journal whose APPLIED transition is
  atomic with its durable local-state receipt; and
* fixed row, payload, page, segment, retry, and scan bounds.

The database must live on a local, non-synchronised filesystem.  SQLite uses a
FULL-synchronous rollback journal (not WAL mode) so checkpoint starvation
cannot create an unbounded ``-wal`` file.  A process-lifetime OS lock fences the
single writer.  Any authentication, chain, sequence, replay, binding, or
contract mismatch places the runtime in management-only mode; it never repairs
or reseeds an uncertain tail automatically.
"""

from __future__ import annotations

import builtins
import base64
import hashlib
import hmac
import inspect
import json
import math
import os
import re
import sqlite3
import sys
import time
import dis
import types
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence, TypeVar

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from pm_state_contract import (
    DEFAULT_AUTHORITY,
    DEFAULT_REGISTRY_HASH,
    EvidenceMode,
    StateEvidence,
    StateResolution,
)


RUNTIME_SCHEMA_VERSION = "fxpm.authenticated-state-runtime.v1"
CHECKPOINT_SCHEMA_VERSION = "fxpm.authenticated-state-checkpoint.v1"
BAR_EVENT_SCHEMA_VERSION = "fxpm.authenticated-bar-event.v1"
ORDER_EVENT_SCHEMA_VERSION = "fxpm.authenticated-order-event.v1"
ZERO_HASH = hashlib.sha256(b"").hexdigest()
SQLITE_APPLICATION_ID = 0x4658504D  # ASCII "FXPM"
SQLITE_USER_VERSION = 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:@/+\-]{1,160}$")
_TIMEFRAMES = frozenset({"M5", "M15", "M30", "H1", "H4", "D1"})
_TIMEFRAME_SECONDS = {
    "M5": 5 * 60,
    "M15": 15 * 60,
    "M30": 30 * 60,
    "H1": 60 * 60,
    "H4": 4 * 60 * 60,
    "D1": 24 * 60 * 60,
}
_MT5_EXECUTION_RETCODES = frozenset({10008, 10009, 10010})
_MT5_RETCODE_STATUSES = {
    10008: frozenset({"PLACED"}),
    10009: frozenset({"EXECUTED", "FILLED"}),
    10010: frozenset({"PARTIALLY_FILLED"}),
}
_MT5_NO_EFFECT_STATUSES = frozenset({"REJECTED", "NO_EFFECT", "CANCELLED", "EXPIRED"})
_CLOUD_PARTS = frozenset(
    {"onedrive", "dropbox", "google drive", "googledrive", "icloud", "box"}
)


class StateRuntimeError(RuntimeError):
    """Base class for fail-closed runtime errors."""


class AuthenticationError(StateRuntimeError):
    """An HMAC or authenticated-evidence check failed."""


class IntegrityError(StateRuntimeError):
    """A sequence, hash chain, replay, or SQLite integrity check failed."""


class BindingMismatchError(StateRuntimeError):
    """Stored state belongs to a different immutable identity."""


class CapacityExceededError(StateRuntimeError):
    """A configured finite storage or payload cap would be exceeded."""


class PublicationError(StateRuntimeError):
    """A transactional publication could not be verified."""


class LockTimeoutError(StateRuntimeError):
    """The local writer fence or SQLite write lock was not acquired in time."""


class RecoveryRequiredError(StateRuntimeError):
    """Trading mutation was attempted while management-only."""


class OrderTransitionError(StateRuntimeError):
    """An invalid or unverifiable order transition was requested."""


class StorageLocationError(StateRuntimeError):
    """Operational state was directed to an unsafe filesystem location."""


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _normal_identifier(value: Any, field_name: str, *, upper: bool = False) -> str:
    result = str(value or "").strip()
    if upper:
        result = result.upper()
    if not _IDENTIFIER_RE.fullmatch(result):
        raise ValueError(f"{field_name} is not a canonical identifier")
    return result


def _normal_sha256(value: Any, field_name: str) -> str:
    result = str(value or "")
    if not _SHA256_RE.fullmatch(result):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return result


def _nonnegative_exact_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a nonnegative integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be a nonnegative integer") from exc
    if result != value or result < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer")
    return result


def _positive_exact_int(value: Any, field_name: str) -> int:
    result = _nonnegative_exact_int(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _normal_timestamp(value: Any, field_name: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field_name} is required")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _timestamp_value(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    if depth > 32:
        raise ValueError("canonical JSON nesting exceeds 32 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON forbids NaN and infinity")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("canonical JSON mapping keys must be strings")
            result[key] = _json_safe(item, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_safe(item, depth=depth + 1) for item in value]
    raise ValueError(f"unsupported canonical JSON value: {type(value).__name__}")


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _deep_freeze(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


_ENGINE_METHODS = (
    "initial_state",
    "validate_state_evidence",
    "apply_closed_bar",
    "initial_order_state",
    "apply_order_result",
)
_MAX_ENGINE_DEPENDENCY_DEPTH = 32
_FORBIDDEN_REDUCER_NAMES = frozenset(
    {
        "__import__",
        "compile",
        "delattr",
        "eval",
        "exec",
        "getattr",
        "globals",
        "hasattr",
        "attrgetter",
        "methodcaller",
        "itemgetter",
        "locals",
        "modules",
        "setattr",
        "vars",
        "__builtins__",
        "__class__",
        "__dict__",
        "__file__",
        "__getattribute__",
        "__globals__",
        "__loader__",
        "__spec__",
        "__subclasses__",
    }
)
_FORBIDDEN_REDUCER_OPCODES = frozenset({"IMPORT_NAME", "IMPORT_FROM", "IMPORT_STAR"})


def _assert_static_reducer_function(function: types.FunctionType, *, path: str) -> None:
    """Reject execution surfaces a finite release manifest cannot close.

    Direct calls and direct module attributes are recursively authenticated by
    the dependency surface below.  Runtime reflection, local/dynamic imports,
    ``sys.modules`` lookup, and namespace subscripting can select code which is
    absent from that finite graph, so production reducer admission refuses
    them rather than pretending bytecode introspection is complete.
    """

    def inspect_code(code: types.CodeType, code_path: str) -> None:
        instructions = tuple(dis.get_instructions(code))
        forbidden_opcodes = sorted(
            {
                item.opname
                for item in instructions
                if item.opname in _FORBIDDEN_REDUCER_OPCODES
            }
        )
        forbidden_names = sorted(
            {
                name
                for name in code.co_names
                if name in _FORBIDDEN_REDUCER_NAMES
                or (name.startswith("__") and name.endswith("__"))
            }
        )
        if forbidden_opcodes or forbidden_names:
            details = ",".join((*forbidden_opcodes, *forbidden_names))
            raise ValueError(
                f"state engine dependency {code_path} uses an unauditable "
                f"dynamic surface: {details}"
            )
        for index, constant in enumerate(code.co_consts):
            if isinstance(constant, types.CodeType):
                inspect_code(constant, f"{code_path}.code[{index}]")

    inspect_code(function.__code__, path)


def _code_constant_surface(value: Any) -> Any:
    if isinstance(value, types.CodeType):
        return {"kind": "code", "surface": _code_surface(value)}
    if value is None or value is Ellipsis or isinstance(value, (str, bool, int)):
        return {
            "kind": type(value).__name__,
            "value": str(value) if value is Ellipsis else value,
        }
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("state engine code constants forbid NaN and infinity")
        return {"kind": "float", "value": value}
    if isinstance(value, bytes):
        return {
            "kind": "bytes",
            "length": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if isinstance(value, (tuple, frozenset)):
        items = [_code_constant_surface(item) for item in value]
        if isinstance(value, frozenset):
            items.sort(key=_canonical_bytes)
        return {"kind": type(value).__name__, "items": items}
    raise ValueError(
        "unsupported state engine code constant: "
        f"{type(value).__module__}.{type(value).__qualname__}"
    )


def _code_surface(code: types.CodeType) -> dict[str, Any]:
    """Stable code description unaffected by CPython adaptive quickening."""

    return {
        "co_code_sha256": hashlib.sha256(code.co_code).hexdigest(),
        "constants": [_code_constant_surface(item) for item in code.co_consts],
        "names": list(code.co_names),
        "varnames": list(code.co_varnames),
        "freevars": list(code.co_freevars),
        "cellvars": list(code.co_cellvars),
        "argcount": code.co_argcount,
        "posonlyargcount": code.co_posonlyargcount,
        "kwonlyargcount": code.co_kwonlyargcount,
        "nlocals": code.co_nlocals,
        "stacksize": code.co_stacksize,
        "flags": code.co_flags,
        "exception_table_sha256": hashlib.sha256(code.co_exceptiontable).hexdigest(),
    }


def _module_origin_sha256(module: types.ModuleType) -> Optional[str]:
    origin = getattr(getattr(module, "__spec__", None), "origin", None)
    if not origin or origin in {"built-in", "frozen"}:
        origin = getattr(module, "__file__", None)
    if not origin:
        return None
    path = Path(str(origin))
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _referenced_global_attributes(
    function: types.FunctionType,
) -> dict[str, tuple[tuple[str, ...], ...]]:
    """Return LOAD_GLOBAL -> LOAD_ATTR chains used by one Python function."""

    instructions = tuple(dis.get_instructions(function))
    result: dict[str, set[tuple[str, ...]]] = {}
    for index, instruction in enumerate(instructions):
        if instruction.opname not in {"LOAD_GLOBAL", "LOAD_NAME"} or not isinstance(
            instruction.argval, str
        ):
            continue
        chain: list[str] = []
        cursor = index + 1
        while cursor < len(instructions) and instructions[cursor].opname in {
            "CACHE",
            "LOAD_ATTR",
            "LOAD_METHOD",
        }:
            current = instructions[cursor]
            if current.opname in {"LOAD_ATTR", "LOAD_METHOD"}:
                chain.append(str(current.argval))
            cursor += 1
        if chain:
            result.setdefault(instruction.argval, set()).add(tuple(chain))
    return {name: tuple(sorted(chains)) for name, chains in sorted(result.items())}


def _engine_dependency_surface(
    value: Any,
    *,
    path: str,
    seen: dict[int, str],
    depth: int = 0,
    attribute_chains: Sequence[Sequence[str]] = (),
) -> Any:
    """Canonical, transitive reducer dependency description.

    Reducer bytecode alone is not a contract: defaults, closures, helpers,
    mutable globals, module attributes, class constants, and instance
    configuration can all change behaviour without changing a method's code
    object.  This description binds every executable surface we permit.  An
    unsupported stateful dependency is rejected instead of being represented
    by a process-specific ``repr``.
    """

    if depth > _MAX_ENGINE_DEPENDENCY_DEPTH:
        raise ValueError("state engine dependency graph exceeds 32 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return {"kind": type(value).__name__, "value": value}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("state engine dependencies forbid NaN and infinity")
        return {"kind": "float", "value": value}
    if isinstance(value, bytes):
        return {
            "kind": "bytes",
            "length": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if isinstance(value, Path):
        return {
            "kind": "path",
            "type": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": str(value),
        }

    identity = id(value)
    if identity in seen:
        return {"kind": "cycle", "target": seen[identity]}

    if isinstance(value, Mapping):
        seen[identity] = path
        pairs = [
            {
                "key": _engine_dependency_surface(
                    key,
                    path=f"{path}.key",
                    seen=seen,
                    depth=depth + 1,
                ),
                "value": _engine_dependency_surface(
                    item,
                    path=f"{path}[{key!r}]",
                    seen=seen,
                    depth=depth + 1,
                ),
            }
            for key, item in value.items()
        ]
        pairs.sort(key=_canonical_bytes)
        return {"kind": "mapping", "items": pairs}
    if isinstance(value, (tuple, list)):
        seen[identity] = path
        return {
            "kind": type(value).__name__,
            "items": [
                _engine_dependency_surface(
                    item,
                    path=f"{path}[{index}]",
                    seen=seen,
                    depth=depth + 1,
                )
                for index, item in enumerate(value)
            ],
        }
    if isinstance(value, (set, frozenset)):
        seen[identity] = path
        items = [
            _engine_dependency_surface(
                item,
                path=f"{path}.member",
                seen=seen,
                depth=depth + 1,
            )
            for item in value
        ]
        items.sort(key=_canonical_bytes)
        return {"kind": type(value).__name__, "items": items}

    if isinstance(value, types.ModuleType):
        if type(value) is not types.ModuleType or any(
            name in vars(value) for name in ("__getattr__", "__dir__")
        ):
            raise ValueError(
                f"state engine dependency {path} uses an unauditable dynamic "
                "surface: dynamic module namespace"
            )
        seen[identity] = path
        attributes: dict[str, Any] = {}
        for chain in attribute_chains:
            current: Any = value
            for name in chain:
                if not hasattr(current, name):
                    raise ValueError(
                        f"state engine dependency {path}.{'.'.join(chain)} is absent"
                    )
                current = getattr(current, name)
            label = ".".join(chain)
            attributes[label] = _engine_dependency_surface(
                current,
                path=f"{path}.{label}",
                seen=seen,
                depth=depth + 1,
            )
        version = getattr(value, "__version__", None)
        return {
            "kind": "module",
            "name": value.__name__,
            "version": version if isinstance(version, (str, int, float)) else None,
            "origin_sha256": _module_origin_sha256(value),
            "referenced_attributes": attributes,
        }

    if isinstance(value, types.MethodType):
        return _engine_dependency_surface(
            value.__func__,
            path=path,
            seen=seen,
            depth=depth + 1,
        )
    if isinstance(value, (types.BuiltinFunctionType, types.BuiltinMethodType)):
        forbidden_builtin = tuple(
            candidate
            for name in _FORBIDDEN_REDUCER_NAMES
            if hasattr(builtins, name)
            for candidate in (getattr(builtins, name),)
            if isinstance(
                candidate,
                (types.BuiltinFunctionType, types.BuiltinMethodType),
            )
        )
        if any(value is candidate for candidate in forbidden_builtin):
            raise ValueError(
                f"state engine dependency {path} uses an unauditable dynamic "
                f"surface via builtin alias: {value.__name__}"
            )
        # ``builtin_function_or_method`` values may secretly retain a mutable
        # receiver (for example ``some_dict.get``).  Module/qualname alone is
        # then not an executable identity: mutating ``some_dict`` changes the
        # reducer while leaving both strings unchanged.  Permit only receiver-
        # free builtins and exact module-owned C functions whose module artifact
        # is itself bound below.  Mutable/object receivers are rejected rather
        # than represented by a lossy ``repr``.
        receiver = getattr(value, "__self__", None)
        receiver_surface: Optional[dict[str, Any]] = None
        if receiver is not None:
            if type(receiver) is not types.ModuleType:
                raise ValueError(
                    f"state engine dependency {path} uses an unauditable dynamic "
                    "surface: bound builtin receiver"
                )
            receiver_surface = {
                "module": receiver.__name__,
                "origin_sha256": _module_origin_sha256(receiver),
            }
        return {
            "kind": "builtin-callable",
            "module": getattr(value, "__module__", None),
            "qualname": getattr(
                value, "__qualname__", getattr(value, "__name__", None)
            ),
            "receiver": receiver_surface,
        }
    if isinstance(value, types.FunctionType):
        _assert_static_reducer_function(value, path=path)
        seen[identity] = f"function:{value.__module__}.{value.__qualname__}"
        global_attributes = _referenced_global_attributes(value)
        globals_surface: dict[str, Any] = {}
        for name in sorted(set(value.__code__.co_names)):
            if name in value.__globals__:
                dependency = value.__globals__[name]
            elif hasattr(builtins, name):
                dependency = getattr(builtins, name)
            else:
                continue
            globals_surface[name] = _engine_dependency_surface(
                dependency,
                path=f"{path}.global:{name}",
                seen=seen,
                depth=depth + 1,
                attribute_chains=global_attributes.get(name, ()),
            )
        closure_surface: dict[str, Any] = {}
        if value.__closure__:
            for name, cell in zip(value.__code__.co_freevars, value.__closure__):
                try:
                    cell_value = cell.cell_contents
                except ValueError as exc:
                    raise ValueError(
                        f"state engine closure cell {path}.{name} is empty"
                    ) from exc
                closure_surface[name] = _engine_dependency_surface(
                    cell_value,
                    path=f"{path}.closure:{name}",
                    seen=seen,
                    depth=depth + 1,
                )
        return {
            "kind": "python-function",
            "module": value.__module__,
            "qualname": value.__qualname__,
            "code_surface": _code_surface(value.__code__),
            "defaults": _engine_dependency_surface(
                value.__defaults__,
                path=f"{path}.defaults",
                seen=seen,
                depth=depth + 1,
            ),
            "kwdefaults": _engine_dependency_surface(
                value.__kwdefaults__,
                path=f"{path}.kwdefaults",
                seen=seen,
                depth=depth + 1,
            ),
            "closure": closure_surface,
            "globals": globals_surface,
        }

    if isinstance(value, staticmethod):
        return _engine_dependency_surface(
            value.__func__, path=path, seen=seen, depth=depth + 1
        )
    if isinstance(value, classmethod):
        return _engine_dependency_surface(
            value.__func__, path=path, seen=seen, depth=depth + 1
        )
    if isinstance(value, property):
        seen[identity] = path
        return {
            "kind": "property",
            "get": _engine_dependency_surface(
                value.fget, path=f"{path}.fget", seen=seen, depth=depth + 1
            ),
            "set": _engine_dependency_surface(
                value.fset, path=f"{path}.fset", seen=seen, depth=depth + 1
            ),
            "delete": _engine_dependency_surface(
                value.fdel, path=f"{path}.fdel", seen=seen, depth=depth + 1
            ),
        }
    if isinstance(value, type):
        if type(value) is not type:
            raise ValueError(
                f"state engine dependency {path} uses an unauditable dynamic "
                "surface: custom metaclass namespace"
            )
        if value.__module__ == "builtins":
            return {"kind": "builtin-type", "qualname": value.__qualname__}
        seen[identity] = f"class:{value.__module__}.{value.__qualname__}"
        namespace: dict[str, Any] = {}
        for name, member in sorted(vars(value).items()):
            if name.startswith("__") and name.endswith("__"):
                continue
            namespace[name] = _engine_dependency_surface(
                member,
                path=f"{path}.{name}",
                seen=seen,
                depth=depth + 1,
            )
        return {
            "kind": "python-class",
            "module": value.__module__,
            "qualname": value.__qualname__,
            "namespace": namespace,
        }

    raise ValueError(
        "unsupported state engine dependency at "
        f"{path}: {type(value).__module__}.{type(value).__qualname__}"
    )


def _engine_implementation_sha256(engine: DeterministicStateEngine) -> str:
    return _canonical_sha256(_engine_release_manifest(engine))


def _engine_type_without_dynamic_dispatch(engine: DeterministicStateEngine) -> type:
    engine_type = type(engine)
    if type(engine_type) is not type:
        raise ValueError("state engine custom metaclass dispatch is forbidden")
    forbidden_dispatch = {
        "__getattribute__",
        "__getattr__",
        "__setattr__",
        "__delattr__",
    }
    for engine_class in engine_type.__mro__:
        if engine_class is object:
            continue
        defined = forbidden_dispatch & set(vars(engine_class))
        if defined:
            raise ValueError(
                "state engine custom instance dispatch is forbidden: "
                + ",".join(sorted(defined))
            )
    try:
        instance_namespace = object.__getattribute__(engine, "__dict__")
    except AttributeError:
        instance_namespace = {}
    shadowed = set(instance_namespace) & ({"identity"} | set(_ENGINE_METHODS))
    if shadowed:
        raise ValueError(
            "state engine instance cannot shadow admitted reducer functions: "
            + ",".join(sorted(shadowed))
        )
    return engine_type


def _module_file_identity(function: types.FunctionType) -> dict[str, Any]:
    module = sys.modules.get(function.__module__)
    if not isinstance(module, types.ModuleType):
        raise ValueError(
            f"state engine function module is not loaded: {function.__module__}"
        )
    origin_sha = _module_origin_sha256(module)
    if origin_sha is None:
        raise ValueError(
            f"state engine function module has no immutable file: {function.__module__}"
        )
    return {
        "module": function.__module__,
        "qualname": function.__qualname__,
        "module_file_sha256": origin_sha,
    }


def _admitted_engine_functions(
    engine: DeterministicStateEngine,
) -> dict[str, types.FunctionType]:
    engine_type = _engine_type_without_dynamic_dispatch(engine)
    functions: dict[str, types.FunctionType] = {}
    for name in _ENGINE_METHODS:
        descriptor = inspect.getattr_static(engine_type, name, None)
        if not isinstance(descriptor, types.FunctionType):
            raise ValueError(f"state engine {name} must be one plain Python function")
        _assert_static_reducer_function(descriptor, path=f"engine.method:{name}")
        functions[name] = descriptor
    return functions


def _engine_identity_function(
    engine: DeterministicStateEngine,
) -> types.FunctionType:
    engine_type = _engine_type_without_dynamic_dispatch(engine)
    descriptor = inspect.getattr_static(engine_type, "identity", None)
    if type(descriptor) is not property or not isinstance(
        descriptor.fget, types.FunctionType
    ):
        raise ValueError("state engine identity must be one plain read-only property")
    if descriptor.fset is not None or descriptor.fdel is not None:
        raise ValueError("state engine identity property must be read-only")
    _assert_static_reducer_function(descriptor.fget, path="engine.identity")
    return descriptor.fget


def _engine_identity(engine: DeterministicStateEngine) -> dict[str, Any]:
    identity_function = _engine_identity_function(engine)
    identity = _json_safe(dict(identity_function(engine)))
    if not isinstance(identity, dict) or not identity:
        raise ValueError("state engine identity must be a non-empty mapping")
    return identity


def _engine_release_manifest(engine: DeterministicStateEngine) -> dict[str, Any]:
    engine_type = _engine_type_without_dynamic_dispatch(engine)
    functions = _admitted_engine_functions(engine)
    identity_function = _engine_identity_function(engine)
    admitted: dict[str, Any] = {}
    for name, function in {"identity": identity_function, **functions}.items():
        admitted[name] = {
            **_module_file_identity(function),
            "dependency_surface_sha256": _canonical_sha256(
                _engine_dependency_surface(
                    function,
                    path=f"engine.release:{name}",
                    seen={},
                )
            ),
        }
    return {
        "schema_version": "fxpm.reducer-function-allowlist.v1",
        "engine_module": engine_type.__module__,
        "engine_qualname": engine_type.__qualname__,
        "python_cache_tag": sys.implementation.cache_tag,
        "admitted_functions": admitted,
    }


def _engine_instance_configuration_sha256(engine: DeterministicStateEngine) -> str:
    configuration: dict[str, Any] = {}
    engine_type = _engine_type_without_dynamic_dispatch(engine)
    try:
        configuration.update(object.__getattribute__(engine, "__dict__"))
    except AttributeError:
        pass
    for engine_class in engine_type.__mro__:
        slots = engine_class.__dict__.get("__slots__", ())
        if isinstance(slots, str):
            slots = (slots,)
        for name in slots:
            if name in {"__dict__", "__weakref__"} or name in configuration:
                continue
            try:
                configuration[name] = object.__getattribute__(engine, name)
            except AttributeError:
                continue
    canonical = _json_safe(configuration)
    return _canonical_sha256(
        {
            "schema_version": "fxpm.reducer-instance-configuration.v1",
            "configuration": canonical,
        }
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _bounded_payload(value: Any, cap: int, field_name: str) -> tuple[bytes, str]:
    payload = _canonical_bytes(value)
    if len(payload) > cap:
        raise CapacityExceededError(
            f"{field_name} is {len(payload)} bytes; configured cap is {cap}"
        )
    return payload, hashlib.sha256(payload).hexdigest()


def _decode_payload(payload: bytes | str, cap: int, field_name: str) -> Any:
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    if len(raw) > cap:
        raise CapacityExceededError(f"stored {field_name} exceeds configured cap")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"stored {field_name} is not canonical JSON") from exc
    if _canonical_bytes(value) != raw:
        raise IntegrityError(f"stored {field_name} is not canonically encoded")
    return value


def _fault_point(stage: str) -> None:
    """No-op seam monkeypatched by fault-injection tests."""

    del stage


@dataclass(frozen=True)
class SourceEpoch:
    broker: str
    server: str
    account_id: str
    data_source: str
    source_instance_id: str
    session_calendar_id: str
    source_artifact_sha256: str
    epoch_id: str
    started_at: str
    schema_version: str = "fxpm.source-epoch.v2"

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker", _normal_identifier(self.broker, "broker"))
        object.__setattr__(self, "server", _normal_identifier(self.server, "server"))
        object.__setattr__(
            self, "account_id", _normal_identifier(self.account_id, "account_id")
        )
        object.__setattr__(
            self, "data_source", _normal_identifier(self.data_source, "data_source")
        )
        object.__setattr__(
            self,
            "source_instance_id",
            _normal_identifier(self.source_instance_id, "source_instance_id"),
        )
        object.__setattr__(
            self,
            "session_calendar_id",
            _normal_identifier(self.session_calendar_id, "session_calendar_id"),
        )
        object.__setattr__(
            self,
            "source_artifact_sha256",
            _normal_sha256(self.source_artifact_sha256, "source_artifact_sha256"),
        )
        object.__setattr__(
            self, "epoch_id", _normal_identifier(self.epoch_id, "epoch_id")
        )
        object.__setattr__(
            self, "started_at", _normal_timestamp(self.started_at, "started_at")
        )
        if self.schema_version != "fxpm.source-epoch.v2":
            raise ValueError("unsupported source epoch schema")

    def canonical(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "server": self.server,
            "account_id": self.account_id,
            "data_source": self.data_source,
            "source_instance_id": self.source_instance_id,
            "session_calendar_id": self.session_calendar_id,
            "source_artifact_sha256": self.source_artifact_sha256,
            "epoch_id": self.epoch_id,
            "started_at": self.started_at,
            "schema_version": self.schema_version,
        }

    @property
    def identity_sha256(self) -> str:
        return _canonical_sha256(self.canonical())


@dataclass(frozen=True)
class WinnerRecord:
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    strategy_name: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    requested_features: tuple[str, ...] = ()
    include_regime_controller: bool = True
    initial_error_bound: Optional[float] = None
    tolerance: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "cell_id", _normal_identifier(self.cell_id, "cell_id"))
        object.__setattr__(
            self, "symbol", _normal_identifier(self.symbol, "symbol", upper=True)
        )
        timeframe = _normal_identifier(self.timeframe, "timeframe", upper=True)
        if timeframe not in _TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {timeframe}")
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(
            self, "regime", _normal_identifier(self.regime, "regime", upper=True)
        )
        object.__setattr__(
            self,
            "strategy_name",
            _normal_identifier(self.strategy_name, "strategy_name"),
        )
        params = json.loads(_canonical_bytes(dict(self.parameters)).decode("utf-8"))
        object.__setattr__(self, "parameters", _deep_freeze(params))
        features = tuple(
            sorted(
                dict.fromkeys(
                    _normal_identifier(item, "requested_feature")
                    for item in self.requested_features
                )
            )
        )
        object.__setattr__(self, "requested_features", features)
        object.__setattr__(
            self, "include_regime_controller", bool(self.include_regime_controller)
        )
        for name in ("initial_error_bound", "tolerance"):
            raw = getattr(self, name)
            if raw is not None:
                number = float(raw)
                if not math.isfinite(number) or number <= 0.0:
                    raise ValueError(f"{name} must be finite and positive")
                object.__setattr__(self, name, number)

    def canonical(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "regime": self.regime,
            "strategy_name": self.strategy_name,
            "parameters": json.loads(_canonical_bytes(self.parameters).decode("utf-8")),
            "requested_features": list(self.requested_features),
            "include_regime_controller": self.include_regime_controller,
            "initial_error_bound": self.initial_error_bound,
            "tolerance": self.tolerance,
        }


@dataclass(frozen=True)
class WinnerRoster:
    roster_id: str
    created_at: str
    registry_hash: str
    winners: tuple[WinnerRecord, ...]
    schema_version: str = "fxpm.winner-roster.v1"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "roster_id", _normal_identifier(self.roster_id, "roster_id")
        )
        object.__setattr__(
            self, "created_at", _normal_timestamp(self.created_at, "created_at")
        )
        registry_hash = _normal_sha256(self.registry_hash, "registry_hash")
        if registry_hash != DEFAULT_REGISTRY_HASH:
            raise BindingMismatchError(
                "winner roster registry does not match current state authority"
            )
        object.__setattr__(self, "registry_hash", registry_hash)
        supplied_winners = tuple(self.winners)
        if not supplied_winners:
            raise ValueError("winner roster must contain at least one winner")
        if not all(isinstance(winner, WinnerRecord) for winner in supplied_winners):
            raise TypeError("winner roster entries must be WinnerRecord instances")
        winners = tuple(sorted(supplied_winners, key=lambda item: item.cell_id))
        ids = [winner.cell_id for winner in winners]
        if len(ids) != len(set(ids)):
            raise ValueError("winner roster contains duplicate cell_id values")
        object.__setattr__(self, "winners", winners)
        if self.schema_version != "fxpm.winner-roster.v1":
            raise ValueError("unsupported winner roster schema")

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "roster_id": self.roster_id,
            "created_at": self.created_at,
            "registry_hash": self.registry_hash,
            "winners": [winner.canonical() for winner in self.winners],
        }

    @property
    def identity_sha256(self) -> str:
        return _canonical_sha256(self.canonical())


class HMACAuthenticator:
    """External-key authenticator.  There is intentionally no fallback key."""

    def __init__(self, key: bytes):
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("an external HMAC key of at least 32 bytes is required")
        self._key = bytes(key)

    def sign(self, domain: str, body: Any) -> str:
        material = domain.encode("ascii") + b"\x00" + _canonical_bytes(body)
        return hmac.new(self._key, material, hashlib.sha256).hexdigest()

    def verify(self, domain: str, body: Any, supplied: Any) -> None:
        expected = self.sign(domain, body)
        if not hmac.compare_digest(expected, str(supplied or "")):
            raise AuthenticationError(f"HMAC verification failed for {domain}")


def _verify_external_ed25519(
    trusted_public_keys: Mapping[str, bytes],
    *,
    authority_key_id: str,
    domain: str,
    body: Mapping[str, Any],
    signature_base64: str,
) -> None:
    """Verify an externally issued artifact without any local signing oracle."""

    key_id = _normal_identifier(authority_key_id, "authority_key_id")
    raw_key = trusted_public_keys.get(key_id)
    if type(raw_key) is not bytes or len(raw_key) != 32:
        raise AuthenticationError(f"trusted public key is absent for {domain}")
    try:
        signature = base64.b64decode(str(signature_base64), validate=True)
        if len(signature) != 64:
            raise ValueError("Ed25519 signatures are exactly 64 bytes")
        material = domain.encode("ascii") + b"\x00" + _canonical_bytes(body)
        Ed25519PublicKey.from_public_bytes(raw_key).verify(signature, material)
    except (InvalidSignature, TypeError, ValueError) as exc:
        raise AuthenticationError(
            f"external Ed25519 verification failed for {domain}"
        ) from exc


def _owned_public_key_registry(
    value: Mapping[str, bytes], *, name: str
) -> Mapping[str, bytes]:
    if not isinstance(value, Mapping) or not value:
        raise AuthenticationError(f"{name} public-key registry is required")
    owned: dict[str, bytes] = {}
    for raw_key_id, raw_key in value.items():
        key_id = _normal_identifier(raw_key_id, f"{name} authority_key_id")
        if type(raw_key) is not bytes or len(raw_key) != 32:
            raise AuthenticationError(f"{name} public key must be exactly 32 bytes")
        if key_id in owned:
            raise AuthenticationError(f"duplicate {name} authority_key_id")
        owned[key_id] = bytes(raw_key)
    return MappingProxyType(owned)


@dataclass(frozen=True)
class AuthenticatedCadenceReceipt:
    """Separately keyed V5 acquisition/activation evidence for one closed bar."""

    receipt_id: str
    issued_at: str
    acquisition_schema: str
    activation_schema: str
    source_epoch_sha256: str
    source_artifact_sha256: str
    broker: str
    server: str
    account_id: str
    data_source: str
    source_instance_id: str
    session_calendar_id: str
    symbol: str
    timeframe: str
    source_sequence: int
    predecessor_source_sequence: int
    predecessor_close_time: Optional[str]
    predecessor_phase_seconds: Optional[int]
    open_time: str
    close_time: str
    observed_phase_seconds: int
    phase_transition: str
    boundary_kind: str
    duration_kind: str
    source_session_id: str
    source_connection_id: str
    authority_key_id: str
    signature_base64: str
    schema_version: str = "fxpm.authenticated-cadence-receipt.v2"

    def __post_init__(self) -> None:
        for name in (
            "authority_key_id",
            "receipt_id",
            "broker",
            "server",
            "account_id",
            "data_source",
            "source_instance_id",
            "session_calendar_id",
            "source_session_id",
            "source_connection_id",
        ):
            object.__setattr__(
                self, name, _normal_identifier(getattr(self, name), name)
            )
        object.__setattr__(
            self, "symbol", _normal_identifier(self.symbol, "symbol", upper=True)
        )
        timeframe = _normal_identifier(self.timeframe, "timeframe", upper=True)
        if timeframe not in _TIMEFRAMES:
            raise ValueError("cadence receipt timeframe is unsupported")
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(
            self, "issued_at", _normal_timestamp(self.issued_at, "issued_at")
        )
        for name in ("source_epoch_sha256", "source_artifact_sha256"):
            object.__setattr__(self, name, _normal_sha256(getattr(self, name), name))
        source_sequence = _positive_exact_int(self.source_sequence, "source_sequence")
        predecessor_sequence = _nonnegative_exact_int(
            self.predecessor_source_sequence, "predecessor_source_sequence"
        )
        object.__setattr__(self, "source_sequence", source_sequence)
        object.__setattr__(self, "predecessor_source_sequence", predecessor_sequence)
        predecessor_close = (
            None
            if self.predecessor_close_time is None
            else _normal_timestamp(
                self.predecessor_close_time, "predecessor_close_time"
            )
        )
        object.__setattr__(self, "predecessor_close_time", predecessor_close)
        predecessor_phase = (
            None
            if self.predecessor_phase_seconds is None
            else _nonnegative_exact_int(
                self.predecessor_phase_seconds, "predecessor_phase_seconds"
            )
        )
        object.__setattr__(self, "predecessor_phase_seconds", predecessor_phase)
        object.__setattr__(
            self, "open_time", _normal_timestamp(self.open_time, "open_time")
        )
        object.__setattr__(
            self, "close_time", _normal_timestamp(self.close_time, "close_time")
        )
        object.__setattr__(
            self,
            "observed_phase_seconds",
            _nonnegative_exact_int(
                self.observed_phase_seconds, "observed_phase_seconds"
            ),
        )
        for name, supported in (
            (
                "phase_transition",
                {"GENESIS", "SAME", "FORWARD_ONE_HOUR", "BACKWARD_ONE_HOUR"},
            ),
            ("boundary_kind", {"GENESIS", "CONTIGUOUS", "SESSION_GAP"}),
            ("duration_kind", {"NOMINAL", "DST_SHORT", "DST_LONG"}),
        ):
            normalized = _normal_identifier(getattr(self, name), name, upper=True)
            if normalized not in supported:
                raise ValueError(f"unsupported cadence {name}")
            object.__setattr__(self, name, normalized)
        if self.acquisition_schema != "fxpm.native-acquisition.v5":
            raise ValueError("cadence receipt is not from V5 acquisition")
        if self.activation_schema != "fxpm.native-activation.v5":
            raise ValueError("cadence receipt is not from V5 activation")
        if self.schema_version != "fxpm.authenticated-cadence-receipt.v2":
            raise ValueError("unsupported cadence receipt schema")
        if not str(self.signature_base64):
            raise AuthenticationError("cadence receipt lacks an external signature")

    def body(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name not in {"signature_base64"}
        }

    def canonical(self) -> dict[str, Any]:
        return {**self.body(), "signature_base64": self.signature_base64}

    @property
    def identity_sha256(self) -> str:
        return _canonical_sha256(self.canonical())

    def verify(
        self,
        trusted_public_keys: Mapping[str, bytes],
        source: SourceEpoch,
        symbol: str,
        timeframe: str,
    ) -> None:
        _verify_external_ed25519(
            trusted_public_keys,
            authority_key_id=self.authority_key_id,
            domain="fxpm-v5-acquisition-cadence",
            body=self.body(),
            signature_base64=self.signature_base64,
        )
        expected_source = {
            "source_epoch_sha256": source.identity_sha256,
            "source_artifact_sha256": source.source_artifact_sha256,
            "broker": source.broker,
            "server": source.server,
            "account_id": source.account_id,
            "data_source": source.data_source,
            "source_instance_id": source.source_instance_id,
            "session_calendar_id": source.session_calendar_id,
            "symbol": symbol,
            "timeframe": timeframe,
        }
        for name, expected in expected_source.items():
            if getattr(self, name) != expected:
                raise BindingMismatchError(f"cadence receipt mismatch: {name}")
        open_value = _timestamp_value(self.open_time)
        close_value = _timestamp_value(self.close_time)
        if open_value >= close_value:
            raise AuthenticationError("cadence receipt has a non-positive bar")
        duration = int((close_value - open_value).total_seconds())
        nominal = _TIMEFRAME_SECONDS[self.timeframe]
        expected_duration = {
            "NOMINAL": nominal,
            "DST_SHORT": nominal - 3600,
            "DST_LONG": nominal + 3600,
        }[self.duration_kind]
        if self.timeframe not in {"H4", "D1"} and self.duration_kind != "NOMINAL":
            raise AuthenticationError("DST cadence is only admissible for H4/D1")
        if duration != expected_duration or expected_duration < 300:
            raise AuthenticationError(
                "cadence duration violates the hard timeframe contract"
            )
        observed_phase = int(open_value.timestamp()) % nominal
        if observed_phase != self.observed_phase_seconds:
            raise AuthenticationError("cadence observed phase is false")
        if self.timeframe not in {"H4", "D1"} and observed_phase != 0:
            raise AuthenticationError(
                "cadence grid phase violates the hard timeframe contract"
            )
        if self.source_sequence == 1:
            if (
                self.predecessor_source_sequence != 0
                or self.predecessor_close_time is not None
                or self.predecessor_phase_seconds is not None
                or self.phase_transition != "GENESIS"
                or self.boundary_kind != "GENESIS"
            ):
                raise AuthenticationError("cadence genesis predecessor is invalid")
            return
        if self.predecessor_source_sequence != self.source_sequence - 1:
            raise AuthenticationError("cadence successor sequence is not exact")
        if (
            self.predecessor_close_time is None
            or self.predecessor_phase_seconds is None
        ):
            raise AuthenticationError("cadence successor lacks predecessor evidence")
        predecessor_close = _timestamp_value(self.predecessor_close_time)
        if open_value < predecessor_close:
            raise AuthenticationError("cadence successor overlaps its predecessor")
        expected_boundary = (
            "CONTIGUOUS" if open_value == predecessor_close else "SESSION_GAP"
        )
        if self.boundary_kind != expected_boundary:
            raise AuthenticationError("cadence boundary classification is false")
        phase_delta = (
            self.observed_phase_seconds - self.predecessor_phase_seconds
        ) % nominal
        expected_phase_transition = (
            "SAME"
            if phase_delta == 0
            else "FORWARD_ONE_HOUR"
            if phase_delta == 3600
            else "BACKWARD_ONE_HOUR"
            if phase_delta == (nominal - 3600) % nominal
            else "INVALID"
        )
        if self.phase_transition != expected_phase_transition:
            raise AuthenticationError("cadence phase transition is false")
        if self.phase_transition != "SAME" and self.timeframe not in {"H4", "D1"}:
            raise AuthenticationError(
                "phase shifts are only admissible for H4/D1 calendar bars"
            )


@dataclass(frozen=True)
class AuthenticatedReducerRelease:
    """Out-of-band signed admission contract for one exact reducer release.

    The deployment/release step issues this artifact before a runtime is
    opened.  Runtime introspection remains a useful continuous tamper alarm,
    but it is deliberately not allowed to authenticate its own code.
    """

    release_id: str
    issued_at: str
    python_cache_tag: str
    engine_module: str
    engine_qualname: str
    function_allowlist: Mapping[str, Any]
    declared_identity_sha256: str
    implementation_sha256: str
    instance_configuration_sha256: str
    state_registry_hash: str
    authority_key_id: str
    signature_base64: str
    schema_version: str = "fxpm.authenticated-reducer-release.v3"

    def __post_init__(self) -> None:
        _normal_identifier(self.release_id, "reducer release_id")
        _normal_identifier(self.authority_key_id, "reducer authority_key_id")
        object.__setattr__(
            self, "issued_at", _normal_timestamp(self.issued_at, "reducer issued_at")
        )
        _normal_identifier(self.python_cache_tag, "python_cache_tag")
        _normal_identifier(self.engine_module, "engine_module")
        _normal_identifier(self.engine_qualname, "engine_qualname")
        allowlist = _json_safe(dict(self.function_allowlist))
        if (
            not isinstance(allowlist, dict)
            or allowlist.get("schema_version") != "fxpm.reducer-function-allowlist.v1"
        ):
            raise ValueError("reducer release function allowlist is invalid")
        object.__setattr__(self, "function_allowlist", _deep_freeze(allowlist))
        for name in (
            "declared_identity_sha256",
            "implementation_sha256",
            "instance_configuration_sha256",
            "state_registry_hash",
        ):
            _normal_sha256(getattr(self, name), name)
        if self.state_registry_hash != DEFAULT_REGISTRY_HASH:
            raise BindingMismatchError("reducer release registry hash mismatch")
        if self.schema_version != "fxpm.authenticated-reducer-release.v3":
            raise ValueError("unsupported authenticated reducer release schema")
        if not str(self.signature_base64):
            raise AuthenticationError(
                "authenticated reducer release lacks an external signature"
            )

    def body(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "release_id": self.release_id,
            "issued_at": self.issued_at,
            "python_cache_tag": self.python_cache_tag,
            "engine_module": self.engine_module,
            "engine_qualname": self.engine_qualname,
            "function_allowlist": self.function_allowlist,
            "declared_identity_sha256": self.declared_identity_sha256,
            "implementation_sha256": self.implementation_sha256,
            "instance_configuration_sha256": self.instance_configuration_sha256,
            "state_registry_hash": self.state_registry_hash,
            "authority_key_id": self.authority_key_id,
        }

    def canonical(self) -> dict[str, Any]:
        return {**self.body(), "signature_base64": self.signature_base64}

    @property
    def identity_sha256(self) -> str:
        return _canonical_sha256(self.canonical())

    def verify(
        self,
        trusted_public_keys: Mapping[str, bytes],
        engine: DeterministicStateEngine,
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        _verify_external_ed25519(
            trusted_public_keys,
            authority_key_id=self.authority_key_id,
            domain="fxpm-reducer-release",
            body=self.body(),
            signature_base64=self.signature_base64,
        )
        identity = _engine_identity(engine)
        manifest = _engine_release_manifest(engine)
        configuration_sha = _engine_instance_configuration_sha256(engine)
        observed = {
            "python_cache_tag": str(sys.implementation.cache_tag),
            "engine_module": type(engine).__module__,
            "engine_qualname": type(engine).__qualname__,
            "function_allowlist": manifest,
            "declared_identity_sha256": _canonical_sha256(identity),
            "implementation_sha256": _canonical_sha256(manifest),
            "instance_configuration_sha256": configuration_sha,
            "state_registry_hash": DEFAULT_REGISTRY_HASH,
        }
        for name, expected in observed.items():
            if getattr(self, name) != expected:
                detail = (
                    "state engine instance configuration changed"
                    if name == "instance_configuration_sha256"
                    else f"authenticated reducer release mismatch: {name}"
                )
                raise BindingMismatchError(detail)
        return identity, manifest, configuration_sha


@dataclass(frozen=True)
class AuthenticatedStateEvidence:
    node: str
    symbol: str
    timeframe: str
    mode: str
    identity_sha256: str
    registry_hash: str
    state_schema: str
    transition_schema: str
    payload_sha256: str
    source_epoch_sha256: str
    roster_sha256: str
    payload: Any
    mac: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "symbol",
            _normal_identifier(self.symbol, "evidence symbol", upper=True),
        )
        timeframe = _normal_identifier(self.timeframe, "evidence timeframe", upper=True)
        if timeframe not in _TIMEFRAMES:
            raise ValueError("authenticated evidence timeframe is unsupported")
        object.__setattr__(self, "timeframe", timeframe)
        payload = json.loads(_canonical_bytes(self.payload).decode("utf-8"))
        if _canonical_sha256(payload) != self.payload_sha256:
            raise AuthenticationError("state evidence payload digest mismatch")
        object.__setattr__(self, "payload", _deep_freeze(payload))

    def body(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "mode": self.mode,
            "identity_sha256": self.identity_sha256,
            "registry_hash": self.registry_hash,
            "state_schema": self.state_schema,
            "transition_schema": self.transition_schema,
            "payload_sha256": self.payload_sha256,
            "source_epoch_sha256": self.source_epoch_sha256,
            "roster_sha256": self.roster_sha256,
            "payload": self.payload,
        }

    def canonical(self) -> dict[str, Any]:
        return {**self.body(), "mac": self.mac}

    @classmethod
    def issue_for_default_contract(
        cls,
        key: bytes,
        source: SourceEpoch,
        roster: WinnerRoster,
        node: str,
        payload: Any,
        *,
        symbol: str,
        timeframe: str,
        mode: EvidenceMode | str = EvidenceMode.AUTHENTICATED_REPLAY,
    ) -> "AuthenticatedStateEvidence":
        contract = DEFAULT_AUTHORITY._contracts.get(str(node))  # noqa: SLF001
        if (
            contract is None
            or not contract.checkpoint_schema
            or not contract.transition_schema
        ):
            raise ValueError(f"{node!r} is not a checkpointable default contract")
        payload_sha = _canonical_sha256(payload)
        canonical_symbol = _normal_identifier(symbol, "evidence symbol", upper=True)
        canonical_timeframe = _normal_identifier(
            timeframe, "evidence timeframe", upper=True
        )
        if canonical_timeframe not in _TIMEFRAMES:
            raise ValueError("authenticated evidence timeframe is unsupported")
        evidence_mode = EvidenceMode(mode).value
        identity = _canonical_sha256(
            {
                "node": node,
                "symbol": canonical_symbol,
                "timeframe": canonical_timeframe,
                "mode": evidence_mode,
                "payload_sha256": payload_sha,
                "source_epoch_sha256": source.identity_sha256,
                "roster_sha256": roster.identity_sha256,
            }
        )
        body = {
            "node": str(node),
            "symbol": canonical_symbol,
            "timeframe": canonical_timeframe,
            "mode": evidence_mode,
            "identity_sha256": identity,
            "registry_hash": DEFAULT_REGISTRY_HASH,
            "state_schema": contract.checkpoint_schema,
            "transition_schema": contract.transition_schema,
            "payload_sha256": payload_sha,
            "source_epoch_sha256": source.identity_sha256,
            "roster_sha256": roster.identity_sha256,
            "payload": _json_safe(payload),
        }
        mac = HMACAuthenticator(key).sign("state-evidence", body)
        return cls(**body, mac=mac)

    def verify(
        self,
        authenticator: HMACAuthenticator,
        source: SourceEpoch,
        roster: WinnerRoster,
        symbol: str,
        timeframe: str,
    ) -> StateEvidence:
        authenticator.verify("state-evidence", self.body(), self.mac)
        if self.source_epoch_sha256 != source.identity_sha256:
            raise BindingMismatchError("state evidence source epoch mismatch")
        if self.roster_sha256 != roster.identity_sha256:
            raise BindingMismatchError("state evidence winner roster mismatch")
        if self.symbol != symbol or self.timeframe != timeframe:
            raise BindingMismatchError("state evidence stream scope mismatch")
        if self.registry_hash != DEFAULT_REGISTRY_HASH:
            raise BindingMismatchError("state evidence registry mismatch")
        for name in ("identity_sha256", "payload_sha256"):
            _normal_sha256(getattr(self, name), name)
        expected_identity = _canonical_sha256(
            {
                "node": self.node,
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "mode": EvidenceMode(self.mode).value,
                "payload_sha256": self.payload_sha256,
                "source_epoch_sha256": self.source_epoch_sha256,
                "roster_sha256": self.roster_sha256,
            }
        )
        if self.identity_sha256 != expected_identity:
            raise AuthenticationError("state evidence identity digest mismatch")
        return StateEvidence(
            node=self.node,
            mode=EvidenceMode(self.mode),
            identity_sha256=self.identity_sha256,
            registry_hash=self.registry_hash,
            state_schema=self.state_schema,
            transition_schema=self.transition_schema,
        )


class DeterministicStateEngine(Protocol):
    """Injected state transition authority owned and replayed by the runtime."""

    @property
    def identity(self) -> Mapping[str, Any]: ...

    def initial_state(
        self,
        source: SourceEpoch,
        roster: WinnerRoster,
        symbol: str,
        timeframe: str,
        state_evidence: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    def validate_state_evidence(
        self, evidence: AuthenticatedStateEvidence, payload: Any
    ) -> None: ...

    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    def initial_order_state(
        self,
        source: SourceEpoch,
        roster: WinnerRoster,
        symbol: str,
        timeframe: str,
    ) -> Mapping[str, Any]: ...

    def apply_order_result(
        self,
        state: Mapping[str, Any],
        action: str,
        request: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class StorageReservation:
    authority_identity_sha256: str
    reservation_id: str
    operational_domain_root: str
    stream_subpath: str
    stream_identity_sha256: str
    reserved_peak_bytes: int
    institution_cap_bytes: int
    admitted_streams_cap: int
    receipt_mac: str

    def __post_init__(self) -> None:
        _normal_sha256(self.authority_identity_sha256, "authority_identity_sha256")
        _normal_sha256(self.stream_identity_sha256, "stream_identity_sha256")
        _normal_identifier(self.reservation_id, "reservation_id")
        for name in (
            "reserved_peak_bytes",
            "institution_cap_bytes",
            "admitted_streams_cap",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or int(value) != value or int(value) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.reserved_peak_bytes > self.institution_cap_bytes:
            raise ValueError("reservation exceeds the institution storage cap")
        subpath = Path(self.stream_subpath)
        if subpath.is_absolute() or not self.stream_subpath or ".." in subpath.parts:
            raise ValueError("storage reservation stream_subpath escapes its domain")
        if not str(self.receipt_mac):
            raise ValueError("storage reservation must carry an authenticated receipt")

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerExecutionClaim:
    """Authority receipt assigning one execution to exactly one operation."""

    authority_identity_sha256: str
    claim_id: str
    account_scope_sha256: str
    broker_execution_id: str
    broker_execution_sequence: int
    claim_sequence: int
    stream_identity_sha256: str
    operation_id: str
    operation_sequence: int
    payload_sha256: str
    fencing_token: int
    receipt_mac: str

    def __post_init__(self) -> None:
        for name in (
            "authority_identity_sha256",
            "account_scope_sha256",
            "stream_identity_sha256",
            "payload_sha256",
        ):
            _normal_sha256(getattr(self, name), name)
        _normal_identifier(self.claim_id, "claim_id")
        _normal_identifier(self.operation_id, "operation_id")
        execution_id = _normal_identifier(
            self.broker_execution_id, "broker_execution_id"
        )
        for name in (
            "broker_execution_sequence",
            "claim_sequence",
            "operation_sequence",
            "fencing_token",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or int(value) != value or int(value) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if execution_id != str(self.broker_execution_sequence):
            raise ValueError("broker execution claim id/sequence are not canonical")
        if not str(self.receipt_mac):
            raise ValueError("broker execution claim lacks an authority receipt")

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RuntimeMonotonicWitness:
    authority_identity_sha256: str
    reservation_id: str
    witness_version: int
    writer_epoch: int
    max_bar_sequence: int
    bar_chain_sha256: str
    checkpoint_generation: int
    checkpoint_sha256: str
    max_order_operation_sequence: int
    max_order_event_sequence: int
    order_chain_sha256: str
    reducer_contract_sha256: str
    fencing_token: int
    receipt_mac: str

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


class OperationalStorageAuthority(Protocol):
    """Narrow adapter implemented by the canonical native lifecycle authority."""

    @property
    def identity_sha256(self) -> str: ...

    def reserve_runtime_storage(
        self,
        *,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> StorageReservation: ...

    def verify_runtime_storage(
        self,
        reservation: StorageReservation,
        *,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> None: ...

    def read_runtime_witness(
        self, reservation: StorageReservation
    ) -> RuntimeMonotonicWitness: ...

    def cas_runtime_witness(
        self,
        reservation: StorageReservation,
        expected_witness: RuntimeMonotonicWitness,
        proposed_witness: Mapping[str, Any],
    ) -> RuntimeMonotonicWitness: ...

    def claim_broker_execution(
        self,
        reservation: StorageReservation,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Mapping[str, Any],
    ) -> BrokerExecutionClaim: ...

    def verify_broker_execution_claim(
        self,
        claim: BrokerExecutionClaim,
        reservation: StorageReservation,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Mapping[str, Any],
    ) -> None: ...


@dataclass(frozen=True)
class RuntimeLimits:
    max_db_bytes: int = 32 * 1024 * 1024
    max_total_storage_bytes: int = 65 * 1024 * 1024
    max_json_bytes: int = 256 * 1024
    max_state_bytes: int = 1024 * 1024
    max_bar_events: int = 4096
    max_bar_bytes: int = 16 * 1024 * 1024
    max_bar_segments: int = 8
    max_segment_events: int = 1024
    max_segment_bytes: int = 4 * 1024 * 1024
    max_order_operations: int = 512
    max_order_events: int = 1536
    max_pending_orders: int = 128
    busy_timeout_seconds: float = 2.0
    busy_retry_seconds: float = 0.025
    scan_batch_rows: int = 128
    full_audit_interval_mutations: int = 128

    def __post_init__(self) -> None:
        integer_names = (
            "max_db_bytes",
            "max_total_storage_bytes",
            "max_json_bytes",
            "max_state_bytes",
            "max_bar_events",
            "max_bar_bytes",
            "max_bar_segments",
            "max_segment_events",
            "max_segment_bytes",
            "max_order_operations",
            "max_order_events",
            "max_pending_orders",
            "scan_batch_rows",
            "full_audit_interval_mutations",
        )
        for name in integer_names:
            value = getattr(self, name)
            if isinstance(value, bool) or int(value) != value or int(value) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_db_bytes < 1024 * 1024:
            raise ValueError("max_db_bytes must be at least 1 MiB")
        if self.max_total_storage_bytes < self.max_db_bytes * 2 + 1024:
            raise ValueError(
                "max_total_storage_bytes must cover the DB plus rollback-journal peak"
            )
        if self.max_state_bytes > self.max_db_bytes // 2:
            raise ValueError("max_state_bytes must not exceed half max_db_bytes")
        if self.max_segment_events > self.max_bar_events:
            raise ValueError("segment event cap exceeds total bar event cap")
        if self.max_segment_bytes > self.max_bar_bytes:
            raise ValueError("segment byte cap exceeds total bar byte cap")
        if self.max_order_events < self.max_order_operations * 3:
            raise ValueError("order event cap must reserve three events per operation")
        if self.max_pending_orders > self.max_order_operations:
            raise ValueError("pending order cap exceeds operation cap")
        if self.busy_timeout_seconds <= 0 or self.busy_retry_seconds <= 0:
            raise ValueError("busy retry bounds must be positive")
        if self.busy_retry_seconds > self.busy_timeout_seconds:
            raise ValueError("busy retry interval exceeds total timeout")

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


def validate_local_storage_root(root: Path | str) -> Path:
    raw = os.path.abspath(os.fspath(root))
    if raw.startswith("\\\\"):
        raise StorageLocationError("UNC/network storage is forbidden for runtime state")
    unresolved = Path(raw)
    candidate = unresolved
    while True:
        if os.path.lexists(candidate) and candidate.is_symlink():
            raise StorageLocationError(
                f"symbolic-link storage path is forbidden: {candidate}"
            )
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    resolved = unresolved.resolve(strict=False)
    lowered_parts = {part.casefold() for part in resolved.parts}
    if lowered_parts & _CLOUD_PARTS:
        raise StorageLocationError(
            "cloud-synchronised storage is forbidden for operational runtime state"
        )
    for variable in ("OneDrive", "OneDriveCommercial", "OneDriveConsumer"):
        configured = os.environ.get(variable)
        if configured:
            cloud_root = Path(configured).resolve(strict=False)
            try:
                resolved.relative_to(cloud_root)
            except ValueError:
                pass
            else:
                raise StorageLocationError(
                    f"runtime root is inside cloud-synchronised {variable}"
                )
    if os.name == "nt":
        import ctypes

        existing = unresolved
        while not existing.exists():
            if existing.parent == existing:
                raise StorageLocationError(
                    f"runtime path has no existing volume ancestor: {unresolved}"
                )
            existing = existing.parent
        drive_root = existing.anchor or (existing.drive + "\\")
        drive_type = int(ctypes.windll.kernel32.GetDriveTypeW(str(drive_root)))
        if drive_type != 3:  # DRIVE_FIXED
            raise StorageLocationError(
                "runtime state requires a fixed local volume; "
                f"Windows drive type was {drive_type}"
            )
        invalid = 0xFFFFFFFF
        unsafe_flags = (
            0x00000400  # REPARSE_POINT
            | 0x00001000  # OFFLINE
            | 0x00040000  # RECALL_ON_OPEN
            | 0x00100000  # UNPINNED
            | 0x00400000  # RECALL_ON_DATA_ACCESS
        )
        candidate = unresolved
        while True:
            if candidate.exists():
                attributes = int(
                    ctypes.windll.kernel32.GetFileAttributesW(str(candidate))
                )
                if attributes != invalid and attributes & unsafe_flags:
                    raise StorageLocationError(
                        f"runtime path has unsafe Windows storage attributes: {candidate}"
                    )
            if candidate.parent == candidate:
                break
            candidate = candidate.parent
    return resolved


def _validate_operational_file(path: Path) -> None:
    if not os.path.lexists(path):
        return
    if path.is_symlink():
        raise StorageLocationError(f"operational file is a symbolic link: {path}")
    if os.name == "nt":
        import ctypes

        attributes = int(ctypes.windll.kernel32.GetFileAttributesW(str(path)))
        unsafe_flags = 0x00000400 | 0x00001000 | 0x00040000 | 0x00100000 | 0x00400000
        if attributes != 0xFFFFFFFF and attributes & unsafe_flags:
            raise StorageLocationError(
                f"operational file has unsafe Windows storage attributes: {path}"
            )


class _LifetimeLease:
    def __init__(self, path: Path, timeout: float, interval: float):
        self._path = path
        self._handle: Any = None
        deadline = time.monotonic() + timeout
        path.parent.mkdir(parents=True, exist_ok=True)
        _validate_operational_file(path)
        handle = path.open("a+b", buffering=0)
        _validate_operational_file(path)
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._handle = handle
                return
            except (OSError, BlockingIOError) as exc:
                if time.monotonic() >= deadline:
                    handle.close()
                    raise LockTimeoutError(
                        "runtime writer lease is already held"
                    ) from exc
                time.sleep(min(interval, max(0.0, deadline - time.monotonic())))

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None


@dataclass(frozen=True)
class BarReceipt:
    sequence: int
    source_sequence: int
    event_hash: str
    bar_sha256: str
    state_sha256: str
    close_time: str


@dataclass(frozen=True)
class CheckpointSnapshot:
    generation: int
    slot: str
    checkpoint_sha256: str
    state_sha256: str
    bar_sequence: int
    bar_hash: str
    last_closed_bar_time: Optional[str]


@dataclass(frozen=True)
class OrderIntentReceipt:
    operation_id: str
    operation_sequence: int
    client_request_id: str
    event_hash: str
    request_sha256: str


@dataclass(frozen=True)
class AuthenticatedOrderResult:
    operation_id: str
    operation_sequence: int
    intent_event_hash: str
    request_sha256: str
    result_outcome: str
    broker_execution_id: Optional[str]
    broker_execution_sequence: Optional[int]
    broker_highwater_receipt: Optional[Mapping[str, Any]]
    reconciliation_receipt_id: str
    reconciliation_method: str
    reconciled_at: str
    result: Mapping[str, Any]
    result_sha256: str
    authority_key_id: str
    signature_base64: str
    schema_version: str = "fxpm.authenticated-order-result.v2"

    def __post_init__(self) -> None:
        outcome = _normal_identifier(self.result_outcome, "result_outcome", upper=True)
        if outcome not in {"EXECUTED", "REJECTED_NO_EFFECT"}:
            raise ValueError("result_outcome must be EXECUTED or REJECTED_NO_EFFECT")
        object.__setattr__(self, "result_outcome", outcome)
        object.__setattr__(
            self,
            "authority_key_id",
            _normal_identifier(self.authority_key_id, "broker result authority_key_id"),
        )
        if self.schema_version != "fxpm.authenticated-order-result.v2":
            raise ValueError("unsupported authenticated order-result schema")
        if not str(self.signature_base64):
            raise AuthenticationError("broker result lacks an external signature")
        object.__setattr__(
            self,
            "reconciliation_receipt_id",
            _normal_identifier(
                self.reconciliation_receipt_id,
                "reconciliation_receipt_id",
                upper=False,
            ),
        )
        if outcome == "EXECUTED":
            execution_id = _normal_identifier(
                self.broker_execution_id, "broker_execution_id"
            )
            if (
                isinstance(self.broker_execution_sequence, bool)
                or self.broker_execution_sequence is None
                or int(self.broker_execution_sequence) != self.broker_execution_sequence
                or int(self.broker_execution_sequence) <= 0
            ):
                raise ValueError(
                    "EXECUTED broker_execution_sequence must be a positive integer"
                )
            execution_sequence = int(self.broker_execution_sequence)
            if execution_id != str(execution_sequence):
                raise ValueError(
                    "broker_execution_id must be the canonical broker execution sequence"
                )
            if self.reconciliation_receipt_id != f"EXECUTION:{execution_id}":
                raise ValueError(
                    "EXECUTED results require the EXECUTION receipt namespace"
                )
            object.__setattr__(self, "broker_execution_id", execution_id)
            object.__setattr__(self, "broker_execution_sequence", execution_sequence)
            if not isinstance(self.broker_highwater_receipt, Mapping):
                raise ValueError(
                    "EXECUTED result requires an external broker high-water receipt"
                )
            highwater = json.loads(
                _canonical_bytes(self.broker_highwater_receipt).decode("utf-8")
            )
            object.__setattr__(
                self, "broker_highwater_receipt", _deep_freeze(highwater)
            )
        else:
            if self.broker_execution_id is not None:
                raise ValueError(
                    "REJECTED_NO_EFFECT cannot carry a broker execution id"
                )
            if self.broker_execution_sequence is not None:
                raise ValueError(
                    "REJECTED_NO_EFFECT cannot carry a broker execution sequence"
                )
            if self.broker_highwater_receipt is not None:
                raise ValueError(
                    "REJECTED_NO_EFFECT cannot carry a broker high-water receipt"
                )
            no_effect_digest = self.reconciliation_receipt_id.removeprefix("NO_EFFECT:")
            if not _SHA256_RE.fullmatch(no_effect_digest):
                raise ValueError(
                    "REJECTED_NO_EFFECT requires a hashed NO_EFFECT receipt"
                )
        object.__setattr__(
            self,
            "reconciliation_method",
            _normal_identifier(
                self.reconciliation_method, "reconciliation_method", upper=True
            ),
        )
        object.__setattr__(
            self,
            "reconciled_at",
            _normal_timestamp(self.reconciled_at, "reconciled_at"),
        )
        result = json.loads(_canonical_bytes(self.result).decode("utf-8"))
        if _canonical_sha256(result) != self.result_sha256:
            raise AuthenticationError("broker result payload digest mismatch")
        raw_retcode = result.get("retcode")
        if (
            isinstance(raw_retcode, bool)
            or not isinstance(raw_retcode, int)
            or raw_retcode < 0
        ):
            raise ValueError("broker result requires an exact nonnegative MT5 retcode")
        status = _normal_identifier(
            result.get("status"), "broker result status", upper=True
        )
        ticket_by_name: dict[str, int] = {}
        for name in ("ticket", "order", "deal", "position"):
            raw_ticket = result.get(name, 0)
            if raw_ticket is None:
                raw_ticket = 0
            if (
                isinstance(raw_ticket, bool)
                or not isinstance(raw_ticket, int)
                or raw_ticket < 0
            ):
                raise ValueError(f"broker result {name} must be a nonnegative integer")
            ticket_by_name[name] = raw_ticket
        if outcome == "EXECUTED":
            if raw_retcode not in _MT5_EXECUTION_RETCODES:
                raise ValueError("EXECUTED result carries a non-execution MT5 retcode")
            if status not in _MT5_RETCODE_STATUSES[raw_retcode]:
                raise ValueError("EXECUTED result status contradicts its MT5 retcode")
            # MqlTradeResult exposes distinct deal and order tickets.  A
            # completed market request may carry both and they need not be
            # equal; the performed deal is the canonical side effect.  A
            # PLACED result has no performed deal and is keyed by its order.
            # ``ticket`` is an adapter alias and ``position`` is reconciliation
            # context, never a reason to collapse the MT5 ticket domains.
            if raw_retcode == 10008:
                if ticket_by_name["deal"] > 0:
                    raise ValueError("PLACED result cannot claim a performed deal")
                canonical_ticket = ticket_by_name["order"] or ticket_by_name["ticket"]
            else:
                canonical_ticket = (
                    ticket_by_name["deal"]
                    or ticket_by_name["order"]
                    or ticket_by_name["ticket"]
                )
            execution_sequence = int(self.broker_execution_sequence or 0)
            if canonical_ticket <= 0 or canonical_ticket != execution_sequence:
                raise ValueError(
                    "EXECUTED result canonical deal/order identity is inconsistent"
                )
            if (
                ticket_by_name["ticket"] > 0
                and ticket_by_name["ticket"] != execution_sequence
            ):
                raise ValueError("EXECUTED result ticket alias is inconsistent")
        else:
            if raw_retcode in _MT5_EXECUTION_RETCODES:
                raise ValueError(
                    "REJECTED_NO_EFFECT carries an MT5 execution-success retcode"
                )
            if status not in _MT5_NO_EFFECT_STATUSES:
                raise ValueError(
                    "REJECTED_NO_EFFECT carries an execution-capable status"
                )
            if any(ticket_by_name.values()):
                raise ValueError(
                    "REJECTED_NO_EFFECT cannot carry a positive broker ticket"
                )
        object.__setattr__(self, "result", _deep_freeze(result))

    def body(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "operation_id": self.operation_id,
            "operation_sequence": self.operation_sequence,
            "intent_event_hash": self.intent_event_hash,
            "request_sha256": self.request_sha256,
            "result_outcome": self.result_outcome,
            "broker_execution_id": self.broker_execution_id,
            "broker_execution_sequence": self.broker_execution_sequence,
            "broker_highwater_receipt": self.broker_highwater_receipt,
            "reconciliation_receipt_id": self.reconciliation_receipt_id,
            "reconciliation_method": self.reconciliation_method,
            "reconciled_at": self.reconciled_at,
            "result": self.result,
            "result_sha256": self.result_sha256,
            "authority_key_id": self.authority_key_id,
        }

    def canonical(self) -> dict[str, Any]:
        return {**self.body(), "signature_base64": self.signature_base64}

    def verify(
        self,
        trusted_public_keys: Mapping[str, bytes],
        intent: OrderIntentReceipt,
    ) -> None:
        _verify_external_ed25519(
            trusted_public_keys,
            authority_key_id=self.authority_key_id,
            domain="fxpm-broker-order-result",
            body=self.body(),
            signature_base64=self.signature_base64,
        )
        expected = (
            intent.operation_id,
            intent.operation_sequence,
            intent.event_hash,
            intent.request_sha256,
        )
        supplied = (
            self.operation_id,
            self.operation_sequence,
            self.intent_event_hash,
            self.request_sha256,
        )
        if supplied != expected:
            raise BindingMismatchError("broker result evidence/intent mismatch")


@dataclass(frozen=True)
class OrderResultReceipt:
    operation_id: str
    operation_sequence: int
    event_hash: str
    result_sha256: str
    evidence_sha256: str


@dataclass(frozen=True)
class OrderAppliedReceipt:
    operation_id: str
    operation_sequence: int
    event_hash: str
    local_state_sha256: str
    result_event_hash: str


@dataclass(frozen=True)
class OrderRecovery:
    operation_id: str
    operation_sequence: int
    client_request_id: str
    action: str
    phase: str
    request: Mapping[str, Any]
    result: Optional[Mapping[str, Any]]
    intent_receipt: OrderIntentReceipt
    result_receipt: Optional[OrderResultReceipt]


@dataclass(frozen=True)
class RuntimeRecovery:
    status: str
    reasons: tuple[str, ...]
    pending_orders: tuple[OrderRecovery, ...]
    bar_sequence: int
    checkpoint_generation: int

    @property
    def trading_ready(self) -> bool:
        return self.status == "READY"


_T = TypeVar("_T")


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runtime_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    payload BLOB NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    bar_sequence INTEGER NOT NULL,
    state_payload BLOB NOT NULL,
    state_sha256 TEXT NOT NULL,
    last_closed_bar_time TEXT,
    last_bar_sha256 TEXT,
    last_source_sequence INTEGER NOT NULL CHECK (last_source_sequence >= 0),
    last_source_session_id TEXT,
    last_source_connection_id TEXT,
    last_source_phase_seconds INTEGER,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bar_head (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    sequence INTEGER NOT NULL,
    event_hash TEXT NOT NULL,
    anchor_sequence INTEGER NOT NULL,
    anchor_hash TEXT NOT NULL,
    current_segment INTEGER NOT NULL,
    segment_events INTEGER NOT NULL,
    segment_bytes INTEGER NOT NULL,
    retained_events INTEGER NOT NULL,
    retained_bytes INTEGER NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bar_segments (
    segment_id INTEGER PRIMARY KEY,
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    first_previous_hash TEXT NOT NULL,
    last_event_hash TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bar_events (
    sequence INTEGER PRIMARY KEY,
    segment_id INTEGER NOT NULL REFERENCES bar_segments(segment_id),
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    bar_sha256 TEXT NOT NULL,
    close_time TEXT NOT NULL UNIQUE,
    source_sequence INTEGER NOT NULL UNIQUE CHECK (source_sequence > 0),
    source_session_id TEXT NOT NULL,
    source_connection_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    post_state_sha256 TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_bar_events_segment ON bar_events(segment_id, sequence);
CREATE TABLE IF NOT EXISTS checkpoint_slots (
    slot TEXT PRIMARY KEY CHECK (slot IN ('A', 'B')),
    generation INTEGER NOT NULL UNIQUE,
    checkpoint_sha256 TEXT NOT NULL UNIQUE,
    payload BLOB NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS active_checkpoint (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    slot TEXT NOT NULL CHECK (slot IN ('A', 'B')),
    generation INTEGER NOT NULL,
    checkpoint_sha256 TEXT NOT NULL,
    payload BLOB NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    writer_epoch INTEGER NOT NULL CHECK (writer_epoch > 0),
    max_operation_sequence INTEGER NOT NULL,
    max_client_sequence INTEGER NOT NULL,
    max_broker_execution_sequence INTEGER NOT NULL,
    retired_broker_execution_sequence INTEGER NOT NULL,
    last_broker_execution_sha256 TEXT NOT NULL,
    retired_through_sequence INTEGER NOT NULL,
    event_sequence INTEGER NOT NULL,
    event_hash TEXT NOT NULL,
    anchor_event_sequence INTEGER NOT NULL,
    anchor_event_hash TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_book_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    applied_operation_sequence INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_anchor_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    anchor_event_sequence INTEGER NOT NULL,
    last_applied_operation_sequence INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_operations (
    operation_sequence INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE,
    client_request_id TEXT NOT NULL UNIQUE,
    action TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('INTENT', 'RESULT', 'APPLIED')),
    request_payload BLOB NOT NULL,
    request_sha256 TEXT NOT NULL,
    intent_event_hash TEXT NOT NULL,
    result_payload BLOB,
    result_sha256 TEXT,
    result_event_hash TEXT,
    applied_event_hash TEXT,
    local_state_sha256 TEXT,
    mac TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_order_operations_pending
ON order_operations(phase, operation_sequence);
CREATE TABLE IF NOT EXISTS order_local_state (
    operation_sequence INTEGER PRIMARY KEY REFERENCES order_operations(operation_sequence),
    operation_id TEXT NOT NULL UNIQUE,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    result_event_hash TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_events (
    sequence INTEGER PRIMARY KEY,
    operation_sequence INTEGER NOT NULL REFERENCES order_operations(operation_sequence),
    operation_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('INTENT', 'RESULT', 'APPLIED')),
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    payload BLOB NOT NULL,
    mac TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_order_events_operation ON order_events(operation_sequence, sequence);
"""


class AuthenticatedStateRuntime:
    """Single-writer authenticated runtime with bounded transactional journals."""

    def __init__(
        self,
        root: Path | str,
        *,
        hmac_key: bytes,
        cadence_public_keys: Mapping[str, bytes],
        reducer_release_public_keys: Mapping[str, bytes],
        broker_result_public_keys: Mapping[str, bytes],
        source: SourceEpoch,
        roster: WinnerRoster,
        symbol: str,
        timeframe: str,
        engine: DeterministicStateEngine,
        reducer_release: AuthenticatedReducerRelease,
        storage_authority: OperationalStorageAuthority,
        state_evidence: Sequence[AuthenticatedStateEvidence] = (),
        limits: RuntimeLimits = RuntimeLimits(),
        provision: bool = False,
    ):
        self.root = validate_local_storage_root(root)
        if provision:
            self.root.mkdir(parents=True, exist_ok=True)
        elif not self.root.is_dir():
            raise RecoveryRequiredError(
                "runtime root is absent; explicit provision=True is required"
            )
        self.db_path = self.root / "state_runtime.sqlite3"
        _validate_operational_file(self.db_path)
        if provision and self.db_path.exists():
            raise RecoveryRequiredError(
                "refusing to provision over an existing runtime DB"
            )
        if not provision and not self.db_path.is_file():
            raise RecoveryRequiredError(
                "runtime DB is absent; explicit provision=True is required"
            )
        self.source = source
        self.roster = roster
        self.symbol = _normal_identifier(symbol, "symbol", upper=True)
        self.timeframe = _normal_identifier(timeframe, "timeframe", upper=True)
        if self.timeframe not in _TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {self.timeframe}")
        if not any(
            winner.symbol == self.symbol and winner.timeframe == self.timeframe
            for winner in roster.winners
        ):
            raise BindingMismatchError(
                "winner roster contains no cell for the runtime symbol/timeframe"
            )
        self.engine = engine
        self.limits = limits
        self.authenticator = HMACAuthenticator(hmac_key)
        self.cadence_public_keys = _owned_public_key_registry(
            cadence_public_keys, name="V5 cadence"
        )
        self.reducer_release_public_keys = _owned_public_key_registry(
            reducer_release_public_keys, name="reducer release"
        )
        self.broker_result_public_keys = _owned_public_key_registry(
            broker_result_public_keys, name="broker result"
        )
        registries = (
            self.cadence_public_keys,
            self.reducer_release_public_keys,
            self.broker_result_public_keys,
        )
        flattened = [key for registry in registries for key in registry.values()]
        if len(set(flattened)) != len(flattened):
            raise AuthenticationError(
                "cadence, reducer-release and broker-result authorities must be independent"
            )
        self.cadence_authority_registry_sha256 = _canonical_sha256(
            {key: value.hex() for key, value in self.cadence_public_keys.items()}
        )
        self.reducer_authority_registry_sha256 = _canonical_sha256(
            {
                key: value.hex()
                for key, value in self.reducer_release_public_keys.items()
            }
        )
        self.broker_result_authority_registry_sha256 = _canonical_sha256(
            {key: value.hex() for key, value in self.broker_result_public_keys.items()}
        )
        self.production_integrated = False
        if not isinstance(reducer_release, AuthenticatedReducerRelease):
            raise RecoveryRequiredError(
                "an independently authenticated reducer release is required"
            )
        (
            verified_engine_identity,
            verified_engine_manifest,
            verified_engine_configuration_sha256,
        ) = reducer_release.verify(self.reducer_release_public_keys, engine)
        self.reducer_release = reducer_release
        self.reducer_release_sha256 = reducer_release.identity_sha256
        self._engine_functions = _admitted_engine_functions(engine)
        self._active_reducer_inputs: Optional[list[tuple[str, Any]]] = None
        initial_engine_configuration_sha256 = verified_engine_configuration_sha256
        engine_identity = verified_engine_identity
        self.engine_identity = _deep_freeze(engine_identity)
        self.engine_declared_sha256 = _canonical_sha256(engine_identity)
        self.engine_implementation_sha256 = _canonical_sha256(verified_engine_manifest)
        self.engine_instance_configuration_sha256 = (
            _engine_instance_configuration_sha256(engine)
        )
        if (
            self.engine_instance_configuration_sha256
            != initial_engine_configuration_sha256
        ):
            raise ValueError(
                "state engine mutated its instance configuration during contract binding"
            )
        self.reducer_contract_sha256 = _canonical_sha256(
            {
                "schema_version": "fxpm.reducer-contract.v3",
                "authenticated_release_sha256": self.reducer_release_sha256,
                "declared_identity_sha256": self.engine_declared_sha256,
                "implementation_sha256": self.engine_implementation_sha256,
                "instance_configuration_sha256": (
                    self.engine_instance_configuration_sha256
                ),
                "state_registry_hash": DEFAULT_REGISTRY_HASH,
            }
        )
        self.engine_sha256 = self.reducer_contract_sha256
        if storage_authority is None:
            raise RecoveryRequiredError(
                "an institution-wide operational storage authority is required"
            )
        for method_name in (
            "reserve_runtime_storage",
            "verify_runtime_storage",
            "read_runtime_witness",
            "cas_runtime_witness",
            "claim_broker_execution",
            "verify_broker_execution_claim",
        ):
            if not callable(getattr(storage_authority, method_name, None)):
                raise RecoveryRequiredError(
                    "operational storage authority lacks required method: "
                    + method_name
                )
        self.storage_authority = storage_authority
        self.execution_account_scope_sha256 = _canonical_sha256(
            {
                "schema_version": "fxpm.broker-account-scope.v1",
                "broker": self.source.broker,
                "server": self.source.server,
                "account_id": self.source.account_id,
            }
        )
        authority_identity = _normal_sha256(
            storage_authority.identity_sha256, "storage authority identity"
        )
        stream_identity = _canonical_sha256(
            {
                "source_epoch_sha256": source.identity_sha256,
                "roster_sha256": roster.identity_sha256,
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "engine_sha256": self.engine_sha256,
                "operational_root": str(self.root),
            }
        )
        reservation = storage_authority.reserve_runtime_storage(
            operational_root=self.root,
            stream_identity_sha256=stream_identity,
            requested_peak_bytes=limits.max_total_storage_bytes,
        )
        storage_authority.verify_runtime_storage(
            reservation,
            operational_root=self.root,
            stream_identity_sha256=stream_identity,
            requested_peak_bytes=limits.max_total_storage_bytes,
        )
        if reservation.authority_identity_sha256 != authority_identity:
            raise BindingMismatchError(
                "storage reservation authority identity mismatch"
            )
        domain_root = validate_local_storage_root(reservation.operational_domain_root)
        try:
            self.root.relative_to(domain_root)
        except ValueError as exc:
            raise BindingMismatchError(
                "runtime root escapes the signed operational storage domain"
            ) from exc
        signed_stream_root = (domain_root / reservation.stream_subpath).resolve(
            strict=False
        )
        if signed_stream_root != self.root:
            raise BindingMismatchError("storage reservation subpath/root mismatch")
        if reservation.stream_identity_sha256 != stream_identity:
            raise BindingMismatchError("storage reservation stream identity mismatch")
        self.stream_identity_sha256 = stream_identity
        if reservation.reserved_peak_bytes < limits.max_total_storage_bytes:
            raise CapacityExceededError(
                "storage reservation is smaller than runtime peak"
            )
        if reservation.admitted_streams_cap > 600:
            raise CapacityExceededError(
                "storage authority admits more than 600 FxPM streams"
            )
        self.storage_reservation = reservation
        self._last_verified_witness_proposal: Optional[dict[str, Any]] = None
        self._mutations_since_full_audit = 0
        self._verification_metrics: dict[str, int] = {
            "incremental_audits": 0,
            "full_audits": 0,
            "replayed_bar_events": 0,
            "replayed_order_events": 0,
        }
        initial_witness = storage_authority.read_runtime_witness(reservation)
        self._validate_runtime_witness(initial_witness)
        if provision:
            genesis_values = (
                int(initial_witness.writer_epoch),
                int(initial_witness.max_bar_sequence),
                int(initial_witness.checkpoint_generation),
                int(initial_witness.max_order_operation_sequence),
                int(initial_witness.max_order_event_sequence),
            )
            genesis_hashes = (
                str(initial_witness.bar_chain_sha256),
                str(initial_witness.checkpoint_sha256),
                str(initial_witness.order_chain_sha256),
                str(initial_witness.reducer_contract_sha256),
            )
            if genesis_values != (0, 0, 0, 0, 0) or genesis_hashes != (
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH,
            ):
                raise RecoveryRequiredError(
                    "external monotonic witness is not genesis; reseeding is forbidden"
                )
        self._provision_writer_epoch = int(initial_witness.writer_epoch) + 1
        self._lease: Optional[_LifetimeLease] = None
        self._closed = False
        self._recovery = RuntimeRecovery(
            "MANAGEMENT_ONLY", ("UNINITIALIZED",), (), 0, 0
        )
        self._lease = _LifetimeLease(
            self.root / "state_runtime.writer.lock",
            limits.busy_timeout_seconds,
            limits.busy_retry_seconds,
        )
        try:
            self._initialize_database(tuple(state_evidence), provision=provision)
            self.refresh_recovery()
        except (
            StateRuntimeError,
            sqlite3.Error,
            ValueError,
            TypeError,
            KeyError,
        ) as exc:
            if not provision:
                self._recovery = RuntimeRecovery(
                    "MANAGEMENT_ONLY",
                    (f"OPEN_VERIFICATION_FAILED:{type(exc).__name__}:{exc}",),
                    (),
                    0,
                    0,
                )
                return
            self.close()
            raise
        except Exception:
            self.close()
            raise

    def __enter__(self) -> "AuthenticatedStateRuntime":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        if self._lease is not None:
            self._lease.close()
            self._lease = None
        self._closed = True

    @property
    def recovery(self) -> RuntimeRecovery:
        return self._recovery

    @property
    def trading_ready(self) -> bool:
        return self._recovery.trading_ready

    @property
    def verification_metrics(self) -> Mapping[str, int]:
        """Return monotonic verification-work counters for operational telemetry."""

        return MappingProxyType(dict(self._verification_metrics))

    def refresh_recovery(self) -> RuntimeRecovery:
        self._ensure_open()
        self._last_verified_witness_proposal = None
        self._recovery = self._assess_recovery()
        proposal = self._last_verified_witness_proposal
        if proposal is not None:
            try:
                self._synchronize_runtime_witness(proposal)
            except Exception as exc:
                self._recovery = RuntimeRecovery(
                    "MANAGEMENT_ONLY",
                    (
                        *self._recovery.reasons,
                        f"MONOTONIC_WITNESS_FAILURE:{type(exc).__name__}:{exc}",
                    ),
                    self._recovery.pending_orders,
                    self._recovery.bar_sequence,
                    self._recovery.checkpoint_generation,
                )
        return self._recovery

    def _ensure_open(self) -> None:
        if self._closed:
            raise StateRuntimeError("runtime is closed")

    def _assert_engine_contract_current(self) -> None:
        """Recompute every reducer surface before and after engine execution."""

        (
            current_identity,
            current_manifest,
            current_configuration,
        ) = self.reducer_release.verify(self.reducer_release_public_keys, self.engine)
        if _canonical_sha256(current_identity) != self.engine_declared_sha256:
            raise BindingMismatchError("state engine declared identity changed")
        current_implementation = _canonical_sha256(current_manifest)
        if current_implementation != self.engine_implementation_sha256:
            raise BindingMismatchError(
                "state engine transitive implementation/dependency surface changed"
            )
        if current_configuration != self.engine_instance_configuration_sha256:
            raise BindingMismatchError("state engine instance configuration changed")
        current_contract = _canonical_sha256(
            {
                "schema_version": "fxpm.reducer-contract.v3",
                "authenticated_release_sha256": self.reducer_release_sha256,
                "declared_identity_sha256": self.engine_declared_sha256,
                "implementation_sha256": current_implementation,
                "instance_configuration_sha256": current_configuration,
                "state_registry_hash": DEFAULT_REGISTRY_HASH,
            }
        )
        if current_contract != self.reducer_contract_sha256:
            raise BindingMismatchError("state engine reducer contract changed")

    @staticmethod
    def _reducer_input_value(value: Any) -> Any:
        if isinstance(value, Mapping) or isinstance(value, (list, tuple)):
            return _deep_freeze(json.loads(_canonical_bytes(value).decode("utf-8")))
        if isinstance(value, SourceEpoch):
            return SourceEpoch(**value.canonical())
        if isinstance(value, WinnerRoster):
            canonical = json.loads(_canonical_bytes(value.canonical()).decode("utf-8"))
            canonical["winners"] = tuple(
                WinnerRecord(**winner) for winner in canonical["winners"]
            )
            return WinnerRoster(**canonical)
        if isinstance(value, AuthenticatedStateEvidence):
            return AuthenticatedStateEvidence(
                **json.loads(_canonical_bytes(value.canonical()).decode("utf-8"))
            )
        if value is None or isinstance(value, (str, bool, int, float)):
            return value
        raise ValueError(
            "reducer input is outside the immutable admitted value contract: "
            f"{type(value).__module__}.{type(value).__qualname__}"
        )

    @staticmethod
    def _reducer_input_sha256(arguments: Sequence[Any]) -> str:
        canonical: list[Any] = []
        for value in arguments:
            if isinstance(value, SourceEpoch):
                canonical.append({"kind": "source", "value": value.canonical()})
            elif isinstance(value, WinnerRoster):
                canonical.append({"kind": "roster", "value": value.canonical()})
            elif isinstance(value, AuthenticatedStateEvidence):
                canonical.append({"kind": "evidence", "value": value.canonical()})
            else:
                canonical.append({"kind": "json", "value": value})
        return _canonical_sha256(canonical)

    def _verify_active_reducer_inputs(self) -> None:
        if self._active_reducer_inputs is None:
            return
        for expected_sha, arguments in self._active_reducer_inputs:
            if self._reducer_input_sha256(arguments) != expected_sha:
                raise IntegrityError(
                    "reducer input changed after invocation and before durable commit"
                )

    def _invoke_engine(self, method_name: str, *arguments: Any) -> Any:
        self._assert_engine_contract_current()
        function = self._engine_functions.get(method_name)
        if function is None:
            raise BindingMismatchError("reducer method is outside the signed allowlist")
        private_arguments = tuple(
            self._reducer_input_value(value) for value in arguments
        )
        input_sha = self._reducer_input_sha256(private_arguments)
        if self._active_reducer_inputs is not None:
            self._active_reducer_inputs.append((input_sha, private_arguments))
        try:
            return function(self.engine, *private_arguments)
        finally:
            if self._reducer_input_sha256(private_arguments) != input_sha:
                raise IntegrityError("reducer mutated its private immutable input")
            self._assert_engine_contract_current()

    def _validate_runtime_witness(self, witness: Any) -> None:
        required = (
            "authority_identity_sha256",
            "reservation_id",
            "witness_version",
            "writer_epoch",
            "max_bar_sequence",
            "bar_chain_sha256",
            "checkpoint_generation",
            "checkpoint_sha256",
            "max_order_operation_sequence",
            "max_order_event_sequence",
            "order_chain_sha256",
            "reducer_contract_sha256",
            "fencing_token",
            "receipt_mac",
        )
        if any(not hasattr(witness, name) for name in required):
            raise AuthenticationError("runtime witness shape is incomplete")
        if (
            witness.authority_identity_sha256
            != self.storage_reservation.authority_identity_sha256
        ):
            raise BindingMismatchError("runtime witness authority mismatch")
        if witness.reservation_id != self.storage_reservation.reservation_id:
            raise BindingMismatchError("runtime witness reservation mismatch")
        for name in (
            "witness_version",
            "writer_epoch",
            "max_bar_sequence",
            "checkpoint_generation",
            "max_order_operation_sequence",
            "max_order_event_sequence",
            "fencing_token",
        ):
            value = getattr(witness, name)
            if isinstance(value, bool) or int(value) != value or int(value) < 0:
                raise AuthenticationError(f"runtime witness {name} is invalid")
        for name in (
            "bar_chain_sha256",
            "checkpoint_sha256",
            "order_chain_sha256",
            "reducer_contract_sha256",
        ):
            _normal_sha256(getattr(witness, name), f"runtime witness {name}")
        if int(witness.max_order_event_sequence) < int(
            witness.max_order_operation_sequence
        ):
            raise IntegrityError(
                "runtime witness order event sequence trails operations"
            )
        if not str(witness.receipt_mac):
            raise AuthenticationError("runtime witness lacks an authority receipt")

    def _witness_proposal_from_rows(
        self,
        bar_head: sqlite3.Row,
        order_meta: sqlite3.Row,
        active: Optional[tuple[CheckpointSnapshot, dict[str, Any]]],
    ) -> dict[str, Any]:
        return {
            "writer_epoch": int(order_meta["writer_epoch"]),
            "max_bar_sequence": int(bar_head["sequence"]),
            "bar_chain_sha256": str(bar_head["event_hash"]),
            "checkpoint_generation": active[0].generation if active else 0,
            "checkpoint_sha256": active[0].checkpoint_sha256 if active else ZERO_HASH,
            "max_order_operation_sequence": int(order_meta["max_operation_sequence"]),
            "max_order_event_sequence": int(order_meta["event_sequence"]),
            "order_chain_sha256": str(order_meta["event_hash"]),
            "reducer_contract_sha256": self.reducer_contract_sha256,
        }

    def _compare_witness_to_local(self, witness: Any, local: Mapping[str, Any]) -> str:
        self._validate_runtime_witness(witness)
        witness_epoch = int(witness.writer_epoch)
        local_epoch = int(local["writer_epoch"])
        if local_epoch < witness_epoch:
            raise IntegrityError(
                "local writer epoch is behind external monotonic witness"
            )
        if local_epoch > witness_epoch and not (
            witness_epoch == 0 and local_epoch == 1
        ):
            raise IntegrityError("local writer epoch diverges from external witness")
        ahead = local_epoch > witness_epoch
        witnessed_reducer = str(witness.reducer_contract_sha256)
        local_reducer = str(local["reducer_contract_sha256"])
        if witnessed_reducer == ZERO_HASH and witness_epoch == 0:
            if local_reducer == ZERO_HASH:
                raise IntegrityError("local reducer contract is not provisioned")
            ahead = True
        elif local_reducer != witnessed_reducer:
            raise IntegrityError(
                "local reducer contract conflicts with external monotonic witness"
            )
        surfaces = (
            ("max_bar_sequence", "bar_chain_sha256"),
            ("checkpoint_generation", "checkpoint_sha256"),
            ("max_order_event_sequence", "order_chain_sha256"),
        )
        for counter_name, hash_name in surfaces:
            local_counter = int(local[counter_name])
            witnessed_counter = int(getattr(witness, counter_name))
            local_hash = str(local[hash_name])
            witnessed_hash = str(getattr(witness, hash_name))
            if local_counter < witnessed_counter:
                raise IntegrityError(
                    f"local {counter_name} is behind external monotonic witness"
                )
            if local_counter == witnessed_counter and local_hash != witnessed_hash:
                raise IntegrityError(
                    f"local {hash_name} conflicts with external monotonic witness"
                )
            if local_counter > witnessed_counter:
                if local_hash == witnessed_hash:
                    raise IntegrityError(
                        f"advanced {counter_name} reused its witnessed chain hash"
                    )
                ahead = True
        local_operations = int(local["max_order_operation_sequence"])
        witnessed_operations = int(witness.max_order_operation_sequence)
        if local_operations < witnessed_operations:
            raise IntegrityError(
                "local order operation sequence is behind external monotonic witness"
            )
        if local_operations > witnessed_operations:
            ahead = True
        return "AHEAD" if ahead else "EQUAL"

    def _synchronize_runtime_witness(self, local: Mapping[str, Any]) -> None:
        deadline = time.monotonic() + self.limits.busy_timeout_seconds
        last_error: Optional[BaseException] = None
        while True:
            current = self.storage_authority.read_runtime_witness(
                self.storage_reservation
            )
            relation = self._compare_witness_to_local(current, local)
            if relation == "EQUAL":
                return
            try:
                advanced = self.storage_authority.cas_runtime_witness(
                    self.storage_reservation,
                    current,
                    dict(local),
                )
                self._validate_runtime_witness(advanced)
                if self._compare_witness_to_local(advanced, local) != "EQUAL":
                    raise PublicationError(
                        "storage authority returned a nonmatching runtime witness"
                    )
                return
            except IntegrityError:
                raise
            except Exception as exc:
                last_error = exc
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PublicationError(
                    "external monotonic witness CAS deadline expired"
                ) from last_error
            time.sleep(min(self.limits.busy_retry_seconds, remaining))

    def _require_witness_current(self) -> None:
        failure = next(
            (
                reason
                for reason in self._recovery.reasons
                if reason.startswith("MONOTONIC_WITNESS_FAILURE:")
            ),
            None,
        )
        if failure is not None:
            raise PublicationError(failure)

    def _connect(self, *, initializing: bool = False) -> sqlite3.Connection:
        if validate_local_storage_root(self.root) != self.root:
            raise StorageLocationError("runtime root identity changed")
        _validate_operational_file(self.db_path)
        if self.db_path.resolve(strict=False).parent != self.root:
            raise StorageLocationError("runtime DB escaped its operational root")
        pre_open_identity: Optional[tuple[int, int]] = None
        if self.db_path.exists():
            observed = self.db_path.stat(follow_symlinks=False)
            pre_open_identity = (int(observed.st_dev), int(observed.st_ino))
        connection = sqlite3.connect(
            self.db_path,
            timeout=0.0,
            isolation_level=None,
        )
        try:
            _validate_operational_file(self.db_path)
            connection.row_factory = sqlite3.Row
            database_rows = connection.execute("PRAGMA database_list").fetchall()
            main_paths = [str(row[2]) for row in database_rows if str(row[1]) == "main"]
            if len(main_paths) != 1 or Path(main_paths[0]).resolve(
                strict=True
            ) != self.db_path.resolve(strict=True):
                raise StorageLocationError(
                    "SQLite main handle does not resolve to the admitted runtime DB"
                )
            post_open = self.db_path.stat(follow_symlinks=False)
            post_open_identity = (int(post_open.st_dev), int(post_open.st_ino))
            if (
                pre_open_identity is not None
                and post_open_identity != pre_open_identity
            ):
                raise StorageLocationError(
                    "runtime DB filesystem identity changed while opening SQLite"
                )
            connection.execute("PRAGMA busy_timeout=0")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA secure_delete=ON")
            connection.execute("PRAGMA journal_size_limit=0")
            mode = str(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            if mode != "delete":
                raise IntegrityError(f"SQLite journal mode is {mode}, expected delete")
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            max_pages = max(1, self.limits.max_db_bytes // page_size)
            actual_max = int(
                connection.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if actual_max != max_pages:
                raise IntegrityError("SQLite max_page_count could not be enforced")
            if int(connection.execute("PRAGMA synchronous").fetchone()[0]) != 2:
                raise IntegrityError("SQLite synchronous mode is not FULL")
            if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
                raise IntegrityError("SQLite foreign keys are disabled")
            if not initializing:
                application_id = int(
                    connection.execute("PRAGMA application_id").fetchone()[0]
                )
                user_version = int(
                    connection.execute("PRAGMA user_version").fetchone()[0]
                )
                if application_id != SQLITE_APPLICATION_ID:
                    raise IntegrityError("SQLite application_id mismatch")
                if user_version != SQLITE_USER_VERSION:
                    raise IntegrityError("SQLite user_version mismatch")
            return connection
        except Exception:
            connection.close()
            raise

    @staticmethod
    def _busy_error(exc: sqlite3.OperationalError) -> bool:
        message = str(exc).casefold()
        return "locked" in message or "busy" in message

    def _run_write(self, operation: Callable[[sqlite3.Connection], _T]) -> _T:
        self._ensure_open()
        if self._active_reducer_inputs is not None:
            raise IntegrityError("nested runtime write/reducer transcript is forbidden")
        deadline = time.monotonic() + self.limits.busy_timeout_seconds
        last_busy: Optional[BaseException] = None
        while True:
            connection: Optional[sqlite3.Connection] = None
            commit_started = False
            committed = False
            try:
                self._active_reducer_inputs = []
                connection = self._connect()
                connection.execute("BEGIN IMMEDIATE")
                self._assert_engine_contract_current()
                result = operation(connection)
                # Close the interval between reducer return and durable
                # mutation: a dependency/release change at any point aborts
                # the entire SQLite transaction before COMMIT.
                self._assert_engine_contract_current()
                self._verify_active_reducer_inputs()
                self._check_page_budget(connection)
                _fault_point("before_commit")
                commit_started = True
                connection.execute("COMMIT")
                committed = True
                _fault_point("after_commit")
                return result
            except sqlite3.OperationalError as exc:
                if connection is not None and not committed:
                    try:
                        connection.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                if not self._busy_error(exc):
                    if commit_started:
                        if connection is not None:
                            connection.close()
                            connection = None
                        self._recovery = RuntimeRecovery(
                            "MANAGEMENT_ONLY",
                            (f"AMBIGUOUS_SQLITE_COMMIT:{exc}",),
                            (),
                            self._recovery.bar_sequence,
                            self._recovery.checkpoint_generation,
                        )
                    raise
                last_busy = exc
            except Exception as exc:
                if connection is not None and not committed:
                    try:
                        connection.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                if committed:
                    connection.close()
                    connection = None
                    try:
                        self.refresh_recovery()
                    except (
                        Exception
                    ) as recovery_exc:  # pragma: no cover - last-resort fence
                        self._recovery = RuntimeRecovery(
                            "MANAGEMENT_ONLY",
                            (
                                f"AMBIGUOUS_COMMIT_RECOVERY_FAILED:"
                                f"{type(recovery_exc).__name__}:{recovery_exc}",
                            ),
                            (),
                            self._recovery.bar_sequence,
                            self._recovery.checkpoint_generation,
                        )
                elif isinstance(
                    exc,
                    (
                        AuthenticationError,
                        BindingMismatchError,
                        IntegrityError,
                        StorageLocationError,
                    ),
                ):
                    self._recovery = RuntimeRecovery(
                        "MANAGEMENT_ONLY",
                        (f"PREMUTATION_INVARIANT_FAILURE:{type(exc).__name__}:{exc}",),
                        self._recovery.pending_orders,
                        self._recovery.bar_sequence,
                        self._recovery.checkpoint_generation,
                    )
                raise
            finally:
                self._active_reducer_inputs = None
                if connection is not None:
                    connection.close()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LockTimeoutError(
                    "SQLite write retry deadline expired"
                ) from last_busy
            time.sleep(min(self.limits.busy_retry_seconds, remaining))

    def _run_read(self, operation: Callable[[sqlite3.Connection], _T]) -> _T:
        self._ensure_open()
        deadline = time.monotonic() + self.limits.busy_timeout_seconds
        last_busy: Optional[BaseException] = None
        while True:
            connection: Optional[sqlite3.Connection] = None
            try:
                connection = self._connect()
                return operation(connection)
            except sqlite3.OperationalError as exc:
                if not self._busy_error(exc):
                    raise
                last_busy = exc
            finally:
                if connection is not None:
                    connection.close()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LockTimeoutError(
                    "SQLite read retry deadline expired"
                ) from last_busy
            time.sleep(min(self.limits.busy_retry_seconds, remaining))

    def _check_page_budget(self, connection: sqlite3.Connection) -> None:
        pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        if pages * page_size > self.limits.max_db_bytes:
            raise CapacityExceededError("SQLite database page budget exceeded")
        journal = Path(str(self.db_path) + "-journal")
        journal_bytes = journal.stat().st_size if journal.exists() else 0
        if pages * page_size + journal_bytes > self.limits.max_total_storage_bytes:
            raise CapacityExceededError(
                "SQLite DB plus rollback-journal peak exceeds cap"
            )

    def _binding_body(
        self, evidence: Sequence[AuthenticatedStateEvidence], *, bootstrapped: bool
    ) -> dict[str, Any]:
        return {
            "schema_version": RUNTIME_SCHEMA_VERSION,
            "source": self.source.canonical(),
            "source_epoch_sha256": self.source.identity_sha256,
            "cadence_authority_registry_sha256": (
                self.cadence_authority_registry_sha256
            ),
            "reducer_authority_registry_sha256": (
                self.reducer_authority_registry_sha256
            ),
            "broker_result_authority_registry_sha256": (
                self.broker_result_authority_registry_sha256
            ),
            "roster": self.roster.canonical(),
            "roster_sha256": self.roster.identity_sha256,
            "registry_hash": DEFAULT_REGISTRY_HASH,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "engine_identity": dict(self.engine_identity),
            "engine_declared_sha256": self.engine_declared_sha256,
            "engine_implementation_sha256": self.engine_implementation_sha256,
            "engine_instance_configuration_sha256": (
                self.engine_instance_configuration_sha256
            ),
            "authenticated_reducer_release": self.reducer_release.canonical(),
            "authenticated_reducer_release_sha256": self.reducer_release_sha256,
            "engine_sha256": self.engine_sha256,
            "execution_account_scope_sha256": (self.execution_account_scope_sha256),
            "state_evidence": [item.canonical() for item in evidence],
            "limits": self.limits.canonical(),
            "storage_reservation": self.storage_reservation.canonical(),
            "bootstrapped": bool(bootstrapped),
        }

    def _initialize_database(
        self,
        supplied_evidence: tuple[AuthenticatedStateEvidence, ...],
        *,
        provision: bool,
    ) -> None:
        verified_evidence: tuple[AuthenticatedStateEvidence, ...] = ()
        state_payload = b""
        state_sha = ""
        order_state_payload = b""
        order_state_sha = ""
        if provision:
            # Validate all external inputs and reducer seeds before SQLite can
            # create even an empty operational file.
            verified_evidence = self._verify_evidence_tuple(supplied_evidence)
            initial_state = self._invoke_engine(
                "initial_state",
                self.source,
                self.roster,
                self.symbol,
                self.timeframe,
                {item.node: item.payload for item in verified_evidence},
            )
            state_payload, state_sha = _bounded_payload(
                initial_state, self.limits.max_state_bytes, "initial state"
            )
            initial_order_state = self._invoke_engine(
                "initial_order_state",
                self.source,
                self.roster,
                self.symbol,
                self.timeframe,
            )
            order_state_payload, order_state_sha = _bounded_payload(
                initial_order_state,
                self.limits.max_state_bytes,
                "initial order state",
            )
        connection = self._connect(initializing=provision)
        try:
            if provision:
                # Provisioning is itself a durable mutation.  Re-check the
                # independently signed reducer surface immediately around
                # schema creation, just as normal state writes do around
                # their transaction body and COMMIT.
                self._assert_engine_contract_current()
                connection.execute("PRAGMA page_size=4096")
                connection.execute(f"PRAGMA application_id={SQLITE_APPLICATION_ID}")
                connection.execute(f"PRAGMA user_version={SQLITE_USER_VERSION}")
                connection.executescript(_SCHEMA_SQL)
                self._assert_engine_contract_current()
            else:
                required_tables = {
                    "runtime_meta",
                    "runtime_state",
                    "bar_head",
                    "bar_segments",
                    "bar_events",
                    "checkpoint_slots",
                    "active_checkpoint",
                    "order_meta",
                    "order_operations",
                    "order_local_state",
                    "order_events",
                    "order_book_state",
                    "order_anchor_state",
                }
                actual_tables = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if not required_tables.issubset(actual_tables):
                    raise IntegrityError("runtime DB schema is incomplete")
            self._check_page_budget(connection)
            meta = connection.execute(
                "SELECT payload, mac FROM runtime_meta WHERE singleton=1"
            ).fetchone()
        finally:
            connection.close()
        if not provision:
            if meta is None:
                raise IntegrityError("existing runtime DB has no authenticated binding")
            self._verify_existing_binding(supplied_evidence)
            return
        if meta is not None:
            raise IntegrityError("new runtime DB unexpectedly contains a binding")

        def initialize(connection: sqlite3.Connection) -> None:
            if connection.execute("SELECT 1 FROM runtime_meta").fetchone() is not None:
                raise IntegrityError("runtime was concurrently initialized")
            meta_body = self._binding_body(verified_evidence, bootstrapped=False)
            meta_payload, _ = _bounded_payload(
                meta_body, self.limits.max_json_bytes, "runtime binding"
            )
            connection.execute(
                "INSERT INTO runtime_meta VALUES (1, ?, ?)",
                (meta_payload, self.authenticator.sign("runtime-meta", meta_body)),
            )
            state_body = {
                "bar_sequence": 0,
                "state_sha256": state_sha,
                "last_closed_bar_time": None,
                "last_bar_sha256": None,
                "last_source_sequence": 0,
                "last_source_session_id": None,
                "last_source_connection_id": None,
                "last_source_phase_seconds": None,
            }
            connection.execute(
                "INSERT INTO runtime_state VALUES "
                "(1, 0, ?, ?, NULL, NULL, 0, NULL, NULL, NULL, ?)",
                (
                    state_payload,
                    state_sha,
                    self.authenticator.sign("runtime-state", state_body),
                ),
            )
            head_body = {
                "sequence": 0,
                "event_hash": ZERO_HASH,
                "anchor_sequence": 0,
                "anchor_hash": ZERO_HASH,
                "current_segment": 1,
                "segment_events": 0,
                "segment_bytes": 0,
                "retained_events": 0,
                "retained_bytes": 0,
            }
            connection.execute(
                "INSERT INTO bar_head VALUES (1, 0, ?, 0, ?, 1, 0, 0, 0, 0, ?)",
                (
                    ZERO_HASH,
                    ZERO_HASH,
                    self.authenticator.sign("bar-head", head_body),
                ),
            )
            writer_epoch = self._provision_writer_epoch
            order_body = {
                "writer_epoch": writer_epoch,
                "max_operation_sequence": 0,
                "max_client_sequence": 0,
                "max_broker_execution_sequence": 0,
                "retired_broker_execution_sequence": 0,
                "last_broker_execution_sha256": ZERO_HASH,
                "retired_through_sequence": 0,
                "event_sequence": 0,
                "event_hash": ZERO_HASH,
                "anchor_event_sequence": 0,
                "anchor_event_hash": ZERO_HASH,
            }
            connection.execute(
                "INSERT INTO order_meta VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    writer_epoch,
                    0,
                    0,
                    0,
                    0,
                    ZERO_HASH,
                    0,
                    0,
                    ZERO_HASH,
                    0,
                    ZERO_HASH,
                    self.authenticator.sign("order-meta", order_body),
                ),
            )
            order_state_body = {
                "applied_operation_sequence": 0,
                "payload_sha256": order_state_sha,
            }
            connection.execute(
                "INSERT INTO order_book_state VALUES(1,0,?,?,?)",
                (
                    order_state_payload,
                    order_state_sha,
                    self.authenticator.sign("order-book-state", order_state_body),
                ),
            )
            anchor_state_body = {
                "anchor_event_sequence": 0,
                "last_applied_operation_sequence": 0,
                "payload_sha256": order_state_sha,
            }
            connection.execute(
                "INSERT INTO order_anchor_state VALUES(1,0,0,?,?,?)",
                (
                    order_state_payload,
                    order_state_sha,
                    self.authenticator.sign("order-anchor-state", anchor_state_body),
                ),
            )

        self._run_write(initialize)
        self._verify_existing_binding(supplied_evidence)

    def _verify_evidence_tuple(
        self, evidence: Sequence[AuthenticatedStateEvidence]
    ) -> tuple[AuthenticatedStateEvidence, ...]:
        ordered = tuple(sorted(evidence, key=lambda item: item.node))
        if len({item.node for item in ordered}) != len(ordered):
            raise AuthenticationError("duplicate authenticated state evidence node")
        for item in ordered:
            _bounded_payload(
                item.payload, self.limits.max_state_bytes, "state evidence payload"
            )
            item.verify(
                self.authenticator,
                self.source,
                self.roster,
                self.symbol,
                self.timeframe,
            )
            try:
                self._invoke_engine("validate_state_evidence", item, item.payload)
            except Exception as exc:
                raise IntegrityError(
                    f"state engine rejected authenticated evidence for {item.node}"
                ) from exc
        return ordered

    @staticmethod
    def _evidence_from_dict(value: Mapping[str, Any]) -> AuthenticatedStateEvidence:
        fields = {
            "node",
            "symbol",
            "timeframe",
            "mode",
            "identity_sha256",
            "registry_hash",
            "state_schema",
            "transition_schema",
            "payload_sha256",
            "source_epoch_sha256",
            "roster_sha256",
            "payload",
            "mac",
        }
        if set(value) != fields:
            raise IntegrityError("stored state evidence shape mismatch")
        string_fields = fields - {"payload"}
        arguments = {name: str(value[name]) for name in string_fields}
        arguments["payload"] = value["payload"]
        return AuthenticatedStateEvidence(**arguments)

    def _load_meta(self, connection: sqlite3.Connection) -> dict[str, Any]:
        row = connection.execute(
            "SELECT payload, mac FROM runtime_meta WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise IntegrityError("runtime binding is missing")
        body = _decode_payload(
            row["payload"], self.limits.max_json_bytes, "runtime binding"
        )
        self.authenticator.verify("runtime-meta", body, row["mac"])
        return body

    def _verify_existing_binding(
        self, supplied_evidence: Sequence[AuthenticatedStateEvidence]
    ) -> None:
        def verify(connection: sqlite3.Connection) -> None:
            body = self._load_meta(connection)
            expected_fields = {
                "schema_version": RUNTIME_SCHEMA_VERSION,
                "source": self.source.canonical(),
                "source_epoch_sha256": self.source.identity_sha256,
                "cadence_authority_registry_sha256": (
                    self.cadence_authority_registry_sha256
                ),
                "reducer_authority_registry_sha256": (
                    self.reducer_authority_registry_sha256
                ),
                "broker_result_authority_registry_sha256": (
                    self.broker_result_authority_registry_sha256
                ),
                "roster": self.roster.canonical(),
                "roster_sha256": self.roster.identity_sha256,
                "registry_hash": DEFAULT_REGISTRY_HASH,
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "engine_identity": dict(self.engine_identity),
                "engine_declared_sha256": self.engine_declared_sha256,
                "engine_implementation_sha256": self.engine_implementation_sha256,
                "engine_instance_configuration_sha256": (
                    self.engine_instance_configuration_sha256
                ),
                "authenticated_reducer_release": self.reducer_release.canonical(),
                "authenticated_reducer_release_sha256": (self.reducer_release_sha256),
                "engine_sha256": self.engine_sha256,
                "execution_account_scope_sha256": (self.execution_account_scope_sha256),
                "limits": self.limits.canonical(),
                "storage_reservation": self.storage_reservation.canonical(),
            }
            for key, expected in expected_fields.items():
                if body.get(key) != expected:
                    raise BindingMismatchError(f"runtime binding mismatch: {key}")
            stored = self._verify_evidence_tuple(
                tuple(
                    self._evidence_from_dict(item)
                    for item in body.get("state_evidence", ())
                )
            )
            if supplied_evidence:
                supplied = self._verify_evidence_tuple(supplied_evidence)
                if [item.canonical() for item in supplied] != [
                    item.canonical() for item in stored
                ]:
                    raise BindingMismatchError("supplied state evidence changed")

        self._run_read(verify)

    def _stored_evidence(
        self, connection: sqlite3.Connection
    ) -> tuple[AuthenticatedStateEvidence, ...]:
        meta = self._load_meta(connection)
        evidence = tuple(
            self._evidence_from_dict(item) for item in meta.get("state_evidence", ())
        )
        return self._verify_evidence_tuple(evidence)

    def _state_resolutions(
        self, connection: sqlite3.Connection
    ) -> tuple[tuple[str, StateResolution], ...]:
        evidence = {
            item.node: item.verify(
                self.authenticator,
                self.source,
                self.roster,
                self.symbol,
                self.timeframe,
            )
            for item in self._stored_evidence(connection)
        }
        decisions: list[tuple[str, StateResolution]] = []
        for winner in self.roster.winners:
            if winner.symbol != self.symbol or winner.timeframe != self.timeframe:
                continue
            resolution = DEFAULT_AUTHORITY.resolve_cell(
                winner.strategy_name,
                parameters=winner.parameters,
                requested_features=winner.requested_features,
                timeframe=winner.timeframe,
                include_regime_controller=winner.include_regime_controller,
                initial_error_bound=winner.initial_error_bound,
                tolerance=winner.tolerance,
                state_evidence=evidence,
            )
            decisions.append((winner.cell_id, resolution))
        return tuple(decisions)

    @staticmethod
    def _resolution_payload(
        decisions: Sequence[tuple[str, StateResolution]],
    ) -> list[dict[str, Any]]:
        return _json_safe(
            [
                {"cell_id": cell_id, "resolution": resolution.to_dict()}
                for cell_id, resolution in decisions
            ]
        )

    def _require_admissible_resolutions(
        self, connection: sqlite3.Connection
    ) -> tuple[tuple[str, StateResolution], ...]:
        decisions = self._state_resolutions(connection)
        rejected = [
            cell_id for cell_id, resolution in decisions if not resolution.admissible
        ]
        if rejected:
            raise RecoveryRequiredError(
                "state-contract resolution quarantined cells: " + ",".join(rejected)
            )
        return decisions

    def _runtime_state_body(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "bar_sequence": int(row["bar_sequence"]),
            "state_sha256": str(row["state_sha256"]),
            "last_closed_bar_time": row["last_closed_bar_time"],
            "last_bar_sha256": row["last_bar_sha256"],
            "last_source_sequence": int(row["last_source_sequence"]),
            "last_source_session_id": row["last_source_session_id"],
            "last_source_connection_id": row["last_source_connection_id"],
            "last_source_phase_seconds": row["last_source_phase_seconds"],
        }

    def _load_runtime_state(
        self, connection: sqlite3.Connection
    ) -> tuple[sqlite3.Row, Mapping[str, Any]]:
        row = connection.execute(
            "SELECT * FROM runtime_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise IntegrityError("runtime state row is missing")
        body = self._runtime_state_body(row)
        self.authenticator.verify("runtime-state", body, row["mac"])
        state = _decode_payload(
            row["state_payload"], self.limits.max_state_bytes, "runtime state"
        )
        if (
            hashlib.sha256(bytes(row["state_payload"])).hexdigest()
            != row["state_sha256"]
        ):
            raise IntegrityError("runtime state payload digest mismatch")
        _normal_sha256(row["state_sha256"], "state_sha256")
        if int(row["bar_sequence"]) < 0:
            raise IntegrityError("runtime state has a negative sequence")
        source_sequence = int(row["last_source_sequence"])
        if source_sequence < 0:
            raise IntegrityError("runtime state has a negative source sequence")
        if int(row["bar_sequence"]) == 0:
            if source_sequence != 0 or any(
                row[name] is not None
                for name in (
                    "last_source_session_id",
                    "last_source_connection_id",
                    "last_source_phase_seconds",
                )
            ):
                raise IntegrityError("pristine runtime carries source-tail metadata")
        elif source_sequence <= 0 or any(
            row[name] is None
            for name in (
                "last_source_session_id",
                "last_source_connection_id",
                "last_source_phase_seconds",
            )
        ):
            raise IntegrityError("runtime source-tail metadata is incomplete")
        return row, state

    @staticmethod
    def _bar_head_body(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "sequence": int(row["sequence"]),
            "event_hash": str(row["event_hash"]),
            "anchor_sequence": int(row["anchor_sequence"]),
            "anchor_hash": str(row["anchor_hash"]),
            "current_segment": int(row["current_segment"]),
            "segment_events": int(row["segment_events"]),
            "segment_bytes": int(row["segment_bytes"]),
            "retained_events": int(row["retained_events"]),
            "retained_bytes": int(row["retained_bytes"]),
        }

    def _load_bar_head(self, connection: sqlite3.Connection) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM bar_head WHERE singleton=1").fetchone()
        if row is None:
            raise IntegrityError("bar journal head is missing")
        body = self._bar_head_body(row)
        self.authenticator.verify("bar-head", body, row["mac"])
        for name in ("event_hash", "anchor_hash"):
            _normal_sha256(row[name], name)
        nonnegative = (
            "sequence",
            "anchor_sequence",
            "segment_events",
            "segment_bytes",
            "retained_events",
            "retained_bytes",
        )
        if any(int(row[name]) < 0 for name in nonnegative):
            raise IntegrityError("bar head contains a negative counter")
        if int(row["anchor_sequence"]) > int(row["sequence"]):
            raise IntegrityError("bar anchor is ahead of the journal head")
        return row

    @staticmethod
    def _order_meta_body(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "writer_epoch": int(row["writer_epoch"]),
            "max_operation_sequence": int(row["max_operation_sequence"]),
            "max_client_sequence": int(row["max_client_sequence"]),
            "max_broker_execution_sequence": int(row["max_broker_execution_sequence"]),
            "retired_broker_execution_sequence": int(
                row["retired_broker_execution_sequence"]
            ),
            "last_broker_execution_sha256": str(row["last_broker_execution_sha256"]),
            "retired_through_sequence": int(row["retired_through_sequence"]),
            "event_sequence": int(row["event_sequence"]),
            "event_hash": str(row["event_hash"]),
            "anchor_event_sequence": int(row["anchor_event_sequence"]),
            "anchor_event_hash": str(row["anchor_event_hash"]),
        }

    def _load_order_meta(self, connection: sqlite3.Connection) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM order_meta WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise IntegrityError("order journal metadata is missing")
        body = self._order_meta_body(row)
        self.authenticator.verify("order-meta", body, row["mac"])
        if int(row["writer_epoch"]) <= 0:
            raise IntegrityError("order writer epoch is invalid")
        _normal_sha256(row["event_hash"], "order event hash")
        _normal_sha256(row["anchor_event_hash"], "order anchor hash")
        _normal_sha256(
            row["last_broker_execution_sha256"], "last broker execution hash"
        )
        if int(row["retired_through_sequence"]) > int(row["max_operation_sequence"]):
            raise IntegrityError(
                "order retirement high-water exceeds allocation high-water"
            )
        if int(row["anchor_event_sequence"]) > int(row["event_sequence"]):
            raise IntegrityError("order event anchor is ahead of its head")
        if int(row["retired_broker_execution_sequence"]) > int(
            row["max_broker_execution_sequence"]
        ):
            raise IntegrityError("retired broker execution high-water is invalid")
        return row

    def _load_order_book_state(
        self, connection: sqlite3.Connection
    ) -> tuple[sqlite3.Row, Mapping[str, Any]]:
        row = connection.execute(
            "SELECT * FROM order_book_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise IntegrityError("order book state is missing")
        body = {
            "applied_operation_sequence": int(row["applied_operation_sequence"]),
            "payload_sha256": str(row["payload_sha256"]),
        }
        self.authenticator.verify("order-book-state", body, row["mac"])
        state = _decode_payload(
            row["payload"], self.limits.max_state_bytes, "order book state"
        )
        if _canonical_sha256(state) != row["payload_sha256"]:
            raise IntegrityError("order book state digest mismatch")
        return row, state

    def _load_order_anchor_state(
        self, connection: sqlite3.Connection
    ) -> tuple[sqlite3.Row, Mapping[str, Any]]:
        row = connection.execute(
            "SELECT * FROM order_anchor_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise IntegrityError("order chain anchor state is missing")
        body = {
            "anchor_event_sequence": int(row["anchor_event_sequence"]),
            "last_applied_operation_sequence": int(
                row["last_applied_operation_sequence"]
            ),
            "payload_sha256": str(row["payload_sha256"]),
        }
        self.authenticator.verify("order-anchor-state", body, row["mac"])
        state = _decode_payload(
            row["payload"], self.limits.max_state_bytes, "order anchor state"
        )
        if _canonical_sha256(state) != row["payload_sha256"]:
            raise IntegrityError("order anchor state digest mismatch")
        return row, state

    @staticmethod
    def _segment_body(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "segment_id": int(row["segment_id"]),
            "first_sequence": int(row["first_sequence"]),
            "last_sequence": int(row["last_sequence"]),
            "event_count": int(row["event_count"]),
            "byte_count": int(row["byte_count"]),
            "first_previous_hash": str(row["first_previous_hash"]),
            "last_event_hash": str(row["last_event_hash"]),
        }

    def _verify_segment_rows(self, connection: sqlite3.Connection) -> None:
        segments = connection.execute(
            "SELECT * FROM bar_segments ORDER BY segment_id LIMIT ?",
            (self.limits.max_bar_segments + 1,),
        ).fetchall()
        if len(segments) > self.limits.max_bar_segments:
            raise CapacityExceededError("stored bar segment count exceeds cap")
        for row in segments:
            body = self._segment_body(row)
            self.authenticator.verify("bar-segment", body, row["mac"])
            if int(row["event_count"]) > self.limits.max_segment_events:
                raise CapacityExceededError("stored segment event count exceeds cap")
            if int(row["byte_count"]) > self.limits.max_segment_bytes:
                raise CapacityExceededError("stored segment byte count exceeds cap")
            aggregate = connection.execute(
                "SELECT MIN(sequence), MAX(sequence), COUNT(*), "
                "COALESCE(SUM(LENGTH(payload)),0) FROM bar_events WHERE segment_id=?",
                (row["segment_id"],),
            ).fetchone()
            expected = (
                int(row["first_sequence"]),
                int(row["last_sequence"]),
                int(row["event_count"]),
                int(row["byte_count"]),
            )
            actual = tuple(int(value) for value in aggregate)
            if actual != expected:
                raise IntegrityError("bar segment metadata does not match its rows")
            endpoints = connection.execute(
                "SELECT previous_hash, event_hash FROM bar_events "
                "WHERE segment_id=? ORDER BY sequence LIMIT ?",
                (row["segment_id"], self.limits.max_segment_events + 1),
            ).fetchall()
            if not endpoints:
                raise IntegrityError("empty retained bar segment")
            if endpoints[0]["previous_hash"] != row["first_previous_hash"]:
                raise IntegrityError("bar segment first hash mismatch")
            if endpoints[-1]["event_hash"] != row["last_event_hash"]:
                raise IntegrityError("bar segment last hash mismatch")

    def _load_active_checkpoint(
        self, connection: sqlite3.Connection
    ) -> Optional[tuple[CheckpointSnapshot, dict[str, Any]]]:
        pointer = connection.execute(
            "SELECT * FROM active_checkpoint WHERE singleton=1"
        ).fetchone()
        if pointer is None:
            return None
        pointer_body = _decode_payload(
            pointer["payload"], self.limits.max_json_bytes, "checkpoint pointer"
        )
        self.authenticator.verify("checkpoint-pointer", pointer_body, pointer["mac"])
        expected_pointer = {
            "slot": pointer["slot"],
            "generation": int(pointer["generation"]),
            "checkpoint_sha256": pointer["checkpoint_sha256"],
        }
        if pointer_body != expected_pointer:
            raise IntegrityError("checkpoint pointer columns and payload disagree")
        slot = connection.execute(
            "SELECT * FROM checkpoint_slots WHERE slot=?", (pointer["slot"],)
        ).fetchone()
        if slot is None:
            raise IntegrityError("active checkpoint slot is missing")
        if (
            int(slot["generation"]) != int(pointer["generation"])
            or slot["checkpoint_sha256"] != pointer["checkpoint_sha256"]
        ):
            raise IntegrityError("active checkpoint pointer/slot mismatch")
        body = _decode_payload(
            slot["payload"],
            self.limits.max_state_bytes + self.limits.max_json_bytes,
            "checkpoint",
        )
        self.authenticator.verify("checkpoint", body, slot["mac"])
        if (
            hashlib.sha256(bytes(slot["payload"])).hexdigest()
            != slot["checkpoint_sha256"]
        ):
            raise IntegrityError("checkpoint digest mismatch")
        if body.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise IntegrityError("checkpoint schema mismatch")
        bindings = {
            "registry_hash": DEFAULT_REGISTRY_HASH,
            "source_epoch_sha256": self.source.identity_sha256,
            "roster_sha256": self.roster.identity_sha256,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "engine_sha256": self.engine_sha256,
            "storage_reservation_sha256": _canonical_sha256(
                self.storage_reservation.canonical()
            ),
            "state_evidence_sha256": _canonical_sha256(
                [item.canonical() for item in self._stored_evidence(connection)]
            ),
        }
        for name, expected in bindings.items():
            if body.get(name) != expected:
                raise BindingMismatchError(f"checkpoint binding mismatch: {name}")
        if int(body.get("generation", -1)) != int(slot["generation"]):
            raise IntegrityError("checkpoint generation mismatch")
        predecessor = body.get("predecessor_checkpoint_sha256")
        if int(slot["generation"]) == 1:
            if predecessor != ZERO_HASH:
                raise IntegrityError(
                    "first checkpoint predecessor is not the zero anchor"
                )
        else:
            other_slot = "B" if slot["slot"] == "A" else "A"
            previous = connection.execute(
                "SELECT * FROM checkpoint_slots WHERE slot=?", (other_slot,)
            ).fetchone()
            if (
                previous is None
                or int(previous["generation"]) != int(slot["generation"]) - 1
                or previous["checkpoint_sha256"] != predecessor
            ):
                raise IntegrityError("A/B checkpoint predecessor chain is broken")
            previous_body = _decode_payload(
                previous["payload"],
                self.limits.max_state_bytes + self.limits.max_json_bytes,
                "previous checkpoint",
            )
            self.authenticator.verify("checkpoint", previous_body, previous["mac"])
            if (
                hashlib.sha256(bytes(previous["payload"])).hexdigest()
                != previous["checkpoint_sha256"]
            ):
                raise IntegrityError("previous checkpoint digest mismatch")
        decisions = self._resolution_payload(self._state_resolutions(connection))
        if body.get("state_resolutions") != decisions:
            raise IntegrityError("checkpoint state-resolution decision mismatch")
        state = body.get("state")
        if _canonical_sha256(state) != body.get("state_sha256"):
            raise IntegrityError("checkpoint state hash mismatch")
        snapshot = CheckpointSnapshot(
            generation=int(body["generation"]),
            slot=str(slot["slot"]),
            checkpoint_sha256=str(slot["checkpoint_sha256"]),
            state_sha256=str(body["state_sha256"]),
            bar_sequence=int(body["bar_sequence"]),
            bar_hash=str(body["bar_hash"]),
            last_closed_bar_time=body.get("last_closed_bar_time"),
        )
        return snapshot, body

    def _verify_bar_chain_and_replay(
        self,
        connection: sqlite3.Connection,
        checkpoint: tuple[CheckpointSnapshot, dict[str, Any]],
    ) -> None:
        snapshot, checkpoint_body = checkpoint
        head = self._load_bar_head(connection)
        state_row, durable_state = self._load_runtime_state(connection)
        if int(head["anchor_sequence"]) != snapshot.bar_sequence:
            raise IntegrityError("bar anchor sequence does not match checkpoint")
        if head["anchor_hash"] != snapshot.bar_hash:
            raise IntegrityError("bar anchor hash does not match checkpoint")
        if int(head["sequence"]) != int(state_row["bar_sequence"]):
            raise IntegrityError("bar head and runtime state sequence differ")
        retained_count, retained_bytes = connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)),0) FROM bar_events"
        ).fetchone()
        if int(retained_count) != int(head["retained_events"]):
            raise IntegrityError("retained bar row count mismatch")
        if int(retained_bytes) != int(head["retained_bytes"]):
            raise IntegrityError("retained bar byte count mismatch")
        if int(retained_count) > self.limits.max_bar_events:
            raise CapacityExceededError("stored bar event count exceeds cap")
        if int(retained_bytes) > self.limits.max_bar_bytes:
            raise CapacityExceededError("stored bar payload bytes exceed cap")
        self._verify_segment_rows(connection)
        replay_state: Mapping[str, Any] = _json_safe(checkpoint_body["state"])
        previous_hash = snapshot.bar_hash
        expected_sequence = snapshot.bar_sequence + 1
        last_close = checkpoint_body.get("last_closed_bar_time")
        last_source_sequence = int(checkpoint_body.get("last_source_sequence", -1))
        last_source_session_id = checkpoint_body.get("last_source_session_id")
        last_source_connection_id = checkpoint_body.get("last_source_connection_id")
        last_source_phase_seconds = checkpoint_body.get("last_source_phase_seconds")
        cursor = connection.execute("SELECT * FROM bar_events ORDER BY sequence")
        seen = 0
        while True:
            batch = cursor.fetchmany(self.limits.scan_batch_rows)
            if not batch:
                break
            for row in batch:
                seen += 1
                self._verification_metrics["replayed_bar_events"] += 1
                if seen > self.limits.max_bar_events:
                    raise CapacityExceededError("bar replay scan exceeded cap")
                if int(row["sequence"]) != expected_sequence:
                    raise IntegrityError("bar journal contains a gap or reorder")
                if row["previous_hash"] != previous_hash:
                    raise IntegrityError("bar journal previous hash mismatch")
                bar = _decode_payload(
                    row["payload"], self.limits.max_json_bytes, "bar event"
                )
                if _canonical_sha256(bar) != row["bar_sha256"]:
                    raise IntegrityError("bar payload digest mismatch")
                cadence = self._cadence_receipt_from_value(bar.get("cadence_receipt"))
                cadence.verify(
                    self.cadence_public_keys,
                    self.source,
                    self.symbol,
                    self.timeframe,
                )
                if cadence.canonical() != bar.get("cadence_receipt"):
                    raise AuthenticationError(
                        "persisted cadence receipt canonical form changed"
                    )
                if int(bar.get("source_phase_seconds", -1)) != int(
                    cadence.observed_phase_seconds
                ):
                    raise AuthenticationError(
                        "persisted bar phase disagrees with cadence receipt"
                    )
                if row["close_time"] != bar.get("close_time"):
                    raise IntegrityError("bar close time column mismatch")
                for column in (
                    "source_sequence",
                    "source_session_id",
                    "source_connection_id",
                ):
                    if row[column] != bar.get(column):
                        raise IntegrityError(f"bar {column} column mismatch")
                if last_close is not None and _timestamp_value(
                    row["close_time"]
                ) <= _timestamp_value(last_close):
                    raise IntegrityError("bar close times are not strictly increasing")
                self._validate_source_continuity(
                    bar,
                    last_close_time=last_close,
                    last_source_sequence=last_source_sequence,
                    last_source_session_id=last_source_session_id,
                    last_source_connection_id=last_source_connection_id,
                    last_source_phase_seconds=last_source_phase_seconds,
                )
                event_body = {
                    "schema_version": BAR_EVENT_SCHEMA_VERSION,
                    "sequence": int(row["sequence"]),
                    "segment_id": int(row["segment_id"]),
                    "previous_hash": str(row["previous_hash"]),
                    "bar_sha256": str(row["bar_sha256"]),
                    "close_time": str(row["close_time"]),
                    "source_sequence": int(row["source_sequence"]),
                    "source_session_id": str(row["source_session_id"]),
                    "source_connection_id": str(row["source_connection_id"]),
                    "post_state_sha256": str(row["post_state_sha256"]),
                }
                event_hash = _canonical_sha256(event_body)
                if event_hash != row["event_hash"]:
                    raise IntegrityError("bar event hash mismatch")
                self.authenticator.verify("bar-event", event_body, row["mac"])
                replay_state = self._invoke_engine(
                    "apply_closed_bar", replay_state, bar
                )
                _, replay_sha = _bounded_payload(
                    replay_state, self.limits.max_state_bytes, "replayed runtime state"
                )
                if replay_sha != row["post_state_sha256"]:
                    raise IntegrityError("deterministic state replay diverged")
                previous_hash = event_hash
                expected_sequence += 1
                last_close = row["close_time"]
                last_source_sequence = int(row["source_sequence"])
                last_source_session_id = str(row["source_session_id"])
                last_source_connection_id = str(row["source_connection_id"])
                last_source_phase_seconds = int(bar["source_phase_seconds"])
        if expected_sequence - 1 != int(head["sequence"]):
            raise IntegrityError("bar journal head sequence mismatch")
        if previous_hash != head["event_hash"]:
            raise IntegrityError("bar journal head hash mismatch")
        if _canonical_sha256(replay_state) != state_row["state_sha256"]:
            raise IntegrityError("replayed state differs from durable runtime state")
        if _canonical_bytes(replay_state) != bytes(state_row["state_payload"]):
            raise IntegrityError("replayed state payload differs from durable state")
        if last_close != state_row["last_closed_bar_time"]:
            raise IntegrityError("replayed last-close marker mismatch")
        if (
            last_source_sequence != int(state_row["last_source_sequence"])
            or last_source_session_id != state_row["last_source_session_id"]
            or last_source_connection_id != state_row["last_source_connection_id"]
            or last_source_phase_seconds != state_row["last_source_phase_seconds"]
        ):
            raise IntegrityError("replayed source-tail metadata mismatch")

    @staticmethod
    def _operation_body(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "operation_sequence": int(row["operation_sequence"]),
            "operation_id": str(row["operation_id"]),
            "client_request_id": str(row["client_request_id"]),
            "action": str(row["action"]),
            "phase": str(row["phase"]),
            "request_sha256": str(row["request_sha256"]),
            "intent_event_hash": str(row["intent_event_hash"]),
            "result_sha256": row["result_sha256"],
            "result_event_hash": row["result_event_hash"],
            "applied_event_hash": row["applied_event_hash"],
            "local_state_sha256": row["local_state_sha256"],
        }

    @staticmethod
    def _authenticated_result_from_dict(
        value: Mapping[str, Any],
    ) -> AuthenticatedOrderResult:
        fields = {
            "schema_version",
            "operation_id",
            "operation_sequence",
            "intent_event_hash",
            "request_sha256",
            "result_outcome",
            "broker_execution_id",
            "broker_execution_sequence",
            "broker_highwater_receipt",
            "reconciliation_receipt_id",
            "reconciliation_method",
            "reconciled_at",
            "result",
            "result_sha256",
            "authority_key_id",
            "signature_base64",
        }
        if set(value) != fields:
            raise IntegrityError("stored broker result evidence shape mismatch")
        return AuthenticatedOrderResult(
            operation_id=str(value["operation_id"]),
            operation_sequence=int(value["operation_sequence"]),
            intent_event_hash=str(value["intent_event_hash"]),
            request_sha256=str(value["request_sha256"]),
            result_outcome=str(value["result_outcome"]),
            broker_execution_id=(
                str(value["broker_execution_id"])
                if value["broker_execution_id"] is not None
                else None
            ),
            broker_execution_sequence=(
                int(value["broker_execution_sequence"])
                if value["broker_execution_sequence"] is not None
                else None
            ),
            broker_highwater_receipt=(
                value["broker_highwater_receipt"]
                if value["broker_highwater_receipt"] is not None
                else None
            ),
            reconciliation_receipt_id=str(value["reconciliation_receipt_id"]),
            reconciliation_method=str(value["reconciliation_method"]),
            reconciled_at=str(value["reconciled_at"]),
            result=value["result"],
            result_sha256=str(value["result_sha256"]),
            authority_key_id=str(value["authority_key_id"]),
            signature_base64=str(value["signature_base64"]),
            schema_version=str(value["schema_version"]),
        )

    @staticmethod
    def _broker_execution_claim_from_value(value: Any) -> BrokerExecutionClaim:
        fields = {
            "authority_identity_sha256",
            "claim_id",
            "account_scope_sha256",
            "broker_execution_id",
            "broker_execution_sequence",
            "claim_sequence",
            "stream_identity_sha256",
            "operation_id",
            "operation_sequence",
            "payload_sha256",
            "fencing_token",
            "receipt_mac",
        }
        if isinstance(value, Mapping):
            supplied = dict(value)
        else:
            try:
                supplied = {name: getattr(value, name) for name in fields}
            except AttributeError as exc:
                raise AuthenticationError(
                    "broker execution authority returned an incomplete claim"
                ) from exc
        if set(supplied) != fields:
            raise AuthenticationError("broker execution claim shape mismatch")
        return BrokerExecutionClaim(
            authority_identity_sha256=str(supplied["authority_identity_sha256"]),
            claim_id=str(supplied["claim_id"]),
            account_scope_sha256=str(supplied["account_scope_sha256"]),
            broker_execution_id=str(supplied["broker_execution_id"]),
            broker_execution_sequence=int(supplied["broker_execution_sequence"]),
            claim_sequence=int(supplied["claim_sequence"]),
            stream_identity_sha256=str(supplied["stream_identity_sha256"]),
            operation_id=str(supplied["operation_id"]),
            operation_sequence=int(supplied["operation_sequence"]),
            payload_sha256=str(supplied["payload_sha256"]),
            fencing_token=int(supplied["fencing_token"]),
            receipt_mac=str(supplied["receipt_mac"]),
        )

    def _broker_execution_claim_arguments(
        self, result: AuthenticatedOrderResult
    ) -> dict[str, Any]:
        if (
            result.result_outcome != "EXECUTED"
            or result.broker_execution_id is None
            or result.broker_execution_sequence is None
        ):
            raise OrderTransitionError(
                "only EXECUTED results can enter the broker execution authority"
            )
        return {
            "account_scope_sha256": self.execution_account_scope_sha256,
            "broker_execution_id": result.broker_execution_id,
            "broker_execution_sequence": result.broker_execution_sequence,
            "operation_id": result.operation_id,
            "operation_sequence": result.operation_sequence,
            "payload_sha256": _canonical_sha256(result.canonical()),
            "broker_highwater_receipt": result.broker_highwater_receipt,
        }

    def _validate_broker_execution_claim(
        self,
        claim: Any,
        result: AuthenticatedOrderResult,
    ) -> BrokerExecutionClaim:
        normalized = self._broker_execution_claim_from_value(claim)
        expected = self._broker_execution_claim_arguments(result)
        comparisons = {
            "authority_identity_sha256": (
                self.storage_reservation.authority_identity_sha256
            ),
            "account_scope_sha256": expected["account_scope_sha256"],
            "broker_execution_id": expected["broker_execution_id"],
            "broker_execution_sequence": expected["broker_execution_sequence"],
            "stream_identity_sha256": self.stream_identity_sha256,
            "operation_id": expected["operation_id"],
            "operation_sequence": expected["operation_sequence"],
            "payload_sha256": expected["payload_sha256"],
        }
        for name, expected_value in comparisons.items():
            if getattr(normalized, name) != expected_value:
                raise BindingMismatchError(f"broker execution claim mismatch: {name}")
        return normalized

    def _claim_broker_execution(
        self, result: AuthenticatedOrderResult
    ) -> BrokerExecutionClaim:
        arguments = self._broker_execution_claim_arguments(result)
        try:
            claim = self.storage_authority.claim_broker_execution(
                self.storage_reservation, **arguments
            )
        except Exception as exc:
            # The canonical lifecycle authority keeps its exception hierarchy
            # independent of this standalone module.  A CAS mismatch emitted
            # by this exact claim operation means the execution identity was
            # concurrently or previously assigned; other authority failures
            # (capacity, authentication, recovery, I/O) retain their type.
            if type(exc).__name__ == "LifecycleCASMismatch":
                raise OrderTransitionError(
                    "broker execution is already claimed by another operation"
                ) from exc
            raise
        normalized = self._validate_broker_execution_claim(claim, result)
        self.storage_authority.verify_broker_execution_claim(
            claim, self.storage_reservation, **arguments
        )
        return normalized

    def _verify_broker_execution_claim(
        self,
        claim: BrokerExecutionClaim,
        result: AuthenticatedOrderResult,
    ) -> None:
        normalized = self._validate_broker_execution_claim(claim, result)
        arguments = self._broker_execution_claim_arguments(result)
        self.storage_authority.verify_broker_execution_claim(
            normalized, self.storage_reservation, **arguments
        )

    def _authenticated_result_record_from_dict(
        self, value: Mapping[str, Any]
    ) -> tuple[AuthenticatedOrderResult, Optional[BrokerExecutionClaim]]:
        if set(value) != {"authenticated_result", "broker_execution_claim"}:
            raise IntegrityError("stored authenticated result record shape mismatch")
        authenticated = value["authenticated_result"]
        if not isinstance(authenticated, Mapping):
            raise IntegrityError("stored authenticated result evidence is malformed")
        result = self._authenticated_result_from_dict(authenticated)
        raw_claim = value["broker_execution_claim"]
        if result.result_outcome == "EXECUTED":
            if raw_claim is None:
                raise IntegrityError("EXECUTED result lacks an authority claim")
            claim = self._broker_execution_claim_from_value(raw_claim)
            self._verify_broker_execution_claim(claim, result)
            return result, claim
        if raw_claim is not None:
            raise IntegrityError(
                "REJECTED_NO_EFFECT result incorrectly carries an execution claim"
            )
        return result, None

    def _verify_order_chain(
        self, connection: sqlite3.Connection
    ) -> tuple[OrderRecovery, ...]:
        meta = self._load_order_meta(connection)
        anchor_row, replay_order_state = self._load_order_anchor_state(connection)
        global_state_row, global_order_state = self._load_order_book_state(connection)
        if int(anchor_row["anchor_event_sequence"]) != int(
            meta["anchor_event_sequence"]
        ):
            raise IntegrityError("order state anchor/event anchor mismatch")
        last_applied_sequence = int(anchor_row["last_applied_operation_sequence"])
        previous_hash = str(meta["anchor_event_hash"])
        expected_sequence = int(meta["anchor_event_sequence"]) + 1
        cursor = connection.execute("SELECT * FROM order_events ORDER BY sequence")
        seen = 0
        while True:
            batch = cursor.fetchmany(self.limits.scan_batch_rows)
            if not batch:
                break
            for row in batch:
                seen += 1
                self._verification_metrics["replayed_order_events"] += 1
                if seen > self.limits.max_order_events:
                    raise CapacityExceededError("order event scan exceeded cap")
                if int(row["sequence"]) != expected_sequence:
                    raise IntegrityError(
                        "order event journal contains a gap or reorder"
                    )
                if row["previous_hash"] != previous_hash:
                    raise IntegrityError("order event previous hash mismatch")
                payload = _decode_payload(
                    row["payload"], self.limits.max_json_bytes, "order event"
                )
                event_body = {
                    "schema_version": ORDER_EVENT_SCHEMA_VERSION,
                    "sequence": int(row["sequence"]),
                    "operation_sequence": int(row["operation_sequence"]),
                    "operation_id": str(row["operation_id"]),
                    "phase": str(row["phase"]),
                    "previous_hash": str(row["previous_hash"]),
                    "payload_sha256": _canonical_sha256(payload),
                }
                event_hash = _canonical_sha256(event_body)
                if event_hash != row["event_hash"]:
                    raise IntegrityError("order event hash mismatch")
                self.authenticator.verify("order-event", event_body, row["mac"])
                if row["phase"] == "APPLIED":
                    operation = connection.execute(
                        "SELECT * FROM order_operations WHERE operation_sequence=?",
                        (row["operation_sequence"],),
                    ).fetchone()
                    if operation is None or operation["phase"] != "APPLIED":
                        raise IntegrityError("APPLIED event lacks an APPLIED operation")
                    self.authenticator.verify(
                        "order-operation",
                        self._operation_body(operation),
                        operation["mac"],
                    )
                    request = _decode_payload(
                        operation["request_payload"],
                        self.limits.max_json_bytes,
                        "order request",
                    )
                    result_evidence_payload = _decode_payload(
                        operation["result_payload"],
                        self.limits.max_json_bytes,
                        "authenticated order result",
                    )
                    result_evidence, _ = self._authenticated_result_record_from_dict(
                        result_evidence_payload
                    )
                    result_evidence.verify(
                        self.broker_result_public_keys,
                        self._intent_receipt_from_row(operation),
                    )
                    replay_order_state = self._invoke_engine(
                        "apply_order_result",
                        replay_order_state,
                        str(operation["action"]),
                        request,
                        result_evidence.result,
                    )
                    _, replay_sha = _bounded_payload(
                        replay_order_state,
                        self.limits.max_state_bytes,
                        "replayed order state",
                    )
                    if payload != {
                        "local_state_sha256": replay_sha,
                        "result_event_hash": operation["result_event_hash"],
                    }:
                        raise IntegrityError("APPLIED event state transition diverged")
                    local = connection.execute(
                        "SELECT * FROM order_local_state WHERE operation_sequence=?",
                        (row["operation_sequence"],),
                    ).fetchone()
                    if local is None or local["payload_sha256"] != replay_sha:
                        raise IntegrityError(
                            "APPLIED event/local-state snapshot mismatch"
                        )
                    if _canonical_bytes(replay_order_state) != bytes(local["payload"]):
                        raise IntegrityError("replayed order state payload diverged")
                    last_applied_sequence = int(row["operation_sequence"])
                previous_hash = event_hash
                expected_sequence += 1
        if expected_sequence - 1 != int(meta["event_sequence"]):
            raise IntegrityError("order event head sequence mismatch")
        if previous_hash != meta["event_hash"]:
            raise IntegrityError("order event head hash mismatch")
        operations = connection.execute(
            "SELECT * FROM order_operations ORDER BY operation_sequence LIMIT ?",
            (self.limits.max_order_operations + 1,),
        ).fetchall()
        if len(operations) > self.limits.max_order_operations:
            raise CapacityExceededError("stored order operation count exceeds cap")
        pending: list[OrderRecovery] = []
        last_operation = int(meta["retired_through_sequence"])
        last_broker_execution = int(meta["retired_broker_execution_sequence"])
        last_broker_id_sha: Optional[str] = None
        for row in operations:
            sequence = int(row["operation_sequence"])
            if sequence != last_operation + 1:
                raise IntegrityError("order operation detail contains a gap or reorder")
            if sequence > int(meta["max_operation_sequence"]):
                raise IntegrityError("order detail exceeds allocation high-water")
            last_operation = sequence
            self.authenticator.verify(
                "order-operation", self._operation_body(row), row["mac"]
            )
            phase_rows = connection.execute(
                "SELECT phase,event_hash,payload FROM order_events WHERE operation_sequence=? "
                "ORDER BY sequence",
                (sequence,),
            ).fetchall()
            expected_phases = {
                "INTENT": ("INTENT",),
                "RESULT": ("INTENT", "RESULT"),
                "APPLIED": ("INTENT", "RESULT", "APPLIED"),
            }[str(row["phase"])]
            if tuple(item["phase"] for item in phase_rows) != expected_phases:
                raise IntegrityError(
                    "order operation event phases are incomplete or reordered"
                )
            expected_hashes = [row["intent_event_hash"]]
            if row["phase"] in {"RESULT", "APPLIED"}:
                expected_hashes.append(row["result_event_hash"])
            if row["phase"] == "APPLIED":
                expected_hashes.append(row["applied_event_hash"])
            if [item["event_hash"] for item in phase_rows] != expected_hashes:
                raise IntegrityError("order operation/event receipt hashes disagree")
            request = _decode_payload(
                row["request_payload"], self.limits.max_json_bytes, "order request"
            )
            if _canonical_sha256(request) != row["request_sha256"]:
                raise IntegrityError("order request digest mismatch")
            intent_payload = _decode_payload(
                phase_rows[0]["payload"],
                self.limits.max_json_bytes,
                "order intent event",
            )
            if intent_payload != {
                "action": row["action"],
                "client_request_id": row["client_request_id"],
                "request": request,
            }:
                raise IntegrityError("order INTENT event/operation mismatch")
            result: Optional[Mapping[str, Any]] = None
            result_receipt: Optional[OrderResultReceipt] = None
            if row["phase"] in {"RESULT", "APPLIED"}:
                if row["result_payload"] is None:
                    raise IntegrityError("order result payload is missing")
                result_evidence_payload = _decode_payload(
                    row["result_payload"],
                    self.limits.max_json_bytes,
                    "authenticated order result",
                )
                result_evidence, _ = self._authenticated_result_record_from_dict(
                    result_evidence_payload
                )
                result_evidence.verify(
                    self.broker_result_public_keys,
                    self._intent_receipt_from_row(row),
                )
                if result_evidence.result_outcome == "EXECUTED":
                    execution_sequence = result_evidence.broker_execution_sequence
                    execution_id = result_evidence.broker_execution_id
                    if execution_sequence is None or execution_id is None:
                        raise IntegrityError(
                            "EXECUTED result lacks canonical execution identity"
                        )
                    if execution_sequence > last_broker_execution:
                        last_broker_execution = execution_sequence
                        last_broker_id_sha = hashlib.sha256(
                            execution_id.encode("utf-8")
                        ).hexdigest()
                    elif execution_sequence == last_broker_execution:
                        candidate_sha = hashlib.sha256(
                            execution_id.encode("utf-8")
                        ).hexdigest()
                        if (
                            last_broker_id_sha is not None
                            and candidate_sha != last_broker_id_sha
                        ):
                            raise IntegrityError(
                                "broker execution high-water identity diverged"
                            )
                result = result_evidence.result
                if result_evidence.result_sha256 != row["result_sha256"]:
                    raise IntegrityError("order result digest mismatch")
                if bytes(phase_rows[1]["payload"]) != bytes(row["result_payload"]):
                    raise IntegrityError("order RESULT event/operation mismatch")
                result_receipt = OrderResultReceipt(
                    operation_id=str(row["operation_id"]),
                    operation_sequence=sequence,
                    event_hash=str(row["result_event_hash"]),
                    result_sha256=str(row["result_sha256"]),
                    evidence_sha256=hashlib.sha256(
                        bytes(row["result_payload"])
                    ).hexdigest(),
                )
            if row["phase"] == "APPLIED":
                local = connection.execute(
                    "SELECT * FROM order_local_state WHERE operation_sequence=?",
                    (sequence,),
                ).fetchone()
                if local is None:
                    raise IntegrityError("APPLIED order lacks durable local state")
                local_body = {
                    "operation_sequence": sequence,
                    "operation_id": row["operation_id"],
                    "payload_sha256": local["payload_sha256"],
                    "result_event_hash": local["result_event_hash"],
                }
                self.authenticator.verify("order-local-state", local_body, local["mac"])
                local_payload = _decode_payload(
                    local["payload"], self.limits.max_state_bytes, "order local state"
                )
                if _canonical_sha256(local_payload) != local["payload_sha256"]:
                    raise IntegrityError("order local-state digest mismatch")
                if local["payload_sha256"] != row["local_state_sha256"]:
                    raise IntegrityError("order/local-state receipt mismatch")
                if local["result_event_hash"] != row["result_event_hash"]:
                    raise IntegrityError("order/local-state result binding mismatch")
            else:
                pending.append(
                    OrderRecovery(
                        operation_id=str(row["operation_id"]),
                        operation_sequence=sequence,
                        client_request_id=str(row["client_request_id"]),
                        action=str(row["action"]),
                        phase=str(row["phase"]),
                        request=_deep_freeze(request),
                        result=_deep_freeze(result) if result is not None else None,
                        intent_receipt=self._intent_receipt_from_row(row),
                        result_receipt=result_receipt,
                    )
                )
        if last_operation != int(meta["max_operation_sequence"]):
            raise IntegrityError(
                "order allocation high-water has missing detail or tombstone"
            )
        if last_broker_execution != int(meta["max_broker_execution_sequence"]):
            raise IntegrityError("broker execution high-water has missing detail")
        if (
            last_broker_id_sha is not None
            and last_broker_id_sha != meta["last_broker_execution_sha256"]
        ):
            raise IntegrityError("last broker execution identity hash mismatch")
        if len(pending) > self.limits.max_pending_orders:
            raise CapacityExceededError("pending order count exceeds cap")
        if _canonical_bytes(replay_order_state) != _canonical_bytes(global_order_state):
            raise IntegrityError(
                "replayed order state differs from durable global state"
            )
        if int(global_state_row["applied_operation_sequence"]) != last_applied_sequence:
            raise IntegrityError("order state applied-operation marker mismatch")
        return tuple(pending)

    def _authenticate_bar_event_row(self, row: sqlite3.Row) -> Mapping[str, Any]:
        """Authenticate one retained bar event without replaying its prefix."""

        bar = _decode_payload(row["payload"], self.limits.max_json_bytes, "bar event")
        if _canonical_sha256(bar) != row["bar_sha256"]:
            raise IntegrityError("bar payload digest mismatch")
        cadence = self._cadence_receipt_from_value(bar.get("cadence_receipt"))
        cadence.verify(
            self.cadence_public_keys,
            self.source,
            self.symbol,
            self.timeframe,
        )
        if cadence.canonical() != bar.get("cadence_receipt"):
            raise AuthenticationError(
                "persisted cadence receipt canonical form changed"
            )
        if int(bar.get("source_phase_seconds", -1)) != int(
            cadence.observed_phase_seconds
        ):
            raise AuthenticationError(
                "persisted bar phase disagrees with cadence receipt"
            )
        if row["close_time"] != bar.get("close_time"):
            raise IntegrityError("bar close time column mismatch")
        for column in (
            "source_sequence",
            "source_session_id",
            "source_connection_id",
        ):
            if row[column] != bar.get(column):
                raise IntegrityError(f"bar {column} column mismatch")
        event_body = {
            "schema_version": BAR_EVENT_SCHEMA_VERSION,
            "sequence": int(row["sequence"]),
            "segment_id": int(row["segment_id"]),
            "previous_hash": str(row["previous_hash"]),
            "bar_sha256": str(row["bar_sha256"]),
            "close_time": str(row["close_time"]),
            "source_sequence": int(row["source_sequence"]),
            "source_session_id": str(row["source_session_id"]),
            "source_connection_id": str(row["source_connection_id"]),
            "post_state_sha256": str(row["post_state_sha256"]),
        }
        event_hash = _canonical_sha256(event_body)
        if event_hash != row["event_hash"]:
            raise IntegrityError("bar event hash mismatch")
        self.authenticator.verify("bar-event", event_body, row["mac"])
        return bar

    def _verify_bar_heads_incremental(
        self,
        connection: sqlite3.Connection,
        active: tuple[CheckpointSnapshot, dict[str, Any]],
    ) -> tuple[sqlite3.Row, sqlite3.Row]:
        """Verify authenticated bar anchors and endpoints in bounded work."""

        snapshot, checkpoint_body = active
        head = self._load_bar_head(connection)
        state_row, _ = self._load_runtime_state(connection)
        if (
            int(head["anchor_sequence"]) != snapshot.bar_sequence
            or head["anchor_hash"] != snapshot.bar_hash
        ):
            raise IntegrityError("bar anchor does not match the active checkpoint")
        if int(head["sequence"]) != int(state_row["bar_sequence"]):
            raise IntegrityError("bar head and runtime state sequence differ")
        retained = int(head["sequence"]) - int(head["anchor_sequence"])
        if retained != int(head["retained_events"]):
            raise IntegrityError(
                "bar retained-event counter disagrees with its anchors"
            )
        if retained > self.limits.max_bar_events:
            raise CapacityExceededError("stored bar event count exceeds cap")
        if int(head["retained_bytes"]) > self.limits.max_bar_bytes:
            raise CapacityExceededError("stored bar payload bytes exceed cap")
        first = connection.execute(
            "SELECT * FROM bar_events ORDER BY sequence LIMIT 1"
        ).fetchone()
        last = connection.execute(
            "SELECT * FROM bar_events ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        if retained == 0:
            if (
                first is not None
                or last is not None
                or head["event_hash"] != head["anchor_hash"]
            ):
                raise IntegrityError(
                    "empty bar journal disagrees with its authenticated head"
                )
            if connection.execute("SELECT 1 FROM bar_segments LIMIT 1").fetchone():
                raise IntegrityError("empty bar journal retains segment metadata")
            return head, state_row
        if first is None or last is None:
            raise IntegrityError("nonempty bar head lacks endpoint rows")
        first_bar = self._authenticate_bar_event_row(first)
        if int(first["sequence"]) != int(head["anchor_sequence"]) + 1:
            raise IntegrityError("bar journal first retained sequence is invalid")
        if first["previous_hash"] != head["anchor_hash"]:
            raise IntegrityError("bar journal first retained hash is invalid")
        self._validate_source_continuity(
            first_bar,
            last_close_time=checkpoint_body.get("last_closed_bar_time"),
            last_source_sequence=int(checkpoint_body.get("last_source_sequence", -1)),
            last_source_session_id=checkpoint_body.get("last_source_session_id"),
            last_source_connection_id=checkpoint_body.get("last_source_connection_id"),
            last_source_phase_seconds=checkpoint_body.get("last_source_phase_seconds"),
        )
        if int(last["sequence"]) != int(first["sequence"]):
            self._authenticate_bar_event_row(last)
        if (
            int(last["sequence"]) != int(head["sequence"])
            or last["event_hash"] != head["event_hash"]
            or last["post_state_sha256"] != state_row["state_sha256"]
            or last["bar_sha256"] != state_row["last_bar_sha256"]
            or last["close_time"] != state_row["last_closed_bar_time"]
            or int(last["source_sequence"]) != int(state_row["last_source_sequence"])
            or last["source_session_id"] != state_row["last_source_session_id"]
            or last["source_connection_id"] != state_row["last_source_connection_id"]
        ):
            raise IntegrityError(
                "bar journal endpoint disagrees with authenticated heads"
            )
        segment = connection.execute(
            "SELECT * FROM bar_segments WHERE segment_id=?",
            (head["current_segment"],),
        ).fetchone()
        if segment is None:
            raise IntegrityError("current bar segment is missing")
        segment_body = self._segment_body(segment)
        self.authenticator.verify("bar-segment", segment_body, segment["mac"])
        if (
            int(segment["last_sequence"]) != int(last["sequence"])
            or segment["last_event_hash"] != last["event_hash"]
            or int(segment["event_count"]) != int(head["segment_events"])
            or int(segment["byte_count"]) != int(head["segment_bytes"])
            or int(segment["event_count"]) > self.limits.max_segment_events
            or int(segment["byte_count"]) > self.limits.max_segment_bytes
        ):
            raise IntegrityError(
                "current bar segment disagrees with its authenticated head"
            )
        segment_first = connection.execute(
            "SELECT previous_hash FROM bar_events WHERE segment_id=? "
            "ORDER BY sequence LIMIT 1",
            (segment["segment_id"],),
        ).fetchone()
        if (
            segment_first is None
            or segment_first["previous_hash"] != segment["first_previous_hash"]
        ):
            raise IntegrityError("current bar segment first hash mismatch")
        return head, state_row

    def _authenticate_order_event_row(self, row: sqlite3.Row) -> Mapping[str, Any]:
        payload = _decode_payload(
            row["payload"], self.limits.max_json_bytes, "order event"
        )
        body = {
            "schema_version": ORDER_EVENT_SCHEMA_VERSION,
            "sequence": int(row["sequence"]),
            "operation_sequence": int(row["operation_sequence"]),
            "operation_id": str(row["operation_id"]),
            "phase": str(row["phase"]),
            "previous_hash": str(row["previous_hash"]),
            "payload_sha256": _canonical_sha256(payload),
        }
        event_hash = _canonical_sha256(body)
        if event_hash != row["event_hash"]:
            raise IntegrityError("order event hash mismatch")
        self.authenticator.verify("order-event", body, row["mac"])
        return payload

    def _verify_pending_orders_incremental(
        self, connection: sqlite3.Connection, meta: sqlite3.Row
    ) -> tuple[OrderRecovery, ...]:
        rows = connection.execute(
            "SELECT * FROM order_operations "
            "WHERE phase IN ('INTENT','RESULT') ORDER BY operation_sequence LIMIT ?",
            (self.limits.max_pending_orders + 1,),
        ).fetchall()
        if len(rows) > self.limits.max_pending_orders:
            raise CapacityExceededError("pending order count exceeds cap")
        pending: list[OrderRecovery] = []
        for row in rows:
            sequence = int(row["operation_sequence"])
            if not (
                int(meta["retired_through_sequence"])
                < sequence
                <= int(meta["max_operation_sequence"])
            ):
                raise IntegrityError("pending order lies outside authenticated bounds")
            self.authenticator.verify(
                "order-operation", self._operation_body(row), row["mac"]
            )
            request = _decode_payload(
                row["request_payload"], self.limits.max_json_bytes, "order request"
            )
            if _canonical_sha256(request) != row["request_sha256"]:
                raise IntegrityError("order request digest mismatch")
            events = connection.execute(
                "SELECT * FROM order_events WHERE operation_sequence=? "
                "ORDER BY sequence LIMIT 3",
                (sequence,),
            ).fetchall()
            expected_phases = (
                ("INTENT",) if row["phase"] == "INTENT" else ("INTENT", "RESULT")
            )
            if tuple(event["phase"] for event in events) != expected_phases:
                raise IntegrityError("pending order event phases are incomplete")
            payloads = [self._authenticate_order_event_row(event) for event in events]
            if events[0]["event_hash"] != row["intent_event_hash"] or payloads[0] != {
                "action": row["action"],
                "client_request_id": row["client_request_id"],
                "request": request,
            }:
                raise IntegrityError("pending INTENT event/operation mismatch")
            result: Optional[Mapping[str, Any]] = None
            result_receipt: Optional[OrderResultReceipt] = None
            if row["phase"] == "RESULT":
                if events[1]["event_hash"] != row["result_event_hash"]:
                    raise IntegrityError("pending RESULT receipt hash mismatch")
                stored_record = _decode_payload(
                    row["result_payload"],
                    self.limits.max_json_bytes,
                    "authenticated order result",
                )
                if payloads[1] != stored_record:
                    raise IntegrityError("pending RESULT event/operation mismatch")
                evidence, _ = self._authenticated_result_record_from_dict(stored_record)
                evidence.verify(
                    self.broker_result_public_keys,
                    self._intent_receipt_from_row(row),
                )
                if evidence.result_sha256 != row["result_sha256"]:
                    raise IntegrityError("pending order result digest mismatch")
                result = evidence.result
                result_receipt = OrderResultReceipt(
                    operation_id=str(row["operation_id"]),
                    operation_sequence=sequence,
                    event_hash=str(row["result_event_hash"]),
                    result_sha256=str(row["result_sha256"]),
                    evidence_sha256=hashlib.sha256(
                        bytes(row["result_payload"])
                    ).hexdigest(),
                )
            pending.append(
                OrderRecovery(
                    operation_id=str(row["operation_id"]),
                    operation_sequence=sequence,
                    client_request_id=str(row["client_request_id"]),
                    action=str(row["action"]),
                    phase=str(row["phase"]),
                    request=_deep_freeze(request),
                    result=_deep_freeze(result) if result is not None else None,
                    intent_receipt=self._intent_receipt_from_row(row),
                    result_receipt=result_receipt,
                )
            )
        return tuple(pending)

    def _verify_order_heads_incremental(
        self, connection: sqlite3.Connection
    ) -> tuple[sqlite3.Row, tuple[OrderRecovery, ...]]:
        meta = self._load_order_meta(connection)
        anchor_row, anchor_state = self._load_order_anchor_state(connection)
        book_row, book_state = self._load_order_book_state(connection)
        if int(anchor_row["anchor_event_sequence"]) != int(
            meta["anchor_event_sequence"]
        ):
            raise IntegrityError("order state anchor/event anchor mismatch")
        retained_events = int(meta["event_sequence"]) - int(
            meta["anchor_event_sequence"]
        )
        retained_operations = int(meta["max_operation_sequence"]) - int(
            meta["retired_through_sequence"]
        )
        if not (0 <= retained_events <= self.limits.max_order_events):
            raise CapacityExceededError("retained order event bounds are invalid")
        if not (0 <= retained_operations <= self.limits.max_order_operations):
            raise CapacityExceededError("retained order operation bounds are invalid")
        first_event = connection.execute(
            "SELECT * FROM order_events ORDER BY sequence LIMIT 1"
        ).fetchone()
        last_event = connection.execute(
            "SELECT * FROM order_events ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        if retained_events == 0:
            if (
                first_event is not None
                or last_event is not None
                or meta["event_hash"] != meta["anchor_event_hash"]
            ):
                raise IntegrityError("empty order journal disagrees with its head")
        else:
            if first_event is None or last_event is None:
                raise IntegrityError("nonempty order head lacks endpoint rows")
            self._authenticate_order_event_row(first_event)
            if int(first_event["sequence"]) != int(meta["anchor_event_sequence"]) + 1:
                raise IntegrityError("first retained order event sequence is invalid")
            if first_event["previous_hash"] != meta["anchor_event_hash"]:
                raise IntegrityError("first retained order event hash is invalid")
            if int(last_event["sequence"]) != int(first_event["sequence"]):
                self._authenticate_order_event_row(last_event)
            if (
                int(last_event["sequence"]) != int(meta["event_sequence"])
                or last_event["event_hash"] != meta["event_hash"]
            ):
                raise IntegrityError("order event endpoint disagrees with its head")
        first_operation = connection.execute(
            "SELECT * FROM order_operations ORDER BY operation_sequence LIMIT 1"
        ).fetchone()
        last_operation = connection.execute(
            "SELECT * FROM order_operations ORDER BY operation_sequence DESC LIMIT 1"
        ).fetchone()
        if retained_operations == 0:
            if first_operation is not None or last_operation is not None:
                raise IntegrityError("retired order head retains operation detail")
        else:
            if first_operation is None or last_operation is None:
                raise IntegrityError("order operation head lacks endpoint rows")
            if int(first_operation["operation_sequence"]) != int(
                meta["retired_through_sequence"]
            ) + 1 or int(last_operation["operation_sequence"]) != int(
                meta["max_operation_sequence"]
            ):
                raise IntegrityError("order operation endpoints disagree with metadata")
            for operation in (
                first_operation,
                *(() if first_operation is last_operation else (last_operation,)),
            ):
                self.authenticator.verify(
                    "order-operation", self._operation_body(operation), operation["mac"]
                )
        applied_sequence = int(book_row["applied_operation_sequence"])
        anchor_applied = int(anchor_row["last_applied_operation_sequence"])
        if (
            not anchor_applied
            <= applied_sequence
            <= int(meta["max_operation_sequence"])
        ):
            raise IntegrityError(
                "order book applied marker lies outside authenticated bounds"
            )
        if applied_sequence == anchor_applied:
            if _canonical_bytes(book_state) != _canonical_bytes(anchor_state):
                raise IntegrityError("order book state diverged from its active anchor")
        else:
            local = connection.execute(
                "SELECT * FROM order_local_state WHERE operation_sequence=?",
                (applied_sequence,),
            ).fetchone()
            if local is None:
                raise IntegrityError("order book head lacks its local-state receipt")
            local_body = {
                "operation_sequence": applied_sequence,
                "operation_id": local["operation_id"],
                "payload_sha256": local["payload_sha256"],
                "result_event_hash": local["result_event_hash"],
            }
            self.authenticator.verify("order-local-state", local_body, local["mac"])
            if local["payload_sha256"] != book_row["payload_sha256"] or bytes(
                local["payload"]
            ) != bytes(book_row["payload"]):
                raise IntegrityError("order book head/local-state receipt mismatch")
        return meta, self._verify_pending_orders_incremental(connection, meta)

    def _verify_incremental_runtime(
        self, connection: sqlite3.Connection
    ) -> tuple[
        tuple[CheckpointSnapshot, dict[str, Any]],
        tuple[OrderRecovery, ...],
        sqlite3.Row,
        sqlite3.Row,
    ]:
        self._assert_engine_contract_current()
        self._load_meta(connection)
        self._require_admissible_resolutions(connection)
        active = self._load_active_checkpoint(connection)
        if active is None:
            raise RecoveryRequiredError("active checkpoint is missing")
        head, _ = self._verify_bar_heads_incremental(connection, active)
        order_meta, pending = self._verify_order_heads_incremental(connection)
        local = self._witness_proposal_from_rows(head, order_meta, active)
        external = self.storage_authority.read_runtime_witness(self.storage_reservation)
        self._compare_witness_to_local(external, local)
        self._verification_metrics["incremental_audits"] += 1
        return active, pending, head, order_meta

    def _assess_recovery(self) -> RuntimeRecovery:
        self._verification_metrics["full_audits"] += 1
        self._mutations_since_full_audit = 0
        try:
            self._assert_engine_contract_current()

            def assess(connection: sqlite3.Connection) -> RuntimeRecovery:
                integrity = str(
                    connection.execute("PRAGMA integrity_check(1)").fetchone()[0]
                )
                if integrity != "ok":
                    raise IntegrityError(f"SQLite integrity_check failed: {integrity}")
                foreign_violation = connection.execute(
                    "PRAGMA foreign_key_check"
                ).fetchone()
                if foreign_violation is not None:
                    raise IntegrityError(
                        "SQLite foreign_key_check failed: "
                        + repr(tuple(foreign_violation))
                    )
                meta = self._load_meta(connection)
                decisions = self._state_resolutions(connection)
                rejected = tuple(
                    cell_id
                    for cell_id, resolution in decisions
                    if not resolution.admissible
                )
                head = self._load_bar_head(connection)
                active = self._load_active_checkpoint(connection)
                pending = self._verify_order_chain(connection)
                order_meta = self._load_order_meta(connection)
                generation = 0
                reasons: list[str] = []
                if rejected:
                    reasons.extend(
                        f"STATE_CONTRACT_QUARANTINED:{item}" for item in rejected
                    )
                if active is None:
                    state_row, _ = self._load_runtime_state(connection)
                    pristine = (
                        not bool(meta.get("bootstrapped"))
                        and int(head["sequence"]) == 0
                        and int(state_row["bar_sequence"]) == 0
                        and connection.execute(
                            "SELECT COUNT(*) FROM bar_events"
                        ).fetchone()[0]
                        == 0
                    )
                    if pristine:
                        self._last_verified_witness_proposal = (
                            self._witness_proposal_from_rows(head, order_meta, active)
                        )
                    if pristine and not reasons:
                        return RuntimeRecovery(
                            "AUTHENTICATED_BOOTSTRAP_REQUIRED", (), pending, 0, 0
                        )
                    reasons.append("ACTIVE_CHECKPOINT_MISSING")
                else:
                    generation = active[0].generation
                    self._verify_bar_chain_and_replay(connection, active)
                    self._last_verified_witness_proposal = (
                        self._witness_proposal_from_rows(head, order_meta, active)
                    )
                if pending:
                    reasons.extend(
                        f"ORDER_{item.phase}_RECOVERY_REQUIRED:{item.operation_id}"
                        for item in pending
                    )
                if reasons:
                    return RuntimeRecovery(
                        "MANAGEMENT_ONLY",
                        tuple(reasons),
                        pending,
                        int(head["sequence"]),
                        generation,
                    )
                return RuntimeRecovery(
                    "READY", (), (), int(head["sequence"]), generation
                )

            return self._run_read(assess)
        except StateRuntimeError as exc:
            return RuntimeRecovery(
                "MANAGEMENT_ONLY", (f"{type(exc).__name__}:{exc}",), (), 0, 0
            )
        except Exception as exc:
            return RuntimeRecovery(
                "MANAGEMENT_ONLY",
                (f"INTEGRITY_FAILURE:{type(exc).__name__}:{exc}",),
                (),
                0,
                0,
            )

    def _assess_recovery_incremental(self) -> RuntimeRecovery:
        """Assess the live heads in bounded work between scheduled full audits."""

        try:

            def assess(connection: sqlite3.Connection) -> RuntimeRecovery:
                active, pending, head, order_meta = self._verify_incremental_runtime(
                    connection
                )
                self._last_verified_witness_proposal = self._witness_proposal_from_rows(
                    head, order_meta, active
                )
                if pending:
                    return RuntimeRecovery(
                        "MANAGEMENT_ONLY",
                        tuple(
                            f"ORDER_{item.phase}_RECOVERY_REQUIRED:{item.operation_id}"
                            for item in pending
                        ),
                        pending,
                        int(head["sequence"]),
                        active[0].generation,
                    )
                return RuntimeRecovery(
                    "READY",
                    (),
                    (),
                    int(head["sequence"]),
                    active[0].generation,
                )

            return self._run_read(assess)
        except StateRuntimeError as exc:
            return RuntimeRecovery(
                "MANAGEMENT_ONLY", (f"{type(exc).__name__}:{exc}",), (), 0, 0
            )
        except Exception as exc:
            return RuntimeRecovery(
                "MANAGEMENT_ONLY",
                (f"INTEGRITY_FAILURE:{type(exc).__name__}:{exc}",),
                (),
                0,
                0,
            )

    def _refresh_recovery_after_mutation(self) -> RuntimeRecovery:
        """Advance heads cheaply and force a complete audit on a fixed cadence."""

        self._ensure_open()
        self._mutations_since_full_audit += 1
        if (
            self._mutations_since_full_audit
            >= self.limits.full_audit_interval_mutations
        ):
            return self.refresh_recovery()
        self._last_verified_witness_proposal = None
        self._recovery = self._assess_recovery_incremental()
        proposal = self._last_verified_witness_proposal
        if proposal is not None:
            try:
                self._synchronize_runtime_witness(proposal)
            except Exception as exc:
                self._recovery = RuntimeRecovery(
                    "MANAGEMENT_ONLY",
                    (
                        *self._recovery.reasons,
                        f"MONOTONIC_WITNESS_FAILURE:{type(exc).__name__}:{exc}",
                    ),
                    self._recovery.pending_orders,
                    self._recovery.bar_sequence,
                    self._recovery.checkpoint_generation,
                )
        return self._recovery

    def _require_ready(self) -> None:
        if not self._recovery.trading_ready:
            raise RecoveryRequiredError(
                f"runtime is {self._recovery.status}: {';'.join(self._recovery.reasons)}"
            )

    def _verify_full_mutation_invariants(
        self, connection: sqlite3.Connection
    ) -> tuple[
        tuple[CheckpointSnapshot, dict[str, Any]],
        tuple[OrderRecovery, ...],
    ]:
        self._verification_metrics["full_audits"] += 1
        self._mutations_since_full_audit = 0
        self._assert_engine_contract_current()
        integrity = str(connection.execute("PRAGMA integrity_check(1)").fetchone()[0])
        if integrity != "ok":
            raise IntegrityError(
                f"SQLite integrity_check failed before mutation: {integrity}"
            )
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise IntegrityError("SQLite foreign-key violation exists before mutation")
        self._load_meta(connection)
        self._require_admissible_resolutions(connection)
        active = self._load_active_checkpoint(connection)
        if active is None:
            raise RecoveryRequiredError("active checkpoint is missing")
        self._verify_bar_chain_and_replay(connection, active)
        pending = self._verify_order_chain(connection)
        head = self._load_bar_head(connection)
        order_meta = self._load_order_meta(connection)
        local = self._witness_proposal_from_rows(head, order_meta, active)
        external = self.storage_authority.read_runtime_witness(self.storage_reservation)
        self._compare_witness_to_local(external, local)
        return active, pending

    def _verify_mutation_invariants(
        self, connection: sqlite3.Connection
    ) -> tuple[
        tuple[CheckpointSnapshot, dict[str, Any]],
        tuple[OrderRecovery, ...],
    ]:
        active, pending, _, _ = self._verify_incremental_runtime(connection)
        return active, pending

    def _checkpoint_transaction(
        self, connection: sqlite3.Connection, *, bootstrap: bool
    ) -> CheckpointSnapshot:
        pending: tuple[OrderRecovery, ...]
        if not bootstrap:
            _, pending = self._verify_full_mutation_invariants(connection)
        else:
            pending = self._verify_order_chain(connection)
        meta = self._load_meta(connection)
        decisions = self._require_admissible_resolutions(connection)
        evidence_sha256 = _canonical_sha256(
            [item.canonical() for item in self._stored_evidence(connection)]
        )
        state_row, state = self._load_runtime_state(connection)
        head = self._load_bar_head(connection)
        order_meta = self._load_order_meta(connection)
        order_state_row, _ = self._load_order_book_state(connection)
        if pending:
            raise RecoveryRequiredError(
                "checkpoint publication is forbidden while orders need recovery"
            )
        active = self._load_active_checkpoint(connection)
        if bootstrap:
            pristine = (
                active is None
                and not bool(meta.get("bootstrapped"))
                and int(head["sequence"]) == 0
                and int(state_row["bar_sequence"]) == 0
                and connection.execute("SELECT COUNT(*) FROM bar_events").fetchone()[0]
                == 0
            )
            if not pristine:
                if (
                    active is not None
                    and bool(meta.get("bootstrapped"))
                    and active[0].bar_sequence == int(head["sequence"])
                ):
                    return active[0]
                raise RecoveryRequiredError(
                    "bootstrap is permitted only for a pristine authenticated DB"
                )
            local = self._witness_proposal_from_rows(head, order_meta, active)
            external = self.storage_authority.read_runtime_witness(
                self.storage_reservation
            )
            self._compare_witness_to_local(external, local)
        else:
            if active is None or not bool(meta.get("bootstrapped")):
                raise RecoveryRequiredError("an authenticated bootstrap is required")
            self._verify_bar_chain_and_replay(connection, active)
            if (
                active[0].bar_sequence == int(head["sequence"])
                and active[0].bar_hash == head["event_hash"]
                and active[0].state_sha256 == state_row["state_sha256"]
                and int(active[1].get("last_source_sequence", -1))
                == int(state_row["last_source_sequence"])
                and active[1].get("last_source_session_id")
                == state_row["last_source_session_id"]
                and active[1].get("last_source_connection_id")
                == state_row["last_source_connection_id"]
                and active[1].get("last_source_phase_seconds")
                == state_row["last_source_phase_seconds"]
                and int(head["retained_events"]) == 0
                and int(active[1]["order_event_sequence"])
                == int(order_meta["event_sequence"])
                and active[1]["order_event_hash"] == order_meta["event_hash"]
                and int(active[1]["order_max_operation_sequence"])
                == int(order_meta["max_operation_sequence"])
                and int(active[1]["order_max_client_sequence"])
                == int(order_meta["max_client_sequence"])
                and int(active[1]["order_max_broker_execution_sequence"])
                == int(order_meta["max_broker_execution_sequence"])
                and active[1]["order_last_broker_execution_sha256"]
                == order_meta["last_broker_execution_sha256"]
                and int(active[1]["order_retired_through_sequence"])
                == int(order_meta["retired_through_sequence"])
                and int(active[1]["order_applied_operation_sequence"])
                == int(order_state_row["applied_operation_sequence"])
                and active[1]["order_state_sha256"] == order_state_row["payload_sha256"]
            ):
                return active[0]

        previous_generation = active[0].generation if active else 0
        predecessor_sha = active[0].checkpoint_sha256 if active else ZERO_HASH
        generation = previous_generation + 1
        slot = "A" if active is None or active[0].slot == "B" else "B"
        checkpoint_body = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "generation": generation,
            "predecessor_checkpoint_sha256": predecessor_sha,
            "registry_hash": DEFAULT_REGISTRY_HASH,
            "source_epoch_sha256": self.source.identity_sha256,
            "roster_sha256": self.roster.identity_sha256,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "engine_sha256": self.engine_sha256,
            "storage_reservation_sha256": _canonical_sha256(
                self.storage_reservation.canonical()
            ),
            "state_evidence_sha256": evidence_sha256,
            "state_resolutions": self._resolution_payload(decisions),
            "state": state,
            "state_sha256": state_row["state_sha256"],
            "last_closed_bar_time": state_row["last_closed_bar_time"],
            "last_bar_sha256": state_row["last_bar_sha256"],
            "last_source_sequence": int(state_row["last_source_sequence"]),
            "last_source_session_id": state_row["last_source_session_id"],
            "last_source_connection_id": state_row["last_source_connection_id"],
            "last_source_phase_seconds": state_row["last_source_phase_seconds"],
            "bar_sequence": int(head["sequence"]),
            "bar_hash": str(head["event_hash"]),
            "order_event_sequence": int(order_meta["event_sequence"]),
            "order_event_hash": str(order_meta["event_hash"]),
            "order_max_operation_sequence": int(order_meta["max_operation_sequence"]),
            "order_max_client_sequence": int(order_meta["max_client_sequence"]),
            "order_max_broker_execution_sequence": int(
                order_meta["max_broker_execution_sequence"]
            ),
            "order_last_broker_execution_sha256": str(
                order_meta["last_broker_execution_sha256"]
            ),
            "order_retired_through_sequence": int(
                order_meta["retired_through_sequence"]
            ),
            "order_applied_operation_sequence": int(
                order_state_row["applied_operation_sequence"]
            ),
            "order_state_sha256": str(order_state_row["payload_sha256"]),
            "published_at": _utc_now(),
        }
        checkpoint_payload, checkpoint_sha = _bounded_payload(
            checkpoint_body,
            self.limits.max_state_bytes + self.limits.max_json_bytes,
            "checkpoint",
        )
        checkpoint_mac = self.authenticator.sign("checkpoint", checkpoint_body)
        connection.execute(
            "INSERT INTO checkpoint_slots(slot,generation,checkpoint_sha256,payload,mac) "
            "VALUES(?,?,?,?,?) ON CONFLICT(slot) DO UPDATE SET "
            "generation=excluded.generation,checkpoint_sha256=excluded.checkpoint_sha256,"
            "payload=excluded.payload,mac=excluded.mac",
            (slot, generation, checkpoint_sha, checkpoint_payload, checkpoint_mac),
        )
        pointer_body = {
            "slot": slot,
            "generation": generation,
            "checkpoint_sha256": checkpoint_sha,
        }
        pointer_payload, _ = _bounded_payload(
            pointer_body, self.limits.max_json_bytes, "checkpoint pointer"
        )
        connection.execute(
            "INSERT INTO active_checkpoint VALUES(1,?,?,?,?,?) "
            "ON CONFLICT(singleton) DO UPDATE SET slot=excluded.slot,"
            "generation=excluded.generation,checkpoint_sha256=excluded.checkpoint_sha256,"
            "payload=excluded.payload,mac=excluded.mac",
            (
                slot,
                generation,
                checkpoint_sha,
                pointer_payload,
                self.authenticator.sign("checkpoint-pointer", pointer_body),
            ),
        )
        connection.execute("DELETE FROM bar_events")
        connection.execute("DELETE FROM bar_segments")
        compacted_head = {
            "sequence": int(head["sequence"]),
            "event_hash": str(head["event_hash"]),
            "anchor_sequence": int(head["sequence"]),
            "anchor_hash": str(head["event_hash"]),
            "current_segment": int(head["current_segment"]) + 1,
            "segment_events": 0,
            "segment_bytes": 0,
            "retained_events": 0,
            "retained_bytes": 0,
        }
        connection.execute(
            "UPDATE bar_head SET anchor_sequence=?,anchor_hash=?,current_segment=?,"
            "segment_events=0,segment_bytes=0,retained_events=0,retained_bytes=0,mac=? "
            "WHERE singleton=1",
            (
                compacted_head["anchor_sequence"],
                compacted_head["anchor_hash"],
                compacted_head["current_segment"],
                self.authenticator.sign("bar-head", compacted_head),
            ),
        )
        if bootstrap:
            meta["bootstrapped"] = True
            meta_payload, _ = _bounded_payload(
                meta, self.limits.max_json_bytes, "runtime binding"
            )
            connection.execute(
                "UPDATE runtime_meta SET payload=?,mac=? WHERE singleton=1",
                (meta_payload, self.authenticator.sign("runtime-meta", meta)),
            )
        return CheckpointSnapshot(
            generation=generation,
            slot=slot,
            checkpoint_sha256=checkpoint_sha,
            state_sha256=str(state_row["state_sha256"]),
            bar_sequence=int(head["sequence"]),
            bar_hash=str(head["event_hash"]),
            last_closed_bar_time=state_row["last_closed_bar_time"],
        )

    def _verify_checkpoint_receipt(
        self, expected: CheckpointSnapshot
    ) -> CheckpointSnapshot:
        def verify(connection: sqlite3.Connection) -> CheckpointSnapshot:
            active = self._load_active_checkpoint(connection)
            if active is None or active[0] != expected:
                raise PublicationError("published checkpoint failed verify-after-write")
            head = self._load_bar_head(connection)
            if int(head["retained_events"]) != 0:
                raise PublicationError(
                    "checkpoint publication did not compact bar rows"
                )
            return active[0]

        return self._run_read(verify)

    def bootstrap_checkpoint(self) -> CheckpointSnapshot:
        """Explicitly authenticate a pristine initial state; never an inferred tail."""

        self._ensure_open()
        if self._recovery.status not in {
            "AUTHENTICATED_BOOTSTRAP_REQUIRED",
            "MANAGEMENT_ONLY",
        }:
            raise RecoveryRequiredError("runtime does not require bootstrap")
        snapshot = self._run_write(
            lambda connection: self._checkpoint_transaction(connection, bootstrap=True)
        )
        verified = self._verify_checkpoint_receipt(snapshot)
        self.refresh_recovery()
        self._require_ready()
        self._require_witness_current()
        if not self._recovery.trading_ready:
            raise PublicationError(
                "authenticated bootstrap committed but runtime did not become READY"
            )
        return verified

    def publish_checkpoint(self) -> CheckpointSnapshot:
        """Publish inactive A/B slot and compact covered bars atomically."""

        self._require_ready()
        snapshot = self._run_write(
            lambda connection: self._checkpoint_transaction(connection, bootstrap=False)
        )
        verified = self._verify_checkpoint_receipt(snapshot)
        self.refresh_recovery()
        self._require_ready()
        self._require_witness_current()
        return verified

    def active_checkpoint(self) -> Optional[CheckpointSnapshot]:
        self._ensure_open()

        def load(connection: sqlite3.Connection) -> Optional[CheckpointSnapshot]:
            active = self._load_active_checkpoint(connection)
            return active[0] if active is not None else None

        return self._run_read(load)

    @staticmethod
    def _cadence_receipt_from_value(value: Any) -> AuthenticatedCadenceReceipt:
        if isinstance(value, AuthenticatedCadenceReceipt):
            return value
        if not isinstance(value, Mapping):
            raise AuthenticationError(
                "closed bar requires an authenticated V5 cadence receipt"
            )
        expected = set(AuthenticatedCadenceReceipt.__dataclass_fields__)
        if set(value) != expected:
            raise AuthenticationError("cadence receipt fields are not exact")
        try:
            return AuthenticatedCadenceReceipt(**dict(value))
        except (TypeError, ValueError) as exc:
            raise AuthenticationError("cadence receipt is malformed") from exc

    def _normal_closed_bar(self, bar: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(bar, Mapping):
            raise ValueError("bar must be a mapping")
        if bar.get("closed") is not True:
            raise ValueError("only explicitly closed bars may enter the journal")
        open_time = _normal_timestamp(bar.get("open_time"), "bar.open_time")
        close_time = _normal_timestamp(bar.get("close_time"), "bar.close_time")
        open_value = _timestamp_value(open_time)
        close_value = _timestamp_value(close_time)
        if open_value >= close_value:
            raise ValueError("bar open_time must precede close_time")
        if close_value > datetime.now(timezone.utc):
            raise ValueError("bar close_time is in the future")
        cadence = self._cadence_receipt_from_value(bar.get("cadence_receipt"))
        cadence.verify(
            self.cadence_public_keys,
            self.source,
            self.symbol,
            self.timeframe,
        )
        supplied_bindings = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "data_source": self.source.data_source,
            "source_instance_id": self.source.source_instance_id,
            "session_calendar_id": self.source.session_calendar_id,
            "source_artifact_sha256": self.source.source_artifact_sha256,
            "source_epoch_sha256": self.source.identity_sha256,
        }
        for name, expected in supplied_bindings.items():
            if name not in bar:
                raise BindingMismatchError(f"closed bar requires exact {name}")
            if str(bar[name]) != expected:
                raise BindingMismatchError(
                    f"closed bar {name} does not match runtime source envelope"
                )
        source_sequence = _positive_exact_int(
            bar.get("source_sequence"), "bar.source_sequence"
        )
        source_session_id = _normal_identifier(
            bar.get("source_session_id"), "bar.source_session_id"
        )
        source_connection_id = _normal_identifier(
            bar.get("source_connection_id"), "bar.source_connection_id"
        )
        cadence_bindings = {
            "source_sequence": source_sequence,
            "open_time": open_time,
            "close_time": close_time,
            "source_session_id": source_session_id,
            "source_connection_id": source_connection_id,
        }
        for name, expected in cadence_bindings.items():
            if getattr(cadence, name) != expected:
                raise BindingMismatchError(
                    f"closed bar/cadence receipt mismatch: {name}"
                )
        if not isinstance(bar.get("session_gap"), bool):
            raise ValueError("bar.session_gap must be an explicit boolean")
        if not isinstance(bar.get("source_reconnect"), bool):
            raise ValueError("bar.source_reconnect must be an explicit boolean")
        if bool(bar["session_gap"]) != (cadence.boundary_kind == "SESSION_GAP"):
            raise BindingMismatchError(
                "closed bar session_gap disagrees with authenticated cadence"
            )
        prices: dict[str, float] = {}
        for name in ("open", "high", "low", "close"):
            try:
                value = float(bar[name])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"bar.{name} must be finite") from exc
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"bar.{name} must be finite and positive")
            prices[name] = value
        if prices["high"] < max(prices["open"], prices["close"], prices["low"]):
            raise ValueError("bar high is inconsistent with OHLC")
        if prices["low"] > min(prices["open"], prices["close"], prices["high"]):
            raise ValueError("bar low is inconsistent with OHLC")
        counts: dict[str, int] = {}
        for name in ("tick_volume", "spread", "real_volume"):
            raw = bar.get(name, 0)
            if isinstance(raw, bool):
                raise ValueError(f"bar.{name} must be a nonnegative integer")
            try:
                value = int(raw)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"bar.{name} must be a nonnegative integer") from exc
            if value < 0 or float(value) != float(raw):
                raise ValueError(f"bar.{name} must be a nonnegative integer")
            counts[name] = value
        return {
            "schema_version": "fxpm.closed-bar.v1",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "data_source": self.source.data_source,
            "source_instance_id": self.source.source_instance_id,
            "session_calendar_id": self.source.session_calendar_id,
            "source_artifact_sha256": self.source.source_artifact_sha256,
            "source_epoch_sha256": self.source.identity_sha256,
            "source_sequence": source_sequence,
            "source_session_id": source_session_id,
            "source_connection_id": source_connection_id,
            "session_gap": bool(bar["session_gap"]),
            "source_reconnect": bool(bar["source_reconnect"]),
            "source_phase_seconds": cadence.observed_phase_seconds,
            "cadence_receipt": cadence.canonical(),
            "open_time": open_time,
            "close_time": close_time,
            **prices,
            **counts,
        }

    @staticmethod
    def _validate_source_continuity(
        bar: Mapping[str, Any],
        *,
        last_close_time: Optional[str],
        last_source_sequence: int,
        last_source_session_id: Optional[str],
        last_source_connection_id: Optional[str],
        last_source_phase_seconds: Optional[int],
    ) -> None:
        source_sequence = int(bar["source_sequence"])
        cadence = bar.get("cadence_receipt")
        if not isinstance(cadence, Mapping):
            raise AuthenticationError("bar cadence evidence is absent")
        if last_close_time is None:
            if last_source_sequence != 0 or any(
                value is not None
                for value in (
                    last_source_session_id,
                    last_source_connection_id,
                    last_source_phase_seconds,
                )
            ):
                raise OrderTransitionError(
                    "source predecessor metadata is inconsistent"
                )
            if bar["session_gap"] or bar["source_reconnect"]:
                raise OrderTransitionError(
                    "first admitted source bar cannot claim a gap or reconnect"
                )
            if (
                int(cadence["predecessor_source_sequence"]) != 0
                or cadence["predecessor_close_time"] is not None
                or cadence["predecessor_phase_seconds"] is not None
            ):
                raise AuthenticationError(
                    "first cadence receipt contains a false predecessor"
                )
            return
        if source_sequence != last_source_sequence + 1:
            raise OrderTransitionError(
                "source_sequence contains a duplicate, gap, reset, or reorder"
            )
        open_value = _timestamp_value(str(bar["open_time"]))
        last_close_value = _timestamp_value(last_close_time)
        if open_value < last_close_value:
            raise OrderTransitionError("closed bars must not overlap")
        temporal_gap = open_value > last_close_value
        if bool(bar["session_gap"]) != temporal_gap:
            raise OrderTransitionError(
                "source session-gap marker disagrees with the observed bar boundary"
            )
        session_changed = str(bar["source_session_id"]) != str(last_source_session_id)
        if session_changed != temporal_gap:
            raise OrderTransitionError(
                "source session lineage must change exactly at an admitted gap"
            )
        connection_changed = str(bar["source_connection_id"]) != str(
            last_source_connection_id
        )
        if bool(bar["source_reconnect"]) != connection_changed:
            raise OrderTransitionError(
                "source reconnect marker disagrees with connection lineage"
            )
        if (
            int(cadence["predecessor_source_sequence"]) != last_source_sequence
            or cadence["predecessor_close_time"] != last_close_time
            or int(cadence["predecessor_phase_seconds"])
            != int(last_source_phase_seconds)
        ):
            raise AuthenticationError(
                "cadence receipt predecessor does not match the durable source tail"
            )

    def _verify_bar_receipt(self, expected: BarReceipt) -> BarReceipt:
        def verify(connection: sqlite3.Connection) -> BarReceipt:
            head = self._load_bar_head(connection)
            state_row, _ = self._load_runtime_state(connection)
            if (
                int(head["sequence"]) != expected.sequence
                or int(state_row["last_source_sequence"]) != expected.source_sequence
                or head["event_hash"] != expected.event_hash
                or state_row["state_sha256"] != expected.state_sha256
                or state_row["last_bar_sha256"] != expected.bar_sha256
                or state_row["last_closed_bar_time"] != expected.close_time
            ):
                raise PublicationError("bar append failed verify-after-write")
            row = connection.execute(
                "SELECT event_hash,bar_sha256,post_state_sha256,close_time,source_sequence "
                "FROM bar_events WHERE sequence=?",
                (expected.sequence,),
            ).fetchone()
            if row is not None and tuple(row) != (
                expected.event_hash,
                expected.bar_sha256,
                expected.state_sha256,
                expected.close_time,
                expected.source_sequence,
            ):
                raise PublicationError("bar event receipt columns disagree")
            return expected

        return self._run_read(verify)

    def append_closed_bar(self, bar: Mapping[str, Any]) -> BarReceipt:
        """Apply and journal one strictly increasing closed bar atomically."""

        self._require_ready()
        normalized = self._normal_closed_bar(bar)
        bar_payload, bar_sha = _bounded_payload(
            normalized, self.limits.max_json_bytes, "closed bar"
        )

        def append(connection: sqlite3.Connection) -> BarReceipt:
            self._verify_mutation_invariants(connection)
            head = self._load_bar_head(connection)
            state_row, state = self._load_runtime_state(connection)
            last_close = state_row["last_closed_bar_time"]
            if last_close is not None and normalized["close_time"] == last_close:
                if state_row["last_bar_sha256"] != bar_sha:
                    raise IntegrityError(
                        "same close time was reused for a different bar"
                    )
                return BarReceipt(
                    sequence=int(head["sequence"]),
                    source_sequence=int(state_row["last_source_sequence"]),
                    event_hash=str(head["event_hash"]),
                    bar_sha256=bar_sha,
                    state_sha256=str(state_row["state_sha256"]),
                    close_time=normalized["close_time"],
                )
            if last_close is not None and _timestamp_value(
                normalized["close_time"]
            ) <= _timestamp_value(last_close):
                raise IntegrityError("closed bars must be strictly increasing")
            self._validate_source_continuity(
                normalized,
                last_close_time=last_close,
                last_source_sequence=int(state_row["last_source_sequence"]),
                last_source_session_id=state_row["last_source_session_id"],
                last_source_connection_id=state_row["last_source_connection_id"],
                last_source_phase_seconds=state_row["last_source_phase_seconds"],
            )
            if int(head["retained_events"]) + 1 > self.limits.max_bar_events:
                raise CapacityExceededError(
                    "bar event cap reached; publish a checkpoint"
                )
            if (
                int(head["retained_bytes"]) + len(bar_payload)
                > self.limits.max_bar_bytes
            ):
                raise CapacityExceededError(
                    "bar byte cap reached; publish a checkpoint"
                )
            _fault_point("before_bar_reducer")
            next_state = self._invoke_engine("apply_closed_bar", state, normalized)
            next_state_payload, next_state_sha = _bounded_payload(
                next_state, self.limits.max_state_bytes, "runtime state"
            )
            segment_id = int(head["current_segment"])
            segment_events = int(head["segment_events"])
            segment_bytes = int(head["segment_bytes"])
            if segment_events and (
                segment_events + 1 > self.limits.max_segment_events
                or segment_bytes + len(bar_payload) > self.limits.max_segment_bytes
            ):
                segment_count = int(
                    connection.execute("SELECT COUNT(*) FROM bar_segments").fetchone()[
                        0
                    ]
                )
                if segment_count >= self.limits.max_bar_segments:
                    raise CapacityExceededError(
                        "bar segment cap reached; publish a checkpoint"
                    )
                segment_id += 1
                segment_events = 0
                segment_bytes = 0
            if len(bar_payload) > self.limits.max_segment_bytes:
                raise CapacityExceededError("one bar exceeds the segment byte cap")
            sequence = int(head["sequence"]) + 1
            event_body = {
                "schema_version": BAR_EVENT_SCHEMA_VERSION,
                "sequence": sequence,
                "segment_id": segment_id,
                "previous_hash": str(head["event_hash"]),
                "bar_sha256": bar_sha,
                "close_time": normalized["close_time"],
                "source_sequence": normalized["source_sequence"],
                "source_session_id": normalized["source_session_id"],
                "source_connection_id": normalized["source_connection_id"],
                "post_state_sha256": next_state_sha,
            }
            event_hash = _canonical_sha256(event_body)
            new_segment_events = segment_events + 1
            new_segment_bytes = segment_bytes + len(bar_payload)
            existing_segment = connection.execute(
                "SELECT * FROM bar_segments WHERE segment_id=?", (segment_id,)
            ).fetchone()
            if existing_segment is None:
                segment_body = {
                    "segment_id": segment_id,
                    "first_sequence": sequence,
                    "last_sequence": sequence,
                    "event_count": 1,
                    "byte_count": len(bar_payload),
                    "first_previous_hash": str(head["event_hash"]),
                    "last_event_hash": event_hash,
                }
                connection.execute(
                    "INSERT INTO bar_segments VALUES(?,?,?,?,?,?,?,?)",
                    (
                        segment_id,
                        sequence,
                        sequence,
                        1,
                        len(bar_payload),
                        head["event_hash"],
                        event_hash,
                        self.authenticator.sign("bar-segment", segment_body),
                    ),
                )
            else:
                segment_body = {
                    "segment_id": segment_id,
                    "first_sequence": int(existing_segment["first_sequence"]),
                    "last_sequence": sequence,
                    "event_count": new_segment_events,
                    "byte_count": new_segment_bytes,
                    "first_previous_hash": str(existing_segment["first_previous_hash"]),
                    "last_event_hash": event_hash,
                }
                connection.execute(
                    "UPDATE bar_segments SET last_sequence=?,event_count=?,byte_count=?,"
                    "last_event_hash=?,mac=? WHERE segment_id=?",
                    (
                        sequence,
                        new_segment_events,
                        new_segment_bytes,
                        event_hash,
                        self.authenticator.sign("bar-segment", segment_body),
                        segment_id,
                    ),
                )
            connection.execute(
                "INSERT INTO bar_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    sequence,
                    segment_id,
                    head["event_hash"],
                    event_hash,
                    bar_sha,
                    normalized["close_time"],
                    normalized["source_sequence"],
                    normalized["source_session_id"],
                    normalized["source_connection_id"],
                    bar_payload,
                    next_state_sha,
                    self.authenticator.sign("bar-event", event_body),
                ),
            )
            state_body = {
                "bar_sequence": sequence,
                "state_sha256": next_state_sha,
                "last_closed_bar_time": normalized["close_time"],
                "last_bar_sha256": bar_sha,
                "last_source_sequence": normalized["source_sequence"],
                "last_source_session_id": normalized["source_session_id"],
                "last_source_connection_id": normalized["source_connection_id"],
                "last_source_phase_seconds": normalized["source_phase_seconds"],
            }
            connection.execute(
                "UPDATE runtime_state SET bar_sequence=?,state_payload=?,state_sha256=?,"
                "last_closed_bar_time=?,last_bar_sha256=?,last_source_sequence=?,"
                "last_source_session_id=?,last_source_connection_id=?,"
                "last_source_phase_seconds=?,mac=? WHERE singleton=1",
                (
                    sequence,
                    next_state_payload,
                    next_state_sha,
                    normalized["close_time"],
                    bar_sha,
                    normalized["source_sequence"],
                    normalized["source_session_id"],
                    normalized["source_connection_id"],
                    normalized["source_phase_seconds"],
                    self.authenticator.sign("runtime-state", state_body),
                ),
            )
            new_head = {
                "sequence": sequence,
                "event_hash": event_hash,
                "anchor_sequence": int(head["anchor_sequence"]),
                "anchor_hash": str(head["anchor_hash"]),
                "current_segment": segment_id,
                "segment_events": new_segment_events,
                "segment_bytes": new_segment_bytes,
                "retained_events": int(head["retained_events"]) + 1,
                "retained_bytes": int(head["retained_bytes"]) + len(bar_payload),
            }
            connection.execute(
                "UPDATE bar_head SET sequence=?,event_hash=?,current_segment=?,"
                "segment_events=?,segment_bytes=?,retained_events=?,retained_bytes=?,mac=? "
                "WHERE singleton=1",
                (
                    sequence,
                    event_hash,
                    segment_id,
                    new_segment_events,
                    new_segment_bytes,
                    new_head["retained_events"],
                    new_head["retained_bytes"],
                    self.authenticator.sign("bar-head", new_head),
                ),
            )
            return BarReceipt(
                sequence=sequence,
                source_sequence=normalized["source_sequence"],
                event_hash=event_hash,
                bar_sha256=bar_sha,
                state_sha256=next_state_sha,
                close_time=normalized["close_time"],
            )

        receipt = self._run_write(append)
        verified = self._verify_bar_receipt(receipt)
        self._refresh_recovery_after_mutation()
        self._require_ready()
        self._require_witness_current()
        return verified

    def _write_order_meta(
        self, connection: sqlite3.Connection, body: Mapping[str, Any]
    ) -> None:
        connection.execute(
            "UPDATE order_meta SET writer_epoch=?,max_operation_sequence=?,"
            "max_client_sequence=?,max_broker_execution_sequence=?,"
            "retired_broker_execution_sequence=?,last_broker_execution_sha256=?,"
            "retired_through_sequence=?,event_sequence=?,"
            "event_hash=?,anchor_event_sequence=?,anchor_event_hash=?,mac=? "
            "WHERE singleton=1",
            (
                body["writer_epoch"],
                body["max_operation_sequence"],
                body["max_client_sequence"],
                body["max_broker_execution_sequence"],
                body["retired_broker_execution_sequence"],
                body["last_broker_execution_sha256"],
                body["retired_through_sequence"],
                body["event_sequence"],
                body["event_hash"],
                body["anchor_event_sequence"],
                body["anchor_event_hash"],
                self.authenticator.sign("order-meta", body),
            ),
        )

    def _order_event_values(
        self,
        meta: sqlite3.Row | Mapping[str, Any],
        operation_sequence: int,
        operation_id: str,
        phase: str,
        payload: Mapping[str, Any],
    ) -> tuple[int, str, bytes, str]:
        event_payload, _ = _bounded_payload(
            payload, self.limits.max_json_bytes, "order event"
        )
        event_sequence = int(meta["event_sequence"]) + 1
        event_body = {
            "schema_version": ORDER_EVENT_SCHEMA_VERSION,
            "sequence": event_sequence,
            "operation_sequence": operation_sequence,
            "operation_id": operation_id,
            "phase": phase,
            "previous_hash": str(meta["event_hash"]),
            "payload_sha256": hashlib.sha256(event_payload).hexdigest(),
        }
        event_hash = _canonical_sha256(event_body)
        return (
            event_sequence,
            event_hash,
            event_payload,
            self.authenticator.sign("order-event", event_body),
        )

    def _insert_order_event(
        self,
        connection: sqlite3.Connection,
        meta: sqlite3.Row | Mapping[str, Any],
        operation_sequence: int,
        operation_id: str,
        phase: str,
        values: tuple[int, str, bytes, str],
    ) -> None:
        sequence, event_hash, payload, mac = values
        connection.execute(
            "INSERT INTO order_events VALUES(?,?,?,?,?,?,?,?)",
            (
                sequence,
                operation_sequence,
                operation_id,
                phase,
                meta["event_hash"],
                event_hash,
                payload,
                mac,
            ),
        )

    def _compact_orders_for_capacity(
        self, connection: sqlite3.Connection, meta: sqlite3.Row
    ) -> sqlite3.Row:
        operation_count = int(
            connection.execute("SELECT COUNT(*) FROM order_operations").fetchone()[0]
        )
        event_count = int(
            connection.execute("SELECT COUNT(*) FROM order_events").fetchone()[0]
        )
        if (
            operation_count + 1 <= self.limits.max_order_operations
            and event_count + 3 <= self.limits.max_order_events
        ):
            return meta
        pending = int(
            connection.execute(
                "SELECT COUNT(*) FROM order_operations WHERE phase!='APPLIED'"
            ).fetchone()[0]
        )
        if pending:
            raise CapacityExceededError(
                "order journal is full while recovery detail is still required"
            )
        global_row, global_state = self._load_order_book_state(connection)
        global_payload, global_sha = _bounded_payload(
            global_state, self.limits.max_state_bytes, "order book state"
        )
        anchor_body = {
            "anchor_event_sequence": int(meta["event_sequence"]),
            "last_applied_operation_sequence": int(
                global_row["applied_operation_sequence"]
            ),
            "payload_sha256": global_sha,
        }
        connection.execute(
            "UPDATE order_anchor_state SET anchor_event_sequence=?,"
            "last_applied_operation_sequence=?,payload=?,payload_sha256=?,mac=? "
            "WHERE singleton=1",
            (
                anchor_body["anchor_event_sequence"],
                anchor_body["last_applied_operation_sequence"],
                global_payload,
                global_sha,
                self.authenticator.sign("order-anchor-state", anchor_body),
            ),
        )
        connection.execute("DELETE FROM order_local_state")
        connection.execute("DELETE FROM order_events")
        connection.execute("DELETE FROM order_operations")
        body = self._order_meta_body(meta)
        body["retired_through_sequence"] = int(meta["max_operation_sequence"])
        body["retired_broker_execution_sequence"] = int(
            meta["max_broker_execution_sequence"]
        )
        body["anchor_event_sequence"] = int(meta["event_sequence"])
        body["anchor_event_hash"] = str(meta["event_hash"])
        self._write_order_meta(connection, body)
        return self._load_order_meta(connection)

    def _operation_or_retired(
        self,
        connection: sqlite3.Connection,
        operation_id: str,
        operation_sequence: int,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM order_operations WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if row is None:
            meta = self._load_order_meta(connection)
            if operation_sequence <= int(meta["retired_through_sequence"]):
                raise OrderTransitionError(
                    "operation detail was authenticated and retired"
                )
            raise OrderTransitionError("unknown order operation")
        if int(row["operation_sequence"]) != operation_sequence:
            raise OrderTransitionError("operation receipt sequence mismatch")
        self.authenticator.verify(
            "order-operation", self._operation_body(row), row["mac"]
        )
        return row

    @staticmethod
    def _intent_receipt_from_row(row: sqlite3.Row) -> OrderIntentReceipt:
        return OrderIntentReceipt(
            operation_id=str(row["operation_id"]),
            operation_sequence=int(row["operation_sequence"]),
            client_request_id=str(row["client_request_id"]),
            event_hash=str(row["intent_event_hash"]),
            request_sha256=str(row["request_sha256"]),
        )

    def begin_order_intent(
        self,
        *,
        client_sequence: int,
        action: str,
        request: Mapping[str, Any],
    ) -> OrderIntentReceipt:
        """Durably allocate an epoch-bound intent before any broker side effect.

        ``client_sequence`` is a caller-persisted, strictly monotonic idempotency
        sequence.  A retired sequence is rejected forever rather than silently
        allocating a new broker operation.
        """

        self._require_ready()
        if isinstance(client_sequence, bool) or int(client_sequence) != client_sequence:
            raise ValueError("client_sequence must be a positive integer")
        client_sequence = int(client_sequence)
        if client_sequence <= 0:
            raise ValueError("client_sequence must be a positive integer")
        action = _normal_identifier(action, "order action", upper=True)
        request_payload, request_sha = _bounded_payload(
            request, self.limits.max_json_bytes, "order request"
        )

        def begin(connection: sqlite3.Connection) -> OrderIntentReceipt:
            _, pending_recovery = self._verify_mutation_invariants(connection)
            if pending_recovery:
                raise RecoveryRequiredError(
                    "new INTENT is forbidden while prior orders need recovery"
                )
            meta = self._load_order_meta(connection)
            stream_namespace = self.storage_reservation.stream_identity_sha256[:32]
            client_request_id = (
                f"{stream_namespace}:{int(meta['writer_epoch']):020d}:"
                f"client:{client_sequence:020d}"
            )
            existing = connection.execute(
                "SELECT * FROM order_operations WHERE client_request_id=?",
                (client_request_id,),
            ).fetchone()
            if existing is not None:
                self.authenticator.verify(
                    "order-operation", self._operation_body(existing), existing["mac"]
                )
                if (
                    existing["action"] != action
                    or existing["request_sha256"] != request_sha
                ):
                    raise OrderTransitionError(
                        "client sequence was reused for a different order intent"
                    )
                return self._intent_receipt_from_row(existing)
            if client_sequence <= int(meta["max_client_sequence"]):
                raise OrderTransitionError("client sequence is stale or retired")
            if client_sequence != int(meta["max_client_sequence"]) + 1:
                raise OrderTransitionError("client sequence contains a gap or reorder")
            pending = int(
                connection.execute(
                    "SELECT COUNT(*) FROM order_operations "
                    "WHERE phase IN ('INTENT','RESULT')"
                ).fetchone()[0]
            )
            if pending >= self.limits.max_pending_orders:
                raise CapacityExceededError("pending order operation cap reached")
            meta = self._compact_orders_for_capacity(connection, meta)
            operation_sequence = int(meta["max_operation_sequence"]) + 1
            operation_id = (
                f"{stream_namespace}:{int(meta['writer_epoch']):020d}:"
                f"{operation_sequence:020d}"
            )
            event_values = self._order_event_values(
                meta,
                operation_sequence,
                operation_id,
                "INTENT",
                {
                    "action": action,
                    "client_request_id": client_request_id,
                    "request": _json_safe(dict(request)),
                },
            )
            operation_body = {
                "operation_sequence": operation_sequence,
                "operation_id": operation_id,
                "client_request_id": client_request_id,
                "action": action,
                "phase": "INTENT",
                "request_sha256": request_sha,
                "intent_event_hash": event_values[1],
                "result_sha256": None,
                "result_event_hash": None,
                "applied_event_hash": None,
                "local_state_sha256": None,
            }
            connection.execute(
                "INSERT INTO order_operations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_sequence,
                    operation_id,
                    client_request_id,
                    action,
                    "INTENT",
                    request_payload,
                    request_sha,
                    event_values[1],
                    None,
                    None,
                    None,
                    None,
                    None,
                    self.authenticator.sign("order-operation", operation_body),
                ),
            )
            self._insert_order_event(
                connection,
                meta,
                operation_sequence,
                operation_id,
                "INTENT",
                event_values,
            )
            meta_body = self._order_meta_body(meta)
            meta_body["max_operation_sequence"] = operation_sequence
            meta_body["max_client_sequence"] = client_sequence
            meta_body["event_sequence"] = event_values[0]
            meta_body["event_hash"] = event_values[1]
            self._write_order_meta(connection, meta_body)
            row = connection.execute(
                "SELECT * FROM order_operations WHERE operation_sequence=?",
                (operation_sequence,),
            ).fetchone()
            if row is None:
                raise PublicationError("order intent row vanished before commit")
            return self._intent_receipt_from_row(row)

        receipt = self._run_write(begin)
        self._run_read(
            lambda connection: self._verify_intent_receipt(connection, receipt)
        )
        self._refresh_recovery_after_mutation()
        self._require_witness_current()
        return receipt

    def _verify_intent_receipt(
        self, connection: sqlite3.Connection, receipt: OrderIntentReceipt
    ) -> None:
        row = self._operation_or_retired(
            connection, receipt.operation_id, receipt.operation_sequence
        )
        if self._intent_receipt_from_row(row) != receipt:
            raise PublicationError("order intent failed verify-after-write")
        event = connection.execute(
            "SELECT event_hash FROM order_events WHERE operation_sequence=? AND phase='INTENT'",
            (receipt.operation_sequence,),
        ).fetchone()
        if event is None or event["event_hash"] != receipt.event_hash:
            raise PublicationError("order intent event failed verify-after-write")

    def record_order_result(
        self, receipt: OrderIntentReceipt, result: AuthenticatedOrderResult
    ) -> OrderResultReceipt:
        """Record the reconciled broker result without resubmitting the intent."""

        self._ensure_open()
        if not isinstance(result, AuthenticatedOrderResult):
            raise TypeError("result must be AuthenticatedOrderResult")
        result.verify(self.broker_result_public_keys, receipt)
        result_sha = result.result_sha256

        def preflight(
            connection: sqlite3.Connection,
        ) -> Optional[OrderResultReceipt]:
            self._verify_mutation_invariants(connection)
            row = self._operation_or_retired(
                connection, receipt.operation_id, receipt.operation_sequence
            )
            if self._intent_receipt_from_row(row) != receipt:
                raise OrderTransitionError("intent receipt authentication mismatch")
            if row["phase"] not in {"INTENT", "RESULT", "APPLIED"}:
                raise OrderTransitionError("order is not awaiting a broker result")
            if row["phase"] in {"RESULT", "APPLIED"}:
                stored_record = _decode_payload(
                    row["result_payload"],
                    self.limits.max_json_bytes,
                    "authenticated order result",
                )
                stored_result, _ = self._authenticated_result_record_from_dict(
                    stored_record
                )
                if stored_result.canonical() != result.canonical():
                    raise OrderTransitionError("broker result changed on retry")
                return OrderResultReceipt(
                    operation_id=receipt.operation_id,
                    operation_sequence=receipt.operation_sequence,
                    event_hash=str(row["result_event_hash"]),
                    result_sha256=result_sha,
                    evidence_sha256=hashlib.sha256(
                        bytes(row["result_payload"])
                    ).hexdigest(),
                )
            return None

        # Never consume an account-wide execution identity for a forged or
        # absent local intent.  The external claim is durable first; a crash
        # before the SQLite commit is recovered by exact idempotent retry.
        existing_receipt = self._run_read(preflight)
        if existing_receipt is not None:
            self._run_read(
                lambda connection: self._verify_result_receipt(
                    connection, existing_receipt
                )
            )
            self._require_witness_current()
            return existing_receipt
        execution_claim: Optional[BrokerExecutionClaim] = None
        if result.result_outcome == "EXECUTED":
            execution_claim = self._claim_broker_execution(result)
        result_record = {
            "authenticated_result": result.canonical(),
            "broker_execution_claim": (
                execution_claim.canonical() if execution_claim is not None else None
            ),
        }
        result_payload, result_evidence_sha = _bounded_payload(
            result_record,
            self.limits.max_json_bytes,
            "authenticated order result",
        )

        def record(connection: sqlite3.Connection) -> OrderResultReceipt:
            self._verify_mutation_invariants(connection)
            row = self._operation_or_retired(
                connection, receipt.operation_id, receipt.operation_sequence
            )
            if self._intent_receipt_from_row(row) != receipt:
                raise OrderTransitionError("intent receipt authentication mismatch")
            if row["phase"] in {"RESULT", "APPLIED"}:
                if (
                    row["result_sha256"] != result_sha
                    or hashlib.sha256(bytes(row["result_payload"])).hexdigest()
                    != result_evidence_sha
                ):
                    raise OrderTransitionError("broker result changed on retry")
                return OrderResultReceipt(
                    operation_id=receipt.operation_id,
                    operation_sequence=receipt.operation_sequence,
                    event_hash=str(row["result_event_hash"]),
                    result_sha256=result_sha,
                    evidence_sha256=result_evidence_sha,
                )
            if row["phase"] != "INTENT":
                raise OrderTransitionError("order is not awaiting a broker result")
            meta = self._load_order_meta(connection)
            event_values = self._order_event_values(
                meta,
                receipt.operation_sequence,
                receipt.operation_id,
                "RESULT",
                result_record,
            )
            operation_body = self._operation_body(row)
            operation_body.update(
                {
                    "phase": "RESULT",
                    "result_sha256": result_sha,
                    "result_event_hash": event_values[1],
                }
            )
            connection.execute(
                "UPDATE order_operations SET phase='RESULT',result_payload=?,"
                "result_sha256=?,result_event_hash=?,mac=? WHERE operation_sequence=?",
                (
                    result_payload,
                    result_sha,
                    event_values[1],
                    self.authenticator.sign("order-operation", operation_body),
                    receipt.operation_sequence,
                ),
            )
            self._insert_order_event(
                connection,
                meta,
                receipt.operation_sequence,
                receipt.operation_id,
                "RESULT",
                event_values,
            )
            meta_body = self._order_meta_body(meta)
            if result.result_outcome == "EXECUTED":
                execution_sequence = result.broker_execution_sequence
                execution_id = result.broker_execution_id
                if execution_sequence is None or execution_id is None:
                    raise IntegrityError(
                        "EXECUTED result lacks canonical execution identity"
                    )
                if execution_sequence > int(meta["max_broker_execution_sequence"]):
                    meta_body["max_broker_execution_sequence"] = execution_sequence
                    meta_body["last_broker_execution_sha256"] = hashlib.sha256(
                        execution_id.encode("utf-8")
                    ).hexdigest()
            meta_body["event_sequence"] = event_values[0]
            meta_body["event_hash"] = event_values[1]
            self._write_order_meta(connection, meta_body)
            return OrderResultReceipt(
                operation_id=receipt.operation_id,
                operation_sequence=receipt.operation_sequence,
                event_hash=event_values[1],
                result_sha256=result_sha,
                evidence_sha256=result_evidence_sha,
            )

        result_receipt = self._run_write(record)
        self._run_read(
            lambda connection: self._verify_result_receipt(connection, result_receipt)
        )
        self._refresh_recovery_after_mutation()
        self._require_witness_current()
        return result_receipt

    def _verify_result_receipt(
        self, connection: sqlite3.Connection, receipt: OrderResultReceipt
    ) -> sqlite3.Row:
        row = self._operation_or_retired(
            connection, receipt.operation_id, receipt.operation_sequence
        )
        if row["phase"] not in {"RESULT", "APPLIED"}:
            raise OrderTransitionError("operation has no durable broker result")
        if (
            row["result_event_hash"] != receipt.event_hash
            or row["result_sha256"] != receipt.result_sha256
            or hashlib.sha256(bytes(row["result_payload"])).hexdigest()
            != receipt.evidence_sha256
        ):
            raise OrderTransitionError("result receipt authentication mismatch")
        return row

    def apply_order_result_state(
        self, receipt: OrderResultReceipt
    ) -> OrderAppliedReceipt:
        """Deterministically apply RESULT to local order state and APPLIED atomically."""

        self._ensure_open()

        def apply(connection: sqlite3.Connection) -> OrderAppliedReceipt:
            self._verify_mutation_invariants(connection)
            row = self._verify_result_receipt(connection, receipt)
            if row["phase"] == "APPLIED":
                return OrderAppliedReceipt(
                    receipt.operation_id,
                    receipt.operation_sequence,
                    str(row["applied_event_hash"]),
                    str(row["local_state_sha256"]),
                    receipt.event_hash,
                )
            request = _decode_payload(
                row["request_payload"], self.limits.max_json_bytes, "order request"
            )
            result_evidence_payload = _decode_payload(
                row["result_payload"],
                self.limits.max_json_bytes,
                "authenticated order result",
            )
            result_evidence, _ = self._authenticated_result_record_from_dict(
                result_evidence_payload
            )
            result_evidence.verify(
                self.broker_result_public_keys,
                self._intent_receipt_from_row(row),
            )
            global_row, current_state = self._load_order_book_state(connection)
            next_state = self._invoke_engine(
                "apply_order_result",
                current_state,
                str(row["action"]),
                request,
                result_evidence.result,
            )
            state_payload, state_sha = _bounded_payload(
                next_state, self.limits.max_state_bytes, "order book state"
            )
            meta = self._load_order_meta(connection)
            event_values = self._order_event_values(
                meta,
                receipt.operation_sequence,
                receipt.operation_id,
                "APPLIED",
                {
                    "local_state_sha256": state_sha,
                    "result_event_hash": receipt.event_hash,
                },
            )
            local_body = {
                "operation_sequence": receipt.operation_sequence,
                "operation_id": receipt.operation_id,
                "payload_sha256": state_sha,
                "result_event_hash": receipt.event_hash,
            }
            connection.execute(
                "INSERT INTO order_local_state VALUES(?,?,?,?,?,?)",
                (
                    receipt.operation_sequence,
                    receipt.operation_id,
                    state_payload,
                    state_sha,
                    receipt.event_hash,
                    self.authenticator.sign("order-local-state", local_body),
                ),
            )
            book_body = {
                "applied_operation_sequence": receipt.operation_sequence,
                "payload_sha256": state_sha,
            }
            connection.execute(
                "UPDATE order_book_state SET applied_operation_sequence=?,payload=?,"
                "payload_sha256=?,mac=? WHERE singleton=1",
                (
                    receipt.operation_sequence,
                    state_payload,
                    state_sha,
                    self.authenticator.sign("order-book-state", book_body),
                ),
            )
            operation_body = self._operation_body(row)
            operation_body.update(
                {
                    "phase": "APPLIED",
                    "applied_event_hash": event_values[1],
                    "local_state_sha256": state_sha,
                }
            )
            connection.execute(
                "UPDATE order_operations SET phase='APPLIED',applied_event_hash=?,"
                "local_state_sha256=?,mac=? WHERE operation_sequence=?",
                (
                    event_values[1],
                    state_sha,
                    self.authenticator.sign("order-operation", operation_body),
                    receipt.operation_sequence,
                ),
            )
            self._insert_order_event(
                connection,
                meta,
                receipt.operation_sequence,
                receipt.operation_id,
                "APPLIED",
                event_values,
            )
            meta_body = self._order_meta_body(meta)
            meta_body["event_sequence"] = event_values[0]
            meta_body["event_hash"] = event_values[1]
            self._write_order_meta(connection, meta_body)
            del global_row
            return OrderAppliedReceipt(
                receipt.operation_id,
                receipt.operation_sequence,
                event_values[1],
                state_sha,
                receipt.event_hash,
            )

        applied = self._run_write(apply)
        self._run_read(
            lambda connection: self._verify_applied_receipt(connection, applied)
        )
        self._refresh_recovery_after_mutation()
        self._require_ready()
        self._require_witness_current()
        return applied

    def _verify_applied_receipt(
        self, connection: sqlite3.Connection, receipt: OrderAppliedReceipt
    ) -> None:
        row = self._operation_or_retired(
            connection, receipt.operation_id, receipt.operation_sequence
        )
        if (
            row["phase"] != "APPLIED"
            or row["applied_event_hash"] != receipt.event_hash
            or row["local_state_sha256"] != receipt.local_state_sha256
            or row["result_event_hash"] != receipt.result_event_hash
        ):
            raise PublicationError("APPLIED receipt failed verify-after-write")
        self._verify_order_heads_incremental(connection)

    def pending_order_recovery(self) -> tuple[OrderRecovery, ...]:
        """Return bounded exact recovery work; never infer broker outcomes."""

        self._ensure_open()
        return self._run_read(self._verify_order_chain)


__all__ = [
    "AuthenticatedCadenceReceipt",
    "AuthenticatedOrderResult",
    "AuthenticatedReducerRelease",
    "AuthenticatedStateEvidence",
    "AuthenticatedStateRuntime",
    "AuthenticationError",
    "BarReceipt",
    "BindingMismatchError",
    "CapacityExceededError",
    "CheckpointSnapshot",
    "DeterministicStateEngine",
    "HMACAuthenticator",
    "IntegrityError",
    "LockTimeoutError",
    "OrderAppliedReceipt",
    "OrderIntentReceipt",
    "OrderRecovery",
    "OrderResultReceipt",
    "OrderTransitionError",
    "OperationalStorageAuthority",
    "PublicationError",
    "RecoveryRequiredError",
    "RuntimeLimits",
    "RuntimeRecovery",
    "SourceEpoch",
    "StateRuntimeError",
    "StorageLocationError",
    "StorageReservation",
    "WinnerRecord",
    "WinnerRoster",
    "validate_local_storage_root",
]
