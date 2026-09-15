"""Typed event contracts and the contract registry (spec §7.2).

Applications register :class:`EventContract` subclasses for important event
families. A contract names the event, pins a schema ``version``, and declares
typed fields with metadata: descriptions, sensitive markers, cardinality
hints, deprecation notes and examples::

    from observe_core import EventContract, field


    class AssetMaterialized(EventContract):
        name = "asset.materialized"
        version = 2

        asset: str = field(description="Canonical asset key")
        rows: int = field(description="Rows written", cardinality="bounded")
        duration_ms: float = 0.0
        partition: str | None = None

Contract validation is configurable per ``ObserveSettings.contract_validation``
— ``off`` / ``warn`` (production default) / ``strict``. ``warn`` records a
diagnostic and a counter but never fails application work; ``strict`` raises
:class:`ContractViolation` when explicitly enabled.
"""

from __future__ import annotations

import dataclasses
import hashlib
import threading
import types
import typing
from typing import Annotated, Any, ClassVar, Literal, get_args, get_origin, get_type_hints

import orjson
from pydantic import BaseModel, ConfigDict

ValidationMode = Literal["off", "warn", "strict"]


class ContractViolation(Exception):
    """Raised in ``strict`` mode when an event breaks its registered contract."""


@dataclasses.dataclass(frozen=True)
class FieldMeta:
    """Declarative metadata carried by one contract field."""

    description: str | None = None
    sensitive: bool = False
    """Marked fields are always redacted regardless of key-name rules."""
    cardinality: Literal["low", "bounded", "high"] | None = None
    """Hint for storage/query planning: ``low`` = few values, ``bounded`` =
    moderate, ``high`` = unbounded (never index or label by it)."""
    deprecated: str | None = None
    """Deprecation note; set to a reason string when the field is phased out."""
    examples: tuple[Any, ...] = ()


def field(
    default: Any = dataclasses.MISSING,
    *,
    description: str | None = None,
    sensitive: bool = False,
    cardinality: Literal["low", "bounded", "high"] | None = None,
    deprecated: str | None = None,
    examples: tuple[Any, ...] | list[Any] = (),
) -> Any:
    """Declare a contract field with metadata.

    Mirrors :func:`dataclasses.field`: positional ``default`` (or
    ``default_factory=``) makes the field optional; no default makes it
    required at validation time.
    """
    meta = FieldMeta(
        description=description,
        sensitive=sensitive,
        cardinality=cardinality,
        deprecated=deprecated,
        examples=tuple(examples),
    )
    kwargs: dict[str, Any] = {"metadata": {"observe_contract": meta}}
    if default is not dataclasses.MISSING:
        kwargs["default"] = default
    return dataclasses.field(**kwargs)


class FieldSpec:
    """One validated contract field: name, requiredness, type, metadata."""

    __slots__ = ("metadata", "name", "py_type", "required")

    def __init__(self, name: str, py_type: Any, required: bool, metadata: FieldMeta) -> None:
        self.name = name
        self.py_type = py_type
        self.required = required
        self.metadata = metadata


class ContractSpec:
    """Compiled form of an :class:`EventContract` subclass."""

    def __init__(self, contract: type[EventContract]) -> None:
        self.contract = contract
        self.name = contract.name
        self.version = contract.version
        self.description = contract.description
        self.fields = _compile_fields(contract)

    @property
    def schema_id(self) -> str:
        """Stable identifier: ``<event name>/<major version>``."""
        return f"{self.name}/v{self.version}"

    @property
    def schema_hash(self) -> str:
        """Reproducible content hash over the JSON Schema representation."""
        canonical = orjson.dumps(self.json_schema(), option=orjson.OPT_SORT_KEYS)
        return hashlib.sha256(canonical).hexdigest()

    @property
    def sensitive_fields(self) -> frozenset[str]:
        """Field names marked ``sensitive`` — always redacted."""
        return frozenset(name for name, spec in self.fields.items() if spec.metadata.sensitive)

    def json_schema(self) -> dict[str, Any]:
        """Export the contract's attribute shape as JSON Schema (spec §7.3)."""
        properties: dict[str, Any] = {}
        required: list[str] = []
        for name, spec in self.fields.items():
            node = _json_type(spec.py_type)
            meta = spec.metadata
            if meta.description:
                node["description"] = meta.description
            if meta.sensitive:
                node["x-sensitive"] = True
            if meta.cardinality:
                node["x-cardinality"] = meta.cardinality
            if meta.deprecated:
                node["deprecated"] = True
                node["x-deprecation-note"] = meta.deprecated
            if meta.examples:
                node["examples"] = list(meta.examples)
            properties[name] = node
            if spec.required:
                required.append(name)
        schema: dict[str, Any] = {
            "$id": f"phlo-observe://contracts/{self.schema_id}",
            "title": self.name,
            "type": "object",
            "properties": properties,
            "additionalProperties": True,
            "x-schema-version": self.version,
        }
        if required:
            schema["required"] = required
        if self.description:
            schema["description"] = self.description
        return schema

    def validate_attributes(self, attributes: dict[str, Any]) -> list[str]:
        """Check attributes against the contract; return violation messages."""
        violations: list[str] = []
        for name, spec in self.fields.items():
            if spec.required and name not in attributes:
                violations.append(f"missing required field {name!r}")
                continue
            if name in attributes and not _matches(attributes[name], spec.py_type):
                violations.append(
                    f"field {name!r} expected {_type_name(spec.py_type)}, "
                    f"got {type(attributes[name]).__name__}"
                )
        return violations


class _ContractModel(BaseModel):
    """Scratch model used to JSON-encode example/default values."""

    model_config = ConfigDict(extra="allow")


class EventContract:
    """Base class for typed event contracts.

    Class attributes::

        name        canonical event name (required)
        version     integer schema version (required, bump on change)
        description human description of the event family

    Annotated attributes become contract fields. Class-level attributes named
    like fields but annotated ``ClassVar`` are ignored.
    """

    name: ClassVar[str]
    version: ClassVar[int] = 1
    description: ClassVar[str | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Compile and register the contract when a subclass is defined."""
        super().__init_subclass__(**kwargs)
        if cls.__dict__.get("__abstract__"):
            return
        if "name" not in cls.__dict__ or "version" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must declare class attributes `name` and `version`")
        spec = ContractSpec(cls)
        cls._spec = spec  # type: ignore[attr-defined]
        register_contract(spec)

    @classmethod
    def spec(cls) -> ContractSpec:
        """The compiled :class:`ContractSpec` for this contract."""
        return cls._spec  # type: ignore[attr-defined]


def _compile_fields(contract: type[EventContract]) -> dict[str, FieldSpec]:
    fields: dict[str, FieldSpec] = {}
    hints = get_type_hints(contract, include_extras=True)
    for name, py_type in hints.items():
        if name.startswith("_") or get_origin(py_type) is ClassVar:
            continue
        meta = _field_meta(contract, name)
        required = not _has_default(contract, name) and not _is_optional(py_type)
        fields[name] = FieldSpec(name, py_type, required, meta)
    return fields


def _field_meta(contract: type[EventContract], name: str) -> FieldMeta:
    """Extract :class:`FieldMeta` set via ``field()`` or ``Annotated``."""
    default = contract.__dict__.get(name, dataclasses.MISSING)
    if isinstance(default, dataclasses.Field):
        found = default.metadata.get("observe_contract")
        if isinstance(found, FieldMeta):
            return found
    hints = get_type_hints(contract, include_extras=True)
    annotation = hints.get(name)
    if get_origin(annotation) is Annotated:
        for extra in get_args(annotation)[1:]:
            if isinstance(extra, FieldMeta):
                return extra
    return FieldMeta()


def _has_default(contract: type[EventContract], name: str) -> bool:
    if name not in contract.__dict__:
        return False
    default = contract.__dict__[name]
    if isinstance(default, dataclasses.Field):
        return (
            default.default is not dataclasses.MISSING
            or default.default_factory is not dataclasses.MISSING
        )
    return True


def _is_optional(py_type: Any) -> bool:
    origin = get_origin(py_type)
    if origin in (types.UnionType, typing.Union):
        return type(None) in get_args(py_type)
    return False


def _matches(value: Any, py_type: Any) -> bool:
    """Structural type check for contract validation (not full pydantic)."""
    if value is None:
        return _is_optional(py_type) or py_type is None or py_type is type(None)
    if get_origin(py_type) is Annotated:
        return _matches(value, get_args(py_type)[0])
    origin = get_origin(py_type)
    if origin in (types.UnionType, typing.Union):
        return any(_matches(value, arg) for arg in get_args(py_type))
    if origin in (list, tuple, set, frozenset):
        if not isinstance(value, origin if origin is not frozenset else (set, frozenset)):
            return False
        args = get_args(py_type)
        if not args or args == ((),):
            return True
        item_type = (
            args[0]
            if origin is not tuple
            else (args[0] if len(args) == 2 and args[1] is Ellipsis else None)
        )
        if item_type is None:
            # tuple[T, U, ...] heterogeneous: check pairwise
            return len(value) == len(args) and all(
                _matches(v, t) for v, t in zip(value, args, strict=True)
            )
        return all(_matches(item, item_type) for item in value)
    if origin is dict:
        if not isinstance(value, dict):
            return False
        args = get_args(py_type)
        if len(args) != 2:
            return True
        key_t, val_t = args
        return all(_matches(k, key_t) and _matches(v, val_t) for k, v in value.items())
    if origin is not None:
        # Generic alias we don't model deeply: accept by container type.
        try:
            return isinstance(value, origin)
        except TypeError:
            return True
    if py_type is Any or py_type is object:
        return True
    if isinstance(py_type, type):
        # bool is a subclass of int in Python; keep them distinct.
        if py_type is int and isinstance(value, bool):
            return False
        if py_type is float and isinstance(value, bool):
            return False
        return isinstance(value, py_type)
    return True


def _type_name(py_type: Any) -> str:
    return getattr(py_type, "__name__", str(py_type))


def _json_type(py_type: Any) -> dict[str, Any]:
    """Map a Python annotation to a JSON Schema fragment."""
    if get_origin(py_type) is Annotated:
        return _json_type(get_args(py_type)[0])
    origin = get_origin(py_type)
    if origin in (types.UnionType, typing.Union):
        args = list(get_args(py_type))
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1 and len(args) == 2:
            node = _json_type(non_none[0])
            typ = node.get("type")
            if isinstance(typ, str):
                node["type"] = sorted({typ, "null"})
            elif isinstance(typ, list):
                node["type"] = sorted({*typ, "null"})
            else:
                # No scalar type to union with: express via anyOf.
                return {"anyOf": [node, {"type": "null"}]}
            return node
        return {"anyOf": [_json_type(arg) for arg in args]}
    if origin in (list, set, frozenset):
        args = get_args(py_type)
        return {"type": "array", "items": _json_type(args[0]) if args else {}}
    if origin is tuple:
        return {"type": "array"}
    if origin is dict:
        return {"type": "object"}
    return {
        str: {"type": "string"},
        bool: {"type": "boolean"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bytes: {"type": "string", "contentEncoding": "base64"},
        dict: {"type": "object"},
        list: {"type": "array"},
    }.get(py_type, {})


# -- registry ----------------------------------------------------------------

_registry: dict[str, ContractSpec] = {}
_registry_lock = threading.Lock()


def register_contract(spec: ContractSpec) -> None:
    """Register a compiled contract under ``<name>/v<version>``."""
    with _registry_lock:
        existing = _registry.get(spec.schema_id)
        if existing is not None and existing.schema_hash != spec.schema_hash:
            raise ValueError(
                f"contract {spec.schema_id} already registered with different fields; "
                "bump `version` for a schema change"
            )
        _registry[spec.schema_id] = spec


def get_contract(name: str, version: int | None = None) -> ContractSpec | None:
    """Look up a contract; latest registered version when ``version`` is None."""
    with _registry_lock:
        if version is not None:
            return _registry.get(f"{name}/v{version}")
        candidates = [spec for key, spec in _registry.items() if spec.name == name]
        if not candidates:
            return None
        return max(candidates, key=lambda spec: spec.version)


def all_contracts() -> list[ContractSpec]:
    """All registered contracts, sorted by name then version."""
    with _registry_lock:
        return sorted(_registry.values(), key=lambda spec: (spec.name, spec.version))


def clear_contracts() -> None:
    """Reset the registry (tests only)."""
    with _registry_lock:
        _registry.clear()


def validate_event_attributes(
    event_name: str,
    attributes: dict[str, Any],
    *,
    contract_version: int | None = None,
    mode: ValidationMode = "warn",
) -> list[str]:
    """Validate attributes against a registered contract.

    Returns violation messages. In ``strict`` mode raises
    :class:`ContractViolation` instead; in ``off`` mode always returns empty.
    ``warn`` is the production default: violations surface as diagnostics,
    not failures.
    """
    if mode == "off":
        return []
    spec = get_contract(event_name, contract_version)
    if spec is None:
        return []
    violations = spec.validate_attributes(attributes)
    if violations and mode == "strict":
        raise ContractViolation(
            f"event {event_name!r} violates contract {spec.schema_id}: " + "; ".join(violations)
        )
    return violations
