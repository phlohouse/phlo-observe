"""Schema registry: export registered contracts and classify changes (spec §7.3, §44).

The SDK side of the registry lives in :mod:`observe_core.contracts`
(``register_contract``/``get_contract``/``all_contracts``). This module adds:

- :func:`export_schemas` — a serializable snapshot for shipping to the
  observer or committing to ``schemas/``;
- :func:`diff_contracts` — breaking vs non-breaking classification between
  two registered versions of the same contract, following §44:

  non-breaking: add optional field, expand enum safely, add metadata;
  breaking: rename/remove field, change meaning, incompatible type change,
  identifier-semantics change.
"""

from __future__ import annotations

import types
import typing
from typing import Annotated, Any, get_args, get_origin

from observe_core.contracts import ContractSpec, all_contracts

SCHEMA_REGISTRY_VERSION = 1


def export_schemas() -> dict[str, Any]:
    """Serialize every registered contract as a registry document.

    The document is deterministic (sorted, content-hashed) so it can be
    diffed in review or stored in the observer's ``observe_schema_registry``
    table (spec §24.2).
    """
    entries = [
        {
            "schema_id": spec.schema_id,
            "event": spec.name,
            "version": spec.version,
            "schema_hash": spec.schema_hash,
            "schema": spec.json_schema(),
        }
        for spec in all_contracts()
    ]
    return {"registry_version": SCHEMA_REGISTRY_VERSION, "schemas": entries}


def diff_contracts(old: ContractSpec, new: ContractSpec) -> dict[str, Any]:
    """Classify the change between two versions of one contract.

    Returns ``{"breaking": [...], "non_breaking": [...], "compatible": bool}``:
    ``compatible`` is False when any breaking change exists — the observer may
    reject the newer schema only when configured to enforce compatibility.
    """
    breaking: list[str] = []
    non_breaking: list[str] = []
    old_fields = old.fields
    new_fields = new.fields
    breaking.extend(f"removed field {name!r}" for name in old_fields.keys() - new_fields.keys())
    for name in new_fields.keys() - old_fields.keys():
        spec = new_fields[name]
        if spec.required:
            breaking.append(f"added required field {name!r}")
        else:
            non_breaking.append(f"added optional field {name!r}")
    for name in old_fields.keys() & new_fields.keys():
        old_f, new_f = old_fields[name], new_fields[name]
        if not _types_compatible(old_f.py_type, new_f.py_type):
            breaking.append(
                f"field {name!r} changed type incompatibly: "
                f"{_name(old_f.py_type)} -> {_name(new_f.py_type)}"
            )
        elif _type_widened(old_f.py_type, new_f.py_type):
            non_breaking.append(f"field {name!r} widened type")
        if not old_f.required and new_f.required:
            breaking.append(f"field {name!r} became required")
        elif old_f.required and not new_f.required:
            non_breaking.append(f"field {name!r} became optional")
        if old_f.metadata.sensitive != new_f.metadata.sensitive:
            non_breaking.append(f"field {name!r} sensitivity flag changed")
        if old_f.metadata.deprecated != new_f.metadata.deprecated:
            non_breaking.append(f"field {name!r} deprecation changed")
    return {
        "schema_id_old": old.schema_id,
        "schema_id_new": new.schema_id,
        "breaking": breaking,
        "non_breaking": non_breaking,
        "compatible": not breaking,
    }


def _name(py_type: Any) -> str:
    return getattr(py_type, "__name__", str(py_type))


def _base_type(py_type: Any) -> Any:
    """Unwrap ``Annotated``/``Optional`` to the concrete inner type."""
    if get_origin(py_type) is Annotated:
        return _base_type(get_args(py_type)[0])
    origin = get_origin(py_type)
    if origin in (types.UnionType, typing.Union):
        non_none = [a for a in get_args(py_type) if a is not type(None)]
        if len(non_none) == 1:
            return _base_type(non_none[0])
        return py_type
    return py_type


def _types_compatible(old: Any, new: Any) -> bool:
    """True when ``new`` accepts everything ``old`` did (same or widened)."""
    old_b, new_b = _base_type(old), _base_type(new)
    if old_b == new_b:
        return True
    # int -> float is the classic safe widening.
    if old_b is int and new_b is float:
        return True
    # Union that newly includes the old type is a widening.
    new_origin = get_origin(new_b)
    if new_origin in (types.UnionType, typing.Union):
        return any(_types_compatible(old_b, arg) for arg in get_args(new_b))
    return False


def _type_widened(old: Any, new: Any) -> bool:
    return _base_type(old) != _base_type(new)


def sensitive_attribute_paths() -> dict[str, list[str]]:
    """Map event names to attribute keys contracts marked ``sensitive``.

    The redactor consumes this: contract-marked fields are always redacted
    regardless of key-name heuristics (spec §34).
    """
    return {
        spec.name: sorted(spec.sensitive_fields)
        for spec in all_contracts()
        if spec.sensitive_fields
    }
