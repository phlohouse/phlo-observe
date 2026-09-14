"""Value normalization and canonical JSON serialization.

Serialization uses ``orjson``: it is measurably faster than stdlib ``json``,
natively rejects non-finite floats by normalizing them to ``null`` (handled
explicitly below so the behaviour is documented, not accidental), and emits
deterministic UTF-8 output without relying on ``repr``.

Normalization rules (applied before any drain sees an event):

- ``datetime``/``date``/``time`` -> ISO 8601 strings (UTC for datetimes)
- ``UUID`` -> ``str``
- ``Path`` -> ``str``
- ``bytes`` -> UTF-8 text when decodable, else ``{"_observe_b64": ...}``
- ``Enum`` -> its value
- dataclasses -> ``asdict`` result, Pydantic models -> ``model_dump``
- mappings/sequences/sets -> normalized recursively up to ``max_depth``
- pandas/polars/pyarrow tabular objects -> a small summary marker, never rows
- anything else -> an explicit unserializable marker with the type name

Arbitrary ``repr()`` output is never used: it can leak secrets.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import enum
import math
import uuid
from collections.abc import Mapping, Sequence, Set
from pathlib import Path
from typing import Any

import orjson
from pydantic import BaseModel

from observe_core.timestamps import format_rfc3339

DEFAULT_MAX_DEPTH = 8
"""Default maximum nesting depth for normalized attribute values."""

_TABULAR_MODULES = ("pandas", "polars", "pyarrow", "dask", "modin")
"""Modules whose tabular objects are summarized instead of serialized."""


def _is_tabular(value: Any) -> bool:
    """Detect dataframe-like objects without importing the heavy libraries."""
    module = type(value).__module__.partition(".")[0]
    return module in _TABULAR_MODULES and hasattr(value, "shape")


def _tabular_summary(value: Any) -> dict[str, Any]:
    """Return a small, safe summary of a dataframe-like object."""
    summary: dict[str, Any] = {
        "_observe_summary": "tabular",
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
    }
    try:
        shape = getattr(value, "shape", None)
        if shape is not None:
            summary["rows"] = int(shape[0])
            summary["columns"] = int(shape[1])
        columns = getattr(value, "columns", None)
        if columns is not None:
            summary["column_names"] = [str(c) for c in list(columns)[:50]]
    except Exception:  # noqa: S110 - summarization must never break emit
        pass
    return summary


def normalize_value(value: Any, *, max_depth: int = DEFAULT_MAX_DEPTH, _depth: int = 0) -> Any:
    """Normalize an arbitrary value into JSON-safe data.

    Returns only ``dict``/``list``/``str``/``int``/``float``/``bool``/``None``
    so the result always round-trips through orjson.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        # Explicit normalization: NaN and infinities become null rather than
        # producing invalid JSON or raising.
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dt.datetime):
        return format_rfc3339(value)
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, enum.Enum):
        return normalize_value(value.value, max_depth=max_depth, _depth=_depth + 1)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return {"_observe_b64": base64.b64encode(raw).decode("ascii")}
    if _depth >= max_depth:
        return {"_observe_truncated": f"max_depth {max_depth} exceeded"}
    if _is_tabular(value):
        return _tabular_summary(value)
    if isinstance(value, BaseModel):
        return normalize_value(
            value.model_dump(mode="python"), max_depth=max_depth, _depth=_depth + 1
        )
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return normalize_value(dataclasses.asdict(value), max_depth=max_depth, _depth=_depth + 1)
    if isinstance(value, Mapping):
        return {
            str(k): normalize_value(v, max_depth=max_depth, _depth=_depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (Sequence, Set)) and not isinstance(value, str):
        return [normalize_value(v, max_depth=max_depth, _depth=_depth + 1) for v in value]
    return {"_observe_unserializable": f"{type(value).__module__}.{type(value).__qualname__}"}


def dumps(value: Any) -> bytes:
    """Serialize normalized data to canonical UTF-8 JSON bytes."""
    return orjson.dumps(value)


def loads(data: bytes | str) -> Any:
    """Parse canonical JSON."""
    return orjson.loads(data)


def dumps_line(value: Any) -> bytes:
    r"""Serialize to a single JSON line terminated with ``\n``."""
    return orjson.dumps(value, option=orjson.OPT_APPEND_NEWLINE)
