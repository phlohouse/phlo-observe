"""Value normalization and canonical JSON tests."""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import math
import uuid
from pathlib import Path

from observe_core.serialization import dumps, loads, normalize_value
from observe_core.timestamps import UTC
from pydantic import BaseModel


class _Color(enum.Enum):
    RED = "red"


@dataclasses.dataclass
class _Point:
    x: int
    y: int


class _Model(BaseModel):
    a: int
    b: str


def test_scalars_passthrough():
    assert normalize_value(None) is None
    assert normalize_value(True) is True
    assert normalize_value(7) == 7
    assert normalize_value(1.5) == 1.5
    assert normalize_value("s") == "s"


def test_nan_and_infinity_become_null():
    assert normalize_value(math.nan) is None
    assert normalize_value(math.inf) is None
    assert normalize_value(-math.inf) is None


def test_datetime_uuid_path_enum():
    when = dt.datetime(2026, 9, 14, 1, 2, 3, tzinfo=UTC)
    uid = uuid.uuid4()
    assert normalize_value(when) == "2026-09-14T01:02:03.000Z"
    assert normalize_value(uid) == str(uid)
    assert normalize_value(Path("/tmp/x")) == "/tmp/x"
    assert normalize_value(_Color.RED) == "red"


def test_bytes():
    assert normalize_value(b"hello") == "hello"
    binary = normalize_value(b"\x89PNG\x00")
    assert "_observe_b64" in binary


def test_dataclass_and_pydantic():
    assert normalize_value(_Point(1, 2)) == {"x": 1, "y": 2}
    assert normalize_value(_Model(a=1, b="x")) == {"a": 1, "b": "x"}


def test_nested_and_sets():
    out = normalize_value({"a": [1, {2, 3}], "b": {"c": (4, 5)}})
    assert out["a"][0] == 1
    assert sorted(out["a"][1]) == [2, 3]
    assert out["b"]["c"] == [4, 5]


def test_max_depth_marker():
    deep = {"l": {"l": {"l": {"l": {"l": {"l": {"l": {"l": {"l": "x"}}}}}}}}}
    out = normalize_value(deep, max_depth=4)
    node = out
    for _ in range(4):
        node = node["l"]
    assert "_observe_truncated" in node


class _FakeFrame:
    """Duck-typed dataframe stand-in."""

    __module__ = "pandas.core.frame"

    def __init__(self) -> None:
        self.shape = (10, 3)
        self.columns = ["a", "b", "c"]


def test_dataframe_not_serialized():
    out = normalize_value(_FakeFrame())
    assert out["_observe_summary"] == "tabular"
    assert out["rows"] == 10
    assert out["column_names"] == ["a", "b", "c"]


def test_arbitrary_object_gets_marker_not_repr():
    class Secret:
        def __repr__(self) -> str:
            return "SECRET_VALUE"

    out = normalize_value(Secret())
    assert out["_observe_unserializable"].endswith("Secret")
    assert "SECRET_VALUE" not in str(out)


def test_dumps_loads_roundtrip():
    data = {"a": 1, "b": ["x", "y"], "c": {"d": None}}
    assert loads(dumps(data)) == data
    assert isinstance(dumps(data), bytes)
