"""Events validate against the canonical JSON Schemas."""

from __future__ import annotations

import json

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaError
from observe_core import SourceInfo, event, flush, observe
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012


def _events(drain: MemoryDrain) -> list[dict]:
    flush(2.0)
    return [e.data for e in drain.events]


@pytest.fixture
def registry(schema_dir):
    """A referencing Registry resolving the sibling schema $refs."""
    resources = []
    for path in schema_dir.glob("*.schema.json"):
        schema = json.loads(path.read_text())
        resources.append(
            (schema["$id"], Resource.from_contents(schema, default_specification=DRAFT202012))
        )
    return Registry().with_resources(resources)


def test_emitted_event_matches_envelope_schema(
    captured: tuple[Runtime, MemoryDrain], envelope_schema, registry
):
    _, drain = captured
    with observe("asset.materialize", category="data") as evt:
        evt.set_correlation(run_id="R1", asset_key="warehouse.orders")
        evt.set(rows=10)
    (ev,) = _events(drain)
    Draft202012Validator(envelope_schema, registry=registry).validate(ev)


def test_error_event_matches_schemas(
    captured: tuple[Runtime, MemoryDrain], envelope_schema, registry
):
    _, drain = captured
    try:
        with observe("quality.validate", category="quality"):
            raise RuntimeError("validation failed")
    except RuntimeError:
        pass
    (ev,) = _events(drain)
    Draft202012Validator(envelope_schema, registry=registry).validate(ev)
    assert ev["error"]["exception_type"] == "RuntimeError"


def test_source_event_matches_schema(
    captured: tuple[Runtime, MemoryDrain], envelope_schema, source_schema, registry
):
    _, drain = captured
    event(
        "external.asset_materialization",
        source=SourceInfo(producer="dagster", kind="ASSET_MATERIALIZATION"),
    )
    (ev,) = _events(drain)
    Draft202012Validator(envelope_schema, registry=registry).validate(ev)
    Draft202012Validator(source_schema, registry=registry).validate(ev["source"])


def test_schema_rejects_bad_envelope(envelope_schema, registry):
    with pytest.raises(SchemaError):
        Draft202012Validator(envelope_schema, registry=registry).validate({"event": "x.y"})


def test_schema_files_are_valid_json(schema_dir):
    for path in schema_dir.glob("*.schema.json"):
        parsed = json.loads(path.read_text())
        assert parsed["$schema"].startswith("http")
