"""Contract tests: emitted events validate against the published schemas.

These pin the wire format: if observe-core ever emits a payload that fails
`schemas/event-envelope-v2.schema.json`, this suite fails before a deploy
breaks consumers. V1 envelopes (`schema_version` 1.x, no V2 extensions) must
additionally still validate against `event-envelope-v1.schema.json` — V2 is
additive (spec §43).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from observe_core.ids import new_event_id
from observe_core.models import EventEnvelope
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"


def _registry() -> Registry:
    resources = []
    for path in SCHEMA_DIR.glob("*.schema.json"):
        schema = json.loads(path.read_text())
        resources.append(
            (path.name, Resource.from_contents(schema, default_specification=DRAFT202012))
        )
    return Registry().with_resources(resources)


@pytest.fixture(scope="module")
def envelope_validator() -> Validator:
    """Validator for the current emitted envelope (V2)."""
    schema = json.loads((SCHEMA_DIR / "event-envelope-v2.schema.json").read_text())
    return Draft202012Validator(schema, registry=_registry())


@pytest.fixture(scope="module")
def v1_validator() -> Validator:
    """Validator for the legacy V1 envelope (backward-compatibility pin)."""
    schema = json.loads((SCHEMA_DIR / "event-envelope-v1.schema.json").read_text())
    return Draft202012Validator(schema, registry=_registry())


def test_minimal_envelope(envelope_validator: Validator) -> None:
    env = EventEnvelope(
        event_id=new_event_id(),
        event="pipeline.run",
        category="pipeline",
        outcome="success",
        severity="info",
        delivery="telemetry",
        observed_at="2025-01-01T00:00:00Z",
        service={"name": "contract-test"},
    )
    envelope_validator.validate(env.to_canonical_dict())


def test_full_envelope(envelope_validator: Validator) -> None:
    env = EventEnvelope(
        event_id=new_event_id(),
        event="wap.promote",
        category="wap",
        outcome="success",
        severity="info",
        delivery="critical",
        observed_at="2025-01-01T00:00:00Z",
        started_at="2025-01-01T00:00:00Z",
        ended_at="2025-01-01T00:01:00Z",
        duration_ms=60000,
        service={"name": "svc", "version": "1.2.3", "environment": "prod"},
        correlation={
            "run_id": "r-1",
            "root_run_id": "root-r-1",
            "trace_id": uuid.uuid4().hex,
            "asset_key": "mart.fct_orders",
            "branch": "wap/audit",
            "table": "mart.fct_orders",
            "snapshot_id": "42",
        },
        attributes={"rows_written": 10},
        error=None,
        source={"producer": "contract-test", "adapter": "canonical.1.0"},
    )
    envelope_validator.validate(env.to_canonical_dict())


def test_every_event_field_survives_roundtrip(
    envelope_validator: Validator,
) -> None:
    """Serialization round-trip stays schema-valid (the wire format)."""
    env = EventEnvelope(
        event_id=new_event_id(),
        event="pipeline.step",
        category="pipeline",
        outcome="failure",
        severity="error",
        delivery="telemetry",
        observed_at="2025-06-30T12:00:00Z",
        service={"name": "svc"},
        correlation={"run_id": "r-2"},
        attributes={"step_key": "model_a"},
        error={
            "message": "step blew up",
            "exception_type": "RuntimeError",
            "code": "STEP_FAILED",
            "retryable": True,
        },
    )
    wire = json.loads(env.to_json_bytes())
    envelope_validator.validate(wire)
    reparsed = EventEnvelope.model_validate(wire)
    assert reparsed.event_id == env.event_id


def test_schema_rejects_missing_required(
    envelope_validator: Validator,
) -> None:
    """The schema itself must reject an event missing required fields."""
    from jsonschema import ValidationError

    with pytest.raises(ValidationError):
        envelope_validator.validate({"schema_version": "1.0", "event_id": str(uuid.uuid4())})


def test_v1_envelope_still_validates(v1_validator: Validator) -> None:
    """A V1-shaped emission stays valid under the V1 schema (spec §43)."""
    env = EventEnvelope(
        schema_version="1.0",
        event_id=new_event_id(),
        event="pipeline.run",
        category="pipeline",
        outcome="success",
        severity="info",
        delivery="telemetry",
        observed_at="2025-01-01T00:00:00Z",
        service={"name": "contract-test"},
    )
    v1_validator.validate(env.to_canonical_dict())


def test_v2_envelope_extensions_validate(envelope_validator: Validator) -> None:
    """entities/tags/contract round-trip through the V2 schema."""
    env = EventEnvelope(
        event_id=new_event_id(),
        event="asset.materialized",
        category="data",
        outcome="success",
        severity="info",
        delivery="telemetry",
        observed_at="2025-01-01T00:00:00Z",
        service={"name": "contract-test"},
        entities={"subject": "asset://silver/samples"},
        tags={"team": "data"},
        contract={
            "name": "asset.materialized",
            "version": 1,
            "schema_id": "asset.materialized/v1",
            "schema_hash": "0" * 64,
        },
    )
    envelope_validator.validate(env.to_canonical_dict())
