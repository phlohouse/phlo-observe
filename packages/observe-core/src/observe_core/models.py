"""Canonical event model: enums, nested objects and the event envelope.

The envelope mirrors ``schemas/event-envelope-v1.schema.json``. Keep the two in
sync; contract tests validate emitted envelopes against the schema.
"""

from __future__ import annotations

import datetime as dt
import re
from enum import StrEnum
from typing import Annotated, Any

import orjson
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, PlainSerializer, model_validator

from observe_core.timestamps import ensure_utc, format_rfc3339

SCHEMA_VERSION = "2.0"
"""Canonical schema version emitted by this library (V2 envelope)."""

SCHEMA_VERSION_V1 = "1.0"
"""The V1 envelope version, still accepted for ingestion (spec §43)."""

SCHEMA_VERSION_PATTERN = re.compile(r"^[12]\.\d+$")
EVENT_NAME_PATTERN = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")
DURATION_TOLERANCE_MS = 2.0
"""Allowed drift between ``duration_ms`` and ``ended_at - started_at``."""

Rfc3339DateTime = Annotated[
    dt.datetime,
    AfterValidator(ensure_utc),
    PlainSerializer(format_rfc3339, return_type=str, when_used="json-unless-none"),
]
"""An aware datetime serialized as UTC RFC3339 with a ``Z`` suffix."""


class Category(StrEnum):
    """High-level event classification."""

    APPLICATION = "application"
    PIPELINE = "pipeline"
    DATA = "data"
    QUALITY = "quality"
    WAP = "wap"
    QUERY = "query"
    STORAGE = "storage"
    INFRASTRUCTURE = "infrastructure"
    SECURITY = "security"
    OBSERVER = "observer"
    METRIC = "metric"
    LINEAGE = "lineage"
    INSIGHT = "insight"
    INCIDENT = "incident"
    OTHER = "other"


class Outcome(StrEnum):
    """Terminal outcome of the operation described by the event."""

    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class Severity(StrEnum):
    """Severity of the emitted record."""

    TRACE = "trace"
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    CRITICAL = "critical"


class Delivery(StrEnum):
    """Durability class controlling queue pressure and spool behaviour."""

    DEBUG = "debug"
    TELEMETRY = "telemetry"
    CRITICAL = "critical"


SEVERITY_RANK: dict[Severity, int] = {
    Severity.TRACE: 10,
    Severity.DEBUG: 20,
    Severity.INFO: 30,
    Severity.WARN: 40,
    Severity.ERROR: 50,
    Severity.CRITICAL: 60,
}
"""Numeric ordering for severities (StrEnum ``<`` compares alphabetically)."""


def severity_at_least(severity: Severity, floor: Severity) -> bool:
    """Return True when ``severity`` is at least ``floor`` in importance."""
    return SEVERITY_RANK[severity] >= SEVERITY_RANK[floor]


class ServiceInfo(BaseModel):
    """Identity of the emitting service."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str | None = None
    instance_id: str | None = None
    environment: str | None = None
    host: str | None = None


CORRELATION_KEYS: tuple[str, ...] = (
    "trace_id",
    "span_id",
    "parent_span_id",
    "run_id",
    "root_run_id",
    "job_id",
    "invocation_id",
    "asset_key",
    "partition_key",
    "branch",
    "table",
    "snapshot_id",
    "pipeline",
    "experiment_id",
    "request_id",
)
"""Canonical correlation keys. Anything else belongs in ``Correlation.extra``."""


class Correlation(BaseModel):
    """Correlation identifiers relating an event to runs, assets and traces."""

    model_config = ConfigDict(extra="forbid")

    trace_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    run_id: str | None = None
    root_run_id: str | None = None
    job_id: str | None = None
    invocation_id: str | None = None
    asset_key: str | None = None
    partition_key: str | None = None
    branch: str | None = None
    table: str | None = None
    snapshot_id: str | None = None
    pipeline: str | None = None
    experiment_id: str | None = None
    request_id: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)

    def canonical_dict(self) -> dict[str, str | None]:
        """Return only the canonical identifier keys (no ``extra``)."""
        return {key: getattr(self, key) for key in CORRELATION_KEYS}


class ContractRef(BaseModel):
    """Contract reference on a V2 envelope (spec §7.2/§7.3).

    Names the registered :class:`~observe_core.contracts.EventContract` the
    event validated against.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    version: int
    schema_id: str | None = None
    schema_hash: str | None = None


class ErrorInfo(BaseModel):
    """Structured, machine-readable failure details."""

    model_config = ConfigDict(extra="forbid")

    code: str | None = None
    message: str
    why: str | None = None
    fix: str | None = None
    exception_type: str | None = None
    retryable: bool = False
    stacktrace: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class SourceInfo(BaseModel):
    """Canonical representation of the producer an event originated from."""

    model_config = ConfigDict(extra="forbid")

    producer: str
    producer_version: str | None = None
    kind: str | None = None
    raw_ref: str | None = None
    adapter: str | None = None
    ingested_at: Rfc3339DateTime | None = None


class EventEnvelope(BaseModel):
    """The canonical normalized event emitted, transported and stored.

    ``started_at``/``ended_at``/``duration_ms`` are null for instantaneous
    events. When all three are present, ``duration_ms`` must be consistent with
    the timestamps within :data:`DURATION_TOLERANCE_MS`.
    """

    model_config = ConfigDict(extra="allow")

    schema_version: str = SCHEMA_VERSION
    event_id: str
    event: str
    category: Category = Category.OTHER
    outcome: Outcome = Outcome.UNKNOWN
    severity: Severity = Severity.INFO
    delivery: Delivery = Delivery.TELEMETRY
    started_at: Rfc3339DateTime | None = None
    ended_at: Rfc3339DateTime | None = None
    duration_ms: float | None = None
    observed_at: Rfc3339DateTime
    service: ServiceInfo
    correlation: Correlation = Field(default_factory=Correlation)
    attributes: dict[str, Any] = Field(default_factory=dict)
    error: ErrorInfo | None = None
    source: SourceInfo | None = None
    # V2 envelope extensions (spec §43): additive, optional, and omitted from
    # the canonical dict when empty so 1.x emissions stay byte-compatible.
    entities: dict[str, str] = Field(default_factory=dict)
    """Canonical entity identifiers by role, e.g. ``{"asset": "asset://silver/samples"}``."""
    tags: dict[str, str] = Field(default_factory=dict)
    """Searchable labels (spec §23)."""
    contract: ContractRef | None = None
    """Contract the event validated against, when one is registered."""

    @model_validator(mode="after")
    def _check_envelope(self) -> EventEnvelope:
        if not SCHEMA_VERSION_PATTERN.match(self.schema_version):
            raise ValueError(f"schema_version must be 1.x or 2.x, got {self.schema_version!r}")
        if not EVENT_NAME_PATTERN.match(self.event):
            raise ValueError(
                f"event name must be lowercase ASCII dot-delimited, got {self.event!r}"
            )
        if (
            self.started_at is not None
            and self.ended_at is not None
            and self.duration_ms is not None
        ):
            actual_ms = (self.ended_at - self.started_at).total_seconds() * 1000.0
            if abs(actual_ms - self.duration_ms) > DURATION_TOLERANCE_MS:
                raise ValueError(
                    "duration_ms is inconsistent with started_at/ended_at "
                    f"(duration_ms={self.duration_ms}, derived={actual_ms:.3f})"
                )
        if self.error is not None and not self.error.message:
            raise ValueError("error.message must be non-empty when error is present")
        return self

    def to_canonical_dict(self) -> dict[str, Any]:
        """Return the JSON-mode dict matching the emitted envelope version.

        Empty V2 extensions are dropped so a ``1.x`` envelope stays
        byte-compatible with ``event-envelope-v1``.
        """
        data = self.model_dump(mode="json")
        for key in ("entities", "tags"):
            if not data.get(key):
                data.pop(key, None)
        if data.get("contract") is None:
            data.pop("contract", None)
        return data

    def to_json_bytes(self) -> bytes:
        """Serialize the envelope to canonical UTF-8 JSON bytes."""
        return orjson.dumps(self.to_canonical_dict())

    @classmethod
    def from_json(cls, data: bytes | str) -> EventEnvelope:
        """Parse an envelope from canonical JSON."""
        return cls.model_validate(orjson.loads(data))
