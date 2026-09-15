"""SQLAlchemy models: raw_events, events, and V2 state-engine tables.

PostgreSQL is the canonical store; column types use ``JSONB`` on Postgres and
fall back to generic JSON elsewhere so the test suite can run on SQLite.

V2 adds the derived-state families from spec §24.2: entity registry,
relationship edges, asset/branch projections, baselines, insights,
incidents, schema registry and the ingest quarantine. Every projection row
carries ``provenance`` (spec §12.3) and all projections are rebuildable
from canonical events (spec §12.4).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import DateTime, Double, ForeignKey, Index, Integer, String, Text, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

JsonColumn = JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    """Observer declarative base."""


class RawEvent(Base):
    """Preserved source payload for debugging normalization."""

    __tablename__ = "raw_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    received_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    producer: Mapped[str] = mapped_column(String(64), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    source_version: Mapped[str | None] = mapped_column(String(64))
    content_type: Mapped[str] = mapped_column(
        String(128), nullable=False, default="application/json"
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    adapter: Mapped[str | None] = mapped_column(String(64))
    normalization_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    normalization_error: Mapped[str | None] = mapped_column(Text)
    expires_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_raw_events_received_at", "received_at"),)


class Event(Base):
    """Normalized canonical event row."""

    __tablename__ = "events"

    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    event: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(32), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    delivery: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[float | None] = mapped_column(Double)
    observed_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    service_name: Mapped[str | None] = mapped_column(String(128))
    service_version: Mapped[str | None] = mapped_column(String(64))
    environment: Mapped[str | None] = mapped_column(String(64))
    trace_id: Mapped[str | None] = mapped_column(String(64))
    span_id: Mapped[str | None] = mapped_column(String(64))
    run_id: Mapped[str | None] = mapped_column(String(128))
    job_id: Mapped[str | None] = mapped_column(String(255))
    invocation_id: Mapped[str | None] = mapped_column(String(128))
    asset_key: Mapped[str | None] = mapped_column(String(512))
    partition_key: Mapped[str | None] = mapped_column(String(255))
    branch: Mapped[str | None] = mapped_column(String(255))
    table_name: Mapped[str | None] = mapped_column(String(512))
    snapshot_id: Mapped[str | None] = mapped_column(String(128))
    pipeline: Mapped[str | None] = mapped_column(String(255))
    correlation_method: Mapped[str | None] = mapped_column(String(64))
    attributes: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    error: Mapped[dict[str, Any] | None] = mapped_column(JsonColumn)
    source: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    """Full canonical envelope as received; basis for duplicate detection."""
    raw_event_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("raw_events.id", ondelete="SET NULL"), nullable=True
    )
    entities: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    """V2 canonical entity identifiers by role (spec §9.2)."""
    tags: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    """V2 searchable labels (spec §23)."""
    contract_id: Mapped[str | None] = mapped_column(String(255))
    """V2 contract schema_id the event validated against, when registered."""

    __table_args__ = (
        Index("ix_events_observed_at", "observed_at"),
        Index("ix_events_run_observed", "run_id", "observed_at"),
        Index("ix_events_asset_observed", "asset_key", "observed_at"),
        Index("ix_events_event_observed", "event", "observed_at"),
        Index("ix_events_outcome_observed", "outcome", "observed_at"),
        Index("ix_events_trace_id", "trace_id"),
        Index("ix_events_branch", "branch"),
        Index("ix_events_table_observed", "table_name", "observed_at"),
    )


class Run(Base):
    """Derived run projection maintained from correlated events."""

    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_name: Mapped[str | None] = mapped_column(String(255))
    service_name: Mapped[str | None] = mapped_column(String(128))
    environment: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    started_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[float | None] = mapped_column(Double)
    trigger: Mapped[str | None] = mapped_column(String(64))
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    warning_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    asset_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    branch: Mapped[str | None] = mapped_column(String(255))
    updated_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    summary: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    provenance: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    """Derivation provenance: derived_from event ids, rule, rule_version, derived_at (§12.3)."""

    __table_args__ = (Index("ix_runs_updated_at", "updated_at"),)


class Entity(Base):
    """Entity registry row (spec §24.2).

    ``entity_id`` is the canonical namespaced URI (``asset://a/b``,
    ``run://dagster/x``). Correlation-field events register under the
    canonical form derived by ``observe_core.identifiers``.
    """

    __tablename__ = "observe_entities"

    entity_id: Mapped[str] = mapped_column(String(1024), primary_key=True)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(512))
    first_seen_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    provenance: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_observe_entities_kind", "kind"),
        Index("ix_observe_entities_last_seen", "last_seen_at"),
    )


class Relationship(Base):
    """Typed edge between entities (spec §13).

    ``method`` is ``explicit`` when the edge came from envelope fields and
    ``inferred`` when derived by an inference rule; confidence is 1.0 for
    explicit edges and rule-supplied for inferred ones (§13.2).
    """

    __tablename__ = "observe_relationships"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    from_entity: Mapped[str] = mapped_column(String(1024), nullable=False)
    to_entity: Mapped[str] = mapped_column(String(1024), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(64), nullable=False)
    method: Mapped[str] = mapped_column(String(32), nullable=False, default="explicit")
    confidence: Mapped[float] = mapped_column(Double, nullable=False, default=1.0)
    first_seen_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    source_event_ids: Mapped[list[str]] = mapped_column(JsonColumn, nullable=False, default=list)
    provenance: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_observe_rel_from", "from_entity"),
        Index("ix_observe_rel_to", "to_entity"),
        Index("ix_observe_rel_type", "relationship_type"),
        Index(
            "uq_observe_rel_edge",
            "from_entity",
            "to_entity",
            "relationship_type",
            unique=True,
        ),
    )


class Asset(Base):
    """Asset projection (spec §12.1): last activity, freshness, health."""

    __tablename__ = "observe_assets"

    entity_id: Mapped[str] = mapped_column(String(1024), primary_key=True)
    asset_key: Mapped[str] = mapped_column(String(512), nullable=False)
    last_materialized_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    last_event_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    freshness_sla_seconds: Mapped[float | None] = mapped_column(Double)
    attributes: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    provenance: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    updated_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_observe_assets_key", "asset_key"),)


class Baseline(Base):
    """Rolling statistic per (entity, metric) for anomaly rules (spec §18.2)."""

    __tablename__ = "observe_baselines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    metric: Mapped[str] = mapped_column(String(128), nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    median: Mapped[float | None] = mapped_column(Double)
    mad: Mapped[float | None] = mapped_column(Double)
    mean: Mapped[float | None] = mapped_column(Double)
    p10: Mapped[float | None] = mapped_column(Double)
    p90: Mapped[float | None] = mapped_column(Double)
    samples: Mapped[list[float]] = mapped_column(JsonColumn, nullable=False, default=list)
    updated_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("uq_observe_baselines_entity_metric", "entity_id", "metric", unique=True),
    )


class Insight(Base):
    """Deterministic rule insight with lifecycle state (spec §16.2)."""

    __tablename__ = "observe_insights"

    insight_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    rule_id: Mapped[str] = mapped_column(String(128), nullable=False)
    rule_version: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    entity_id: Mapped[str | None] = mapped_column(String(1024))
    evidence_event_ids: Mapped[list[str]] = mapped_column(JsonColumn, nullable=False, default=list)
    evidence_metric_ids: Mapped[list[str]] = mapped_column(JsonColumn, nullable=False, default=list)
    recommended_action: Mapped[str | None] = mapped_column(Text)
    recommended_action_verified: Mapped[bool] = mapped_column(
        Integer, nullable=False, default=0
    )  # bool via Integer for cross-dialect
    created_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    dedupe_key: Mapped[str | None] = mapped_column(String(512))
    attributes: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_observe_insights_state", "state"),
        Index("ix_observe_insights_entity", "entity_id"),
        Index("ix_observe_insights_dedupe", "dedupe_key"),
    )


class Incident(Base):
    """Incident grouping insights by entity/time (spec §20)."""

    __tablename__ = "observe_incidents"

    incident_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="warn")
    started_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))
    entities: Mapped[list[str]] = mapped_column(JsonColumn, nullable=False, default=list)
    insight_ids: Mapped[list[str]] = mapped_column(JsonColumn, nullable=False, default=list)
    timeline: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    impact: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    attributes: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    updated_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_observe_incidents_state", "state"),)


class SchemaRecord(Base):
    """Registered contract/schema version (spec §8.3)."""

    __tablename__ = "observe_schemas"

    schema_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_json: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    registered_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)


class AgentAnalysis(Base):
    """LLM-generated analysis provenance (spec §21.4, §24.2).

    Stores which model produced which output from which evidence IDs so a
    generated conclusion is always traceable to its inputs.
    """

    __tablename__ = "observe_agent_analyses"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    model: Mapped[str] = mapped_column(String(255), nullable=False)
    prompt_template_version: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_event_ids: Mapped[list[str]] = mapped_column(JsonColumn, nullable=False, default=list)
    output: Mapped[dict[str, Any]] = mapped_column(JsonColumn, nullable=False, default=dict)
    feedback: Mapped[dict[str, Any] | None] = mapped_column(JsonColumn)
    subject: Mapped[str | None] = mapped_column(String(1024))
    """Entity or run the analysis is about, when applicable."""

    __table_args__ = (Index("ix_observe_agent_analyses_created", "created_at"),)


class IngestFailure(Base):
    """Quarantined raw payload (spec §24.2 observe_ingest_failures).

    Adapter-level failures land here when the payload cannot be normalized
    at all — ``raw_events`` already captures per-event normalization errors,
    so this table is for payload-level failures (parse errors, oversized
    bodies, adapter crashes) that have no normalizable events.
    """

    __tablename__ = "observe_ingest_failures"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    received_at: Mapped[Any] = mapped_column(DateTime(timezone=True), nullable=False)
    producer: Mapped[str] = mapped_column(String(64), nullable=False)
    adapter: Mapped[str | None] = mapped_column(String(64))
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JsonColumn)
    error_code: Mapped[str] = mapped_column(String(64), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    replayed: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)
    replayed_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_observe_ingest_failures_received", "received_at"),)
