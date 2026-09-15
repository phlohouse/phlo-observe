"""SQLAlchemy models: raw_events, events, runs projection.

PostgreSQL is the canonical store; column types use ``JSONB`` on Postgres and
fall back to generic JSON elsewhere so the test suite can run on SQLite.
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

    __table_args__ = (Index("ix_runs_updated_at", "updated_at"),)
