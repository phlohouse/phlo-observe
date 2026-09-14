"""initial schema: raw_events, events, runs

Revision ID: 0001
Revises:
Create Date: 2025-01-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSONB_OR_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "raw_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("producer", sa.String(length=64), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_version", sa.String(length=64), nullable=True),
        sa.Column("content_type", sa.String(length=128), nullable=False),
        sa.Column("payload", JSONB_OR_JSON, nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("adapter", sa.String(length=64), nullable=True),
        sa.Column("normalization_status", sa.String(length=32), nullable=False),
        sa.Column("normalization_error", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_raw_events_received_at", "raw_events", ["received_at"])

    op.create_table(
        "events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("event", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=32), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("delivery", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Double(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("service_name", sa.String(length=128), nullable=True),
        sa.Column("service_version", sa.String(length=64), nullable=True),
        sa.Column("environment", sa.String(length=64), nullable=True),
        sa.Column("trace_id", sa.String(length=64), nullable=True),
        sa.Column("span_id", sa.String(length=64), nullable=True),
        sa.Column("run_id", sa.String(length=128), nullable=True),
        sa.Column("job_id", sa.String(length=255), nullable=True),
        sa.Column("invocation_id", sa.String(length=128), nullable=True),
        sa.Column("asset_key", sa.String(length=512), nullable=True),
        sa.Column("partition_key", sa.String(length=255), nullable=True),
        sa.Column("branch", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=512), nullable=True),
        sa.Column("snapshot_id", sa.String(length=128), nullable=True),
        sa.Column("pipeline", sa.String(length=255), nullable=True),
        sa.Column("correlation_method", sa.String(length=64), nullable=True),
        sa.Column("attributes", JSONB_OR_JSON, nullable=False),
        sa.Column("error", JSONB_OR_JSON, nullable=True),
        sa.Column("source", JSONB_OR_JSON, nullable=False),
        sa.Column("payload", JSONB_OR_JSON, nullable=False),
        sa.Column("raw_event_id", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(["raw_event_id"], ["raw_events.id"]),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index("ix_events_observed_at", "events", ["observed_at"])
    op.create_index("ix_events_run_observed", "events", ["run_id", "observed_at"])
    op.create_index("ix_events_asset_observed", "events", ["asset_key", "observed_at"])
    op.create_index("ix_events_event_observed", "events", ["event", "observed_at"])
    op.create_index("ix_events_outcome_observed", "events", ["outcome", "observed_at"])
    op.create_index("ix_events_trace_id", "events", ["trace_id"])
    op.create_index("ix_events_branch", "events", ["branch"])
    op.create_index("ix_events_table_observed", "events", ["table_name", "observed_at"])

    op.create_table(
        "runs",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("job_name", sa.String(length=255), nullable=True),
        sa.Column("service_name", sa.String(length=128), nullable=True),
        sa.Column("environment", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Double(), nullable=True),
        sa.Column("trigger", sa.String(length=64), nullable=True),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("error_count", sa.Integer(), nullable=False),
        sa.Column("warning_count", sa.Integer(), nullable=False),
        sa.Column("asset_count", sa.Integer(), nullable=False),
        sa.Column("branch", sa.String(length=255), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("summary", JSONB_OR_JSON, nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index("ix_runs_updated_at", "runs", ["updated_at"])


def downgrade() -> None:
    op.drop_table("runs")
    op.drop_table("events")
    op.drop_table("raw_events")
