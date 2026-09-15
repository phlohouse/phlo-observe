"""v2 state engine: entities, relationships, assets, baselines, insights,
incidents, schemas, ingest failures; V2 event columns.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSONB_OR_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    with op.batch_alter_table("events") as batch:
        batch.add_column(sa.Column("entities", JSONB_OR_JSON, nullable=False, server_default="{}"))
        batch.add_column(sa.Column("tags", JSONB_OR_JSON, nullable=False, server_default="{}"))
        batch.add_column(sa.Column("contract_id", sa.String(length=255), nullable=True))

    with op.batch_alter_table("runs") as batch:
        batch.add_column(
            sa.Column("provenance", JSONB_OR_JSON, nullable=False, server_default="{}")
        )

    op.create_table(
        "observe_entities",
        sa.Column("entity_id", sa.String(length=1024), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=512), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attributes", JSONB_OR_JSON, nullable=False),
        sa.Column("provenance", JSONB_OR_JSON, nullable=False),
        sa.PrimaryKeyConstraint("entity_id"),
    )
    op.create_index("ix_observe_entities_kind", "observe_entities", ["kind"])
    op.create_index("ix_observe_entities_last_seen", "observe_entities", ["last_seen_at"])

    op.create_table(
        "observe_relationships",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("from_entity", sa.String(length=1024), nullable=False),
        sa.Column("to_entity", sa.String(length=1024), nullable=False),
        sa.Column("relationship_type", sa.String(length=64), nullable=False),
        sa.Column("method", sa.String(length=32), nullable=False),
        sa.Column("confidence", sa.Double(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_event_ids", JSONB_OR_JSON, nullable=False),
        sa.Column("provenance", JSONB_OR_JSON, nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_observe_rel_from", "observe_relationships", ["from_entity"])
    op.create_index("ix_observe_rel_to", "observe_relationships", ["to_entity"])
    op.create_index("ix_observe_rel_type", "observe_relationships", ["relationship_type"])
    op.create_index(
        "uq_observe_rel_edge",
        "observe_relationships",
        ["from_entity", "to_entity", "relationship_type"],
        unique=True,
    )

    op.create_table(
        "observe_assets",
        sa.Column("entity_id", sa.String(length=1024), nullable=False),
        sa.Column("asset_key", sa.String(length=512), nullable=False),
        sa.Column("last_materialized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("freshness_sla_seconds", sa.Double(), nullable=True),
        sa.Column("attributes", JSONB_OR_JSON, nullable=False),
        sa.Column("provenance", JSONB_OR_JSON, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("entity_id"),
    )
    op.create_index("ix_observe_assets_key", "observe_assets", ["asset_key"])

    op.create_table(
        "observe_baselines",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("entity_id", sa.String(length=1024), nullable=False),
        sa.Column("metric", sa.String(length=128), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("median", sa.Double(), nullable=True),
        sa.Column("mad", sa.Double(), nullable=True),
        sa.Column("mean", sa.Double(), nullable=True),
        sa.Column("p10", sa.Double(), nullable=True),
        sa.Column("p90", sa.Double(), nullable=True),
        sa.Column("samples", JSONB_OR_JSON, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_observe_baselines_entity_metric",
        "observe_baselines",
        ["entity_id", "metric"],
        unique=True,
    )

    op.create_table(
        "observe_insights",
        sa.Column("insight_id", sa.Uuid(), nullable=False),
        sa.Column("rule_id", sa.String(length=128), nullable=False),
        sa.Column("rule_version", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("entity_id", sa.String(length=1024), nullable=True),
        sa.Column("evidence_event_ids", JSONB_OR_JSON, nullable=False),
        sa.Column("evidence_metric_ids", JSONB_OR_JSON, nullable=False),
        sa.Column("recommended_action", sa.Text(), nullable=True),
        sa.Column("recommended_action_verified", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dedupe_key", sa.String(length=512), nullable=True),
        sa.Column("attributes", JSONB_OR_JSON, nullable=False),
        sa.PrimaryKeyConstraint("insight_id"),
    )
    op.create_index("ix_observe_insights_state", "observe_insights", ["state"])
    op.create_index("ix_observe_insights_entity", "observe_insights", ["entity_id"])
    op.create_index("ix_observe_insights_dedupe", "observe_insights", ["dedupe_key"])

    op.create_table(
        "observe_incidents",
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("entities", JSONB_OR_JSON, nullable=False),
        sa.Column("insight_ids", JSONB_OR_JSON, nullable=False),
        sa.Column("timeline", JSONB_OR_JSON, nullable=False),
        sa.Column("impact", JSONB_OR_JSON, nullable=False),
        sa.Column("attributes", JSONB_OR_JSON, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("incident_id"),
    )
    op.create_index("ix_observe_incidents_state", "observe_incidents", ["state"])

    op.create_table(
        "observe_schemas",
        sa.Column("schema_id", sa.String(length=255), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("schema_hash", sa.String(length=64), nullable=False),
        sa.Column("schema_json", JSONB_OR_JSON, nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("schema_id"),
    )

    op.create_table(
        "observe_ingest_failures",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("producer", sa.String(length=64), nullable=False),
        sa.Column("adapter", sa.String(length=64), nullable=True),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload", JSONB_OR_JSON, nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("replayed", sa.Integer(), nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_observe_ingest_failures_received",
        "observe_ingest_failures",
        ["received_at"],
    )


def downgrade() -> None:
    op.drop_table("observe_ingest_failures")
    op.drop_table("observe_schemas")
    op.drop_table("observe_incidents")
    op.drop_table("observe_insights")
    op.drop_table("observe_baselines")
    op.drop_table("observe_assets")
    op.drop_table("observe_relationships")
    op.drop_table("observe_entities")
    with op.batch_alter_table("runs") as batch:
        batch.drop_column("provenance")
    with op.batch_alter_table("events") as batch:
        batch.drop_column("contract_id")
        batch.drop_column("tags")
        batch.drop_column("entities")
