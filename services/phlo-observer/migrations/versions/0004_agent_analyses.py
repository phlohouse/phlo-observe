"""v2: observe_agent_analyses — LLM output provenance (spec 21.4/24.2).

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSONB_OR_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "observe_agent_analyses",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("model", sa.String(length=255), nullable=False),
        sa.Column("prompt_template_version", sa.String(length=64), nullable=False),
        sa.Column("evidence_event_ids", JSONB_OR_JSON, nullable=False),
        sa.Column("output", JSONB_OR_JSON, nullable=False),
        sa.Column("feedback", JSONB_OR_JSON, nullable=True),
        sa.Column("subject", sa.String(length=1024), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_observe_agent_analyses_created",
        "observe_agent_analyses",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_table("observe_agent_analyses")
