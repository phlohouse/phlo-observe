"""Track failed derived-state updates durably for operator repair."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "observe_projection_failures",
        sa.Column("failure_id", sa.Uuid(), primary_key=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_ids", postgresql.JSONB(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("error_type", sa.String(128), nullable=False),
    )
    op.create_index(
        "ix_projection_failures_occurred", "observe_projection_failures", ["occurred_at"]
    )


def downgrade() -> None:
    op.drop_table("observe_projection_failures")
