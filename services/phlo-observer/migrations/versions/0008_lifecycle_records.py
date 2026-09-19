"""Persist operator lifecycle decisions independently of projections."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "observe_lifecycle_records",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("target_type", sa.String(length=16), nullable=False),
        sa.Column("target_id", sa.UUID(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("transitioned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_lifecycle_target",
        "observe_lifecycle_records",
        ["target_type", "target_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_lifecycle_target", table_name="observe_lifecycle_records")
    op.drop_table("observe_lifecycle_records")
