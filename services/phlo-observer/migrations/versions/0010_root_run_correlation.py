"""Persist root-run correlation for nested run timelines.

Revision ID: 0010
Revises: 0009
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("events", sa.Column("root_run_id", sa.String(length=128), nullable=True))
    op.create_index(
        "ix_events_root_run_observed",
        "events",
        ["root_run_id", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_events_root_run_observed", table_name="events")
    op.drop_column("events", "root_run_id")
